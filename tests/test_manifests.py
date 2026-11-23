"""RayJob manifest built from a JobSpec, and KubeRay status → job state mapping."""

from __future__ import annotations

import pytest

from platform_api.config import ROOT, read_env_file
from platform_api.manifests import CODE_DIR, build_rayjob, ray_resources_param
from platform_api.models import JobSpec, JobState
from platform_api.reconciler import observe

VERSIONS = read_env_file(ROOT / "infra" / "versions.env")


def build(**spec: object) -> dict:
    s = JobSpec(entrypoint="python train.py", files={"train.py": ""}, **spec)  # type: ignore[arg-type]
    return build_rayjob(
        "abc12345", "team-a", s, image=VERSIONS["RKP_IMAGE"], ray_version=VERSIONS["RAY_VERSION"]
    )


def test_rayjob_shape() -> None:
    rj = build(workers=3, cpu=2, memory="2Gi", retries=2)
    spec = rj["spec"]
    assert rj["metadata"]["name"] == "rkp-abc12345"
    assert rj["metadata"]["labels"]["rkp.io/team"] == "team-a"
    assert spec["entrypoint"] == f"env -C {CODE_DIR} python train.py"
    assert spec["backoffLimit"] == 2
    assert spec["shutdownAfterJobFinishes"] is True
    assert spec["rayClusterSpec"]["rayVersion"] == VERSIONS["RAY_VERSION"]
    assert spec["rayClusterSpec"]["headGroupSpec"]["rayStartParams"] == {"num-cpus": "0"}
    group = spec["rayClusterSpec"]["workerGroupSpecs"][0]
    assert group["replicas"] == group["minReplicas"] == group["maxReplicas"] == 3
    assert group["rayStartParams"] == {"num-cpus": "2"}
    worker = group["template"]["spec"]["containers"][0]
    assert {"name": "PYTHONPATH", "value": CODE_DIR} in worker["env"]
    assert {"name": "RKP_JOB_ID", "value": "abc12345"} in worker["env"]
    assert worker["resources"] == {
        "requests": {"cpu": "1000m", "memory": "1024Mi"},
        "limits": {"cpu": "2", "memory": "2048Mi"},
    }


def test_every_container_uses_the_pinned_image_without_pulling() -> None:
    spec = build()["spec"]
    pods = [
        spec["submitterPodTemplate"],
        spec["rayClusterSpec"]["headGroupSpec"]["template"],
        spec["rayClusterSpec"]["workerGroupSpecs"][0]["template"],
    ]
    for pod in pods:
        for c in pod["spec"]["containers"]:
            assert c["image"] == VERSIONS["RKP_IMAGE"]
            assert c["imagePullPolicy"] == "Never"
            assert c["resources"]["requests"] and c["resources"]["limits"]


def test_no_files_means_no_code_volume() -> None:
    s = JobSpec(entrypoint="python -c 'print(1)'")
    rj = build_rayjob("abc12345", "team-a", s, image="x", ray_version="2.59.0")
    assert rj["spec"]["entrypoint"] == "python -c 'print(1)'"
    volumes = rj["spec"]["rayClusterSpec"]["headGroupSpec"]["template"]["spec"]["volumes"]
    assert "code" not in {v["name"] for v in volumes}


def test_shared_data_is_read_only_and_checkpoints_are_per_team() -> None:
    rj = build_rayjob("abc12345", "team-b", JobSpec(entrypoint="x"), image="x", ray_version="2")
    for pod in (
        rj["spec"]["rayClusterSpec"]["headGroupSpec"]["template"],
        rj["spec"]["rayClusterSpec"]["workerGroupSpecs"][0]["template"],
    ):
        volumes = {v["name"]: v["hostPath"] for v in pod["spec"]["volumes"]}
        assert volumes["checkpoints"] == {"path": "/shared/checkpoints/team-b", "type": "Directory"}
        assert volumes["shared-data"]["path"] == "/shared/data"
        mounts = {m["name"]: m for m in pod["spec"]["containers"][0]["volumeMounts"]}
        assert mounts["shared-data"]["readOnly"] is True
        assert mounts["checkpoints"]["mountPath"] == "/shared/checkpoints"
    submitter = rj["spec"]["submitterPodTemplate"]["spec"]["containers"][0]
    assert "volumeMounts" not in submitter


def test_custom_resources_are_shell_quoted_json() -> None:
    group = build(resources={"sim_gpu": 1})["spec"]["rayClusterSpec"]["workerGroupSpecs"][0]
    assert group["rayStartParams"]["resources"] == '"{\\"sim_gpu\\": 1}"'
    assert ray_resources_param({"b": 2, "a": 1}) == '"{\\"a\\": 1, \\"b\\": 2}"'


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        ({}, JobState.PENDING),
        ({"jobDeploymentStatus": "Initializing"}, JobState.PENDING),
        ({"jobDeploymentStatus": "Running", "jobStatus": "PENDING"}, JobState.PENDING),
        ({"jobDeploymentStatus": "Running", "jobStatus": "RUNNING"}, JobState.RUNNING),
        ({"jobDeploymentStatus": "Running", "jobStatus": "SUCCEEDED"}, JobState.RUNNING),
        ({"jobDeploymentStatus": "Retrying", "jobStatus": "FAILED"}, JobState.RETRYING),
        ({"jobDeploymentStatus": "Complete", "jobStatus": "SUCCEEDED"}, JobState.SUCCEEDED),
        ({"jobDeploymentStatus": "Complete", "jobStatus": "FAILED"}, JobState.FAILED),
        ({"jobDeploymentStatus": "Failed", "jobStatus": ""}, JobState.FAILED),
        ({"jobDeploymentStatus": "ValidationFailed"}, JobState.FAILED),
        ({"jobDeploymentStatus": "Complete", "jobStatus": "STOPPED"}, JobState.CANCELLED),
    ],
)
def test_observe(status: dict, expected: JobState) -> None:
    assert observe({"status": status}).state == expected


def test_observe_uses_rays_own_timestamps() -> None:
    obs = observe(
        {
            "status": {
                "jobDeploymentStatus": "Complete",
                "jobStatus": "SUCCEEDED",
                "rayJobInfo": {
                    "startTime": "2026-11-23T09:00:05Z",
                    "endTime": "2026-11-23T09:00:09Z",
                },
            }
        }
    )
    assert obs.started_at is not None and obs.started_at.isoformat() == "2026-11-23T09:00:05+00:00"
    assert obs.finished_at is not None and obs.finished_at.second == 9


@pytest.mark.parametrize(
    "bad", ["python a.py && rm -rf /", "python a.py; id", "a | b", "echo $HOME"]
)
def test_entrypoint_rejects_shell_operators(bad: str) -> None:
    with pytest.raises(ValueError, match="plain command"):
        JobSpec(entrypoint=bad)


def test_long_kuberay_message_is_shortened_to_its_first_line() -> None:
    msg = "Unexpected error: worker killed (OOM)\n" + "detail\n" * 60
    obs = observe({"status": {"jobDeploymentStatus": "Failed", "message": msg}})
    assert obs.message == "Unexpected error: worker killed (OOM)"
    obs = observe({"status": {"jobDeploymentStatus": "Failed", "message": "x" * 1000}})
    assert obs.message is not None and len(obs.message) == 300
