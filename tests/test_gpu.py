"""Simulated GPUs (fake K8s): gpu-sim worker group, GPU quota admission, pending reasons."""

from __future__ import annotations

import pytest
from kubernetes.utils import parse_quantity

from platform_api.manifests import build_rayjob, job_requests
from platform_api.models import JobSpec, Resources, memory_mib
from platform_api.reconciler import reconcile_once
from tests.conftest import KEY_A, KEY_B, Platform

GPU_JOB = {
    "entrypoint": "python gpu_sim.py 60",
    "files": {"gpu_sim.py": "pass"},
    "workers": 0,
    "gpus": 1,
    "memory": "512Mi",
}
CPU_JOB = {**GPU_JOB, "workers": 1, "gpus": 0}
QUOTA = Resources(cpu_m=2000, mem_mib=4096, gpu=1)


def groups(spec: JobSpec) -> dict[str, dict]:
    rj = build_rayjob("abc", "team-a", spec, image="i", ray_version="2.59.0")
    return {g["groupName"]: g for g in rj["spec"]["rayClusterSpec"]["workerGroupSpecs"]}


def test_gpu_job_gets_a_gpu_sim_group_holding_one_gpu_each() -> None:
    g = groups(JobSpec(**GPU_JOB))
    assert set(g) == {"gpu-sim"}  # workers=0: no CPU group at all
    params = g["gpu-sim"]["rayStartParams"]
    assert params == {"num-cpus": "1", "resources": '"{\\"sim_gpu\\": 1}"'}
    res = g["gpu-sim"]["template"]["spec"]["containers"][0]["resources"]
    assert res["requests"]["example.com/gpu"] == res["limits"]["example.com/gpu"] == "1"


def test_mixed_job_keeps_gpus_off_the_cpu_group() -> None:
    g = groups(JobSpec(**{**GPU_JOB, "workers": 2}))
    assert set(g) == {"cpu", "gpu-sim"}
    cpu = g["cpu"]["template"]["spec"]["containers"][0]["resources"]
    assert "example.com/gpu" not in cpu["requests"]
    assert "resources" not in g["cpu"]["rayStartParams"]


def test_job_requests_count_the_gpu_and_match_the_manifest() -> None:
    spec = JobSpec(**{**GPU_JOB, "workers": 1})
    rj = build_rayjob("abc", "team-a", spec, image="i", ray_version="2.59.0")
    cluster = rj["spec"]["rayClusterSpec"]
    templates = [cluster["headGroupSpec"]["template"], rj["spec"]["submitterPodTemplate"]]
    templates += [g["template"] for g in cluster["workerGroupSpecs"] for _ in range(g["replicas"])]
    total = Resources()
    for t in templates:
        req = t["spec"]["containers"][0]["resources"]["requests"]
        total += Resources(
            int(parse_quantity(req["cpu"]) * 1000),
            memory_mib(req["memory"]),
            int(req.get("example.com/gpu", 0)),
        )
    assert job_requests(spec) == total
    assert total.gpu == 1


@pytest.mark.parametrize(
    "bad", [{"workers": 0, "gpus": 0}, {"gpus": 2}, {"resources": {"sim_gpu": 1}}]
)
def test_invalid_gpu_specs_are_422(platform: Platform, bad: dict) -> None:
    r = platform.http.post("/jobs", json={**GPU_JOB, **bad}, headers=platform.as_team(KEY_A))
    assert r.status_code == 422, r.text


def post(p: Platform, body: dict, key: str = KEY_A) -> dict:
    r = p.http.post("/jobs", json=body, headers=p.as_team(key))
    assert r.status_code == 201, r.text
    return r.json()


def test_one_gpu_per_team_quota_queues_the_second_gpu_job_but_not_cpu_jobs(
    platform: Platform,
) -> None:
    platform.kube.quotas.update({"team-a": QUOTA, "team-b": QUOTA})
    first = post(platform, GPU_JOB)
    second = post(platform, GPU_JOB)
    cpu = post(platform, CPU_JOB)
    other_team = post(platform, GPU_JOB, KEY_B)
    assert first["state"] == "SUBMITTED"
    assert second["state"] == "QUEUED"
    assert "1 GPU" in second["message"]
    # FIFO: a CPU job behind a queued GPU job waits too (no queue jumping), even though it fits.
    assert cpu["state"] == "QUEUED"
    # team-b has its own GPU quota; the node's single GPU is contended at the scheduler instead.
    assert other_team["state"] == "SUBMITTED"


def test_cpu_job_is_admitted_while_the_teams_gpu_is_busy(platform: Platform) -> None:
    platform.kube.quotas["team-a"] = QUOTA
    assert post(platform, GPU_JOB)["state"] == "SUBMITTED"
    assert post(platform, CPU_JOB)["state"] == "SUBMITTED"


def test_gpu_job_without_gpu_quota_is_rejected(platform: Platform) -> None:
    platform.kube.quotas["team-a"] = Resources(2000, 4096, gpu=0)
    r = platform.http.post("/jobs", json=GPU_JOB, headers=platform.as_team(KEY_A))
    assert r.status_code == 422
    assert "1 GPU" in r.json()["detail"]


def test_unschedulable_reason_shows_while_pending_and_clears_once_running(
    platform: Platform,
) -> None:
    job = post(platform, GPU_JOB)
    admitter = platform.http.app.state.admitter  # type: ignore[attr-defined]
    msg = "0/1 nodes are available: 1 Insufficient example.com/gpu."
    platform.kube.unschedulable[job["id"]] = msg
    platform.kube.set_status("team-a", job["k8s_name"], "Initializing")
    reconcile_once(platform.registry, platform.kube, admitter)
    got = platform.registry.get(job["id"])
    assert got is not None and got.state == "PENDING"
    assert got.message == f"waiting for scheduling: {msg}"

    del platform.kube.unschedulable[job["id"]]
    platform.kube.set_status("team-a", job["k8s_name"], "Running", "RUNNING")
    reconcile_once(platform.registry, platform.kube, admitter)
    got = platform.registry.get(job["id"])
    assert got is not None and got.state == "RUNNING" and got.message is None
