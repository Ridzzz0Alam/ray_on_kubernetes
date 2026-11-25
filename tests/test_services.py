"""Served models (fake K8s): POST/GET/DELETE /services, RayService manifest, quota, status."""

from __future__ import annotations

import json

import httpx
import pytest
from kubernetes.utils import parse_quantity

from platform_api.manifests import build_rayservice, service_requests
from platform_api.models import Resources, ServiceSpec, ServiceState, memory_mib
from platform_api.reconciler import observe_service, reconcile_once
from tests.conftest import KEY_A, KEY_B, Platform

HELLO = {"entrypoint": "python t.py", "files": {"t.py": "pass"}, "workers": 1}


def trained_job(p: Platform, key: str = KEY_A) -> dict:
    job = p.http.post("/jobs", json=HELLO, headers=p.as_team(key)).json()
    team = job["team"]
    p.kube.set_status(team, job["k8s_name"], "Complete", "SUCCEEDED")
    reconcile_once(p.registry, p.kube, p.http.app.state.admitter)  # type: ignore[attr-defined]
    return job


def serve(p: Platform, job_id: str, key: str = KEY_A, **extra: object) -> httpx.Response:
    body = {"name": "sentiment", "job_id": job_id, **extra}
    return p.http.post("/services", json=body, headers=p.as_team(key))


def test_create_service_builds_an_autoscaled_rayservice_with_its_code(platform: Platform) -> None:
    job = trained_job(platform)
    r = serve(platform, job["id"])
    assert r.status_code == 201, r.text
    svc = r.json()
    assert svc["state"] == "DEPLOYING"
    assert svc["endpoint"] == "http://rkp-svc-sentiment-serve-svc.team-a.svc:8000/predict"

    rs = platform.kube.rayservices[("team-a", "rkp-svc-sentiment")]
    app = json.loads(rs["spec"]["serveConfigV2"])["applications"][0]
    assert app["import_path"] == "app:build"
    assert app["args"] == {"run_dir": f"/shared/checkpoints/{job['id']}", "profile": "tiny"}
    dep = app["deployments"][0]
    assert dep["ray_actor_options"] == {"num_cpus": 0.5}
    scaling = dep["autoscaling_config"]
    assert (scaling["min_replicas"], scaling["max_replicas"]) == (1, 3)
    assert scaling["target_ongoing_requests"] == 2

    cm = platform.kube.configmaps[("team-a", "rkp-svc-sentiment")]
    assert set(cm["data"]) == {"app.py", "profiles.py"}
    owner = cm["metadata"]["ownerReferences"][0]
    assert (owner["kind"], owner["uid"]) == ("RayService", rs["metadata"]["uid"])


def test_service_cluster_mounts_checkpoints_read_only_and_has_no_submitter() -> None:
    spec = ServiceSpec(name="s", job_id="abc")
    rs = build_rayservice("team-a", spec, image="i", ray_version="2.59.0")
    cluster = rs["spec"]["rayClusterConfig"]
    head = cluster["headGroupSpec"]["template"]["spec"]["containers"][0]
    worker_group = cluster["workerGroupSpecs"][0]
    assert cluster["headGroupSpec"]["rayStartParams"] == {"num-cpus": "0", "no-monitor": "true"}
    assert worker_group["rayStartParams"] == {"num-cpus": "2"}
    mounts = {m["name"]: m for m in head["volumeMounts"]}
    assert mounts["checkpoints"]["readOnly"] is True
    assert mounts["code"]["mountPath"] == "/home/ray/rkp-code"
    assert "submitterPodTemplate" not in rs["spec"]

    total = Resources()
    for template, n in ((cluster["headGroupSpec"]["template"], 1), (worker_group["template"], 1)):
        req = template["spec"]["containers"][0]["resources"]["requests"]
        one = Resources(int(parse_quantity(req["cpu"]) * 1000), memory_mib(req["memory"]))
        for _ in range(n):
            total += one
    assert service_requests(spec) == total


def test_service_needs_a_succeeded_job_of_the_same_team(platform: Platform) -> None:
    running = platform.http.post("/jobs", json=HELLO, headers=platform.as_team(KEY_A)).json()
    assert serve(platform, running["id"]).status_code == 409
    other = trained_job(platform, KEY_B)
    assert serve(platform, other["id"]).status_code == 404  # team-b's job is invisible to team-a
    assert serve(platform, "nope").status_code == 404
    assert not platform.kube.rayservices


def test_duplicate_name_is_409_and_teams_have_separate_names(platform: Platform) -> None:
    job_a, job_b = trained_job(platform), trained_job(platform, KEY_B)
    assert serve(platform, job_a["id"]).status_code == 201
    assert serve(platform, job_a["id"]).status_code == 409
    assert serve(platform, job_b["id"], KEY_B).status_code == 201


@pytest.mark.parametrize(
    "bad",
    [
        {"name": "Bad_Name"},
        {"max_replicas": 5},
        {"min_replicas": 3, "max_replicas": 2},
        {"cpu": 1, "max_replicas": 3},
    ],
)
def test_invalid_service_spec_is_422(platform: Platform, bad: dict) -> None:
    assert serve(platform, "x", **bad).status_code == 422


def test_service_counts_against_quota_and_delete_frees_it(platform: Platform) -> None:
    platform.kube.quotas["team-a"] = Resources(2000, 4096)
    job = trained_job(platform)
    assert serve(platform, job["id"]).status_code == 201  # 1.25 CPU / 2560Mi
    queued = platform.http.post("/jobs", json=HELLO, headers=platform.as_team(KEY_A)).json()
    assert queued["state"] == "QUEUED"  # 2560 + 2504 > 4096
    r = serve(platform, job["id"], name="second")
    assert r.status_code == 409 and "queued" in r.json()["detail"]

    d = platform.http.delete("/services/sentiment", headers=platform.as_team(KEY_A))
    assert d.json()["state"] == "DELETED"
    assert ("team-a", "rkp-svc-sentiment") not in platform.kube.rayservices
    assert platform.http.get("/services", headers=platform.as_team(KEY_A)).json() == []
    reconcile_once(platform.registry, platform.kube, platform.http.app.state.admitter)  # type: ignore[attr-defined]
    job_now = platform.http.get(f"/jobs/{queued['id']}", headers=platform.as_team(KEY_A)).json()
    assert job_now["state"] == "SUBMITTED"


def test_service_is_busy_while_a_finished_jobs_pods_terminate(platform: Platform) -> None:
    platform.kube.quotas["team-a"] = Resources(2000, 4096)
    job = trained_job(platform)  # finished: the registry no longer counts it
    platform.kube.pods["team-a"] = {f"job/{job['id']}": Resources(1250, 3072)}
    r = serve(platform, job["id"])
    assert r.status_code == 409
    assert r.json()["detail"] == (
        "service needs 1.25 CPU / 2560Mi, team-a uses 1.25 CPU / 3072Mi of 2 CPU / 4096Mi, "
        "1.25 CPU / 3072Mi of it from finished work whose pods are still terminating: "
        "retry in a few seconds"
    )
    platform.kube.pods["team-a"] = {}
    assert serve(platform, job["id"]).status_code == 201


def test_service_bigger_than_quota_is_422(platform: Platform) -> None:
    platform.kube.quotas["team-a"] = Resources(2000, 4096)
    r = serve(platform, trained_job(platform)["id"], workers=2, memory="4Gi")
    assert r.status_code == 422 and "whole quota" in r.json()["detail"]


def test_other_team_cannot_see_or_delete_a_service(platform: Platform) -> None:
    serve(platform, trained_job(platform)["id"])
    b = platform.as_team(KEY_B)
    assert platform.http.get("/services/sentiment", headers=b).status_code == 404
    assert platform.http.delete("/services/sentiment", headers=b).status_code == 404
    assert ("team-a", "rkp-svc-sentiment") in platform.kube.rayservices


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        ({}, ServiceState.DEPLOYING),
        (
            {
                "pendingServiceStatus": {
                    "applicationStatuses": {"sentiment": {"status": "DEPLOYING"}}
                }
            },
            ServiceState.DEPLOYING,
        ),
        ({"conditions": [{"type": "Ready", "status": "True"}]}, ServiceState.RUNNING),
        ({"serviceStatus": "Running"}, ServiceState.RUNNING),
        (
            {
                "conditions": [{"type": "Ready", "status": "True"}],
                "activeServiceStatus": {
                    "applicationStatuses": {
                        "sentiment": {"status": "DEPLOY_FAILED", "message": "no checkpoint\nTrace"}
                    }
                },
            },
            ServiceState.UNHEALTHY,
        ),
    ],
)
def test_observe_service(status: dict, expected: ServiceState) -> None:
    state, message = observe_service({"status": status})
    assert state == expected
    if expected == ServiceState.UNHEALTHY:
        assert message == "no checkpoint"
