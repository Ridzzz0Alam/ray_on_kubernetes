"""API behaviour against a fake Kubernetes: auth, team isolation, submit, logs, cancel."""

from __future__ import annotations

from platform_api.reconciler import reconcile_once
from tests.conftest import KEY_A, KEY_B, Platform

HELLO = {"entrypoint": "python hello.py", "files": {"hello.py": "print('hi')"}}


def submit(p: Platform, key: str = KEY_A, **spec: object) -> dict:
    r = p.http.post("/jobs", json={**HELLO, **spec}, headers=p.as_team(key))
    assert r.status_code == 201, r.text
    return r.json()


def test_healthz_needs_no_key(platform: Platform) -> None:
    assert platform.http.get("/healthz").json() == {"status": "ok"}


def test_missing_or_unknown_key_is_401(platform: Platform) -> None:
    assert platform.http.get("/jobs").status_code == 401
    assert platform.http.get("/jobs", headers={"X-API-Key": "nope"}).status_code == 401


def test_submit_creates_rayjob_and_code_in_the_keys_namespace(platform: Platform) -> None:
    job = submit(platform, workers=1)
    assert job["state"] == "SUBMITTED"
    assert job["team"] == "team-a"
    assert "files" not in job["spec"]
    key = ("team-a", job["k8s_name"])
    rayjob = platform.kube.rayjobs[key]
    cm = platform.kube.configmaps[key]
    assert rayjob["spec"]["rayClusterSpec"]["workerGroupSpecs"][0]["replicas"] == 1
    assert cm["data"] == {"hello.py": "print('hi')"}
    assert cm["metadata"]["ownerReferences"][0]["uid"] == rayjob["metadata"]["uid"]


def test_namespace_in_body_is_rejected(platform: Platform) -> None:
    r = platform.http.post(
        "/jobs", json={**HELLO, "namespace": "team-b"}, headers=platform.as_team(KEY_A)
    )
    assert r.status_code == 422
    assert not platform.kube.rayjobs


def test_invalid_spec_is_422(platform: Platform) -> None:
    for bad in ({"workers": 9}, {"memory": "1TB"}, {"files": {"../x.py": ""}}):
        r = platform.http.post("/jobs", json={**HELLO, **bad}, headers=platform.as_team(KEY_A))
        assert r.status_code == 422, bad


def test_other_team_cannot_read_logs_or_cancel(platform: Platform) -> None:
    job = submit(platform, KEY_A)
    b = platform.as_team(KEY_B)
    assert platform.http.get(f"/jobs/{job['id']}", headers=b).status_code == 403
    assert platform.http.get(f"/jobs/{job['id']}/logs", headers=b).status_code == 403
    assert platform.http.delete(f"/jobs/{job['id']}", headers=b).status_code == 403
    assert platform.kube.rayjobs, "team-b's cancel must not delete team-a's RayJob"
    assert platform.http.get("/jobs", headers=b).json() == []
    assert len(platform.http.get("/jobs", headers=platform.as_team(KEY_A)).json()) == 1


def test_unknown_job_is_404(platform: Platform) -> None:
    assert platform.http.get("/jobs/deadbeef", headers=platform.as_team(KEY_A)).status_code == 404


def test_lifecycle_to_succeeded_keeps_logs_and_deletes_rayjob(platform: Platform) -> None:
    job = submit(platform)
    ns_name = ("team-a", job["k8s_name"])
    a = platform.as_team(KEY_A)

    platform.kube.set_status(*ns_name, "Initializing")
    reconcile_once(platform.registry, platform.kube)
    assert platform.http.get(f"/jobs/{job['id']}", headers=a).json()["state"] == "PENDING"

    platform.kube.set_status(*ns_name, "Running", "RUNNING")
    platform.kube.logs[ns_name] = "line 1\nline 2\nhello from rkp"
    reconcile_once(platform.registry, platform.kube)
    running = platform.http.get(f"/jobs/{job['id']}", headers=a).json()
    assert running["state"] == "RUNNING"
    assert running["started_at"] is not None
    assert platform.http.get(f"/jobs/{job['id']}/logs?tail=1", headers=a).text == "hello from rkp"

    platform.kube.set_status(*ns_name, "Complete", "SUCCEEDED")
    reconcile_once(platform.registry, platform.kube)
    done = platform.http.get(f"/jobs/{job['id']}", headers=a).json()
    assert done["state"] == "SUCCEEDED"
    assert done["finished_at"] is not None
    assert ns_name not in platform.kube.rayjobs, "finished RayJobs are cleaned up"
    # logs are served from the registry once the pods are gone
    assert "hello from rkp" in platform.http.get(f"/jobs/{job['id']}/logs", headers=a).text


def test_cancel_deletes_rayjob_and_is_final(platform: Platform) -> None:
    job = submit(platform)
    ns_name = ("team-a", job["k8s_name"])
    a = platform.as_team(KEY_A)
    platform.kube.set_status(*ns_name, "Running", "RUNNING")
    platform.kube.logs[ns_name] = "partial output"

    r = platform.http.delete(f"/jobs/{job['id']}", headers=a)
    assert r.status_code == 200
    assert r.json()["state"] == "CANCELLED"
    assert ns_name not in platform.kube.rayjobs
    assert ns_name not in platform.kube.configmaps
    assert platform.http.get(f"/jobs/{job['id']}/logs", headers=a).text == "partial output"
    assert platform.http.delete(f"/jobs/{job['id']}", headers=a).status_code == 409

    reconcile_once(platform.registry, platform.kube)  # must not resurrect or re-mark it
    assert platform.http.get(f"/jobs/{job['id']}", headers=a).json()["state"] == "CANCELLED"


def test_rayjob_deleted_outside_rkp_marks_failed(platform: Platform) -> None:
    job = submit(platform)
    platform.kube.rayjobs.clear()
    reconcile_once(platform.registry, platform.kube)
    got = platform.http.get(f"/jobs/{job['id']}", headers=platform.as_team(KEY_A)).json()
    assert got["state"] == "FAILED"
    assert "outside rkp" in got["message"]


def test_failed_configmap_rolls_back_rayjob(platform: Platform) -> None:
    from platform_api.k8s import KubeError

    def boom(namespace: str, body: dict) -> None:
        raise KubeError(403, "forbidden")

    platform.kube.create_configmap = boom  # type: ignore[method-assign]
    r = platform.http.post("/jobs", json=HELLO, headers=platform.as_team(KEY_A))
    assert r.status_code == 502
    assert not platform.kube.rayjobs
    assert platform.http.get("/jobs", headers=platform.as_team(KEY_A)).json() == []


def test_job_shorter_than_a_poll_still_gets_started_at(platform: Platform) -> None:
    job = submit(platform)
    info = {"startTime": "2026-11-23T09:00:05Z", "endTime": "2026-11-23T09:00:09Z"}
    platform.kube.set_status("team-a", job["k8s_name"], "Complete", "SUCCEEDED", rayJobInfo=info)
    reconcile_once(platform.registry, platform.kube)
    got = platform.http.get(f"/jobs/{job['id']}", headers=platform.as_team(KEY_A)).json()
    assert got["state"] == "SUCCEEDED"
    assert got["started_at"].startswith("2026-11-23T09:00:05")
    assert got["finished_at"].startswith("2026-11-23T09:00:09")
