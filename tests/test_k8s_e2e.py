"""End-to-end against the kind cluster: `make up && make teams && make test-k8s`.

The training tests need `make image` and `make data` too, and take most of the ~10 min.

The API runs in-process with the real Kubernetes client; reconcile_once is driven by the test.
"""

from __future__ import annotations

import re
import shutil
import subprocess
import time
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from platform_api.config import ROOT, Settings, read_env_file
from platform_api.k8s import RealKube
from platform_api.main import create_app
from platform_api.reconciler import reconcile_once
from sdk.client import load_code

pytestmark = pytest.mark.k8s
KEY = "e2e-team-a-key-0123456789"
KEY_B = "e2e-team-b-key-0123456789"
HELLO = (ROOT / "workloads" / "hello.py").read_text()
TRAIN = {
    "entrypoint": "python train.py --profile tiny --epochs 2",
    "files": load_code(ROOT / "workloads" / "train"),
}
CHECKPOINTS = ROOT / ".shared" / "checkpoints" / "team-a"
DONE = {"SUCCEEDED", "FAILED", "CANCELLED"}


@pytest.fixture
def api(tmp_path: Path) -> Iterator[tuple[TestClient, RealKube]]:
    versions = read_env_file(ROOT / "infra" / "versions.env")
    kube = RealKube("kind-rkp")
    settings = Settings(
        api_keys={KEY: "team-a", KEY_B: "team-b"},
        db_path=tmp_path / "registry.db",
        image=versions["RKP_IMAGE"],
        ray_version=versions["RAY_VERSION"],
    )
    app = create_app(settings, kube, run_reconciler=False)
    with TestClient(app, headers={"X-API-Key": KEY}) as http:
        yield http, kube


def wait_for(
    http: TestClient,
    job_id: str,
    states: set[str],
    timeout: float = 300,
    headers: dict[str, str] | None = None,
) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        state = http.app.state  # type: ignore[attr-defined]
        reconcile_once(state.registry, state.kube, state.admitter)
        job = http.get(f"/jobs/{job_id}", headers=headers).json()
        if job["state"] in states:
            return job
        time.sleep(3)
    raise AssertionError(f"{job_id} never reached {states}; last state {job['state']}")


def gone(kube: RealKube, name: str, timeout: float = 60) -> bool:
    deadline = time.monotonic() + timeout
    while kube.get_rayjob("team-a", name) is not None:
        if time.monotonic() > deadline:
            return False
        time.sleep(2)
    return True


def pods_gone(
    kube: RealKube, job_id: str, timeout: float = 90, team: str = "team-a", label: str = "job-id"
) -> bool:
    """No pod at all (head, worker or submitter) is left for the job (or service)."""
    deadline = time.monotonic() + timeout
    while kube._core.list_namespaced_pod(team, label_selector=f"rkp.io/{label}={job_id}").items:
        if time.monotonic() > deadline:
            return False
        time.sleep(3)
    return True


def test_hello_succeeds_with_driver_logs_and_cleanup(api: tuple[TestClient, RealKube]) -> None:
    http, kube = api
    r = http.post("/jobs", json={"entrypoint": "python hello.py", "files": {"hello.py": HELLO}})
    assert r.status_code == 201, r.text
    job = wait_for(http, r.json()["id"], {"SUCCEEDED", "FAILED", "CANCELLED"})
    assert job["state"] == "SUCCEEDED", job["message"]
    assert job["started_at"] and job["finished_at"]
    assert "hello from rkp" in http.get(f"/jobs/{job['id']}/logs").text
    assert gone(kube, job["k8s_name"])


def test_cancel_running_job(api: tuple[TestClient, RealKube]) -> None:
    http, kube = api
    sleeper = "import time\nfor i in range(600):\n    print(i, flush=True)\n    time.sleep(1)\n"
    r = http.post(
        "/jobs",
        json={"entrypoint": "python sleep.py", "files": {"sleep.py": sleeper}, "workers": 1},
    )
    job = wait_for(http, r.json()["id"], {"RUNNING"})
    assert http.delete(f"/jobs/{job['id']}").json()["state"] == "CANCELLED"
    assert gone(kube, job["k8s_name"])


def test_training_survives_worker_kill_and_resumes(api: tuple[TestClient, RealKube]) -> None:
    """Phase 4: a worker dies during epoch 2; Ray Train restarts the group from epoch 1."""
    http, kube = api
    r = http.post("/jobs", json=TRAIN)
    assert r.status_code == 201, r.text
    job_id = r.json()["id"]
    chaos = subprocess.Popen(
        ["bash", str(ROOT / "scripts" / "chaos_kill_worker.sh"), "team-a", job_id, "600"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        job = wait_for(http, job_id, DONE, timeout=900)
        out = chaos.communicate(timeout=30)[0]
        assert chaos.returncode == 0, out
        assert "killed worker pod" in out
        assert job["state"] == "SUCCEEDED", job["message"]
        logs = http.get(f"/jobs/{job_id}/logs").text
        assert "resumed from checkpoint at epoch 1" in logs
        assert "'epoch': 2" in logs
        # checkpoint_<timestamp>/ dirs (not Ray's checkpoint_manager_snapshot.json)
        kept = [p for p in (CHECKPOINTS / job_id).glob("checkpoint_*") if p.is_dir()]
        assert 1 <= len(kept) <= 2  # CheckpointConfig(num_to_keep=2)
        assert pods_gone(kube, job_id)
    finally:
        chaos.kill()
        shutil.rmtree(CHECKPOINTS / job_id, ignore_errors=True)


def test_always_failing_job_is_retried_then_failed(api: tuple[TestClient, RealKube]) -> None:
    http, kube = api
    r = http.post(
        "/jobs",
        json={
            "entrypoint": "python fail.py",
            "files": {"fail.py": "raise SystemExit('boom')\n"},
            "workers": 1,
            "memory": "512Mi",
            "retries": 2,
        },
    )
    job = wait_for(http, r.json()["id"], DONE, timeout=600)
    assert job["state"] == "FAILED"
    assert job["message"].startswith("failed after 3 attempts"), job["message"]
    assert pods_gone(kube, job["id"])


def test_cancel_during_training_leaves_no_pods(api: tuple[TestClient, RealKube]) -> None:
    http, kube = api
    r = http.post("/jobs", json=TRAIN)
    job_id = r.json()["id"]
    try:
        wait_for(http, job_id, {"RUNNING"})
        deadline = time.monotonic() + 180  # cancel once the workers are actually training
        while "[rank 0]" not in (kube.job_logs("team-a", f"rkp-{job_id}") or ""):
            assert time.monotonic() < deadline, "training never started"
            time.sleep(3)
        assert http.delete(f"/jobs/{job_id}").json()["state"] == "CANCELLED"
        assert pods_gone(kube, job_id)
    finally:
        shutil.rmtree(CHECKPOINTS / job_id, ignore_errors=True)


SA = "system:serviceaccount:rkp-system:rkp-api"


@pytest.mark.parametrize(
    ("verb", "resource", "namespace", "allowed"),
    [
        ("create", "rayjobs.ray.io", "team-a", True),
        ("delete", "rayjobs.ray.io", "team-b", True),
        ("get", "pods/log", "team-a", True),
        ("get", "resourcequotas", "team-b", True),
        ("create", "rayjobs.ray.io", "default", False),  # only team namespaces
        ("create", "rayjobs.ray.io", "kube-system", False),
        ("delete", "pods", "team-a", False),  # only what the API needs
        ("get", "secrets", "team-a", False),
        ("update", "resourcequotas", "team-a", False),  # can read its quota, not raise it
    ],
)
def test_api_service_account_rbac(verb: str, resource: str, namespace: str, allowed: bool) -> None:
    r = subprocess.run(
        [
            "kubectl",
            "--context",
            "kind-rkp",
            "auth",
            "can-i",
            verb,
            resource,
            "-n",
            namespace,
            "--as",
            SA,
        ],
        capture_output=True,
        text=True,
    )
    assert r.stdout.strip() == ("yes" if allowed else "no"), (r.stdout, r.stderr)


def test_noisy_neighbour_queues_team_a_but_not_team_b(api: tuple[TestClient, RealKube]) -> None:
    """team-a fills its quota with a long job; its next job waits while team-b's runs."""
    http, kube = api
    b = {"X-API-Key": KEY_B}
    sleeper = {
        "entrypoint": "python sleep.py 300",
        "files": {"sleep.py": (ROOT / "workloads" / "sleep.py").read_text()},
        "workers": 1,
    }
    hog = http.post("/jobs", json=sleeper).json()
    extra = http.post("/jobs", json=sleeper).json()
    assert hog["state"] == "SUBMITTED"
    assert extra["state"] == "QUEUED", extra
    assert "waiting for quota" in extra["message"]
    assert kube.get_rayjob("team-a", extra["k8s_name"]) is None
    try:
        hello = {"entrypoint": "python hello.py", "files": {"hello.py": HELLO}, "workers": 1}
        r = http.post("/jobs", json=hello, headers=b)
        assert r.json()["state"] == "SUBMITTED", r.text
        job_b = wait_for(http, r.json()["id"], DONE, headers=b)
        assert job_b["state"] == "SUCCEEDED", job_b["message"]
        assert http.get(f"/jobs/{extra['id']}").json()["state"] == "QUEUED"
        assert http.get(f"/jobs/{hog['id']}").json()["state"] == "RUNNING"

        # Freeing team-a's quota admits its queued job once hog's pods have terminated (the
        # ResourceQuota counts them until then), so none of its pods is refused by the quota.
        http.delete(f"/jobs/{hog['id']}")
        admitted = wait_for(http, extra["id"], {"SUBMITTED", "PENDING", "RUNNING"}, timeout=120)
        assert admitted["message"] == "admitted from queue"
        wait_for(http, extra["id"], {"RUNNING"}, timeout=180)
        events = subprocess.run(
            ["kubectl", "--context", "kind-rkp", "-n", "team-a", "get", "events", "-o", "json"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        refused = [
            line for line in events.splitlines() if "exceeded quota" in line and extra["id"] in line
        ]
        assert not refused, refused
    finally:
        for job in (hog, extra):
            if http.get(f"/jobs/{job['id']}").json()["state"] not in DONE:
                http.delete(f"/jobs/{job['id']}")
        assert pods_gone(kube, hog["id"]) and pods_gone(kube, extra["id"])


def test_serve_latest_checkpoint_then_delete(api: tuple[TestClient, RealKube]) -> None:
    """Phase 5: train 1 epoch, serve its checkpoint as a RayService, predict, then delete."""
    http, kube = api
    r = http.post(
        "/jobs", json={**TRAIN, "entrypoint": "python train.py --profile tiny --epochs 1"}
    )
    job_id = r.json()["id"]
    pf: subprocess.Popen[str] | None = None
    try:
        job = wait_for(http, job_id, DONE, timeout=600)
        assert job["state"] == "SUCCEEDED", job["message"]
        # The training job's pods take a few seconds to terminate and still hold quota: the
        # API says 409 "retry in a few seconds" until they're gone, so retry like a client.
        deadline = time.monotonic() + 120
        while (
            r := http.post("/services", json={"name": "e2e", "job_id": job_id})
        ).status_code == 409:
            assert "retry in a few seconds" in r.text, r.text
            assert time.monotonic() < deadline, r.text
            time.sleep(3)
        assert r.status_code == 201, r.text
        deadline = time.monotonic() + 300
        while (svc := http.get("/services/e2e").json())["state"] != "RUNNING":
            assert svc["state"] == "DEPLOYING", svc
            assert time.monotonic() < deadline, "service never became RUNNING"
            time.sleep(5)

        pf = subprocess.Popen(
            [
                "kubectl",
                "--context",
                "kind-rkp",
                "-n",
                "team-a",
                "port-forward",
                "svc/rkp-svc-e2e-serve-svc",
                ":8000",
            ],
            stdout=subprocess.PIPE,
            text=True,
        )
        assert pf.stdout is not None
        m = re.search(r"127\.0\.0\.1:(\d+)", pf.stdout.readline())
        assert m, "port-forward did not start"
        out = httpx.post(
            f"http://127.0.0.1:{m[1]}/predict",
            json={"texts": ["Great product, works perfectly.", "Broke after two days."]},
            timeout=30,
        ).json()
        assert [p["label"] in {"positive", "negative"} for p in out["predictions"]] == [True] * 2
        assert out["checkpoint"].startswith("checkpoint_2")

        assert http.delete("/services/e2e").json()["state"] == "DELETED"
        assert http.get("/services").json() == []
        assert pods_gone(kube, "e2e", label="service")
        assert kube.get_rayservice("team-a", "rkp-svc-e2e") is None
    finally:
        if pf:
            pf.kill()
        if http.get("/services").json():
            http.delete("/services/e2e")
        shutil.rmtree(CHECKPOINTS / job_id, ignore_errors=True)


def test_gpu_job_waits_for_the_sim_gpu_while_cpu_jobs_run(
    api: tuple[TestClient, RealKube],
) -> None:
    """Phase 8: the node has one sim GPU (`make teams` -> `make gpu-sim`). team-a holds it;
    team-b's GPU job is admitted but stays PENDING (scheduler: Insufficient example.com/gpu)
    while team-b's CPU job runs and succeeds; then the GPU job gets the GPU and succeeds."""
    http, kube = api
    b = {"X-API-Key": KEY_B}
    gpu_code = {"gpu_sim.py": (ROOT / "workloads" / "gpu_sim.py").read_text()}
    gpu = {"files": gpu_code, "workers": 0, "gpus": 1, "memory": "640Mi"}
    hold = http.post("/jobs", json={**gpu, "entrypoint": "python gpu_sim.py 90"}).json()
    jobs = [(hold, None)]
    try:
        assert wait_for(http, hold["id"], {"RUNNING"})["state"] == "RUNNING"
        b_gpu = http.post(
            "/jobs", json={**gpu, "entrypoint": "python gpu_sim.py 12"}, headers=b
        ).json()
        b_cpu = http.post(
            "/jobs",
            json={
                "entrypoint": "python hello.py",
                "files": {"hello.py": HELLO},
                "workers": 1,
                "memory": "512Mi",
            },
            headers=b,
        ).json()
        jobs += [(b_gpu, b), (b_cpu, b)]
        assert b_gpu["state"] == b_cpu["state"] == "SUBMITTED"  # both fit team-b's quota

        done_cpu = wait_for(http, b_cpu["id"], DONE, headers=b)
        assert done_cpu["state"] == "SUCCEEDED", done_cpu["message"]
        waiting = http.get(f"/jobs/{b_gpu['id']}", headers=b).json()
        if http.get(f"/jobs/{hold['id']}").json()["state"] == "RUNNING":
            # Still blocked on the node's only GPU, and the reason is surfaced.
            assert waiting["state"] == "PENDING", waiting
            assert "Insufficient example.com/gpu" in (waiting["message"] or ""), waiting

        done_gpu = wait_for(http, b_gpu["id"], DONE, timeout=420, headers=b)
        assert done_gpu["state"] == "SUCCEEDED", done_gpu["message"]
        assert done_gpu["message"] != waiting["message"]  # the stale reason was cleared
        logs = http.get(f"/jobs/{b_gpu['id']}/logs", headers=b).text
        assert "'sim_gpu': 1.0" in logs and "gpu-sim-worker" in logs
    finally:
        for job, headers in jobs:
            if http.get(f"/jobs/{job['id']}", headers=headers).json()["state"] not in DONE:
                http.delete(f"/jobs/{job['id']}", headers=headers)
        for job, headers in jobs:
            team = "team-b" if headers else "team-a"
            assert pods_gone(kube, job["id"], team=team)
