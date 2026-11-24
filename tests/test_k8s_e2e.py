"""End-to-end against the kind cluster: `make up && make teams && make test-k8s`.

The training tests need `make image` and `make data` too, and take most of the ~10 min.

The API runs in-process with the real Kubernetes client; reconcile_once is driven by the test.
"""

from __future__ import annotations

import shutil
import subprocess
import time
from collections.abc import Iterator
from pathlib import Path

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


def pods_gone(kube: RealKube, job_id: str, timeout: float = 90, team: str = "team-a") -> bool:
    """No pod at all (head, worker or submitter) is left for the job."""
    deadline = time.monotonic() + timeout
    while kube._core.list_namespaced_pod(team, label_selector=f"rkp.io/job-id={job_id}").items:
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

        # Freeing team-a's quota admits its queued job on the next reconcile.
        http.delete(f"/jobs/{hog['id']}")
        admitted = wait_for(http, extra["id"], {"SUBMITTED", "PENDING", "RUNNING"}, timeout=30)
        assert admitted["message"] == "admitted from queue"
    finally:
        for job in (hog, extra):
            if http.get(f"/jobs/{job['id']}").json()["state"] not in DONE:
                http.delete(f"/jobs/{job['id']}")
        assert pods_gone(kube, hog["id"]) and pods_gone(kube, extra["id"])
