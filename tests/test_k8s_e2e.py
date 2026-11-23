"""End-to-end against the kind cluster: `make up && make teams && make test-k8s` (~2 min).

The API runs in-process with the real Kubernetes client; reconcile_once is driven by the test.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from platform_api.config import ROOT, Settings, read_env_file
from platform_api.k8s import RealKube
from platform_api.main import create_app
from platform_api.reconciler import reconcile_once

pytestmark = pytest.mark.k8s
KEY = "e2e-team-a-key-0123456789"
HELLO = (ROOT / "workloads" / "hello.py").read_text()


@pytest.fixture
def api(tmp_path: Path) -> Iterator[tuple[TestClient, RealKube]]:
    versions = read_env_file(ROOT / "infra" / "versions.env")
    kube = RealKube("kind-rkp")
    settings = Settings(
        api_keys={KEY: "team-a"},
        db_path=tmp_path / "registry.db",
        image=versions["RKP_IMAGE"],
        ray_version=versions["RAY_VERSION"],
    )
    app = create_app(settings, kube, run_reconciler=False)
    with TestClient(app, headers={"X-API-Key": KEY}) as http:
        yield http, kube


def wait_for(http: TestClient, job_id: str, states: set[str], timeout: float = 300) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        reconcile_once(http.app.state.registry, http.app.state.kube)  # type: ignore[attr-defined]
        job = http.get(f"/jobs/{job_id}").json()
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
