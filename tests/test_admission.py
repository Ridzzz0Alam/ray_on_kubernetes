"""Whole-job admission against team quotas (fake K8s): reject, queue FIFO, admit on free."""

from __future__ import annotations

import yaml
from kubernetes.utils import parse_quantity

from platform_api.config import ROOT
from platform_api.k8s import UNBOUNDED, quota_from_hard
from platform_api.manifests import build_rayjob, job_requests
from platform_api.models import JobSpec, Resources, memory_mib
from platform_api.reconciler import reconcile_once
from tests.conftest import KEY_A, KEY_B, Platform

SMALL = {"entrypoint": "python s.py", "files": {"s.py": "pass"}, "workers": 1}  # 0.8 CPU/2504Mi
QUOTA = Resources(cpu_m=2000, mem_mib=4096)


def post(p: Platform, key: str = KEY_A, **spec: object) -> dict:
    r = p.http.post("/jobs", json={**SMALL, **spec}, headers=p.as_team(key))
    assert r.status_code == 201, r.text
    return r.json()


def finish(p: Platform, job: dict) -> None:
    p.kube.set_status("team-a", job["k8s_name"], "Complete", "SUCCEEDED")
    reconcile_once(p.registry, p.kube, p.http.app.state.admitter)  # type: ignore[attr-defined]


def state(p: Platform, job: dict, key: str = KEY_A) -> str:
    return p.http.get(f"/jobs/{job['id']}", headers=p.as_team(key)).json()["state"]


def test_job_requests_match_the_manifest() -> None:
    spec = JobSpec(entrypoint="python x.py", workers=3, cpu=2, memory="2Gi")
    rj = build_rayjob("abc", "team-a", spec, image="i", ray_version="2.59.0")
    pods = [
        (rj["spec"]["rayClusterSpec"]["headGroupSpec"]["template"], 1),
        (rj["spec"]["rayClusterSpec"]["workerGroupSpecs"][0]["template"], spec.workers),
        (rj["spec"]["submitterPodTemplate"], 1),
    ]
    total = Resources()
    for template, count in pods:
        req = template["spec"]["containers"][0]["resources"]["requests"]
        one = Resources(int(parse_quantity(req["cpu"]) * 1000), memory_mib(req["memory"]))
        for _ in range(count):
            total += one
    assert job_requests(spec) == total


def test_no_quota_means_no_queueing(platform: Platform) -> None:
    jobs = [post(platform) for _ in range(3)]
    assert [j["state"] for j in jobs] == ["SUBMITTED"] * 3


def test_job_bigger_than_the_whole_quota_is_rejected(platform: Platform) -> None:
    platform.kube.quotas["team-a"] = QUOTA
    r = platform.http.post(
        "/jobs", json={**SMALL, "workers": 4, "memory": "2Gi"}, headers=platform.as_team(KEY_A)
    )
    assert r.status_code == 422
    assert "whole quota is 2 CPU / 4096Mi" in r.json()["detail"]
    assert not platform.kube.rayjobs


def test_over_quota_job_queues_then_runs_when_quota_frees(platform: Platform) -> None:
    platform.kube.quotas["team-a"] = QUOTA
    first = post(platform)
    second = post(platform)
    assert first["state"] == "SUBMITTED"
    assert second["state"] == "QUEUED"
    assert second["message"] == (
        "waiting for quota: needs 0.8 CPU / 2504Mi, team-a uses 0.8 CPU / 2504Mi of 2 CPU / 4096Mi"
    )
    assert ("team-a", second["k8s_name"]) not in platform.kube.rayjobs

    reconcile_once(platform.registry, platform.kube, platform.http.app.state.admitter)  # type: ignore[attr-defined]
    assert state(platform, second) == "QUEUED"  # nothing freed yet

    finish(platform, first)
    assert state(platform, first) == "SUCCEEDED"
    assert state(platform, second) == "SUBMITTED"
    assert ("team-a", second["k8s_name"]) in platform.kube.rayjobs
    assert ("team-a", second["k8s_name"]) in platform.kube.configmaps  # code shipped too


def test_one_team_queueing_does_not_affect_the_other(platform: Platform) -> None:
    platform.kube.quotas.update({"team-a": QUOTA, "team-b": QUOTA})
    a = [post(platform) for _ in range(3)]
    b = post(platform, KEY_B)
    assert [j["state"] for j in a] == ["SUBMITTED", "QUEUED", "QUEUED"]
    assert b["state"] == "SUBMITTED"


def test_queue_is_fifo_a_small_job_does_not_jump_a_big_one(platform: Platform) -> None:
    platform.kube.quotas["team-a"] = QUOTA
    running = post(platform)
    big = post(platform, workers=2)  # 1.3 CPU / 3272Mi: needs the whole quota
    small = post(platform, memory="256Mi")  # would fit next to `running`, but must wait
    assert (big["state"], small["state"]) == ("QUEUED", "QUEUED")
    assert small["message"].endswith("(1 queued ahead)")

    finish(platform, running)
    assert state(platform, big) == "SUBMITTED"
    assert state(platform, small) == "QUEUED"


def test_cancel_a_queued_job_touches_no_kubernetes_objects(platform: Platform) -> None:
    platform.kube.quotas["team-a"] = QUOTA
    post(platform)
    queued = post(platform)
    r = platform.http.delete(f"/jobs/{queued['id']}", headers=platform.as_team(KEY_A))
    assert r.json()["state"] == "CANCELLED"
    assert platform.kube.deleted == []


def test_quota_parsing_takes_the_tightest_and_ignores_unset_keys() -> None:
    assert quota_from_hard([]) is None
    q = quota_from_hard(
        [{"requests.cpu": "2", "requests.memory": "4Gi"}, {"requests.cpu": "1500m"}]
    )
    assert q == Resources(1500, 4096)
    assert quota_from_hard([{"count/pods": "5"}]) == Resources(UNBOUNDED, UNBOUNDED)


def test_team_quotas_fit_one_training_job_but_not_two_small_ones() -> None:
    """Guards the sizing reasoning written in infra/k8s/teams/quotas.yaml."""
    docs = list(yaml.safe_load_all((ROOT / "infra" / "k8s" / "teams" / "quotas.yaml").read_text()))
    hards = {
        d["metadata"]["namespace"]: d["spec"]["hard"] for d in docs if d["kind"] == "ResourceQuota"
    }
    assert set(hards) == {"team-a", "team-b"}
    training = job_requests(JobSpec(entrypoint="python train.py"))  # 2 workers, defaults
    small = job_requests(JobSpec(**SMALL))
    total = Resources()
    for hard in hards.values():
        quota = quota_from_hard([hard])
        assert quota is not None
        assert training.fits_in(quota)
        assert not (small + small).fits_in(quota)
        total += quota
    assert total.fits_in(Resources(cpu_m=4000, mem_mib=15 * 1024))  # sum of quotas <= the node
