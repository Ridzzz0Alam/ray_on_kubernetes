"""Cost report and Prometheus metrics, both computed from registry rows with fixed timestamps."""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from typer.testing import CliRunner

from platform_api.config import ROOT
from platform_api.cost import Rates, parse_since, team_cost
from platform_api.metrics import RegistryCollector
from platform_api.models import JobSpec, JobState, ServiceSpec
from platform_api.registry import Registry
from sdk import cli
from sdk.client import Client
from tests.conftest import KEY_A, KEY_B, Platform

T0 = datetime(2026, 11, 24, 10, 0, tzinfo=UTC)
RATES = Rates(cpu_hour=0.02, gb_hour=0.003)
SMALL = JobSpec(entrypoint="python x.py", workers=1)  # requests 0.8 CPU / 2504Mi


def h(n: float) -> datetime:
    return T0 + timedelta(hours=n)


def add_job(
    reg: Registry,
    job_id: str,
    *,
    team: str = "team-a",
    submitted: datetime = T0,
    started: datetime | None = None,
    finished: datetime | None = None,
    state: JobState | None = None,
    spec: JobSpec = SMALL,
) -> None:
    reg.insert(job_id, team, f"rkp-{job_id}", spec)
    with sqlite3.connect(reg.path) as c:
        c.execute("UPDATE jobs SET submitted_at = ? WHERE id = ?", (submitted.isoformat(), job_id))
    if started:
        reg.transition(job_id, JobState.RUNNING, started_at=started)
    if finished:
        reg.transition(job_id, state or JobState.SUCCEEDED, finished_at=finished)
    elif state:
        reg.transition(job_id, state)


def add_service(
    reg: Registry, name: str, created: datetime, deleted: datetime | None, team: str = "team-a"
) -> None:
    reg.insert_service(team, f"rkp-svc-{name}", ServiceSpec(name=name, job_id="x"))
    with sqlite3.connect(reg.path) as c:
        c.execute(
            "UPDATE services SET created_at = ?, deleted_at = ? WHERE name = ?",
            (created.isoformat(), deleted and deleted.isoformat(), name),
        )


@pytest.fixture
def reg(tmp_path: Path) -> Registry:
    return Registry(tmp_path / "r.db")


# --- cost ------------------------------------------------------------------------------------


def test_finished_job_is_billed_requests_times_runtime(reg: Registry) -> None:
    add_job(reg, "j1", started=h(1), finished=h(3))  # 2 hours
    c = team_cost(reg, "team-a", h(0), h(10), RATES)
    assert c.cpu_hours == pytest.approx(0.8 * 2, abs=1e-4)
    assert c.gb_hours == pytest.approx(2504 / 1024 * 2, abs=1e-4)
    assert c.cost == pytest.approx(1.6 * 0.02 + 2504 / 1024 * 2 * 0.003, abs=1e-4)
    assert [line.id for line in c.lines] == ["j1"]


def test_only_the_part_inside_the_window_counts(reg: Registry) -> None:
    add_job(reg, "j1", started=h(1), finished=h(3))
    assert team_cost(reg, "team-a", h(2), h(10), RATES).cpu_hours == pytest.approx(0.8)
    assert team_cost(reg, "team-a", h(1.5), h(2), RATES).cpu_hours == pytest.approx(0.4)
    assert team_cost(reg, "team-a", h(4), h(10), RATES).lines == []


def test_running_job_is_billed_until_now_and_queued_jobs_cost_nothing(reg: Registry) -> None:
    add_job(reg, "running", started=h(1))
    add_job(reg, "queued", state=JobState.QUEUED)
    add_job(reg, "cancelled-in-queue", state=JobState.CANCELLED)
    c = team_cost(reg, "team-a", h(0), h(4), RATES)
    assert [line.id for line in c.lines] == ["running"]
    assert c.cpu_hours == pytest.approx(0.8 * 3)


def test_services_are_billed_from_creation_to_deletion(reg: Registry) -> None:
    add_service(reg, "old", created=h(0), deleted=h(2))  # 1.25 CPU / 2560Mi
    add_service(reg, "live", created=h(3), deleted=None)
    c = team_cost(reg, "team-a", h(1), h(5), RATES)
    by_id = {line.id: line for line in c.lines}
    assert by_id["old"].hours == pytest.approx(1)
    assert by_id["live"].hours == pytest.approx(2)
    assert c.cpu_hours == pytest.approx(1.25 * 3, abs=1e-4)
    assert c.gb_hours == pytest.approx(2.5 * 3, abs=1e-4)


def test_other_teams_are_not_included(reg: Registry) -> None:
    add_job(reg, "a", started=h(1), finished=h(2))
    add_job(reg, "b", team="team-b", started=h(1), finished=h(5))
    add_service(reg, "bsvc", created=h(0), deleted=None, team="team-b")
    assert [line.id for line in team_cost(reg, "team-a", h(0), h(9), RATES).lines] == ["a"]


@pytest.mark.parametrize(
    ("text", "delta"),
    [
        ("90m", timedelta(minutes=90)),
        ("24h", timedelta(hours=24)),
        ("7d", timedelta(days=7)),
        ("2w", timedelta(weeks=2)),
    ],
)
def test_parse_since(text: str, delta: timedelta) -> None:
    assert parse_since(text) == delta


@pytest.mark.parametrize("bad", ["", "7", "0d", "1y", "-3d", "7 days"])
def test_parse_since_rejects(bad: str) -> None:
    with pytest.raises(ValueError):
        parse_since(bad)


def test_repo_rates_file_loads() -> None:
    rates = Rates.load(ROOT / "config" / "rates.yaml")
    assert rates.cpu_hour > 0 and rates.gb_hour > 0 and rates.currency == "USD"


def test_cost_endpoint_is_scoped_to_the_callers_team(platform: Platform) -> None:
    now = datetime.now(UTC)
    add_job(
        platform.registry,
        "mine",
        started=now - timedelta(hours=2),
        finished=now - timedelta(hours=1),
    )
    add_job(platform.registry, "theirs", team="team-b", started=now - timedelta(hours=2))
    r = platform.http.get("/cost", params={"since": "24h"}, headers=platform.as_team(KEY_A))
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["team"] == "team-a"
    assert [line["id"] for line in body["lines"]] == ["mine"]
    assert body["cpu_hours"] == pytest.approx(0.8, abs=1e-3)
    bad = platform.http.get("/cost", params={"since": "forever"}, headers=platform.as_team(KEY_A))
    assert bad.status_code == 422


def test_cli_cost_prints_every_configured_team_and_a_total(
    platform: Platform, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = datetime.now(UTC)
    add_job(platform.registry, "a1", started=now - timedelta(hours=1), finished=now)
    keys = {"team-a": KEY_A, "team-b": KEY_B}
    monkeypatch.setattr(cli, "configured_teams", lambda: ["team-a", "team-b"])
    monkeypatch.setattr(
        cli.Client,
        "from_config",
        classmethod(lambda _cls, team=None: Client("", keys[team or "team-a"], http=platform.http)),
    )
    r = CliRunner().invoke(cli.app, ["cost", "--since", "24h"])
    assert r.exit_code == 0, r.output
    lines = r.stdout.splitlines()
    team_a = next(x for x in lines if x.startswith("team-a")).split()
    assert team_a[1:4] == ["1", "0", "0.800"]
    assert any(x.startswith("team-b") for x in lines)
    assert lines[-1].startswith("total")


# --- metrics ---------------------------------------------------------------------------------


def samples(reg: Registry, now: datetime) -> dict[tuple[str, tuple[tuple[str, str], ...]], float]:
    out = {}
    for family in RegistryCollector(reg, ["team-a", "team-b"]).collect(now=now):
        for s in family.samples:
            out[(s.name, tuple(sorted(s.labels.items())))] = s.value
    return out


def test_metrics_queue_runtime_and_state_counts(reg: Registry) -> None:
    add_job(reg, "a1", submitted=h(0), started=h(0) + timedelta(seconds=34), finished=h(0.1))
    add_job(reg, "a2", submitted=h(0), started=h(0) + timedelta(seconds=137))
    add_job(reg, "a3", submitted=h(0), state=JobState.QUEUED)
    add_job(
        reg,
        "b1",
        team="team-b",
        submitted=h(0),
        started=h(0) + timedelta(seconds=34),
        finished=h(0.05),
        state=JobState.FAILED,
    )
    s = samples(reg, now=h(0) + timedelta(seconds=200))
    a, b = (("team", "team-a"),), (("team", "team-b"),)

    assert s[("rkp_jobs_total", (("state", "SUCCEEDED"), ("team", "team-a")))] == 1
    assert s[("rkp_jobs_total", (("state", "FAILED"), ("team", "team-b")))] == 1
    assert s[("rkp_jobs_active", (("state", "RUNNING"), ("team", "team-a")))] == 1
    assert s[("rkp_jobs_active", (("state", "QUEUED"), ("team", "team-a")))] == 1
    assert s[("rkp_jobs_active", (("state", "QUEUED"), ("team", "team-b")))] == 0  # zero-filled

    assert s[("rkp_job_queue_seconds_count", a)] == 2
    assert s[("rkp_job_queue_seconds_sum", a)] == pytest.approx(34 + 137)
    assert s[("rkp_job_queue_seconds_bucket", (("le", "60.0"), ("team", "team-a")))] == 1
    assert s[("rkp_job_runtime_seconds_count", a)] == 1
    assert s[("rkp_job_runtime_seconds_sum", a)] == pytest.approx(360 - 34)

    assert s[("rkp_job_last_queue_seconds", a)] == pytest.approx(137)
    assert s[("rkp_job_last_queue_seconds", b)] == pytest.approx(34)
    assert s[("rkp_job_oldest_waiting_seconds", a)] == pytest.approx(200)  # a3, still queued
    assert s[("rkp_job_oldest_waiting_seconds", b)] == 0


def test_metrics_endpoint_serves_prometheus_text(platform: Platform) -> None:
    r = platform.http.get("/metrics")  # no key needed: aggregate numbers only
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/plain")
    assert 'rkp_jobs_total{state="SUCCEEDED",team="team-a"} 0.0' in r.text
    assert 'rkp_services_live{team="team-b"} 0.0' in r.text


def test_dashboard_only_queries_metrics_the_api_exports(reg: Registry) -> None:
    import json
    import re

    dash = json.loads((ROOT / "dashboards" / "rkp.json").read_text())
    exprs = [t["expr"] for p in dash["panels"] for t in p.get("targets", [])]
    used = {
        re.sub(r"_(bucket|sum|count)$", "", m) for e in exprs for m in re.findall(r"rkp_\w+", e)
    }
    exported = {f.name for f in RegistryCollector(reg, ["team-a"]).collect()}
    exported |= {f"{n}_total" for n in exported}  # counters are exposed with _total
    assert used and used <= exported, used - exported
    assert all(p["datasource"]["uid"] == "prometheus" for p in dash["panels"] if "datasource" in p)
