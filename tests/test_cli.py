"""The rkp CLI and SDK, driven against the in-process API."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from platform_api.models import Resources
from platform_api.reconciler import reconcile_once
from sdk import cli
from sdk import client as client_mod
from sdk.client import Client, RkpError, load_code, resolve
from tests.conftest import KEY_A, KEY_B, Platform

runner = CliRunner()
HELLO = {"entrypoint": "python t.py", "files": {"t.py": "pass"}, "workers": 1}


@pytest.fixture
def as_team(platform: Platform, monkeypatch: pytest.MonkeyPatch):  # type: ignore[no-untyped-def]
    keys = {"team-a": KEY_A, "team-b": KEY_B}
    monkeypatch.setattr(
        cli.Client,
        "from_config",
        classmethod(lambda _cls, team=None: Client("", keys[team or "team-a"], http=platform.http)),
    )
    return platform


def test_submit_list_status_logs_cancel(as_team: Platform, tmp_path: Path) -> None:
    script = tmp_path / "hello.py"
    script.write_text("print('hi')\n")
    r = runner.invoke(cli.app, ["submit", "--team", "team-a", "--no-wait", str(script), "--", "-x"])
    assert r.exit_code == 0, r.output
    job_id = r.stdout.strip()
    job = as_team.registry.get(job_id)
    assert job is not None
    assert job.spec["entrypoint"] == "python hello.py -x"

    r = runner.invoke(cli.app, ["list", "--team", "team-a"])
    assert job_id in r.stdout and "SUBMITTED" in r.stdout

    as_team.kube.logs[("team-a", job.k8s_name)] = "hi\n"
    assert runner.invoke(cli.app, ["logs", job_id]).stdout == "hi\n"

    r = runner.invoke(cli.app, ["status", job_id, "--team", "team-b"])
    assert isinstance(r.exception, RkpError) and r.exception.status == 403

    r = runner.invoke(cli.app, ["cancel", job_id])
    assert r.exit_code == 0 and "CANCELLED" in r.stdout


def test_wait_exits_nonzero_unless_succeeded(as_team: Platform, tmp_path: Path) -> None:
    script = tmp_path / "x.py"
    script.write_text("")
    job_id = runner.invoke(cli.app, ["submit", "--no-wait", str(script)]).stdout.strip()
    runner.invoke(cli.app, ["cancel", job_id])
    r = runner.invoke(cli.app, ["status", "--wait", job_id])
    assert r.exit_code == 1
    assert "CANCELLED" in r.output


def test_load_code_dir_skips_dotfiles_and_subdirs(tmp_path: Path) -> None:
    (tmp_path / "main.py").write_text("a")
    (tmp_path / ".env").write_text("secret")
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "x.py").write_text("b")
    assert load_code(tmp_path) == {"main.py": "a"}


def test_resolve_reads_team_key_from_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = tmp_path / "rkp.toml"
    cfg.write_text(
        'api_url = "http://h:1"\ndefault_team = "team-a"\n[keys]\nteam-a = "A"\nteam-b = "B"\n'
    )
    monkeypatch.setenv("RKP_CONFIG", str(cfg))
    monkeypatch.delenv("RKP_API_KEY", raising=False)
    monkeypatch.delenv("RKP_API_URL", raising=False)
    assert resolve() == ("http://h:1", "A")
    assert resolve("team-b") == ("http://h:1", "B")
    with pytest.raises(RkpError):
        resolve("team-c")


def _trained_job(p: Platform) -> str:
    job = p.http.post("/jobs", json=HELLO, headers=p.as_team(KEY_A)).json()
    p.kube.set_status("team-a", job["k8s_name"], "Complete", "SUCCEEDED")
    reconcile_once(p.registry, p.kube, p.http.app.state.admitter)  # type: ignore[attr-defined]
    return job["id"]


def test_serve_retries_while_finished_pods_terminate_then_waits_for_running(
    as_team: Platform, monkeypatch: pytest.MonkeyPatch
) -> None:
    as_team.kube.quotas["team-a"] = Resources(2000, 4096)
    job_id = _trained_job(as_team)
    as_team.kube.pods["team-a"] = {f"job/{job_id}": Resources(1250, 3072)}
    sleeps: list[float] = []

    def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        as_team.kube.pods["team-a"] = {}  # the old pods are gone
        for rs in as_team.kube.rayservices.values():  # and the service comes up
            rs["status"] = {"conditions": [{"type": "Ready", "status": "True"}]}

    monkeypatch.setattr(client_mod.time, "sleep", fake_sleep)
    r = runner.invoke(cli.app, ["serve", "create", "sentiment", job_id])
    assert r.exit_code == 0, r.output
    assert "quota busy, retrying" in r.stderr and "RUNNING" in r.stderr
    assert r.stdout.strip() == "sentiment"
    assert len(sleeps) == 2  # one 409 retry, one DEPLOYING poll

    r = runner.invoke(cli.app, ["serve", "list"])
    assert "sentiment" in r.stdout and "RUNNING" in r.stdout
    r = runner.invoke(cli.app, ["serve", "status", "sentiment", "--json"])
    assert json.loads(r.stdout)["job_id"] == job_id
    r = runner.invoke(cli.app, ["serve", "delete", "sentiment"])
    assert r.stdout.strip() == "sentiment DELETED"
    assert runner.invoke(cli.app, ["serve", "list"]).stdout.count("\n") == 1  # header only


def test_serve_gives_up_on_busy_quota_after_retry_for(
    as_team: Platform, monkeypatch: pytest.MonkeyPatch
) -> None:
    as_team.kube.quotas["team-a"] = Resources(2000, 4096)
    job_id = _trained_job(as_team)
    as_team.kube.pods["team-a"] = {f"job/{job_id}": Resources(1250, 3072)}
    monkeypatch.setattr(client_mod.time, "sleep", lambda _s: None)
    r = runner.invoke(cli.app, ["serve", "create", "sentiment", job_id, "--retry-for", "0"])
    assert isinstance(r.exception, RkpError) and r.exception.status == 409
    assert "retry in a few seconds" in r.exception.detail


def test_serve_does_not_retry_other_conflicts(
    as_team: Platform, monkeypatch: pytest.MonkeyPatch
) -> None:
    job = as_team.http.post("/jobs", json=HELLO, headers=as_team.as_team(KEY_A)).json()
    monkeypatch.setattr(client_mod.time, "sleep", lambda _s: pytest.fail("retried"))
    r = runner.invoke(cli.app, ["serve", "create", "sentiment", job["id"]])
    assert isinstance(r.exception, RkpError) and "not SUCCEEDED" in r.exception.detail


def test_serve_wait_exits_nonzero_when_unhealthy(
    as_team: Platform, monkeypatch: pytest.MonkeyPatch
) -> None:
    job_id = _trained_job(as_team)

    def fail_deploy(_s: float) -> None:
        for rs in as_team.kube.rayservices.values():
            apps = {"sentiment": {"status": "DEPLOY_FAILED", "message": "no checkpoint"}}
            rs["status"] = {"activeServiceStatus": {"applicationStatuses": apps}}

    monkeypatch.setattr(client_mod.time, "sleep", fail_deploy)
    r = runner.invoke(cli.app, ["serve", "create", "sentiment", job_id])
    assert r.exit_code == 1
    assert "UNHEALTHY  (no checkpoint)" in r.stderr
