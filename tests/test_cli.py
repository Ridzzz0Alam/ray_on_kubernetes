"""The rkp CLI and SDK, driven against the in-process API."""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from sdk import cli
from sdk.client import Client, RkpError, load_code, resolve
from tests.conftest import KEY_A, KEY_B, Platform

runner = CliRunner()


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
