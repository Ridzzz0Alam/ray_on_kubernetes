"""`rkp` CLI: submit | status | logs | cancel | list."""

from __future__ import annotations

import json
import shlex
import time
from pathlib import Path
from typing import Annotated

import typer

from sdk.client import Client, RkpError, load_code

app = typer.Typer(help="Submit and manage Ray jobs on the rkp platform.", no_args_is_help=True)

TeamOpt = Annotated[
    str | None, typer.Option("--team", "-t", help="Team whose API key to use (from rkp.toml)")
]


def _client(team: str | None) -> Client:
    return Client.from_config(team)


def _follow(client: Client, job_id: str) -> None:
    """Print each state change; exit non-zero unless the job succeeded."""
    start = time.monotonic()
    state = None
    for job in client.watch(job_id):
        state = job["state"]
        msg = f"  ({job['message']})" if job.get("message") else ""
        color = {"SUCCEEDED": "green", "FAILED": "red", "CANCELLED": "yellow"}.get(state)
        typer.secho(f"[{time.monotonic() - start:5.0f}s] {state}{msg}", fg=color, err=True)
    if state != "SUCCEEDED":
        raise typer.Exit(1)


@app.command()
def submit(
    path: Annotated[Path, typer.Argument(exists=True, help="Script file or flat directory")],
    args: Annotated[list[str] | None, typer.Argument(help="Script arguments (after --)")] = None,
    team: TeamOpt = None,
    script: Annotated[str, typer.Option(help="Entry script when PATH is a directory")] = "main.py",
    workers: Annotated[int, typer.Option(min=1, max=4)] = 2,
    cpu: Annotated[int, typer.Option(min=1, max=2, help="CPUs per worker")] = 1,
    memory: Annotated[str, typer.Option(help="Memory per worker")] = "1536Mi",
    retries: Annotated[int, typer.Option(min=0, max=5)] = 0,
    wait: Annotated[bool, typer.Option("--wait/--no-wait", help="Follow until it ends")] = True,
) -> None:
    """Submit a script as a Ray job."""
    files = load_code(path)
    entry = path.name if path.is_file() else script
    if entry not in files:
        typer.secho(f"{entry} not found in {path}", fg="red", err=True)
        raise typer.Exit(2)
    entrypoint = shlex.join(["python", entry, *(args or [])])
    client = _client(team)
    job = client.submit(entrypoint, files, workers=workers, cpu=cpu, memory=memory, retries=retries)
    typer.echo(job["id"])
    if wait:
        _follow(client, job["id"])


@app.command()
def status(
    job_id: str,
    team: TeamOpt = None,
    as_json: Annotated[bool, typer.Option("--json")] = False,
    wait: Annotated[bool, typer.Option("--wait", help="Follow until it ends")] = False,
) -> None:
    """Show one job."""
    client = _client(team)
    if wait:
        _follow(client, job_id)
        return
    job = client.get(job_id)
    if as_json:
        typer.echo(json.dumps(job, indent=2))
        return
    for key in ("id", "team", "state", "message", "submitted_at", "started_at", "finished_at"):
        typer.echo(f"{key:13} {job.get(key) or '-'}")


@app.command()
def logs(
    job_id: str,
    team: TeamOpt = None,
    tail: Annotated[int | None, typer.Option(min=1)] = None,
) -> None:
    """Print the driver logs of a job."""
    typer.echo(_client(team).logs(job_id, tail), nl=False)


@app.command()
def cancel(job_id: str, team: TeamOpt = None) -> None:
    """Cancel a pending or running job."""
    job = _client(team).cancel(job_id)
    typer.echo(f"{job['id']} {job['state']}")


@app.command("list")
def list_jobs(
    team: TeamOpt = None, limit: Annotated[int, typer.Option(min=1, max=500)] = 20
) -> None:
    """List your team's most recent jobs."""
    jobs = _client(team).list(limit)
    typer.echo(f"{'ID':10} {'STATE':10} {'SUBMITTED':20} ENTRYPOINT")
    for j in jobs:
        submitted = j["submitted_at"][:19].replace("T", " ")
        typer.echo(f"{j['id']:10} {j['state']:10} {submitted:20} {j['spec']['entrypoint']}")


def main() -> None:
    """Entry point: turn API errors into a one-line message instead of a traceback."""
    try:
        app()
    except RkpError as e:
        typer.secho(f"error: {e}", fg="red", err=True)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
