"""`rkp` CLI: submit | status | logs | cancel | list | cost | serve {create,status,list,delete}."""

from __future__ import annotations

import json
import shlex
import time
from pathlib import Path
from typing import Annotated

import typer

from sdk.client import Client, RkpError, configured_teams, load_code

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
    workers: Annotated[int, typer.Option(min=0, max=4, help="CPU workers")] = 2,
    gpus: Annotated[int, typer.Option(min=0, max=1, help="gpu-sim workers (1 sim GPU each)")] = 0,
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
    job = client.submit(
        entrypoint, files, workers=workers, gpus=gpus, cpu=cpu, memory=memory, retries=retries
    )
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


@app.command()
def cost(
    team: TeamOpt = None,
    since: Annotated[str, typer.Option(help="Window: 90m, 24h, 7d, 2w")] = "7d",
    as_json: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """CPU-hours, GB-hours and cost per team (every team you have a key for, or --team)."""
    teams = [team] if team else configured_teams() or [None]
    reports = [_client(t).cost(since) for t in teams]
    if as_json:
        typer.echo(json.dumps(reports, indent=2))
        return
    cur = reports[0]["currency"]
    typer.echo(f"usage over the last {since} (requests x time; rates from config/rates.yaml)")
    typer.echo(
        f"{'TEAM':10} {'JOBS':>5} {'SVCS':>5} {'CPU-HOURS':>10} {'GB-HOURS':>10} {'COST':>10}"
    )
    for r in reports:
        kinds = [line["kind"] for line in r["lines"]]
        typer.echo(
            f"{r['team']:10} {kinds.count('job'):>5} {kinds.count('service'):>5} "
            f"{r['cpu_hours']:>10.3f} {r['gb_hours']:>10.3f} {r['cost']:>10.4f} {cur}"
        )
    if len(reports) > 1:
        cpu = sum(r["cpu_hours"] for r in reports)
        gb = sum(r["gb_hours"] for r in reports)
        total = sum(r["cost"] for r in reports)
        typer.echo(f"{'total':10} {'':>5} {'':>5} {cpu:>10.3f} {gb:>10.3f} {total:>10.4f} {cur}")


serve_app = typer.Typer(help="Serve a trained checkpoint with Ray Serve.", no_args_is_help=True)
app.add_typer(serve_app, name="serve")


def _print_service(svc: dict) -> None:
    for key in ("name", "team", "job_id", "state", "message", "endpoint", "created_at"):
        typer.echo(f"{key:11} {svc.get(key) or '-'}")


@serve_app.command("create")
def serve_create(
    name: Annotated[str, typer.Argument(help="Service name, unique within the team")],
    job_id: Annotated[str, typer.Argument(help="SUCCEEDED training job whose checkpoint to serve")],
    team: TeamOpt = None,
    profile: Annotated[str, typer.Option(help="Model profile: tiny | distilbert")] = "tiny",
    min_replicas: Annotated[int, typer.Option(min=1, max=4)] = 1,
    max_replicas: Annotated[int, typer.Option(min=1, max=4)] = 3,
    workers: Annotated[int, typer.Option(min=1, max=2)] = 1,
    cpu: Annotated[int, typer.Option(min=1, max=2, help="Ray CPUs per worker")] = 2,
    memory: Annotated[str, typer.Option(help="Memory per worker")] = "2Gi",
    retry_for: Annotated[
        float, typer.Option(min=0, help="Seconds to retry while finished work frees quota")
    ] = 120,
    wait: Annotated[bool, typer.Option("--wait/--no-wait", help="Follow until RUNNING")] = True,
) -> None:
    """Deploy a service; exits non-zero unless it ends up RUNNING (with --wait)."""
    client = _client(team)
    svc = client.serve(
        name,
        job_id,
        retry_for=retry_for,
        on_retry=lambda msg: typer.secho(f"quota busy, retrying: {msg}", fg="yellow", err=True),
        profile=profile,
        min_replicas=min_replicas,
        max_replicas=max_replicas,
        workers=workers,
        cpu=cpu,
        memory=memory,
    )
    typer.echo(svc["name"])
    if not wait:
        return
    start = time.monotonic()
    for svc in client.watch_service(name):
        msg = f"  ({svc['message']})" if svc.get("message") else ""
        color = {"RUNNING": "green", "UNHEALTHY": "red"}.get(svc["state"])
        typer.secho(f"[{time.monotonic() - start:5.0f}s] {svc['state']}{msg}", fg=color, err=True)
    if svc["state"] != "RUNNING":
        raise typer.Exit(1)
    typer.secho(f"POST {svc['endpoint']}", err=True)


@serve_app.command("status")
def serve_status(
    name: str, team: TeamOpt = None, as_json: Annotated[bool, typer.Option("--json")] = False
) -> None:
    """Show one service."""
    svc = _client(team).service(name)
    if as_json:
        typer.echo(json.dumps(svc, indent=2))
    else:
        _print_service(svc)


@serve_app.command("list")
def serve_list(team: TeamOpt = None) -> None:
    """List your team's live services."""
    typer.echo(f"{'NAME':20} {'STATE':10} {'JOB':10} ENDPOINT")
    for s in _client(team).services():
        typer.echo(f"{s['name']:20} {s['state']:10} {s['job_id']:10} {s['endpoint']}")


@serve_app.command("delete")
def serve_delete(name: str, team: TeamOpt = None) -> None:
    """Delete a service and its Ray cluster."""
    svc = _client(team).unserve(name)
    typer.echo(f"{svc['name']} {svc['state']}")


def main() -> None:
    """Entry point: turn API errors into a one-line message instead of a traceback."""
    try:
        app()
    except RkpError as e:
        typer.secho(f"error: {e}", fg="red", err=True)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
