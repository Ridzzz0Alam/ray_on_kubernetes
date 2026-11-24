"""FastAPI service. Run with `make api` (uvicorn --factory platform_api.main:create_app).

No `from __future__ import annotations` here: FastAPI must resolve the local Annotated aliases.
"""

import asyncio
import contextlib
import logging
import secrets
import sqlite3
from collections.abc import AsyncIterator, Mapping
from datetime import datetime
from typing import Annotated, Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import PlainTextResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest, start_http_server

from platform_api.admission import Admitter, QuotaBusy, QuotaExceeded
from platform_api.config import ROOT, Settings
from platform_api.cost import Rates, TeamCost, parse_since, team_cost
from platform_api.k8s import KubeClient, KubeError, RealKube
from platform_api.logs import setup_logging
from platform_api.manifests import service_endpoint
from platform_api.metrics import make_registry
from platform_api.models import (
    TERMINAL,
    Job,
    JobSpec,
    JobState,
    Service,
    ServiceSpec,
    ServiceState,
)
from platform_api.reconciler import observe_service, run_forever
from platform_api.registry import Registry, utcnow

log = logging.getLogger("rkp.api")
# The platform-owned serving code, shipped in the RayService's ConfigMap (like a job's code).
SERVE_FILES = (
    ROOT / "workloads" / "serve" / "app.py",
    ROOT / "workloads" / "train" / "profiles.py",
)


def create_app(
    settings: Settings | None = None,
    kube: KubeClient | None = None,
    *,
    run_reconciler: bool = True,
) -> FastAPI:
    settings = settings or Settings.from_env()
    setup_logging()
    kube = kube or RealKube(settings.kube_context)
    registry = Registry(settings.db_path)
    admitter = Admitter(registry, kube, image=settings.image, ray_version=settings.ray_version)

    metrics = make_registry(registry, settings.api_keys.values())

    @contextlib.asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        task = None
        server = None
        if settings.metrics_addr:  # e.g. 172.18.0.1:9108, reachable from kind pods only
            host, _, port = settings.metrics_addr.rpartition(":")
            server, _thread = start_http_server(int(port), addr=host, registry=metrics)
            log.info("metrics listener", extra={"addr": settings.metrics_addr})
        if run_reconciler:
            task = asyncio.create_task(
                run_forever(registry, kube, admitter, settings.reconcile_interval)
            )
        yield
        if task:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        if server:
            server.shutdown()

    app = FastAPI(title="rkp platform API", version="0.1.0", lifespan=lifespan)
    app.state.registry = registry
    app.state.kube = kube
    app.state.admitter = admitter

    def team_of(x_api_key: Annotated[str | None, Header()] = None) -> str:
        """The key decides the team, and the team is the namespace. Never trust the body."""
        if x_api_key:
            for key, team in settings.api_keys.items():
                if secrets.compare_digest(key.encode(), x_api_key.encode()):
                    return team
        raise HTTPException(401, "missing or unknown X-API-Key")

    Team = Annotated[str, Depends(team_of)]

    def owned_job(job_id: str, team: Team) -> Job:
        job = registry.get(job_id)
        if job is None:
            raise HTTPException(404, f"job {job_id} not found")
        if job.team != team:
            raise HTTPException(403, f"job {job_id} belongs to another team")
        return job

    OwnedJob = Annotated[Job, Depends(owned_job)]

    @app.exception_handler(KubeError)
    async def _kube_error(_: Request, exc: KubeError) -> PlainTextResponse:
        log.error("kubernetes error", extra={"status": exc.status, "error": str(exc)})
        return PlainTextResponse(str(exc), status_code=502)

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/metrics")
    def prometheus_metrics() -> Response:
        """Per-team job and service metrics from the registry (no key: aggregate numbers only)."""
        return Response(generate_latest(metrics), media_type=CONTENT_TYPE_LATEST)

    @app.get("/cost")
    def cost(team: Team, since: Annotated[str, Query()] = "7d") -> TeamCost:
        """The caller's team usage in [now - since, now]: CPU-hours, GB-hours and cost."""
        try:
            window = parse_since(since)
        except ValueError as e:
            raise HTTPException(422, str(e)) from None
        until = utcnow()
        return team_cost(registry, team, until - window, until, Rates.load(settings.rates_path))

    @app.post("/jobs", status_code=201)
    def submit(spec: JobSpec, team: Team) -> Job:
        """201 with state SUBMITTED, or QUEUED when the team's quota is busy right now.
        422 when the job is bigger than the team's whole quota."""
        try:
            job = admitter.submit(team, spec)
        except QuotaExceeded as e:
            raise HTTPException(422, str(e)) from None
        log.info(
            "job submitted",
            extra={"job_id": job.id, "team": team, "workers": spec.workers, "state": job.state},
        )
        return job

    @app.get("/jobs")
    def list_jobs(team: Team, limit: Annotated[int, Query(ge=1, le=500)] = 50) -> list[Job]:
        return registry.list(team, limit)

    @app.get("/jobs/{job_id}")
    def get_job(job: OwnedJob) -> Job:
        return job

    @app.get("/jobs/{job_id}/logs", response_class=PlainTextResponse)
    def job_logs(job: OwnedJob, tail: Annotated[int | None, Query(ge=1)] = None) -> str:
        if job.state in TERMINAL:
            logs = registry.logs(job.id) or ""
            return "\n".join(logs.splitlines()[-tail:]) if tail else logs
        return kube.job_logs(job.team, job.k8s_name, tail) or ""

    @app.delete("/jobs/{job_id}")
    def cancel(job: OwnedJob) -> Job:
        if job.state in TERMINAL:
            raise HTTPException(409, f"job {job.id} already {job.state}")
        if job.state != JobState.QUEUED:  # a queued job has nothing in Kubernetes yet
            logs = kube.job_logs(job.team, job.k8s_name)
            if logs:
                registry.set_logs(job.id, logs)
            kube.delete_rayjob(job.team, job.k8s_name)
        registry.transition(job.id, JobState.CANCELLED, "cancelled by user")
        log.info("job cancelled", extra={"job_id": job.id, "team": job.team})
        updated = registry.get(job.id)
        assert updated is not None
        return updated

    def service_view(team: str, row: Mapping[str, Any]) -> Service:
        spec = ServiceSpec.model_validate_json(row["spec"])
        if row["deleted_at"]:
            state, message = ServiceState.DELETED, None
        else:
            rs = kube.get_rayservice(team, row["k8s_name"])
            if rs is None:
                state, message = ServiceState.UNHEALTHY, "RayService was deleted outside rkp"
            else:
                state, message = observe_service(rs)
        return Service(
            name=spec.name,
            team=team,
            k8s_name=row["k8s_name"],
            job_id=spec.job_id,
            state=state,
            message=message,
            endpoint=service_endpoint(spec.name, team),
            spec=spec.model_dump(),
            created_at=datetime.fromisoformat(row["created_at"]),
            deleted_at=row["deleted_at"] and datetime.fromisoformat(row["deleted_at"]),
        )

    def live_service(name: str, team: Team) -> sqlite3.Row:
        row = registry.live_service(team, name)
        if row is None:
            raise HTTPException(404, f"service {name} not found")
        return row

    LiveService = Annotated[sqlite3.Row, Depends(live_service)]

    @app.post("/services", status_code=201)
    def create_service(spec: ServiceSpec, team: Team) -> Service:
        """Serve the latest checkpoint of one of the team's SUCCEEDED training jobs."""
        job = registry.get(spec.job_id)
        if job is None or job.team != team:  # another team's job is just "not found"
            raise HTTPException(404, f"job {spec.job_id} not found")
        if job.state != JobState.SUCCEEDED:
            raise HTTPException(409, f"job {spec.job_id} is {job.state}, not SUCCEEDED")
        if registry.live_service(team, spec.name) is not None:
            raise HTTPException(409, f"service {spec.name} already exists")
        files = {f.name: f.read_text() for f in SERVE_FILES}
        try:
            admitter.create_service(team, spec, files)
        except QuotaExceeded as e:
            raise HTTPException(422, str(e)) from None
        except QuotaBusy as e:
            raise HTTPException(409, str(e)) from None
        row = registry.live_service(team, spec.name)
        assert row is not None
        return service_view(team, row)

    @app.get("/services")
    def list_services(team: Team) -> list[Service]:
        return [service_view(team, r) for r in registry.live_services(team)]

    @app.get("/services/{name}")
    def get_service(row: LiveService, team: Team) -> Service:
        return service_view(team, row)

    @app.delete("/services/{name}")
    def delete_service(row: LiveService, team: Team) -> Service:
        kube.delete_rayservice(team, row["k8s_name"])
        registry.mark_service_deleted(team, row["name"])
        log.info("service deleted", extra={"team": team, "service": row["name"]})
        deleted = {**dict(row), "deleted_at": utcnow().isoformat()}
        return service_view(team, deleted)

    return app
