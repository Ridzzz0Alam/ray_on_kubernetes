"""FastAPI service. Run with `make api` (uvicorn --factory platform_api.main:create_app).

No `from __future__ import annotations` here: FastAPI must resolve the local Annotated aliases.
"""

import asyncio
import contextlib
import logging
import secrets
from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import PlainTextResponse

from platform_api.admission import Admitter, QuotaExceeded
from platform_api.config import Settings
from platform_api.k8s import KubeClient, KubeError, RealKube
from platform_api.logs import setup_logging
from platform_api.models import TERMINAL, Job, JobSpec, JobState
from platform_api.reconciler import run_forever
from platform_api.registry import Registry

log = logging.getLogger("rkp.api")


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

    @contextlib.asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        task = None
        if run_reconciler:
            task = asyncio.create_task(
                run_forever(registry, kube, admitter, settings.reconcile_interval)
            )
        yield
        if task:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

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

    return app
