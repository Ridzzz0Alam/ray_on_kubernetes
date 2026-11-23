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

from platform_api.config import Settings
from platform_api.k8s import KubeClient, KubeError, RealKube
from platform_api.logs import setup_logging
from platform_api.manifests import build_code_configmap, build_rayjob, k8s_name
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

    @contextlib.asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        task = None
        if run_reconciler:
            task = asyncio.create_task(run_forever(registry, kube, settings.reconcile_interval))
        yield
        if task:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    app = FastAPI(title="rkp platform API", version="0.1.0", lifespan=lifespan)
    app.state.registry = registry
    app.state.kube = kube

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
        job_id = secrets.token_hex(4)
        name = k8s_name(job_id)
        rayjob = build_rayjob(
            job_id, team, spec, image=settings.image, ray_version=settings.ray_version
        )
        created = kube.create_rayjob(team, rayjob)
        if spec.files:
            cm = build_code_configmap(job_id, team, spec, owner_uid=created["metadata"]["uid"])
            try:
                kube.create_configmap(team, cm)
            except KubeError:
                kube.delete_rayjob(team, name)
                raise
        job = registry.insert(job_id, team, name, spec)
        log.info("job submitted", extra={"job_id": job_id, "team": team, "workers": spec.workers})
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
