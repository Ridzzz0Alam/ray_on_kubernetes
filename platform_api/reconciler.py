"""Poll RayJob status into the registry, and clean up RayJobs once they finish."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from platform_api.admission import Admitter
from platform_api.k8s import KubeClient
from platform_api.models import TERMINAL, Job, JobState
from platform_api.registry import Registry

log = logging.getLogger("rkp.reconciler")
_DONE_DEPLOYMENT = {"Complete", "Failed", "ValidationFailed"}
MAX_STORED_LOG_CHARS = 1_000_000
MAX_MESSAGE_CHARS = 300


def _short(message: str | None) -> str | None:
    """KubeRay copies Ray's whole error (an OOM report is ~60 lines); keep the first line."""
    if not message:
        return None
    first = message.strip().splitlines()[0]
    return first if len(first) <= MAX_MESSAGE_CHARS else first[: MAX_MESSAGE_CHARS - 1] + "…"


@dataclass(frozen=True)
class Observation:
    state: JobState
    message: str | None = None
    started_at: datetime | None = None  # when Ray started the driver (status.rayJobInfo)
    finished_at: datetime | None = None


def _time(value: object) -> datetime | None:
    return datetime.fromisoformat(value) if isinstance(value, str) and value else None


def observe(rayjob: dict[str, Any]) -> Observation:
    """Map KubeRay's (jobDeploymentStatus, jobStatus) to our job state.

    Times come from Ray itself rather than from when we polled, so a job shorter than the
    poll interval still gets an accurate started_at (needed for queue time)."""
    status = rayjob.get("status") or {}
    deployment = status.get("jobDeploymentStatus", "")
    ray_status = status.get("jobStatus", "")
    message = _short(status.get("message"))
    info = status.get("rayJobInfo") or {}
    started, finished = _time(info.get("startTime")), _time(info.get("endTime"))
    # With backoffLimit, KubeRay counts failed attempts in status.failed (each one a fresh cluster).
    failed_attempts = int(status.get("failed") or 0)
    if deployment in _DONE_DEPLOYMENT:
        if ray_status == "SUCCEEDED":
            state = JobState.SUCCEEDED
        elif ray_status == "STOPPED":
            state = JobState.CANCELLED
        else:
            state, message = JobState.FAILED, message or _short(status.get("reason"))
            if failed_attempts > 1:
                message = f"failed after {failed_attempts} attempts: {message or 'unknown error'}"
        return Observation(state, message, started, finished or _time(status.get("endTime")))
    if deployment == "Retrying":
        allowed = int((rayjob.get("spec") or {}).get("backoffLimit") or 0) + 1
        return Observation(JobState.RETRYING, f"attempt {failed_attempts} of {allowed} failed")
    # Ray reports the job done a few seconds before KubeRay marks the deployment Complete;
    # until then it's still RUNNING (not back to PENDING).
    if ray_status in {"RUNNING", "SUCCEEDED", "FAILED", "STOPPED"}:
        return Observation(JobState.RUNNING, None, started)
    return Observation(JobState.PENDING)


def finalize(registry: Registry, kube: KubeClient, job: Job, obs: Observation) -> None:
    """Keep the driver logs, delete the RayJob (cascading to its cluster, submitter and code),
    then record the terminal state. Doing it in this order means a failed delete is retried."""
    logs = kube.job_logs(job.team, job.k8s_name)
    registry.set_logs(job.id, (logs or "")[-MAX_STORED_LOG_CHARS:])
    kube.delete_rayjob(job.team, job.k8s_name)
    if registry.transition(job.id, obs.state, obs.message, obs.started_at, obs.finished_at):
        log.info("job finished", extra={"job_id": job.id, "team": job.team, "state": obs.state})


def reconcile_once(registry: Registry, kube: KubeClient, admitter: Admitter | None = None) -> None:
    """Sync admitted jobs from their RayJobs, then admit queued jobs into the freed quota."""
    for job in registry.active():
        if job.state == JobState.QUEUED:
            continue  # no RayJob yet; the admitter owns it
        try:
            rayjob = kube.get_rayjob(job.team, job.k8s_name)
            if rayjob is None:
                registry.transition(job.id, JobState.FAILED, "RayJob was deleted outside rkp")
                log.warning("rayjob missing", extra={"job_id": job.id, "team": job.team})
                continue
            obs = observe(rayjob)
            if obs.state in TERMINAL:
                finalize(registry, kube, job, obs)
            elif registry.transition(job.id, obs.state, obs.message, obs.started_at):
                log.info(
                    "job state", extra={"job_id": job.id, "team": job.team, "state": obs.state}
                )
        except Exception:
            log.exception("reconcile failed", extra={"job_id": job.id})
    if admitter is not None:
        try:
            admitter.admit_queued()
        except Exception:
            log.exception("admission failed")


async def run_forever(
    registry: Registry, kube: KubeClient, admitter: Admitter, interval: float
) -> None:
    while True:
        await asyncio.to_thread(reconcile_once, registry, kube, admitter)
        await asyncio.sleep(interval)
