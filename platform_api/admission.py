"""Whole-job admission against the team's ResourceQuota, with a FIFO queue per team.

Kubernetes checks quota one pod at a time. A RayJob is a gang (head + workers + submitter), so
two half-admitted jobs could each hold a head and leave neither able to start its workers. The
API admits a job only when all of its pods fit, keeps the rest QUEUED in submission order, and
rejects up front a job that could never fit. The ResourceQuota stays as the hard backstop.
"""

from __future__ import annotations

import logging
import secrets
import threading

from platform_api.k8s import KubeClient, KubeError
from platform_api.manifests import build_code_configmap, build_rayjob, job_requests, k8s_name
from platform_api.models import Job, JobSpec, JobState, Resources
from platform_api.registry import Registry

log = logging.getLogger("rkp.admission")


class QuotaExceeded(Exception):
    """The job is bigger than the team's whole quota, so it can never be admitted."""


class Admitter:
    def __init__(self, registry: Registry, kube: KubeClient, *, image: str, ray_version: str):
        self.registry = registry
        self.kube = kube
        self.image = image
        self.ray_version = ray_version
        # The API's threadpool and the reconciler both admit; one lock keeps the sum honest.
        self._lock = threading.Lock()

    def in_use(self, team: str) -> Resources:
        """Requests of the team's admitted, unfinished jobs (counted from the registry, so a
        RayJob whose pods don't exist yet still counts)."""
        total = Resources()
        for job in self.registry.active(team):
            if job.state != JobState.QUEUED:
                total += job_requests(JobSpec.model_validate(job.spec))
        return total

    def submit(self, team: str, spec: JobSpec) -> Job:
        need = job_requests(spec)
        with self._lock:
            quota = self.kube.get_quota(team)
            if quota is not None and not need.fits_in(quota):
                raise QuotaExceeded(
                    f"job requests {need} but {team}'s whole quota is {quota}: "
                    "use fewer workers or less memory"
                )
            job_id = secrets.token_hex(4)
            name = k8s_name(job_id)
            queued = [j for j in self.registry.active(team) if j.state == JobState.QUEUED]
            if queued or not self._fits_now(team, need, quota):
                msg = self._waiting_message(team, need, quota, ahead=len(queued))
                job = self.registry.insert(job_id, team, name, spec, JobState.QUEUED, msg)
                log.info("job queued", extra={"job_id": job_id, "team": team, "need": str(need)})
                return job
            self._launch(job_id, team, spec)
            return self.registry.insert(job_id, team, name, spec)

    def admit_queued(self) -> None:
        """Start queued jobs, oldest first per team, while they fit. Stop at the first one
        that doesn't, so a big job isn't starved by small ones behind it."""
        with self._lock:
            teams = {j.team for j in self.registry.active() if j.state == JobState.QUEUED}
            for team in sorted(teams):
                quota = self.kube.get_quota(team)
                for job in self.registry.active(team):
                    if job.state != JobState.QUEUED:
                        continue
                    spec = self.registry.spec(job.id)
                    need = job_requests(spec)
                    if not self._fits_now(team, need, quota):
                        msg = self._waiting_message(team, need, quota, ahead=0)
                        self.registry.set_message(job.id, msg)
                        break
                    try:
                        self._launch(job.id, team, spec)
                    except KubeError as e:
                        self.registry.transition(job.id, JobState.FAILED, f"admission: {e}")
                        continue
                    self.registry.transition(job.id, JobState.SUBMITTED, "admitted from queue")
                    log.info("job admitted", extra={"job_id": job.id, "team": team})

    def _fits_now(self, team: str, need: Resources, quota: Resources | None) -> bool:
        return quota is None or (self.in_use(team) + need).fits_in(quota)

    def _waiting_message(
        self, team: str, need: Resources, quota: Resources | None, ahead: int
    ) -> str:
        why = f"waiting for quota: needs {need}, {team} uses {self.in_use(team)} of {quota}"
        return why + (f" ({ahead} queued ahead)" if ahead else "")

    def _launch(self, job_id: str, team: str, spec: JobSpec) -> None:
        """Create the RayJob, then its code ConfigMap (owned by the RayJob)."""
        rayjob = build_rayjob(job_id, team, spec, image=self.image, ray_version=self.ray_version)
        created = self.kube.create_rayjob(team, rayjob)
        if spec.files:
            cm = build_code_configmap(job_id, team, spec, owner_uid=created["metadata"]["uid"])
            try:
                self.kube.create_configmap(team, cm)
            except KubeError:
                self.kube.delete_rayjob(team, k8s_name(job_id))
                raise
