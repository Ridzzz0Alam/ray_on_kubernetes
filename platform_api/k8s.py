"""The only module that talks to Kubernetes. Tests swap `KubeClient` for a fake."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from kubernetes import client, config
from kubernetes.client.exceptions import ApiException
from kubernetes.utils import parse_quantity

from platform_api.manifests import SUBMITTER_CONTAINER
from platform_api.models import GPU_RESOURCE, UNBOUNDED, Resources

GROUP, VERSION, PLURAL = "ray.io", "v1", "rayjobs"
SERVICES = "rayservices"


class KubeError(Exception):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(f"kubernetes API error {status}: {message}")
        self.status = status


class KubeClient(Protocol):
    def create_rayjob(self, namespace: str, body: dict[str, Any]) -> dict[str, Any]: ...

    def get_rayjob(self, namespace: str, name: str) -> dict[str, Any] | None: ...

    def delete_rayjob(self, namespace: str, name: str) -> None:
        """Delete the RayJob and (through owner references) its cluster and code. Idempotent."""

    def create_configmap(self, namespace: str, body: dict[str, Any]) -> None: ...

    def create_rayservice(self, namespace: str, body: dict[str, Any]) -> dict[str, Any]: ...

    def get_rayservice(self, namespace: str, name: str) -> dict[str, Any] | None: ...

    def delete_rayservice(self, namespace: str, name: str) -> None:
        """Delete the RayService and (through owner references) its cluster and code. Idempotent."""

    def job_logs(self, namespace: str, name: str, tail_lines: int | None = None) -> str | None:
        """Logs of the newest submitter pod, which tails the Ray driver. None if no pod yet."""

    def pending_reason(self, namespace: str, job_id: str) -> str | None:
        """Why one of the job's pods can't be scheduled (the scheduler's PodScheduled=False
        message, e.g. "0/1 nodes are available: 1 Insufficient example.com/gpu."), or None."""

    def get_quota(self, namespace: str) -> Resources | None:
        """The namespace's hard requests.cpu / requests.memory (tightest across its
        ResourceQuotas), or None when it has no quota."""

    def pod_requests(self, namespace: str) -> dict[str, Resources]:
        """Requests of the rkp pods that the namespace's ResourceQuota still counts, summed per
        owner ("job/<id>" or "service/<name>"). Same rule as the quota: a pod stops counting
        once it has finished (Succeeded/Failed) or its deletion grace period has run out."""


def quota_from_hard(hards: list[dict[str, str]]) -> Resources | None:
    if not hards:
        return None

    def tightest(key: str) -> float | None:
        values = [parse_quantity(h[key]) for h in hards if key in h]
        return min(values) if values else None

    cpu, mem = tightest("requests.cpu"), tightest("requests.memory")
    gpu = tightest(f"requests.{GPU_RESOURCE}")
    return Resources(
        UNBOUNDED if cpu is None else int(cpu * 1000),
        UNBOUNDED if mem is None else int(mem / 2**20),
        UNBOUNDED if gpu is None else int(gpu),
    )


def pod_owner(labels: dict[str, str]) -> str | None:
    """The rkp job or service a pod belongs to, from the labels manifests.py puts on it."""
    if "rkp.io/job-id" in labels:
        return f"job/{labels['rkp.io/job-id']}"
    if "rkp.io/service" in labels:
        return f"service/{labels['rkp.io/service']}"
    return None


def quota_counts(pod: Any, now: datetime) -> bool:
    """Kubernetes' pod quota rule: finished pods, and terminating pods past their grace
    period, no longer count against the quota."""
    if pod.status and pod.status.phase in ("Succeeded", "Failed"):
        return False
    deleted = pod.metadata.deletion_timestamp
    if deleted is not None:
        grace = pod.metadata.deletion_grace_period_seconds or 0
        return now <= deleted + timedelta(seconds=grace)
    return True


def pod_spec_requests(pod: Any) -> Resources:
    total = Resources()
    for c in pod.spec.containers:
        req = (c.resources and c.resources.requests) or {}
        total += Resources(
            int(parse_quantity(req.get("cpu", "0")) * 1000),
            int(parse_quantity(req.get("memory", "0")) / 2**20),
            int(parse_quantity(req.get(GPU_RESOURCE, "0"))),
        )
    return total


class RealKube:
    """`KubeClient` backed by the official client and the local kubeconfig."""

    def __init__(self, context: str | None = None) -> None:
        api = config.new_client_from_config(context=context)
        self._custom = client.CustomObjectsApi(api)
        self._core = client.CoreV1Api(api)
        self._delete_opts = client.V1DeleteOptions(propagation_policy="Background")

    @staticmethod
    def _wrap(e: Exception) -> KubeError:
        return KubeError(getattr(e, "status", 0) or 0, getattr(e, "reason", None) or str(e))

    def _create(self, plural: str, namespace: str, body: dict[str, Any]) -> dict[str, Any]:
        try:
            return self._custom.create_namespaced_custom_object(
                GROUP, VERSION, namespace, plural, body
            )
        except ApiException as e:
            raise self._wrap(e) from e

    def _get(self, plural: str, namespace: str, name: str) -> dict[str, Any] | None:
        try:
            return self._custom.get_namespaced_custom_object(
                GROUP, VERSION, namespace, plural, name
            )
        except ApiException as e:
            if e.status == 404:
                return None
            raise self._wrap(e) from e

    def _delete(self, plural: str, namespace: str, name: str) -> None:
        try:
            self._custom.delete_namespaced_custom_object(
                GROUP, VERSION, namespace, plural, name, body=self._delete_opts
            )
        except ApiException as e:
            if e.status != 404:
                raise self._wrap(e) from e

    def create_rayjob(self, namespace: str, body: dict[str, Any]) -> dict[str, Any]:
        return self._create(PLURAL, namespace, body)

    def get_rayjob(self, namespace: str, name: str) -> dict[str, Any] | None:
        return self._get(PLURAL, namespace, name)

    def delete_rayjob(self, namespace: str, name: str) -> None:
        self._delete(PLURAL, namespace, name)

    def create_rayservice(self, namespace: str, body: dict[str, Any]) -> dict[str, Any]:
        return self._create(SERVICES, namespace, body)

    def get_rayservice(self, namespace: str, name: str) -> dict[str, Any] | None:
        return self._get(SERVICES, namespace, name)

    def delete_rayservice(self, namespace: str, name: str) -> None:
        self._delete(SERVICES, namespace, name)

    def create_configmap(self, namespace: str, body: dict[str, Any]) -> None:
        try:
            self._core.create_namespaced_config_map(namespace, body)
        except ApiException as e:
            raise self._wrap(e) from e

    def job_logs(self, namespace: str, name: str, tail_lines: int | None = None) -> str | None:
        try:
            pods = self._core.list_namespaced_pod(namespace, label_selector=f"job-name={name}")
            if not pods.items:
                return None
            newest = max(pods.items, key=lambda p: p.metadata.creation_timestamp)
            kwargs: dict[str, Any] = {"container": SUBMITTER_CONTAINER}
            if tail_lines is not None:
                kwargs["tail_lines"] = tail_lines
            return self._core.read_namespaced_pod_log(newest.metadata.name, namespace, **kwargs)
        except ApiException as e:
            if e.status in (400, 404):  # container still creating, or pod just went away
                return None
            raise self._wrap(e) from e

    def get_quota(self, namespace: str) -> Resources | None:
        try:
            quotas = self._core.list_namespaced_resource_quota(namespace)
        except ApiException as e:
            raise self._wrap(e) from e
        return quota_from_hard([q.spec.hard or {} for q in quotas.items if q.spec])

    def pod_requests(self, namespace: str) -> dict[str, Resources]:
        try:
            pods = self._core.list_namespaced_pod(
                namespace, label_selector="app.kubernetes.io/managed-by=rkp"
            )
        except ApiException as e:
            raise self._wrap(e) from e
        now = datetime.now(UTC)
        totals: dict[str, Resources] = {}
        for pod in pods.items:
            owner = pod_owner(pod.metadata.labels or {})
            if owner is None or not quota_counts(pod, now):
                continue
            totals[owner] = totals.get(owner, Resources()) + pod_spec_requests(pod)
        return totals

    def pending_reason(self, namespace: str, job_id: str) -> str | None:
        try:
            pods = self._core.list_namespaced_pod(
                namespace, label_selector=f"rkp.io/job-id={job_id}"
            )
        except ApiException as e:
            raise self._wrap(e) from e
        for pod in pods.items:
            for cond in (pod.status and pod.status.conditions) or []:
                if cond.type == "PodScheduled" and cond.status == "False" and cond.message:
                    return cond.message.strip().splitlines()[0]
        return None
