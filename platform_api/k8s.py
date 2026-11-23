"""The only module that talks to Kubernetes. Tests swap `KubeClient` for a fake."""

from __future__ import annotations

from typing import Any, Protocol

from kubernetes import client, config
from kubernetes.client.exceptions import ApiException

from platform_api.manifests import SUBMITTER_CONTAINER

GROUP, VERSION, PLURAL = "ray.io", "v1", "rayjobs"


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

    def job_logs(self, namespace: str, name: str, tail_lines: int | None = None) -> str | None:
        """Logs of the newest submitter pod, which tails the Ray driver. None if no pod yet."""


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

    def create_rayjob(self, namespace: str, body: dict[str, Any]) -> dict[str, Any]:
        try:
            return self._custom.create_namespaced_custom_object(
                GROUP, VERSION, namespace, PLURAL, body
            )
        except ApiException as e:
            raise self._wrap(e) from e

    def get_rayjob(self, namespace: str, name: str) -> dict[str, Any] | None:
        try:
            return self._custom.get_namespaced_custom_object(
                GROUP, VERSION, namespace, PLURAL, name
            )
        except ApiException as e:
            if e.status == 404:
                return None
            raise self._wrap(e) from e

    def delete_rayjob(self, namespace: str, name: str) -> None:
        try:
            self._custom.delete_namespaced_custom_object(
                GROUP, VERSION, namespace, PLURAL, name, body=self._delete_opts
            )
        except ApiException as e:
            if e.status != 404:
                raise self._wrap(e) from e

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
