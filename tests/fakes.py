"""In-memory stand-in for platform_api.k8s.KubeClient."""

from __future__ import annotations

import uuid
from typing import Any


class FakeKube:
    def __init__(self) -> None:
        self.rayjobs: dict[tuple[str, str], dict[str, Any]] = {}
        self.configmaps: dict[tuple[str, str], dict[str, Any]] = {}
        self.logs: dict[tuple[str, str], str] = {}
        self.deleted: list[tuple[str, str]] = []

    def create_rayjob(self, namespace: str, body: dict[str, Any]) -> dict[str, Any]:
        obj = {**body, "metadata": {**body["metadata"], "uid": str(uuid.uuid4())}}
        self.rayjobs[(namespace, body["metadata"]["name"])] = obj
        return obj

    def get_rayjob(self, namespace: str, name: str) -> dict[str, Any] | None:
        return self.rayjobs.get((namespace, name))

    def delete_rayjob(self, namespace: str, name: str) -> None:
        self.rayjobs.pop((namespace, name), None)
        self.configmaps.pop((namespace, name), None)  # owner-reference cascade
        self.deleted.append((namespace, name))

    def create_configmap(self, namespace: str, body: dict[str, Any]) -> None:
        self.configmaps[(namespace, body["metadata"]["name"])] = body

    def job_logs(self, namespace: str, name: str, tail_lines: int | None = None) -> str | None:
        logs = self.logs.get((namespace, name))
        if logs is not None and tail_lines:
            return "\n".join(logs.splitlines()[-tail_lines:])
        return logs

    # test helper: what the KubeRay operator would write
    def set_status(
        self, namespace: str, name: str, deployment: str, job: str = "", **extra: Any
    ) -> None:
        self.rayjobs[(namespace, name)]["status"] = {
            "jobDeploymentStatus": deployment,
            "jobStatus": job,
            **extra,
        }
