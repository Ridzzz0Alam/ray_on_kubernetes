"""Build the Kubernetes objects for a job: a RayJob plus a ConfigMap holding the user's code."""

from __future__ import annotations

import json
from typing import Any

from platform_api.models import JobSpec, memory_mib

CODE_DIR = "/home/ray/rkp-code"
SUBMITTER_CONTAINER = "ray-job-submitter"
HEAD_MEMORY_MIB = 2048
TTL_SECONDS = 60  # safety net; the reconciler deletes finished RayJobs itself


def k8s_name(job_id: str) -> str:
    return f"rkp-{job_id}"


def _labels(job_id: str, team: str) -> dict[str, str]:
    return {"app.kubernetes.io/managed-by": "rkp", "rkp.io/job-id": job_id, "rkp.io/team": team}


def ray_resources_param(resources: dict[str, float]) -> str:
    """KubeRay passes rayStartParams to `ray start` through a shell, so the JSON is quoted."""
    clean = {k: int(v) if float(v).is_integer() else v for k, v in resources.items()}
    return '"' + json.dumps(clean, sort_keys=True).replace('"', '\\"') + '"'


def _container(name: str, image: str, cpu: str, cpu_req: str, mem_mib: int, code: bool) -> dict:
    c: dict[str, Any] = {
        "name": name,
        "image": image,
        "imagePullPolicy": "Never",  # images are kind-loaded, never pulled at pod start
        "resources": {
            "requests": {"cpu": cpu_req, "memory": f"{mem_mib // 2}Mi"},
            "limits": {"cpu": cpu, "memory": f"{mem_mib}Mi"},
        },
    }
    if code:
        c["volumeMounts"] = [{"name": "code", "mountPath": CODE_DIR}]
        c["env"] = [{"name": "PYTHONPATH", "value": CODE_DIR}]  # Ray workers can import it too
    return c


def build_rayjob(
    job_id: str, team: str, spec: JobSpec, *, image: str, ray_version: str
) -> dict[str, Any]:
    name = k8s_name(job_id)
    has_code = bool(spec.files)
    volumes = [{"name": "code", "configMap": {"name": name}}] if has_code else []
    # KubeRay's submitter runs `ray job submit -- <entrypoint>` through a shell, so `cd x && ...`
    # would split there. `env -C` changes directory without any shell operator.
    entrypoint = f"env -C {CODE_DIR} {spec.entrypoint}" if has_code else spec.entrypoint

    worker_params: dict[str, str] = {"num-cpus": str(spec.cpu)}
    if spec.resources:
        worker_params["resources"] = ray_resources_param(spec.resources)

    # Requests are half the limits (as measured in Phase 1) so the laptop isn't over-reserved.
    # The head carries dashboard, GCS, autoscaler and the driver: at 1.5Gi Ray's memory monitor
    # killed the JobSupervisor (Phase 2), so it gets 2Gi.
    head = _container("ray-head", image, "1", "250m", HEAD_MEMORY_MIB, has_code)
    worker = _container(
        "ray-worker", image, str(spec.cpu), f"{spec.cpu * 500}m", memory_mib(spec.memory), has_code
    )
    submitter = _container(SUBMITTER_CONTAINER, image, "500m", "50m", 512, False)
    submitter["resources"]["requests"]["memory"] = "200Mi"

    return {
        "apiVersion": "ray.io/v1",
        "kind": "RayJob",
        "metadata": {"name": name, "labels": _labels(job_id, team)},
        "spec": {
            "entrypoint": entrypoint,
            "shutdownAfterJobFinishes": True,
            "ttlSecondsAfterFinished": TTL_SECONDS,
            "backoffLimit": spec.retries,
            "submitterPodTemplate": {
                "metadata": {"labels": _labels(job_id, team)},
                "spec": {"restartPolicy": "Never", "containers": [submitter]},
            },
            "rayClusterSpec": {
                "rayVersion": ray_version,
                "headGroupSpec": {
                    "rayStartParams": {"num-cpus": "0"},  # no tasks on the head
                    "template": {
                        "metadata": {"labels": _labels(job_id, team)},
                        "spec": {"containers": [head], "volumes": volumes},
                    },
                },
                "workerGroupSpecs": [
                    {
                        "groupName": "cpu",
                        "replicas": spec.workers,
                        "minReplicas": spec.workers,
                        "maxReplicas": spec.workers,
                        "rayStartParams": worker_params,
                        "template": {
                            "metadata": {"labels": _labels(job_id, team)},
                            "spec": {"containers": [worker], "volumes": volumes},
                        },
                    }
                ],
            },
        },
    }


def build_code_configmap(
    job_id: str, team: str, spec: JobSpec, *, owner_uid: str
) -> dict[str, Any]:
    """Owned by the RayJob, so deleting the RayJob garbage-collects the code too."""
    name = k8s_name(job_id)
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": name,
            "labels": _labels(job_id, team),
            "ownerReferences": [
                {
                    "apiVersion": "ray.io/v1",
                    "kind": "RayJob",
                    "name": name,
                    "uid": owner_uid,
                    "blockOwnerDeletion": False,
                }
            ],
        },
        "data": dict(spec.files),
    }
