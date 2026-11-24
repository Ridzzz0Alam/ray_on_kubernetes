"""Build the Kubernetes objects: a RayJob (job) or RayService (served model), each with a
ConfigMap holding its code."""

from __future__ import annotations

import json
from typing import Any

from platform_api.models import (
    GPU_RESOURCE,
    REPLICA_CPUS,
    SIM_GPU,
    JobSpec,
    Resources,
    ServiceSpec,
    memory_mib,
)

CODE_DIR = "/home/ray/rkp-code"
# On the kind node, /shared is the repo's ./.shared (infra/kind/cluster.yaml).
SHARED_DATA = "/shared/data"  # datasets + model weights from `make data`, read-only for jobs
CHECKPOINTS = "/shared/checkpoints"  # each job sees only its own team's subdir here
SUBMITTER_CONTAINER = "ray-job-submitter"
HEAD_MEMORY_MIB = 3072
HEAD_CPU_REQUEST_M = 250
SUBMITTER_REQUEST = Resources(cpu_m=50, mem_mib=200)
TTL_SECONDS = 60  # safety net; the reconciler deletes finished RayJobs itself


SERVE_APP = "sentiment"
SERVE_DEPLOYMENT = "Sentiment"  # the class name in workloads/serve/app.py
SERVE_PORT = 8000


def k8s_name(job_id: str) -> str:
    return f"rkp-{job_id}"


def service_k8s_name(name: str) -> str:
    return f"rkp-svc-{name}"


def service_endpoint(name: str, team: str) -> str:
    """KubeRay fronts a RayService's Serve proxies with the Service `<name>-serve-svc`."""
    return f"http://{service_k8s_name(name)}-serve-svc.{team}.svc:{SERVE_PORT}/predict"


def _labels(job_id: str, team: str) -> dict[str, str]:
    return {"app.kubernetes.io/managed-by": "rkp", "rkp.io/job-id": job_id, "rkp.io/team": team}


def _service_labels(name: str, team: str) -> dict[str, str]:
    return {"app.kubernetes.io/managed-by": "rkp", "rkp.io/service": name, "rkp.io/team": team}


def ray_resources_param(resources: dict[str, float]) -> str:
    """KubeRay passes rayStartParams to `ray start` through a shell, so the JSON is quoted."""
    clean = {k: int(v) if float(v).is_integer() else v for k, v in resources.items()}
    return '"' + json.dumps(clean, sort_keys=True).replace('"', '\\"') + '"'


def _container(
    name: str,
    image: str,
    cpu: str,
    request: Resources,
    mem_mib: int,
    mounts: list[dict[str, Any]] | None = None,
    env: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    c: dict[str, Any] = {
        "name": name,
        "image": image,
        "imagePullPolicy": "Never",  # images are kind-loaded, never pulled at pod start
        "resources": {
            "requests": {"cpu": f"{request.cpu_m}m", "memory": f"{request.mem_mib}Mi"},
            "limits": {"cpu": cpu, "memory": f"{mem_mib}Mi"},
        },
    }
    if request.gpu:  # extended resources can't be overcommitted: request == limit
        c["resources"]["requests"][GPU_RESOURCE] = str(request.gpu)
        c["resources"]["limits"][GPU_RESOURCE] = str(request.gpu)
    if mounts:
        c["volumeMounts"] = mounts
    if env:
        c["env"] = env
    return c


# Requests are half the limits (as measured in Phase 1) so the laptop isn't over-reserved.
def _head_request() -> Resources:
    return Resources(HEAD_CPU_REQUEST_M, HEAD_MEMORY_MIB // 2)


def _worker_request(cpu: int, memory: str) -> Resources:
    return Resources(cpu * 500, memory_mib(memory) // 2)


def _gpu_worker_request(cpu: int, memory: str) -> Resources:
    return _worker_request(cpu, memory) + Resources(gpu=1)


def job_requests(spec: JobSpec) -> Resources:
    """Everything the job's pods request together: what it counts against the team quota."""
    total = _head_request() + SUBMITTER_REQUEST
    for _ in range(spec.workers):
        total += _worker_request(spec.cpu, spec.memory)
    for _ in range(spec.gpus):
        total += _gpu_worker_request(spec.cpu, spec.memory)
    return total


def service_requests(spec: ServiceSpec) -> Resources:
    """A RayService has no submitter: just the head and its fixed workers."""
    total = _head_request()
    for _ in range(spec.workers):
        total += _worker_request(spec.cpu, spec.memory)
    return total


def _ray_pod_storage(
    job_id: str, team: str, has_code: bool, code_cm: str, checkpoints_readonly: bool = False
) -> tuple[list[dict], list[dict], list[dict]]:
    """Volumes, mounts and env shared by the head and workers.

    hostPath type Directory (not DirectoryOrCreate): `make teams` creates the dirs owned by the
    host user (uid 1000 = the image's `ray` user). Auto-created ones would be root-owned and
    unwritable."""
    volumes: list[dict] = [
        {"name": "shared-data", "hostPath": {"path": SHARED_DATA, "type": "Directory"}},
        {
            "name": "checkpoints",
            "hostPath": {"path": f"{CHECKPOINTS}/{team}", "type": "Directory"},
        },
    ]
    mounts: list[dict] = [
        {"name": "shared-data", "mountPath": SHARED_DATA, "readOnly": True},
        {"name": "checkpoints", "mountPath": CHECKPOINTS, "readOnly": checkpoints_readonly},
    ]
    env = [
        {"name": "RKP_JOB_ID", "value": job_id},
        {"name": "RKP_TEAM", "value": team},
        {"name": "RKP_STORAGE", "value": CHECKPOINTS},
        {"name": "RKP_DATA", "value": SHARED_DATA},
    ]
    if has_code:
        volumes.append({"name": "code", "configMap": {"name": code_cm}})
        mounts.append({"name": "code", "mountPath": CODE_DIR})
        env.append({"name": "PYTHONPATH", "value": CODE_DIR})  # Ray workers can import it too
    return volumes, mounts, env


def _worker_group(
    name: str, replicas: int, params: dict[str, str], container: dict, labels: dict, volumes: list
) -> dict[str, Any]:
    return {
        "groupName": name,
        "replicas": replicas,
        "minReplicas": replicas,
        "maxReplicas": replicas,
        "rayStartParams": params,
        "template": {
            "metadata": {"labels": labels},
            "spec": {"containers": [container], "volumes": volumes},
        },
    }


def _ray_cluster(
    labels: dict[str, str],
    storage: tuple[list[dict], list[dict], list[dict]],
    *,
    workers: int,
    cpu: int,
    memory: str,
    resources: dict[str, float],
    image: str,
    ray_version: str,
    gpus: int = 0,
) -> dict[str, Any]:
    """RayCluster spec shared by RayJob (`rayClusterSpec`) and RayService (`rayClusterConfig`).

    `gpus` adds a gpu-sim worker group: each pod requests one example.com/gpu from Kubernetes
    (so the scheduler and team quotas count it) and starts Ray with {"sim_gpu": 1} (so Ray
    places sim_gpu tasks and Train workers there)."""
    volumes, mounts, env = storage
    worker_params: dict[str, str] = {"num-cpus": str(cpu)}
    if resources:
        worker_params["resources"] = ray_resources_param(resources)
    # The head carries dashboard, GCS, autoscaler, the driver and (Ray Train V2) the 0-CPU
    # TrainController. Ray's memory monitor killed jobs at 1.5Gi (Phase 2: hello) and at 2Gi
    # (Phase 3: a torch-importing driver + controller reached 1.91GB), so it gets 3Gi.
    head = _container("ray-head", image, "1", _head_request(), HEAD_MEMORY_MIB, mounts, env)
    groups = []
    if workers:
        worker = _container(
            "ray-worker",
            image,
            str(cpu),
            _worker_request(cpu, memory),
            memory_mib(memory),
            mounts,
            env,
        )
        groups.append(_worker_group("cpu", workers, worker_params, worker, labels, volumes))
    if gpus:
        gpu_worker = _container(
            "ray-worker",
            image,
            str(cpu),
            _gpu_worker_request(cpu, memory),
            memory_mib(memory),
            mounts,
            env,
        )
        gpu_params = {
            "num-cpus": str(cpu),
            "resources": ray_resources_param({**resources, SIM_GPU: 1}),
        }
        groups.append(_worker_group("gpu-sim", gpus, gpu_params, gpu_worker, labels, volumes))
    return {
        "rayVersion": ray_version,
        "headGroupSpec": {
            "rayStartParams": {"num-cpus": "0"},  # no tasks or replicas on the head
            "template": {
                "metadata": {"labels": labels},
                "spec": {"containers": [head], "volumes": volumes},
            },
        },
        "workerGroupSpecs": groups,
    }


def build_rayjob(
    job_id: str, team: str, spec: JobSpec, *, image: str, ray_version: str
) -> dict[str, Any]:
    name = k8s_name(job_id)
    has_code = bool(spec.files)
    # KubeRay's submitter runs `ray job submit -- <entrypoint>` through a shell, so `cd x && ...`
    # would split there. `env -C` changes directory without any shell operator.
    entrypoint = f"env -C {CODE_DIR} {spec.entrypoint}" if has_code else spec.entrypoint
    submitter = _container(SUBMITTER_CONTAINER, image, "500m", SUBMITTER_REQUEST, 512)
    cluster = _ray_cluster(
        _labels(job_id, team),
        _ray_pod_storage(job_id, team, has_code, name),
        workers=spec.workers,
        cpu=spec.cpu,
        memory=spec.memory,
        resources=spec.resources,
        image=image,
        ray_version=ray_version,
        gpus=spec.gpus,
    )
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
            "rayClusterSpec": cluster,
        },
    }


def serve_config(spec: ServiceSpec) -> dict[str, Any]:
    """serveConfigV2: one app built by workloads/serve/app.py:build, autoscaled on in-flight
    requests. Delays are short so the demo scales within a minute (defaults: 30s up, 600s down)."""
    return {
        "applications": [
            {
                "name": SERVE_APP,
                "import_path": "app:build",
                "route_prefix": "/",
                # The team's checkpoint dir is mounted at CHECKPOINTS; the run is named by job id.
                "args": {"run_dir": f"{CHECKPOINTS}/{spec.job_id}", "profile": spec.profile},
                "deployments": [
                    {
                        "name": SERVE_DEPLOYMENT,
                        "ray_actor_options": {"num_cpus": REPLICA_CPUS},
                        "max_ongoing_requests": 8,
                        "autoscaling_config": {
                            "min_replicas": spec.min_replicas,
                            "initial_replicas": spec.min_replicas,
                            "max_replicas": spec.max_replicas,
                            "target_ongoing_requests": spec.target_ongoing_requests,
                            "upscale_delay_s": 5,
                            "downscale_delay_s": 30,
                            "metrics_interval_s": 2,
                            "look_back_period_s": 10,
                        },
                    }
                ],
            }
        ]
    }


def build_rayservice(
    team: str, spec: ServiceSpec, *, image: str, ray_version: str
) -> dict[str, Any]:
    name = service_k8s_name(spec.name)
    labels = _service_labels(spec.name, team)
    cluster = _ray_cluster(
        labels,
        _ray_pod_storage(spec.job_id, team, True, name, checkpoints_readonly=True),
        workers=spec.workers,
        cpu=spec.cpu,
        memory=spec.memory,
        resources={},
        image=image,
        ray_version=ray_version,
    )
    return {
        "apiVersion": "ray.io/v1",
        "kind": "RayService",
        "metadata": {"name": name, "labels": labels},
        "spec": {
            # JSON is valid YAML, and KubeRay parses this field as YAML.
            "serveConfigV2": json.dumps(serve_config(spec), indent=2),
            "rayClusterConfig": cluster,
        },
    }


def code_configmap(
    name: str,
    labels: dict[str, str],
    files: dict[str, str],
    *,
    owner_kind: str,
    owner_uid: str,
) -> dict[str, Any]:
    """Owned by its RayJob/RayService (same name), so deleting that garbage-collects the code."""
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": name,
            "labels": labels,
            "ownerReferences": [
                {
                    "apiVersion": "ray.io/v1",
                    "kind": owner_kind,
                    "name": name,
                    "uid": owner_uid,
                    "blockOwnerDeletion": False,
                }
            ],
        },
        "data": dict(files),
    }


def build_code_configmap(
    job_id: str, team: str, spec: JobSpec, *, owner_uid: str
) -> dict[str, Any]:
    return code_configmap(
        k8s_name(job_id),
        _labels(job_id, team),
        spec.files,
        owner_kind="RayJob",
        owner_uid=owner_uid,
    )


def build_service_configmap(
    team: str, spec: ServiceSpec, files: dict[str, str], *, owner_uid: str
) -> dict[str, Any]:
    return code_configmap(
        service_k8s_name(spec.name),
        _service_labels(spec.name, team),
        files,
        owner_kind="RayService",
        owner_uid=owner_uid,
    )
