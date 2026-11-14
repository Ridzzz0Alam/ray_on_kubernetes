# ARCHITECTURE.md

## 1. Goals and non-goals

**Goals:** self-service job submission for multiple teams, distributed training that survives worker
failures, an autoscaled inference service, fair resource isolation between teams, and visibility into usage and cost.
Everything runs on a **2-core / 15 GB laptop**.

**Non-goals:** production auth, HA, multi-node clusters, real GPUs locally (they're simulated, see §7).

## 2. System overview

```
               ┌─────────────── host (laptop) ───────────────┐
  rkp CLI/SDK ─┤  FastAPI platform_api  ── SQLite registry   │
   (X-API-Key) │      │  reconciler loop                     │
               └──────┼──────────────────────────────────────┘
                      │ kubernetes python client (kubeconfig → kind)
┌─────────────────────▼──────── kind (single node) ───────────────────────┐
│ ns: kuberay-system   KubeRay operator                                    │
│ ns: monitoring       Prometheus (minimal) · Grafana · kube-state-metrics │
│                                                                          │
│ ns: team-a  [ResourceQuota, LimitRange]      ns: team-b  [ ... ]         │
│   RayJob ──► RayCluster (head + N workers)     RayService ──► Serve      │
│                   │                                                      │
│                   ▼  hostPath /shared  (data cache, checkpoints)         │
└──────────────────────────────────────────────────────────────────────────┘
```

## 3. Components

### 3.1 platform_api (FastAPI)
- **Auth:** `X-API-Key` → `team` → namespace `team-<name>`. Keys live in a git-ignored YAML file.
  Every handler gets its namespace from the key, never from the request body.
- **Endpoints:** `POST /jobs`, `GET /jobs`, `GET /jobs/{id}`, `GET /jobs/{id}/logs`,
  `DELETE /jobs/{id}` (cancel), `POST /services`, `DELETE /services/{name}`, `GET /metrics`, `GET /healthz`.
- **`k8s.py`:** the only module that talks to Kubernetes (`CustomObjectsApi` for `ray.io/v1`
  RayJob/RayService, `CoreV1Api` for pod logs and quotas). It sits behind a protocol so tests can use a fake.
- **Registry (SQLite):** one source of truth for job history. CRs are short-lived, but the registry keeps the record.
- **Reconciler:** an asyncio task that polls RayJob status every 5s and records state transitions with
  timestamps. This is where `queue_seconds = started_at - submitted_at` comes from.

### 3.2 SDK / CLI (`sdk/`)
A thin `httpx` client plus a Typer CLI (`rkp`). The CLI reads the API URL and key from env or `~/.config/rkp.toml`.

### 3.3 Ray runtime image (`docker/`)
One image for every workload: `rayproject/ray:<pinned>-py312-cpu` with CPU-only torch,
transformers and datasets baked in. Built once and `kind load`ed. Workloads never `pip install` at runtime,
so there are no slow starts and no network dependence. User code is shipped through RayJob
`runtimeEnvYAML.working_dir` (a zipped dir, limited to small scripts).

### 3.4 Training workload (`workloads/train/`)
- `TorchTrainer` with `ScalingConfig(num_workers=2, use_gpu=False, resources_per_worker={"CPU": 1})`.
- `torch.set_num_threads(1)` per worker, so 2 workers don't fight over 2 physical cores.
- Profiles: `tiny` = `prajjwal1/bert-tiny` (about 4M params, default), `distilbert` (opt-in, slow).
- Data: Amazon reviews sentiment subset, tokenized once and cached to `/shared/data`.

### 3.5 Serving workload (`workloads/serve/`)
A Ray Serve deployment that loads the latest checkpoint for a given team/job. It's deployed as a `RayService`
with `autoscaling_config` (min 1, max 3, `target_ongoing_requests` ≈ 2).

## 4. Job lifecycle

```
SUBMITTED ──► PENDING (pods waiting: quota / capacity) ──► RUNNING ──► SUCCEEDED
                │                                           │  │
                └──────────────── CANCELLED ◄───────────────┘  └─► RETRYING ─► FAILED
```

| Concern | Mechanism |
|---|---|
| Checkpointing | `ray.train.report(metrics, checkpoint=...)` every epoch → `RunConfig(storage_path="/shared/checkpoints/<team>/<job>")`, `CheckpointConfig(num_to_keep=2)` |
| Resume after worker loss | `FailureConfig(max_failures=3)`. The training loop calls `ray.train.get_checkpoint()` and restores model, optimizer and epoch |
| Retry failed jobs | RayJob `spec.backoffLimit = retries`. Each retry starts a new cluster, and the same `storage_path` + run name means it resumes from the checkpoint |
| Cancellation | API deletes the RayJob CR (the operator tears down the RayCluster), and the registry marks it `CANCELLED` |
| Cleanup | `shutdownAfterJobFinishes: true`, `ttlSecondsAfterFinished: 60` |

Checkpoints sit on a hostPath, so they survive pod death and cluster recreation. On a single
node that's equivalent to shared storage. In production this would be S3/GCS (§9).

## 5. Multi-tenancy

- One namespace per team, each with:
  - a `ResourceQuota` (e.g. `requests.cpu: 2`, `requests.memory: 3Gi`, `count/rayjobs.ray.io: 3`)
  - a `LimitRange` that sets default requests, so pods without them still count against the quota
- **Isolation guarantees:** the API scopes every call to the key's namespace. RBAC (when the API runs in-cluster)
  limits the ServiceAccount to `ray.io` resources in `team-*` namespaces.
- **No starvation:** a quota is a hard ceiling per team. Team-a filling its quota can't use
  team-b's share, because the node's capacity is sized so that the sum of the team quotas is no more than allocatable.
- **Queueing:** quota-rejected pods leave the RayJob Pending, and that shows up as queue time.
  The stretch goal is Kueue, which gives proper admission queues and fair sharing.

## 6. Resource budget

The node has about 4 vCPU and 15 GB. The stack target is **≤ 3 CPU requested and ≤ 7 GB RAM at peak**, leaving the desktop usable.

| Component | CPU req | Mem limit | Notes |
|---|---|---|---|
| kind control plane | ~0.5 | ~1.2 GB | single node, no workers |
| KubeRay operator | 0.1 | 256 Mi | |
| Ray head (per cluster) | 0.5 | 1.5 Gi | `num-cpus: 0`, so no tasks run on the head |
| Ray worker × 2 | 1 each | 1.5 Gi each | `tiny` profile; `distilbert` needs about 2.5 Gi each |
| Prometheus | 0.1 | 512 Mi | 6h retention, 30s scrape |
| Grafana | 0.1 | 256 Mi | |
| kube-state-metrics | 0.05 | 128 Mi | |
| platform_api (host) | – | ~150 MB | uvicorn, outside the cluster |
| **Total (one training job + monitoring)** | **~3.3** | **~6.8 GB** | |

Rules: one RayCluster at a time, except in the multi-tenancy demo, which uses 1 worker per team.
Turn monitoring off when you don't need it (`make monitoring-down`).

## 7. GPU awareness

- **Locally:** a Ray custom resource. A `gpu-sim` worker group starts with
  `rayStartParams.resources: '"{\"sim_gpu\": 1}"'`, and jobs request `resources_per_worker={"sim_gpu": 1}`.
  This exercises the scheduling path (placement, pending when it's busy) with no hardware.
  It can also be done at the K8s layer by patching the node's capacity with `example.com/gpu`, so quotas count "GPUs".
- **Cloud (optional):** the same RayJob with `num_gpus=1` on a short-lived spot GPU node pool, created and deleted by script.

## 8. Observability

- **Metrics sources:** Ray (built-in Prometheus exporter on head/worker pods, through a PodMonitor), the API's
  `/metrics` (`prometheus_client`), and kube-state-metrics (`kube_resourcequota`, pod phases).
- **Key metrics:** `rkp_jobs_total{team,state}`, `rkp_job_queue_seconds_bucket{team}`,
  `rkp_job_runtime_seconds{team}`, `ray_node_cpu_utilization`, `kube_resourcequota{namespace}`.
- **Dashboard:** job states per team, p50/p95 queue time, CPU/mem used vs. quota, Serve replicas and QPS.
- **Cost:** `cost = Σ runtime × (cpu_req × cpu_rate + mem_gb × mem_rate [+ gpu × gpu_rate])` from the registry.
  Rates go in `config/rates.yaml`.

## 9. Production gaps (deliberately not built)

| Here | Production |
|---|---|
| hostPath checkpoints | S3/GCS through Ray `storage_path` |
| API keys in YAML | OIDC/SSO, a team mapping from groups |
| SQLite registry | Postgres |
| polling reconciler | K8s watch / informer, or a controller |
| ResourceQuota + simple queue | Kueue with fair sharing and preemption |
| single kind node | managed K8s, separate node pools (CPU/GPU, spot) |

## 10. Key technology choices

| Choice | Alternative | Reason |
|---|---|---|
| KubeRay RayJob | `ray job submit` against a long-lived cluster | A cluster per job gives clean isolation per team, per-job resources and automatic teardown |
| FastAPI + SQLite | expose the K8s API directly | Hides K8s from users, enforces team scoping, keeps history |
| bert-tiny default | DistilBERT | Fits a 2-core CPU budget, and the platform mechanics are the point, not the model quality |
| Helm with pinned values | kube-prometheus-stack defaults | The full stack uses more than 2 GB RAM, while the minimal setup is about 0.9 GB |
