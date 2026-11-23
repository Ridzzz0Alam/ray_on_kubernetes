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
- **Auth:** `X-API-Key` → `team`, and the team name is its namespace (`team-a`). Keys live in the git-ignored
  `config/api_keys.yaml` (`make keys`). Every handler gets its namespace from the key, never from the request
  body. Another team's job returns 403.
- **Endpoints:** `POST /jobs`, `GET /jobs`, `GET /jobs/{id}`, `GET /jobs/{id}/logs`,
  `DELETE /jobs/{id}` (cancel), `POST /services`, `DELETE /services/{name}`, `GET /metrics`, `GET /healthz`.
- **`k8s.py`:** the only module that talks to Kubernetes (`CustomObjectsApi` for `ray.io/v1`
  RayJob/RayService, `CoreV1Api` for pod logs and quotas). It sits behind a protocol so tests can use a fake.
- **Registry (SQLite):** one source of truth for job history. CRs are short-lived, but the registry keeps the record.
- **Reconciler:** an asyncio task that polls RayJob status every 5s and records state transitions.
  `started_at`/`finished_at` come from `status.rayJobInfo` (Ray's own times), so `queue_seconds =
  started_at - submitted_at` is right even for jobs shorter than a poll. On a terminal state it copies the
  submitter pod's logs (which tail the driver) into SQLite, then deletes the RayJob.

### 3.2 SDK / CLI (`sdk/`)
A thin `httpx` client plus a Typer CLI (`rkp`). The CLI reads the API URL and key from env (`RKP_API_URL`,
`RKP_API_KEY`) or a TOML file (`RKP_CONFIG`, else `~/.config/rkp.toml`, else the repo's `config/rkp.toml`).
`--team` picks which key from the file to use.

### 3.3 Ray runtime image (`docker/`)
One image for every workload: `rayproject/ray:<pinned>-py312-cpu` with CPU-only torch,
transformers and datasets baked in. Built once and `kind load`ed. Workloads never `pip install` at runtime,
so there are no slow starts and no network dependence. User code (a script or a flat directory, ≤ 512 KiB)
is sent inline to the API, which stores it in a ConfigMap owned by the RayJob. It's mounted at
`/home/ray/rkp-code` on the head and workers (on `PYTHONPATH`), and the entrypoint runs as
`env -C /home/ray/rkp-code <cmd>`. KubeRay's submitter puts the entrypoint through a shell, so shell
operators (`&& ; |` …) are rejected.

### 3.4 Training workload (`workloads/train/`)
- `TorchTrainer` with `ScalingConfig(num_workers=2, use_gpu=False, resources_per_worker={"CPU": 1})`.
- `torch.set_num_threads(1)` per worker, so 2 workers don't fight over 2 physical cores.
- Profiles: `tiny` = `prajjwal1/bert-tiny` (about 4M params, default), `distilbert` (opt-in, slow).
  Model and dataset revisions are pinned by commit SHA in `profiles.py`.
- Data: `fancyzhx/amazon_polarity`, a shuffled 4k train / 1k eval subset (max_len 128). `make data` streams it and
  tokenizes it once inside the Ray image into `/shared/data`, so jobs train offline.
- Each worker evaluates its shard of the eval set, then an `all_reduce` gives global loss/accuracy.
  `ray.train.report` runs every epoch.
- Storage: the API mounts `/shared/data` read-only and the team's `/shared/checkpoints/<team>` at
  `/shared/checkpoints` on head and workers. `RunConfig(storage_path=$RKP_STORAGE, name=$RKP_JOB_ID)`.

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
| Cancellation | API saves the logs so far, deletes the RayJob CR (owner references remove the RayCluster, submitter Job and code ConfigMap), and the registry marks it `CANCELLED` |
| Cleanup | The reconciler deletes finished RayJobs after saving their logs. `shutdownAfterJobFinishes: true` and `ttlSecondsAfterFinished: 60` are the safety net |

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
| Ray head (per cluster) | 0.25 | 3 Gi | `num-cpus: 0`, so no tasks run on the head, but the driver, TrainController and Train V2 helper actors do. Measured 2.76 GiB during training |
| Ray worker × 2 | 1 each | 1.5 Gi each | `tiny` profile, measured 1.33 GiB; `distilbert` needs about 2.5 Gi each |
| Prometheus | 0.1 | 512 Mi | 6h retention, 30s scrape |
| Grafana | 0.1 | 256 Mi | |
| kube-state-metrics | 0.05 | 128 Mi | |
| platform_api (host) | – | ~150 MB | uvicorn, outside the cluster |
| **Total (one training job + monitoring)** | **~3.1** | **~8.5 GB** | Sum of limits, which are ceilings. Measured: **6.9 GiB for `tiny` training with no monitoring**, already near the 7 GB target. Phase 7 must keep monitoring small or turn it on only for demos |

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
