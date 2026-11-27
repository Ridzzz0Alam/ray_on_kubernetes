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
  `DELETE /jobs/{id}` (cancel), `POST /services`, `GET /services[/{name}]`, `DELETE /services/{name}`, `GET /metrics`, `GET /healthz`.
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
`--team` picks which key from the file to use. Commands: `submit | status | logs | cancel | list | cost` for jobs and
`serve create | status | list | delete` for services. `serve create` retries the API's transient 409
("retry in a few seconds") and then waits until the service is RUNNING.

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
A Ray Serve deployment that loads the latest `checkpoint_<ts>/state.pt` of a team's training job and serves
`POST /predict`. `POST /services {name, job_id}` (the job must be the team's own and SUCCEEDED) creates a `RayService`
whose `serveConfigV2` calls `app:build` with `args: {run_dir, profile}`, plus `autoscaling_config` (min 1, max 3,
`target_ongoing_requests: 2`, `upscale_delay_s: 5`, `downscale_delay_s: 30`). The code ships in a ConfigMap owned by
the RayService, the same way job code does. Replicas take 0.5 Ray CPU and scale inside a fixed worker group
(1 × 2 CPU / 2Gi). There's no KubeRay pod autoscaling on the laptop. A service counts against the team quota: it's
admitted only if it fits now (409 if not, no queue, because it's long-running), and `GET /services/{name}` reads its state
live from the RayService. Measured: 1 → 3 → 1 replicas, with throughput capped at about 90 req/s by the single Serve
HTTP proxy on the head (`docs/demos/serving.md`).

## 4. Job lifecycle

```
QUEUED (in the API: team quota busy) ──► SUBMITTED ──► PENDING (pods starting) ──► RUNNING ──► SUCCEEDED
   │                                           │                                    │  │
   └─────────────────────────────────── CANCELLED ◄─────────────────────────────────┘  └─► RETRYING ─► FAILED
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
  - a `ResourceQuota` (`requests.cpu: 2`, `requests.memory: 4Gi`, `count/rayjobs.ray.io: 3`). That fits one default
    2-worker training job (1.3 CPU / 3272Mi of requests) but not two 1-worker jobs (2 × 2504Mi)
  - a `LimitRange` that sets default requests, so pods without them still count against the quota
- **Isolation guarantees:** the API scopes every call to the key's namespace. RBAC (`infra/k8s/teams/rbac.yaml`, used
  when the API runs in-cluster) binds the `rkp-system/rkp-api` ServiceAccount only inside team namespaces, and only to
  RayJobs/RayServices, code ConfigMaps, pod logs and reading quotas.
- **No starvation:** a quota is a hard ceiling per team. Team-a filling its quota can't use
  team-b's share, because the node's capacity is sized so that the sum of the team quotas is no more than allocatable.
- **Queueing (whole-job admission):** Kubernetes checks quota per pod, but a RayJob is a gang (head + workers +
  submitter). Left to K8s, two queued jobs could each get a head admitted, fill the quota, and leave neither able
  to start its workers. So `platform_api/admission.py` admits a job only when *all* its pod requests fit next to
  the team's other admitted jobs (counted from the registry), plus any pods of finished jobs or deleted services
  that are still terminating (the quota counts them until they're gone, by the same rule). A job that doesn't fit now is `QUEUED` with
  nothing created in K8s. The reconciler admits the queue FIFO per team, stopping at the first job that doesn't
  fit, so big jobs aren't starved. A job bigger than the whole quota is rejected with a 422. The ResourceQuota
  stays as the per-pod backstop. Queue time = submit → Ray driver start. Demo: `docs/demos/multi-tenancy.md`.
  The stretch goal is Kueue, which gives proper admission queues and fair sharing.

## 6. Resource budget

The node has about 4 vCPU and 15 GB. The stack target is **≤ 3 CPU requested and ≤ 7 GB RAM at peak**, leaving the desktop usable.

| Component | CPU req | Mem limit | Notes |
|---|---|---|---|
| kind control plane | ~0.5 | ~1.2 GB | single node, no workers |
| KubeRay operator | 0.1 | 256 Mi | |
| Ray head (per cluster) | 0.25 | 3 Gi | `num-cpus: 0`, so no tasks run on the head, but the driver, TrainController and Train V2 helper actors do. Measured 2.76 GiB during training |
| Ray worker × 2 | 1 each | 1.5 Gi each | `tiny` profile, measured 1.33 GiB; `distilbert` needs about 2.5 Gi each |
| Prometheus | 0.05 | 512 Mi | 6h retention, 30s scrape. Measured idle: 43 MB |
| Grafana | 0.025 | 384 Mi | OOM-killed at 256Mi during start-up. Measured idle: 236 MB |
| kube-state-metrics | 0.01 | 128 Mi | Measured idle: 14 MB |
| platform_api (host) | – | ~150 MB | uvicorn, outside the cluster |
| **Total (one training job + monitoring)** | **~3.1** | **~8.5 GB** | Sum of limits, which are ceilings. Measured: **6.9 GiB for `tiny` training with no monitoring**, already near the 7 GB target. Phase 7 must keep monitoring small or turn it on only for demos |

Rules: one RayCluster at a time, except in the multi-tenancy and GPU demos, which use small 1-worker clusters.
Monitoring off during training (7.01 GiB with it on). The GPU demo's three small clusters peaked at 7.65 GiB with monitoring off
until the heads were started with `--no-monitor` (no autoscaler monitor, ~115 MB each); now 6.84 GiB in the full `make test-k8s`.
Turn monitoring off when you don't need it (`make monitoring-down`).

## 7. GPU awareness

- **Locally (simulated):** the node advertises an extended resource, `example.com/gpu: 1` (`make gpu-sim`, called by
  `make teams`; it patches node capacity the way a device plugin would). `JobSpec.gpus: 1` adds a `gpu-sim` worker
  group whose pods request and limit `example.com/gpu: 1` and start Ray with `{"sim_gpu": 1}`. Scripts ask for it with
  `resources={"sim_gpu": 1}` (tasks) or `resources_per_worker={"CPU": 1, "sim_gpu": 1}` (Ray Train, `train.py --sim-gpu`).
- **Who waits where:** each team's quota has `requests.example.com/gpu: 1`, and `Resources.gpu` is part of whole-job
  admission, so a team's 2nd GPU job is QUEUED in the API. Across teams, GPUs are overcommitted (2 quotas, 1 node GPU):
  both jobs are admitted and the scheduler keeps the second gpu-sim pod `Pending: Insufficient example.com/gpu`.
  KubeRay waits for every pod before submitting, so the job stays PENDING, and the reconciler shows the scheduler's
  reason in the job message. CPU jobs are unaffected. Demo: `docs/demos/gpu-awareness.md`.
- **Cloud (optional, not done):** the same job with `nvidia.com/gpu` and Ray's `GPU` resource on a short-lived GPU
  node pool, created and deleted by script. Needs owner approval at the time (AGENTS.md §4).

## 8. Observability

- **Metrics sources:** Ray's metrics agent on every rkp head/worker pod (Prometheus `kubernetes_sd` on pods, keeping
  the port named `metrics`), the API (a collector in `platform_api/metrics.py` that computes everything from the
  registry at scrape time), and kube-state-metrics (`kube_resourcequota`; collectors limited to resourcequotas, pods
  and namespaces). The API runs on the host, so `make api` adds a metrics-only listener on the kind network gateway
  (`RKP_METRICS_ADDR`, e.g. `172.18.0.1:9108`). Prometheus scrapes that, and the REST API stays on 127.0.0.1.
- **Key metrics:** `rkp_jobs_total{team,state}` (finished jobs), `rkp_jobs_active{team,state}`,
  `rkp_job_queue_seconds{team}` / `rkp_job_runtime_seconds{team}` (histograms), `rkp_job_last_queue_seconds{team}`,
  `rkp_job_oldest_waiting_seconds{team}`, `rkp_services_live{team}`, `ray_node_cpu_utilization`, `ray_node_mem_used`,
  `ray_serve_autoscaling_target_replicas`, `ray_serve_num_http_requests_total`, `kube_resourcequota{namespace}`.
- **Dashboard** (`dashboards/rkp.json`, provisioned by `make monitoring`): queue time per team (last started job, oldest
  waiting job, p50/p95), jobs by state, outcomes, runtime, CPU/memory requests vs. quota, Ray node CPU/memory,
  Serve replicas and requests/s. Demo: `docs/demos/observability.md`.
- **Cost:** `GET /cost?since=7d` (the caller's team) and `rkp cost` (every team the CLI has a key for).
  `cost = Σ hours_held × (cpu_requests × cpu_rate + mem_requests_GiB × gb_rate)`. A job is held from driver start to
  finish and a service from create to delete, both clipped to the window. Rates are in `config/rates.yaml`.

## 9. Production gaps (deliberately not built)

| Here | Production |
|---|---|
| hostPath checkpoints | S3/GCS through Ray `storage_path` |
| API keys in YAML | OIDC/SSO, a team mapping from groups |
| SQLite registry | Postgres |
| polling reconciler | K8s watch / informer, or a controller |
| ResourceQuota + FIFO admission queue in the API | Kueue with fair sharing and preemption |
| single kind node | managed K8s, separate node pools (CPU/GPU, spot) |

## 10. Key technology choices

| Choice | Alternative | Reason |
|---|---|---|
| KubeRay RayJob | `ray job submit` against a long-lived cluster | A cluster per job gives clean isolation per team, per-job resources and automatic teardown |
| FastAPI + SQLite | expose the K8s API directly | Hides K8s from users, enforces team scoping, keeps history |
| bert-tiny default | DistilBERT | Fits a 2-core CPU budget, and the platform mechanics are the point, not the model quality |
| Helm with pinned values | kube-prometheus-stack defaults | The full stack uses more than 2 GB RAM, while the minimal setup is about 0.9 GB |
