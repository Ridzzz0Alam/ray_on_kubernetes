# PLAN.md

A phased build plan for the Ray-on-Kubernetes ML platform. Each phase ends with something that runs and can be demoed.
Do phases in order. A phase is done only when all of its **Acceptance** boxes are ticked
and the demo command has actually been run.

Hardware: 2 cores / 4 threads, 15 GB RAM, no GPU. Every phase must fit the budget in
[ARCHITECTURE.md §6](ARCHITECTURE.md#6-resource-budget). Rough total: 3–5 weekends.

---

## Phase 0: Bootstrap (½ evening)

- [ ] Install `kind`, `kubectl` and `helm` into `~/.local/bin` with pinned versions (`make tools`)
- [ ] Set up the uv project (`pyproject.toml`, Python 3.12, ruff, pytest) and the `Makefile` skeleton
- [ ] Add a GitHub Actions workflow that runs `make lint test` (no cluster in CI)
- [ ] Record pinned versions in `infra/versions.env` (kind node image, KubeRay, Ray, Helm charts)

**Acceptance**
- [ ] `make lint test` passes locally and in CI
- [ ] `kind version`, `kubectl version --client` and `helm version` all work

## Phase 1: Cluster + hello-world RayJob (weekend 1, day 1)

- [ ] `infra/kind/cluster.yaml`: a single node, with a hostPath mount `./.shared → /shared` for checkpoints and data
- [ ] Install the KubeRay operator with Helm, using the pinned chart and low resource requests
- [ ] `docker/Dockerfile`: `rayproject/ray:<pinned>-py312-cpu` + CPU torch + transformers + datasets, then `kind load` it
- [ ] A hand-written `RayJob` YAML that runs a trivial Ray script (sums numbers on 2 workers)
- [ ] `make up`, `make down` and `make image`

**Acceptance**
- [ ] `make up && kubectl apply -f infra/k8s/examples/hello-rayjob.yaml` reaches `SUCCEEDED`
- [ ] The cluster is cleaned up after the job (`shutdownAfterJobFinishes: true`)
- [ ] Peak RAM for the whole stack stays under 5 GB (check with `docker stats`)

## Phase 2: Submission API + SDK (weekend 1, day 2)

- [ ] FastAPI service: `POST /jobs`, `GET /jobs/{id}`, `GET /jobs/{id}/logs`, `DELETE /jobs/{id}`, `GET /jobs?team=`
- [ ] Auth: an `X-API-Key` header that maps to a team, and the team maps to a namespace (keys in a local config file, git-ignored)
- [ ] Job registry in SQLite (id, team, spec, state, submitted_at, started_at, finished_at)
- [ ] A reconciler loop that syncs RayJob status into the registry
- [ ] Build the RayJob manifest from a Pydantic `JobSpec` (entrypoint, workers, cpu, memory, custom resources)
- [ ] Python SDK (`sdk/`) and an `rkp` CLI: `rkp submit|status|logs|cancel|list`
- [ ] Unit tests with a fake K8s client

**Acceptance**
- [ ] `rkp submit --team team-a workloads/hello.py` → `rkp status` shows RUNNING then SUCCEEDED
- [ ] `rkp logs <id>` streams driver logs. `rkp cancel <id>` stops the job and the state becomes CANCELLED
- [ ] A team-b key cannot read or cancel team-a's jobs (403), and there's a test for this

## Phase 3: Distributed training with Ray Train (weekend 2, day 1)

- [ ] `workloads/train/train.py`: `TorchTrainer` with 2 workers and a CPU/gloo backend
- [ ] Model profiles: `tiny` (default, `prajjwal1/bert-tiny`) and `distilbert` (opt-in)
- [ ] Data: an Amazon reviews sentiment subset (about 4k train / 1k eval, max_len 128), cached once to `/shared/data`
- [ ] Report loss/accuracy each epoch with `ray.train.report`

**Acceptance**
- [ ] The `tiny` profile trains 2 epochs across 2 workers in under 10 min, with eval accuracy above 75%
- [ ] Submitted through `rkp submit`, not by hand

## Phase 4: Lifecycle (checkpoint, resume, retry, cancel) (weekend 2, day 2)

- [ ] Checkpoint every epoch to `/shared/checkpoints/<team>/<job>` (`RunConfig(storage_path=...)`, keep the last 2)
- [ ] Resume: on start, load `ray.train.get_checkpoint()` and continue from the saved epoch and optimizer state
- [ ] Worker-level retries: `FailureConfig(max_failures=3)`
- [ ] Job-level retries: RayJob `backoffLimit`, exposed as `retries` in `JobSpec`
- [ ] `scripts/chaos_kill_worker.sh`: deletes one worker pod partway through training

**Acceptance**
- [ ] Chaos test: kill a worker during epoch 2. The job still SUCCEEDS, and the logs show it resumed from the epoch-1 checkpoint, not epoch 0
- [ ] An entrypoint that always fails is retried N times and then marked FAILED
- [ ] Cancelling during training leaves no Ray pods behind
- [ ] This is an automated integration test (`make test-k8s`)

## Phase 5: Inference with Ray Serve (weekend 3, day 1)

- [ ] `workloads/serve/app.py`: loads the latest checkpoint and serves `POST /predict`
- [ ] Deploy as a `RayService` through the API (`POST /services`)
- [ ] Autoscaling: `min_replicas: 1`, `max_replicas: 3`, set on `target_ongoing_requests`
- [ ] `scripts/load_test.py` (async httpx) ramps from 1 to 20 concurrent clients

**Acceptance**
- [ ] Under load the replicas scale from 1 to at least 2, and they scale back down when the load stops (capture this in the Ray dashboard or Grafana)
- [ ] p50 latency for the `tiny` model is reported

## Phase 6: Multi-tenancy (weekend 3, day 2)

- [ ] `make teams`: namespaces `team-a` and `team-b`, a `ResourceQuota` for each (e.g. 2 CPU / 3Gi), plus a `LimitRange`
- [ ] RBAC: the API's ServiceAccount can only manage Ray CRs in team namespaces
- [ ] The API checks the request against the remaining quota and rejects or queues it with a clear error
- [ ] `scripts/noisy_neighbour.sh`: team-a submits more jobs than its quota allows, and team-b submits one job

**Acceptance**
- [ ] Team-a's extra jobs wait or are rejected, while team-b's job is scheduled and SUCCEEDS
- [ ] The demo is recorded in `docs/demos/multi-tenancy.md` with commands and output
- [ ] (Stretch) Replace the hand-rolled queueing with **Kueue** `ClusterQueue`/`LocalQueue` per team

## Phase 7: Observability + cost (weekend 4, day 1)

- [ ] Minimal Prometheus + Grafana via Helm (no Alertmanager or node-exporter, 6h retention)
- [ ] Scrape Ray metrics (PodMonitor), the API's `/metrics` and kube-state-metrics (quota usage)
- [ ] API metrics: `rkp_jobs_total{team,state}`, `rkp_job_queue_seconds{team}` (histogram), `rkp_job_runtime_seconds{team}`
- [ ] A Grafana dashboard JSON in `dashboards/`: job states, queue time, CPU/mem per team, quota usage
- [ ] `rkp cost --since 7d`: CPU-hours and GB-hours per team × configurable rates → a table/CSV

**Acceptance**
- [ ] After the Phase 6 demo, the dashboard shows queue time going up for team-a and staying flat for team-b
- [ ] The cost report numbers match the registry runtimes × requested resources (unit-tested)

## Phase 8: GPU awareness (weekend 4, day 2)

Local (free):
- [ ] Add the Ray custom resource `{"sim_gpu": 1}` to a dedicated worker group, so a job requests `resources_per_worker={"sim_gpu": 1}`
- [ ] (Optional) Patch the node with the extended resource `example.com/gpu: 2` so K8s quotas can limit "GPUs" per team

Cloud (optional, about $2–5, **needs owner approval at the time**):
- [ ] A script that creates a short-lived GKE/EKS cluster with one spot GPU node and runs the same RayJob with `num_gpus=1`
- [ ] A teardown script, run in the same session, with the deletion verified

**Acceptance**
- [ ] A sim_gpu job stays Pending until the GPU worker group has capacity, and CPU jobs aren't blocked by it
- [ ] (If cloud) The cost and runtime are recorded in `docs/demos/gpu.md`, and all cloud resources are deleted

## Phase 9: Polish (weekend 5)

- [ ] README: architecture diagram, a 5-minute quickstart, demo GIFs/screenshots, and the results table
- [ ] `docs/demos/*.md` for each acceptance demo
- [ ] Only now update the resume bullets with real numbers
- [ ] (Stretch) Flyte or a simple `train → evaluate → deploy` pipeline that promotes a model only if accuracy ≥ the threshold

---

## Out of scope (on purpose)

- Real auth (OIDC), HA control plane, multi-node kind, a service mesh, and object storage (MinIO). These are noted
  as production follow-ups in ARCHITECTURE.md §9 and aren't built here, because of the hardware limits.

## Decision log

| Date | Decision | Why |
|---|---|---|
| 2026-11-14 | Single-node kind, hostPath for checkpoints | 2-core / 15 GB laptop. MinIO would cost about 500 MB RAM for no demo value |
| 2026-11-14 | Default model `bert-tiny`, DistilBERT opt-in | DistilBERT on 2 CPU cores takes too long per epoch for fast iteration |
| 2026-11-14 | API runs on the host (uvicorn) in dev, in-cluster later | Saves RAM and gives a faster edit loop |
| 2026-11-14 | SQLite job registry next to the RayJob CRs | Keeps history after CRs are deleted, and is needed for queue-time and cost metrics |
