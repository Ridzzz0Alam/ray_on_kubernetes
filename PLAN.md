# PLAN.md

A **one-weekend** build plan (about 2 focused days). Each block ends with something that runs and can be demoed.
Work in order. A phase is done only when all of its **Acceptance** boxes are ticked and the
demo command has actually been run.

Hardware: 2 cores / 4 threads, 15 GB RAM, no GPU. Every phase must fit the budget in
[ARCHITECTURE.md §6](ARCHITECTURE.md#6-resource-budget). Expect about 1–2 h of the weekend to go to image
builds and pulls and CPU training runs, so start long-running steps first and write code while they run.

| Block | Phases | Result |
|---|---|---|
| Pre-work | 0 | Tools, Python env, CI ✅ |
| Sat morning | 1 | kind + KubeRay + Ray image + hello-world RayJob |
| Sat afternoon | 2, 3 | API + CLI, distributed training submitted via `rkp submit` |
| Sat evening | 4 | Checkpoint, kill-a-worker resume test, retries, cancel |
| Sun morning | 6 | Team namespaces + quotas, noisy-neighbour demo |
| Sun afternoon | 5, 7 | Ray Serve autoscaling, Grafana dashboard, cost report |
| Sun evening | 8 (local), 9 | Simulated GPU scheduling, README + demo docs |

**If time runs short, cut in this order:** cloud GPU run → Flyte pipeline → Kueue → Grafana
dashboard polish (keep the raw metrics and the cost report) → Serve load-test graphs.

---

## Phase 0: Bootstrap (pre-work) ✅

- [x] Install `kind`, `kubectl` and `helm` into `~/.local/bin` with pinned versions and checksum checks (`make tools`)
- [x] Set up the uv project (`pyproject.toml`, Python 3.12, ruff, pytest) and the `Makefile` skeleton
- [x] Add a GitHub Actions workflow that runs `make check` + shellcheck (no cluster in CI)
- [x] Record pinned versions in `infra/versions.env`, with a test that rejects floating tags

**Acceptance**
- [x] `make check` passes locally (5 tests). CI runs on push
- [x] `kind version`, `kubectl version --client` and `helm version` all work

## Phase 1: Cluster + hello-world RayJob (Sat morning)

- [ ] `infra/kind/cluster.yaml`: a single node using `KIND_NODE_IMAGE`, with a hostPath mount `./.shared → /shared`
- [ ] Install the KubeRay operator with Helm (`KUBERAY_CHART_VERSION`, low resource requests)
- [ ] `docker/Dockerfile`: `RAY_IMAGE` + CPU torch + transformers + datasets, then `kind load` it.
      **Start this build first.** It's the longest step
- [ ] `infra/k8s/examples/hello-rayjob.yaml`: a trivial Ray script on 2 workers
- [ ] `make up`, `make down` and `make image`

**Acceptance**
- [ ] `make up && kubectl apply -f infra/k8s/examples/hello-rayjob.yaml` reaches `SUCCEEDED`
- [ ] The cluster is cleaned up after the job (`shutdownAfterJobFinishes: true`)
- [ ] Peak RAM for the whole stack stays under 5 GB (`docker stats`)

## Phase 2: Submission API + SDK (Sat afternoon)

- [ ] FastAPI: `POST /jobs`, `GET /jobs/{id}`, `GET /jobs/{id}/logs`, `DELETE /jobs/{id}`, `GET /jobs`
- [ ] Auth: `X-API-Key` → team → namespace (keys in a git-ignored config file)
- [ ] Job registry in SQLite (id, team, spec, state, submitted_at, started_at, finished_at)
- [ ] A reconciler loop that syncs RayJob status into the registry
- [ ] Build the RayJob manifest from a Pydantic `JobSpec` (entrypoint, workers, cpu, memory, custom resources, retries)
- [ ] SDK + `rkp` CLI: `submit | status | logs | cancel | list`
- [ ] Unit tests with a fake K8s client

**Acceptance**
- [ ] `rkp submit --team team-a workloads/hello.py` → RUNNING → SUCCEEDED
- [ ] `rkp logs <id>` shows the driver logs. `rkp cancel <id>` makes the state CANCELLED
- [ ] A team-b key cannot read or cancel team-a's jobs (403), and there's a test for this

## Phase 3: Distributed training with Ray Train (Sat afternoon)

- [ ] `workloads/train/train.py`: `TorchTrainer` with 2 workers and a CPU/gloo backend, `torch.set_num_threads(1)`
- [ ] Profiles: `tiny` (default, `prajjwal1/bert-tiny`) and `distilbert` (opt-in)
- [ ] Data: an Amazon reviews sentiment subset (about 4k train / 1k eval, max_len 128), cached to `/shared/data`
- [ ] Report loss/accuracy each epoch with `ray.train.report`

**Acceptance**
- [ ] `tiny` trains 2 epochs across 2 workers in under 10 min, with eval accuracy above 75%
- [ ] Submitted through `rkp submit`

## Phase 4: Lifecycle (checkpoint, resume, retry, cancel) (Sat evening)

- [ ] Checkpoint every epoch to `/shared/checkpoints/<team>/<job>` (`CheckpointConfig(num_to_keep=2)`)
- [ ] Resume from `ray.train.get_checkpoint()`, restoring model, optimizer and epoch
- [ ] `FailureConfig(max_failures=3)` for worker retries, and RayJob `backoffLimit` for job retries
- [ ] `scripts/chaos_kill_worker.sh`: deletes a worker pod partway through training

**Acceptance**
- [ ] Kill a worker during epoch 2. The job SUCCEEDS, and the logs show it resumed from the epoch-1 checkpoint
- [ ] An entrypoint that always fails is retried N times and then marked FAILED
- [ ] Cancelling during training leaves no Ray pods behind
- [ ] This is covered by `make test-k8s`

## Phase 6: Multi-tenancy (Sun morning)

Done before Phase 5 because Phase 7's dashboard needs this demo's data.

- [ ] `make teams`: namespaces `team-a` and `team-b`, each with a `ResourceQuota` (e.g. 2 CPU / 3Gi) and a `LimitRange`
- [ ] RBAC: the API's ServiceAccount can only manage `ray.io` resources in `team-*` namespaces
- [ ] The API rejects or queues requests that go over quota, with a clear error
- [ ] `scripts/noisy_neighbour.sh`: team-a goes over its quota, while team-b submits one job

**Acceptance**
- [ ] Team-a's extra jobs wait or are rejected, while team-b's job is scheduled and SUCCEEDS
- [ ] Recorded in `docs/demos/multi-tenancy.md` with commands and output
- [ ] (Stretch) Kueue `ClusterQueue`/`LocalQueue` per team

## Phase 5: Inference with Ray Serve (Sun afternoon)

- [ ] `workloads/serve/app.py`: loads the latest checkpoint and serves `POST /predict`
- [ ] Deployed as a `RayService` via `POST /services`, autoscaling min 1 / max 3 on `target_ongoing_requests`
- [ ] `scripts/load_test.py` (async httpx) ramps from 1 to 20 concurrent clients

**Acceptance**
- [ ] Replicas scale from 1 to at least 2 under load, and back down afterwards. p50 latency is reported

## Phase 7: Observability + cost (Sun afternoon)

- [ ] Minimal Prometheus + Grafana via Helm (no Alertmanager or node-exporter, 6h retention)
- [ ] Scrape Ray metrics, the API's `/metrics` and kube-state-metrics
- [ ] API metrics: `rkp_jobs_total{team,state}`, `rkp_job_queue_seconds{team}`, `rkp_job_runtime_seconds{team}`
- [ ] Grafana dashboard JSON in `dashboards/`
- [ ] `rkp cost --since 7d`: CPU-hours and GB-hours per team × rates from `config/rates.yaml`

**Acceptance**
- [ ] After the Phase 6 demo, the dashboard shows team-a's queue time going up and team-b's staying flat
- [ ] Cost report numbers are unit-tested against the registry

## Phase 8: GPU awareness (Sun evening)

- [ ] Ray custom resource `{"sim_gpu": 1}` on a dedicated worker group, requested by the job with `resources_per_worker`
- [ ] (Optional) The node extended resource `example.com/gpu`, so quotas limit "GPUs" per team
- [ ] (Optional, about $2–5, **owner approval required at the time**) Same job on a short-lived cloud GPU node, with teardown in the same session

**Acceptance**
- [ ] A sim_gpu job stays Pending until GPU-sim capacity frees up, and CPU jobs aren't blocked by it

## Phase 9: Polish (Sun evening)

- [ ] README: architecture diagram, a 5-minute quickstart, screenshots, results table
- [ ] `docs/demos/*.md` for each acceptance demo
- [ ] Update the resume bullets with real numbers, but only for features that were actually built
- [ ] (Stretch) Flyte or a simple `train → evaluate → deploy` pipeline that promotes a model only if accuracy ≥ the threshold

---

## Out of scope (on purpose)

Real auth (OIDC), HA control plane, multi-node kind, a service mesh and MinIO. These are listed as
production follow-ups in ARCHITECTURE.md §9.

## Decision log

| Date | Decision | Why |
|---|---|---|
| 2026-11-14 | Single-node kind, hostPath for checkpoints | 2-core / 15 GB laptop. MinIO would cost about 500 MB RAM for no demo value |
| 2026-11-14 | Default model `bert-tiny`, DistilBERT opt-in | DistilBERT on 2 CPU cores is too slow for fast iteration |
| 2026-11-14 | API runs on the host (uvicorn) in dev | Saves RAM and gives a faster edit loop |
| 2026-11-14 | SQLite job registry next to the RayJob CRs | Keeps history after CRs are deleted, and is needed for queue-time and cost metrics |
| 2026-11-14 | Compressed to a one-weekend plan | The owner wants it done in about 2 days. Phase 6 moved before 5 to feed the Phase 7 dashboard |
| 2026-11-14 | Pins: kind v0.33.0 (node v1.37.0), kubectl v1.37.2, Helm v4.3.1, KubeRay 1.7.2, Ray 2.59.0 / py3.12 | Prefer releases at least 2 weeks old. Skipped Helm 4.3.2 (1 day old) and Ray 2.60.0 (10 days old) |
