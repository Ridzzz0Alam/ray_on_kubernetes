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

## Phase 1: Cluster + hello-world RayJob (Sat morning) ✅

- [x] `infra/kind/cluster.yaml`: a single node using `KIND_NODE_IMAGE`, with a hostPath mount `./.shared → /shared`
- [x] Install the KubeRay operator with Helm (`KUBERAY_CHART_VERSION`, low resource requests)
- [x] `docker/Dockerfile`: `RAY_IMAGE` + CPU torch + transformers + datasets, then `kind load` it.
      **Start this build first.** It's the longest step
- [x] `infra/k8s/examples/hello-rayjob.yaml`: a trivial Ray script on 2 workers
- [x] `make up`, `make down` and `make image`

**Acceptance**
- [x] `make up && kubectl apply -f infra/k8s/examples/hello-rayjob.yaml` reaches `SUCCEEDED`
- [x] The cluster is cleaned up after the job (`shutdownAfterJobFinishes: true`)
- [x] Peak RAM for the whole stack stays under 5 GB (`docker stats`): measured 3.9 GiB peak, 1.0 GiB idle

## Phase 2: Submission API + SDK (Sat afternoon) ✅

- [x] FastAPI: `POST /jobs`, `GET /jobs/{id}`, `GET /jobs/{id}/logs`, `DELETE /jobs/{id}`, `GET /jobs`
- [x] Auth: `X-API-Key` → team → namespace (keys in a git-ignored config file)
- [x] Job registry in SQLite (id, team, spec, state, submitted_at, started_at, finished_at)
- [x] A reconciler loop that syncs RayJob status into the registry
- [x] Build the RayJob manifest from a Pydantic `JobSpec` (entrypoint, workers, cpu, memory, custom resources, retries)
- [x] SDK + `rkp` CLI: `submit | status | logs | cancel | list`
- [x] Unit tests with a fake K8s client

**Acceptance**
- [x] `rkp submit --team team-a workloads/hello.py` → RUNNING → SUCCEEDED
- [x] `rkp logs <id>` shows the driver logs. `rkp cancel <id>` makes the state CANCELLED
- [x] A team-b key cannot read or cancel team-a's jobs (403), and there's a test for this

**Result (2026-11-23):** `rkp submit` went SUBMITTED → PENDING (2s) → RUNNING (42s) → SUCCEEDED (62s), with
6 tasks spread over both workers. Node peak was 4.49 GiB. Cancelling a RUNNING job took it to CANCELLED, and
everything in `team-a` was gone 10s later. Team-b got 403 on GET, logs and DELETE. Tests: 43 unit, plus 2
`@pytest.mark.k8s` (`make test-k8s`, 1m45s).

## Phase 3: Distributed training with Ray Train (Sat afternoon) ✅

- [x] `workloads/train/train.py`: `TorchTrainer` with 2 workers and a CPU/gloo backend, `torch.set_num_threads(1)`
- [x] Profiles: `tiny` (default, `prajjwal1/bert-tiny`) and `distilbert` (opt-in)
- [x] Data: an Amazon reviews sentiment subset (about 4k train / 1k eval, max_len 128), cached to `/shared/data`
- [x] Report loss/accuracy each epoch with `ray.train.report`

**Acceptance**
- [x] `tiny` trains 2 epochs across 2 workers in under 10 min, with eval accuracy above 75%
- [x] Submitted through `rkp submit`

**Result (2026-11-23):** `rkp submit --team team-a workloads/train --script train.py -- --profile tiny --epochs 2`
reached SUCCEEDED in 2m27s: about 40s of pod start-up, then 97s of training at about 35s per epoch. Eval accuracy
was 85.1% after epoch 1 and 85.3% after epoch 2 (seed 0, the same in 4 runs). Peak memory, measured the way Ray
counts it (cgroup minus page cache): head 2.76 GiB, workers 1.33 GiB each, node 6.9 GiB.

## Phase 4: Lifecycle (checkpoint, resume, retry, cancel) (Sat evening) ✅

- [x] Checkpoint every epoch to `/shared/checkpoints/<team>/<job>` (`CheckpointConfig(num_to_keep=2)`)
- [x] Resume from `ray.train.get_checkpoint()`, restoring model, optimizer and epoch
- [x] `FailureConfig(max_failures=3)` for worker retries, and RayJob `backoffLimit` for job retries
- [x] `scripts/chaos_kill_worker.sh`: deletes a worker pod partway through training

**Acceptance**
- [x] Kill a worker during epoch 2. The job SUCCEEDS, and the logs show it resumed from the epoch-1 checkpoint
- [x] An entrypoint that always fails is retried N times and then marked FAILED
- [x] Cancelling during training leaves no Ray pods behind
- [x] This is covered by `make test-k8s`

## Phase 6: Multi-tenancy (Sun morning) ✅

Done before Phase 5 because Phase 7's dashboard needs this demo's data.

- [x] `make teams`: namespaces `team-a` and `team-b`, each with a `ResourceQuota` (2 CPU / 4Gi of requests) and a `LimitRange`
- [x] RBAC: the API's ServiceAccount can only manage `ray.io` resources in `team-*` namespaces
- [x] The API rejects or queues requests that go over quota, with a clear error
- [x] `scripts/noisy_neighbour.sh`: team-a goes over its quota, while team-b submits one job

**Acceptance**
- [x] Team-a's extra jobs wait or are rejected, while team-b's job is scheduled and SUCCEEDS
- [x] Recorded in `docs/demos/multi-tenancy.md` with commands and output
- [ ] (Stretch, skipped) Kueue `ClusterQueue`/`LocalQueue` per team

## Phase 5: Inference with Ray Serve (Sun afternoon) ✅

- [x] `workloads/serve/app.py`: loads the latest checkpoint and serves `POST /predict`
- [x] Deployed as a `RayService` via `POST /services`, autoscaling min 1 / max 3 on `target_ongoing_requests`
- [x] `scripts/load_test.py` (async httpx) ramps from 1 to 20 concurrent clients

**Acceptance**
- [x] Replicas scale from 1 to at least 2 under load, and back down afterwards. p50 latency is reported

## Phase 7: Observability + cost (Sun afternoon) ✅

- [x] Minimal Prometheus + Grafana via Helm (no Alertmanager or node-exporter, 6h retention)
- [x] Scrape Ray metrics, the API's `/metrics` and kube-state-metrics
- [x] API metrics: `rkp_jobs_total{team,state}`, `rkp_job_queue_seconds{team}`, `rkp_job_runtime_seconds{team}`
- [x] Grafana dashboard JSON in `dashboards/`
- [x] `rkp cost --since 7d`: CPU-hours and GB-hours per team × rates from `config/rates.yaml`

**Acceptance**
- [x] After the Phase 6 demo, the dashboard shows team-a's queue time going up and team-b's staying flat
- [x] Cost report numbers are unit-tested against the registry

## Phase 8: GPU awareness (Sun evening) ✅

- [x] Ray custom resource `{"sim_gpu": 1}` on a dedicated worker group, requested by the job with `resources_per_worker`
- [x] (Optional) The node extended resource `example.com/gpu`, so quotas limit "GPUs" per team
- [ ] (Optional, **not done**: about $2–5, owner approval required at the time) Same job on a short-lived cloud GPU node, with teardown in the same session

**Acceptance**
- [x] A sim_gpu job stays Pending until GPU-sim capacity frees up, and CPU jobs aren't blocked by it

## Phase 9: Polish (Sun evening)

- [x] `rkp serve create|status|list|delete` CLI; `create` retries the transient 409 "retry in a few seconds"

- [x] README: architecture diagram, a 5-minute quickstart (measured ~6 min), screenshots, results table
- [ ] `docs/demos/*.md` for each acceptance demo
- [ ] Turn the rough README case study into the full one: fill in §12 Results, remove 🚧 markers, add screenshots
- [ ] Update the resume bullets (below) with real numbers, but only for features that were actually built.
      Drafts:
      - Built a self-service ML platform on Kubernetes (KubeRay) with a FastAPI submission API and Python SDK,
        running distributed PyTorch training with Ray Train and autoscaled inference with Ray Serve.
      - Implemented checkpointing, automatic retries, cancellation and per-team resource quotas, recovering
        training from worker failures, with Prometheus/Grafana dashboards for job and resource usage.
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
| 2026-11-20 | Image `rkp-ray:2.59.0-1`: torch 2.15.0 (CPU index), transformers 5.22.2, datasets 5.1.0 | Newest releases at least 2 weeks old. CPU wheel index avoids about 2 GB of CUDA libraries. Tag suffix `-N` is bumped when deps change |
| 2026-11-20 | Node image pinned via `kind create --image`, not in `cluster.yaml` | Keeps `infra/versions.env` the single source of truth |
| 2026-11-23 | User code ships in a ConfigMap (owned by the RayJob) mounted on head + workers, not `runtimeEnvYAML.working_dir` | No upload step from the submitter pod. Deleting the RayJob garbage-collects it. Limit 512 KiB of flat files, enough for scripts |
| 2026-11-23 | Entrypoint is `env -C <code dir> <cmd>`, and shell metacharacters are rejected | KubeRay's submitter runs the entrypoint through a shell: `cd x && python y` ran only `cd` on Ray and the rest in the submitter pod |
| 2026-11-23 | Reconciler stores the driver logs in SQLite, then deletes the finished RayJob | Fixes the CRs left over in Phase 1 without losing logs |
| 2026-11-23 | `started_at` / `finished_at` come from `status.rayJobInfo`, not from poll time | A job shorter than the 5s poll still gets correct times, which queue-time metrics need |
| 2026-11-23 | Ray head memory 1.5 Gi → 2 Gi | At 1.5 Gi, Ray's memory monitor killed the JobSupervisor on the head (1.44 GB used) |
| 2026-11-23 | Team name = namespace (`team-a`). `--team` picks the client key, and the server only trusts the key | Simple mapping. Nothing in the request body can choose a namespace (extra fields → 422) |
| 2026-11-23 | `make data` downloads and tokenizes once inside the Ray image into `.shared/data`. Pods mount it read-only, and training runs offline | No network or HF downloads at pod start. Model and dataset revisions are pinned by commit SHA in `workloads/train/profiles.py` |
| 2026-11-23 | Every job gets `/shared/data` (read-only) and `/shared/checkpoints` = hostPath `/shared/checkpoints/<team>`, plus `RKP_JOB_ID` / `RKP_TEAM` / `RKP_STORAGE` | Ray Train V2 needs a `storage_path` shared by all nodes. A per-team subdir keeps teams out of each other's checkpoints. `make teams` creates the dirs as uid 1000 (= the image's `ray` user) |
| 2026-11-23 | Ray head memory 2 Gi → 3 Gi | At 2 Gi a training job was OOM-killed by Ray's memory monitor (1.91 GB). Measured during training: 2.69 GB anon, across 31 processes: driver + TrainController (each ~0.4 GB with torch), 4 Train V2 helper actors (~0.2 GB each, 0 CPU, so they land on the head) and ~10 dashboard module subprocesses. `ray start` can't disable dashboard modules |
| 2026-11-23 | Default worker memory 1 Gi → 1.5 Gi | A Ray Train worker with torch + transformers peaked at 1.33 GB |
| 2026-11-23 | `bert-tiny` lr 1e-3, batch 32, linear decay | It reached 85% on the first try. bert-tiny tolerates a high learning rate |
| 2026-11-23 | Python deps resolved with `uv lock --exclude-newer 2026-11-09` | Same "at least 2 weeks old" rule as the other pins. Ruff stays on 0.17.5 from Phase 0 |
| 2026-11-24 | Only rank 0 saves the checkpoint (`{model, optim, sched, epoch}` in one `state.pt`). Every rank still calls `ray.train.report` | DDP weights are identical on every rank. One copy halves the disk writes on the shared hostPath |
| 2026-11-24 | The retry count comes from KubeRay's `status.failed`: FAILED says `failed after N attempts: ...`, RETRYING says `attempt k of N failed` | The RayJob gets deleted once it finishes, so the registry message is the only place the attempt count survives |
| 2026-11-24 | `chaos_kill_worker.sh` triggers on the first `checkpoint_<ts>/` dir showing up in the host's `.shared` | It doesn't parse logs or depend on timing, and it reliably lands during epoch 2. The glob needs the trailing `/`, because Ray also writes `checkpoint_manager_snapshot.json` |
| 2026-11-24 | Whole-job admission + FIFO queue per team in the API (new state `QUEUED`), not K8s pod-level quota rejection | K8s checks quota per pod, and a RayJob is a gang: two half-admitted jobs (heads in, workers rejected) can deadlock a team's quota. A queued job has no K8s objects, its message says why it waits, and FIFO means big jobs aren't starved. Too big for the whole quota → 422 |
| 2026-11-24 | Team quota is 2 CPU / 4Gi of requests (ARCHITECTURE said 3Gi) | A default 2-worker training job requests 3272Mi. 4Gi fits it, but not two 1-worker jobs (2 × 2504Mi), which is what the demo needs. 2 teams × 4Gi stays under the node's 15 GB |
| 2026-11-24 | The admitter counts in-use quota from the registry (admitted, unfinished jobs), not the ResourceQuota's `status.used` | `status.used` lags: a just-created RayJob has no pods yet, so its requests show up seconds later and a second job would be over-admitted |
| 2026-11-24 | RBAC: one ClusterRole bound with RoleBindings only in team namespaces. The dev API keeps using the host kubeconfig | It gives the API no rights outside team namespaces. Running the API in-cluster as the SA is deployment work that's not in the plan; `make test-k8s` checks the rights with `kubectl auth can-i --as` |
| 2026-11-24 | Serve replicas autoscale inside a fixed worker group (1 × 2 CPU, 0.5 CPU per replica). No KubeRay in-tree pod autoscaler | Pod autoscaling adds an autoscaler sidecar and 30s+ pod starts, which the 7 GB laptop budget can't spare. Replica autoscaling is what the acceptance measures |
| 2026-11-24 | Serve autoscaling delays are 5s up and 30s down (Ray defaults: 30s / 600s) | So the demo scales up and back down within a minute or two |
| 2026-11-24 | Services count against the team quota but aren't queued: the API returns 409 when the quota is busy, and 422 when a service could never fit | A long-running service waiting in a queue behind jobs isn't useful. Counting it keeps the Phase 6 ceilings honest |
| 2026-11-24 | Serve code ships in a RayService-owned ConfigMap (`app.py` + `profiles.py`), not baked into the image | Same mechanism as job code: no image rebuild, and it's garbage-collected with the service |
| 2026-11-24 | Pins: prometheus chart 29.47.0 (Prometheus v3.16.0), grafana-community/grafana 14.4.1 (Grafana 13.3.1) | Newest releases at least 2 weeks old. `grafana/helm-charts` stopped publishing in Jan 2026; the chart moved to grafana-community |
| 2026-11-24 | API metrics are computed from the SQLite registry at scrape time (a custom collector), not counted in process | The registry is the source of truth. The numbers survive API restarts and match `rkp list`. `rkp_jobs_total` counts only finished (terminal) jobs so it stays monotonic. Live states are a separate gauge, `rkp_jobs_active` |
| 2026-11-24 | A second, metrics-only listener binds the kind network gateway (`make api` sets `RKP_METRICS_ADDR`). The API itself stays on 127.0.0.1 | Prometheus runs in kind and the API runs on the host. Binding the bridge address means pods can scrape it but the LAN can't |
| 2026-11-24 | Monitoring images are saved for the local platform only (`docker save --platform`) and loaded with `kind load image-archive`, with `pullPolicy: Never` | `kind load docker-image` imports `--all-platforms` and fails on multi-arch images under Docker's containerd image store. Without `Never` the node silently pulled the image at pod start |
| 2026-11-24 | Grafana limit 256Mi → 384Mi, with unified alerting and plugin pre-install off | Grafana 13 was OOM-killed at 256Mi during start-up. It idles at about 236 MB |
| 2026-11-24 | Cost = pod requests × time held (job: driver start → finish; service: create → delete), clipped to the window | It bills what a team reserves against its quota. Queued jobs hold nothing. Cluster start-up before driver start (~35s) isn't billed, because the registry has no admitted-at time |
| 2026-11-24 | Simulated GPU = node extended resource `example.com/gpu` (scheduler + quota) **and** Ray resource `sim_gpu` on a dedicated `gpu-sim` worker group, via `JobSpec.gpus` | Each job has its own RayCluster, so a Ray-only resource would never contend between jobs. Contention has to happen where real GPUs contend: in the K8s scheduler. `sim_gpu` keeps Ray placement honest |
| 2026-11-24 | GPU quota 1 per team, node capacity 1: overcommitted across teams on purpose | Within a team the API queues the 2nd GPU job. Across teams both are admitted and the scheduler serializes them (Pending), which is the acceptance scenario and how a shared GPU pool behaves |
| 2026-11-24 | The reconciler copies the scheduler's `PodScheduled=False` message into the job while PENDING, and clears it once the job runs | "Why is my job pending?" answered in `rkp status` without kubectl |
| 2026-11-24 | Demo worker sizes: 640Mi GPU workers, 512Mi CPU worker | A Ray worker node's own processes use ~0.45 GB. At 512Mi a task-running worker was OOM-killed in a loop by Ray's memory monitor. The sizes fit two jobs in team-b's 4Gi quota (2056 + 1992Mi) |
| 2026-11-25 | Ray heads start with `--no-monitor` | The autoscaler monitor (~115 MB private per head) is unused with fixed-size worker groups, and KubeRay probes only the dashboard agent. Brought the 3-cluster GPU scenario from 7.65 to 6.84 GiB peak in `make test-k8s`. The Ray Client server is also unused but can't be disabled (`ray start` rejects port 0) |
| 2026-11-25 | Admission also counts terminating pods of finished jobs and deleted services, using Kubernetes' own pod quota rule | Closes the quota hand-over gap: the API freed a job's quota when its RayJob was deleted, and the next job's pods were refused by the ResourceQuota (`exceeded quota … used 4040Mi, limited 4Gi`) until the old pods were gone. Counting actual pod requests (one pod list per team) beats a fixed delay. Side effect: a service created right after its training job finishes gets 409 "retry in a few seconds" for that moment instead of pods held back by the quota |
| 2026-11-27 | `rkp serve create` retries only the 409 that says "retry in a few seconds", for up to 120s | That 409 clears on its own once a finished job's pods are gone. Other 409s (job not SUCCEEDED, name taken, quota held by running work) and 422s need the user to act, so the CLI fails at once on those |
| 2026-11-27 | Screenshots via headless Chrome driven over the DevTools protocol (a small Node script), cropped the same way | The connected Chrome extension couldn't reach the laptop's localhost, there's no PIL or ImageMagick, and Chrome's `--screenshot` flag fires before the Ray dashboard renders |
