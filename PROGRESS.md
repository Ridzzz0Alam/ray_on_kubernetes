# PROGRESS.md: session handoff log

The handoff file between work sessions (human or agent). **Read it first and update it last.**
Newest entry on top. Keep each entry short: what was done, current state, next step, gotchas.
Long-term plan → [PLAN.md](PLAN.md). Design → [ARCHITECTURE.md](ARCHITECTURE.md). Rules → [AGENTS.md](AGENTS.md).

---

## 2026-11-27: Phase 9 started: `rkp serve` CLI

**Done**
- `rkp serve create NAME JOB_ID | status NAME | list | delete NAME` (`sdk/cli.py`). The SDK adds `Client.serve/service/services/unserve/watch_service`.
- `create` retries only the 409 containing "retry in a few seconds" (`TRANSIENT_BUSY`), every 3s for up to `--retry-for` (default 120s).
  Any other 409 or 422 fails at once. `--wait` (the default) follows DEPLOYING → RUNNING and exits 1 on UNHEALTHY.
- 4 new CLI tests (110 unit tests in total). Live run on kind: 1-epoch tiny job SUCCEEDED in 110s, then `rkp serve create` reached
  RUNNING in 45s, `/predict` → positive 0.926, then delete. That live run did **not** hit the 409 (the pods were already gone),
  so the retry is covered by the unit test and the existing e2e test.
- Docs: serving demo, README quickstart, ARCHITECTURE §3.2, PLAN Phase 9 checkbox + decision log.

**State:** committed, not pushed. The cluster is deleted and nothing is running. `make test-k8s` was not re-run (no API change).

**Next:** the rest of Phase 9: README architecture diagram, 5-minute quickstart, screenshots, results table, §12 Results, remove the 🚧 markers,
and resume bullets with real numbers.

---

## 2026-11-25: Phase 8 follow-ups (memory budget, quota hand-over gap). Session paused here

**Done**
- **Memory budget:** Ray heads start with `--no-monitor` (`HEAD_START_PARAMS` in `platform_api/manifests.py`). Measured per process, a tiny
  job's head uses ~1.4 GB private memory, and the autoscaler monitor (~115 MB) is unused with fixed-size worker groups.
  Full `make test-k8s` peak: **7.65 → 6.84 GiB** (monitoring off; the peak is in the GPU test). `scripts/gpu_contention.sh 90`: 5.71 GiB.
- **Quota hand-over gap:** `Admitter.usage()` = registry-counted admitted work + requests of rkp pods still terminating for finished jobs or
  deleted services (`KubeClient.pod_requests`, same rule as K8s' pod quota evaluator: not counted once Succeeded/Failed or past
  deletionTimestamp + grace). Messages say how much is "from finished work whose pods are still terminating".
  A service created right after its training job finishes now gets **409 "retry in a few seconds"** for that moment.
- The noisy-neighbour e2e test asserts the queued job gets no `exceeded quota` events. Without the fix it fails
  (`used: requests.memory=4040Mi, limited: 4Gi`), and with it it passes. The serve e2e test retries the 409.
- Tests: 106 unit tests, **17/17 `make test-k8s`** (17m17s).

**State:** everything is committed (not pushed). The cluster is deleted and nothing is running.

**Next:** Phase 9 (polish: README quickstart, architecture diagram, screenshots, results table, `rkp serve` CLI, which should retry
the 409 "retry in a few seconds"). The cloud GPU run is still optional and needs owner approval.

**Gotchas**
- `ray start --ray-client-server-port=0` crashes the head (Ray only accepts ports 1024–65535), so the unused Ray Client server (~112 MB) stays.
  Trimming dashboard modules is only possible through the dashboard's own argument, not `ray start`.
- Plain `uv run pytest` also runs the k8s tests. Use `make test`. `cmd | tail` in a background run hides the exit code, so use `; echo rc=$?`.
- Measure node memory with `docker exec rkp-control-plane` reading `memory.current − active_file − inactive_file` from the cgroup.

---

## 2026-11-24: Phase 8 complete (GPU awareness, simulated). Session paused here

**Done**
- Simulated GPU: `make gpu-sim` (called by `make teams`) patches the node's capacity to `example.com/gpu: 1` and waits for allocatable.
  `JobSpec.gpus` (0–1) adds a `gpu-sim` worker group (requests and limits `example.com/gpu: 1`, Ray `{"sim_gpu": 1}`). `workers` may now be 0.
  `sim_gpu` is reserved in `resources`.
- Team quotas gain `requests.example.com/gpu: "1"`. `Resources.gpu` is part of admission: a team's 2nd GPU job is QUEUED in the API, and across
  teams the scheduler holds the second gpu-sim pod Pending.
- The reconciler puts the scheduler's PodScheduled=False message on PENDING jobs (`waiting for scheduling: … Insufficient example.com/gpu`)
  and clears it once the job runs.
- `workloads/gpu_sim.py`, `train.py --sim-gpu`, `rkp submit --gpus`, `scripts/gpu_contention.sh`, a Grafana "Simulated GPUs: used vs quota" panel,
  and `docs/demos/gpu-awareness.md`.
- **Screenshots now live in `docs/screenshots/phase-N/`.** Phase 7's were moved there, and Phase 8 adds 4 (2 Grafana, 2 Ray dashboard).
  The owner is writing a report later: keep capturing per phase.
- Tests: 102 unit tests and **17/17 `make test-k8s`** (16m27s), including the new GPU contention test.

**Measured:** team-b's GPU job waited 150s Pending while team-a held the GPU, and team-b's CPU job finished in 36s meanwhile. Sim-GPU training
(1 epoch, 1 worker on gpu-sim) reached 84.6%. The cloud GPU run was **not done** (needs owner approval).

**State:** everything is pushed. The cluster is deleted and nothing is running. `.shared/` keeps the data and checkpoints (git-ignored).

**Open items for next session**
1. **Memory over budget in the GPU scenario:** 3 small Ray clusters at once peaked at **7.65 GiB with monitoring off** (the `make test-k8s` GPU test).
   Options: slim the Ray head for non-training jobs, or restructure the test/demo so only 2 clusters overlap.
2. Quota hand-over gap (from Phase 7): count a finished job until its pods are gone.
3. Then **Phase 9** (polish: README quickstart, architecture diagram, screenshots, results table, `rkp serve` CLI).

**Gotchas**
- A Ray worker node's own processes use ~0.45 GB. At 512Mi a task-running worker gets OOM-killed by Ray's memory monitor in a retry loop, and the job
  shows RUNNING forever. Use ≥640Mi for workers that run tasks.
- Node allocatable for a patched extended resource appears only on the kubelet's next status sync (10–30s). `make gpu-sim` waits for it.
- Agreed with the owner: **monitoring off during training** (and the GPU demo).
- `pgrep -f`/`pkill -f` from an agent shell match the shell itself (exit 144). Kill by PID from a separate `pgrep -af` listing.

---

## 2026-11-24: Phase 7 complete (observability + cost)

**Done**
- `make monitoring` / `monitoring-down` / `grafana`: prometheus chart 29.47.0 (server + kube-state-metrics only, 30s, 6h, emptyDir;
  chart default jobs disabled via its `scrapeConfigs` map) and grafana-community/grafana 14.4.1, with the dashboard provisioned from
  `dashboards/rkp.json`. Images are `docker save --platform` → `kind load image-archive`, with `pullPolicy: Never`.
- `platform_api/metrics.py`: a registry-backed collector (`rkp_jobs_total`, `rkp_jobs_active`, queue/runtime histograms,
  `rkp_job_last_queue_seconds`, `rkp_job_oldest_waiting_seconds`, `rkp_services_live`). It's served on `GET /metrics`, plus on a
  metrics-only listener at `RKP_METRICS_ADDR` (`make api` sets the kind gateway, `172.18.0.1:9108`).
- `platform_api/cost.py` + `GET /cost?since=` + `rkp cost`. Rates are in `config/rates.yaml` (committed).
- `docs/demos/observability.md` with 2 dashboard screenshots (`docs/demos/img/`).
- Tests: 92 unit tests (cost arithmetic against registry rows, metric values, the CLI table, and a check that the dashboard only uses exported metrics).
  `make test-k8s` was **not** re-run this phase (no change to K8s paths); the live demo, a training run and a serving run all went through the new API.

**Measured:** in Prometheus, team-a's last queue time went 34 → 141 → 246s and team-b's stayed at 33s. Every dashboard query returned data.
Memory: monitoring idles at ~0.3 GB (Grafana 236 MB), the cluster + monitoring idles at 1.74 GiB, the 2-team demo + monitoring peaked at 4.53 GiB,
and **training + monitoring peaked at 7.01 GiB** (the budget is 7).

**State:** the cluster is deleted, and the API, port-forwards and samplers are stopped. `.cache/` holds the rendered values and image list (git-ignored).

**Next: Phase 8** (GPU awareness: a `gpu-sim` worker group with the Ray custom resource `sim_gpu`). Cloud GPU runs need explicit approval (AGENTS.md §4).

**Gotchas**
- Use `ray_serve_autoscaling_target_replicas` for the replica count. Summing `ray_serve_deployment_replica_healthy` undercounts.
- Quota hand-over gap: the admitter frees a finished job's quota when its RayJob is deleted, but its pods take a few seconds to terminate, and
  the K8s quota still counts them. New pods get held back briefly by the backstop (visible on the dashboard as "used" touching 4 GiB). The fix is to
  count a job until its pods are gone.
- prometheus chart ≥ 26 builds `scrape_configs` from the `scrapeConfigs` map. Overriding `serverFiles.prometheus.yml` duplicates jobs and crashes.
- Grafana 13 needs >256Mi at start-up.
- The agent safety check blocks `rm` inside heredocs and compound commands. Use file tools or `truncate`.

---

## 2026-11-24: Phase 5 complete (Ray Serve)

**Done**
- `workloads/serve/app.py`: Serve deployment `Sentiment` (built with `app:build`) that loads the latest `checkpoint_<ts>/state.pt` of a run.
  `POST /predict` takes `{"text"}` or `{"texts": [≤32]}`, `GET /` returns checkpoint info, and bad input → 422.
- API: `POST/GET/DELETE /services` (+ `GET /services/{name}`). The job must be the team's own and SUCCEEDED. Services are stored in a new
  `services` registry table, and their state is read live from the RayService (`observe_service`). Services count in `Admitter.in_use`:
  busy quota → 409, never fits → 422.
- `manifests.py`: a shared `_ray_cluster()` builds both RayJob and RayService. `build_rayservice` has a read-only checkpoint mount and a
  RayService-owned code ConfigMap.
- `scripts/load_test.py`: closed-loop async httpx stages, replica count from the dashboard's `/api/serve/applications/`. Exits 0 only if it scaled up *and* down.
- `docs/demos/serving.md`, plus the serve e2e test (train 1 epoch → serve → predict → delete → no pods).
- Tests: 71 unit tests; `make test-k8s`: 16 tests, all passing, 13m34s.

**Measured:** replicas went 1 → 3 under load (2 at +42s with 10 clients, 3 at +55s) and back to 1 38s after the load stopped. 0 errors in 8,948 requests.
p50 was 12 ms at 1 client and 210 ms at 20. **Throughput was flat at about 90 req/s:** `ray::ProxyActor` (the head's Serve HTTP proxy) sat at 100% of a core,
the head container at 98% of its 1-CPU limit, and the replicas at 27–36% each. The node peaked at 5.06 GiB while serving.
Service creation → RUNNING took about 35s.

**State:** the cluster is deleted, and the API and port-forwards are stopped. `.shared/checkpoints/team-a/e31235ff/` (2-epoch run) stays, so serving can reuse it.

**Next: Phase 7** (observability + cost): minimal Prometheus + Grafana, API `/metrics`, the dashboard, and `rkp cost`.
The registry already has everything cost needs: job timestamps and spec requests, plus `services.created_at/deleted_at`.

**Gotchas**
- KubeRay creates `<name>-serve-svc` only once the app is ready. Use the stable `<name>-head-svc` for the dashboard (8265), not the
  `<cluster>-head-svc` with the random suffix.
- The API's dev port is 8000, so port-forward Serve to **8001**.
- There's no CLI for services yet (only the HTTP API). The SDK/CLI could get `rkp serve` in Phase 9 polish.
- The serve app needs a Phase 4+ checkpoint. Phase 3 runs have none, and you get UNHEALTHY with "no checkpoint_*/".
- `pkill -f <pattern>` from an agent shell matches its own bash and exits 144. Check with `pgrep` and `kill` the PIDs.

---

## 2026-11-24: Phase 6 complete (multi-tenancy)

**Done**
- `infra/k8s/teams/quotas.yaml`: a `ResourceQuota` per team (2 CPU / 4Gi of requests, 3 RayJobs) plus a `LimitRange`. `rbac.yaml`
  adds the `rkp-system/rkp-api` SA with a ClusterRole bound only in team namespaces. `make teams` applies all of it.
- `platform_api/admission.py`: whole-job admission. A job is admitted only if all its pod requests fit (`manifests.job_requests`).
  Otherwise it's **QUEUED** (new state, no K8s objects) with a "waiting for quota" message, and the reconciler admits FIFO per team.
  A job bigger than the whole quota → **422**. Cancelling a QUEUED job touches nothing in K8s.
- `scripts/noisy_neighbour.sh` (+ `scripts/job_table.py`), `workloads/sleep.py`, `docs/demos/multi-tenancy.md`.
- Tests: 55 unit tests and 15 k8s tests (9 RBAC `can-i` cases, plus a noisy-neighbour test), all passing. `make test-k8s` takes 10m58s.

**Measured (demo, 3 team-a jobs + 1 team-b):** team-a's queue time was 34s → 137s → 244s. Team-b's stayed at 34s and it SUCCEEDED while 2 team-a
jobs were still queued. Node peak with 2 clusters at once: **5.20 GiB** (excluding page cache).

**State:** the cluster is deleted, the API is stopped, and the dev registry `.rkp/` is deleted.

**Next: Phase 5** (Ray Serve: `workloads/serve/app.py` loads the latest checkpoint, `POST /services` → RayService, autoscaling 1→3,
`scripts/load_test.py`). Serving needs a trained checkpoint, so run a training job first. Its 2 workers fit the team quota.

**Gotchas**
- The admitter counts in-use quota from the registry, not `ResourceQuota.status.used` (that lags until pods exist).
  Anything that creates pods in a team namespace outside the API isn't counted, but K8s still enforces the quota per pod.
- `started_at` is the Ray driver start, so "queue time" includes about 34s of cluster start-up even with no queueing.
- The dev API still uses the host kubeconfig. The SA's rights are only checked by `kubectl auth can-i --as` in `make test-k8s`.
- Phase 5's `RayService` will need admission too (a serving cluster takes quota). It isn't covered yet: `POST /services` should go through `Admitter`.

---

## 2026-11-24: Phase 4 complete (checkpoint, resume, retry, cancel)

**Done**
- `train.py`: rank 0 saves `{model, optim, sched, epoch}` to `state.pt` each epoch through `ray.train.report(..., checkpoint=)`.
  It uses `CheckpointConfig(num_to_keep=2)` and `FailureConfig(max_failures=3)`. On start it restores from `get_checkpoint()` and prints
  `[rank 0] resumed from checkpoint at epoch N`.
- Reconciler: the FAILED message reads `failed after N attempts: …` (from KubeRay `status.failed`), and RETRYING reads `attempt k of N failed`.
- `scripts/chaos_kill_worker.sh <team> <job-id>` waits for the first `checkpoint_<ts>/` dir in `.shared`, then deletes one worker pod.
- `make test-k8s` now has 5 tests. The new ones are worker-kill resume, an always-failing job with `retries=2`, and cancel during training with no pods left.
- Tests: 46 unit tests, 5 k8s tests, all passing. `make test-k8s` takes **9m36s**.

**Measured (chaos run):** the kill landed during epoch 2. Rank 1 got a gloo "connection reset", and the controller logged
"Error count: 1 (max allowed: 3)". It restarted the group and resumed at epoch 1, then epoch 2 finished at 85.6%. Submit → SUCCEEDED in about 2m50s
(about 2m27s without the kill). I didn't measure memory during the recovery.

**State:** the cluster is deleted. `.shared/checkpoints/team-a/` still has 6 run dirs from Phase 3 (git-ignored, safe to delete).

**Next: Phase 6** (multi-tenancy, done before 5). It covers ResourceQuotas for team-a/team-b and the noisy-neighbour demo with the `tiny` profile (see PLAN.md).

**Gotchas**
- Ray writes `checkpoint_manager_snapshot.json` next to the `checkpoint_<ts>/` dirs, so match dirs only (`checkpoint_2*/`).
- Ray also prints `[rank 0]` lines twice in the driver logs (they get echoed). Tests use `in` and don't count them.
- Never reuse a job id as a run name. Ray Train restores any existing run with the same `storage_path` + `name`. That's how
  `backoffLimit` retries resume, but it would also make a "new" run resume an old one.
- shellcheck isn't installed locally, only in CI.

---

## 2026-11-23: Phase 3 complete (Ray Train)

**Done**
- `workloads/train/`: `profiles.py` (tiny/distilbert, with pinned HF SHAs), `prepare_data.py` (`make data`) and `train.py`
  (`TorchTrainer`, 2 workers on gloo, `set_num_threads(1)`, per-epoch all_reduce eval, `ray.train.report`).
- The API now mounts `/shared/data` (ro) and `/shared/checkpoints/<team>` → `/shared/checkpoints` on head and workers,
  and sets `RKP_JOB_ID`/`RKP_TEAM`/`RKP_STORAGE`/`RKP_DATA`. `make teams` also creates the checkpoint dirs.
- Head 2 Gi → 3 Gi. Default worker memory 1 Gi → 1536Mi. The reconciler keeps RUNNING while Ray says done but KubeRay doesn't yet.
- Tests: 45 unit tests, and `make test-k8s` (2) still passes.

**Measured:** 85.3% eval accuracy after 2 epochs (85.1% after 1). 97s of training. Submit → SUCCEEDED in 2m27s.
Peak (Ray's count, without page cache): head 2.76 GiB, worker 1.33 GiB, node **6.9 GiB**.

**State:** the cluster is deleted. `.shared/data` (62 MB, git-ignored) stays, so you don't need `make data` again.

**Next: Phase 4** (checkpoint/resume/retry/cancel). `train.py` already has `storage_path`/`name`. Add checkpoints
with `ray.train.report(metrics, checkpoint=...)` and resume with `get_checkpoint()`. With checkpoints, `result.metrics` gets filled.

**Gotchas**
- **The memory budget is tight:** 6.9 GiB node peak for tiny training, before monitoring. Phase 7 has to keep Prometheus/Grafana minimal.
- The head is the memory hog: ~10 dashboard subprocesses plus Train V2's 0-CPU helper actors all land there. `ray start` can't turn dashboard modules off.
- Measure memory the way Ray does (`memory.current − active_file − inactive_file`). `docker stats` and raw `memory.current` include page cache and look about 0.3 to 0.4 GB worse.
- `bert-tiny` on HF has no `model_type` or tokenizer config. `prepare_data.py` loads it with the Bert* classes and re-saves it complete.
- Agent shells: the Claude Code safety check blocks inline docker commands that combine the auto-remove flag with
  `$$(...)` substitution. Use `make data` (it uses `$(shell id -u)`) rather than typing the docker command by hand.

---

## 2026-11-23: Phase 2 complete (API + SDK + CLI)

**Done**
- `platform_api/`: FastAPI (`POST/GET /jobs`, `GET /jobs/{id}`, `/logs`, `DELETE`, `/healthz`), with `X-API-Key` → team (= namespace).
  The SQLite registry is `.rkp/registry.db`. The reconciler polls every 5s. `k8s.py` is the only K8s module (`KubeClient` protocol).
  Logs are JSON.
- `sdk/`: `Client` (httpx) and the `rkp` CLI (`submit | status | logs | cancel | list`, plus `--wait`). The project is now a
  hatchling package (`[project.scripts] rkp`).
- `make teams` (namespaces only, quotas come in Phase 6), `make keys` (git-ignored `config/api_keys.yaml` + `config/rkp.toml`),
  `make api`, `make test-k8s`. Added `workloads/hello.py`.
- Tests: 43 unit tests (fake K8s), plus 2 `k8s` tests (submit→SUCCEEDED with logs and cleanup, and cancel while RUNNING).

**Measured:** hello via the CLI went PENDING 2s → RUNNING 42s → SUCCEEDED 62s. Node peak 4.49 GiB. Cancel → CANCELLED,
and the namespace was empty within 10s. Team-b got 403 on GET, logs and DELETE. `make test-k8s` takes 1m45s.

**State:** the cluster is deleted (`make down`). The API is stopped. The dev registry `.rkp/` was deleted. The dev keys in `config/` stay (git-ignored).

**Next: Phase 3** (Ray Train, see PLAN.md). Run `make up && make teams`, then `make api` in another terminal. Training code goes in
`workloads/train/` as a flat dir, submitted with `rkp submit workloads/train --script train.py`. Measure the RAM peak against the 7 GB budget.

**Gotchas**
- **KubeRay's submitter runs the entrypoint through a shell.** `cd x && python y` ran only `cd` on Ray, and the job still
  "succeeded". The API now rejects `; & | < > $` and backticks, and uses `env -C <dir> <cmd>`.
- A head limit of 1.5 Gi OOM-killed the JobSupervisor (Ray's memory monitor). The head is now 2 Gi.
- A job shorter than the 5s poll skips RUNNING in `rkp --wait` output, but `started_at` still comes from `status.rayJobInfo`.
- FastAPI + `from __future__ import annotations` + `Annotated` aliases defined inside `create_app` → params silently become
  query params (422). `main.py` doesn't use the future import.
- `uv lock --exclude-newer` re-resolves everything. Check that existing pins didn't move (ruff went down to 0.17.4 and had to be restored).
- Driver logs start with a transient "infeasible resource requests" line: the driver starts before the workers join. It's harmless.

---

## 2026-11-20: Phase 1 complete (cluster + hello RayJob)

**Done**
- `docker/Dockerfile` + `docker/requirements.txt`: `RAY_IMAGE` + torch 2.15.0 (CPU index), transformers 5.22.2,
  datasets 5.1.0 → `RKP_IMAGE=rkp-ray:2.59.0-1` (new key in `infra/versions.env`). About 3 min to build, 5 GB on disk.
- `infra/kind/cluster.yaml` (single node `rkp`, `./.shared → /shared`), `infra/helm/kuberay-operator-values.yaml`.
- `infra/k8s/examples/hello-rayjob.yaml`: head (`num-cpus: 0`) + 2 workers, script in a ConfigMap,
  `shutdownAfterJobFinishes: true`, `ttlSecondsAfterFinished: 30`, `imagePullPolicy: Never`.
- Makefile: `up` (idempotent, kind-loads the image if built), `down`, `image`, `load`, `hello`.
- Tests: 7 pass. The new ones check that the manifest, Dockerfile and versions.env agree, and that image deps are pinned.

**Measured**
- `make hello`: SUCCEEDED in 43s, with tasks spread over both workers. The RayCluster and its pods were gone 30s after it finished.
- Node RAM: 1.0 GiB idle (kind + operator), **3.9 GiB peak** during the job, under the 5 GB target.

**State:** the cluster has been deleted (`make down`). The `rkp-ray:2.59.0-1` image stays in local Docker.

**Next: Phase 2** (API + SDK, see PLAN.md). Run `make up` first, which recreates the cluster and loads the image (about 1.5 min).

**Gotchas**
- `kind load` of the 5 GB image takes about 1.5 min. Avoid rebuilding unless deps change, and bump the `-N` tag when they do.
- kind accepts the relative `hostPath: ./.shared`, but only when you run from the repo root (the Makefile always does).
- The RayJob CR and its completed submitter pod stay after the TTL. Only the RayCluster is deleted, so Phase 2 must delete finished CRs.
- Don't wait on a process with `pgrep -f "<cmd>"` inside a shell whose own command line contains `<cmd>`. It matches itself and never exits.

---

## 2026-11-14: Rough case study in README + Excalidraw diagrams

**Done**
- README.md rewritten as a rough case study (problem, architecture, job flow, fault tolerance, tenancy,
  serving, observability, decisions, running, testing, results placeholders). Not-yet-built parts are marked 🚧,
  and §12 Results says TBD on purpose. **Don't invent numbers.**
- 4 diagrams in `docs/diagrams/`: architecture, job-lifecycle, fault-tolerance, multi-tenancy.
  Source of truth is `docs/diagrams/src/*.json` (Excalidraw element skeletons). `make diagrams` regenerates
  the `.excalidraw` (editable) and `.svg` (README) files with `scripts/render_diagrams.py` (pure Python, with the Virgil font
  inlined, cached in git-ignored `.cache/`).
- Resume bullet drafts moved from the README to PLAN.md Phase 9.

**Next:** still Phase 1 (see the entry below). When a phase ships, update its README section: drop 🚧 and add real results.

**Gotchas**
- Rendering via headless Chrome + the Excalidraw JS library was blocked by the sandbox, so the renderer is a
  hand-rolled Python SVG writer. If you edit a diagram in excalidraw.com, copy the changes back into `src/*.json`,
  or `make diagrams` will overwrite them.
- To preview an SVG locally: `google-chrome --headless=new --screenshot=out.png --window-size=W,H file://.../x.svg`.

---

## 2026-11-14: Phase 0 complete

**Done**
- Wrote the docs: AGENTS.md (agent rules, incl. **no AI attribution in git**), CLAUDE.md (imports AGENTS.md),
  PLAN.md (rewritten as a one-weekend plan), ARCHITECTURE.md, this file.
- `infra/versions.env` pins all versions (kind v0.33.0, node v1.37.0, kubectl v1.37.2, Helm v4.3.1,
  KubeRay chart 1.7.2, Ray 2.59.0, image `rayproject/ray:2.59.0-py312-cpu`).
- `scripts/install-tools.sh` (`make tools`) installs kind/kubectl/helm into `~/.local/bin` with sha256 checks.
  It's idempotent, and these are installed on the dev machine.
- uv project (Python 3.12.13, ruff 0.17.5, pytest 9.2.0) with `uv.lock`. `make check` → 5 tests passing.
- `.github/workflows/ci.yml`: `uv sync --locked` + `make check` + shellcheck.

**State**
- No cluster exists yet. Nothing is running. Docker images pulled: `koalaman/shellcheck:v0.10.0` only.

**Next: Phase 1 (see PLAN.md)**
1. Start `make image` first (pull about 870 MB `RAY_IMAGE` + add CPU torch/transformers). It's the slowest step.
2. Meanwhile: `infra/kind/cluster.yaml` (single node, `.shared` → `/shared` mount), `make up/down`
   (kind create + `helm install kuberay-operator`), and `infra/k8s/examples/hello-rayjob.yaml`.
3. Acceptance: the hello RayJob reaches SUCCEEDED, and peak stack RAM stays under 5 GB.

**Gotchas**
- The dev machine has 2 cores / 15 GB RAM / no GPU. Follow the budget in ARCHITECTURE.md §6.
- The host Python is 3.13, but the project uses 3.12 (uv-managed) to match the Ray image. Always use `uv run`.
- Helm is **v4**. Check KubeRay chart install flags against Helm 4 if anything errors.
- shellcheck isn't installed locally. Run it with
  `docker run --rm -v "$PWD:/mnt:ro" -w /mnt koalaman/shellcheck:v0.10.0 scripts/*.sh`.
