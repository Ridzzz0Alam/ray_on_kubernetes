# PROGRESS.md: session handoff log

The handoff file between work sessions (human or agent). **Read it first and update it last.**
Newest entry on top. Keep each entry short: what was done, current state, next step, gotchas.
Long-term plan → [PLAN.md](PLAN.md). Design → [ARCHITECTURE.md](ARCHITECTURE.md). Rules → [AGENTS.md](AGENTS.md).

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
