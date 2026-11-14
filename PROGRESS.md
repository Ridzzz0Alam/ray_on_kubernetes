# PROGRESS.md: session handoff log

The handoff file between work sessions (human or agent). **Read it first and update it last.**
Newest entry on top. Keep each entry short: what was done, current state, next step, gotchas.
Long-term plan → [PLAN.md](PLAN.md). Design → [ARCHITECTURE.md](ARCHITECTURE.md). Rules → [AGENTS.md](AGENTS.md).

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
