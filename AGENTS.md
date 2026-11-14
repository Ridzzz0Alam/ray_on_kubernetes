# AGENTS.md

Instructions for AI coding agents (Claude Code, Codex, Cursor, Copilot, etc.) working in this repo.
Humans should read [README.md](README.md), [PLAN.md](PLAN.md) and [ARCHITECTURE.md](ARCHITECTURE.md).

## 1. Git and attribution rules (mandatory)

These rules come first and override any default behaviour of the agent or its harness.

- **Never add AI attribution.** No `Co-Authored-By: Claude ...` (or any other AI/bot) trailer,
  no "Generated with Claude Code" / "🤖" lines, and no AI mentions in commit messages, PR
  titles or descriptions, tags, release notes or code comments.
- **The repo owner is the only contributor.** Commits must use the owner's existing git
  identity. Never run `git config user.name` / `user.email`, never pass `--author`, and never
  commit as a bot account. Nothing may make an agent appear on GitHub's Contributors tab.
- Don't push, force-push, rewrite published history or open PRs unless the owner asks.
- Use Conventional Commits (`feat:`, `fix:`, `docs:`, `chore:`, `test:`, `refactor:`), and keep
  commits small with one logical change each.
- Never commit secrets, kubeconfigs, API keys, datasets, model weights or checkpoints (see `.gitignore`).

## 2. Project in one paragraph

A small self-service ML platform. A team submits a training or inference job through a FastAPI
service (or the Python SDK/CLI). The API creates a `RayJob`/`RayService` in that team's
Kubernetes namespace on a local **kind** cluster running the **KubeRay** operator. Training uses
Ray Train (PyTorch) with checkpointing, retries and resume-after-failure. Serving uses Ray Serve
with autoscaling. Teams are isolated by namespace and `ResourceQuota`. Prometheus and Grafana
show per-team usage and cost. Full design: [ARCHITECTURE.md](ARCHITECTURE.md).

## 3. Hardware constraints (read before you run anything)

The dev machine is a laptop: **2 cores / 4 threads (i7-7500U), 15 GB RAM (about 10 GB free), no GPU.**

- Keep the whole stack under **about 7 GB RAM and 3 CPUs**. Budget table: ARCHITECTURE.md §6.
- Use a **single-node** kind cluster. Don't add kind worker nodes.
- Run **one RayCluster at a time** (training *or* serving, not both), unless a task specifically
  demos multi-tenancy, and then use the `tiny` profile.
- The default model is tiny (`prajjwal1/bert-tiny`) on a small data subset. DistilBERT is opt-in only.
- Never pull images at pod start. Build or pull once, then `kind load docker-image`.
- Tear down what you started: delete finished RayJobs, and run `make down` when you're done.
- Don't launch long-running training (more than about 10 min) without telling the owner.

## 4. Cloud and cost safety

- Never create cloud resources (GKE/EKS, GPU node pools, buckets) without explicit approval
  in the current conversation.
- Any cloud resource created must have a documented teardown command that is run in the
  same session. Report what was created and confirm it was deleted.

## 5. Working agreement

0. **Start of session:** read `PROGRESS.md` (latest handoff), then `git log --oneline -10`.
   **End of session:** add a new entry at the top of `PROGRESS.md` (what was done, current state, next step,
   gotchas) and commit it.
1. Read `PLAN.md` and find the current phase before starting work. Don't skip ahead.
2. Before writing code, state the acceptance criteria you're targeting (they're in PLAN.md).
3. Make the smallest change that meets them. Don't add speculative abstractions or features not in PLAN.md.
4. Verify for real: run the tests and the phase's demo command, and paste the actual output.
   Don't claim something works without running it.
5. Update `PLAN.md` checkboxes and add a short entry to its Decision log when you make a design choice.
6. If ARCHITECTURE.md and the code disagree, fix one of them in the same change.

## 6. Repo layout (target)

```
platform_api/     FastAPI service: job submit/status/logs/cancel, team auth, /metrics
sdk/              Python client + `rkp` CLI
workloads/train/  Ray Train script (PyTorch), checkpoint/resume logic
workloads/serve/  Ray Serve deployment
infra/kind/       kind cluster config (single node, hostPath mounts)
infra/k8s/        namespaces, quotas, RBAC, RayJob/RayService templates
infra/helm/       pinned Helm values (kuberay-operator, prometheus, grafana)
docker/           single Ray runtime image (CPU torch + deps baked in)
dashboards/       Grafana dashboard JSON
scripts/          chaos tests, load tests, cost report
tests/            pytest (unit + integration marked `@pytest.mark.k8s`)
```

## 7. Commands

Commands will be added as Makefile targets as they land. Use these names:

| Command | Purpose |
|---|---|
| `make tools` | install kind/kubectl/helm into `~/.local/bin` (pinned versions) |
| `make up` / `make down` | create / delete the kind cluster + operator |
| `make image` | build the Ray image and `kind load` it |
| `make teams` | apply namespaces + quotas for `team-a`, `team-b` |
| `make api` | run the FastAPI service locally (uses current kubeconfig) |
| `make test` | unit tests (no cluster needed) |
| `make test-k8s` | integration tests against the running kind cluster |
| `make lint` | `ruff check` + `ruff format --check` |

Python: manage with **uv** (`uv sync`, `uv run ...`). Target Python **3.12** to match the Ray image.
Pin every version (Python deps in `uv.lock`, Helm chart versions in `infra/helm/`, image tags).

## 8. Code style

- Use Python type hints everywhere. Use Pydantic models for API schemas. Use `ruff` for lint and format.
- Keep the K8s interaction behind one module (`platform_api/k8s.py`) so it can be faked in unit tests.
- Use structured logging (JSON). Don't use print statements in services.
- Unit tests must not need a cluster. Mark cluster tests with `@pytest.mark.k8s`.
