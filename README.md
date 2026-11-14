# Ray on Kubernetes ML platform

**Status:** Phase 0 done (bootstrap). See [PLAN.md](PLAN.md) for progress, [ARCHITECTURE.md](ARCHITECTURE.md)
for the design and [PROGRESS.md](PROGRESS.md) for the latest session handoff.

A small self-service ML platform. A "team" submits a training or inference job through an API, and the
platform runs it on Kubernetes with Ray.

**Skills covered:** Kubernetes, Ray, distributed training, scheduling and autoscaling, fault tolerance for ML
workloads, multi-tenant isolation, GPU awareness.

## What it includes

- **Cluster:** a local Kubernetes cluster with kind, plus the KubeRay operator.
- **Job submission API:** a FastAPI service plus a small Python SDK/CLI. A user submits a job (training script,
  resources, team name), and the API creates a RayJob. Endpoints for status, logs and cancel.
- **Distributed training:** Ray Train runs a small PyTorch model across several workers (a BERT-family
  sentiment classifier on Amazon reviews).
- **Lifecycle:** checkpoints during training, resuming from the last checkpoint after a worker is killed
  (tested), automatic retries, cancellation.
- **Inference:** the trained model is served with Ray Serve, with autoscaling based on request load.
- **Multi-tenancy:** one namespace per team with ResourceQuota limits, plus a demo showing one team can't starve another.
- **Observability and cost:** Prometheus/Grafana for job status, queue time and per-team resource usage, and a
  cost-per-team report.
- **GPU awareness:** Ray custom resources simulate GPU scheduling locally. An optional run on a short-lived cloud GPU node.
- **Stretch:** a train → evaluate → deploy pipeline (Flyte).

## Quickstart (so far)

```bash
make tools   # pinned kind / kubectl / helm → ~/.local/bin
make sync    # Python 3.12 env via uv
make check   # lint + unit tests
```

## Resume bullets (draft: only use once built)

- Built a self-service ML platform on Kubernetes (KubeRay) with a FastAPI submission API and Python SDK,
  running distributed PyTorch training with Ray Train and autoscaled inference with Ray Serve.
- Implemented checkpointing, automatic retries, cancellation and per-team resource quotas, recovering
  training from worker failures, with Prometheus/Grafana dashboards for job and resource usage.
