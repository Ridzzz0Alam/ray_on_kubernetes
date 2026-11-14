Ray on Kubernetes ML platform
Status: [ ] Not started

Gaps it closes: Kubernetes, Ray, distributed training, scheduling and autoscaling, fault tolerance for ML workloads, multi-tenant isolation, GPU awareness. These are the core requirements of the Revolut AI Platform Engineer role.

What to build: a small self-service platform where a "team" submits a training or inference job through an API and the platform runs it on Kubernetes with Ray.

Cluster: a local Kubernetes cluster with kind, plus the KubeRay operator.
Job submission API: a FastAPI service plus a small Python SDK. A user submits a job (training script, resources, team name), and the API creates a RayJob on Kubernetes. Add endpoints to check status, view logs and cancel.
Distributed training: use Ray Train to train a small PyTorch model across several workers, for example DistilBERT on the Amazon reviews dataset from the sentiment project.
Lifecycle:
save checkpoints during training
resume from the last checkpoint after a worker is killed (test this)
retry failed jobs automatically
support cancellation
Inference: serve the trained model with Ray Serve, with autoscaling based on request load.
Multi-tenancy: one Kubernetes namespace per team, with ResourceQuota limits. Show that one team can't starve another.
Observability and cost: Prometheus and Grafana dashboards for job status, queue time and resource usage per team, plus a simple cost-per-team report.
GPU (optional, a few dollars): run the same training job once on a cloud GPU node (a short-lived GKE or EKS GPU node pool), then delete it. Locally, use Ray custom resources to simulate GPU scheduling.
Stretch: orchestrate train → evaluate → deploy with Flyte instead of a plain script.
Possible resume bullets (only after building):

Built a self-service ML platform on Kubernetes (KubeRay) with a FastAPI submission API and Python SDK, running distributed PyTorch training with Ray Train and autoscaled inference with Ray Serve.
Implemented checkpointing, automatic retries, cancellation and per-team resource quotas, recovering training from worker failures, with Prometheus/Grafana dashboards for job and resource usage.
Rough effort: 3–5 weekends.