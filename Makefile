SHELL := /bin/bash
.DEFAULT_GOAL := help

include infra/versions.env
CLUSTER     := rkp
KUBECTX     := kind-$(CLUSTER)
KUBERAY_REPO := https://ray-project.github.io/kuberay-helm/

.PHONY: help tools sync lint fmt test test-k8s check diagrams up down image load hello teams keys api

help: ## Show available targets
	@grep -hE '^[a-zA-Z0-9_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*## "}{printf "  \033[36m%-12s\033[0m %s\n",$$1,$$2}'

tools: ## Install pinned kind/kubectl/helm into ~/.local/bin
	bash scripts/install-tools.sh

sync: ## Create/refresh the Python env from uv.lock
	uv sync --locked

lint: ## Ruff lint + format check
	uv run ruff check .
	uv run ruff format --check .

fmt: ## Auto-fix lint and format
	uv run ruff check --fix .
	uv run ruff format .

test: ## Unit tests (no cluster needed)
	uv run pytest -m "not k8s"

test-k8s: ## Integration tests against the running kind cluster (make up && make teams first)
	uv run pytest -m k8s

check: lint test ## What CI runs

diagrams: ## Render docs/diagrams/src/*.json → .excalidraw + .svg
	uv run python scripts/render_diagrams.py

up: ## Create the kind cluster + KubeRay operator (idempotent)
	@mkdir -p .shared
	@kind get clusters 2>/dev/null | grep -qx $(CLUSTER) || \
		kind create cluster --config infra/kind/cluster.yaml --image $(KIND_NODE_IMAGE) --wait 120s
	helm upgrade --install kuberay-operator kuberay-operator \
		--repo $(KUBERAY_REPO) --version $(KUBERAY_CHART_VERSION) \
		--kube-context $(KUBECTX) -n kuberay-system --create-namespace \
		-f infra/helm/kuberay-operator-values.yaml --wait --timeout 5m
	@if docker image inspect $(RKP_IMAGE) >/dev/null 2>&1; then $(MAKE) --no-print-directory load; \
	else echo "note: $(RKP_IMAGE) not built yet; run 'make image'"; fi

down: ## Delete the kind cluster (and everything in it)
	kind delete cluster --name $(CLUSTER)

image: ## Build the Ray runtime image; kind-load it if the cluster is up
	docker build --build-arg RAY_IMAGE=$(RAY_IMAGE) -t $(RKP_IMAGE) docker/
	@if kind get clusters 2>/dev/null | grep -qx $(CLUSTER); then $(MAKE) --no-print-directory load; fi

load: ## Side-load the Ray runtime image into kind
	kind load docker-image $(RKP_IMAGE) --name $(CLUSTER)

hello: ## Phase 1 smoke test: run the hello RayJob and wait for SUCCEEDED
	kubectl --context $(KUBECTX) apply -f infra/k8s/examples/hello-rayjob.yaml
	kubectl --context $(KUBECTX) wait rayjob/hello --for=jsonpath='{.status.jobStatus}'=SUCCEEDED --timeout=10m
	kubectl --context $(KUBECTX) logs -l job-name=hello --tail=20

teams: ## Create the team namespaces (quotas come in Phase 6)
	kubectl --context $(KUBECTX) apply -f infra/k8s/teams/

keys: ## Generate dev API keys: config/api_keys.yaml (server) + config/rkp.toml (CLI)
	uv run python scripts/gen_keys.py

api: ## Run the platform API on 127.0.0.1:8000 (uses the kind-rkp kube context)
	uv run uvicorn --factory platform_api.main:create_app --host 127.0.0.1 --port 8000 --no-access-log
