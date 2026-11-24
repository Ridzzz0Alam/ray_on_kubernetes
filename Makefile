SHELL := /bin/bash
.DEFAULT_GOAL := help

include infra/versions.env
CLUSTER     := rkp
KUBECTX     := kind-$(CLUSTER)
KUBERAY_REPO := https://ray-project.github.io/kuberay-helm/
TEAMS       := team-a team-b
PROFILE     ?= tiny
UIDGID      := $(shell id -u):$(shell id -g)
PROM_REPO    := https://prometheus-community.github.io/helm-charts
GRAFANA_REPO := https://grafana-community.github.io/helm-charts
METRICS_PORT := 9108
# IPv4 gateway of kind's docker network: the host as seen from pods. Lazy (=) so docker isn't
# queried on every make call. The API's metrics listener binds it so Prometheus can scrape the host.
KIND_GATEWAY = $(shell docker network inspect kind -f '{{range .IPAM.Config}}{{.Gateway}} {{end}}' 2>/dev/null | tr ' ' '\n' | grep -E '^[0-9]+\.' | head -1)

.PHONY: help tools sync lint fmt test test-k8s check diagrams up down image load hello teams keys api data monitoring monitoring-down grafana

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

teams: ## Team namespaces + quotas + LimitRanges + API RBAC, and their checkpoint dirs
	mkdir -p .shared/data $(addprefix .shared/checkpoints/,$(TEAMS))
	kubectl --context $(KUBECTX) apply -f infra/k8s/teams/

data: ## Download + tokenize the training data/model once into .shared/data (PROFILE=tiny|distilbert)
	mkdir -p .shared/data
	docker run --rm --user $(UIDGID) --env HF_HOME=/shared/data/.hf-cache \
		--volume $(CURDIR)/.shared:/shared --volume $(CURDIR)/workloads/train:/w:ro --workdir /w \
		$(RKP_IMAGE) python prepare_data.py --profile $(PROFILE)

keys: ## Generate dev API keys: config/api_keys.yaml (server) + config/rkp.toml (CLI)
	uv run python scripts/gen_keys.py

api: ## Run the platform API on 127.0.0.1:8000 (+ /metrics for Prometheus on the kind gateway)
	RKP_METRICS_ADDR=$(if $(KIND_GATEWAY),$(KIND_GATEWAY):$(METRICS_PORT)) \
		uv run uvicorn --factory platform_api.main:create_app --host 127.0.0.1 --port 8000 --no-access-log

monitoring: ## Minimal Prometheus + Grafana in ns monitoring (~0.9 GB; images pulled once, kind-loaded)
	@test -n "$(KIND_GATEWAY)" || { echo "kind network not found: run make up first"; exit 1; }
	@mkdir -p .cache
	sed 's/RKP_API_METRICS_TARGET/$(KIND_GATEWAY):$(METRICS_PORT)/' infra/helm/prometheus-values.yaml > .cache/prometheus-values.yaml
	{ helm template prometheus prometheus --repo $(PROM_REPO) --version $(PROMETHEUS_CHART_VERSION) \
		-n monitoring -f .cache/prometheus-values.yaml; \
	  helm template grafana grafana --repo $(GRAFANA_REPO) --version $(GRAFANA_CHART_VERSION) \
		-n monitoring -f infra/helm/grafana-values.yaml; } \
		| grep -oE 'image: "?[^" ]+' | sed -E 's/image: "?//' | sort -u > .cache/monitoring-images.txt
	@# Save only the local platform: `kind load docker-image` imports --all-platforms and fails on
	@# multi-arch images whose other platforms were never pulled (containerd image store).
	@set -e; while read -r img; do \
		docker image inspect "$$img" >/dev/null 2>&1 || docker pull -q "$$img" >/dev/null; \
		docker save --platform linux/$$(docker version -f '{{.Server.Arch}}') -o .cache/img.tar "$$img"; \
		kind load image-archive .cache/img.tar --name $(CLUSTER) >/dev/null; echo "loaded $$img"; \
	done < .cache/monitoring-images.txt; truncate -s 0 .cache/img.tar
	helm upgrade --install prometheus prometheus --repo $(PROM_REPO) --version $(PROMETHEUS_CHART_VERSION) \
		--kube-context $(KUBECTX) -n monitoring --create-namespace -f .cache/prometheus-values.yaml --wait --timeout 5m
	helm upgrade --install grafana grafana --repo $(GRAFANA_REPO) --version $(GRAFANA_CHART_VERSION) \
		--kube-context $(KUBECTX) -n monitoring -f infra/helm/grafana-values.yaml \
		--set-file dashboards.rkp.rkp.json=dashboards/rkp.json --wait --timeout 5m

monitoring-down: ## Remove Prometheus + Grafana (frees ~0.9 GB for training runs)
	-helm uninstall grafana prometheus --kube-context $(KUBECTX) -n monitoring
	kubectl --context $(KUBECTX) delete namespace monitoring --ignore-not-found

grafana: ## Port-forward Grafana to http://127.0.0.1:3000 (anonymous viewer; admin password in secret grafana)
	kubectl --context $(KUBECTX) -n monitoring port-forward svc/grafana 3000:80
