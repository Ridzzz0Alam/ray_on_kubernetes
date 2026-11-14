SHELL := /bin/bash
.DEFAULT_GOAL := help

.PHONY: help tools sync lint fmt test check

help: ## Show available targets
	@grep -hE '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*## "}{printf "  \033[36m%-12s\033[0m %s\n",$$1,$$2}'

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

check: lint test ## What CI runs
