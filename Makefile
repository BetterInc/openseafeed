# OpenSeaFeed developer Makefile.
# Everything here is reproducible on a laptop: local stack via docker compose,
# plus the usual cargo workflows.

COMPOSE ?= docker compose
# ALL profiles, so `make dev-down` never orphans feed connectors (a plain
# `docker compose down` skips services in inactive profiles, leaving them
# running and holding the network -> "resource is busy").
COMPOSE_ALL ?= $(COMPOSE) --profile feeds --profile aisstream --profile denmark --profile dma-history

.DEFAULT_GOAL := help

.PHONY: help dev dev-logs dev-down test lint fmt fmt-check build-release e2e

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

dev: ## Build & start the full local stack (NATS + all services)
	$(COMPOSE) up --build -d
	@echo
	$(COMPOSE) ps

dev-logs: ## Follow logs from the local stack
	$(COMPOSE) logs -f

dev-down: ## Stop and remove the local stack incl. all feed connectors (keeps named volumes)
	$(COMPOSE_ALL) down

test: ## Run the full workspace test suite
	cargo test --workspace

lint: ## Run clippy with warnings denied (skips gracefully if clippy is absent)
	@if cargo clippy --version >/dev/null 2>&1; then \
		cargo clippy --workspace --all-targets -- -D warnings; \
	else \
		echo "clippy not installed — run: rustup component add clippy"; \
	fi

fmt: ## Format all code
	cargo fmt --all

fmt-check: ## Check formatting without modifying files
	cargo fmt --all -- --check

build-release: ## Build all binaries in release mode
	cargo build --release --workspace

e2e: ## Run the end-to-end test (placeholder until scripts/e2e.sh exists)
	@if [ -x scripts/e2e.sh ]; then \
		scripts/e2e.sh; \
	else \
		echo "TODO: scripts/e2e.sh not found — end-to-end test not yet implemented"; \
	fi
