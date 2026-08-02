# Falcon Cache — common tasks.
#
# `make` on its own prints this list. Nothing here is required to use the
# project: every target is a thin wrapper over a cargo or docker command you
# could type yourself, kept in one place so they are discoverable and hard to
# get subtly wrong.

BINARY      := falcon
IMAGE       := falcon-cache
TAG         := latest
# `cargo install` respects CARGO_INSTALL_ROOT; fall back to the standard path.
INSTALL_ROOT := $(or $(CARGO_INSTALL_ROOT),$(HOME)/.cargo)

.DEFAULT_GOAL := help
.PHONY: help build install uninstall run test test-timing check fmt lint audit docker-build docker-run docker-stop compose-up compose-down clean

help: ## Show this help
	@echo "Falcon Cache — make targets"
	@echo
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'
	@echo
	@echo "Quickstart:  make install && falcon serve"
	@echo "With Docker: make docker-run"

## --- build & install ---------------------------------------------------

build: ## Build the release binary (target/release/falcon)
	cargo build --release -p falcon-cli
	@echo
	@echo "Built: target/release/$(BINARY)"
	@echo "Run it with ./target/release/$(BINARY) serve, or 'make install' to put it on PATH."

install: ## Install `falcon` onto your PATH via cargo
	cargo install --path crates/falcon-cli --locked
	@echo
	@echo "Installed: $(INSTALL_ROOT)/bin/$(BINARY)"
	@command -v $(BINARY) >/dev/null 2>&1 \
		|| echo "NOTE: $(INSTALL_ROOT)/bin is not on your PATH — add it to your shell profile."

uninstall: ## Remove the installed `falcon` binary
	cargo uninstall falcon-cli || true

run: ## Build and run a node in the foreground
	cargo run --release -p falcon-cli -- serve

## --- quality -----------------------------------------------------------

check: fmt lint test ## Run before committing: fmt + clippy + tests

test: ## Run the whole test suite
	cargo test --workspace

test-timing: ## Run the timing-sensitive tests (needs an otherwise idle machine)
	cargo test --workspace --release -- --ignored --test-threads=1

fmt: ## Check formatting (use `cargo fmt --all` to fix)
	cargo fmt --all --check

lint: ## Clippy with warnings denied
	cargo clippy --workspace --all-targets -- -D warnings

audit: ## Check dependencies for security advisories
	@command -v cargo-audit >/dev/null 2>&1 || cargo install cargo-audit --locked
	cargo audit

## --- docker ------------------------------------------------------------

docker-build: ## Build the container image
	docker build -f docker/Dockerfile -t $(IMAGE):$(TAG) .

docker-run: docker-build ## Build and run a container on ports 8080/6380
	docker rm -f $(IMAGE) >/dev/null 2>&1 || true
	docker run -d --name $(IMAGE) -m 512m -p 8080:8080 -p 6380:6380 $(IMAGE):$(TAG)
	@echo "Waiting for the node to become healthy..."
	@for i in $$(seq 1 30); do \
		curl -fsS http://127.0.0.1:8080/healthz >/dev/null 2>&1 && break; \
		sleep 1; \
	done
	@echo
	@curl -fsS http://127.0.0.1:8080/healthz && echo " <- node is up on :8080 (wire on :6380)"
	@echo "Logs:  docker logs -f $(IMAGE)"
	@echo "Stop:  make docker-stop"

docker-stop: ## Stop and remove the container
	docker rm -f $(IMAGE) >/dev/null 2>&1 || true

compose-up: ## Start via docker compose
	docker compose -f docker/docker-compose.yml up -d --build

compose-down: ## Stop the compose stack
	docker compose -f docker/docker-compose.yml down

clean: ## Remove build artifacts
	cargo clean
