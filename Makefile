# Streamhouse OSS — local developer workflow. First run: `make build up` (or `make demo`).
COMPOSE := docker compose -f deploy/compose/docker-compose.yml --profile apps
MVN     := mvn -B
SHCTL   := bin/shctl
DEMO    := demos/shop-assistant

# Testcontainers needs the Docker socket of the active context (Colima, OrbStack, Docker Desktop...).
export DOCKER_HOST ?= $(shell docker context inspect --format '{{.Endpoints.docker.Host}}' 2>/dev/null)
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE ?= /var/run/docker.sock
ifneq ($(wildcard /opt/homebrew/opt/openjdk@21),)
export JAVA_HOME ?= /opt/homebrew/opt/openjdk@21
endif

.PHONY: help build test up down clean demo generate e2e logs

help: ## Show this help
	@grep -E '^[a-z0-9-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "} {printf "  %-10s %s\n", $$1, $$2}'

build: ## Compile and package all modules (no tests); images are built from the packaged jars
	$(MVN) package -DskipTests

test: ## Run unit and integration tests (needs Docker)
	$(MVN) verify

up: ## Start the whole platform and wait until it is healthy (run `make build` first)
	$(COMPOSE) up -d --build --wait

down: ## Stop the platform, keep data
	$(COMPOSE) down

clean: ## Stop the platform and delete all data
	$(COMPOSE) down -v

demo: build up ## Build, start everything and apply the native e-commerce pipeline
	$(SHCTL) login --username admin --password admin
	$(SHCTL) sql -f $(DEMO)/sql/pipeline.sql --wait

$(DEMO)/.venv:
	python3 -m venv $@ && $@/bin/pip install -q -e $(DEMO)

generate: $(DEMO)/.venv ## Stream orders into the shop database (Ctrl-C to stop)
	$(DEMO)/.venv/bin/shop-assistant --env oss simulate

e2e: ## End-to-end check against the running platform: pipeline, freshness, Iceberg, lineage, MCP, grants, audit
	python3 e2e/e2e.py

logs: ## Follow the logs of the streamhouse services
	$(COMPOSE) logs -f control-plane context-engine
