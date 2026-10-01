# Streamhouse OSS — local developer workflow.
COMPOSE := docker compose -f deploy/compose/docker-compose.yml --profile apps
MVN     := mvn -B
SHCTL   := bin/shctl

# Testcontainers needs the Docker socket of the active context (Colima, OrbStack, Docker Desktop...).
export DOCKER_HOST ?= $(shell docker context inspect --format '{{.Endpoints.docker.Host}}' 2>/dev/null)
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE ?= /var/run/docker.sock
ifneq ($(wildcard /opt/homebrew/opt/openjdk@21),)
export JAVA_HOME ?= /opt/homebrew/opt/openjdk@21
endif

.PHONY: help build test up down clean demo generate e2e logs

help: ## Show this help
	@grep -E '^[a-z0-9-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "} {printf "  %-10s %s\n", $$1, $$2}'

build: ## Compile and package all modules (no tests)
	$(MVN) package -DskipTests

test: ## Run unit and integration tests (needs Docker)
	$(MVN) verify

up: build ## Start the whole platform locally and wait until it is healthy
	$(COMPOSE) up -d --build --wait

down: ## Stop the platform, keep data
	$(COMPOSE) down

clean: ## Stop the platform and delete all data
	$(COMPOSE) down -v

demo: up ## Start everything and apply the e-commerce pipeline
	$(SHCTL) login --username admin --password admin
	$(SHCTL) sql -f examples/ecommerce/pipeline.sql --wait

generate: ## Stream random orders into the shop database (Ctrl-C to stop)
	examples/ecommerce/generate-orders.sh

e2e: up ## End-to-end check: pipeline, freshness, Iceberg, lineage, MCP, grants, audit
	python3 e2e/e2e.py

logs: ## Follow the logs of the streamhouse services
	$(COMPOSE) logs -f control-plane context-engine
