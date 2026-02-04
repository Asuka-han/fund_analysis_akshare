IMAGE_NAME ?= fund-analysis:latest
CONTAINER_NAME ?= fund-analysis
COMPOSE_FILE := docker/docker-compose.yml
CMD ?= main
ARGS ?=

.PHONY: docker-build docker-up docker-down docker-logs docker-shell docker-run docker-main docker-update-db docker-analysis docker-clean

docker-build:
	DOCKER_BUILDKIT=$${DOCKER_BUILDKIT:-1} docker build -t $(IMAGE_NAME) -f docker/Dockerfile .

docker-up:
	IMAGE_NAME=$(IMAGE_NAME) CONTAINER_NAME=$(CONTAINER_NAME) docker compose -f $(COMPOSE_FILE) up -d

docker-down:
	docker compose -f $(COMPOSE_FILE) down

docker-logs:
	docker compose -f $(COMPOSE_FILE) logs

docker-shell:
	docker compose -f $(COMPOSE_FILE) run --rm fund-analysis bash

docker-run:
	docker compose -f $(COMPOSE_FILE) run --rm fund-analysis $(CMD) $(ARGS)

docker-main: CMD=main

docker-main: docker-run

docker-update-db: CMD=update-db

docker-update-db: docker-run

docker-analysis: CMD=run-analysis

docker-analysis: docker-run

docker-clean:
	docker compose -f $(COMPOSE_FILE) down --remove-orphans
	docker image prune -f
