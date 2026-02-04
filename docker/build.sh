#!/usr/bin/env bash
set -e

IMAGE_NAME=${IMAGE_NAME:-fund-analysis:latest}
DOCKER_BUILDKIT=${DOCKER_BUILDKIT:-1}
export DOCKER_BUILDKIT

docker build -t "$IMAGE_NAME" -f docker/Dockerfile ..
