#!/usr/bin/env bash
set -e

CMD=${1:-main}
shift || true

docker compose -f docker/docker-compose.yml run --rm fund-analysis "$CMD" "$@"
