#!/usr/bin/env bash
#
# docker compose wrapper for the demo stack.
#
# Always loads the canonical ClickHouse/Keeper image pins from
# infra/clickhouse.env (the same file the justfile and the test suites
# use) plus the local .env with the generated passwords, so the demo
# can never drift onto a different server version than the rest of the
# repository.
#
# Usage (any docker compose subcommand / flags are forwarded):
#   ./compose.sh up -d
#   ./compose.sh --profile iceberg up -d
#   ./compose.sh pull && ./compose.sh up -d
#   ./compose.sh ps
#   ./compose.sh down
#
# Override the image for one invocation:
#   CLICKHOUSE_IMAGE=clickhouse/clickhouse-server:26.3.33 ./compose.sh up -d
#
set -euo pipefail

cd "$(dirname "$0")"

if [[ ! -f .env ]]; then
  echo "ERROR: .env missing. Run ./setup.sh first." >&2
  exit 1
fi

exec docker compose \
  --env-file ../clickhouse.env \
  --env-file .env \
  "$@"
