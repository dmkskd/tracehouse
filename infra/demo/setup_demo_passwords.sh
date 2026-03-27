#!/bin/bash
# Set read-only user password on all ClickHouse nodes.
# Called by the password-init service in docker-compose.yml.

set -euo pipefail

DEFAULT_PASSWORD="${CLICKHOUSE_DEFAULT_PASSWORD:?CLICKHOUSE_DEFAULT_PASSWORD must be set}"
READONLY_PASSWORD="${CLICKHOUSE_READONLY_PASSWORD:?CLICKHOUSE_READONLY_PASSWORD must be set}"

NODES="ch-s1r1 ch-s1r2 ch-s2r1 ch-s2r2"

for node in $NODES; do
  echo "Setting read_only password on $node..."
  for i in $(seq 1 30); do
    if clickhouse client --host "$node" --password "$DEFAULT_PASSWORD" \
        --query "ALTER USER read_only IDENTIFIED BY '${READONLY_PASSWORD}'" 2>&1; then
      echo "  $node — done"
      break
    fi
    sleep 1
  done
done

echo "All nodes configured."
