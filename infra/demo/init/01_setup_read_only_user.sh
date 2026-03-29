#!/bin/bash
# Set up the read-only user with the password from the environment.
# This wrapper exists because SQL doesn't support env-var interpolation
# and docker-entrypoint-initdb.d runs .sh files via bash.
set -euo pipefail

PASSWORD="${CLICKHOUSE_READONLY_PASSWORD:?CLICKHOUSE_READONLY_PASSWORD must be set}"

sed "s/CHANGEME_RO_PASSWORD/${PASSWORD}/g" \
  /docker-entrypoint-initdb.d/init/01_setup_read_only_user.sql \
  | clickhouse-client --host localhost --multiquery
