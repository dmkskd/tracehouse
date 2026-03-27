#!/bin/bash
# Creates the read-only user with the password from CLICKHOUSE_READONLY_PASSWORD.
# Runs as a Docker entrypoint init script on each CH node.

set -euo pipefail

READONLY_PW="${CLICKHOUSE_READONLY_PASSWORD:-changeme}"

clickhouse client --user default --password "${CLICKHOUSE_PASSWORD:-}" --query "
  CREATE SETTINGS PROFILE IF NOT EXISTS readonly_profile
    SETTINGS readonly = 2, allow_introspection_functions = 1;

  CREATE USER IF NOT EXISTS read_only
    IDENTIFIED BY '${READONLY_PW}'
    SETTINGS PROFILE readonly_profile;

  GRANT SELECT ON *.* TO read_only;
  GRANT SHOW DATABASES, SHOW TABLES, SHOW COLUMNS ON *.* TO read_only;
  GRANT dictGet ON *.* TO read_only;
" --multiquery

echo "read_only user configured."
