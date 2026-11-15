#!/usr/bin/env bash
set -euo pipefail

# Use the same Node major as CI for browser installation and test workers.
# npm caches this runtime; no shell configuration or global install is required.
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec npm exec --yes --package=node@22 -- node \
  "$script_dir/../../node_modules/playwright/cli.js" "$@"
