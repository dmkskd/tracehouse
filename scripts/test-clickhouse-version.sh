#!/usr/bin/env bash
# Run every ClickHouse-backed test suite against one exact server image.
#
# Usage:
#   ./scripts/test-clickhouse-version.sh clickhouse/clickhouse-server:25.8.28.1-alpine

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: ./scripts/test-clickhouse-version.sh <clickhouse-image>

Runs core integration, proxy integration, data-utils, and E2E tests with the
same CLICKHOUSE_IMAGE.
EOF
}

if [[ $# -eq 1 && ( "$1" == "-h" || "$1" == "--help" ) ]]; then
  usage
  exit 0
fi

if [[ $# -ne 1 ]]; then
  usage >&2
  exit 1
fi

IMAGE="$1"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo ""
echo "============================================================"
echo "ClickHouse compatibility tests: $IMAGE"
echo "============================================================"

echo "Preparing image before parallel Testcontainers workers..."
if docker image inspect "$IMAGE" >/dev/null 2>&1; then
  echo "Using cached image: $IMAGE"
else
  if ! docker pull "$IMAGE"; then
    # Docker Desktop can report a stale containerd lease even when another
    # operation completed the image locally. Inspect once more before failing.
    if docker image inspect "$IMAGE" >/dev/null 2>&1; then
      echo "Warning: pull reported an error, but the image is available locally."
    else
      echo ""
      echo "✗ FAILED: $IMAGE (image pull)"
      exit 1
    fi
  fi
fi

if ! TRACEHOUSE_COMPACT_TEST_OUTPUT=1 CLICKHOUSE_IMAGE="$IMAGE" just test-clickhouse; then
  echo ""
  echo "✗ FAILED: $IMAGE (ClickHouse-backed tests)"
  exit 1
fi

if ! CLICKHOUSE_IMAGE="$IMAGE" just e2e; then
  echo ""
  echo "✗ FAILED: $IMAGE (E2E tests)"
  exit 1
fi

echo "  ✓ $IMAGE passed"
