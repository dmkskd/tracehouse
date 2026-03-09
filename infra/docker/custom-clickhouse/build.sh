#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE_NAME="${1:-custom-clickhouse:latest}"

# Mode 1: Pre-compiled binary exists — just swap it into the official image
if [ -f "$SCRIPT_DIR/clickhouse" ]; then
  echo "Found local binary — building image with pre-compiled clickhouse..."
  docker build -t "$IMAGE_NAME" "$SCRIPT_DIR"
  echo "Done: $IMAGE_NAME"
  exit 0
fi

# Mode 2: No binary — need to compile from source
CH_SOURCE="${2:-}"
if [ -z "$CH_SOURCE" ]; then
  echo "Usage:"
  echo "  Option A: Place a compiled 'clickhouse' binary in $SCRIPT_DIR/ and re-run."
  echo "  Option B: $0 <image-name> <path-to-clickhouse-source>"
  echo ""
  echo "Example:"
  echo "  $0 custom-clickhouse:latest ~/dev/ClickHouse"
  exit 1
fi

if [ ! -f "$CH_SOURCE/CMakeLists.txt" ]; then
  echo "Error: $CH_SOURCE doesn't look like a ClickHouse source tree."
  exit 1
fi

echo "Building ClickHouse from source at $CH_SOURCE (this will take a while)..."
docker build \
  -f "$SCRIPT_DIR/Dockerfile.build" \
  -t "$IMAGE_NAME" \
  "$CH_SOURCE"

echo "Done: $IMAGE_NAME"
