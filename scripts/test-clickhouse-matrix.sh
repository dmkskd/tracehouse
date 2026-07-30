#!/usr/bin/env bash
# Run the complete ClickHouse compatibility suite for a pinned image matrix.
#
# Usage:
#   ./scripts/test-clickhouse-matrix.sh
#   ./scripts/test-clickhouse-matrix.sh --list
#   ./scripts/test-clickhouse-matrix.sh clickhouse/clickhouse-server:23.8.2.7-alpine
#
# Explicit image arguments replace the pinned matrix. CLICKHOUSE_TEST_MATRIX
# can also provide a comma-separated override.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MATRIX_FILE="$ROOT/scripts/clickhouse-test-matrix.txt"
LIST_ONLY=false
EXPLICIT_IMAGES=()

usage() {
  cat <<'EOF'
Usage: ./scripts/test-clickhouse-matrix.sh [options] [clickhouse-image ...]

Options:
  --matrix-file <path>  Read the default image list from another file.
  --list                Print the effective matrix without running tests.
  -h, --help            Show this help.

With no image arguments, the script reads scripts/clickhouse-test-matrix.txt.
Set CLICKHOUSE_TEST_MATRIX to a comma-separated list for a one-off override.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --matrix-file)
      if [[ $# -lt 2 ]]; then
        echo "Missing value for --matrix-file" >&2
        exit 1
      fi
      MATRIX_FILE="$2"
      shift 2
      ;;
    --list)
      LIST_ONLY=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      while [[ $# -gt 0 ]]; do
        EXPLICIT_IMAGES+=("$1")
        shift
      done
      ;;
    -*)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
    *)
      EXPLICIT_IMAGES+=("$1")
      shift
      ;;
  esac
done

CLICKHOUSE_TEST_IMAGES=()
if [[ ${#EXPLICIT_IMAGES[@]} -gt 0 ]]; then
  CLICKHOUSE_TEST_IMAGES=("${EXPLICIT_IMAGES[@]}")
elif [[ -n "${CLICKHOUSE_TEST_MATRIX:-}" ]]; then
  IFS=',' read -r -a CLICKHOUSE_TEST_IMAGES <<< "$CLICKHOUSE_TEST_MATRIX"
else
  if [[ ! -f "$MATRIX_FILE" ]]; then
    echo "ClickHouse test matrix not found: $MATRIX_FILE" >&2
    exit 1
  fi
  while IFS= read -r image || [[ -n "$image" ]]; do
    [[ -z "$image" || "$image" == \#* ]] && continue
    CLICKHOUSE_TEST_IMAGES+=("$image")
  done < "$MATRIX_FILE"
fi

if [[ ${#CLICKHOUSE_TEST_IMAGES[@]} -eq 0 ]]; then
  echo "ClickHouse test matrix is empty" >&2
  exit 1
fi

if [[ "$LIST_ONLY" == "true" ]]; then
  printf '%s\n' "${CLICKHOUSE_TEST_IMAGES[@]}"
  exit 0
fi

echo "Running ClickHouse compatibility matrix (${#CLICKHOUSE_TEST_IMAGES[@]} images)"
for image in "${CLICKHOUSE_TEST_IMAGES[@]}"; do
  "$ROOT/scripts/test-clickhouse-version.sh" "$image"
done

echo ""
echo "  ✓ All ${#CLICKHOUSE_TEST_IMAGES[@]} ClickHouse images passed"
