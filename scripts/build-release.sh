#!/usr/bin/env bash
# Build release artifacts: standalone binary + single-file HTML + Grafana plugin.
# Usage:
#   ./scripts/build-release.sh              # build for current platform
#   ./scripts/build-release.sh --target aarch64-apple-darwin
#   ./scripts/build-release.sh --skip-tests  # skip the pre-release test suite
#
# Outputs go to release/ directory.

set -euo pipefail

TARGET=""
SKIP_TESTS=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --target) TARGET="$2"; shift 2 ;;
    --skip-tests) SKIP_TESTS=true; shift ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RELEASE_DIR="$ROOT/release"
rm -rf "$RELEASE_DIR"
mkdir -p "$RELEASE_DIR"

echo "=== Installing dependencies ==="
cd "$ROOT"
npm install
npm run build --workspace=packages/core
npm run build --workspace=packages/ui-shared

# ── Tests ─────────────────────────────────────────────────────
if [[ "$SKIP_TESTS" == "true" ]]; then
  echo ""
  echo "=== Skipping tests (--skip-tests) ==="
else
  echo ""
  echo "=== Running tests ==="
  cd "$ROOT/frontend" && npm test
  cd "$ROOT/packages/core" && npx vitest run
  cd "$ROOT/packages/core" && npx vitest run --config vitest.integration.config.ts
  echo "  ✓ All tests passed"
fi

# ── Single-file HTML ────────────────────────────────────────────
echo ""
echo "=== Building single-file HTML ==="
cd "$ROOT/frontend"
npm run build:single
cp dist/tracehouse.html "$RELEASE_DIR/tracehouse.html"
echo "  → release/tracehouse.html"

# ── Grafana plugin ────────────────────────────────────────────
echo ""
echo "=== Building Grafana plugin ==="
cd "$ROOT/grafana-app-plugin"
npm install
npm run build
PLUGIN_ARCHIVE="tracehouse-grafana-plugin"
mkdir -p "$RELEASE_DIR/$PLUGIN_ARCHIVE"
cp -r dist/* "$RELEASE_DIR/$PLUGIN_ARCHIVE/"
cd "$RELEASE_DIR"
zip -r "${PLUGIN_ARCHIVE}.zip" "$PLUGIN_ARCHIVE"
rm -rf "$PLUGIN_ARCHIVE"
echo "  → release/${PLUGIN_ARCHIVE}.zip"

# ── Standalone binary ───────────────────────────────────────────
echo ""
echo "=== Building frontend for binary embedding ==="
cd "$ROOT/frontend"
TH_BUNDLED_PROXY=true npx vite build --config vite.singlefile.config.ts

echo ""
echo "=== Building Rust binary ==="
cd "$ROOT/infra/binary"
CARGO_ARGS=(build --release)
if [[ -n "$TARGET" ]]; then
  CARGO_ARGS+=(--target "$TARGET")
fi
cargo "${CARGO_ARGS[@]}"

# Locate the built binary
if [[ -n "$TARGET" ]]; then
  BIN_PATH="$ROOT/infra/binary/target/$TARGET/release/tracehouse"
else
  BIN_PATH="$ROOT/infra/binary/target/release/tracehouse"
fi

# Determine platform suffix for the archive name
if [[ -n "$TARGET" ]]; then
  PLATFORM="$TARGET"
else
  OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
  ARCH="$(uname -m)"
  PLATFORM="${OS}-${ARCH}"
fi

ARCHIVE_NAME="tracehouse-${PLATFORM}"
ARCHIVE_DIR="$RELEASE_DIR/$ARCHIVE_NAME"
mkdir -p "$ARCHIVE_DIR"
cp "$BIN_PATH" "$ARCHIVE_DIR/tracehouse"

# Create tarball
cd "$RELEASE_DIR"
tar czf "${ARCHIVE_NAME}.tar.gz" "$ARCHIVE_NAME"
rm -rf "$ARCHIVE_DIR"
echo "  → release/${ARCHIVE_NAME}.tar.gz"

# ── Docker quickstart image ───────────────────────────────────────
echo ""
echo "=== Building Docker quickstart image ==="
cd "$ROOT"
docker compose -f infra/quickstart/docker-compose.yml build
echo "  ✓ Docker quickstart image built"

# ── Summary ─────────────────────────────────────────────────────
echo ""
echo "=== Release artifacts ==="
ls -lh "$RELEASE_DIR"
