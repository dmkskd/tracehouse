#!/usr/bin/env bash
# Build, package, and run the Grafana plugin validator against local source.
# No git commit / push required.
set -euo pipefail

PLUGIN_ID="dmkskd-tracehouse-app"
PLUGIN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$PLUGIN_DIR/.." && pwd)"
ZIP_OUT="$PLUGIN_DIR/$PLUGIN_ID.zip"
SOURCE_ZIP="/tmp/${PLUGIN_ID}-source.zip"
ANALYZER="${1:-}"

echo "==> Building plugin"
cd "$REPO_ROOT"
npm run build
cd "$PLUGIN_DIR"

echo "==> Packaging $ZIP_OUT"
rm -f "$ZIP_OUT"
rm -rf "$PLUGIN_DIR/$PLUGIN_ID"
cp -r "$PLUGIN_DIR/dist" "$PLUGIN_DIR/$PLUGIN_ID"
(cd "$PLUGIN_DIR" && zip -qr "$ZIP_OUT" "$PLUGIN_ID")
rm -rf "$PLUGIN_DIR/$PLUGIN_ID"

bash "$PLUGIN_DIR/scripts/package-source.sh" "$SOURCE_ZIP"

ANALYZER_ARGS=()
if [[ -n "$ANALYZER" ]]; then
  echo "==> Running validator (analyzer: $ANALYZER)"
  ANALYZER_ARGS=(-analyzer "$ANALYZER")
else
  echo "==> Running validator (all analyzers)"
fi

docker run --pull=always --platform linux/amd64 --rm \
  -v /tmp:/tmp \
  -v "$PLUGIN_DIR":/plugin \
  grafana/plugin-validator-cli \
  "${ANALYZER_ARGS[@]}" \
  -sourceCodeUri "/tmp/$(basename "$SOURCE_ZIP")" \
  "/plugin/$PLUGIN_ID.zip"
