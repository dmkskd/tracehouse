#!/usr/bin/env bash
# Package grafana-app-plugin/dist into the same ZIP layout used for releases.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT_DIR_INPUT="${1:-release}"
OUT_DIR="$OUT_DIR_INPUT"
PLUGIN_DIST="$REPO_ROOT/grafana-app-plugin/dist"

if [[ ! -f "$PLUGIN_DIST/plugin.json" ]]; then
  echo "Plugin dist not found. Build grafana-app-plugin first: $PLUGIN_DIST" >&2
  exit 1
fi

PLUGIN_ID="$(node -p "require('$PLUGIN_DIST/plugin.json').id")"
PLUGIN_VERSION="$(node -p "require('$PLUGIN_DIST/plugin.json').info.version")"
PLUGIN_ARCHIVE="${PLUGIN_ID}-${PLUGIN_VERSION}.zip"
PLUGIN_SHA1="${PLUGIN_ARCHIVE}.sha1"

mkdir -p "$OUT_DIR"
OUT_DIR="$(cd "$OUT_DIR" && pwd)"

rm -rf "$OUT_DIR/$PLUGIN_ID" "$OUT_DIR/$PLUGIN_ARCHIVE" "$OUT_DIR/$PLUGIN_SHA1"
mkdir -p "$OUT_DIR/$PLUGIN_ID"
cp -r "$PLUGIN_DIST/"* "$OUT_DIR/$PLUGIN_ID/"

(cd "$OUT_DIR" && zip -qr "$PLUGIN_ARCHIVE" "$PLUGIN_ID")

if command -v sha1sum >/dev/null 2>&1; then
  sha1sum "$OUT_DIR/$PLUGIN_ARCHIVE" | cut -f1 -d' ' > "$OUT_DIR/$PLUGIN_SHA1"
else
  shasum -a 1 "$OUT_DIR/$PLUGIN_ARCHIVE" | cut -f1 -d' ' > "$OUT_DIR/$PLUGIN_SHA1"
fi

echo "archive=$OUT_DIR_INPUT/$PLUGIN_ARCHIVE"
echo "archive_sha1=$OUT_DIR_INPUT/$PLUGIN_SHA1"
echo "archive_name=$PLUGIN_ARCHIVE"
echo "archive_sha1_name=$PLUGIN_SHA1"
