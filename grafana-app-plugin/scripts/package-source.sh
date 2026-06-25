#!/usr/bin/env bash
# Build the pruned source archive expected by Grafana's plugin validator.
set -euo pipefail

PLUGIN_ID="dmkskd-tracehouse-app"
PLUGIN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$PLUGIN_DIR/.." && pwd)"
SOURCE_ZIP="${1:-/tmp/${PLUGIN_ID}-source.zip}"

if [[ ! -d "$PLUGIN_DIR/dist" ]]; then
  echo "Plugin dist not found at $PLUGIN_DIR/dist. Run the plugin build first." >&2
  exit 1
fi

mkdir -p "$(dirname "$SOURCE_ZIP")"
SOURCE_ZIP="$(cd "$(dirname "$SOURCE_ZIP")" && pwd)/$(basename "$SOURCE_ZIP")"

echo "==> Zipping local source to $SOURCE_ZIP"
rm -f "$SOURCE_ZIP"
STAGE_DIR="$(mktemp -d)"
trap 'rm -rf "$STAGE_DIR"' EXIT

# The validator finds sourceCodeDir from package.json, then looks for source
# files at <sourceCodeDir>/src/<path>. Source map paths are like
# packages/core/src/X and frontend/src/X, so those dirs must be inside src/.
mkdir -p "$STAGE_DIR/tracehouse"
echo "    staging plugin at root"
rsync -a \
  --exclude 'node_modules' \
  --exclude '.DS_Store' \
  --exclude '*.zip' \
  --exclude '/dist' \
  "$PLUGIN_DIR/" "$STAGE_DIR/tracehouse/"

echo "    staging packages and frontend inside src/ for validator"
mkdir -p "$STAGE_DIR/tracehouse/src/packages" "$STAGE_DIR/tracehouse/src/frontend"
rsync -a \
  --exclude 'node_modules' \
  --exclude '.DS_Store' \
  --exclude '/e2e' \
  --exclude '/proxy' \
  --exclude 'package.json' --exclude 'package-lock.json' \
  "$REPO_ROOT/packages/" "$STAGE_DIR/tracehouse/src/packages/"
rsync -a \
  --exclude 'node_modules' \
  --exclude '.DS_Store' \
  --exclude '/dist' \
  --exclude 'package.json' --exclude 'package-lock.json' \
  --exclude 'tsconfig*.json' --exclude 'vite.config*' --exclude 'vitest.config*' \
  --exclude 'index.html' --exclude '.eslintrc*' \
  "$REPO_ROOT/frontend/" "$STAGE_DIR/tracehouse/src/frontend/"

# Prune frontend/src and packages/*/src to only files actually referenced by
# the bundle's source maps. Keeping unbundled files in the source zip causes
# Grafana code-rules to flag unrelated app code.
echo "==> Pruning unbundled source files"
node -e '
const fs = require("fs"), path = require("path");
const distDir = process.argv[1];
const stageRoot = process.argv[2];
const refs = new Set();
for (const f of fs.readdirSync(distDir).filter(f => f.endsWith(".map"))) {
  const m = JSON.parse(fs.readFileSync(path.join(distDir, f), "utf8"));
  for (const s of (m.sources || [])) {
    for (const prefix of ["frontend/src/", "packages/"]) {
      const i = s.indexOf(prefix);
      if (i !== -1) refs.add(s.substring(i).replace(/\?.*$/, ""));
    }
  }
}
function walk(dir, root, out) {
  if (!fs.existsSync(dir)) return;
  for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
    const fp = path.join(dir, e.name);
    if (e.isDirectory()) walk(fp, root, out);
    else out.push(path.relative(root, fp));
  }
}
let pruned = 0;
for (const sub of ["src/frontend/src", "src/packages"]) {
  const all = [];
  walk(path.join(stageRoot, sub), path.join(stageRoot, "src"), all);
  for (const rel of all) {
    if (!/\.(ts|tsx|js|jsx|css|scss|less)$/.test(rel)) continue;
    if (sub === "src/packages" && !/\/src\//.test(rel)) continue;
    if (!refs.has(rel)) {
      fs.unlinkSync(path.join(stageRoot, "src", rel));
      pruned++;
    }
  }
}
console.log("    pruned " + pruned + " unbundled files");
' "$PLUGIN_DIR/dist" "$STAGE_DIR/tracehouse"

# Some files re-exported via packages/core actually live in packages/ui-shared.
# The source map references them as packages/core/src/*, so copy them to where
# the validator expects to find them.
echo "==> Fixing cross-package re-exports"
node -e '
const fs = require("fs"), path = require("path");
const distDir = process.argv[1];
const srcDir = path.join(process.argv[2], "src");
const repoRoot = process.argv[3];
for (const f of fs.readdirSync(distDir).filter(f => f.endsWith(".map"))) {
  const m = JSON.parse(fs.readFileSync(path.join(distDir, f), "utf8"));
  for (let i = 0; i < m.sources.length; i++) {
    let s = m.sources[i].replace(/^webpack:\/\/\//, "").replace(/^webpack:\/\/[^\/]+\//, "");
    if (s.startsWith("packages/core/src/")) {
      const expected = path.join(srcDir, s);
      if (!fs.existsSync(expected)) {
        const alt = path.join(repoRoot, s.replace("packages/core/src/", "packages/ui-shared/src/"));
        if (fs.existsSync(alt)) {
          fs.mkdirSync(path.dirname(expected), { recursive: true });
          fs.copyFileSync(alt, expected);
        }
      }
    }
  }
}
' "$PLUGIN_DIR/dist" "$STAGE_DIR/tracehouse" "$REPO_ROOT"

(cd "$STAGE_DIR" && zip -qr "$SOURCE_ZIP" tracehouse)
echo "$SOURCE_ZIP"
