#!/usr/bin/env bash
# Apply one image override to an Altinity operator manifest.
# Altinity manifests use a normal, complete container image reference.

set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "Usage: $0 <manifest.yaml> <source-repository> <image-override>" >&2
  exit 2
fi

MANIFEST="$1"
SOURCE_REPOSITORY="$2"
IMAGE_OVERRIDE="$3"

if [[ ! -f "$MANIFEST" ]]; then
  echo "Manifest not found: $MANIFEST" >&2
  exit 2
fi

if [[ -z "$IMAGE_OVERRIDE" ]]; then
  cat "$MANIFEST"
  exit 0
fi

sed -E \
  "s#${SOURCE_REPOSITORY}:[A-Za-z0-9._-]+#${IMAGE_OVERRIDE}#g" \
  "$MANIFEST"
