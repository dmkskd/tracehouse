#!/usr/bin/env bash
# Apply one image override to a ClickHouse operator manifest.
# The operator stores the repository and tag in separate YAML fields.

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

TARGET_REPOSITORY="${IMAGE_OVERRIDE%:*}"
TARGET_TAG="${IMAGE_OVERRIDE##*:}"
if [[ "$IMAGE_OVERRIDE" == *@* || "$TARGET_REPOSITORY" == "$IMAGE_OVERRIDE" || "$TARGET_TAG" == */* ]]; then
  echo "ClickHouse operator manifests require a tagged image: $IMAGE_OVERRIDE" >&2
  exit 2
fi

sed -E \
  -e "s#repository: ${SOURCE_REPOSITORY}#repository: ${TARGET_REPOSITORY}#g" \
  -e "/repository: ${TARGET_REPOSITORY//\//\\/}/,/tag:/ s#tag: \"[A-Za-z0-9._-]+\"#tag: \"${TARGET_TAG}\"#" \
  "$MANIFEST"
