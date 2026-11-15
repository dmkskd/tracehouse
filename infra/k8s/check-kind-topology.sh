#!/usr/bin/env bash
# Refuse to reuse a Kind cluster created for a different development topology.
set -euo pipefail

CLUSTER_NAME="${1:?Usage: check-kind-topology.sh CLUSTER CONFIG}"
CONFIG="${2:?Usage: check-kind-topology.sh CLUSTER CONFIG}"
EXPECTED_NODES=$(awk '$1 == "-" && $2 == "role:" { count++ } END { print count+0 }' "$CONFIG")
EXISTING_NODES=$(kind get nodes --name "$CLUSTER_NAME")
ACTUAL_NODES=$(printf '%s\n' "$EXISTING_NODES" | awk 'NF { count++ } END { print count+0 }')

if [[ "$EXPECTED_NODES" -eq 0 ]]; then
    echo "No Kind nodes found in config: $CONFIG" >&2
    exit 1
fi
if [[ "$ACTUAL_NODES" -ne "$EXPECTED_NODES" ]]; then
    echo "Kind cluster '$CLUSTER_NAME' has $ACTUAL_NODES nodes, but $CONFIG requires $EXPECTED_NODES." >&2
    echo "The existing cluster cannot be reused for this topology. No manifests were applied." >&2
    echo "Recreating it deletes its local data. After backing up any needed data, run just k8s-stop and retry the intended startup command." >&2
    exit 1
fi
