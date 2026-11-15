#!/usr/bin/env bash
# Load the locally built app into Kind and deploy Grafana with app provisioning.
set -euo pipefail

K8S_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${K8S_DIR}/../.." && pwd)"
CLUSTER_NAME="tracehouse-dev"
CONTEXT="kind-${CLUSTER_NAME}"
IMAGE="tracehouse-grafana:dev"
PLUGIN_DIST="${REPO_ROOT}/grafana-app-plugin/dist"

if [[ ! -f "${PLUGIN_DIST}/plugin.json" || ! -f "${PLUGIN_DIST}/module.js" ]]; then
    echo "Missing Grafana app build. Run: just k8s-grafana" >&2
    exit 1
fi

# Check the intended cluster before building or applying anything.
kubectl --context "$CONTEXT" get namespace clickhouse >/dev/null

docker build --file "${K8S_DIR}/grafana.Dockerfile" --tag "$IMAGE" "$PLUGIN_DIST"
kind load docker-image --name "$CLUSTER_NAME" "$IMAGE"

kubectl --context "$CONTEXT" apply -f "${K8S_DIR}/grafana-dashboards-cm.yaml"
kubectl --context "$CONTEXT" apply -f "${K8S_DIR}/grafana.yaml"
# The local image tag stays fixed; recreate the pod to pick up each new build.
kubectl --context "$CONTEXT" rollout restart deployment/grafana -n clickhouse
kubectl --context "$CONTEXT" rollout status deployment/grafana -n clickhouse --timeout=300s

echo "TraceHouse in Grafana: http://localhost:3001/a/dmkskd-tracehouse-app"
