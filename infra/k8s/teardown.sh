#!/bin/bash
# ClickHouse Kubernetes Teardown Script
# Removes the Kind cluster and all resources

set -e

CLUSTER_NAME="tracehouse-dev"

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }

if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
    log_info "Deleting Kind cluster '${CLUSTER_NAME}'..."
    kind delete cluster --name "${CLUSTER_NAME}"
    log_info "Cluster deleted successfully"
else
    log_info "Cluster '${CLUSTER_NAME}' does not exist"
fi
