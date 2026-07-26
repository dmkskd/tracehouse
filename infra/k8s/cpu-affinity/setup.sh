#!/bin/bash
# CPU Affinity experiment — Kind cluster with static CPU Manager
#
# Proves that ClickHouse honours cgroup-assigned cores when the kubelet
# pins pods via the static CPU Manager policy (Guaranteed QoS, integer CPUs).
#
# Each ClickHouse replica gets 4 exclusive cores.  Verify with:
#   kubectl exec <pod> -n clickhouse -- cat /sys/fs/cgroup/cpuset.cpus.effective
#
# Uses: Altinity operator (more flexible pod templates)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
K8S_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
CLUSTER_NAME="tracehouse-cpu"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_step()  { echo -e "${CYAN}[STEP]${NC} $1"; }

# ─── Prerequisites ───────────────────────────────────────────────────────────

check_prerequisites() {
    log_info "Checking prerequisites..."

    local missing=()
    for tool in kind kubectl; do
        if ! command -v "$tool" &>/dev/null; then
            missing+=("$tool")
        fi
    done

    if [ ${#missing[@]} -ne 0 ]; then
        log_error "Missing required tools: ${missing[*]}"
        echo "  brew install ${missing[*]}"
        exit 1
    fi

    log_info "All prerequisites satisfied"
}

# ─── Cluster ─────────────────────────────────────────────────────────────────

create_cluster() {
    if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
        log_warn "Cluster '${CLUSTER_NAME}' already exists"
        return 0
    fi

    log_step "Creating Kind cluster '${CLUSTER_NAME}' with static CPU Manager..."
    kind create cluster --config "${SCRIPT_DIR}/kind-config.yaml"

    log_info "Waiting for nodes to be ready..."
    kubectl wait --for=condition=Ready nodes --all --timeout=120s
}

# Pin Kind worker containers to distinct host core sets via docker cpuset.
# This is the HOST-level isolation layer — the k8s CPU Manager does the
# pod-level pinning within each worker's assigned cores.
pin_worker_cores() {
    log_step "Pinning Kind worker containers to distinct host cores..."

    local TOTAL_CORES
    TOTAL_CORES=$(sysctl -n hw.ncpu 2>/dev/null || nproc)

    # Reserve cores 0-1 for control-plane / system; split the rest 50/50
    local WORKER_CORES=$(( TOTAL_CORES - 2 ))
    local HALF=$(( WORKER_CORES / 2 ))

    local W1_START=2
    local W1_END=$(( W1_START + HALF - 1 ))
    local W2_START=$(( W1_END + 1 ))
    local W2_END=$(( W2_START + HALF - 1 ))

    log_info "  ${TOTAL_CORES} host cores detected"
    log_info "  worker-1 → cores ${W1_START}-${W1_END}"
    log_info "  worker-2 → cores ${W2_START}-${W2_END}"

    docker update --cpuset-cpus "${W1_START}-${W1_END}" "${CLUSTER_NAME}-worker"  || log_warn "Could not pin worker-1"
    docker update --cpuset-cpus "${W2_START}-${W2_END}" "${CLUSTER_NAME}-worker2" || log_warn "Could not pin worker-2"

    # Restart so the kubelet re-reads the cgroup cpuset
    docker restart "${CLUSTER_NAME}-worker" "${CLUSTER_NAME}-worker2" >/dev/null 2>&1 || true
    sleep 5
    kubectl wait --for=condition=Ready nodes --all --timeout=120s
}

# ─── Altinity Operator ───────────────────────────────────────────────────────

install_operator() {
    if kubectl get deployment clickhouse-operator -n kube-system &>/dev/null; then
        log_warn "Altinity operator already installed"
        return 0
    fi

    log_step "Installing Altinity ClickHouse Operator..."
    kubectl apply -f https://raw.githubusercontent.com/Altinity/clickhouse-operator/master/deploy/operator/clickhouse-operator-install-bundle.yaml

    log_info "Waiting for operator to be ready..."
    sleep 10
    kubectl wait --for=condition=Available deployment/clickhouse-operator -n kube-system --timeout=180s
}

# ─── ClickHouse ──────────────────────────────────────────────────────────────

deploy_clickhouse() {
    log_step "Creating namespace..."
    kubectl apply -f "${K8S_DIR}/namespace.yaml"

    log_info "Deploying Keeper..."
    kubectl apply -f "${SCRIPT_DIR}/keeper.yaml"

    log_info "Waiting for Keeper..."
    sleep 15
    for i in $(seq 1 30); do
        STATUS=$(kubectl get chk cpu-keeper -n clickhouse -o jsonpath='{.status.status}' 2>/dev/null || echo "")
        if [ "$STATUS" = "Completed" ]; then
            log_info "Keeper is ready"
            break
        fi
        log_info "  Keeper status: ${STATUS:-pending} (attempt $i/30)"
        sleep 10
    done

    log_info "Deploying ClickHouse (Guaranteed QoS — 4 exclusive cores per replica)..."
    kubectl apply -f "${SCRIPT_DIR}/clickhouse-installation.yaml"

    log_info "Waiting for ClickHouse..."
    sleep 20
    for i in $(seq 1 30); do
        STATUS=$(kubectl get chi cpu-cluster -n clickhouse -o jsonpath='{.status.status}' 2>/dev/null || echo "")
        if [ "$STATUS" = "Completed" ]; then
            log_info "ClickHouse cluster is ready"
            break
        fi
        log_info "  ClickHouse status: ${STATUS:-pending} (attempt $i/30)"
        sleep 10
    done

    # Apply profiling settings
    local CH_POD
    CH_POD=$(kubectl get pods -n clickhouse -l "clickhouse.altinity.com/chi=cpu-cluster" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)

    if [ -n "$CH_POD" ]; then
        log_info "Configuring profiling settings..."
        kubectl exec -n clickhouse "$CH_POD" -- clickhouse client --multiquery <<'EOF' && \
            log_info "Profile settings applied" || \
            log_warn "Profile settings skipped"
ALTER USER default SETTINGS
    allow_introspection_functions = 1,
    query_profiler_cpu_time_period_ns = 10000000,
    query_profiler_real_time_period_ns = 10000000,
    memory_profiler_step = 0,
    log_processors_profiles = 1;
EOF

        # Setup process sampling (auto-detects cluster topology)
        log_info "Setting up process sampling..."
        kubectl cp "${REPO_ROOT}/infra/scripts/setup_sampling.sh" "clickhouse/${CH_POD}:/tmp/setup_sampling.sh"
        kubectl exec -n clickhouse "$CH_POD" -- bash /tmp/setup_sampling.sh --host localhost --yes && \
            log_info "Process sampling configured" || \
            log_warn "Process sampling setup skipped"
    fi
}

# ─── Verification ────────────────────────────────────────────────────────────

verify_pinning() {
    log_step "Verifying CPU pinning..."

    local PODS
    PODS=$(kubectl get pods -n clickhouse -l "clickhouse.altinity.com/chi=cpu-cluster" \
        -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')

    for POD in $PODS; do
        local NODE CPUSET NPROC
        NODE=$(kubectl get pod "$POD" -n clickhouse -o jsonpath='{.spec.nodeName}')

        # Read the effective cpuset from within the container
        CPUSET=$(kubectl exec -n clickhouse "$POD" -- \
            cat /sys/fs/cgroup/cpuset.cpus.effective 2>/dev/null || \
            kubectl exec -n clickhouse "$POD" -- \
            cat /sys/fs/cgroup/cpuset/cpuset.cpus 2>/dev/null || \
            echo "unknown")

        NPROC=$(kubectl exec -n clickhouse "$POD" -- nproc 2>/dev/null || echo "?")

        echo -e "  ${GREEN}${POD}${NC}  node=${NODE}  cpuset=${CPUSET}  nproc=${NPROC}"
    done

    echo ""
    log_info "If cpuset shows distinct core ranges per pod, CPU pinning is active."
    log_info "ClickHouse should report the same core count in:"
    log_info "  SELECT value FROM system.settings WHERE name = 'max_threads'"
}

# ─── Info ────────────────────────────────────────────────────────────────────

print_info() {
    echo ""
    echo "═══════════════════════════════════════════════════════════════"
    echo -e "${GREEN}CPU Affinity cluster is ready!${NC}"
    echo "═══════════════════════════════════════════════════════════════"
    echo ""
    echo "Connection:"
    echo "  ClickHouse Native: localhost:9000"
    echo "  ClickHouse HTTP:   localhost:8123"
    echo ""
    echo "Verify CPU pinning:"
    echo "  kubectl exec <pod> -n clickhouse -- cat /sys/fs/cgroup/cpuset.cpus.effective"
    echo "  SELECT value FROM system.settings WHERE name = 'max_threads'"
    echo ""
    echo "Run a CPU-heavy query and check the Core Timeline in tracehouse"
    echo "to confirm activity is restricted to the assigned cores."
    echo ""
    echo "Teardown:"
    echo "  kind delete cluster --name ${CLUSTER_NAME}"
    echo ""
}

# ─── Main ────────────────────────────────────────────────────────────────────

main() {
    check_prerequisites
    create_cluster
    pin_worker_cores
    install_operator
    deploy_clickhouse
    verify_pinning
    print_info
}

main "$@"
