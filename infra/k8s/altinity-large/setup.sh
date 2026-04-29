#!/bin/bash
# ClickHouse Kubernetes Setup — altinity-large (4 shards × 2 replicas)
# Creates a Kind cluster with Altinity operator and an 8-node ClickHouse cluster

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
K8S_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
CLUSTER_NAME="tracehouse-dev"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

check_prerequisites() {
    log_info "Checking prerequisites..."
    local missing=()
    if ! command -v kind &> /dev/null; then missing+=("kind"); fi
    if ! command -v kubectl &> /dev/null; then missing+=("kubectl"); fi
    if [ ${#missing[@]} -ne 0 ]; then
        log_error "Missing required tools: ${missing[*]}"
        echo "  brew install ${missing[*]}"
        exit 1
    fi
    log_info "All prerequisites satisfied"
}

create_cluster() {
    if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
        log_warn "Cluster '${CLUSTER_NAME}' already exists"
        return 0
    fi

    log_info "Creating Kind cluster '${CLUSTER_NAME}' (8 workers)..."
    kind create cluster --config "${SCRIPT_DIR}/kind-config.yaml"

    log_info "Waiting for cluster to be ready..."
    kubectl wait --for=condition=Ready nodes --all --timeout=120s
}

install_altinity_operator() {
    if kubectl get deployment clickhouse-operator -n kube-system &> /dev/null; then
        log_warn "Altinity operator already installed"
        return 0
    fi

    log_info "Installing Altinity ClickHouse Operator..."
    kubectl apply -f https://raw.githubusercontent.com/Altinity/clickhouse-operator/master/deploy/operator/clickhouse-operator-install-bundle.yaml

    log_info "Waiting for Altinity operator to be ready..."
    sleep 10
    kubectl wait --for=condition=Available deployment/clickhouse-operator -n kube-system --timeout=180s
}

deploy_minio() {
    log_info "Deploying MinIO..."
    kubectl apply -f "${K8S_DIR}/minio.yaml"

    log_info "Waiting for MinIO to be ready..."
    kubectl wait --for=condition=Available deployment/minio -n clickhouse --timeout=120s || true

    log_info "Waiting for MinIO bucket init..."
    kubectl wait --for=condition=Complete job/minio-init -n clickhouse --timeout=60s || log_warn "MinIO init job not complete yet"
}

deploy_lakekeeper() {
    log_info "Deploying Lakekeeper (Iceberg REST catalog)..."
    kubectl apply -f "${K8S_DIR}/lakekeeper.yaml"

    log_info "Waiting for Lakekeeper DB to be ready..."
    kubectl wait --for=condition=Available deployment/lakekeeper-db -n clickhouse --timeout=120s || true

    log_info "Waiting for Lakekeeper migration..."
    kubectl wait --for=condition=Complete job/lakekeeper-migrate -n clickhouse --timeout=120s || log_warn "Lakekeeper migration not complete yet"

    log_info "Waiting for Lakekeeper to be ready..."
    kubectl wait --for=condition=Available deployment/lakekeeper -n clickhouse --timeout=120s || true

    log_info "Waiting for Lakekeeper warehouse init..."
    kubectl wait --for=condition=Complete job/lakekeeper-init -n clickhouse --timeout=60s || log_warn "Lakekeeper init not complete yet"
}

deploy_clickhouse() {
    log_info "Creating namespace..."
    kubectl apply -f "${K8S_DIR}/namespace.yaml"

    deploy_minio

    # Keeper (3 replicas)
    log_info "Deploying Keeper (3 replicas)..."
    kubectl apply -f "${SCRIPT_DIR}/keeper.yaml"

    log_info "Waiting for Keeper to be ready..."
    sleep 15
    for i in $(seq 1 30); do
        STATUS=$(kubectl get chk dev-keeper -n clickhouse -o jsonpath='{.status.status}' 2>/dev/null || echo "")
        if [ "$STATUS" = "Completed" ]; then
            log_info "Keeper is ready"
            break
        fi
        log_info "  Keeper status: ${STATUS:-pending} (attempt $i/30)"
        sleep 10
    done

    # ClickHouse cluster (4s × 2r)
    log_info "Deploying ClickHouse cluster (4 shards × 2 replicas)..."
    kubectl apply -f "${SCRIPT_DIR}/clickhouse-installation.yaml"

    log_info "Waiting for ClickHouse to be ready..."
    sleep 20
    for i in $(seq 1 30); do
        STATUS=$(kubectl get chi dev-cluster -n clickhouse -o jsonpath='{.status.status}' 2>/dev/null || echo "")
        if [ "$STATUS" = "Completed" ]; then
            log_info "ClickHouse cluster is ready"
            break
        fi
        log_info "  ClickHouse status: ${STATUS:-pending} (attempt $i/30)"
        sleep 10
    done

    # Wait for ALL pods to be ready before running DDL
    log_info "Waiting for all 8 ClickHouse pods to be ready..."
    kubectl wait --for=condition=Ready pod -l "clickhouse.altinity.com/chi=dev-cluster" -n clickhouse --timeout=300s || true

    deploy_lakekeeper

    # Setup users and sampling
    local CH_POD
    CH_POD=$(kubectl get pods -n clickhouse -l "clickhouse.altinity.com/chi=dev-cluster" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
    if [ -n "$CH_POD" ]; then
        local CH_CONTAINER
        CH_CONTAINER=$(kubectl get pod -n clickhouse "$CH_POD" -o jsonpath='{.spec.containers[0].name}' 2>/dev/null || echo "clickhouse")

        log_info "Creating read_only user..."
        kubectl exec -n clickhouse -c "$CH_CONTAINER" "$CH_POD" -- clickhouse client --multiquery < "${K8S_DIR}/../scripts/setup_read_only_user.sql" && \
            log_info "read_only user created" || \
            log_warn "read_only user setup skipped"

        log_info "Configuring default profile settings..."
        kubectl exec -n clickhouse -c "$CH_CONTAINER" "$CH_POD" -- clickhouse client --multiquery <<'EOF' && \
            log_info "Profile settings applied" || \
            log_warn "Profile settings skipped"
ALTER USER default SETTINGS
    allow_introspection_functions = 1,
    query_profiler_cpu_time_period_ns = 10000000,
    query_profiler_real_time_period_ns = 10000000,
    memory_profiler_step = 0,
    log_processors_profiles = 1;
EOF

        # Wait for cluster config to show all 8 nodes
        local EXPECTED_NODES=8
        log_info "Waiting for cluster config (expecting $EXPECTED_NODES nodes in 'dev' cluster)..."
        for i in $(seq 1 60); do
            ACTUAL=$(kubectl exec -n clickhouse -c "$CH_CONTAINER" "$CH_POD" -- \
                clickhouse client --query "SELECT count() FROM system.clusters WHERE cluster = 'dev'" 2>/dev/null || echo "0")
            if [[ "$ACTUAL" -ge "$EXPECTED_NODES" ]]; then
                log_info "Cluster config ready: $ACTUAL nodes in 'dev' cluster"
                break
            fi
            log_info "  system.clusters shows ${ACTUAL}/${EXPECTED_NODES} nodes (attempt $i/60)"
            sleep 5
        done

        # Setup process sampling
        log_info "Setting up process sampling..."
        kubectl cp -c "$CH_CONTAINER" "${K8S_DIR}/../scripts/setup_sampling.sh" "clickhouse/${CH_POD}:/tmp/setup_sampling.sh"
        kubectl exec -n clickhouse -c "$CH_CONTAINER" "$CH_POD" -- bash /tmp/setup_sampling.sh --host localhost --yes --cluster dev && \
            log_info "Process sampling configured" || \
            log_warn "Process sampling setup skipped"
    fi

    # Prometheus & Grafana
    log_info "Deploying Prometheus..."
    kubectl apply -f "${K8S_DIR}/prometheus.yaml"
    kubectl wait --for=condition=Available deployment/prometheus -n clickhouse --timeout=120s || true

    log_info "Deploying Grafana..."
    kubectl apply -f "${K8S_DIR}/grafana-dashboards-cm.yaml"
    kubectl apply -f "${K8S_DIR}/grafana.yaml"
    kubectl wait --for=condition=Available deployment/grafana -n clickhouse --timeout=120s || true
}

print_info() {
    echo ""
    echo "═══════════════════════════════════════════════════════════════"
    echo -e "${GREEN}altinity-large cluster is ready! (4 shards × 2 replicas)${NC}"
    echo "═══════════════════════════════════════════════════════════════"
    echo ""
    echo "Connection details:"
    echo "  ClickHouse Native: localhost:9000"
    echo "  ClickHouse HTTP:   localhost:8123"
    echo "  Grafana:           localhost:3001"
    echo "  Prometheus:        localhost:9090"
    echo "  Lakekeeper:        kubectl port-forward -n clickhouse svc/lakekeeper 8181:8181"
    echo "  MinIO Console:     kubectl port-forward -n clickhouse svc/minio 9002:9001"
    echo ""
    echo "Test connection:"
    echo "  curl 'http://localhost:8123/?query=SELECT%201'"
    echo "  curl 'http://localhost:8123/' --data-binary 'SELECT hostName(), count() FROM system.clusters WHERE cluster='\''dev'\'' GROUP BY 1'"
    echo ""
    echo "Useful commands:"
    echo "  just k8s-status     # Check cluster status"
    echo "  just k8s-connect    # Open ClickHouse client"
    echo "  just k8s-logs       # View ClickHouse logs"
    echo "  just k8s-stop       # Tear down cluster"
    echo ""
}

main() {
    check_prerequisites
    create_cluster
    install_altinity_operator
    deploy_clickhouse
    print_info
}

main "$@"
