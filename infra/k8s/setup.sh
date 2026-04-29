#!/bin/bash
# ClickHouse Kubernetes Setup Script
# Creates a Kind cluster with ClickHouse Operator and a dev cluster

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
CLUSTER_NAME="tracehouse-dev"

# Default operator: clickhouse (ClickHouse Cloud operator)
# Override with: ./setup.sh --operator altinity
OPERATOR="clickhouse"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --operator)
            OPERATOR="$2"
            shift 2
            ;;
        *)
            shift
            ;;
    esac
done

if [[ "$OPERATOR" != "clickhouse" && "$OPERATOR" != "altinity" ]]; then
    echo "Unknown operator: $OPERATOR (use 'clickhouse' or 'altinity')"
    exit 1
fi

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    local missing=()
    
    if ! command -v kind &> /dev/null; then
        missing+=("kind")
    fi
    
    if ! command -v kubectl &> /dev/null; then
        missing+=("kubectl")
    fi
    
    if [ ${#missing[@]} -ne 0 ]; then
        log_error "Missing required tools: ${missing[*]}"
        echo ""
        echo "Install on macOS with:"
        echo "  brew install ${missing[*]}"
        exit 1
    fi
    
    log_info "All prerequisites satisfied"
}

# Create Kind cluster
create_cluster() {
    if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
        log_warn "Cluster '${CLUSTER_NAME}' already exists"
        return 0
    fi
    
    log_info "Creating Kind cluster '${CLUSTER_NAME}'..."
    kind create cluster --config "${SCRIPT_DIR}/kind-config.yaml"
    
    log_info "Waiting for cluster to be ready..."
    kubectl wait --for=condition=Ready nodes --all --timeout=120s
}

# Install cert-manager (required for operator webhooks)
install_cert_manager() {
    if kubectl get namespace cert-manager &> /dev/null; then
        log_warn "cert-manager already installed"
        return 0
    fi
    
    log_info "Installing cert-manager..."
    kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.16.2/cert-manager.yaml
    
    log_info "Waiting for cert-manager to be ready..."
    kubectl wait --for=condition=Available deployment --all -n cert-manager --timeout=180s
}

# Install ClickHouse Operator
install_operator() {
    if [[ "$OPERATOR" == "altinity" ]]; then
        install_altinity_operator
    else
        install_clickhouse_operator
    fi
}

install_clickhouse_operator() {
    if kubectl get namespace clickhouse-operator-system &> /dev/null; then
        log_warn "ClickHouse Cloud operator already installed"
        return 0
    fi
    
    log_info "Installing ClickHouse Cloud Operator..."
    
    # Install from official release
    kubectl apply -f https://github.com/ClickHouse/clickhouse-operator/releases/latest/download/clickhouse-operator.yaml
    
    log_info "Waiting for operator to be ready..."
    sleep 10
    kubectl wait --for=condition=Available deployment -n clickhouse-operator-system --all --timeout=180s
}

install_altinity_operator() {
    if kubectl get namespace kube-system -o jsonpath='{.metadata.labels}' 2>/dev/null | grep -q "altinity"; then
        log_warn "Altinity operator already installed"
        return 0
    fi

    log_info "Installing Altinity ClickHouse Operator..."

    kubectl apply -f https://raw.githubusercontent.com/Altinity/clickhouse-operator/master/deploy/operator/clickhouse-operator-install-bundle.yaml

    log_info "Waiting for Altinity operator to be ready..."
    sleep 10
    kubectl wait --for=condition=Available deployment/clickhouse-operator -n kube-system --timeout=180s
}

# Deploy MinIO (S3-compatible object storage) — shared across all operators
deploy_minio() {
    log_info "Deploying MinIO..."
    kubectl apply -f "${SCRIPT_DIR}/minio.yaml"

    log_info "Waiting for MinIO to be ready..."
    kubectl wait --for=condition=Available deployment/minio -n clickhouse --timeout=120s || true

    log_info "Waiting for MinIO bucket init..."
    kubectl wait --for=condition=Complete job/minio-init -n clickhouse --timeout=60s || log_warn "MinIO init job not complete yet"
}

# Deploy Lakekeeper (Iceberg REST catalog) — shared across all operators
deploy_lakekeeper() {
    log_info "Deploying Lakekeeper (Iceberg REST catalog)..."
    kubectl apply -f "${SCRIPT_DIR}/lakekeeper.yaml"

    log_info "Waiting for Lakekeeper DB to be ready..."
    kubectl wait --for=condition=Available deployment/lakekeeper-db -n clickhouse --timeout=120s || true

    log_info "Waiting for Lakekeeper migration..."
    kubectl wait --for=condition=Complete job/lakekeeper-migrate -n clickhouse --timeout=120s || log_warn "Lakekeeper migration not complete yet"

    log_info "Waiting for Lakekeeper to be ready..."
    kubectl wait --for=condition=Available deployment/lakekeeper -n clickhouse --timeout=120s || true

    log_info "Waiting for Lakekeeper warehouse init..."
    kubectl wait --for=condition=Complete job/lakekeeper-init -n clickhouse --timeout=60s || log_warn "Lakekeeper init not complete yet"
}

# Deploy ClickHouse cluster using ClickHouse Cloud operator
deploy_clickhouse_cloud() {
    log_info "Deploying Keeper cluster (Cloud operator)..."
    kubectl apply -f "${SCRIPT_DIR}/keeper-cluster.yaml"
    
    log_info "Waiting for Keeper to be ready..."
    sleep 15
    kubectl wait --for=condition=Ready pod -l app=dev-keeper-keeper -n clickhouse --timeout=300s || true
    
    log_info "Creating S3 storage ConfigMap..."
    kubectl apply -f "${SCRIPT_DIR}/s3-storage-config.yaml"

    # TLS certificate via cert-manager (self-signed for dev)
    log_info "Creating TLS certificate via cert-manager..."
    kubectl apply -f - <<'EOF'
apiVersion: cert-manager.io/v1
kind: Issuer
metadata:
  name: clickhouse-selfsigned
  namespace: clickhouse
spec:
  selfSigned: {}
---
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: clickhouse-tls
  namespace: clickhouse
spec:
  secretName: clickhouse-tls
  duration: 8760h # 1 year
  issuerRef:
    name: clickhouse-selfsigned
    kind: Issuer
  commonName: clickhouse
  dnsNames:
    - localhost
    - "*.clickhouse.svc.cluster.local"
    - "*.clickhouse-headless.clickhouse.svc.cluster.local"
  ipAddresses:
    - "127.0.0.1"
EOF
    log_info "Waiting for TLS certificate to be ready..."
    kubectl wait --for=condition=Ready certificate/clickhouse-tls -n clickhouse --timeout=60s || log_warn "Certificate not ready yet"

    kubectl apply -f "${SCRIPT_DIR}/clickhouse-cluster.yaml"
    
    log_info "Waiting for ClickHouse to be ready..."
    sleep 15

    # FIXME: Fix disks/ directory ownership (created by root, needs to be owned by clickhouse uid 101)
    log_info "Fixing disks/ permissions on ClickHouse PVCs..."
    PVCS=$(kubectl get pvc -n clickhouse -l app.kubernetes.io/name=clickhouse -o jsonpath='{.items[*].metadata.name}' 2>/dev/null)
    if [ -z "$PVCS" ]; then
        # Fallback: grab all clickhouse-storage-volume PVCs
        PVCS=$(kubectl get pvc -n clickhouse -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null | grep "clickhouse-storage-volume")
    fi
    IDX=0
    for PVC in $PVCS; do
        log_info "  Fixing permissions on PVC: $PVC"
        kubectl run "fix-perms-${IDX}" --rm -i --restart=Never -n clickhouse --image=busybox \
            --overrides="{\"spec\":{\"containers\":[{\"name\":\"fix\",\"image\":\"busybox\",\"command\":[\"sh\",\"-c\",\"mkdir -p /data/var-lib-clickhouse/disks && chown 101:101 /data/var-lib-clickhouse/disks\"],\"volumeMounts\":[{\"name\":\"data\",\"mountPath\":\"/data\"}]}],\"volumes\":[{\"name\":\"data\",\"persistentVolumeClaim\":{\"claimName\":\"${PVC}\"}}]}}" \
            2>/dev/null || true
        IDX=$((IDX + 1))
    done

    # Restart CH pods to pick up fixed permissions
    kubectl delete pods -n clickhouse -l app=dev-cluster-clickhouse 2>/dev/null || true
    sleep 15

    kubectl wait --for=condition=Ready pod -l app=dev-cluster-clickhouse -n clickhouse --timeout=300s || true
}

# Deploy ClickHouse cluster using Altinity operator
deploy_clickhouse_altinity() {
    log_info "Deploying Keeper (Altinity ClickHouseKeeperInstallation)..."
    kubectl apply -f "${SCRIPT_DIR}/altinity/keeper.yaml"

    log_info "Waiting for Keeper CRD to be Completed..."
    sleep 15
    # Wait for the ClickHouseKeeperInstallation to reach Completed status
    for i in $(seq 1 30); do
        STATUS=$(kubectl get chk dev-keeper -n clickhouse -o jsonpath='{.status.status}' 2>/dev/null || echo "")
        if [ "$STATUS" = "Completed" ]; then
            log_info "Keeper is ready"
            break
        fi
        log_info "  Keeper status: ${STATUS:-pending} (attempt $i/30)"
        sleep 10
    done

    log_info "Deploying ClickHouse cluster (Altinity operator)..."
    kubectl apply -f "${SCRIPT_DIR}/altinity/clickhouse-installation.yaml"

    log_info "Waiting for ClickHouse to be ready..."
    sleep 20
    # Wait for the ClickHouseInstallation to reach Completed status
    for i in $(seq 1 30); do
        STATUS=$(kubectl get chi dev-cluster -n clickhouse -o jsonpath='{.status.status}' 2>/dev/null || echo "")
        if [ "$STATUS" = "Completed" ]; then
            log_info "ClickHouse cluster is ready"
            break
        fi
        log_info "  ClickHouse status: ${STATUS:-pending} (attempt $i/30)"
        sleep 10
    done
}

# Deploy ClickHouse cluster
deploy_clickhouse() {
    log_info "Creating namespace..."
    kubectl apply -f "${SCRIPT_DIR}/namespace.yaml"

    deploy_minio

    if [[ "$OPERATOR" == "altinity" ]]; then
        deploy_clickhouse_altinity
    else
        deploy_clickhouse_cloud
    fi

    deploy_lakekeeper

    # Wait for ALL ClickHouse pods to be ready before running DDL.
    # The operator may report "Completed" before all nodes can accept connections,
    # and ON CLUSTER DDL silently skips unreachable nodes.
    log_info "Waiting for all ClickHouse pods to be ready..."
    if [[ "$OPERATOR" == "altinity" ]]; then
        kubectl wait --for=condition=Ready pod -l "clickhouse.altinity.com/chi=dev-cluster" -n clickhouse --timeout=300s || true
    else
        kubectl wait --for=condition=Ready pod -l app=dev-cluster-clickhouse -n clickhouse --timeout=300s || true
    fi

    # Create read-only user via SQL
    log_info "Creating read_only user..."
    local CH_POD
    if [[ "$OPERATOR" == "altinity" ]]; then
        CH_POD=$(kubectl get pods -n clickhouse -l "clickhouse.altinity.com/chi=dev-cluster" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
    else
        CH_POD=$(kubectl get pods -n clickhouse -l app=dev-cluster-clickhouse -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
    fi
    if [ -n "$CH_POD" ]; then
        # Detect container name (Altinity: "clickhouse", ClickHouse.com: "clickhouse-server")
        local CH_CONTAINER
        CH_CONTAINER=$(kubectl get pod -n clickhouse "$CH_POD" -o jsonpath='{.spec.containers[0].name}' 2>/dev/null || echo "clickhouse")

        kubectl exec -n clickhouse -c "$CH_CONTAINER" "$CH_POD" -- clickhouse client --multiquery < "${SCRIPT_DIR}/../scripts/setup_read_only_user.sql" && \
            log_info "read_only user created" || \
            log_warn "read_only user setup skipped"

        # Enable profiling and introspection for the default user
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

        # Wait for the cluster config to be fully populated by the operator.
        # The operator rolls out pods incrementally — early pods see a partial
        # cluster config in system.clusters. ON CLUSTER DDL only reaches nodes
        # listed there, so we must wait until all replicas appear.
        local EXPECTED_NODES=0
        local DETECTED_CLUSTER=""
        if [[ "$OPERATOR" == "altinity" ]]; then
            local CHI_SHARDS CHI_REPLICAS
            CHI_SHARDS=$(kubectl get chi dev-cluster -n clickhouse -o jsonpath='{.spec.configuration.clusters[0].layout.shardsCount}' 2>/dev/null || echo "0")
            CHI_REPLICAS=$(kubectl get chi dev-cluster -n clickhouse -o jsonpath='{.spec.configuration.clusters[0].layout.replicasCount}' 2>/dev/null || echo "0")
            EXPECTED_NODES=$((CHI_SHARDS * CHI_REPLICAS))
            DETECTED_CLUSTER="dev"
            if [[ "$EXPECTED_NODES" -gt 0 ]]; then
                log_info "CHI defines ${CHI_SHARDS}s×${CHI_REPLICAS}r = $EXPECTED_NODES nodes. Waiting for cluster config..."
            fi
        else
            local CHC_SHARDS CHC_REPLICAS
            CHC_SHARDS=$(kubectl get clickhousecluster dev-cluster -n clickhouse -o jsonpath='{.spec.shards}' 2>/dev/null || echo "0")
            CHC_REPLICAS=$(kubectl get clickhousecluster dev-cluster -n clickhouse -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "0")
            EXPECTED_NODES=$((CHC_SHARDS * CHC_REPLICAS))
            if [[ "$EXPECTED_NODES" -gt 0 ]]; then
                log_info "ClickHouseCluster defines ${CHC_SHARDS}s×${CHC_REPLICAS}r = $EXPECTED_NODES nodes. Waiting for cluster config..."
            fi
        fi

        if [[ "$EXPECTED_NODES" -gt 0 ]]; then
            for i in $(seq 1 60); do
                # For the CH.com operator, detect cluster name from system.clusters
                # (pick the largest non-internal cluster). For Altinity, we already know it's 'dev'.
                if [[ -z "$DETECTED_CLUSTER" ]]; then
                    DETECTED_CLUSTER=$(kubectl exec -n clickhouse -c "$CH_CONTAINER" "$CH_POD" -- \
                        clickhouse client --query "SELECT cluster FROM system.clusters GROUP BY cluster ORDER BY count() DESC LIMIT 1" 2>/dev/null || echo "")
                fi
                local ACTUAL
                if [[ -n "$DETECTED_CLUSTER" ]]; then
                    ACTUAL=$(kubectl exec -n clickhouse -c "$CH_CONTAINER" "$CH_POD" -- \
                        clickhouse client --query "SELECT count() FROM system.clusters WHERE cluster = '$DETECTED_CLUSTER'" 2>/dev/null || echo "0")
                else
                    ACTUAL=0
                fi
                if [[ "$ACTUAL" -ge "$EXPECTED_NODES" ]]; then
                    log_info "Cluster config ready: $ACTUAL nodes in '$DETECTED_CLUSTER' cluster"
                    break
                fi
                log_info "  system.clusters shows ${ACTUAL}/${EXPECTED_NODES} nodes (attempt $i/60)"
                sleep 5
            done
        fi

        # Setup process sampling
        log_info "Setting up process sampling..."
        kubectl cp -c "$CH_CONTAINER" "${SCRIPT_DIR}/../scripts/setup_sampling.sh" "clickhouse/${CH_POD}:/tmp/setup_sampling.sh"
        local SAMPLING_CLUSTER=""
        if [[ -n "$DETECTED_CLUSTER" ]]; then
            SAMPLING_CLUSTER="--cluster $DETECTED_CLUSTER"
        fi
        local SAMPLING_ARGS="--host localhost --yes $SAMPLING_CLUSTER"
        kubectl exec -n clickhouse -c "$CH_CONTAINER" "$CH_POD" -- bash /tmp/setup_sampling.sh $SAMPLING_ARGS && \
            log_info "Process sampling configured" || \
            log_warn "Process sampling setup skipped"
    fi
    
    log_info "Deploying Prometheus..."
    kubectl apply -f "${SCRIPT_DIR}/prometheus.yaml"
    
    log_info "Waiting for Prometheus to be ready..."
    kubectl wait --for=condition=Available deployment/prometheus -n clickhouse --timeout=120s || true

    log_info "Deploying Grafana..."
    kubectl apply -f "${SCRIPT_DIR}/grafana-dashboards-cm.yaml"
    kubectl apply -f "${SCRIPT_DIR}/grafana.yaml"
    
    log_info "Waiting for Grafana to be ready..."
    kubectl wait --for=condition=Available deployment/grafana -n clickhouse --timeout=120s || true
}

# Print connection info
print_info() {
    echo ""
    echo "═══════════════════════════════════════════════════════════════"
    echo -e "${GREEN}ClickHouse Kubernetes cluster is ready! (${OPERATOR} operator)${NC}"
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
    echo ""
    echo "Useful commands:"
    echo "  just k8s-status    # Check cluster status"
    echo "  just k8s-connect   # Open ClickHouse client"
    echo "  just k8s-logs      # View ClickHouse logs"
    echo "  just k8s-stop      # Tear down cluster"
    echo ""
}

# Main
main() {
    check_prerequisites
    create_cluster
    install_cert_manager
    install_operator
    deploy_clickhouse
    print_info
}

main "$@"
