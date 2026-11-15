# Kubernetes Infrastructure (Kind + ClickHouse Operator)

Alternative to Docker Compose for running ClickHouse locally using Kubernetes.

## Prerequisites

- [kind](https://kind.sigs.k8s.io/docs/user/quick-start/#installation) - Kubernetes in Docker
- [kubectl](https://kubernetes.io/docs/tasks/tools/) - Kubernetes CLI
- Docker, Node.js/npm, and `just` (also used to build the local Grafana app)

### Quick Install (macOS)

```bash
brew install kind kubectl
```

## Quick Start

```bash
just k8s-start

# Check status
just k8s-status

# Connect to ClickHouse
just k8s-connect

# Tear down
just k8s-stop
```

## Architecture

This setup uses the official [ClickHouse Kubernetes Operator](https://clickhouse.com/docs/clickhouse-operator/overview) which provides:

- Declarative cluster management via CRDs
- ClickHouse Keeper for distributed coordination
- Automatic lifecycle management (scaling, upgrades)
- Persistent storage via PVCs

### Components

```
┌─────────────────────────────────────────────────────────┐
│                    Kind Cluster                          │
│  ┌─────────────────────────────────────────────────────┐│
│  │              clickhouse-operator                     ││
│  │         (manages ClickHouse resources)               ││
│  └─────────────────────────────────────────────────────┘│
│  ┌─────────────────┐  ┌─────────────────────────────────┐│
│  │  KeeperCluster  │  │      ClickHouseCluster          ││
│  │   (1 replica)   │  │        (1 replica)              ││
│  │                 │  │                                 ││
│  │  Port: 2181     │  │  Native: 9000 (NodePort 30900)  ││
│  │                 │  │  HTTP:   8123 (NodePort 30123)  ││
│  │  Metrics: 9363 (NodePort 30363) ││
│  ┌─────────────────┐  ┌─────────────────────────────────┐│
│  │   Prometheus     │  │         Grafana                 ││
│  │   Port: 9090     │  │   Port: 3000 (NodePort 30301)   ││
│  │  (NodePort 30090)│  │                                 ││
│  └─────────────────┘  └─────────────────────────────────┘│
│  └─────────────────┘  └─────────────────────────────────┘│
└─────────────────────────────────────────────────────────┘
```

## Connecting from Host

After deployment, ClickHouse is accessible at:

- **Native protocol**: `localhost:9000`
- **HTTP interface**: `localhost:8123`
- **Prometheus**: `localhost:30090`
- **Grafana**: `localhost:3001`

Grafana includes the locally built TraceHouse app, enabled automatically and
connected through the provisioned ClickHouse datasource. Open
<http://localhost:3001/a/dmkskd-tracehouse-app> after startup.

`just k8s-start`, `just k8s-start-altinity`, and `just k8s-start-altinity-large`
build the plugin, package it into a local Grafana image, load that image into
Kind, and wait for Grafana's rollout. This also updates existing clusters;
no cluster recreation or separate Grafana test stack is needed.

To rebuild the app and refresh only Grafana after editing the frontend:

```bash
just k8s-grafana
```

This restarts Grafana but leaves ClickHouse running. The deployment uses the
local unsigned plugin build and retains the existing anonymous Admin access
for this development environment.

The regular and large setups share the Kind cluster name `tracehouse-dev`.
Startup refuses to reuse a cluster with a different node count: the regular
configuration has four workers, while `k8s-start-altinity-large` requires eight.
Switching between these configurations requires backing up any needed data,
then running `just k8s-stop` before the intended startup command. Stopping the
Kind cluster deletes its local data.

```bash
# Using clickhouse client
clickhouse client --host localhost --port 9000

# Using HTTP
curl "http://localhost:8123/?query=SELECT%201"
```

## Files

| File | Description |
|------|-------------|
| `kind-config.yaml` | Kind cluster configuration with port mappings |
| `namespace.yaml` | Kubernetes namespace for ClickHouse resources |
| `keeper-cluster.yaml` | ClickHouse Keeper cluster (coordination) |
| `clickhouse-cluster.yaml` | ClickHouse server cluster |
| `setup.sh` | Automated setup script |
| `prometheus.yaml` | Prometheus deployment and config |
| `grafana.yaml` | Grafana deployment with datasources |

## Customization

### Scaling

Edit `clickhouse-cluster.yaml`:

```yaml
spec:
  replicas: 3  # Increase replicas
  shards: 2    # Add shards for horizontal scaling
```

### Storage

Modify storage size in cluster manifests:

```yaml
dataVolumeClaimSpec:
  resources:
    requests:
      storage: 50Gi  # Increase from default 10Gi
```

## Troubleshooting

```bash
# View operator logs
kubectl logs -n clickhouse-system -l app.kubernetes.io/name=clickhouse-operator

# View ClickHouse logs
kubectl logs -n clickhouse -l app.kubernetes.io/name=clickhouse

# Describe cluster status
kubectl describe clickhousecluster -n clickhouse dev-cluster

# Get all resources
kubectl get all -n clickhouse
```
