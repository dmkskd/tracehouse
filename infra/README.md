# Infrastructure

> 📖 For a complete getting-started walkthrough, see [docs/site/docs/getting-started.md](../docs/site/docs/getting-started.md).

Three options for running ClickHouse locally:

1. **Local Binary** — Simplest, no containers, just ClickHouse
2. **Docker Compose** — Full stack with Prometheus + Grafana
3. **Kubernetes (Kind)** — Multi-replica setup, closest to production

Both setups provide the same services:

| Service | Local Binary | Docker Compose | K8s (Kind) |
|---------|--------------|----------------|------------|
| ClickHouse Native | `localhost:9000` | `localhost:9000` | `localhost:9000` (NodePort 30900) |
| ClickHouse HTTP | `localhost:8123` | `localhost:8123` | `localhost:8123` (NodePort 30123) |
| ClickHouse Metrics | `localhost:9363/metrics` | `localhost:9363/metrics` | `localhost:30363/metrics` |
| Prometheus | — | `localhost:9090` | `localhost:30090` |
| Grafana | — | `localhost:3001` | `localhost:3001` (NodePort 30301) |

## Local Binary (No Containers)

Fastest way to get ClickHouse running. Uses the same configuration as Docker/K8s.

```bash
cd infra/local

# Setup and start
./setup.sh

# Stop
./stop.sh
```

Installs ClickHouse if needed, creates data/logs directories, and starts the server with all the monitoring configs (OpenTelemetry, Prometheus metrics, text_log).

See `local/README.md` for details.

## Docker Compose

Simplest way to get started. Runs ClickHouse, Prometheus, and Grafana.

```bash
# Start everything
just docker-start

# Or background + app services
just dev

# Stop
just docker-stop
```

The canonical server and Keeper images live in `infra/clickhouse.env`.
Repository-managed `just` commands load that file automatically. Commands run
directly with Docker Compose use `latest` unless an image is supplied.

Override either full image for one command without editing tracked files:

```bash
CLICKHOUSE_IMAGE=clickhouse/clickhouse-server:23.8.16.40-alpine just test-clickhouse
CLICKHOUSE_IMAGE=altinity/clickhouse-server:26.3.16.10001.altinitystable just test-clickhouse
CLICKHOUSE_KEEPER_IMAGE=clickhouse/clickhouse-keeper:26.6.4 just k8s-start
```

`just clickhouse-images` prints the two effective references. Overrides are
process-local; these commands do not rewrite the defaults or Kubernetes
manifests.

### What's included

- The ClickHouse version pinned in `infra/clickhouse.env`, with Prometheus metrics enabled
- Prometheus scraping ClickHouse every 15s
- Grafana with ClickHouse and Prometheus datasources auto-provisioned
- Persistent volumes for data, logs, and Grafana state

### Files

| Path | Description |
|------|-------------|
| `clickhouse.env` | Canonical ClickHouse server and Keeper images |
| `docker/docker-compose.yml` | Service definitions |
| `docker/clickhouse-config/prometheus.xml` | Enables ClickHouse `/metrics` endpoint on port 9363 |
| `docker/prometheus/prometheus.yml` | Prometheus scrape config |
| `docker/grafana/provisioning/datasources/` | Auto-provisioned Grafana datasources |
| `docker/grafana/dashboards/` | Grafana dashboard JSON files |

## Kubernetes (Kind)

Uses the official ClickHouse Operator for declarative cluster management. Good for testing multi-replica setups.

```bash
# Full setup (creates Kind cluster, installs operator, deploys everything)
just k8s-up

# Status
just k8s-status

# Connect to ClickHouse
just k8s-connect

# Tear down
just k8s-down
```

### Prerequisites

```bash
brew install kind kubectl helm
```

### Architecture

```
Kind Cluster
├── cert-manager (operator dependency)
├── clickhouse-operator (manages CH resources)
├── clickhouse namespace
│   ├── KeeperCluster (1 replica, coordination)
│   ├── ClickHouseCluster (2 replicas, 1 shard)
│   ├── Prometheus (scrapes CH metrics)
│   └── Grafana (dashboards)
```

See `k8s/README.md` for more details on the K8s setup.

## Test Data

```bash
# Quick generate (1M rows, many small parts)
just generate-data-quick

# Heavy generate (10M rows, triggers lots of merges)
just generate-data-heavy

# Custom
just generate-data 5000000 2 100000

# Generate query activity
just run-queries
```

## Grafana

Grafana starts with two datasources pre-configured:

- ClickHouse (default) — for querying system tables directly
- Prometheus — for time-series metrics scraped from ClickHouse

Open `http://localhost:3001` (no login required in dev mode).
