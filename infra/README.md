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
docker-compose -f infra/docker/docker-compose.yml up

# Or background + app services
just dev

# Stop
docker-compose -f infra/docker/docker-compose.yml down
```

### What's included

- ClickHouse 26.1 with Prometheus metrics endpoint enabled
- Prometheus scraping ClickHouse every 15s
- Grafana with ClickHouse and Prometheus datasources auto-provisioned
- Persistent volumes for data, logs, and Grafana state

### Files

| Path | Description |
|------|-------------|
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
# Quick load (1M rows, many small parts)
just load-data-quick

# Heavy load (10M rows, triggers lots of merges)
just load-data-heavy

# Custom
just load-data 5000000 2 100000

# Generate query activity
just run-queries
```

## Grafana

Grafana starts with two datasources pre-configured:

- ClickHouse (default) — for querying system tables directly
- Prometheus — for time-series metrics scraped from ClickHouse

Open `http://localhost:3001` (no login required in dev mode).
