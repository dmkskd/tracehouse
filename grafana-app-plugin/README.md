# TraceHouse for Grafana

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Grafana](https://img.shields.io/badge/Grafana->=10.0.0-orange)](https://grafana.com)

Monitor ClickHouse merges, queries, parts, replication, and storage from Grafana.

## What is TraceHouse?

TraceHouse queries ClickHouse `system.*` tables (parts, merges, queries, replication queue, etc.) through the Grafana ClickHouse datasource. No external agents needed.

## Features

### Database Explorer

Browse databases, tables, columns, and storage details.

![Database Explorer](https://raw.githubusercontent.com/dmkskd/tracehouse/main/grafana-app-plugin/src/img/screenshots/database-explorer.png)

### Time Travel

Replay how tables, parts, and merges evolved over any time window. See when parts were created, merged, or moved.

![Time Travel](https://raw.githubusercontent.com/dmkskd/tracehouse/main/grafana-app-plugin/src/img/screenshots/time-travel.png)

### Query History

Query breakdown by user, query kind, and status. Identify slow queries, see normalized query patterns, and link from query history to time-travel views.

![Query History](https://raw.githubusercontent.com/dmkskd/tracehouse/main/grafana-app-plugin/src/img/screenshots/history.png)

### System Map

Visual overview of table sizes, part counts, and storage distribution across your cluster.

![System Map](https://raw.githubusercontent.com/dmkskd/tracehouse/main/grafana-app-plugin/src/img/screenshots/system-map.png)

### 3D Analytics

3D surface visualizations for query and merge patterns over time.

![Analytics](https://raw.githubusercontent.com/dmkskd/tracehouse/main/grafana-app-plugin/src/img/screenshots/analytics.png)

### Overview Dashboard

Cluster health at a glance: CPU, memory, active queries, running merges, and replication lag across all nodes.

### Merge Tracker

Real-time merge monitoring with per-merge progress bars, throughput, elapsed/remaining time, and X-ray detail view.

### Engine Internals

Inspect MergeTree parts, partitions, storage policies, and data distribution.

### Cluster Topology

Multi-node view with shard/replica status and per-node resource utilization.

### Replication

Replication queue depth, log lag, and inter-replica sync status.

## Requirements

- Grafana >= 10.0.0
- ClickHouse 21.8+ with `system.*` tables accessible
- [Grafana ClickHouse datasource](https://grafana.com/grafana/plugins/grafana-clickhouse-datasource/) plugin installed and configured

## Getting Started

1. Install the plugin from the Grafana plugin catalog or side-load the signed zip
2. Enable the plugin under **Administration > Plugins > TraceHouse**
3. Ensure you have a ClickHouse datasource configured pointing to your cluster
4. Open **TraceHouse** from the sidebar and start exploring

## Configuration

Go to the TraceHouse **Configuration** page to set:

- **Allowed Refresh Rates** — Control which auto-refresh intervals are available. Disabling aggressive rates (1s, 2s) reduces load on your ClickHouse cluster.
- **Default Refresh Rate** — The refresh interval applied when a user first opens any TraceHouse page.

## Documentation

- [Documentation](https://dmkskd.github.io/tracehouse/)
- [GitHub Repository](https://github.com/dmkskd/tracehouse)
- [Bug Reports & Feature Requests](https://github.com/dmkskd/tracehouse/issues)

## License

Apache 2.0 — see [LICENSE](https://github.com/dmkskd/tracehouse/blob/main/LICENSE) for details.
