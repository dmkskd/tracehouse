# TraceHouse - Grafana App Plugin

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Grafana](https://img.shields.io/badge/Grafana->=10.0.0-orange)](https://grafana.com)

Comprehensive ClickHouse monitoring with 3D visualizations, merge tracking, query analysis, and time travel.

## Overview

TraceHouse is a Grafana app plugin that provides deep observability into ClickHouse internals. It goes beyond basic metrics to give you real-time, interactive views of what your ClickHouse cluster is actually doing — from active merges and part movements to query execution patterns and storage layout over time.

## Features

- **Overview** — Cluster health at a glance: CPU, memory, queries, merges, replication lag
- **Engine Internals** — Inspect MergeTree parts, partitions, and storage policies
- **Cluster** — Multi-node topology, shard/replica status, and resource utilization
- **Explorer** — Browse databases, tables, columns, and storage details
- **Time Travel** — Replay how tables, parts, and merges evolved over any time window
- **Queries** — Active and historical query analysis with breakdowns by user, query kind, and status
- **Merges** — Real-time merge tracker with per-merge progress, throughput, and X-ray details
- **Replication** — Replication queue depth, log lag, and inter-replica sync status
- **Analytics** — 3D surface visualizations for query and merge patterns over time

## Screenshots

<!-- Replace with actual screenshots of your plugin -->
![Overview](https://github.com/tracehouse/tracehouse/raw/main/docs/screenshots/overview.png)
![Merges](https://github.com/tracehouse/tracehouse/raw/main/docs/screenshots/merges.png)
![Time Travel](https://github.com/tracehouse/tracehouse/raw/main/docs/screenshots/timetravel.png)

## Requirements

- Grafana >= 10.0.0
- A running ClickHouse instance (21.8+) with `system.*` tables accessible
- The [Grafana ClickHouse datasource](https://grafana.com/grafana/plugins/grafana-clickhouse-datasource/) plugin installed and configured

## Getting Started

1. Install the plugin from the Grafana plugin catalog (or side-load the signed zip)
2. Enable the plugin under **Administration > Plugins > TraceHouse**
3. Ensure you have a ClickHouse datasource configured pointing to your cluster
4. Navigate to **TraceHouse** in the sidebar to start exploring

## Configuration

After enabling the plugin, go to its **Configuration** page to set:

- **Allowed Refresh Rates** — Control which auto-refresh intervals are available to users. Disabling aggressive rates (1s, 2s) reduces load on your ClickHouse cluster.
- **Default Refresh Rate** — The refresh interval selected by default when a user opens any TraceHouse page.

## Development

```bash
# Install dependencies
npm install

# Start development build (watch mode)
npm run dev

# Production build
npm run build

# Run tests
npm test
```

To run locally with Grafana, use the provided Docker Compose setup:

```bash
# From the repository root
docker compose -f infra/docker/docker-compose.yml --profile monitoring up
```

This starts ClickHouse, Prometheus, and Grafana with the plugin auto-loaded.

## Contributing

We welcome contributions! Please:

1. Fork the repository
2. Create a feature branch
3. Submit a pull request with a clear description of your changes

## Feedback and Support

- **Bug reports**: Open an issue on [GitHub](https://github.com/tracehouse/tracehouse/issues)
- **Feature requests**: Open a discussion on [GitHub](https://github.com/tracehouse/tracehouse/discussions)

## License

This plugin is distributed under the [Apache 2.0 License](LICENSE).
