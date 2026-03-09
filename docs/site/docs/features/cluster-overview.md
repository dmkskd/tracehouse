# Cluster Overview

The Cluster Overview page provides a real-time dashboard of your ClickHouse cluster's health and resource utilization.

## What You See

- **CPU Attribution** - Total CPU broken down by queries, merges, mutations, replication, and other
- **Memory Breakdown** - RSS decomposed into query buffers, mark cache, uncompressed cache, primary keys, dictionaries, and jemalloc overhead
- **Disk I/O** - Read/write throughput attributed to queries, merges, and background operations
- **Cluster Topology** - Visual map of shards and replicas with per-node health indicators

## Data Sources

| Metric | Live Source | Historical Source |
|--------|-----------|-------------------|
| Total CPU | `system.asynchronous_metrics` | `system.metric_log` |
| Per-query CPU | `system.processes` ProfileEvents | `system.query_log` ProfileEvents |
| Memory | `system.asynchronous_metrics` + virtual tables | - |
| Disk I/O | `system.processes` ProfileEvents | `system.part_log` ProfileEvents |

## Polling Intervals

- **Tier 1 metrics** (gauges, counters): every 2–5 seconds
- **Tier 2 metrics** (log tables): every 30 seconds
- **Structural data** (parts, replicas): every 60 seconds

All queries are read-only SELECTs against `system.*` tables with near-zero overhead.
