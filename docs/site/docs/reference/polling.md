# Polling & Performance

:::caution Work In Progress
The tiered polling model described below is planned but not yet fully implemented.
:::

Reference for every polling interval in the app - what each page queries, which system tables are hit, and the load impact.

## Polling Tiers

| Tier | Interval | Tables | Cost |
|------|----------|--------|------|
| Tier 1 (live state) | 2–5s | `asynchronous_metrics`, `metrics`, `processes`, `merges`, `mutations` | Near-zero (all virtual) |
| Tier 2 (recent completed) | 30s | `query_log`, `part_log`, `events` (deltas) | MergeTree reads, small time windows |
| Tier 3 (structural) | 60s | `parts`, `disks`, `replicas`, `dictionaries` | Virtual or light aggregation |

## Total Overhead

~10 lightweight read-only queries per 30-second cycle. All are `SELECT` statements against `system.*` tables.

## System Table Types

| Type | Examples | Cost |
|------|----------|------|
| Virtual | `metrics`, `processes`, `merges`, `parts` | Computed from in-memory state - free |
| MergeTree | `query_log`, `part_log`, `metric_log` | Persisted, grow over time, need TTL management |

## Per-Page Breakdown

### Cluster Overview
- `system.asynchronous_metrics` (2s) - CPU, memory gauges
- `system.metrics` (2s) - active connections, running queries
- `system.events` (5s) - cumulative counters delta
- `system.clusters` (60s) - topology

### Database Explorer
- `system.databases` (60s) - database list
- `system.tables` (60s) - table metadata
- `system.parts` (30s) - part details for selected table
- `system.columns` (60s) - column statistics

### Merge Tracker
- `system.merges` (2s) - active merges
- `system.part_log` (30s) - completed merge history
- `system.mutations` (5s) - active mutations

### Query Monitor
- `system.processes` (2s) - running queries
- `system.query_log` (30s) - historical queries

### Engine Internals
- `system.asynchronous_metrics` (5s) - thread pools, memory
- `system.dictionaries` (60s) - dictionary status
- `system.trace_log` (30s) - CPU profiling data

## Scaling Considerations

- For clusters with 100+ tables, structural queries (Tier 3) may take longer
- `system.parts` can be expensive on tables with thousands of parts - the app uses `WHERE active = 1` filtering
- `system.query_log` queries use time-bounded windows to avoid scanning the full log
