# Changelog

All notable changes to the TraceHouse Grafana plugin will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.18.2] - 2026-07-29

### Improvements
- **Time Travel:** Added Overall and Per server views with multi-server selection.
- **Time Travel:** Overall CPU and memory now use total usage and capacity across selected servers.

## [0.18.1] - 2026-07-27

### Bug fixes
- Fixed time-range handling across Events, Analytics, Queries, Merges, and Time Travel by normalizing custom ranges and generated ClickHouse timestamps to explicit UTC, preventing time shifts when the browser and ClickHouse server use different time zones.

## [0.18.0] - 2026-07-27

### New features
- **Events:** New view for investigating errors, failures, restarts, and related ClickHouse activity.
- **Time Travel:** Added event correlation to the Timeline.
- **Query Analysis:** Added [`EXPLAIN ANALYZE`](https://clickhouse.com/docs/reference/statements/explain#explain-analyze).
- **Analytics / Dashboards:** Added Focus mode for exploring large dashboards one panel at a time.

### Improvements
- Reorganized the top navigation and grouped additional functionality under **More**.
- **Queries / Merges:** Unified running and historical activity in a consistent view.

### Bug fixes
- Fixed a CSS issue affecting the Running and History tabs on the Queries and Merges pages.

## [0.10.0] - 2026-02-17

### Features
- Analytics page with 3D surface visualizations for query and merge patterns
- Merge tracker with real-time progress, throughput, and X-ray drill-down
- Time Travel replay for parts, merges, and table evolution over any time window
- Query analysis with breakdowns by user, query kind, and status
- Cluster topology view with shard/replica status and resource utilization
- Engine Internals browser for MergeTree parts, partitions, and storage policies
- Database Explorer for browsing tables, columns, and storage details
- Replication monitoring with queue depth, log lag, and sync status
- Overview dashboard with cluster health: CPU, memory, queries, merges, replication lag
- Configurable refresh rates with admin controls to limit aggressive polling
