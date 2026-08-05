# Changelog

All notable changes to the TraceHouse Grafana plugin will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.18.5] - 2026-08-05

### Improvements
- **Time Travel:** Added a link to the Analytics "Workload Breakdown" dashboard for a precise view of the sampled totals.

### Bug fixes
- Database, table, and column names are now escaped in queries, fixing identifiers that contain backticks or backslashes.

## [0.18.4] - 2026-07-31

### Improvements
- **Merges:** Failed merges and mutations can now be filtered by one or more ClickHouse error codes, with suggestions based on the current results.
- **Events:** Added a Merges lane for failed merges, mutations, and MergeTree part operations.

### Bug fixes
- **Merges:** Fixed a bug where filters ran after the result limit, causing failed merges to be missed.
- **Merges:** Fixed failed details being replaced by another replica's successful merge.
- **Queries / Merges:** Fixed running activity outside the selected time range.

## [0.18.3] - 2026-07-31

### Improvements
- **Queries / Merges:** You can now select multiple values for the same filter and use quick presets for running, recent, failed, and slow activity.
- **Queries:** Failed queries can now be filtered by one or more ClickHouse error codes, with suggestions based on the current results.
- **Time Travel:** Enhanced the timeline navigator with Average, Maximum, and Change views, adaptive scales, and smooth navigation through historical data.
- **Events:** Reduced initial collection to 100 events per source and added a visible truncation indicator.
- **Analytics Dashboards:** Panels that require a newer ClickHouse version now show a clear compatibility message instead of failing.

### Bug fixes
- **Events:** Fixed event lists failing to load on ClickHouse versions earlier than 24.1 by using a compatible query when distributed `LIMIT BY` is unavailable.
- **Queries:** Query history now includes ClickHouse `ExceptionBeforeStart` errors, and server filters use the server's actual `hostName()` instead of connection aliases. Query details now also load on ClickHouse 23.8.
- **Mutations:** Fixed the Mutations view on ClickHouse 23.8. Active and completed mutations now load, with a warning that this version cannot identify killed mutations.
- **Distributed Query Analysis:** The query topology view now works on ClickHouse 23.8 through 24.8. These versions provide less processor detail than ClickHouse 25.3+, so TraceHouse uses the available query data and explains the limitation.
- **Grafana:** Preserved repeated URL parameters so multi-value filters survive navigation and reloads.
- **Cluster Connections:** Feature availability is now rechecked after connecting to or switching clusters, preventing stale results and unnecessary errors in ClickHouse logs.

### Security
- Updated DOMPurify and Immutable dependencies to address npm audit findings.

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
