# ClickHouse Internals Reference

Background on the ClickHouse system tables and concepts that TraceHouse relies on.

## System Table Taxonomy

| Category | Table | Type | Actor |
|----------|-------|------|-------|
| Instantaneous gauges | `system.metrics` | Virtual | All |
| Background-computed gauges | `system.asynchronous_metrics` | Virtual | Server |
| Cumulative counters | `system.events` | Virtual | All |
| Time-series of gauges+counters | `system.metric_log` | MergeTree | All |
| Time-series of async metrics | `system.asynchronous_metric_log` | MergeTree | Server |
| Running queries | `system.processes` | Virtual | Users |
| Query history | `system.query_log` | MergeTree | Users |
| Per-thread query details | `system.query_thread_log` | MergeTree | Users |
| MV execution | `system.query_views_log` | MergeTree | Background |
| Running merges | `system.merges` | Virtual | Background |
| Part lifecycle events | `system.part_log` | MergeTree | Background |
| Active mutations | `system.mutations` | Virtual | Background |
| Replication status | `system.replicas` | Virtual | Background |
| Replication queue | `system.replication_queue` | Virtual | Background |
| Data parts | `system.parts` | Virtual | Storage |
| Column statistics | `system.columns` | Virtual | Storage |
| Disk volumes | `system.disks` | Virtual | Server |
| Tables metadata | `system.tables` | Virtual | Storage |
| Cluster topology | `system.clusters` | Virtual | Server |
| Dictionaries | `system.dictionaries` | Virtual | Storage |
| Server text logs | `system.text_log` | MergeTree | Server |
| Stack trace profiler | `system.trace_log` | MergeTree | Server |
| OpenTelemetry spans | `system.opentelemetry_span_log` | MergeTree | All |
| Crash records | `system.crash_log` | MergeTree | Server |

**Virtual tables** are computed from in-memory state - free to query.
**MergeTree tables** (the `*_log` tables) are persisted and grow over time - they need TTL management.

## Observability Tiers

### Tier 1: Server-Wide Metrics
Global counters and gauges. Cheap to collect, always available. Tells you "how is the server doing" but not "who is responsible."

### Tier 2: Per-Table Metrics
Part counts, sizes, merge activity per table. Available from virtual tables. Tells you "which tables are hot."

### Tier 3: Per-Operation Metrics
Per-query ProfileEvents, per-merge CPU/IO from part_log. The most detailed level. Tells you "this specific query used X CPU."

## ProfileEvents

ProfileEvents are ClickHouse's fine-grained counters attached to individual operations. Key categories:

- **Merge profiling** - `MergedRows`, `MergedUncompressedBytes`, `MergesTimeMilliseconds`
- **Mutations** - `MutatedRows`, `MutatedUncompressedBytes`
- **Insert pressure** - `InsertedRows`, `InsertedBytes`, `DelayedInserts`
- **Replication** - `ReplicatedPartFetches`, `ReplicatedPartMerges`
- **Query performance** - `SelectedRows`, `SelectedBytes`, `SelectedMarks`
- **CPU/OS** - `OSCPUVirtualTimeMicroseconds`, `OSCPUWaitMicroseconds`
- **Disk I/O** - `ReadBufferFromFileDescriptorReadBytes`, `WriteBufferFromFileDescriptorWriteBytes`
- **Caches** - `MarkCacheHits`, `MarkCacheMisses`, `UncompressedCacheHits`
- **Thread pools** - `GlobalThreadPoolSize`, `LocalThreadPoolSize`
