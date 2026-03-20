# Distributed Query Topology

Gantt-style timeline showing coordinator and shard sub-queries for distributed queries.

## Data Source

- **Coordinator**: `system.query_log` via `QUERY_DETAIL` (fetched for every query detail view)
- **Sub-queries**: `system.query_log` via `SUB_QUERIES` query, filtered by `initial_query_id = {coordinator_query_id} AND is_initial_query = 0`

Both queries use `{{cluster_aware:system.query_log}}` → `clusterAllReplicas()` on clusters, with `GROUP BY query_id` dedup to collapse duplicate rows from replicas.

## How Distributed Queries Work

1. **Coordinator** (the node that receives the query) rewrites and dispatches sub-queries to each shard
2. Each **shard** gets a unique `query_id`, but shares the same `initial_query_id` (= coordinator's `query_id`)
3. Coordinator waits for all shards, merges partial results (aggregation states, sorting), returns to client
4. Coordinator wall time ≥ slowest shard duration

## Metrics Per Bar

| Metric | Source | Display |
| --- | --- | --- |
| Duration | `query_duration_ms` | Inline on bar + right column |
| Memory | `memory_usage` | Inline on bar (if wide enough) |
| Rows Read | `read_rows` | Inline on bar (if wide enough) |
| Start Offset | `query_start_time_microseconds` relative to coordinator start | Bar position on timeline |
| Error | `exception_code` | Red bar color |

Narrow bars show metrics via native tooltip on hover.

## Time Alignment

Bars are positioned using microsecond-precision start times (`query_start_time_microseconds`). Each shard bar's left edge = `(shard_start - coordinator_start)` as a percentage of coordinator duration.

## Coordinator Overhead

```
overhead = coordinator_duration_ms - max(shard_duration_ms)
```

Represents time spent on: receiving query, dispatching to shards, merging partial aggregation states, sorting, and returning results.

## Cluster Topology Considerations

- A `Distributed` table query on a 2-shard cluster produces 2 sub-queries (one per shard, ClickHouse picks one replica)
- A `clusterAllReplicas()` query hits all replicas — e.g., 2 shards × 2 replicas = 4 sub-queries
- The dedup in `SUB_QUERIES` handles cases where `clusterAllReplicas` reads the same log entry from multiple replicas

## Navigation

Clicking any bar navigates to that query's full detail view (via `navigateToQuery`). When viewing a sub-query, the topology fetches the coordinator detail + all sibling sub-queries to show the full tree.
