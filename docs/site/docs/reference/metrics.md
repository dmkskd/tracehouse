# Metrics & Formulas

Complete reference of every metric formula used in TraceHouse.

## CPU Attribution

```
Total CPU = Query CPU + Merge CPU + Mutation CPU + Other
```

| Category | Live Source | Historical Source |
|----------|-----------|-------------------|
| Total process CPU | `asynchronous_metrics` → `OSUserTimeNormalized` + `OSSystemTimeNormalized` | `metric_log` → `OSCPUVirtualTimeMicroseconds` |
| Per-query CPU | `processes` → ProfileEvents (`UserTimeMicroseconds` + `SystemTimeMicroseconds`) / elapsed | `query_log` → ProfileEvents |
| Per-merge CPU | Heuristic: ~1.5 cores per active merge from `system.merges` | `part_log` → ProfileEvents (MergeParts) |
| Per-mutation CPU | Heuristic: ~1.0 cores per active mutation from `system.merges` (is_mutation=1) | `part_log` → ProfileEvents (MutatePart) |

:::note The Merge CPU Gap
`system.merges` has no ProfileEvents. Live merge CPU is estimated using: `active_merge_count × estimated_cores_per_merge`. Actual CPU data comes from `system.part_log` for completed merges.
:::

## Memory Attribution

```
RSS = Query memory + Merge memory + Mark cache + Uncompressed cache
    + Primary keys + Dictionaries + Other
```

"Other" includes jemalloc overhead, fragmentation, memory-mapped files, and anything not tracked by the categories above (`RSS - sum(tracked)`). All tracked components are directly queryable from virtual tables (zero-cost reads).

## I/O Attribution

- Live query I/O: `system.processes` ProfileEvents → `OSReadBytes / elapsed`, `OSWriteBytes / elapsed`
- Live merge I/O: `system.merges` → `bytes_read_uncompressed / elapsed`, `bytes_written_uncompressed / elapsed`
- Total server I/O: `system.metric_log` → `avg(ProfileEvent_OSReadBytes)`, `avg(ProfileEvent_OSWriteBytes)` (last 10s)

## Merge Throughput

**Active merges** (overview page — I/O attribution):

```
read_rate  = bytes_read_uncompressed / elapsed
write_rate = bytes_written_uncompressed / elapsed
```

**Completed merges** (merge tracker — on-disk throughput):

```
throughput = size_in_bytes / (duration_ms / 1000)
```

```
Merge amplification = output_size / sum(input_sizes)
```

## Query Efficiency

```
Index pruning = (SelectedMarksTotal - SelectedMarks) / SelectedMarksTotal × 100
Cache hit rate = MarkCacheHits / (MarkCacheHits + MarkCacheMisses) × 100
```

Higher pruning % = better primary key usage (more data skipped). See [Query Details](../features/analytics-query-language.mdx) for interpretation thresholds.

## X-Ray

The X-Ray tab provides a 3D corridor visualization of query resource consumption over time, sampled from `tracehouse.processes_history`:

- **Width** = CPU cores used (`Δ(OSCPUVirtualTimeMicroseconds) / 1e6 / Δt`)
- **Height** = memory MB (`memory_usage / 1048576`)

Additional timeline charts show I/O wait, read throughput (MB/s), and network (KB/s). All delta metrics are normalized to per-second rates via `lagInFrame` window functions.

For distributed queries, X-Ray collects samples from all hosts (`WHERE query_id = {id} OR initial_query_id = {id}`). "ALL" mode sums across hosts; per-host mode isolates individual nodes.

## Distributed Query Topology

A Gantt-style timeline showing coordinator and shard sub-queries. Each bar is time-aligned using `query_start_time_microseconds` and shows duration, memory, and rows inline.

```
Coordinator overhead = coordinator_duration - max(shard_duration)
```

Clicking any bar navigates to that query's detail view.

## Pipeline Profile

Visual DAG from `EXPLAIN PIPELINE` with per-processor metrics from `processors_profile_log`: elapsed time, input/output wait, rows processed. Requires `log_processors_profiles = 1`.

## Thread Breakdown

Per-thread attribution from `query_thread_log`: CPU time, I/O wait, peak memory, rows read/written. Rendered as a Gantt timeline or sortable table, colored by thread type.

## Key Caveats

1. **ProfileEvents in processes** requires ClickHouse 22.x+
2. **Per-operator memory breakdown** is not available - ClickHouse reports total `memory_usage` per query
3. **Log table flush lag** - `*_log` tables flush every ~7.5s; use 30s+ query windows for reliable data
4. **MV double-counting** - INSERT `query_log` ProfileEvents may include MV work; don't add `query_views_log` on top
5. **Thread pool metrics ≠ merge activity** - `MergeTreeBackgroundExecutorThreadsActive` handles many background operations, not just merges
