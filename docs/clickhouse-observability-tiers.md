# ClickHouse Observability: Three Tiers of Metrics

ClickHouse exposes data at three levels of detail. Understanding which
tier you're looking at matters for building the right monitoring.

> **See also:** [ProfileEvents classification](clickhouse-profile-events-classification.md) for a
> complete breakdown of every ProfileEvent by category (merge, mutation, insert, replication, etc.).

---

## Tier 1: SERVER-WIDE (aggregate counters, no per-table or per-query breakdown)

These are global counters/gauges for the entire ClickHouse server process.
They answer: "how is the server doing overall?"

**Scope:** Per-server instance. In a cluster, each node has its own values.
**Prometheus:** Yes — all exposed automatically.
**Correlation key:** None (global).

### system.events (ProfileEvents — cumulative counters)
Prometheus prefix: `ClickHouseProfileEvents_`

Every ProfileEvent from `ProfileEvents.cpp` lives here. These are monotonically
increasing counters since server start. You get rates via `rate()` in Prometheus.

Key ones for us:
| Event | What it tells you |
|---|---|
| `Merge` | Total merges launched since start |
| `MergedRows`, `MergedUncompressedBytes` | Total rows/bytes processed by merges |
| `MergeTotalMilliseconds` | Total time spent merging |
| `MergesRejectedByMemoryLimit` | Merges that couldn't start |
| `DelayedInserts`, `RejectedInserts` | Insert backpressure (merge can't keep up) |
| `InsertedRows`, `InsertedBytes` | Total insert throughput |
| `Query`, `SelectQuery`, `InsertQuery` | Query counts |
| `SelectedParts`, `SelectedMarks` | Read amplification |

**Limitation:** You cannot tell which table, which query, or which specific merge
contributed to these numbers. It's all summed together.

### system.metrics (CurrentMetrics — instant gauges)
Prometheus prefix: `ClickHouseMetrics_`

Point-in-time values. "Right now, how many X are happening?"

| Metric | What it tells you |
|---|---|
| `Merge` | Currently running merges |
| `PartMutation` | Currently running mutations |
| `Query` | Currently executing queries |
| `BackgroundPoolTask` | Active background tasks (merges + mutations + fetches) |
| `BackgroundMergesAndMutationsPoolTask` | Active merge/mutation pool tasks |
| `DiskSpaceReservedForMerge` | Disk reserved for in-flight merges |
| `MemoryTracking` | Current memory usage |
| `OpenFileForRead`, `OpenFileForWrite` | Open file descriptors |
| `TCPConnection`, `HTTPConnection` | Active connections |

### system.asynchronous_metrics (background-computed gauges)
Prometheus prefix: `ClickHouseAsyncMetrics_`

Computed periodically (every ~1s) by a background thread.

| Metric | What it tells you |
|---|---|
| `TotalPartsOfMergeTreeTables` | Total parts across all tables |
| `MaxPartCountForPartition` | Worst-case partition part count (merge backlog signal) |
| `OSMemoryTotal`, `OSMemoryAvailable` | OS memory |
| `LoadAverage1`, `LoadAverage5`, `LoadAverage15` | System load |
| `OSUserTime`, `OSSystemTime`, `OSIdleTime` | CPU breakdown |
| `NumberOfPhysicalCPUCores` | CPU count |
| `Uptime` | Server uptime |

---

## Tier 2: PER-TABLE (state of a specific table's parts, merges, mutations)

These tables have `database` + `table` columns. You can filter to a specific table.
They answer: "what's happening with this table's data?"

**Scope:** Per-table, per-server.
**Prometheus:** Not directly (you query these via SQL).
**Correlation key:** `database` + `table` (and sometimes `partition_id`).

### system.parts — current state of all data parts
**Type:** Live state (not a log — reflects current reality)

| Column | What it tells you |
|---|---|
| `database`, `table` | Which table |
| `name` | Part name (encodes merge level, block range) |
| `partition_id` | Which partition |
| `active` | Is this part live or superseded |
| `rows` | Row count in this part |
| `bytes_on_disk` | Compressed size on disk |
| `data_compressed_bytes` | Compressed data size |
| `data_uncompressed_bytes` | Uncompressed data size |
| `level` | Merge depth (0 = from INSERT, 1+ = from merge) |
| `modification_time` | When part was created |
| `min_block_number`, `max_block_number` | Block range (shows merge lineage) |
| `part_type` | Wide vs Compact |
| `marks` | Number of index granules |
| `primary_key_bytes_in_memory` | PK memory usage |

**Use for:** Part count per table, compression ratios, merge level distribution,
partition health, "too many parts" detection.

### system.merges — currently running merges (live state)
**Type:** Live state (only shows in-progress merges, disappears when done)

| Column | What it tells you |
|---|---|
| `database`, `table` | Which table |
| `elapsed` | Seconds since merge started |
| `progress` | 0.0 to 1.0 completion |
| `num_parts` | Source parts being merged |
| `source_part_names` | Names of source parts |
| `result_part_name` | Output part name |
| `total_size_bytes_compressed` | Total compressed size of source parts |
| `bytes_read_uncompressed` | Bytes read so far |
| `bytes_written_uncompressed` | Bytes written so far |
| `rows_read` | Rows read so far |
| `rows_written` | Rows written so far |
| `columns_written` | Columns written so far |
| `memory_usage` | Current memory usage of this merge |
| `is_mutation` | 1 if this is actually a mutation, not a merge |
| `merge_type` | Type: Regular, TTLDelete, TTLRecompress, etc. |
| `merge_algorithm` | Horizontal vs Vertical |

**Use for:** Real-time merge monitoring, progress tracking, identifying slow merges.
This is what our MergeTracker currently uses.

### system.mutations — mutation state per table
**Type:** Persistent state (stays until mutation completes + cleanup)

| Column | What it tells you |
|---|---|
| `database`, `table` | Which table |
| `mutation_id` | Unique mutation ID |
| `command` | The ALTER command |
| `create_time` | When mutation was submitted |
| `parts_to_do` | Remaining parts to mutate |
| `parts_to_do_names` | Names of remaining parts |
| `is_done` | 0 = in progress, 1 = complete |
| `latest_failed_part` | Last part that failed |
| `latest_fail_time` | When it failed |
| `latest_fail_reason` | Why it failed |

**Use for:** Mutation progress tracking, stuck mutation detection.

### system.replication_queue — pending replication tasks (ReplicatedMergeTree only)
**Type:** Live state (tasks pending in ZK/Keeper)

| Column | What it tells you |
|---|---|
| `database`, `table` | Which table |
| `type` | Task type: `MERGE_PARTS`, `GET_PART`, `MUTATE_PART`, etc. |
| `new_part_name` | Target part |
| `parts_to_merge` | Source parts (for merges) |
| `create_time` | When task was queued |
| `is_currently_executing` | Is it running now |
| `num_tries` | Failed attempt count |
| `last_exception` | Last error |
| `num_postponed` | Times postponed |
| `postpone_reason` | Why postponed |
| `merge_type` | Regular, TTLDelete, etc. |

**Use for:** Replication lag, stuck tasks, merge queue depth per table.

### system.replicas — replica health per table (ReplicatedMergeTree only)
**Type:** Live state

Key columns: `database`, `table`, `is_leader`, `is_readonly`, `queue_size`,
`inserts_in_queue`, `merges_in_queue`, `log_pointer`, `total_replicas`,
`active_replicas`, `last_queue_update`, `absolute_delay`.

**Use for:** Replica lag, leader status, queue depth.

---

## Tier 3: PER-OPERATION (individual query, merge, or mutation with a unique ID)

These are log tables where each row represents a single event/operation.
They answer: "what exactly happened during this specific merge/query?"

**Scope:** Per-operation, identified by `query_id` (UUID).
**Prometheus:** Not directly (query via SQL, or export to external systems).
**Correlation key:** `query_id` (UUID) — this is the golden thread.

### The query_id is everything

Every operation in ClickHouse gets a `query_id`. This includes:
- User SELECT/INSERT queries
- Background merges (ClickHouse assigns an internal query_id)
- Background mutations (same)
- DDL operations

You can join across ALL Tier 3 tables using `query_id`.

### system.query_log — the master record for every operation
**Type:** Append-only log (MergeTree table, persisted to disk)

Each query produces 2 rows: `QueryStart` + `QueryFinish` (or `ExceptionWhileProcessing`).

| Column | What it tells you |
|---|---|
| `query_id` | **The UUID** — join key for everything |
| `type` | QueryStart, QueryFinish, ExceptionBeforeStart, ExceptionWhileProcessing |
| `query` | The SQL text (for merges: internal merge command) |
| `query_kind` | Select, Insert, etc. |
| `query_duration_ms` | Total wall-clock time |
| `read_rows`, `read_bytes` | Rows/bytes read |
| `written_rows`, `written_bytes` | Rows/bytes written |
| `result_rows`, `result_bytes` | Result size |
| `memory_usage` | Peak memory |
| `databases`, `tables` | Which databases/tables were touched |
| `exception`, `exception_code`, `stack_trace` | Error details |
| `is_initial_query` | 1 = user query, 0 = distributed sub-query |
| `initial_query_id` | Parent query ID (for distributed) |
| `user` | Who ran it |
| `thread_ids` | OS thread IDs involved |
| `peak_threads_usage` | Max concurrent threads |
| `ProfileEvents` | **Map(String, UInt64)** — per-query ProfileEvents! |
| `Settings` | Query settings that were active |

**The `ProfileEvents` column is the key insight:** Every ProfileEvent counter
(the same ones from Tier 1) is also recorded PER-QUERY in this column.
So for a specific merge, you can see exactly how many `MergedRows`,
`DiskReadElapsedMicroseconds`, `MergeHorizontalStageTotalMilliseconds`, etc.
that specific merge consumed.

### system.part_log — per-part lifecycle events
**Type:** Append-only log (MergeTree table, persisted to disk)

One row per part event (create, merge, mutate, remove, move).

| Column | What it tells you |
|---|---|
| `query_id` | **Links to query_log** (empty for background ops in older CH) |
| `event_type` | `NewPart`, `MergeParts`, `MutatePart`, `DownloadPart`, `RemovePart`, `MovePart` |
| `merge_reason` | `RegularMerge`, `TTLDeleteMerge`, `TTLRecompressMerge`, `NotAMerge` |
| `merge_algorithm` | `Horizontal`, `Vertical` |
| `database`, `table` | Which table |
| `part_name` | Result part name |
| `partition_id` | Which partition |
| `rows` | Rows in result part |
| `size_in_bytes` | Result part size |
| `merged_from` | **Array of source part names** (merge lineage!) |
| `duration_ms` | How long the operation took |
| `bytes_uncompressed` | Uncompressed bytes |
| `read_rows`, `read_bytes` | What was read |
| `peak_memory_usage` | Memory used |
| `error`, `exception` | Error info |
| `ProfileEvents` | **Per-operation ProfileEvents map** (same as query_log) |

**This is the single best table for merge profiling.** Each merge produces one row
with full timing, source parts, result part, memory usage, and the complete
ProfileEvents breakdown for that specific merge.

Example from the docs — a single merge row contains:
```
ProfileEvents: {
  'Merge':2, 'MergeSourceParts':14, 'MergedRows':3285733,
  'MergedColumns':4, 'GatheredColumns':51,
  'MergedUncompressedBytes':1429207058,
  'MergeTotalMilliseconds':2158, 'MergeExecuteMilliseconds':2155,
  'MergeHorizontalStageTotalMilliseconds':145,
  'MergeVerticalStageTotalMilliseconds':2008,
  'MergeProjectionStageTotalMilliseconds':5,
  'DiskReadElapsedMicroseconds':139058,
  'DiskWriteElapsedMicroseconds':51639,
  ...
}
```

### system.query_thread_log — per-thread breakdown of a query
**Type:** Append-only log

| Column | What it tells you |
|---|---|
| `query_id` | **Links to query_log** |
| `thread_name` | Thread name (e.g., `MergeTreeBackgroundExecutor`, `QueryPipelineEx`) |
| `thread_id` | OS thread ID |
| `query_duration_ms` | Duration from this thread's perspective |
| `read_rows`, `read_bytes` | What this thread read |
| `written_rows`, `written_bytes` | What this thread wrote |
| `memory_usage`, `peak_memory_usage` | Memory from this thread |
| `ProfileEvents` | **Per-thread ProfileEvents** |

**Use for:** Understanding parallelism, which threads did the most work,
thread-level memory/IO breakdown for a specific query or merge.

### system.text_log — raw log messages with query_id
**Type:** Append-only log

| Column | What it tells you |
|---|---|
| `query_id` | **Links to query_log** |
| `level` | Fatal, Critical, Error, Warning, Notice, Information, Debug, Trace, Test |
| `message` | The actual log message |
| `logger_name` | Source component (e.g., `MergeTreeBackgroundExecutor`, `default.my_table`) |
| `event_time_microseconds` | Microsecond precision timestamp |
| `thread_id` | Which thread |

**Use for:** Detailed debugging of a specific operation. Filter by `query_id`
to get all log messages for a specific merge/query. This is what our
`QueryTracer.get_query_logs()` already uses.

### system.trace_log — sampling profiler stack traces
**Type:** Append-only log

| Column | What it tells you |
|---|---|
| `query_id` | **Links to query_log** |
| `trace_type` | `Real` (wall-clock), `CPU`, `Memory`, `MemorySample`, `ProfileEvent` |
| `trace` | Array of instruction pointers (stack trace) |
| `thread_id` | Which thread |
| `size` | For Memory traces: allocation size |
| `event` | For ProfileEvent traces: which event |
| `increment` | For ProfileEvent traces: increment amount |

**Use for:** CPU profiling, memory allocation profiling, finding hot code paths
for a specific query. Can be exported to Chrome trace format / flamegraphs.

### system.opentelemetry_span_log — OpenTelemetry spans
**Type:** Append-only log

| Column | What it tells you |
|---|---|
| `trace_id` | OpenTelemetry trace ID |
| `span_id`, `parent_span_id` | Span hierarchy |
| `operation_name` | What the span represents |
| `start_time_us`, `finish_time_us` | Span timing |
| `attribute.names`, `attribute.values` | Span attributes |

**Use for:** Distributed tracing, waterfall views. Requires OpenTelemetry
to be enabled (`opentelemetry_start_trace_probability`).

### system.query_views_log — materialized view execution per query
**Type:** Append-only log

| Column | What it tells you |
|---|---|
| `initial_query_id` | The triggering query |
| `view_name` | Which MV was executed |
| `view_type` | Materialized, Live, etc. |
| `read_rows`, `written_rows` | IO for this view |
| `ProfileEvents` | Per-view ProfileEvents |

---

## HOW THEY CONNECT (the query_id join)

```
                    query_id (UUID)
                         │
         ┌───────────────┼───────────────────┐
         │               │                   │
   system.query_log  system.part_log  system.text_log
   (master record)   (part events)    (log messages)
         │               │                   │
         │               │                   │
   system.query_     system.trace_log  system.query_
   thread_log        (stack traces)    views_log
   (per-thread)                        (MV execution)
```

For a specific merge, you can:
1. Find it in `system.part_log` WHERE `event_type = 'MergeParts'`
2. Get its `query_id`
3. Join to `system.query_log` for the full query record + per-query ProfileEvents
4. Join to `system.text_log` for all log messages during that merge
5. Join to `system.query_thread_log` for per-thread breakdown
6. Join to `system.trace_log` for CPU/memory profiling

---

## WHAT OUR APP CURRENTLY USES vs WHAT WE COULD USE

### Currently using:
| Tier | Source | What we query |
|---|---|---|
| Tier 1 | `system.metrics` | `MemoryTracking`, `Query`, `Merge`, `BackgroundPoolTask` |
| Tier 1 | `system.events` | `ReadBufferFromFileDescriptorReadBytes`, `WriteBufferFromFileDescriptorWriteBytes` |
| Tier 1 | `system.asynchronous_metrics` | OS memory, CPU metrics |
| Tier 2 | `system.merges` | Live merge progress (MergeTracker) |
| Tier 2 | `system.parts` | Part listing (DatabaseExplorer) |
| Tier 3 | `system.processes` | Running queries (QueryAnalyzer) |
| Tier 3 | `system.query_log` | Query history |
| Tier 3 | `system.text_log` | Per-query log messages (QueryTracer) |
| Tier 3 | `system.opentelemetry_span_log` | OTel spans (QueryTracer) |

### Missing / high-value additions:
| Tier | Source | What we should add |
|---|---|---|
| Tier 1 | `system.events` | `DelayedInserts`, `RejectedInserts`, `MergeTotalMilliseconds`, `MergesRejectedByMemoryLimit` |
| Tier 1 | `system.asynchronous_metrics` | `TotalPartsOfMergeTreeTables`, `MaxPartCountForPartition` |
| Tier 2 | `system.mutations` | Mutation progress tracking |
| Tier 2 | `system.replication_queue` | Replication task queue (if using ReplicatedMergeTree) |
| Tier 2 | `system.replicas` | Replica health/lag |
| **Tier 3** | **`system.part_log`** | **Per-merge ProfileEvents, duration, source parts, memory — this is the biggest gap** |
| Tier 3 | `system.query_log.ProfileEvents` | Per-query ProfileEvents (we query query_log but don't extract the ProfileEvents map) |
| Tier 3 | `system.query_thread_log` | Per-thread breakdown for slow queries |
