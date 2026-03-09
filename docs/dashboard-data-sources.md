# Dashboard Data Sources — Complete Mapping

> Moved from `.kiro/read_please/dashboard_sources.md`. See also
> [metrics-calculations.md](metrics-calculations.md) for the formulas that use these sources,
> and [clickhouse-observability-tiers.md](clickhouse-observability-tiers.md) for the system table architecture.

Every metric displayed in the Overview and Engine Internals views, mapped to its exact ClickHouse source.

Legend:
- **Virtual** = in-memory, zero-cost to query, no disk IO
- **MergeTree** = persisted log table, flushed every ~7.5s
- **Computed** = derived app-side from one or more raw sources

---

## Live View — Data Sources

### Header Bar

| UI Element | Source | Query |
|---|---|---|
| Hostname | `system.asynchronous_metrics` (Virtual) | `SELECT value FROM system.asynchronous_metrics WHERE metric = 'HostName'` — or use `SELECT hostName()` |
| Version | Built-in function | `SELECT version()` |
| Uptime | `system.asynchronous_metrics` (Virtual) | `SELECT value FROM system.asynchronous_metrics WHERE metric = 'Uptime'` |
| Running queries count | `system.metrics` (Virtual) | `SELECT value FROM system.metrics WHERE metric = 'Query'` |
| Running merges count | `system.metrics` (Virtual) | `SELECT value FROM system.metrics WHERE metric = 'Merge'` |
| Active mutations count | `system.mutations` (Virtual) | `SELECT count() FROM system.mutations WHERE is_done = 0` |
| Replication lag | `system.replicas` (Virtual) | `SELECT max(absolute_delay) FROM system.replicas` |
| Readonly replicas | `system.replicas` (Virtual) | `SELECT count() FROM system.replicas WHERE is_readonly = 1` |
| Last poll time | App-side | Timestamp of last successful query cycle |

### Resource Attribution Bar (Hero Section)

#### CPU Attribution

| UI Element | Source | Query / Computation |
|---|---|---|
| Total server CPU % | `system.asynchronous_metrics` (Virtual) | Delta of `OSCPUVirtualTimeMicroseconds` between two polls. `cpu_pct = delta_value / (num_cores × poll_interval_us) × 100`. Num cores from `SELECT value FROM system.asynchronous_metrics WHERE metric = 'NumberOfPhysicalCores'` |
| CPU attributed to queries | `system.processes` (Virtual) — live ProfileEvents | `SELECT sum(ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds']) FROM system.processes` — take delta between polls. **Requires CH 22.x+** for ProfileEvents in processes. |
| CPU attributed to merges | `system.part_log` (MergeTree) — completed merges | `SELECT sum(ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds']) FROM system.part_log WHERE event_type = 'MergeParts' AND event_time > now() - INTERVAL 35 SECOND AND event_time <= now() - INTERVAL 5 SECOND` — gives CPU-seconds consumed by merges completed in the window. Divide by window length for avg CPU. **Note**: `system.merges` (live running merges) does NOT have ProfileEvents — this is the workaround. |
| CPU attributed to mutations | `system.part_log` (MergeTree) — completed mutations | Same as above with `WHERE event_type = 'MutatePart'` |
| CPU "other" | Computed | `total_cpu - query_cpu - merge_cpu - mutation_cpu`. Includes replication, TTL, async inserts, internal housekeeping. |

#### Memory Attribution

| UI Element | Source | Query |
|---|---|---|
| Total RSS | `system.asynchronous_metrics` (Virtual) | `SELECT value FROM system.asynchronous_metrics WHERE metric = 'MemoryResident'` |
| Total system RAM | `system.asynchronous_metrics` (Virtual) | `SELECT value FROM system.asynchronous_metrics WHERE metric = 'OSMemoryTotal'` |
| Memory tracked by queries | `system.processes` (Virtual) | `SELECT sum(memory_usage) FROM system.processes` |
| Memory used by merges | `system.merges` (Virtual) | `SELECT sum(memory_usage) FROM system.merges` |
| Mark cache bytes | `system.asynchronous_metrics` (Virtual) | `SELECT value FROM system.asynchronous_metrics WHERE metric = 'MarkCacheBytes'` |
| Uncompressed cache bytes | `system.asynchronous_metrics` (Virtual) | `SELECT value FROM system.asynchronous_metrics WHERE metric = 'UncompressedCacheBytes'` |
| Primary key in memory | `system.parts` (Virtual) | `SELECT sum(primary_key_bytes_in_memory) FROM system.parts` — **heavier query**, run every 60s |
| Dictionary memory | `system.dictionaries` (Virtual) | `SELECT sum(bytes_allocated) FROM system.dictionaries` |
| "Other" memory | Computed | `RSS - (query_mem + merge_mem + mark_cache + uncomp_cache + pk_mem + dict_mem)` |

#### IO Attribution

| UI Element | Source | Query / Computation |
|---|---|---|
| Total read bytes/s | `system.asynchronous_metrics` (Virtual) | Delta of `OSReadBytes` between polls. This is from `/proc/self/io`, ClickHouse process only. |
| Total write bytes/s | `system.asynchronous_metrics` (Virtual) | Delta of `OSWriteBytes` between polls. |
| Query IO | `system.processes` (Virtual) — live ProfileEvents | `SELECT sum(ProfileEvents['OSReadBytes']), sum(ProfileEvents['OSWriteBytes']) FROM system.processes` — delta between polls |
| Merge IO | `system.part_log` (MergeTree) | `SELECT sum(ProfileEvents['OSReadBytes']), sum(ProfileEvents['OSWriteBytes']) FROM system.part_log WHERE event_type = 'MergeParts' AND event_time > now() - INTERVAL 35 SECOND` |
| Replication IO | `system.part_log` (MergeTree) | Same with `WHERE event_type = 'DownloadPart'` |

### Summary Cards (CPU / Memory / Disk)

| UI Element | Source | Query |
|---|---|---|
| CPU cores used (absolute) | Computed | `OSCPUVirtualTimeMicroseconds` delta / 1e6 / poll_interval = cores used |
| Load average 1/5/15 | `system.asynchronous_metrics` (Virtual) | `SELECT metric, value FROM system.asynchronous_metrics WHERE metric IN ('LoadAverage1', 'LoadAverage5', 'LoadAverage15')` |
| Memory tracked total | `system.metrics` (Virtual) | `SELECT value FROM system.metrics WHERE metric = 'MemoryTracking'` |
| Caches total | Computed | `MarkCacheBytes + UncompressedCacheBytes` from async metrics |
| Disk used % | `system.disks` (Virtual) | `SELECT name, total_space, free_space, round((total_space - free_space) * 100 / total_space, 1) AS pct FROM system.disks` |
| Disk free | `system.disks` (Virtual) | `free_space` from above |
| Replication tables synced | `system.replicas` (Virtual) | `SELECT count() AS total, countIf(absolute_delay < 10 AND NOT is_readonly) AS healthy FROM system.replicas` |

### Alert Banner

| Alert | Source | Query |
|---|---|---|
| Too many parts | `system.parts` (Virtual) | `SELECT database, table, partition_id, count() AS c FROM system.parts WHERE active GROUP BY 1,2,3 HAVING c > 150 ORDER BY c DESC` — run every 60s |
| Readonly replica | `system.replicas` (Virtual) | `SELECT database, table FROM system.replicas WHERE is_readonly = 1` |
| Disk space low | `system.disks` (Virtual) | Alert if `free_space / total_space < 0.15` |
| Stuck mutation | `system.mutations` (Virtual) | `SELECT database, table, command FROM system.mutations WHERE is_done = 0 AND create_time < now() - INTERVAL 1 HOUR` |

### Running Queries Table

| Column | Source | Query |
|---|---|---|
| Query ID | `system.processes` (Virtual) | `query_id` |
| User | `system.processes` (Virtual) | `user` |
| Elapsed | `system.processes` (Virtual) | `elapsed` (seconds float) |
| CPU cores used | `system.processes` (Virtual) — live ProfileEvents | `(ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds']) / 1e6 / elapsed` — gives average cores consumed. **Requires CH 22.x+**. Fallback for older versions: not available from processes alone. |
| Memory | `system.processes` (Virtual) | `memory_usage` (bytes) |
| IO read rate | `system.processes` (Virtual) — live ProfileEvents | `ProfileEvents['OSReadBytes'] / elapsed` — or `ProfileEvents['ReadCompressedBytes'] / elapsed` |
| Rows processed | `system.processes` (Virtual) | `read_rows` |
| Bytes read | `system.processes` (Virtual) | `read_bytes` |
| Progress % | `system.processes` (Virtual) | `query_kind = 'Select'`: estimated from `read_rows / total_rows_approx` if available, or `ProfileEvents['SelectedMarks']` progress. For INSERTs: `written_rows / total_rows_to_write`. **Note**: ClickHouse provides `total_rows_approx` in processes since ~22.x. |
| Query text | `system.processes` (Virtual) | `query` |
| Kind (SELECT/INSERT) | `system.processes` (Virtual) | Infer from `query` prefix or `query_kind` column (available in recent versions) |

### Active Merges Table

| Column | Source | Query |
|---|---|---|
| Table | `system.merges` (Virtual) | `database, table` |
| Part name | `system.merges` (Virtual) | `result_part_name` |
| Elapsed | `system.merges` (Virtual) | `elapsed` (seconds) |
| Progress % | `system.merges` (Virtual) | `progress` (0.0–1.0 float) |
| Memory | `system.merges` (Virtual) | `memory_usage` (bytes) |
| Read MB/s | `system.merges` (Virtual) | `bytes_read_uncompressed / elapsed` |
| Write MB/s | `system.merges` (Virtual) | `bytes_written_uncompressed / elapsed` |
| Rows | `system.merges` (Virtual) | `rows_read` |
| Num parts merging | `system.merges` (Virtual) | `num_parts` |
| CPU estimate | Computed | Not directly available from `system.merges`. **Workaround**: compute average CPU-per-second from recent `system.part_log` completed merges, multiply by `elapsed`. Or: `total_server_cpu - attributed_query_cpu ≈ merge_cpu + other`. |

### Active Mutations Section

| Column | Source | Query |
|---|---|---|
| Table | `system.mutations` (Virtual) | `database, table` |
| Command | `system.mutations` (Virtual) | `command` |
| Parts remaining | `system.mutations` (Virtual) | `parts_to_do` |
| Elapsed | `system.mutations` (Virtual) | `now() - create_time` (seconds) |
| Memory | Not directly available | Mutations run as merge-like operations. Memory shows up in `system.merges` where `is_mutation = 1`. Query: `SELECT memory_usage FROM system.merges WHERE is_mutation = 1` |
| CPU estimate | `system.part_log` (MergeTree) | From completed mutation parts: `SELECT sum(ProfileEvents['UserTimeMicroseconds']) FROM system.part_log WHERE event_type = 'MutatePart' AND query_id = '<mutation_id>'` |

### Replication Section

| Metric | Source | Query |
|---|---|---|
| Tables synced | `system.replicas` (Virtual) | `SELECT count() FROM system.replicas` |
| Max delay | `system.replicas` (Virtual) | `SELECT max(absolute_delay) FROM system.replicas` |
| Queue depth | `system.replicas` (Virtual) | `SELECT sum(queue_size) FROM system.replicas` |
| Active fetches | `system.metrics` (Virtual) | `SELECT value FROM system.metrics WHERE metric = 'BackgroundFetchesPoolTask'` |
| Readonly replicas | `system.replicas` (Virtual) | `SELECT count() FROM system.replicas WHERE is_readonly = 1` |

---

## Engine Internals — Data Sources

### Memory X-Ray

| UI Element | Source | Query | Notes |
|---|---|---|---|
| **Total RSS** | `system.asynchronous_metrics` (Virtual) | `WHERE metric = 'MemoryResident'` | From jemalloc stats |
| **jemalloc.allocated** | `system.asynchronous_metrics` (Virtual) | `WHERE metric = 'jemalloc.allocated'` | Bytes actively allocated by application |
| **jemalloc.resident** | `system.asynchronous_metrics` (Virtual) | `WHERE metric = 'jemalloc.resident'` | Bytes in physically resident pages |
| **jemalloc.mapped** | `system.asynchronous_metrics` (Virtual) | `WHERE metric = 'jemalloc.mapped'` | Bytes mapped by allocator |
| **jemalloc.retained** | `system.asynchronous_metrics` (Virtual) | `WHERE metric = 'jemalloc.retained'` | Bytes retained (not returned to OS) |
| **jemalloc.metadata** | `system.asynchronous_metrics` (Virtual) | `WHERE metric = 'jemalloc.metadata'` | Allocator's own bookkeeping |
| **Fragmentation %** | Computed | `(1 - jemalloc.allocated / jemalloc.resident) × 100` | High fragmentation = wasted RSS |
| **Query working memory** | `system.processes` (Virtual) | `SELECT sum(memory_usage) FROM system.processes` | MemoryTracker per-query. Includes hash tables, sort buffers, JOIN buffers, intermediate blocks. |
| **Mark cache bytes** | `system.asynchronous_metrics` (Virtual) | `WHERE metric = 'MarkCacheBytes'` | Granule offset index cache |
| **Mark cache files** | `system.asynchronous_metrics` (Virtual) | `WHERE metric = 'MarkCacheFiles'` | Number of cached mark files |
| **Mark cache configured size** | Server config | `mark_cache_size` setting. Query: `SELECT value FROM system.settings WHERE name = 'mark_cache_size'` or `SELECT value FROM system.server_settings WHERE name = 'mark_cache_size'` |
| **Mark cache hit rate** | `system.events` (Virtual) | `MarkCacheHits` and `MarkCacheMisses`. Rate = `Hits / (Hits + Misses) × 100`. Take delta between polls for recent rate. |
| **Mark cache misses/sec** | `system.events` (Virtual) | Delta of `MarkCacheMisses` / poll_interval |
| **Uncompressed cache bytes** | `system.asynchronous_metrics` (Virtual) | `WHERE metric = 'UncompressedCacheBytes'` | Decompressed column blocks |
| **Uncompressed cache cells** | `system.asynchronous_metrics` (Virtual) | `WHERE metric = 'UncompressedCacheCells'` | Number of cached blocks |
| **Uncompressed cache configured size** | Server config | `uncompressed_cache_size` setting |
| **Uncompressed cache hit rate** | `system.events` (Virtual) | `UncompressedCacheHits` and `UncompressedCacheMisses`. Delta for rate. |
| **Primary key bytes in memory** | `system.parts` (Virtual) | `SELECT sum(primary_key_bytes_in_memory) FROM system.parts` | Run every 60s. Sparse index always resident. |
| **Primary key bytes allocated** | `system.parts` (Virtual) | `SELECT sum(primary_key_bytes_in_memory_allocated) FROM system.parts` | Includes jemalloc overhead per PK allocation |
| **Dictionary memory** | `system.dictionaries` (Virtual) | `SELECT sum(bytes_allocated) FROM system.dictionaries` | |
| **Dictionary count** | `system.dictionaries` (Virtual) | `SELECT count() FROM system.dictionaries` | |
| **Dictionary names** | `system.dictionaries` (Virtual) | `SELECT name, type, formatReadableSize(bytes_allocated) FROM system.dictionaries` | |
| **Merge buffers memory** | `system.merges` (Virtual) | `SELECT sum(memory_usage) FROM system.merges` | Memory held by active merge operations |
| **jemalloc overhead** | Computed | `jemalloc.resident - jemalloc.allocated` | Thread caches + fragmentation + retained |
| **Other / untracked** | Computed | `MemoryResident - (query_mem + mark_cache + uncomp_cache + pk_mem + dict_mem + merge_mem + jemalloc_overhead)` | Connections, AST cache, compiled expressions, etc. |
| **OS page cache** | `system.asynchronous_metrics` (Virtual) | `WHERE metric = 'OSMemoryCached'` | Linux page cache. Not ClickHouse memory, but important for IO performance. |
| **Free RAM** | Computed | `OSMemoryTotal - MemoryResident - OSMemoryCached - OSMemoryBuffers` | Approximate |

### CPU Core Map

| UI Element | Source | Query | Notes |
|---|---|---|---|
| **Per-core utilization %** | `system.asynchronous_metrics` (Virtual) | Metrics: `OSUserTime_N`, `OSSystemTime_N`, `OSIdleTime_N`, `OSIOWaitTime_N`, `OSNiceTime_N`, `OSSoftIrqTime_N`, `OSStealTime_N` where N = core index (0, 1, 2, ...). These are cumulative counters from `/proc/stat`. Take delta between polls, compute: `user_pct = delta_user / (delta_user + delta_system + delta_idle + delta_iowait) × 100`. | Added in ClickHouse ~22.x (PR #24416). Per-core metrics have suffix `_N`. |
| **Core state (user/sys/iowait/idle)** | Computed | From deltas above. Dominant state = highest delta. | |
| **Core owner (which query/merge)** | **Not directly available per-core** | ClickHouse does not expose which thread runs on which core. **Approximation**: from `system.processes`, each query has `thread_numbers` (array of OS thread IDs). You could map these to cores via `/proc/<pid>/task/<tid>/stat` field 39 (last CPU), but this requires OS-level access, not a CH query. **Simpler approach**: show core utilization heatmap without per-core owner attribution. Instead show aggregate attribution (X cores to queries, Y to merges) computed from ProfileEvents totals. | This is the one metric that's hard to get purely from CH system tables. |
| **Number of cores** | `system.asynchronous_metrics` (Virtual) | `WHERE metric = 'NumberOfPhysicalCores'` or `'NumberOfLogicalCores'` | |
| **Load average** | `system.asynchronous_metrics` (Virtual) | `WHERE metric IN ('LoadAverage1', 'LoadAverage5', 'LoadAverage15')` | |

**Honest note on core→owner mapping**: The per-core heatmap showing "core 5 is running merge:events" as shown in the UI sketch is an *aspirational* visualization. In reality, you can get per-core utilization from async_metrics, but attributing *which core runs which query/merge* requires either:
1. OS-level `/proc` introspection (reading `/proc/<clickhouse_pid>/task/<thread_id>/stat` for each thread's `processor` field)
2. Using `system.query_thread_log` after query completion (has `os_thread_id` but no core assignment)

**What IS directly available**: aggregate CPU attribution (total CPU to queries vs merges vs mutations) from ProfileEvents, and per-core utilization from async_metrics. The sketch should be adjusted to show per-core utilization bars colored by state (user/sys/iowait/idle) without claiming specific owner per core, plus a separate aggregate attribution bar.

### Thread Pools

| UI Element | Source | Query | Notes |
|---|---|---|---|
| Query execution threads (active) | `system.metrics` (Virtual) | `WHERE metric = 'QueryThread'` | Threads currently executing query pipeline operators |
| Query execution threads (max) | `system.settings` / config | `max_threads` per-query setting × concurrent queries. Or global: `SELECT value FROM system.server_settings WHERE name = 'max_thread_pool_size'` | |
| Merge & mutation threads (active) | `system.metrics` (Virtual) | `WHERE metric = 'BackgroundMergesAndMutationsPoolTask'` | |
| Merge & mutation threads (max) | `system.metrics` (Virtual) | `WHERE metric = 'BackgroundMergesAndMutationsPoolSize'` | Or from server config `background_pool_size` |
| Replication fetch threads (active) | `system.metrics` (Virtual) | `WHERE metric = 'BackgroundFetchesPoolTask'` | |
| Replication fetch threads (max) | `system.metrics` (Virtual) | `WHERE metric = 'BackgroundFetchesPoolSize'` | Or `background_fetches_pool_size` |
| Schedule pool (active) | `system.metrics` (Virtual) | `WHERE metric = 'BackgroundSchedulePoolTask'` | |
| Schedule pool (max) | `system.metrics` (Virtual) | `WHERE metric = 'BackgroundSchedulePoolSize'` | |
| IO threads (active) | `system.metrics` (Virtual) | `WHERE metric = 'IOThreads'` | If exposed; availability depends on version |
| Global thread pool (active) | `system.metrics` (Virtual) | `WHERE metric = 'GlobalThreadActive'` | |
| Global thread pool (total) | `system.metrics` (Virtual) | `WHERE metric = 'GlobalThread'` | |
| Move pool (TTL) | `system.metrics` (Virtual) | `WHERE metric = 'BackgroundMovePoolTask'` / `BackgroundMovePoolSize` | |
| Common pool | `system.metrics` (Virtual) | `WHERE metric = 'BackgroundCommonPoolTask'` / `BackgroundCommonPoolSize'` | |

### Primary Key Index by Table

| Column | Source | Query |
|---|---|---|
| Table name | `system.parts` (Virtual) | From GROUP BY |
| PK bytes in memory | `system.parts` (Virtual) | `SELECT database, table, sum(primary_key_bytes_in_memory) AS pk_mem, count() AS parts, sum(rows) AS rows FROM system.parts WHERE active GROUP BY database, table ORDER BY pk_mem DESC LIMIT 20` |
| PK bytes allocated | `system.parts` (Virtual) | `sum(primary_key_bytes_in_memory_allocated)` — includes jemalloc rounding |
| Parts count | `system.parts` (Virtual) | `count()` from above |
| Total rows | `system.parts` (Virtual) | `sum(rows)` from above |
| Granules count | `system.parts` (Virtual) | `SELECT sum(marks) FROM system.parts WHERE active GROUP BY database, table` — `marks` column = number of granules in the part |

### Dictionaries in Memory

| Column | Source | Query |
|---|---|---|
| Name | `system.dictionaries` (Virtual) | `name` |
| Type | `system.dictionaries` (Virtual) | `type` (flat, hashed, complex_key_hashed, range_hashed, etc.) |
| Bytes allocated | `system.dictionaries` (Virtual) | `bytes_allocated` |
| Element count | `system.dictionaries` (Virtual) | `element_count` |
| Load factor | `system.dictionaries` (Virtual) | `load_factor` |
| Source | `system.dictionaries` (Virtual) | `source` |
| Last successful update | `system.dictionaries` (Virtual) | `last_successful_update_time` |
| Loading status | `system.dictionaries` (Virtual) | `loading_status` (Loaded, Loading, Failed) |

Full query:
```sql
SELECT name, type, formatReadableSize(bytes_allocated) AS mem,
       element_count, round(load_factor, 3) AS lf,
       source, loading_status, last_successful_update_time
FROM system.dictionaries
ORDER BY bytes_allocated DESC
```

### jemalloc Arena Summary

| Metric | Source | Query |
|---|---|---|
| All jemalloc.* metrics | `system.asynchronous_metrics` (Virtual) | `SELECT metric, value FROM system.asynchronous_metrics WHERE metric LIKE 'jemalloc.%'` |

Available jemalloc metrics (subset):
- `jemalloc.allocated` — bytes actively in use
- `jemalloc.active` — bytes in active pages
- `jemalloc.metadata` — allocator metadata
- `jemalloc.metadata_thp` — transparent huge pages for metadata
- `jemalloc.resident` — bytes in physically resident pages
- `jemalloc.mapped` — bytes in active extents mapped by allocator
- `jemalloc.retained` — bytes in virtual memory mappings retained
- `jemalloc.background_thread.num_threads` — jemalloc background threads
- `jemalloc.background_thread.num_runs` — background thread runs
- `jemalloc.background_thread.run_intervals` — intervals between runs
- `jemalloc.arenas.all.dirty_purged`, `muzzy_purged` — purge stats

### Per-Query Memory Breakdown

| Element | Source | How to get it |
|---|---|---|
| Total query memory | `system.processes` (Virtual) | `memory_usage` column |
| Hash table (GROUP BY) | **Not directly exposed as a sub-component** | ClickHouse tracks total `memory_usage` per query, not per-operator. **Workaround**: for completed queries, `system.query_log` has `ProfileEvents['HashTableMemoryUsage']` if available (version-dependent). For running queries, you can infer: if query has GROUP BY with high cardinality, most memory is hash tables. |
| JOIN build side | **Not directly exposed** | Similar to above. The memory tracker reports total, not per-operator. `ProfileEvents['CreatedHTJoinEntitiesNumber']` and related counters exist but don't give byte breakdown. |
| Sort buffer | **Not directly exposed** | Same limitation. |
| **What IS available per-query** | `system.processes` (Virtual) | `memory_usage` (total), `read_rows`, `read_bytes`, `written_rows`, `written_bytes`, `elapsed`, `ProfileEvents` map (with CPU time, IO, cache hits/misses, selected marks, etc.) |

**Honest note**: The UI sketch shows a per-query "Memory Anatomy" (hash table: 6.8 GB, JOIN: 3.2 GB, etc.). This level of per-operator memory breakdown is **not available from system tables**. ClickHouse's memory tracker reports a single `memory_usage` per query. To get operator-level breakdown, you would need either:
1. Custom ClickHouse patches exposing per-operator memory
2. Heuristic estimation: e.g., "query has GROUP BY with 45M groups at ~150 bytes/group ≈ 6.8 GB"
3. `system.query_log` ProfileEvents have some counters (e.g. `HashJoinGetDefaultRightRow`, aggregation-related events) but not memory-per-operator

**Recommendation for the UI**: Show total `memory_usage` per query from `system.processes`, plus key ProfileEvents that *hint* at what's consuming memory (e.g., if `SelectedMarks` is huge, decompression buffers are large; if query has GROUP BY and high memory, it's hash tables). Don't claim a specific per-operator breakdown unless you build estimation logic.

### Per-Query ProfileEvents (available from `system.processes`)

These ARE directly available for running queries (CH 22.x+):

| ProfileEvent | What it tells you | Source |
|---|---|---|
| `UserTimeMicroseconds` | CPU time in user mode, summed across threads | `system.processes` ProfileEvents map |
| `SystemTimeMicroseconds` | CPU time in kernel mode | Same |
| `RealTimeMicroseconds` | Wall clock time summed across threads | Same |
| `OSIOWaitMicroseconds` | Time waiting for disk IO | Same |
| `OSCPUWaitMicroseconds` | Time runnable but waiting for free core | Same |
| `ReadCompressedBytes` | Compressed bytes read from MergeTree | Same |
| `OSReadBytes` | Bytes read at OS level (includes page cache misses) | Same |
| `OSWriteBytes` | Bytes written at OS level | Same |
| `SelectedParts` | Number of parts touched | Same |
| `SelectedRanges` | Number of mark ranges selected after PK index | Same |
| `SelectedMarks` | Number of granule marks selected (after index pruning) | Same |
| `MarkCacheHits` | Mark cache hits for this query | Same |
| `MarkCacheMisses` | Mark cache misses for this query | Same |
| `UncompressedCacheHits` | Uncompressed block cache hits | Same |
| `UncompressedCacheMisses` | Uncompressed block cache misses | Same |

**Derived metrics** (app-side computation from ProfileEvents above):

| Derived Metric | Formula |
|---|---|
| CPU cores used (avg) | `(UserTimeMicroseconds + SystemTimeMicroseconds) / RealTimeMicroseconds` |
| IO wait ratio | `OSIOWaitMicroseconds / (UserTimeMicroseconds + SystemTimeMicroseconds + OSIOWaitMicroseconds)` |
| Index pruning effectiveness | `1 - (SelectedMarks / TotalMarks)` where TotalMarks = total marks across selected parts (requires joining with parts info or using `ProfileEvents['SelectedMarks']` vs a precomputed total) |
| Mark cache hit rate (this query) | `MarkCacheHits / (MarkCacheHits + MarkCacheMisses)` |
| Read amplification | `OSReadBytes / ReadCompressedBytes` (>1 means reading more from disk than the compressed data size, indicates page cache misses or fragmented reads) |

---

## Polling Strategy Summary

| Tier | Interval | Tables Queried | Cost |
|---|---|---|---|
| **Tier 1** | Every 5 seconds | `system.asynchronous_metrics`, `system.metrics`, `system.processes`, `system.merges`, `system.mutations` | All Virtual — near-zero cost |
| **Tier 2** | Every 30 seconds | `system.query_log` (recent completed), `system.part_log` (recent completed), `system.events` (for deltas) | MergeTree reads but small time windows, cheap |
| **Tier 3** | Every 60 seconds | `system.parts` (aggregated), `system.disks`, `system.replicas`, `system.dictionaries`, `system.columns` | Virtual or light aggregation |

---

## Version Compatibility Notes

| Feature | Minimum Version | How to Check |
|---|---|---|
| ProfileEvents in `system.processes` | ~22.x | `SELECT * FROM system.processes LIMIT 1 FORMAT Vertical` — look for `ProfileEvents` column |
| Per-core CPU metrics (`OSUserTime_N`) | ~22.x (PR #24416) | `SELECT metric FROM system.asynchronous_metrics WHERE metric LIKE 'OSUserTime_%' LIMIT 1` |
| `query_kind` column in processes | ~22.x | Check column exists |
| `total_rows_approx` in processes | ~22.x | Check column exists |
| `jemalloc.*` async metrics | Most versions | `SELECT metric FROM system.asynchronous_metrics WHERE metric LIKE 'jemalloc%'` |
| `system.server_settings` | ~23.x | `SELECT 1 FROM system.server_settings LIMIT 1` |
| `marks` column in `system.parts` | Most versions | `SELECT marks FROM system.parts LIMIT 1` |
| `primary_key_bytes_in_memory_allocated` | Most versions | Check column exists |
| `MarkCacheBytes` async metric | ~20.x+ | Should be present in all modern versions |
| `ProfileEvents` in `system.part_log` | ~22.x (GitHub #7455) | `SELECT ProfileEvents FROM system.part_log LIMIT 1` |

---

## What's NOT Available from System Tables (Limitations)

| Desired Metric | Status | Workaround |
|---|---|---|
| Per-operator memory breakdown within a query (hash table vs sort buffer vs JOIN) | **Not exposed** | Estimate from query structure + total memory_usage. Or use `EXPLAIN ESTIMATE` for pre-execution sizing hints. |
| Which CPU core is running which thread/query | **Not exposed** | Read `/proc/<pid>/task/<tid>/stat` field 39 for last-run CPU. Or just show per-core utilization without owner attribution. |
| Live ProfileEvents for active merges | **Not exposed** (`system.merges` has no ProfileEvents) | Use `system.part_log` for completed merges. For running merges, use elapsed × historical CPU-per-second from part_log. |
| Memory breakdown by cache tier for a specific query | **Not exposed** | Mark cache hits/misses are per-query in ProfileEvents, but you can't tell how many bytes of mark cache are "owned" by a query. |
| Filesystem cache bytes per query | **Not exposed** | `FilesystemCacheSize` is global, not per-query. |
| Async insert buffer memory | **Partially exposed** | `system.metrics` has `AsynchronousInsertBytes` in recent versions. |