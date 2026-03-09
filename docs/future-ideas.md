# Future Feature Ideas

Ideas and inspiration for features not yet implemented.

---

## From "Know Your ClickHouse" (Azat Khuzhin, 2022)

Source: https://azat.sh/presentations/2022-know-your-clickhouse/

### ProfileEvents Diff Between Two Queries

Compare a slow query vs a fast query by extracting ProfileEvents from `system.query_log`
and computing percentage differences. Could power a "Compare Two Queries" feature in
the Query Monitor.

```sql
WITH
    faster AS (
        SELECT pe.1 AS event_name, pe.2 AS event_value
        FROM (
            SELECT ProfileEvents.Names, ProfileEvents.Values
            FROM system.query_log
            WHERE query_id = '<fast_id>' AND type = 'QueryFinish'
        )
        ARRAY JOIN arrayZip(ProfileEvents.Names, ProfileEvents.Values) AS pe
    ),
    slower AS (...)
SELECT
    event_name,
    formatReadableQuantity(slower.event_value) AS slower_value,
    formatReadableQuantity(faster.event_value) AS faster_value,
    round((slower.event_value - faster.event_value) / slower.event_value, 2) AS diff_q
FROM faster LEFT JOIN slower USING (event_name)
WHERE diff_q > 0.05
ORDER BY event_name ASC
SETTINGS join_use_nulls = 1
```

### Cache Hit Ratio vs Query Latency

Correlate mark cache, uncompressed cache, and page cache hit ratios with p90 query duration.
Could add a "Cache Effectiveness" card to Engine Internals.

Key formulas:
- Mark cache ratio: `MarkCacheHits / (MarkCacheHits + MarkCacheMisses)`
- Page cache ratio: `(OSReadChars - OSReadBytes) / OSReadChars`

### Memory Fragmentation Tracking

Compare RSS against sum of accounted memory (caches, queries, merges, dictionaries, PK).
The gap = fragmentation. Could extend Memory X-Ray with a fragmentation indicator.

### EXPLAIN PIPELINE + Processor Profiling

`EXPLAIN PIPELINE` shows the execution DAG. Combined with `system.processors_profile_log`
(using `log_processors_profiles = 1`), you get per-processor timing. Natural extension
for the Query detail view.

```sql
SELECT
    name,
    sum(elapsed_us) / 1e6 AS elapsed,
    sum(need_data_elapsed_us) / 1e6 AS in_wait,
    sum(port_full_elapsed_us) / 1e6 AS out_wait
FROM system.processors_profile_log
WHERE query_id = '<id>'
GROUP BY name
HAVING elapsed > 2
ORDER BY elapsed DESC
```

### Memory Profiler Flamegraphs

Setting `memory_profiler_sample_probability = 1` enables per-query memory allocation
traces. Could feed into the existing Flamegraph component for memory-focused flamegraphs.

### Query Protection Settings

`force_index_by_date`, `force_primary_key`, `force_data_skipping_indices` — settings
that prevent full scans. Could surface warnings when queries aren't leveraging these.

### External GROUP BY / ORDER BY Detection

When queries hit memory limits, `max_bytes_before_external_group_by` and
`max_bytes_before_external_sort` spill to disk. Could detect when queries would
benefit from these settings and suggest them.
