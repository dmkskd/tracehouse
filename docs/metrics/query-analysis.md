# Query Analysis Metrics

## Query Efficiency Score (Pruning)

How effectively the query used primary key indexes to skip data.

**Source:** `query_log.ProfileEvents` → `SelectedMarks`, `SelectedMarksTotal`

```
efficiency = ((SelectedMarksTotal - SelectedMarks) / SelectedMarksTotal) × 100
```

| Score | Rating |
| --- | --- |
| 90%+ | Excellent, only 10% of marks scanned |
| 50–90% | Good, some pruning |
| <50% | Poor, consider optimizing PK or adding skip indexes |
| null | No marks data (e.g. `SELECT 1`, system queries, non-MergeTree) |

Higher is better (more data skipped). Displayed as "Pruning" badge in the table and "Index Pruning" card in detail view.

> **Tests:** `query-analyzer.integration.test.ts` → "getQueryHistory"

**Why not `result_rows / read_rows`?** A `GROUP BY` over 10M rows returning 5 results is perfectly efficient. Row ratio would incorrectly flag it as poor. Marks pruning measures what matters: how well the PK avoided scanning irrelevant data.

## Index Selectivity

Two levels of selectivity, both from `query_log.ProfileEvents`:

**Parts:** `SelectedParts / SelectedPartsTotal × 100`
**Marks:** `SelectedMarks / SelectedMarksTotal × 100`

Lower is better (more pruning). Color coding: ≤10% green, ≤50% yellow, >50% red. Only available for MergeTree tables.

Marks are finer-grained than parts (default 8192 rows per mark). Index Pruning Effectiveness is the inverse of marks selectivity.

## Mark Cache Hit Rate

```
hit_rate = MarkCacheHits / (MarkCacheHits + MarkCacheMisses) × 100
```

Higher is better. >80% indicates good cache utilization. Low rates may mean cache is too small or queries touch different data each time.

## Parallelism Factor

```
parallelism = (UserTimeMicroseconds + SystemTimeMicroseconds) / RealTimeMicroseconds
```

- 1.0 = single-threaded
- 4.0 = effectively using 4 cores
- Higher = better parallelization

## IO Wait

```
io_wait_pct = OSIOWaitMicroseconds / RealTimeMicroseconds × 100
```

Lower is better. >50% = I/O bound, >20% = moderate, ≤20% = CPU bound or well-cached.

## Query CPU (per-query, running)

Effective CPU cores used by a running query:

```
cpu_cores = (UserTimeMicroseconds + SystemTimeMicroseconds) / (elapsed × 1,000,000)
```

A value of 2.5 means ~2.5 cores on average.

## Color coding policy

Only metrics with clear, unambiguous semantic thresholds get color:
- **Status** (success/failed/running)
- **Pruning / selectivity** (well-defined thresholds from PK behavior)

Everything else (duration, CPU time, peak memory, network, disk, cache hit rate, IO wait) uses default text color. A 2-minute query isn't inherently "good" or "bad" without knowing the workload.
