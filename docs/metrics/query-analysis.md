# Query Analysis Metrics

## Query Efficiency Score (Pruning)

How effectively the query used primary key indexes to skip data.

**Source:** `query_log.ProfileEvents` → `SelectedParts`, `SelectedPartsTotal`, `SelectedMarks`, `SelectedMarksTotal`

```
parts_survival = SelectedParts / SelectedPartsTotal
marks_survival = SelectedMarks / SelectedMarksTotal
efficiency = (1 - parts_survival × marks_survival) × 100
```

| Score | Rating |
| --- | --- |
| 90%+ | Excellent, most parts/marks pruned |
| 70–90% | Good, reasonable pruning |
| 50–70% | Fair, some pruning |
| <50% | Poor, near full-scan territory |
| null | No pruning data (e.g. `SELECT 1`, system queries, non-MergeTree) |

Higher is better (more data skipped). Displayed as "Pruning" badge in the table and "Index Pruning" card in detail view.

> **Tests:** `pruning.test.ts`, `query-mappers.test.ts`

**Why not marks-only pruning?** `SelectedMarksTotal` only counts marks in the parts that survived partition pruning. If a query selects 1 out of 75 parts and reads all marks in that one part, marks-only pruning says 0%, but combined pruning correctly reports 98.7%.

**Why not `result_rows / read_rows`?** A `GROUP BY` over 10M rows returning 5 results is perfectly efficient. Row ratio would incorrectly flag it as poor. Parts and marks pruning measure what matters: how well ClickHouse avoided scanning irrelevant data.

## Index Selectivity

Two levels of selectivity, both from `query_log.ProfileEvents`:

**Parts:** `SelectedParts / SelectedPartsTotal × 100`
**Marks:** `SelectedMarks / SelectedMarksTotal × 100`

Lower is better (more pruning). Color coding: ≤10% green, ≤50% yellow, >50% red. Only available for MergeTree tables.

Marks are finer-grained than parts (default 8192 rows per mark). Index Pruning Effectiveness combines parts selectivity and marks selectivity.

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
