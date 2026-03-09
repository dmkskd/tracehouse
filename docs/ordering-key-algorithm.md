# Ordering Key Efficiency Analysis — Algorithm & Reasoning

## Overview

The Analytics tab analyzes how efficiently queries use a table's ORDER BY (sorting key)
for granule pruning. This document explains the algorithm, its data sources, and the
reasoning behind each diagnostic category.

## Data Sources

### Actual data: `system.query_log` ProfileEvents

The actual pruning effectiveness comes from ClickHouse's own instrumentation, recorded
per-query in `system.query_log`:

| ProfileEvent | What it measures |
|---|---|
| `SelectedMarks` | Granules (index marks) actually read |
| `SelectedMarksTotal` | Total granules available across all parts |
| `SelectedParts` / `SelectedPartsTotal` | Part-level pruning |
| `FilteringMarksWithPrimaryKeyMicroseconds` | Time spent evaluating the primary index |

**Pruning percentage** = `(SelectedMarksTotal - SelectedMarks) / SelectedMarksTotal × 100`

This is the real data — it reflects what the ClickHouse query optimizer actually did,
including partition pruning, primary key filtering, and any skip indexes.

### Which key columns were used: `EXPLAIN json = 1, indexes = 1`

When query patterns are loaded in the UI, the system automatically runs
`EXPLAIN json = 1, indexes = 1` on each sample query. The EXPLAIN result tells us
exactly which ordering key columns the optimizer used for index filtering, what condition
was evaluated, and how many parts/granules were pruned.

The parsed result feeds directly into `diagnoseOrderingKeyUsage()` — when EXPLAIN keys
are available, the diagnostics use the actual key columns reported by the optimizer.

EXPLAIN runs the query planner only (no execution), so it's cheap. However, the result
reflects the current data distribution, which may differ from when the historical query
actually ran.

### Fallback: regex-based WHERE clause analysis

If EXPLAIN fails for a query pattern (e.g., the query references a dropped table), we
fall back to regex-based extraction (`extractWhereColumnsRegex`) to find column references
in WHERE/PREWHERE/HAVING clauses. It matches common patterns:

- Comparisons: `col = val`, `col != val`, `col >= val`
- IN clauses: `col IN (...)`
- BETWEEN: `col BETWEEN ... AND ...`
- LIKE/ILIKE: `col LIKE ...`
- NULL checks: `col IS NULL`, `col IS NOT NULL`
- Function calls: `func(col)` — extracts the inner column

The regex approach is intentionally conservative. It strips comments, replaces string
literals, and filters out SQL keywords to reduce false positives. It handles most real-world
queries well, but can miss columns in complex expressions or CTEs.

### Table metadata: `system.tables` and `system.parts`

- `system.tables.sorting_key` — the ORDER BY expression (e.g., `event_date, user_id, event_time`)
- `system.tables.primary_key` — usually identical to sorting_key unless explicitly different
- `system.parts` — active part count, total rows, total marks

## How ClickHouse Primary Key Pruning Works

ClickHouse uses a **sparse primary index**: one entry per granule (~8192 rows), storing the
first row's key column values. At query time, ClickHouse evaluates the WHERE clause against
these index marks to select which granules to read.

Two algorithms are used, depending on which key columns appear in the WHERE clause:

### Binary Search (leftmost key column)

When the WHERE clause filters on the **first (leftmost)** column of the ORDER BY key,
ClickHouse runs binary search over the index marks. This is O(log₂ n) — very efficient.

Server trace log shows: `Running binary search on index range for part ... (N marks)`

### Generic Exclusion Search (non-leftmost key columns)

When the WHERE clause filters on a key column that is **not the leftmost**, ClickHouse
uses the generic exclusion search algorithm. It iterates over marks trying to *exclude*
granules where the filter condition is provably impossible.

**Effectiveness depends on predecessor column cardinality:**

- **Low-cardinality predecessor** → The same predecessor value spans many consecutive
  granules, so the non-leftmost column's values are locally sorted within those spans.
  ClickHouse can effectively exclude many granules.

- **High-cardinality predecessor** → Each mark has a different predecessor value, so
  ClickHouse cannot make assumptions about the range of values in any granule. Almost
  no granules can be excluded — effectively a full scan.

Server trace log shows: `Used generic exclusion search over index for part ... with N steps`

**This is a critical distinction from B-Tree databases** (MySQL, PostgreSQL) where
non-prefix columns are simply not usable. ClickHouse *always tries* to use all key
columns in the WHERE clause, but the benefit is data-dependent.

Reference: https://clickhouse.com/docs/guides/best-practices/sparse-primary-indexes

### Verifying with EXPLAIN

For any specific query, you can verify the actual index usage:

```sql
EXPLAIN indexes = 1
SELECT count() FROM synthetic_data.events
WHERE event_date = today() - 1 AND user_id = 42;
```

Output shows:
```
Indexes:
  PrimaryKey
    Keys:
      event_date        ← which key columns were used
      user_id
    Condition: ...      ← the exact predicate evaluated against the index
    Parts: 1/3          ← part-level pruning
    Granules: 2/1083    ← granule-level pruning (the key metric)
```

**Note:** `EXPLAIN indexes = 1` does NOT distinguish between binary search and generic
exclusion search. Both appear the same way. To see which algorithm was used, check the
server trace log.

### EXPLAIN Result Display

The EXPLAIN result is displayed inline when a query pattern row is expanded:

- **Keys** — which ordering key columns were used for index filtering
- **Condition** — the exact predicate evaluated against the sparse index
- **Parts** — part-level pruning (selected/total)
- **Granules** — granule-level pruning (selected/total)
- **Skip indexes** — any data-skipping indexes (MinMax, Set, Bloom) that contributed

## Diagnostic Categories

Our algorithm produces one of these diagnoses for each query pattern:

### Full key match ✅
- **Condition:** WHERE clause contains all ORDER BY columns
- **Index algorithm:** Binary search on full prefix
- **Severity:** `good`
- **Example:** ORDER BY (event_date, user_id, event_time), WHERE event_date = ... AND user_id = ... AND event_time >= ...

### Partial key (N/M) ⚠️
- **Condition:** WHERE clause contains the leftmost key column plus some (but not all) subsequent columns
- **Index algorithm:** Binary search on the contiguous prefix
- **Severity:** `warning` (or `good` if actual pruning ≥ 90%)
- **Example:** ORDER BY (event_date, user_id, event_time), WHERE event_date = ... AND user_id = ...
- **Note on gaps:** If WHERE uses columns 1 and 3 but skips column 2, the label shows the
  effective prefix length (1/3, not 2/3). ClickHouse uses binary search for the prefix and
  generic exclusion search for the non-contiguous columns. The reason text explains this.

### Skips leftmost key ❌
- **Condition:** WHERE clause filters on ORDER BY columns but NOT the leftmost one
- **Index algorithm:** Generic exclusion search (data-dependent effectiveness)
- **Severity:** `poor` (or `warning` if actual pruning ≥ 50% — meaning the predecessor
  column has low cardinality and generic exclusion search is working)
- **Example:** ORDER BY (event_date, user_id, event_time), WHERE user_id = 42
- **Key insight:** This is NOT always terrible. If `event_date` has low cardinality
  (e.g., only a few distinct dates), ClickHouse's generic exclusion search can still
  prune effectively. We use the actual pruning ratio to adjust severity.

### No key match ❌
- **Condition:** WHERE clause filters on columns that are NOT in the ORDER BY key
- **Index algorithm:** None — primary index cannot help
- **Severity:** `poor`
- **Example:** ORDER BY (event_date, user_id, event_time), WHERE country_code = 'US'
- **Recommendation:** Consider adding a skip index (minmax, set, bloom_filter) on the
  filtered column, or use a projection with a different ORDER BY.

### No WHERE clause ⚠️
- **Condition:** Query has no WHERE/PREWHERE clause — full table scan
- **Index algorithm:** None
- **Severity:** `warning` (or `poor` if actual pruning < 50%)
- **Note:** Full scans are expected for aggregations over all data (e.g., `SELECT count() FROM t`).
  This is not necessarily a problem.

### No ORDER BY ⚠️
- **Condition:** Table has no sorting key defined
- **Severity:** `warning`
- **Note:** Rare for MergeTree tables, which require ORDER BY.

## Algorithm Limitations

1. **WHERE extraction is heuristic.** The regex-based extraction handles common SQL patterns
   but can miss columns in complex expressions or CTEs, and may produce false positives
   (e.g., extracting column names from non-WHERE contexts). When EXPLAIN indexes data is
   available, the actual optimizer keys are used instead.

2. **We cannot distinguish binary search from generic exclusion search** without running
   `EXPLAIN indexes = 1` or reading the server trace log. We infer the algorithm based on
   which key columns appear in WHERE, which is correct for the common case.

3. **Actual pruning depends on data distribution**, not just query structure. Two queries
   with identical WHERE clauses can have different pruning ratios depending on the data
   in each partition/part. We use the average pruning ratio across all executions of a
   query pattern.

4. **Skip indexes and projections** can improve pruning beyond what the primary key provides.
   Our analysis focuses on primary key usage only. If a table has a minmax skip index on
   `country_code`, a query filtering on `country_code` may show good pruning even though
   we diagnose it as "No key match."

5. **`system.query_log` does not record which key columns were used for index filtering.**
   That information is only available via `EXPLAIN indexes = 1` (which shows `Keys: [...]`)
   or the server trace log. We automatically run EXPLAIN on each query pattern to get the
   actual keys, falling back to heuristic WHERE extraction when EXPLAIN fails.

## Data Flow

```
system.query_log                    system.tables         system.parts
├─ query (SQL text)                 ├─ sorting_key        ├─ rows
├─ normalized_query_hash            ├─ primary_key        ├─ marks
├─ tables[] (array)                 └─ engine             └─ active
├─ ProfileEvents[SelectedMarks]
├─ ProfileEvents[SelectedMarksTotal]
└─ ProfileEvents[FilteringMarksWithPrimaryKeyMicroseconds]
        │                                   │                    │
        ▼                                   ▼                    ▼
   ┌─────────────────────────────────────────────────────────────────┐
   │  TABLE_ORDERING_KEY_EFFICIENCY SQL query (analytics-queries.ts) │
   │  Joins query_log × tables × parts, groups by table             │
   └─────────────────────────────────────────────────────────────────┘
        │
        ▼
   ┌─────────────────────────────────────────────────────────────────┐
   │  TABLE_QUERY_PATTERNS SQL query                                │
   │  Groups by normalized_query_hash for drill-down                │
   └─────────────────────────────────────────────────────────────────┘
        │
        ▼
   ┌──────────────────────────────────────────────────────────────────┐
   │  EXPLAIN json=1, indexes=1  (auto-runs for each query pattern)  │
   │  Returns optimizer data: Keys, Condition, Parts, Granules       │
   └──────────────────────────────────────────────────────────────────┘
        │
        ▼
   ┌──────────────────────────────────────────────────────────────────┐
   │  diagnoseOrderingKeyUsage() — client-side                       │
   │  (ordering-key-diagnostics.ts)                                  │
   │  1. Parse sorting_key → ORDER BY columns                        │
   │  2. If EXPLAIN keys available → use them as matched columns     │
   │     Else → parse sample_query WHERE → heuristic matching        │
   │  3. Prefix length, gap detection, index algorithm inference     │
   │  4. Reconcile with actual pruning ratio from query_log          │
   │  5. Produce diagnosis + severity + reason                       │
   └──────────────────────────────────────────────────────────────────┘
```
