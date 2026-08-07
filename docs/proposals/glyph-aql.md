# Radar Charts in AQL: Working Proposal

This is a working proposal, not user-facing documentation and not implemented yet.

Goal: make radar rendering a formal AQL contract. Most queries should return raw metric columns and let AQL define the radar shape, normalization, labels, and color semantics. SQL-normalized radar arrays remain available as an explicit escape hatch for custom shapes.

## Data Contract

A radar chart is a compact multi-axis shape. It does not have to be five dimensions.

There are two supported data contracts:

1. **AQL-created radar visualizations**: SQL returns raw metric columns; AQL maps those columns to radar axes and normalizes them.
2. **SQL-created radar values**: SQL returns normalized radar arrays directly; AQL only renders those arrays.

Recommended AQL-created fields:

- `radar_column`: display-only table column name created by AQL. This column is not expected in the SQL result.
- `axes`: comma-separated `axis:result_column` mappings. Result columns must exist in the SQL output.
- `ranges`: comma-separated `axis:low..high` ranges. Values at or below `low` map to `0`; values at or above `high` map to `1`.
- `transforms`: optional comma-separated `axis:transform` overrides. If omitted, the profile/default transform is used.
- `profile`: optional named profile that defines axis order, labels, default transforms, color behavior, and tooltip behavior.
- `color`: optional AQL-computed color source, for example `axis_max` or `profile_level`.
- `color_by`: optional numeric result column, expected in `[0, 1]`, used instead of `color` when color should come from SQL.
- `color_scale`: optional color scale name. Default: `pressure`.
- `colors`: optional threshold stops for `color_scale=custom`.

Recommended SQL-created fields:

- `values`: array of numbers, expected in `[0, 1]`. Values outside the range are clamped.
- `labels`: optional array of strings, same order as `values`.
- `color_by`: optional numeric result column, expected in `[0, 1]`, used only for color.
- `color_scale`: optional color scale name. Default: `pressure`.
- `colors`: optional threshold stops for `color_scale=custom`.

Built-in `pressure` color scale:

| Score | Color |
| --- | --- |
| `0.00` | neutral |
| `0.15` | green |
| `0.40` | amber |
| `0.65` | orange |
| `0.85` | red |

Custom color stops:

```sql
color_scale=custom colors=0:#8b949e,0.15:#22c55e,0.40:#f59e0b,0.65:#f97316,0.85:#ef4444
```

Shape and color are separate:

- `axes` + `ranges` + `transforms`, or SQL `values`, control the radar silhouette.
- `color` or `color_by` controls the radar color.
- `color_scale` and `colors` define how the `color_by` value maps to color.
- Raw result columns can still be shown in tooltip/details.

Important:

- `column`, `labels`, `values`, and `color_by` refer to columns returned by the SQL query.
- `radar_column` creates a display-only table column. It is not returned by SQL.
- `color=axis_max` and `color=profile_level` are AQL-computed color sources.

Recommended cell example, with simple SQL:

```sql
-- @cell: type=radar radar_column=shape profile=query_pressure axes=time:query_duration_ms,memory:memory_usage,cpu:cpu_ms,io:io_bytes,scan:scan_pressure ranges=time:100..60000,memory:32Mi..8Gi,cpu:100..60000,io:1Mi..10Gi,scan:0..1 color=profile_level
SELECT
    query_id,
    query_kind,
    type,
    exception,
    query_duration_ms,
    memory_usage,
    ProfileEvents['OSCPUVirtualTimeMicroseconds'] / 1000 AS cpu_ms,
    greatest(
        read_bytes,
        ProfileEvents['ReadBufferFromFileDescriptorReadBytes'],
        ProfileEvents['NetworkReceiveBytes']
    ) AS io_bytes,
    if(
        ProfileEvents['SelectedMarksTotal'] > 0,
        ProfileEvents['SelectedMarks'] / ProfileEvents['SelectedMarksTotal'],
        0
    ) AS scan_pressure
FROM system.query_log
WHERE type = 'QueryFinish'
LIMIT 10
```

In this example, `shape` is created by AQL. SQL only returns raw metric columns.

SQL-created escape hatch:

```sql
-- @cell: column=pressure_values type=radar labels=pressure_labels color_by=pressure_score color_scale=pressure
SELECT
    query_id,
    [0.7, 0.2, 0.5, 0.9] AS pressure_values,
    ['time', 'memory', 'cpu', 'i/o'] AS pressure_labels,
    0.9 AS pressure_score
FROM system.query_log
LIMIT 10
```

With custom colors:

```sql
-- @cell: column=pressure_values type=radar labels=pressure_labels color_by=pressure_score color_scale=custom colors=0:#8b949e,0.15:#22c55e,0.40:#f59e0b,0.65:#f97316,0.85:#ef4444
SELECT
    [time_score, memory_score, cpu_score, io_score, scan_score] AS pressure_values,
    ['time', 'memory', 'cpu', 'i/o', 'scan'] AS pressure_labels,
    greatest(time_score, memory_score, cpu_score, io_score, scan_score) AS pressure_score
FROM ...
```

## Cell Syntax

Recommended single-line form, AQL-created display column:

```sql
-- @cell: type=radar radar_column=shape profile=query_pressure axes=time:query_duration_ms,memory:memory_usage,cpu:cpu_ms,io:io_bytes,scan:scan_pressure ranges=time:100..60000,memory:32Mi..8Gi,cpu:100..60000,io:1Mi..10Gi,scan:0..1 color=profile_level
```

SQL-created form:

```sql
-- @cell: column=pressure_values type=radar labels=pressure_labels color_by=pressure_score color_scale=pressure
```

Example result columns:

```text
query_id | query_duration_ms | memory_usage | cpu_ms | io_bytes | scan_pressure
```

## Chart Syntax

For a radar chart with simple SQL:

```sql
-- @chart: type=radar label=query_id profile=query_pressure axes=time:query_duration_ms,memory:memory_usage,cpu:cpu_ms,io:io_bytes,scan:scan_pressure ranges=time:100..60000,memory:32Mi..8Gi,cpu:100..60000,io:1Mi..10Gi,scan:0..1 color=profile_level
```

For a radar chart from SQL-created values:

```sql
-- @chart: type=radar label=query_id values=pressure_values labels=pressure_labels color_by=pressure_score color_scale=pressure
```

This lets the same data contract power a table cell or a full panel.

## Application Patterns

This pass reviewed the shipped analytics query modules and built-in dashboards:

- `Selects`, `Inserts`, `Merges`, `Merge Analytics`, `Memory`, `Replication`, `Mutations`, `Disks`, `JSON`, `Self-Monitoring`, `Cloud Providers`, `Grafana Imports`, and `Knowledge Base`.
- Existing cell directives are mostly `rag`, `gauge`, and `sparkline`.
- Existing chart directives are mostly time-series, bars, pies, and grouped/stacked variants.

Radar visualizations are most useful where a row has several competing signals and a single RAG/gauge column is too narrow.

### Radar Cells

Use radar cells when the user is scanning many rows and needs a compact visual hint before opening details.

Concrete candidates from current presets:

| Area | Existing query | Why radar cell fits | Suggested dimensions |
| --- | --- | --- | --- |
| Query cost | `Selects#Most Expensive Selects` | Rows already show duration, read rows, read bytes, result rows, memory. A radar would summarize the pressure shape before opening query details. | duration, memory, rows read, bytes read, result size |
| Query cost | `Selects#Recent Selects` | This is a query-picking table. A radar beside `query_id` would make expensive recent queries stand out without adding more columns. | duration, memory, rows read, bytes read, result rows |
| App self-monitoring | `Self-Monitoring#TraceHouse Query Cost Details` | Already has many RAG columns for memory/result bytes. A radar would combine duration, memory, rows, bytes, CPU into one row signal. | avg duration, max memory, total rows read, total bytes read, total CPU |
| App self-monitoring | `Self-Monitoring#TraceHouse Query Executions` | Individual executions are exactly the same shape as query history. | duration, memory, rows read, read bytes, CPU |
| App self-monitoring | `Self-Monitoring#Slowest TraceHouse Queries` | Good for spotting whether "slow" means CPU, read volume, memory, or all of them. | duration, memory, read MB, rows read, CPU |
| Memory | `Memory#Historical Top Memory Queries` | Current RAG focuses only on memory. Radar can separate memory-only queries from memory+CPU+I/O queries. | memory, duration, read bytes, read rows, CPU |
| Memory | `Memory#Memory Query Executions` | Drill target for a query shape; radar helps compare executions of the same shape. | memory, duration, read bytes, read rows, CPU |
| Memory | `Memory#Top Running Queries by Memory` | Running queries can be memory-heavy but otherwise cheap, or heavy across all resources. | memory, elapsed, read rows, read bytes, CPU if available |
| Cloud provider | `Cloud Providers#ClickHouse Cloud Query Cost Details` | Same cost-shape problem as app self-monitoring, but for cloud/provider activity. | avg duration, max memory, rows read, bytes read, CPU |
| Cloud provider | `Cloud Providers#ClickHouse Cloud Query Executions` | Individual query rows; good table-cell use. | duration, memory, rows read, bytes read, CPU |
| Knowledge Base | `Knowledge Base#Most resource-intensive queries` | Current query already ranks by resource counters. A radar would make the offender type visible. | CPU, memory, read rows, read bytes, duration |
| Knowledge Base | `Knowledge Base#Worst Offender Query Ranks` | Multiple ranks are hard to scan as columns. A radar can encode "bad in one dimension" vs "bad everywhere". | CPU rank, memory rank, read rank, time rank, failure rank |
| Inserts | `Inserts#Top Inserts by Memory` | Insert rows have memory, duration, rows, bytes; radar can distinguish big batch from inefficient write. | memory, duration, rows written, bytes written, CPU |
| JSON | `JSON#JSON Columns Inventory` | Existing gauges cover JSON bytes and path-limit pressure separately. A radar could combine schema/path/storage pressure. | JSON bytes, path-limit %, subcolumns, parts, rows |
| JSON | `JSON#JSON Merge Cost Breakdown` | Multiple merge-cost columns already have gauges. Radar could show whether JSON merge cost is CPU, rows, or bytes driven. | rows merged, read bytes, CPU/wall ratio, duration, parts |
| Replication | `Replication#Replica Status` | Multiple health booleans plus lag. A radar can summarize replica health per table. | lag, readonly, session expired, queue size if joined, errors if joined |
| Replication | `Replication#Replication Queue Summary` | Queue type rows have queued/executing/retried/errors. Radar makes bad queue shapes obvious. | queued, executing, retries, max tries, errors |
| Replication | `Replication#Distribution Queue` | Existing RAG columns are several independent failure counters. A radar can combine blocked/errors/broken/pending. | blocked, errors, broken files, pending files, pending bytes |
| Mutations | `Mutations#Active Mutations` | Active mutation rows have progress/failure/wait state; radar can show long-running or stuck mutations. | parts remaining, elapsed, failed parts, latest fail age, block numbers pending |
| Merges | `Merges#Active Merges` | Running merge rows have progress, elapsed, bytes read/written, memory, rows. Radar can show live merge pressure compactly. | elapsed, memory, rows read, bytes read, bytes written |
| Merges | `Merges#Replication Queue Backlog` | Similar to replication queue summary; current table has several status counters. | queued, executing, retried, max tries, errors |
| Tables | `Overview#Table Health` | Already has gauge/RAG/sparkline. A radar could summarize table health in one cell for inventory scanning. | disk %, active parts, part-size variance, rows, bytes |
| Storage | `Knowledge Base#Active Parts per Partition` | Hot partition rows have count pressure and drill into parts. Radar could add size/age alongside count. | part count, total bytes, rows, oldest part age, pressure score |
| Disks | `Disks#Disk Free Space` | Only if extended with I/O/errors; with current single `used_pct`, radar is not useful. | used %, read rate, write rate, reserved, errors |

Best first implementation candidates:

1. `Selects#Most Expensive Selects`
2. `Self-Monitoring#TraceHouse Query Cost Details`
3. `Memory#Historical Top Memory Queries`
4. `JSON#JSON Columns Inventory`
5. `Replication#Replica Status`

Radar cells should be small and deterministic. They should answer: "does this row look unusual compared with the others?"

Recommended behavior:

- render only the radar and color in the cell;
- expose raw values in tooltip;
- keep row sorting/filtering based on ordinary columns;
- clicking the row, not the radar itself, opens details unless the table supports cell actions.

Example:

```sql
-- @cell: column=pressure_values type=radar labels=pressure_labels color_by=pressure_score color_scale=pressure
SELECT
    query_id,
    query_duration_ms,
    memory_usage,
    read_bytes,
    pressure_values,
    pressure_labels,
    pressure_score
FROM ...
```

### Full Radar Charts

Use a full radar chart when the shape itself is the main comparison object.

Concrete candidates from current presets:

| Area | Existing query / dashboard | Why full radar chart fits | Suggested label |
| --- | --- | --- | --- |
| Query comparison | `Selects#Most Expensive Selects` as a new chart variant | Show top N query shapes together. Useful when asking "what kind of expensive queries do we have?" | short query id or normalized query hash |
| App self-monitoring | `Self-Monitoring#TraceHouse Query Cost Details` | Compare TraceHouse internal query shapes by service/component. | service + query hash |
| Cloud provider | `Cloud Providers#ClickHouse Cloud Query Cost Details` | Compare provider/internal/console query shapes. | user or query hash |
| Memory | `Memory#Historical Top Memory Queries` | Compare top memory offenders by whether they are memory-only or multi-resource-heavy. | query hash |
| Merge analytics | `Merge Analytics#Merge Throughput by Table` + `Merge Analytics#Merge Duration by Table` | A new combined query could compare tables by merge duration, throughput, wait time, and active-part pressure. | table |
| Merge imbalance | `Merges#Replica Merge Imbalance` | Compare replicas by local merges, fetches, fetch %, bytes merged/fetched. | replica |
| Replication | `Replication#Replication Queue Summary` | Compare queue types by queued/executing/retry/error profile. | queue type |
| JSON monitoring | `JSON#JSON Columns Inventory` | Compare JSON columns by storage/path pressure. | database.table.column |
| Table inventory | `Overview#Table Health` or `Knowledge Base#Part Type by Table` | Compare tables by storage/parts/part-size shape. | table |
| Cloud/Grafana imports | `Grafana Imports#Heavy Readers at Time` | A full radar chart can be the drill target from read-rate spikes, showing all heavy readers at that timestamp. | query id |

Best first implementation candidates:

1. New `Resources#Server Pressure Radar` chart based on one averaged server-pressure row over the selected time range.
2. New `JSON#JSON Column Pressure Radar` drill target for a single selected JSON column.
3. New `Merges#Replica Merge Shape` chart for one selected replica or table.
4. New `Replication#Queue Pressure Radar` chart for one queue summary row.
5. Keep query-level pressure primarily as `@cell type=radar` in tables, where each query row owns its own shape.

Full radar charts should show multiple radar shapes together, not one large radar alone. The useful part is comparing silhouettes and colors.

Recommended behavior:

- one radar shape per result row;
- label each radar shape with `label`;
- group or facet by an optional dimension later, for example user, database, table, or node;
- tooltip shows raw metric columns and normalized values;
- click opens the relevant query/table/node detail view when available.

Example:

```sql
-- @chart: type=radar label=query_id values=pressure_values labels=pressure_labels color_by=pressure_score color_scale=pressure
SELECT
    substring(query_id, 1, 8) AS query_id,
    pressure_values,
    pressure_labels,
    pressure_score,
    query_duration_ms,
    memory_usage,
    read_bytes
FROM ...
ORDER BY query_duration_ms DESC
LIMIT 50
```

### When Not To Use Radars

Avoid radars when exact magnitude is the primary task.

Poor fits in current presets:

| Existing query | Better current visualization | Why |
| --- | --- | --- |
| `Advanced Dashboard#Queries/second` | area/line | Single time-series metric. |
| `Advanced Dashboard#CPU Usage (cores)` | area | Time trend matters more than shape. |
| `Advanced Dashboard#Selected Bytes/second` | area | Exact rate over time matters. |
| `Inserts#Insert Duration Quantiles (hourly)` | grouped line | Quantile trend over time is the point. |
| `Merges#Merge Events Over Time` | line/bar | Event rate trend, not row comparison. |
| `Memory#Memory Trend (MemoryTracking)` | area | Capacity/trend chart. |
| `Memory#Cache Trend` | grouped line | Cache behavior over time. |
| `Replication#Replication Lag Trend` | area | Time trend and threshold. |
| `Disks#Disk Free Space` | gauge/RAG | Current query has one main metric. |

Also avoid using radars as a replacement for:

- flamegraphs;
- query timelines;
- distributed topology;
- scan-efficiency panels;
- exact capacity planning charts.

In those cases use line charts, bars, tables, timelines, or the existing details views. Radar visualizations are best as a compact pattern-recognition layer.

## Query Monitor Example

This mirrors the current query-history radar idea with simple SQL. AQL owns normalization and color scoring through `profile=query_pressure`.

```sql
-- @cell: type=radar radar_column=shape profile=query_pressure axes=time:query_duration_ms,memory:memory_usage,cpu:cpu_ms,io:io_bytes,scan:scan_pressure ranges=time:100..60000,memory:32Mi..8Gi,cpu:100..60000,io:1Mi..10Gi,scan:0..1 color=profile_level
SELECT
    query_id,
    substring(query_id, 1, 8) AS short_id,
    query_kind,
    type,
    exception,
    query_duration_ms,
    memory_usage,
    ProfileEvents['OSCPUVirtualTimeMicroseconds'] / 1000 AS cpu_ms,
    greatest(
        read_bytes,
        ProfileEvents['ReadBufferFromFileDescriptorReadBytes'],
        ProfileEvents['NetworkReceiveBytes']
    ) AS io_bytes,
    if(
        ProfileEvents['SelectedMarksTotal'] > 0,
        ProfileEvents['SelectedMarks'] / ProfileEvents['SelectedMarksTotal'],
        if(
            ProfileEvents['SelectedPartsTotal'] > 0,
            ProfileEvents['SelectedParts'] / ProfileEvents['SelectedPartsTotal'],
            0
        )
    ) AS scan_pressure,
    read_rows,
    read_bytes,
    result_rows
FROM system.query_log
WHERE event_time >= now() - INTERVAL 1 HOUR
  AND type = 'QueryFinish'
ORDER BY event_time DESC
LIMIT 100
```

## With Explicit Pruning Data

If the query already has pruning counts, make the scan axis a ratio and use a linear transform.

```sql
-- @cell: type=radar radar_column=shape profile=query_pressure axes=time:query_duration_ms,memory:memory_usage,cpu:cpu_ms,io:io_bytes,scan:scan_pressure ranges=time:100..60000,memory:32Mi..8Gi,cpu:100..60000,io:1Mi..10Gi,scan:0..1 transforms=scan:linear color=profile_level
SELECT
    query_id,
    query_duration_ms,
    memory_usage,
    cpu_ms,
    io_bytes,
    greatest(
        ifNull(parts_read / nullIf(parts_total, 0), 0),
        ifNull(marks_read / nullIf(marks_total, 0), 0)
    ) AS scan_pressure
FROM ...
```

## Full Radar Chart Example

```sql
-- @chart: type=radar label=short_id profile=query_pressure axes=time:query_duration_ms,memory:memory_usage,cpu:cpu_ms,io:io_bytes,scan:scan_pressure ranges=time:100..60000,memory:32Mi..8Gi,cpu:100..60000,io:1Mi..10Gi,scan:0..1 color=profile_level
SELECT
    substring(query_id, 1, 8) AS short_id,
    query_duration_ms,
    memory_usage,
    ProfileEvents['OSCPUVirtualTimeMicroseconds'] / 1000 AS cpu_ms,
    greatest(
        read_bytes,
        ProfileEvents['ReadBufferFromFileDescriptorReadBytes'],
        ProfileEvents['NetworkReceiveBytes']
    ) AS io_bytes,
    if(
        ProfileEvents['SelectedMarksTotal'] > 0,
        ProfileEvents['SelectedMarks'] / ProfileEvents['SelectedMarksTotal'],
        if(
            ProfileEvents['SelectedPartsTotal'] > 0,
            ProfileEvents['SelectedParts'] / ProfileEvents['SelectedPartsTotal'],
            0
        )
    ) AS scan_pressure,
    read_rows,
    read_bytes
FROM system.query_log
WHERE event_time >= now() - INTERVAL 1 HOUR
  AND type = 'QueryFinish'
ORDER BY query_duration_ms DESC
LIMIT 50
```

## Export Implication

For Grafana panel export, the exported query should preserve:

- the `@cell` or `@chart` directive;
- the `radar_column`, `axes`, `ranges`, profile, and color configuration for AQL-created radar visualizations;
- the `values`, `labels`, and `color_by` result columns for SQL-created radar values;
- any raw metric columns needed for tooltip/details.

The exported panel should not depend on TraceHouse-only query-history heuristics.

## Review Notes: Profiles and the Normalization Mechanism

This section captures a review discussion of the proposal above. It is not yet agreed; it is here to be reviewed alongside the rest.

### 1. Main design correction: simple SQL first

The first version of this proposal made explicit SQL-created `values` the primary interface. That is too much work for the common cases. Most useful radar visualizations in the app are a small set of raw result columns plus repeated normalization behavior:

- query cost: duration, memory, CPU, I/O, scan;
- JSON pressure: bytes, path pressure, subcolumns, parts, rows;
- replication queue pressure: queued, executing, retries, max tries, errors;
- merge pressure: elapsed, memory, rows, read bytes, written bytes.

The better primary interface is:

```sql
-- @cell: type=radar radar_column=shape profile=query_pressure axes=time:query_duration_ms,memory:memory_usage,cpu:cpu_ms,io:io_bytes,scan:scan_pressure ranges=time:100..60000,memory:32Mi..8Gi,cpu:100..60000,io:1Mi..10Gi,scan:0..1 color=profile_level
SELECT
    query_id,
    query_duration_ms,
    memory_usage,
    cpu_ms,
    io_bytes,
    scan_pressure
FROM ...
```

Here `shape` is a display-only AQL column. SQL does not return it. AQL builds it from `axes`, `ranges`, and profile/default transforms.

The explicit SQL-created form remains important, but it should be the escape hatch:

```sql
-- @cell: column=pressure_values type=radar labels=pressure_labels color_by=pressure_score
SELECT
    [time_score, memory_score, cpu_score] AS pressure_values,
    ['time', 'memory', 'cpu'] AS pressure_labels,
    greatest(time_score, memory_score, cpu_score) AS pressure_score
FROM ...
```

### 2. `column` versus `radar_column`

`column=` and `radar_column=` must have distinct meanings:

- `column=pressure_values` means `pressure_values` is returned by SQL and should be rendered as a radar shape.
- `radar_column=shape` means AQL creates a displayed column named `shape`; SQL returns only the axis source columns.

Parser rule:

- For `type=radar`, require exactly one of `column=` or `radar_column=`.
- For `column=`, require `values` to come from that SQL result column.
- For `radar_column=`, require `axes=` and `ranges=`.
- Directive key order must not matter. Both `type=radar radar_column=shape` and `radar_column=shape type=radar` are valid.
- Existing non-radar cell directives continue to require `column=`.

This is not supported by the current parser yet. Today `parseCellStyleLine()` exits if `column=` is absent and only recognizes `rag`, `gauge`, and `sparkline`.

### 3. Ranges and transforms

Use `ranges`, not `anchors`, in user-facing AQL. A range defines the raw input values that map to the low and high ends of a normalized axis:

```sql
ranges=duration:100..60000,memory:32Mi..8Gi,scan:0..1
```

Meaning:

- `duration <= 100` maps to `0`.
- `duration >= 60000` maps to `1`.
- Values between those endpoints are interpolated by the axis transform.

The transform is separate:

```sql
transforms=duration:log,memory:log,scan:linear
```

If a transform is omitted, the profile default applies. For `profile=query_pressure`, likely defaults are:

| Axis | Default transform | Why |
| --- | --- | --- |
| `time` | `log` | query durations are heavy-tailed |
| `memory` | `log` | memory usage is heavy-tailed |
| `cpu` | `log` | CPU time is heavy-tailed |
| `io` | `log` | byte counts are heavy-tailed |
| `scan` | `linear` when already a ratio, `log` when using read rows/marks | depends on the source column |

Per-axis overrides affect only the named axis. `transforms=scan:linear` does not make every axis linear.

### 4. Full radar charts

`@chart: type=radar` uses the same axis/range/profile semantics, but it does not need `radar_column` because there is no table column to create.

```sql
-- @chart: type=radar label=query_id profile=query_pressure axes=time:query_duration_ms,memory:memory_usage,cpu:cpu_ms,io:io_bytes,scan:scan_pressure ranges=time:100..60000,memory:32Mi..8Gi,cpu:100..60000,io:1Mi..10Gi,scan:0..1 color=profile_level
SELECT
    substring(query_id, 1, 8) AS query_id,
    query_duration_ms,
    memory_usage,
    cpu_ms,
    io_bytes,
    scan_pressure
FROM ...
LIMIT 50
```

For SQL-created values, the existing chart syntax remains:

```sql
-- @chart: type=radar label=query_id values=pressure_values labels=pressure_labels color_by=pressure_score
```

### 5. Current query preview target model

The current Query tab hover preview is the clearest target for the first built-in profile. It is implemented in:

- `frontend/src/utils/queryHoverMetrics.ts`
- `frontend/src/components/query/QueryHoverPreview.tsx`
- `frontend/src/components/query/QueryHistoryTable.tsx`

Current behavior:

- axes are `time`, `memory`, `cpu`, `io`, `scan`;
- `time`, `memory`, `cpu`, and `io` use log normalization;
- `scan` is inverse pruning pressure: no pruning/full scan is high pressure, strong pruning is low pressure;
- I/O uses `max(read_bytes, disk_read_bytes, network_receive_bytes)`;
- color is not a plain `axis_max`; it is a profile-level classifier:
  - errors are high;
  - inserts are moderate;
  - two or more elevated resource axes are high;
  - any resource axis at `0.9` or above is high;
  - scan at `0.9` plus another elevated resource is high;
  - one elevated resource, resource max at `0.45` or above, or scan at `0.75` or above is moderate;
  - otherwise low.

That maps to this profile:

```sql
-- @cell: type=radar radar_column=shape profile=query_pressure axes=time:query_duration_ms,memory:memory_usage,cpu:cpu_ms,io:io_bytes,scan:scan_pressure ranges=time:100..60000,memory:32Mi..8Gi,cpu:100..60000,io:1Mi..10Gi,scan:0..1 color=profile_level
SELECT
    query_id,
    query_kind,
    type,
    exception,
    query_duration_ms,
    memory_usage,
    ProfileEvents['OSCPUVirtualTimeMicroseconds'] / 1000 AS cpu_ms,
    greatest(
        read_bytes,
        ProfileEvents['ReadBufferFromFileDescriptorReadBytes'],
        ProfileEvents['NetworkReceiveBytes']
    ) AS io_bytes,
    if(
        ProfileEvents['SelectedMarksTotal'] > 0,
        ProfileEvents['SelectedMarks'] / ProfileEvents['SelectedMarksTotal'],
        if(
            ProfileEvents['SelectedPartsTotal'] > 0,
            ProfileEvents['SelectedParts'] / ProfileEvents['SelectedPartsTotal'],
            0
        )
    ) AS scan_pressure,
    read_rows,
    read_bytes,
    result_rows
FROM system.query_log
WHERE type = 'QueryFinish'
LIMIT 100
```

This is close to today's behavior. Exact parity requires either:

- exposing the existing combined pruning score as a SQL result column; or
- teaching the profile how to combine selected parts and selected marks the same way `calculatePruning()` does.

### 6. Export implication

For Grafana export there are two possible strategies:

- Keep the query simple and export the radar directive only when the target renderer understands TraceHouse AQL.
- Expand `radar_column` profiles into SQL-created `values`, `labels`, and `color_by` columns during export.

The second strategy is more portable. It means profiles are authoring sugar, not a runtime dependency. Export can target an inline SQL expression using `arrayMap` or repeated per-axis expressions, but that is an export implementation detail rather than the primary authoring model.

### 7. Grafana first cut and follow-up

The first Grafana export cut should treat `@cell type=radar` as **partially supported**:

- export a compact generated SVG image badge in a table cell;
- keep the badge unlabeled so it fills Grafana's small image-cell thumbnail;
- use a larger table cell height when radar badges are present;
- hide the raw axis source columns when they only exist to build the badge.

This is useful as a row-scanning hint, but it is not the final radar UX. Grafana table image cells have hard practical limits:

- the rendered image is thumbnail-sized even when the SVG has a larger intrinsic size;
- row-specific hover/tooltips are not available from a hidden field in the native image cell;
- embedding axis labels inside the table-cell SVG makes the actual shape smaller and less readable.

The follow-up target is full `@chart: type=radar` export. That should create a proper Grafana panel-sized radar visualization rather than a table image cell. The panel should support readable axis labels, per-row labels, useful hover/details, and theme-safe SVG rendering. Until that exists, Grafana export analysis should report radar as partial support:

- `@cell type=radar`: partial, compact SVG badge in a table image cell.
- `@chart type=radar`: partial/not complete, needs a dedicated full radar panel export path.

### Open questions for reviewers

- Should `ranges` be required for every axis, or can built-in profiles ship visible starting ranges?
- Should `profile=query_pressure` own the exact current color classifier, or should it use simpler `axis_max` for AQL dashboards?
- Should scan pressure be expressed as a raw ratio column (`scan_pressure`) or as source counters (`selected_marks`, `selected_marks_total`, etc.) that the profile combines?
- Should export expand radar visualizations into inline SQL arrays by default, or preserve the AQL directive when exporting to TraceHouse-aware targets?
- Should full Grafana `@chart type=radar` use a native/custom panel plugin, a text/HTML panel with generated SVG, or a different Grafana-supported visualization strategy?
