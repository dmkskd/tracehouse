/**
 * Workload Breakdown queries - the "precise" counterpart to Time Travel.
 *
 * Time Travel is a fast, sampled first glance: it caps each activity type at
 * the top 100 rows and drops anything under 1 MB of memory. These queries do
 * full accounting instead - no LIMIT, no memory floor - so the totals add up.
 *
 * Resource cost is attributed to five categories:
 *   Query · Select / Query · Insert / Query · Other  (system.query_log)
 *   Merge / Mutation                                  (system.part_log)
 *
 * Completed work only (QueryFinish, finished part_log rows), which is exactly
 * the after-the-fact precision role. In-flight estimation stays in Time Travel.
 */

import { APP_SOURCE_LIKE } from '@tracehouse/core';

/**
 * Adaptive bucket width in seconds: the selected window divided into ~24 slots.
 *
 * The stacked-bar renderer caps a non-time x-axis at 30 groups, so a fixed
 * 1-minute bucket over a 6h/1d window would emit hundreds of buckets and the
 * cap would silently drop the most recent ones. Keeping the bucket count fixed
 * (~24) means the full range is always shown, at any zoom level.
 *
 * `{{time_range}}` resolves to `now() - INTERVAL <selected>`, so this yields the
 * selected window length in seconds.
 */
const BUCKET_SEC = `greatest(1, intDiv(dateDiff('second', {{time_range}}, now()), 24))`;

/**
 * Coarser bucket (~10 slots) for the per-server clustered-stacked panel: with
 * N servers × M kinds of sub-bars per bucket, fewer buckets keeps it legible.
 */
const SERVER_BUCKET_SEC = `greatest(1, intDiv(dateDiff('second', {{time_range}}, now()), 10))`;

/**
 * Optional per-server scope, driven by the dashboard's "Server" filter dropdown.
 *
 * When a server is picked, `{{drill_value:host}}` becomes `'node'` and this
 * narrows to that node. With no selection the fallback `hostname()` makes it
 * `hostname() = hostname()` - a tautology, so all nodes are included. The host
 * list and this filter both key off `hostname()` so they always agree (same
 * approach the Time Travel page uses).
 */
const HOST_FILTER = `AND hostname() = {{drill_value:host | hostname()}}`;

/** How query_log / part_log rows are split into the stacked series. */
const SPLIT_BY_CATEGORY = {
  seriesQuery: `multiIf(
        query_kind = 'Select', 'Query · Select',
        query_kind = 'Insert', 'Query · Insert',
        'Query · Other'
      )`,
  seriesPart: `if(event_type = 'MutatePart', 'Mutation', 'Merge')`,
  seriesAlias: 'category',
};
const buildComposite = (opts: {
  metricQuery: string;
  metricPart: string;
  /** Charted value built from `sum(cost)` and `${BUCKET_SEC}` (a scalar). */
  valueExpr: string;
  valueAlias: string;
}) => {
  const bucketExpr = `toStartOfInterval(event_time, toIntervalSecond(${BUCKET_SEC}))`;
  const split = SPLIT_BY_CATEGORY;
  return `SELECT
    toString(t) AS t,
    ${split.seriesAlias},
    ${opts.valueExpr} AS ${opts.valueAlias}
  FROM (
    SELECT
      ${bucketExpr} AS t,
      ${split.seriesQuery} AS ${split.seriesAlias},
      ${opts.metricQuery} AS cost
    FROM {{cluster_aware:system.query_log}}
    WHERE type = 'QueryFinish'
      AND event_date >= toDate({{time_range}})
      AND event_time > {{time_range}}
      AND query NOT LIKE ${APP_SOURCE_LIKE}
      ${HOST_FILTER}
    UNION ALL
    SELECT
      ${bucketExpr} AS t,
      ${split.seriesPart} AS ${split.seriesAlias},
      ${opts.metricPart} AS cost
    FROM {{cluster_aware:system.part_log}}
    WHERE event_type IN ('MergeParts', 'MutatePart')
      AND event_date >= toDate({{time_range}})
      AND event_time > {{time_range}}
      ${HOST_FILTER}
  )
  GROUP BY t, ${split.seriesAlias}
  ORDER BY t ASC`;
};

const CPU_COST_Q = `ProfileEvents['OSCPUVirtualTimeMicroseconds']`;
const CPU_COST_P = `ProfileEvents['OSCPUVirtualTimeMicroseconds']`;
const MEM_COST_Q = `memory_usage`;
const MEM_COST_P = `peak_memory_usage`;
const IO_COST_Q = `read_bytes + written_bytes`;
const IO_COST_P = `ProfileEvents['OSReadBytes'] + ProfileEvents['OSWriteBytes']`;
const NET_COST_Q = `ProfileEvents['NetworkSendBytes'] + ProfileEvents['NetworkReceiveBytes']`;
const NET_COST_P = `ProfileEvents['NetworkSendBytes'] + ProfileEvents['NetworkReceiveBytes']`;

const queries: string[] = [
  // ── Time series: CPU composition ──
  `-- @meta: title='CPU by Workload (cores)' group='Workload' interval='1 HOUR' description='Average CPU cores in use over each time bucket (core-seconds ÷ bucket length), stacked by queries / merges / mutations.'
-- @chart: type=stacked_bar group_by=t value=cpu_cores series=category orientation=v style=2d
${buildComposite({ metricQuery: CPU_COST_Q, metricPart: CPU_COST_P, valueExpr: `sum(cost) / 1e6 / ${BUCKET_SEC}`, valueAlias: 'cpu_cores' })}`,

  // ── Time series: peak per-op memory by kind (lines - footprints don't stack) ──
  `-- @meta: title='Peak Memory per Op by Kind (GB)' group='Workload' interval='1 HOUR' description='The largest single-operation peak memory each bucket, by kind - shows which kind runs memory-hungry operations and how that trends. A per-operation footprint, not total RAM.'
-- @chart: type=grouped_line group_by=t value=peak_gb series=category style=2d
${buildComposite({ metricQuery: MEM_COST_Q, metricPart: MEM_COST_P, valueExpr: 'max(cost) / 1073741824', valueAlias: 'peak_gb' })}`,

  // ── Time series: disk I/O composition ──
  `-- @meta: title='Disk I/O by Workload (MB/s)' group='Workload' interval='1 HOUR' description='Throughput: bytes read + written ÷ bucket length (MB/s), stacked by category.'
-- @chart: type=stacked_bar group_by=t value=io_mb_s series=category orientation=v style=2d
${buildComposite({ metricQuery: IO_COST_Q, metricPart: IO_COST_P, valueExpr: `sum(cost) / ${BUCKET_SEC} / 1048576`, valueAlias: 'io_mb_s' })}`,

  // ── Time series: network composition ──
  `-- @meta: title='Network by Workload (MB/s)' group='Workload' interval='1 HOUR' description='Throughput: network bytes sent + received ÷ bucket length (MB/s), stacked by category.'
-- @chart: type=stacked_bar group_by=t value=net_mb_s series=category orientation=v style=2d
${buildComposite({ metricQuery: NET_COST_Q, metricPart: NET_COST_P, valueExpr: `sum(cost) / ${BUCKET_SEC} / 1048576`, valueAlias: 'net_mb_s' })}`,

  // ── Per server: for each time bucket, one bar per server, split by kind ──
  `-- @meta: title='CPU by Server (cores)' group='Workload' interval='1 HOUR' description='One bar per server each bucket, split by activity kind - shows how evenly CPU load is spread across nodes.'
-- @chart: type=grouped_stacked_bar group_by=t value=cpu_cores cluster=host series=kind style=2d
SELECT
    toString(t) AS t,
    host,
    kind,
    sum(cost) / 1e6 / ${SERVER_BUCKET_SEC} AS cpu_cores
FROM (
    SELECT
      toStartOfInterval(event_time, toIntervalSecond(${SERVER_BUCKET_SEC})) AS t,
      hostname() AS host,
      ${SPLIT_BY_CATEGORY.seriesQuery} AS kind,
      ${CPU_COST_Q} AS cost
    FROM {{cluster_aware:system.query_log}}
    WHERE type = 'QueryFinish'
      AND event_date >= toDate({{time_range}})
      AND event_time > {{time_range}}
      AND query NOT LIKE ${APP_SOURCE_LIKE}
      ${HOST_FILTER}
    UNION ALL
    SELECT
      toStartOfInterval(event_time, toIntervalSecond(${SERVER_BUCKET_SEC})) AS t,
      hostname() AS host,
      ${SPLIT_BY_CATEGORY.seriesPart} AS kind,
      ${CPU_COST_P} AS cost
    FROM {{cluster_aware:system.part_log}}
    WHERE event_type IN ('MergeParts', 'MutatePart')
      AND event_date >= toDate({{time_range}})
      AND event_time > {{time_range}}
      ${HOST_FILTER}
  )
GROUP BY t, host, kind
ORDER BY t ASC`,

  // ── Totals: CPU split as a single bar per category ──
  `-- @meta: title='CPU Split (% of workload)' group='Workload' interval='1 HOUR' description='Each category as a share of total CPU consumed over the selected time range. Tooltip shows the average cores in use across that range.'
-- @chart: type=bar group_by=category value=cpu_pct unit=% description=detail style=2d
SELECT
    category,
    round(100 * sum(cost) / nullIf(sum(sum(cost)) OVER (), 0), 1) AS cpu_pct,
    concat(
        toString(round(sum(cost) / 1e6 / greatest(dateDiff('second', {{time_range}}, now()), 1), 2)),
        ' avg cores · ',
        toString(round(sum(cost) / 1e6)),
        ' core-s total'
    ) AS detail
FROM (
    SELECT
      multiIf(
        query_kind = 'Select', 'Query · Select',
        query_kind = 'Insert', 'Query · Insert',
        'Query · Other'
      ) AS category,
      ${CPU_COST_Q} AS cost
    FROM {{cluster_aware:system.query_log}}
    WHERE type = 'QueryFinish'
      AND event_date >= toDate({{time_range}})
      AND event_time > {{time_range}}
      AND query NOT LIKE ${APP_SOURCE_LIKE}
      ${HOST_FILTER}
    UNION ALL
    SELECT
      if(event_type = 'MutatePart', 'Mutation', 'Merge') AS category,
      ${CPU_COST_P} AS cost
    FROM {{cluster_aware:system.part_log}}
    WHERE event_type IN ('MergeParts', 'MutatePart')
      AND event_date >= toDate({{time_range}})
      AND event_time > {{time_range}}
      ${HOST_FILTER}
)
GROUP BY category
ORDER BY cpu_pct DESC`,

  // ── Top contributors: query patterns (drill into Query Detail) ──
  `-- @meta: title='Top Query Patterns by CPU' group='Workload' interval='1 HOUR' description='Heaviest normalized query shapes by total CPU. No 1MB floor, no top-100 cap - the small-but-many queries Time Travel hides show up here.'
-- @link: on=query_id into='Advanced Dashboard#Query Detail by ID'
SELECT
    lower(hex(normalized_query_hash)) AS query_hash,
    argMax(query_id, query_start_time) AS query_id,
    count() AS executions,
    round(sum(${CPU_COST_Q}) / 1e6, 1) AS cpu_core_sec,
    formatReadableSize(sum(memory_usage)) AS total_mem,
    any(substring(normalizeQuery(query), 1, 120)) AS sample_query
FROM {{cluster_aware:system.query_log}}
WHERE type = 'QueryFinish'
    AND event_date >= toDate({{time_range}})
    AND event_time > {{time_range}}
    AND query NOT LIKE ${APP_SOURCE_LIKE}
    ${HOST_FILTER}
GROUP BY normalized_query_hash
ORDER BY cpu_core_sec DESC
LIMIT 30`,

  // ── Top contributors: merge/mutation tables ──
  `-- @meta: title='Top Tables by Merge/Mutation CPU' group='Workload' interval='1 HOUR' description='Tables whose background merges and mutations cost the most CPU over the selected time range.'
SELECT
    database || '.' || table AS tbl,
    countIf(event_type = 'MergeParts') AS merges,
    countIf(event_type = 'MutatePart') AS mutations,
    round(sum(${CPU_COST_P}) / 1e6, 1) AS cpu_core_sec,
    formatReadableSize(sum(peak_memory_usage)) AS peak_mem
FROM {{cluster_aware:system.part_log}}
WHERE event_type IN ('MergeParts', 'MutatePart')
    AND event_date >= toDate({{time_range}})
    AND event_time > {{time_range}}
    ${HOST_FILTER}
GROUP BY tbl
ORDER BY cpu_core_sec DESC
LIMIT 30`,

  // ── Where memory goes: heaviest query shapes by peak memory ──
  `-- @meta: title='Top Query Patterns by Memory' group='Workload' interval='1 HOUR' description='Query shapes with the largest peak memory - where memory pressure comes from. Peak = the single heaviest execution; p95 = its high-end footprint. Drill into a query for detail.'
-- @link: on=query_id into='Advanced Dashboard#Query Detail by ID'
SELECT
    lower(hex(normalized_query_hash)) AS query_hash,
    argMax(query_id, memory_usage) AS query_id,
    count() AS executions,
    formatReadableSize(max(memory_usage)) AS peak_mem,
    formatReadableSize(quantile(0.95)(memory_usage)) AS p95_mem,
    any(substring(normalizeQuery(query), 1, 120)) AS sample_query
FROM {{cluster_aware:system.query_log}}
WHERE type = 'QueryFinish'
    AND event_date >= toDate({{time_range}})
    AND event_time > {{time_range}}
    AND query NOT LIKE ${APP_SOURCE_LIKE}
    ${HOST_FILTER}
GROUP BY normalized_query_hash
ORDER BY max(memory_usage) DESC
LIMIT 30`,

  // ── Where memory goes: heaviest merge/mutation tables by peak memory ──
  `-- @meta: title='Top Tables by Merge/Mutation Memory' group='Workload' interval='1 HOUR' description='Tables whose merges and mutations have the largest peak memory. Peak = the single heaviest operation; p95 = its high-end footprint.'
SELECT
    database || '.' || table AS tbl,
    countIf(event_type = 'MergeParts') AS merges,
    countIf(event_type = 'MutatePart') AS mutations,
    formatReadableSize(max(peak_memory_usage)) AS peak_mem,
    formatReadableSize(quantile(0.95)(peak_memory_usage)) AS p95_mem
FROM {{cluster_aware:system.part_log}}
WHERE event_type IN ('MergeParts', 'MutatePart')
    AND event_date >= toDate({{time_range}})
    AND event_time > {{time_range}}
    ${HOST_FILTER}
GROUP BY tbl
ORDER BY max(peak_memory_usage) DESC
LIMIT 30`,
];

export default queries;
