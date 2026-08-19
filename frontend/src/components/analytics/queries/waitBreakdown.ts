/**
 * Wait Breakdown — where a whole workload's time goes, not just one query's.
 *
 * The query modal answers this per query, but you have to suspect a query
 * before you open it. These panels are the view you reach for without a
 * suspect: which query shapes waste the most time waiting, and whether the
 * cluster's character is changing.
 *
 * Every panel obeys the same rule as the per-query bar. ProfileEvents are
 * summed across threads, so on a parallel query they run to several times the
 * elapsed time — usable as *shares*, never as durations on a wall-clock axis.
 * The denominator throughout is RealTimeMicroseconds.
 *
 * Segment sources (matching packages/core/src/utils/time-breakdown.ts):
 *   CPU      OSCPUVirtualTimeMicroseconds
 *   Queue    OSCPUWaitMicroseconds     — runnable, no free CPU. On a cgroup
 *                                        with a CPU quota this is dominated by
 *                                        CFS throttling, not query contention.
 *   Disk     OSIOWaitMicroseconds      — reads 0 without procfs/taskstats
 *   Network  Network{Receive,Send}ElapsedMicroseconds
 *   Parked   the remainder — unmetered waiting: pipeline threads starved on
 *            ports, coordinators blocked on async shard reads, locks
 */

/** Shared segment arithmetic, so every panel decomposes time identically. */
const SEGMENTS = `
    sum(ProfileEvents['OSCPUVirtualTimeMicroseconds']) AS cpu_us,
    sum(ProfileEvents['OSCPUWaitMicroseconds']) AS queue_us,
    sum(ProfileEvents['OSIOWaitMicroseconds']) AS disk_us,
    sum(ProfileEvents['NetworkReceiveElapsedMicroseconds'] + ProfileEvents['NetworkSendElapsedMicroseconds']) AS network_us,
    sum(ProfileEvents['RealTimeMicroseconds']) AS real_us,
    greatest(real_us - cpu_us - queue_us - disk_us - network_us, 0) AS parked_us`;

/**
 * Same decomposition for a single execution — no aggregation, because this
 * panel ranks individual queries rather than grouping them.
 */
const SEGMENTS_SINGLE = `
    ProfileEvents['OSCPUVirtualTimeMicroseconds'] AS cpu_us,
    ProfileEvents['OSCPUWaitMicroseconds'] AS queue_us,
    ProfileEvents['OSIOWaitMicroseconds'] AS disk_us,
    ProfileEvents['NetworkReceiveElapsedMicroseconds'] + ProfileEvents['NetworkSendElapsedMicroseconds'] AS network_us,
    ProfileEvents['RealTimeMicroseconds'] AS real_us,
    greatest(real_us - cpu_us - queue_us - disk_us - network_us, 0) AS parked_us`;

const queries: string[] = [

  `-- @meta: title='Where Query Time Goes' group='Wait Breakdown' interval='6 HOUR' description='Top 15 query shapes by total thread time, each split into work and waiting. Shares are of that shape\\'s own RealTimeMicroseconds, so every bar sums to 100% — a wide bar is not a busy shape, it is one that spends its time differently. ProfileEvents are summed across threads, so these are shares of thread time, never elapsed time. Parked is the remainder ClickHouse never meters: pipeline threads starved on ports, coordinators blocked on async shard reads, locks.'
-- @chart: type=stacked_bar group_by=query_shape value=share series=segment style=2d
-- @drill: on=query_shape into='Wait Breakdown#Worst Waiting Queries'
-- @source: https://altinity.com/blog/pipeline-optimization-for-clickhouse-distributed-tables-with-synchronous-inserts
WITH shapes AS (
  SELECT
    substring(normalizeQuery(query), 1, 60) AS query_shape,
    ${SEGMENTS}
  FROM {{cluster_aware:system.query_log}}
  WHERE type = 'QueryFinish'
    AND event_time > {{time_range}}
    AND ProfileEvents['RealTimeMicroseconds'] > 0
    AND ({{drill_value:db  | ''}} = '' OR has(databases, {{drill_value:db  | ''}}))
    AND ({{drill_value:tbl | ''}} = '' OR has(tables,    {{drill_value:tbl | ''}}))
  GROUP BY query_shape
  ORDER BY real_us DESC
  LIMIT 15
)
SELECT query_shape, segment, round(100 * value / greatest(real_us, 1), 1) AS share
FROM shapes
ARRAY JOIN
  ['cpu', 'queue', 'disk', 'network', 'parked'] AS segment,
  [cpu_us, queue_us, disk_us, network_us, parked_us] AS value
WHERE value > 0
ORDER BY query_shape, segment`,

  `-- @meta: title='Wait Composition Over Time' group='Wait Breakdown' interval='6 HOUR' description='How the whole cluster spends thread time, per minute — every query, not a top-N. A rising queue band means CPU contention or cgroup throttling arriving; a rising parked band means more waiting on shards or starved pipelines. Shares of RealTimeMicroseconds.'
-- @chart: type=stacked_bar group_by=t value=share series=segment orientation=v style=2d
WITH buckets AS (
  SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    ${SEGMENTS}
  FROM {{cluster_aware:system.query_log}}
  WHERE type = 'QueryFinish'
    AND event_time > {{time_range}}
    AND ProfileEvents['RealTimeMicroseconds'] > 0
    AND ({{drill_value:db  | ''}} = '' OR has(databases, {{drill_value:db  | ''}}))
    AND ({{drill_value:tbl | ''}} = '' OR has(tables,    {{drill_value:tbl | ''}}))
  GROUP BY t
)
SELECT t, segment, round(100 * value / greatest(real_us, 1), 1) AS share
FROM buckets
ARRAY JOIN
  ['cpu', 'queue', 'disk', 'network', 'parked'] AS segment,
  [cpu_us, queue_us, disk_us, network_us, parked_us] AS value
WHERE value > 0
ORDER BY t ASC, segment`,

  `-- @meta: title='Waiting by User' group='Wait Breakdown' interval='6 HOUR' description='Top 12 users by total thread time, each split into work and waiting. Attributes contention to a tenant. Shares of each user\\'s own RealTimeMicroseconds, so every bar sums to 100%.'
-- @chart: type=stacked_bar group_by=user value=share series=segment style=2d
WITH per_user AS (
  SELECT
    user,
    ${SEGMENTS}
  FROM {{cluster_aware:system.query_log}}
  WHERE type = 'QueryFinish'
    AND event_time > {{time_range}}
    AND ProfileEvents['RealTimeMicroseconds'] > 0
  GROUP BY user
  ORDER BY real_us DESC
  LIMIT 12
)
SELECT user, segment, round(100 * value / greatest(real_us, 1), 1) AS share
FROM per_user
ARRAY JOIN
  ['cpu', 'queue', 'disk', 'network', 'parked'] AS segment,
  [cpu_us, queue_us, disk_us, network_us, parked_us] AS value
WHERE value > 0
ORDER BY user, segment`,

  `-- @meta: title='Fixed Cost vs Saturation' group='Wait Breakdown' interval='6 HOUR' description='Median duration against rows read, for the top 6 query shapes by total time — only shapes seen at 3 or more different input sizes, since a single size cannot show a trend. A rising line means the query is saturating: more rows cost more time. A flat line means a fixed per-query cost no amount of tuning the data will shift — round-trips, coordination, or scheduling. Buckets are powers of two.'
-- @chart: type=grouped_line group_by=rows_bucket value=median_ms series=query_shape style=2d render=overlay
-- @source: https://altinity.com/blog/pipeline-optimization-for-clickhouse-distributed-tables-with-synchronous-inserts
WITH top_shapes AS (
  SELECT substring(normalizeQuery(query), 1, 40) AS query_shape
  FROM {{cluster_aware:system.query_log}}
  WHERE type = 'QueryFinish'
    AND event_time > {{time_range}}
    AND read_rows > 0
    AND ({{drill_value:db  | ''}} = '' OR has(databases, {{drill_value:db  | ''}}))
  GROUP BY query_shape
  -- Needs several distinct input sizes, or the line is a single point and the
  -- panel says nothing.
  HAVING count() >= 5 AND uniq(pow(2, floor(log2(greatest(read_rows, 1))))) >= 3
  ORDER BY sum(query_duration_ms) DESC
  LIMIT 6
)
SELECT
  substring(normalizeQuery(query), 1, 40) AS query_shape,
  -- Log-spaced, but base 2: powers of 10 collapsed a whole workload into a
  -- single bucket on real data, leaving nothing to slope. A fixed cost only
  -- shows as flat against a range of input sizes, so the buckets have to be
  -- fine enough to produce one.
  pow(2, floor(log2(greatest(read_rows, 1)))) AS rows_bucket,
  round(median(query_duration_ms), 1) AS median_ms,
  count() AS runs
FROM {{cluster_aware:system.query_log}}
WHERE type = 'QueryFinish'
  AND event_time > {{time_range}}
  AND read_rows > 0
  AND query_shape IN (SELECT query_shape FROM top_shapes)
GROUP BY query_shape, rows_bucket
-- Ordered by bucket first, not by shape: the chart takes its category sequence
-- from the order rows arrive, so sorting within each shape produced a
-- non-monotonic x-axis (… 65536, 8388608, 131072, 32 …) and meaningless curves.
ORDER BY rows_bucket ASC, query_shape`,

  `-- @meta: title='Worst Waiting Queries' group='Wait Breakdown' interval='6 HOUR' description='Top 20 individual executions ranked by thread time spent NOT working (queue + disk + network + parked), so this lists queries whose cost is waiting rather than queries that merely ran longest. Client-submitted queries over 100ms — shard children are excluded since they cannot be opened directly. Drilling a shard-child shape lists the coordinators that produced it, so open one and use its Distributed tab for the per-node split. Bars are absolute thread-milliseconds, so lengths are comparable between queries — unlike the proportional panels, a longer bar here really did waste more time. Click a bar to open that execution, or click a shape in 'Where Query Time Goes' to filter to that shape.'
-- @chart: type=stacked_bar group_by=query_id value=ms series=segment style=2d unit=ms
-- @query_link: on=query_id
WITH worst AS (
  SELECT
    query_id,
    ${SEGMENTS_SINGLE}
  FROM {{cluster_aware:system.query_log}}
  WHERE type = 'QueryFinish'
    -- Coordinators only. Shard children carry is_initial_query = 0 and cannot
    -- be opened — the modal's lookup fetches initial queries only — so listing
    -- them gives rows whose click silently does nothing. Their time is not
    -- lost: open the parent and the Distributed tab breaks it down per node.
    AND is_initial_query = 1
    AND event_time > {{time_range}}
    AND ProfileEvents['RealTimeMicroseconds'] > 0
    AND query_duration_ms > 100
    AND ({{drill_value:db  | ''}} = '' OR has(databases, {{drill_value:db  | ''}}))
    AND ({{drill_value:tbl | ''}} = '' OR has(tables,    {{drill_value:tbl | ''}}))
    -- Set when drilled in from 'Where Query Time Goes'. The substring length
    -- must stay identical to that panel's or the filter matches nothing.
    --
    -- Two arms, because that panel ranks every query_log row while this one
    -- lists coordinators. A client-submitted shape matches on its own text. A
    -- shard-child shape — the __table1 rewrites ClickHouse sends to remote
    -- nodes — can never match it, since those rows carry is_initial_query = 0,
    -- so the second arm matches the coordinator that produced them instead.
    -- Without it, drilling any shard-child shape returned zero rows, which on a
    -- distributed workload is most of the panel.
    AND (
      {{drill_value:query_shape | ''}} = ''
      OR substring(normalizeQuery(query), 1, 60) = {{drill_value:query_shape | ''}}
      OR query_id IN (
        SELECT initial_query_id
        FROM {{cluster_aware:system.query_log}}
        WHERE type = 'QueryFinish'
          AND is_initial_query = 0
          -- Constant-false when nothing was drilled, so the planner drops this
          -- subquery instead of scanning query_log a second time.
          AND {{drill_value:query_shape | ''}} != ''
          AND event_time > {{time_range}}
          AND substring(normalizeQuery(query), 1, 60) = {{drill_value:query_shape | ''}}
      )
    )
  -- Ranked by time spent not working, so the list is the queries whose cost is
  -- waiting rather than the queries that merely ran longest.
  ORDER BY (queue_us + disk_us + network_us + parked_us) DESC
  LIMIT 20
)
-- Absolute thread-milliseconds, not shares. This panel ranks by time wasted,
-- so the bars have to be comparable between queries: a fast query at 72% wait
-- must not outrank a slow one at 40%. 'Where Query Time Goes' is the
-- proportional view; this is the magnitude one.
SELECT query_id, segment, round(value / 1000) AS ms
FROM worst
ARRAY JOIN
  ['cpu', 'queue', 'disk', 'network', 'parked'] AS segment,
  [cpu_us, queue_us, disk_us, network_us, parked_us] AS value
WHERE value > 0
ORDER BY query_id, segment`,

];

export default queries;
