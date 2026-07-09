/**
 * X-Ray dashboard queries — per-second "shape" overlays for many queries at once,
 * on a wall-clock time axis.
 *
 * Each panel plots one line per query (series = query_id) against real time
 * (sample_time bucketed to the second), from tracehouse.processes_history, so a
 * query's shape sits where it actually ran. To stay bounded we rank queries in
 * system.query_log and keep the top 50 by the panel's metric.
 *
 * Database / table filtering goes through system.query_log (processes_history is
 * keyed by query_id only, so query_log's databases[] / tables[] arrays are the
 * link):
 *   {{drill_value:db}}  — a database  (Database filter → has(databases, db))
 *   {{drill_value:tbl}} — a db.table  (Table filter    → has(tables, tbl))
 *
 * Clicking a line (@query_link on=query_id) opens that query in the Query Detail
 * modal on the X-Ray tab.
 *
 * render=overlay draws the many series as a spaghetti overlay (thin translucent
 * lines, no legend, hover to highlight one, click to open its X-Ray).
 *
 * The windowed delta computation mirrors packages/core/src/queries/
 * process-queries.ts. A single replica is assumed; distributed queries sampled
 * on several nodes will interleave.
 */

/** WITH clause shared by every panel: rank the top-N queries by the metric, with db/table filters. */
const TOP_Q = (rankExpr: string) => `WITH top_q AS
(
    SELECT query_id
    FROM {{cluster_aware:system.query_log}}
    WHERE type = 'QueryFinish'
      AND event_time > {{time_range}}
      AND ({{drill_value:db  | ''}} = '' OR has(databases, {{drill_value:db  | ''}}))
      AND ({{drill_value:tbl | ''}} = '' OR has(tables,    {{drill_value:tbl | ''}}))
    ORDER BY ${rankExpr} DESC
    LIMIT 50
)`;

/** Restrict process samples to the ranked top-N queries. */
const IN_TOP_Q = `WHERE query_id IN (SELECT query_id FROM top_q)`;

const queries: string[] = [

  `-- @meta: title='Query CPU Cores' group='X-Ray' interval='1 HOUR' description='Per-second CPU cores per query over time — top 50 by CPU in range. Filter by database/table; click a line to open its X-Ray in query details.'
-- @chart: type=grouped_line group_by=t value=cpu_cores series=query_id style=2d render=overlay
-- @query_link: on=query_id
${TOP_Q("ProfileEvents['OSCPUVirtualTimeMicroseconds']")}
SELECT
    t,
    query_id,
    round(avg(cpu_cores), 2) AS cpu_cores
FROM
(
    SELECT
        query_id,
        toStartOfInterval(sample_time, INTERVAL 1 SECOND) AS t,
        greatest((pe_cpu - lagInFrame(pe_cpu) OVER w) / 1e6 / dt, 0) AS cpu_cores
    FROM
    (
        SELECT
            query_id,
            sample_time,
            ProfileEvents['OSCPUVirtualTimeMicroseconds'] AS pe_cpu,
            greatest(dateDiff('millisecond', lagInFrame(sample_time) OVER w0, sample_time) / 1000, 0.1) AS dt
        FROM {{cluster_aware:tracehouse.processes_history}}
        ${IN_TOP_Q}
        WINDOW w0 AS (PARTITION BY query_id ORDER BY sample_time)
    )
    WINDOW w AS (PARTITION BY query_id ORDER BY sample_time)
)
GROUP BY query_id, t
ORDER BY t, query_id`,

  `-- @meta: title='Query Memory' group='X-Ray' interval='1 HOUR' description='Per-second memory (MB) per query over time — top 50 by peak memory in range. Filter by database/table; click a line to open its X-Ray in query details.'
-- @chart: type=grouped_line group_by=t value=mem_mb series=query_id style=2d render=overlay
-- @query_link: on=query_id
${TOP_Q('memory_usage')}
SELECT
    t,
    query_id,
    round(avg(memory_usage) / 1048576, 1) AS mem_mb
FROM
(
    SELECT
        query_id,
        memory_usage,
        toStartOfInterval(sample_time, INTERVAL 1 SECOND) AS t
    FROM {{cluster_aware:tracehouse.processes_history}}
    ${IN_TOP_Q}
)
GROUP BY query_id, t
ORDER BY t, query_id`,

  `-- @meta: title='Query read_bytes' group='X-Ray' interval='1 HOUR' description='Per-second read throughput (MB/s) per query over time — top 50 by read_bytes in range. Filter by database/table; click a line to open its X-Ray in query details.'
-- @chart: type=grouped_line group_by=t value=read_mb_s series=query_id style=2d render=overlay
-- @query_link: on=query_id
${TOP_Q('read_bytes')}
SELECT
    t,
    query_id,
    round(avg(read_mb_s), 1) AS read_mb_s
FROM
(
    SELECT
        query_id,
        toStartOfInterval(sample_time, INTERVAL 1 SECOND) AS t,
        greatest((read_bytes - lagInFrame(read_bytes) OVER w) / 1048576 / dt, 0) AS read_mb_s
    FROM
    (
        SELECT
            query_id,
            sample_time,
            read_bytes,
            greatest(dateDiff('millisecond', lagInFrame(sample_time) OVER w0, sample_time) / 1000, 0.1) AS dt
        FROM {{cluster_aware:tracehouse.processes_history}}
        ${IN_TOP_Q}
        WINDOW w0 AS (PARTITION BY query_id ORDER BY sample_time)
    )
    WINDOW w AS (PARTITION BY query_id ORDER BY sample_time)
)
GROUP BY query_id, t
ORDER BY t, query_id`,

  `-- @meta: title='Query I/O Wait' group='X-Ray' interval='1 HOUR' description='Per-second I/O wait (s) per query over time — top 50 by I/O wait in range. Filter by database/table; click a line to open its X-Ray in query details.'
-- @chart: type=grouped_line group_by=t value=io_wait_s series=query_id style=2d render=overlay
-- @query_link: on=query_id
${TOP_Q("ProfileEvents['OSCPUWaitMicroseconds']")}
SELECT
    t,
    query_id,
    round(avg(io_wait_s), 3) AS io_wait_s
FROM
(
    SELECT
        query_id,
        toStartOfInterval(sample_time, INTERVAL 1 SECOND) AS t,
        greatest((pe_io - lagInFrame(pe_io) OVER w) / 1e6 / dt, 0) AS io_wait_s
    FROM
    (
        SELECT
            query_id,
            sample_time,
            ProfileEvents['OSCPUWaitMicroseconds'] AS pe_io,
            greatest(dateDiff('millisecond', lagInFrame(sample_time) OVER w0, sample_time) / 1000, 0.1) AS dt
        FROM {{cluster_aware:tracehouse.processes_history}}
        ${IN_TOP_Q}
        WINDOW w0 AS (PARTITION BY query_id ORDER BY sample_time)
    )
    WINDOW w AS (PARTITION BY query_id ORDER BY sample_time)
)
GROUP BY query_id, t
ORDER BY t, query_id`,

];

export default queries;
