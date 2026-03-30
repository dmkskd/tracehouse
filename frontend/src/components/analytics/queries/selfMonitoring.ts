/** Self-monitoring queries — track the app's own query footprint on the server. */
import { APP_SOURCE_LIKE, APP_RE_COMPONENT, APP_RE_COMPONENT_SERVICE } from '@tracehouse/core';

const queries: string[] = [
  `-- @meta: title='App Query Duration by Component' group='Self-Monitoring' interval='1 DAY' description='Stacked percentile bands per component — bar height = p99, segments show p50 / p95−p50 / p99−p95'
-- @chart: type=stacked_bar group_by=component value=value_ms series=metric unit=ms style=2d
-- @drill: on=component into='App Query Duration by Service'
SELECT component, metric, value_ms
FROM (
  SELECT
      component,
      round(p50, 1) AS p50,
      round(p95 - p50, 1) AS p95_delta,
      round(p99 - p95, 1) AS p99_delta
  FROM (
    SELECT
        extractAllGroups(query, '${APP_RE_COMPONENT}')[1][1] AS component,
        quantile(0.5)(query_duration_ms) AS p50,
        quantile(0.95)(query_duration_ms) AS p95,
        quantile(0.99)(query_duration_ms) AS p99
    FROM {{cluster_aware:system.query_log}}
    WHERE type = 'QueryFinish'
      AND query LIKE ${APP_SOURCE_LIKE}
      AND event_time > {{time_range}}
    GROUP BY component
    HAVING component != ''
  )
)
ARRAY JOIN
  ['p50', 'p95', 'p99'] AS metric,
  [p50, p95_delta, p99_delta] AS value_ms
ORDER BY component, metric`,

  `-- @meta: title='App Query Duration by Service' group='Self-Monitoring' interval='1 DAY' description='Drill from component to individual services — stacked percentile bands per service'
-- @chart: type=stacked_bar group_by=service value=value_ms series=metric unit=ms style=2d
-- @drill: on=service into='App Query Cost Details'
SELECT service, metric, value_ms
FROM (
  SELECT
      component, service,
      round(p50, 1) AS p50,
      round(p95 - p50, 1) AS p95_delta,
      round(p99 - p95, 1) AS p99_delta
  FROM (
    SELECT
        extractAllGroups(query, '${APP_RE_COMPONENT_SERVICE}')[1][1] AS component,
        extractAllGroups(query, '${APP_RE_COMPONENT_SERVICE}')[1][2] AS service,
        quantile(0.5)(query_duration_ms) AS p50,
        quantile(0.95)(query_duration_ms) AS p95,
        quantile(0.99)(query_duration_ms) AS p99
    FROM {{cluster_aware:system.query_log}}
    WHERE type = 'QueryFinish'
      AND query LIKE ${APP_SOURCE_LIKE}
      AND event_time > {{time_range}}
    GROUP BY component, service
    HAVING component != '' AND service != '' AND {{drill:component | 1=1}}
  )
)
ARRAY JOIN
  ['p50', 'p95', 'p99'] AS metric,
  [p50, p95_delta, p99_delta] AS value_ms
ORDER BY service, metric`,

  `-- @meta: title='App Query Volume by Component' group='Self-Monitoring' interval='1 DAY' description='Number of queries fired by each app component in the last 24h'
-- @chart: type=pie group_by=component value=query_count style=3d
-- @drill: on=component into='App Query Volume by Service'
SELECT
    extractAllGroups(query, '${APP_RE_COMPONENT}')[1][1] AS component,
    count() AS query_count,
    round(sum(query_duration_ms), 0) AS total_ms
FROM {{cluster_aware:system.query_log}}
WHERE type = 'QueryFinish'
  AND query LIKE ${APP_SOURCE_LIKE}
  AND event_time > {{time_range}}
GROUP BY component
HAVING component != ''
ORDER BY query_count DESC`,

  `-- @meta: title='App Query Volume by Service' group='Self-Monitoring' interval='1 DAY' description='Drill from component to individual services — query count per service'
-- @chart: type=pie group_by=service value=query_count style=3d
-- @drill: on=service into='App Query Cost Details'
SELECT
    extractAllGroups(query, '${APP_RE_COMPONENT_SERVICE}')[1][1] AS component,
    extractAllGroups(query, '${APP_RE_COMPONENT_SERVICE}')[1][2] AS service,
    count() AS query_count,
    round(sum(query_duration_ms), 0) AS total_ms
FROM {{cluster_aware:system.query_log}}
WHERE type = 'QueryFinish'
  AND query LIKE ${APP_SOURCE_LIKE}
  AND event_time > {{time_range}}
GROUP BY component, service
HAVING component != '' AND service != '' AND {{drill:component | 1=1}}
ORDER BY query_count DESC`,

  `-- @meta: title='App Query Cost Details' group='Self-Monitoring' interval='1 DAY' description='Full cost breakdown per unique query shape — duration, memory, rows, bytes, CPU'
-- @link: on=query_hash into='App Query Executions'
-- @cell: column=avg_result_bytes type=rag green<10000 amber<100000
-- @cell: column=avg_memory_mb type=rag green<20 amber<50
-- @cell: column=max_memory_mb type=rag green<50 amber<200
SELECT
    extractAllGroups(query, '${APP_RE_COMPONENT_SERVICE}')[1][1] AS component,
    extractAllGroups(query, '${APP_RE_COMPONENT_SERVICE}')[1][2] AS service,
    lower(hex(normalized_query_hash)) AS query_hash,
    substring(query, 1, 120) AS query_preview,
    count() AS executions,
    -- duration
    round(min(query_duration_ms), 1) AS min_duration_ms,
    round(avg(query_duration_ms), 1) AS avg_duration_ms,
    round(max(query_duration_ms), 1) AS max_duration_ms,
    round(sum(query_duration_ms), 0) AS total_duration_ms,
    -- memory
    round(min(memory_usage) / 1048576, 2) AS min_memory_mb,
    round(avg(memory_usage) / 1048576, 2) AS avg_memory_mb,
    round(max(memory_usage) / 1048576, 2) AS max_memory_mb,
    round(sum(memory_usage) / 1048576, 1) AS total_memory_mb,
    -- rows read
    min(read_rows) AS min_rows_read,
    round(avg(read_rows)) AS avg_rows_read,
    max(read_rows) AS max_rows_read,
    sum(read_rows) AS total_rows_read,
    -- bytes read
    min(read_bytes) AS min_bytes_read,
    round(avg(read_bytes)) AS avg_bytes_read,
    max(read_bytes) AS max_bytes_read,
    sum(read_bytes) AS total_bytes_read,
    -- result sent to client
    min(result_rows) AS min_result_rows,
    round(avg(result_rows)) AS avg_result_rows,
    max(result_rows) AS max_result_rows,
    sum(result_rows) AS total_result_rows,
    min(result_bytes) AS min_result_bytes,
    round(avg(result_bytes)) AS avg_result_bytes,
    max(result_bytes) AS max_result_bytes,
    sum(result_bytes) AS total_result_bytes,
    -- cpu
    round(min(ProfileEvents['OSCPUVirtualTimeMicroseconds']) / 1e6, 3) AS min_cpu_s,
    round(avg(ProfileEvents['OSCPUVirtualTimeMicroseconds']) / 1e6, 3) AS avg_cpu_s,
    round(max(ProfileEvents['OSCPUVirtualTimeMicroseconds']) / 1e6, 3) AS max_cpu_s,
    round(sum(ProfileEvents['OSCPUVirtualTimeMicroseconds']) / 1e6, 2) AS total_cpu_s
FROM {{cluster_aware:system.query_log}}
WHERE type = 'QueryFinish'
  AND query LIKE ${APP_SOURCE_LIKE}
  AND event_time > {{time_range}}
GROUP BY component, service, query_hash, query_preview
HAVING component != '' AND service != '' AND {{drill:component | 1=1}} AND {{drill:service | 1=1}}
ORDER BY total_memory_mb DESC`,

  `-- @meta: title='App Query Executions' group='Self-Monitoring' interval='1 DAY' description='Recent executions for a specific query shape — click a row to open the full Query Detail view'
-- @cell: column=query_duration_ms type=rag green<50 amber<500
-- @cell: column=memory_mb type=rag green<20 amber<100
SELECT
    query_id,
    type,
    event_time,
    user,
    query_duration_ms,
    memory_usage,
    round(memory_usage / 1048576, 2) AS memory_mb,
    read_rows,
    read_bytes,
    round(read_bytes / 1048576, 2) AS read_mb,
    result_rows,
    round(ProfileEvents['OSCPUVirtualTimeMicroseconds'] / 1e6, 3) AS cpu_s,
    substring(query, 1, 200) AS query_text
FROM {{cluster_aware:system.query_log}}
WHERE type = 'QueryFinish'
  AND lower(hex(normalized_query_hash)) = {{drill_value:query_hash | ''}}
  AND event_time > {{time_range}}
ORDER BY event_time DESC
LIMIT 50`,

  `-- @meta: title='App Query Timeline (5min buckets)' group='Self-Monitoring' interval='6 HOUR' description='App query rate and avg duration over time — spot polling spikes'
-- @chart: type=grouped_line group_by=t value=query_count,avg_ms style=2d
SELECT
    toStartOfFiveMinutes(event_time) AS t,
    count() AS query_count,
    round(avg(query_duration_ms), 1) AS avg_ms
FROM {{cluster_aware:system.query_log}}
WHERE type = 'QueryFinish'
  AND query LIKE ${APP_SOURCE_LIKE}
  AND event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Slowest App Queries' group='Self-Monitoring' interval='1 DAY' description='Top 20 slowest individual queries generated by the app'
-- @link: on=query_id into='Query Detail by ID'
SELECT
    event_time,
    query_id,
    query_duration_ms,
    round(memory_usage / 1048576, 2) AS memory_mb,
    round(read_bytes / 1048576, 2) AS read_mb,
    read_rows,
    extractAllGroups(query, '${APP_RE_COMPONENT_SERVICE}')[1][1] AS component,
    extractAllGroups(query, '${APP_RE_COMPONENT_SERVICE}')[1][2] AS service,
    substring(query, 1, 120) AS query_preview
FROM {{cluster_aware:system.query_log}}
WHERE type = 'QueryFinish'
  AND query LIKE ${APP_SOURCE_LIKE}
  AND event_time > {{time_range}}
  AND extractAllGroups(query, '${APP_RE_COMPONENT_SERVICE}')[1][1] != ''
ORDER BY query_duration_ms DESC
LIMIT 20`,

  `-- @meta: title='App Memory & Rows by Component' group='Self-Monitoring' interval='1 DAY' description='Total memory and rows read per component — find the heaviest hitters'
-- @chart: type=bar group_by=component value=total_memory_mb style=2d
SELECT
    extractAllGroups(query, '${APP_RE_COMPONENT}')[1][1] AS component,
    count() AS queries,
    round(sum(memory_usage) / 1048576, 1) AS total_memory_mb,
    sum(read_rows) AS total_rows_read,
    round(sum(read_bytes) / 1048576, 1) AS total_read_mb
FROM {{cluster_aware:system.query_log}}
WHERE type = 'QueryFinish'
  AND query LIKE ${APP_SOURCE_LIKE}
  AND event_time > {{time_range}}
GROUP BY component
HAVING component != ''
ORDER BY total_memory_mb DESC`,

  `-- @meta: title='App Failed Queries' group='Self-Monitoring' interval='7 DAY' description='App queries that threw exceptions — helps catch broken polling or bad SQL'
-- @link: on=query_id into='Query Detail by ID'
SELECT
    event_time,
    query_id,
    extractAllGroups(query, '${APP_RE_COMPONENT_SERVICE}')[1][1] AS component,
    extractAllGroups(query, '${APP_RE_COMPONENT_SERVICE}')[1][2] AS service,
    exception_code,
    substring(exception, 1, 150) AS exception_preview,
    substring(query, 1, 120) AS query_preview
FROM {{cluster_aware:system.query_log}}
WHERE type = 'ExceptionWhileProcessing'
  AND query LIKE ${APP_SOURCE_LIKE}
  AND event_time > {{time_range}}
  AND extractAllGroups(query, '${APP_RE_COMPONENT_SERVICE}')[1][1] != ''
ORDER BY event_time DESC
LIMIT 30`,

  `-- @meta: title='App Query Duration Trend (hourly)' group='Self-Monitoring' interval='2 DAY' description='Hourly p50/p95/p99 of app query duration — detect regressions over time'
-- @chart: type=grouped_line group_by=hour value=p50_ms,p95_ms,p99_ms unit=ms style=2d
SELECT
    toStartOfHour(event_time) AS hour,
    count() AS query_count,
    round(quantile(0.5)(query_duration_ms), 1) AS p50_ms,
    round(quantile(0.95)(query_duration_ms), 1) AS p95_ms,
    round(quantile(0.99)(query_duration_ms), 1) AS p99_ms
FROM {{cluster_aware:system.query_log}}
WHERE type = 'QueryFinish'
  AND query LIKE ${APP_SOURCE_LIKE}
  AND event_time > {{time_range}}
GROUP BY hour
ORDER BY hour ASC`,

  `-- @meta: title='Sampling Status' group='Self-Monitoring' interval='1 HOUR' description='Health check for the system.processes & system.merges sampling pipeline — shows whether each sampler is running on schedule with no gaps or errors'
-- @cell: column=status type=rag green=ok amber=degraded
SELECT
    sampler,
    status,
    last_success_time,
    seconds_since_last_sample,
    total_samples,
    gaps_over_10s,
    exception_preview
FROM (
    SELECT
        r.view AS sampler,
        r.last_success_time AS last_success_time,
        r.retry AS retry,
        substring(r.exception, 1, 120) AS exception_preview,
        h.total_samples AS total_samples,
        h.gaps_over_10s AS gaps_over_10s,
        h.seconds_since_last AS seconds_since_last_sample,
        multiIf(
            r.exception != '', 'error',
            h.total_samples = 0, 'no data',
            h.gaps_over_10s > 5 OR h.seconds_since_last > 30, 'degraded',
            'ok'
        ) AS status
    FROM {{cluster_aware:system.view_refreshes}} AS r
    LEFT JOIN (
        SELECT
            'processes_sampler' AS view,
            count() AS total_samples,
            countIf(gap_s > 10) AS gaps_over_10s,
            date_diff('second', max(sample_time), now()) AS seconds_since_last
        FROM (
            SELECT sample_time,
                   date_diff('millisecond', lagInFrame(sample_time) OVER (ORDER BY sample_time), sample_time) / 1000.0 AS gap_s
            FROM {{cluster_aware:tracehouse.processes_history}}
            WHERE sample_time > {{time_range}}
        )
        UNION ALL
        SELECT
            'merges_sampler' AS view,
            count() AS total_samples,
            countIf(gap_s > 10) AS gaps_over_10s,
            date_diff('second', max(sample_time), now()) AS seconds_since_last
        FROM (
            SELECT sample_time,
                   date_diff('millisecond', lagInFrame(sample_time) OVER (ORDER BY sample_time), sample_time) / 1000.0 AS gap_s
            FROM {{cluster_aware:tracehouse.merges_history}}
            WHERE sample_time > {{time_range}}
        )
    ) AS h ON r.view = h.view
    WHERE r.database = 'tracehouse'
      AND r.view LIKE '%_sampler'
)
ORDER BY sampler`,

  `-- @meta: title='Sampling Refresh Status' group='Self-Monitoring' interval='1 HOUR' description='Detailed refresh state of processes_sampler & merges_sampler refreshable MVs — last success, next refresh, retry count, exceptions'
SELECT
    view AS sampler,
    status,
    last_success_time,
    last_refresh_time,
    next_refresh_time,
    retry,
    read_rows,
    read_bytes,
    substring(exception, 1, 150) AS exception_preview
FROM {{cluster_aware:system.view_refreshes}}
WHERE database = 'tracehouse'
  AND view LIKE '%_sampler'
ORDER BY view`,

  `-- @meta: title='Sampling Cost Trend (5min)' group='Self-Monitoring' interval='6 HOUR' description='Cost of sampling system.processes & system.merges over time — duration, memory, CPU per 5-min bucket'
-- @chart: type=grouped_line group_by=t value=avg_duration_ms,avg_memory_mb,avg_cpu_ms series=sampler style=2d
SELECT
    toStartOfFiveMinutes(event_time) AS t,
    multiIf(
      query LIKE '%FROM system.processes%', 'processes',
      query LIKE '%FROM system.merges%', 'merges',
      'unknown'
    ) AS sampler,
    count() AS executions,
    round(avg(query_duration_ms), 2) AS avg_duration_ms,
    round(avg(memory_usage) / 1048576, 3) AS avg_memory_mb,
    round(avg(ProfileEvents['OSCPUVirtualTimeMicroseconds']) / 1e3, 2) AS avg_cpu_ms
FROM {{cluster_aware:system.query_log}}
WHERE type = 'QueryFinish'
  AND (query LIKE '%INSERT INTO tracehouse.processes\\_history%' OR query LIKE '%INSERT INTO tracehouse.merges\\_history%')
  AND event_time > {{time_range}}
GROUP BY t, sampler
ORDER BY t ASC`,

  `-- @meta: title='Sampling Cost Summary' group='Self-Monitoring' interval='1 DAY' description='Aggregate cost of sampling system.processes & system.merges — duration, memory, rows read, CPU time'
-- @cell: column=avg_duration_ms type=rag green<5 amber<50
-- @cell: column=avg_memory_mb type=rag green<1 amber<10
SELECT
    multiIf(
      query LIKE '%FROM system.processes%', 'processes',
      query LIKE '%FROM system.merges%', 'merges',
      'unknown'
    ) AS sampler,
    count() AS executions,
    round(avg(query_duration_ms), 2) AS avg_duration_ms,
    round(quantile(0.99)(query_duration_ms), 2) AS p99_duration_ms,
    round(sum(query_duration_ms), 0) AS total_duration_ms,
    round(avg(memory_usage) / 1048576, 3) AS avg_memory_mb,
    round(max(memory_usage) / 1048576, 3) AS max_memory_mb,
    round(avg(read_rows)) AS avg_rows_read,
    round(avg(ProfileEvents['OSCPUVirtualTimeMicroseconds']) / 1e6, 4) AS avg_cpu_s,
    round(sum(ProfileEvents['OSCPUVirtualTimeMicroseconds']) / 1e6, 2) AS total_cpu_s
FROM {{cluster_aware:system.query_log}}
WHERE type = 'QueryFinish'
  AND (query LIKE '%INSERT INTO tracehouse.processes\\_history%' OR query LIKE '%INSERT INTO tracehouse.merges\\_history%')
  AND event_time > {{time_range}}
GROUP BY sampler
ORDER BY sampler`,

  `-- @meta: title='Sampling Gaps (processes)' group='Self-Monitoring' interval='6 HOUR' description='Gaps in system.processes sampling — periods where no process snapshots were collected, by hostname'
-- @cell: column=gap_seconds type=rag green<10 amber<30
SELECT
    hostname,
    prev_time,
    sample_time AS gap_start,
    round(gap_seconds, 1) AS gap_seconds
FROM (
    SELECT
        hostname,
        sample_time,
        lagInFrame(sample_time) OVER (PARTITION BY hostname ORDER BY sample_time) AS prev_time,
        date_diff('millisecond', lagInFrame(sample_time) OVER (PARTITION BY hostname ORDER BY sample_time), sample_time) / 1000.0 AS gap_seconds
    FROM {{cluster_aware:tracehouse.processes_history}}
    WHERE sample_time > {{time_range}}
)
WHERE gap_seconds > 5
  AND prev_time > toDateTime64('1970-01-01 00:00:01', 3)
ORDER BY gap_seconds DESC
LIMIT 50`,

  `-- @meta: title='Sampling Gaps (merges)' group='Self-Monitoring' interval='6 HOUR' description='Gaps in system.merges sampling — periods where no merge snapshots were collected, by hostname'
-- @cell: column=gap_seconds type=rag green<10 amber<30
SELECT
    hostname,
    prev_time,
    sample_time AS gap_start,
    round(gap_seconds, 1) AS gap_seconds
FROM (
    SELECT
        hostname,
        sample_time,
        lagInFrame(sample_time) OVER (PARTITION BY hostname ORDER BY sample_time) AS prev_time,
        date_diff('millisecond', lagInFrame(sample_time) OVER (PARTITION BY hostname ORDER BY sample_time), sample_time) / 1000.0 AS gap_seconds
    FROM {{cluster_aware:tracehouse.merges_history}}
    WHERE sample_time > {{time_range}}
)
WHERE gap_seconds > 5
  AND prev_time > toDateTime64('1970-01-01 00:00:01', 3)
ORDER BY gap_seconds DESC
LIMIT 50`,

  `-- @meta: title='Sampling Database Coverage' group='Self-Monitoring' interval='1 HOUR' description='Shows which cluster nodes have the tracehouse database — needed for system.processes & system.merges sampling to work on all nodes'
-- @cell: column=has_tracehouse type=rag green=1 amber=0
SELECT
    hostName() AS host,
    countIf(name = 'tracehouse') AS has_tracehouse
FROM {{cluster_aware:system.databases}}
GROUP BY host
ORDER BY host`,

  `-- @meta: title='App % of Server Load' group='Self-Monitoring' interval='1 DAY' description='What fraction of total server query time is consumed by the app itself'
-- @chart: type=line group_by=hour value=app_pct unit=% style=2d
SELECT
    toStartOfHour(event_time) AS hour,
    sum(query_duration_ms) AS total_server_ms,
    sumIf(query_duration_ms, query LIKE ${APP_SOURCE_LIKE}) AS app_ms,
    round(if(total_server_ms > 0, app_ms / total_server_ms * 100, 0), 2) AS app_pct
FROM {{cluster_aware:system.query_log}}
WHERE type = 'QueryFinish'
  AND event_time > {{time_range}}
GROUP BY hour
ORDER BY hour ASC`,
];

export default queries;
