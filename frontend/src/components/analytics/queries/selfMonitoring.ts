/** Self-monitoring queries — track the app's own query footprint on the server. */

const queries: string[] = [
  `-- @meta: title='App Query Duration by Component' group='Self-Monitoring' interval='1 DAY' description='Stacked percentile bands per component — bar height = p99, segments show p50 / p95−p50 / p99−p95'
-- @chart: type=stacked_bar labels=component values=value_ms group=metric unit=ms style=2d
-- @drill: on=component into='App Query Duration by Service'
SELECT component, metric, value_ms
FROM (
  SELECT
      extractAllGroups(query, 'source:Monitor:(\\w+):')[1][1] AS component,
      round(quantile(0.5)(query_duration_ms), 1) AS p50,
      round(quantile(0.95)(query_duration_ms) - quantile(0.5)(query_duration_ms), 1) AS p95_delta,
      round(quantile(0.99)(query_duration_ms) - quantile(0.95)(query_duration_ms), 1) AS p99_delta
  FROM {{cluster_aware:system.query_log}}
  WHERE type = 'QueryFinish'
    AND query LIKE '%source:Monitor:%'
    AND event_time > {{time_range}}
  GROUP BY component
  HAVING component != ''
)
ARRAY JOIN
  ['p50', 'p95', 'p99'] AS metric,
  [p50, p95_delta, p99_delta] AS value_ms
ORDER BY component, metric`,

  `-- @meta: title='App Query Duration by Service' group='Self-Monitoring' interval='1 DAY' description='Drill from component to individual services — stacked percentile bands per service'
-- @chart: type=stacked_bar labels=service values=value_ms group=metric unit=ms style=2d
-- @drill: on=service into='App Query Cost Details'
SELECT service, metric, value_ms
FROM (
  SELECT
      extractAllGroups(query, 'source:Monitor:(\\w+):(\\w+)')[1][1] AS component,
      extractAllGroups(query, 'source:Monitor:(\\w+):(\\w+)')[1][2] AS service,
      round(quantile(0.5)(query_duration_ms), 1) AS p50,
      round(quantile(0.95)(query_duration_ms) - quantile(0.5)(query_duration_ms), 1) AS p95_delta,
      round(quantile(0.99)(query_duration_ms) - quantile(0.95)(query_duration_ms), 1) AS p99_delta
  FROM {{cluster_aware:system.query_log}}
  WHERE type = 'QueryFinish'
    AND query LIKE '%source:Monitor:%'
    AND event_time > {{time_range}}
  GROUP BY component, service
  HAVING component != '' AND service != '' AND {{drill:component | 1=1}}
)
ARRAY JOIN
  ['p50', 'p95', 'p99'] AS metric,
  [p50, p95_delta, p99_delta] AS value_ms
ORDER BY service, metric`,

  `-- @meta: title='App Query Volume by Component' group='Self-Monitoring' interval='1 DAY' description='Number of queries fired by each app component in the last 24h'
-- @chart: type=pie labels=component values=query_count style=3d
-- @drill: on=component into='App Query Volume by Service'
SELECT
    extractAllGroups(query, 'source:Monitor:(\\w+):')[1][1] AS component,
    count() AS query_count,
    round(sum(query_duration_ms), 0) AS total_ms
FROM {{cluster_aware:system.query_log}}
WHERE type = 'QueryFinish'
  AND query LIKE '%source:Monitor:%'
  AND event_time > {{time_range}}
GROUP BY component
HAVING component != ''
ORDER BY query_count DESC`,

  `-- @meta: title='App Query Volume by Service' group='Self-Monitoring' interval='1 DAY' description='Drill from component to individual services — query count per service'
-- @chart: type=pie labels=service values=query_count style=3d
-- @drill: on=service into='App Query Cost Details'
SELECT
    extractAllGroups(query, 'source:Monitor:(\\w+):(\\w+)')[1][1] AS component,
    extractAllGroups(query, 'source:Monitor:(\\w+):(\\w+)')[1][2] AS service,
    count() AS query_count,
    round(sum(query_duration_ms), 0) AS total_ms
FROM {{cluster_aware:system.query_log}}
WHERE type = 'QueryFinish'
  AND query LIKE '%source:Monitor:%'
  AND event_time > {{time_range}}
GROUP BY component, service
HAVING component != '' AND service != '' AND {{drill:component | 1=1}}
ORDER BY query_count DESC`,

  `-- @meta: title='App Query Cost Details' group='Self-Monitoring' interval='1 DAY' description='Full cost breakdown per unique query shape — duration, memory, rows, bytes, CPU'
-- @link: on=query_hash into='App Query Executions'
-- @rag: column=avg_result_bytes green<10000 amber<100000
-- @rag: column=avg_memory_mb green<20 amber<50
SELECT
    extractAllGroups(query, 'source:Monitor:(\\w+):(\\w+)')[1][1] AS component,
    extractAllGroups(query, 'source:Monitor:(\\w+):(\\w+)')[1][2] AS service,
    lower(hex(normalized_query_hash)) AS query_hash,
    substring(query, 1, 120) AS query_preview,
    count() AS executions,
    -- duration
    round(avg(query_duration_ms), 1) AS avg_duration_ms,
    round(sum(query_duration_ms), 0) AS total_duration_ms,
    -- memory
    round(avg(memory_usage) / 1048576, 2) AS avg_memory_mb,
    round(sum(memory_usage) / 1048576, 1) AS total_memory_mb,
    -- rows read
    round(avg(read_rows)) AS avg_rows_read,
    sum(read_rows) AS total_rows_read,
    -- bytes read
    round(avg(read_bytes)) AS avg_bytes_read,
    sum(read_bytes) AS total_bytes_read,
    -- result sent to client
    round(avg(result_rows)) AS avg_result_rows,
    sum(result_rows) AS total_result_rows,
    round(avg(result_bytes)) AS avg_result_bytes,
    sum(result_bytes) AS total_result_bytes,
    -- cpu
    round(avg(ProfileEvents['OSCPUVirtualTimeMicroseconds']) / 1e6, 3) AS avg_cpu_s,
    round(sum(ProfileEvents['OSCPUVirtualTimeMicroseconds']) / 1e6, 2) AS total_cpu_s
FROM {{cluster_aware:system.query_log}}
WHERE type = 'QueryFinish'
  AND query LIKE '%source:Monitor:%'
  AND event_time > {{time_range}}
GROUP BY component, service, query_hash, query_preview
HAVING component != '' AND service != '' AND {{drill:component | 1=1}} AND {{drill:service | 1=1}}
ORDER BY total_memory_mb DESC`,

  `-- @meta: title='App Query Executions' group='Self-Monitoring' interval='1 DAY' description='Recent executions for a specific query shape — click a row to open the full Query Detail view'
-- @rag: column=query_duration_ms green<50 amber<500
-- @rag: column=memory_mb green<20 amber<100
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
-- @chart: type=grouped_line labels=t values=query_count,avg_ms style=2d
SELECT
    toStartOfFiveMinutes(event_time) AS t,
    count() AS query_count,
    round(avg(query_duration_ms), 1) AS avg_ms
FROM {{cluster_aware:system.query_log}}
WHERE type = 'QueryFinish'
  AND query LIKE '%source:Monitor:%'
  AND event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Slowest App Queries' group='Self-Monitoring' interval='1 DAY' description='Top 20 slowest individual queries generated by the app'
SELECT
    event_time,
    query_duration_ms,
    round(memory_usage / 1048576, 2) AS memory_mb,
    round(read_bytes / 1048576, 2) AS read_mb,
    read_rows,
    extractAllGroups(query, 'source:Monitor:(\\w+):(\\w+)')[1][1] AS component,
    extractAllGroups(query, 'source:Monitor:(\\w+):(\\w+)')[1][2] AS service,
    substring(query, 1, 120) AS query_preview
FROM {{cluster_aware:system.query_log}}
WHERE type = 'QueryFinish'
  AND query LIKE '%source:Monitor:%'
  AND event_time > {{time_range}}
  AND extractAllGroups(query, 'source:Monitor:(\\w+):(\\w+)')[1][1] != ''
ORDER BY query_duration_ms DESC
LIMIT 20`,

  `-- @meta: title='App Memory & Rows by Component' group='Self-Monitoring' interval='1 DAY' description='Total memory and rows read per component — find the heaviest hitters'
-- @chart: type=bar labels=component values=total_memory_mb style=2d
SELECT
    extractAllGroups(query, 'source:Monitor:(\\w+):')[1][1] AS component,
    count() AS queries,
    round(sum(memory_usage) / 1048576, 1) AS total_memory_mb,
    sum(read_rows) AS total_rows_read,
    round(sum(read_bytes) / 1048576, 1) AS total_read_mb
FROM {{cluster_aware:system.query_log}}
WHERE type = 'QueryFinish'
  AND query LIKE '%source:Monitor:%'
  AND event_time > {{time_range}}
GROUP BY component
HAVING component != ''
ORDER BY total_memory_mb DESC`,

  `-- @meta: title='App Failed Queries' group='Self-Monitoring' interval='7 DAY' description='App queries that threw exceptions — helps catch broken polling or bad SQL'
SELECT
    event_time,
    extractAllGroups(query, 'source:Monitor:(\\w+):(\\w+)')[1][1] AS component,
    extractAllGroups(query, 'source:Monitor:(\\w+):(\\w+)')[1][2] AS service,
    exception_code,
    substring(exception, 1, 150) AS exception_preview,
    substring(query, 1, 120) AS query_preview
FROM {{cluster_aware:system.query_log}}
WHERE type = 'ExceptionWhileProcessing'
  AND query LIKE '%source:Monitor:%'
  AND event_time > {{time_range}}
  AND extractAllGroups(query, 'source:Monitor:(\\w+):(\\w+)')[1][1] != ''
ORDER BY event_time DESC
LIMIT 30`,

  `-- @meta: title='App Query Duration Trend (hourly)' group='Self-Monitoring' interval='2 DAY' description='Hourly p50/p95/p99 of app query duration — detect regressions over time'
-- @chart: type=grouped_line labels=hour values=p50_ms,p95_ms,p99_ms unit=ms style=2d
SELECT
    toStartOfHour(event_time) AS hour,
    count() AS query_count,
    round(quantile(0.5)(query_duration_ms), 1) AS p50_ms,
    round(quantile(0.95)(query_duration_ms), 1) AS p95_ms,
    round(quantile(0.99)(query_duration_ms), 1) AS p99_ms
FROM {{cluster_aware:system.query_log}}
WHERE type = 'QueryFinish'
  AND query LIKE '%source:Monitor:%'
  AND event_time > {{time_range}}
GROUP BY hour
ORDER BY hour ASC`,

  `-- @meta: title='App % of Server Load' group='Self-Monitoring' interval='1 DAY' description='What fraction of total server query time is consumed by the app itself'
-- @chart: type=line labels=hour values=app_pct unit=% style=2d
SELECT
    toStartOfHour(event_time) AS hour,
    sum(query_duration_ms) AS total_server_ms,
    sumIf(query_duration_ms, query LIKE '%source:Monitor:%') AS app_ms,
    round(if(total_server_ms > 0, app_ms / total_server_ms * 100, 0), 2) AS app_pct
FROM {{cluster_aware:system.query_log}}
WHERE type = 'QueryFinish'
  AND event_time > {{time_range}}
GROUP BY hour
ORDER BY hour ASC`,
];

export default queries;
