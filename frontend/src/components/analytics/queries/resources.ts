/** Resource monitoring queries — memory-heavy inserts, failed queries, limits. */

const queries: string[] = [
  `-- @meta: title='Top Inserts by Memory' group='Resources' description='Most memory-intensive INSERT operations'
-- Source: https://clickhouse.com/blog/monitoring-troubleshooting-insert-queries-clickhouse
SELECT
    event_time,
    formatReadableSize(memory_usage) AS memory,
    ProfileEvents['UserTimeMicroseconds'] AS userCPU,
    ProfileEvents['SystemTimeMicroseconds'] AS systemCPU,
    substring(query, 1, 120) AS query_preview,
    initial_query_id
FROM {{cluster_aware:system.query_log}}
WHERE query_kind = 'Insert'
  AND type = 'QueryFinish'
ORDER BY memory_usage DESC
LIMIT 15`,

  `-- @meta: title='Failed Queries' group='Resources' interval='1 DAY' description='Queries that produced exceptions in the last day'
-- Source: https://clickhouse.com/blog/monitoring-troubleshooting-select-queries-clickhouse
SELECT
    type,
    event_time,
    query_duration_ms,
    query_id,
    substring(query, 1, 120) AS query_preview,
    exception,
    user
FROM {{cluster_aware:system.query_log}}
WHERE type = 'ExceptionWhileProcessing'
  AND event_time > {{time_range}}
ORDER BY event_time DESC
LIMIT 30`,

  `-- @meta: title='TOO_MANY_SIMULTANEOUS_QUERIES' group='Resources' interval='7 DAY' description='Detect if the server is hitting the simultaneous query limit'
-- Source: https://clickhouse.com/blog/monitoring-troubleshooting-select-queries-clickhouse
SELECT
    event_time,
    query_id,
    user,
    exception,
    substring(query, 1, 120) AS query_preview
FROM {{cluster_aware:system.query_log}}
WHERE type = 'ExceptionBeforeStart'
  AND exception LIKE '%TOO_MANY_SIMULTANEOUS_QUERIES%'
  AND event_time > {{time_range}}
ORDER BY event_time DESC
LIMIT 30`,

  `-- @meta: title='Memory Usage Trend' group='Resources' interval='1 DAY' description='Memory usage over the last 24 hours'
-- @chart: type=area labels=minute values=avg_memory style=3d
SELECT
    toStartOfMinute(event_time) AS minute,
    avg(value) AS avg_memory
FROM {{cluster_aware:system.asynchronous_metric_log}}
WHERE metric = 'MemoryTracking'
  AND event_time > {{time_range}}
GROUP BY minute
ORDER BY minute ASC`,
];

export default queries;
