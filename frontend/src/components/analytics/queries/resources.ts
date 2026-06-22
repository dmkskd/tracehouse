/** Resource monitoring queries - memory-heavy inserts, failed queries, limits. */

const queries: string[] = [
  `-- @meta: title='Server Pressure Radar' group='Resources' interval='1 HOUR' description='One-row server pressure profile for the selected time range using average CPU, memory, I/O, network, and active query load'
-- @chart: type=radar axes=cpu:cpu_pressure,memory:memory_pressure,io:io_bytes,network:network_bytes,queries:active_queries ranges=cpu:0..1,memory:0..1,io:1Mi..10Gi,network:1Mi..10Gi,queries:1..1000 transforms=cpu:linear,memory:linear,io:log,network:log,queries:log color=profile_level
-- @source: https://clickhouse.com/docs/operations/system-tables/metric_log
-- @source: https://clickhouse.com/docs/operations/system-tables/asynchronous_metric_log
SELECT
    (
        SELECT avg(cpu_pressure)
        FROM
        (
            SELECT
                toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
                avgIf(value, metric = 'OSUserTimeNormalized')
                  + avgIf(value, metric = 'OSSystemTimeNormalized')
                  + avgIf(value, metric = 'OSIOWaitTimeNormalized') AS cpu_pressure
            FROM {{cluster_aware:system.asynchronous_metric_log}}
            WHERE event_time > {{time_range}}
              AND metric IN ('OSUserTimeNormalized', 'OSSystemTimeNormalized', 'OSIOWaitTimeNormalized')
            GROUP BY t
        )
    ) AS cpu_pressure,
    (
        SELECT avg(memory_pressure)
        FROM
        (
            SELECT
                toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
                greatest(
                    0,
                    least(
                        1,
                        1 - avgIf(value, metric = 'OSMemoryAvailable') / nullIf(avgIf(value, metric = 'OSMemoryTotal'), 0)
                    )
                ) AS memory_pressure
            FROM {{cluster_aware:system.asynchronous_metric_log}}
            WHERE event_time > {{time_range}}
              AND metric IN ('OSMemoryAvailable', 'OSMemoryTotal')
            GROUP BY t
            HAVING avgIf(value, metric = 'OSMemoryTotal') > 0
        )
    ) AS memory_pressure,
    (
        SELECT avg(io_bytes)
        FROM
        (
            SELECT
                toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
                avg(ProfileEvent_OSReadBytes + ProfileEvent_OSWriteBytes) AS io_bytes
            FROM {{cluster_aware:system.metric_log}}
            WHERE event_time > {{time_range}}
            GROUP BY t
        )
    ) AS io_bytes,
    (
        SELECT avg(network_bytes)
        FROM
        (
            SELECT
                toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
                avg(ProfileEvent_NetworkSendBytes + ProfileEvent_NetworkReceiveBytes) AS network_bytes
            FROM {{cluster_aware:system.metric_log}}
            WHERE event_time > {{time_range}}
            GROUP BY t
        )
    ) AS network_bytes,
    (
        SELECT avg(active_queries)
        FROM
        (
            SELECT
                toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
                avg(CurrentMetric_Query) AS active_queries
            FROM {{cluster_aware:system.metric_log}}
            WHERE event_time > {{time_range}}
            GROUP BY t
        )
    ) AS active_queries`,

  `-- @meta: title='Top Inserts by Memory' group='Resources' description='Most memory-intensive INSERT operations'
-- @source: https://clickhouse.com/blog/monitoring-troubleshooting-insert-queries-clickhouse
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
-- @source: https://clickhouse.com/blog/monitoring-troubleshooting-select-queries-clickhouse
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
-- @source: https://clickhouse.com/blog/monitoring-troubleshooting-select-queries-clickhouse
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
-- @chart: type=area group_by=minute value=avg_memory style=2d color=#a855f7
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
