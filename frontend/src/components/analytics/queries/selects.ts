/** SELECT monitoring queries — expensive queries, duration trends, user breakdown. */

const queries: string[] = [
  `-- @meta: title='Most Expensive SELECTs' group='Selects' interval='1 DAY' description='Top 20 slowest SELECT queries in the last day'
-- Source: https://clickhouse.com/blog/monitoring-troubleshooting-select-queries-clickhouse
SELECT
    type,
    query_start_time,
    query_duration_ms,
    query_id,
    substring(query, 1, 120) AS query_preview,
    read_rows,
    formatReadableSize(read_bytes) AS read_size,
    result_rows,
    formatReadableSize(memory_usage) AS memory,
    user
FROM {{cluster_aware:system.query_log}}
WHERE type != 'QueryStart'
  AND query_kind = 'Select'
  AND event_date >= toDate({{time_range}})
ORDER BY query_duration_ms DESC
LIMIT 20`,

  `-- @meta: title='Avg SELECT Duration by Table' group='Selects' interval='1 DAY' description='Average query duration and request count per table'
-- Source: https://clickhouse.com/blog/monitoring-troubleshooting-select-queries-clickhouse
SELECT
    arrayJoin(tables) AS table,
    count() AS query_count,
    avg(query_duration_ms) AS avg_duration_ms,
    quantile(0.95)(query_duration_ms) AS p95_duration_ms
FROM {{cluster_aware:system.query_log}}
WHERE type = 'QueryFinish'
  AND query_kind = 'Select'
  AND event_time > {{time_range}}
GROUP BY table
ORDER BY avg_duration_ms DESC
LIMIT 20`,

  `-- @meta: title='SELECT Duration Trend (hourly)' group='Selects' interval='2 DAY' description='Hourly average and p95 SELECT duration over the last 2 days'
-- @chart: type=grouped_line group_by=hour value=avg_duration_ms,p95_duration_ms unit=ms style=2d
-- Source: https://clickhouse.com/blog/monitoring-troubleshooting-select-queries-clickhouse
SELECT
    toStartOfHour(event_time) AS hour,
    count() AS query_count,
    avg(query_duration_ms) AS avg_duration_ms,
    quantile(0.95)(query_duration_ms) AS p95_duration_ms
FROM {{cluster_aware:system.query_log}}
WHERE type = 'QueryFinish'
  AND query_kind = 'Select'
  AND event_time > {{time_range}}
GROUP BY hour
ORDER BY hour ASC`,

  `-- @meta: title='Queries by User' group='Selects' interval='1 DAY' description='Number of SELECT queries per user in the last day'
-- @chart: type=pie group_by=user value=query_count style=3d
-- Source: https://clickhouse.com/blog/monitoring-troubleshooting-select-queries-clickhouse
SELECT
    user,
    count() AS query_count,
    avg(query_duration_ms) AS avg_duration_ms,
    formatReadableSize(sum(read_bytes)) AS total_read
FROM {{cluster_aware:system.query_log}}
WHERE type = 'QueryFinish'
  AND query_kind = 'Select'
  AND event_time > {{time_range}}
GROUP BY user
ORDER BY query_count DESC`,

  `-- @meta: title='Recent SELECTs' group='Selects' interval='1 HOUR' description='Most recent SELECT queries with duration — useful for picking query IDs to compare'
-- @link: on=query_id into='Query Detail by ID'
-- Source: https://clickhouse.com/blog/monitoring-troubleshooting-select-queries-clickhouse
SELECT
    query_id,
    substring(query, 1, 120) AS query_preview,
    query_duration_ms,
    user,
    event_time
FROM {{cluster_aware:system.query_log}}
WHERE type != 'QueryStart'
  AND query_kind = 'Select'
  AND event_time > {{time_range}}
ORDER BY event_time DESC
LIMIT 20`,

  `-- @meta: title='Queries by Client' group='Selects' interval='1 DAY' description='SELECT query count per client application'
-- @chart: type=pie group_by=client_name value=query_count style=3d
-- Source: https://clickhouse.com/blog/monitoring-troubleshooting-select-queries-clickhouse
SELECT
    if(empty(client_name), 'unknown/http', client_name) AS client_name,
    count() AS query_count,
    avg(query_duration_ms) AS avg_duration_ms
FROM {{cluster_aware:system.query_log}}
WHERE type = 'QueryFinish'
  AND query_kind = 'Select'
  AND event_time > {{time_range}}
GROUP BY client_name
ORDER BY query_count DESC`,

  `-- @meta: title='Read Rows Distribution' group='Selects' interval='1 DAY' description='Distribution of rows read per SELECT query'
-- @chart: type=pie group_by=bucket value=query_count style=3d
-- Source: https://clickhouse.com/blog/monitoring-troubleshooting-select-queries-clickhouse
SELECT
    multiIf(
      read_rows < 1000, '< 1K',
      read_rows < 100000, '1K-100K',
      read_rows < 1000000, '100K-1M',
      read_rows < 100000000, '1M-100M',
      '> 100M'
    ) AS bucket,
    count() AS query_count
FROM {{cluster_aware:system.query_log}}
WHERE type = 'QueryFinish'
  AND query_kind = 'Select'
  AND event_time > {{time_range}}
GROUP BY bucket
ORDER BY query_count DESC`,
];

export default queries;
