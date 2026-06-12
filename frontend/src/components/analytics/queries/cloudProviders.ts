/** Cloud provider queries — attribute managed ClickHouse Cloud activity from system.query_log. */

const CLOUD_USER_CONDITION = "(endsWith(user, '-internal') OR user = 'sql-console' OR startsWith(user, 'sql-console:'))";
const CLOUD_INTERNAL_CONDITION = "endsWith(user, '-internal')";
const CLOUD_ACTIVITY_SOURCE = "multiIf(endsWith(user, '-internal'), 'Cloud internal', user = 'sql-console' OR startsWith(user, 'sql-console:'), 'Cloud SQL Console', 'Other')";

const queries: string[] = [
  `-- @meta: title='Cloud Provider Query Duration by User' group='Cloud Providers' interval='1 DAY' description='Stacked percentile bands for ClickHouse Cloud internal and SQL Console users'
-- @chart: type=stacked_bar group_by=user value=value_ms series=metric unit=ms style=2d
-- @drill: on=user into='Cloud Provider Query Cost Details'
SELECT user, metric, value_ms
FROM (
  SELECT
      user,
      round(p50, 1) AS p50,
      round(p95 - p50, 1) AS p95_delta,
      round(p99 - p95, 1) AS p99_delta
  FROM (
    SELECT
        user,
        quantile(0.5)(query_duration_ms) AS p50,
        quantile(0.95)(query_duration_ms) AS p95,
        quantile(0.99)(query_duration_ms) AS p99
    FROM {{cluster_aware:system.query_log}}
    WHERE type = 'QueryFinish'
      AND ${CLOUD_USER_CONDITION}
      AND user = {{drill_value:user | user}}
      AND event_time > {{time_range}}
    GROUP BY user
  )
)
ARRAY JOIN
  ['p50', 'p95', 'p99'] AS metric,
  [p50, p95_delta, p99_delta] AS value_ms
ORDER BY user, metric`,

  `-- @meta: title='Cloud Provider Query Volume by User' group='Cloud Providers' interval='1 DAY' description='Query count by managed internal user and SQL Console user'
-- @chart: type=pie group_by=user value=query_count style=3d
-- @drill: on=user into='Cloud Provider Query Cost Details'
SELECT
    ${CLOUD_ACTIVITY_SOURCE} AS activity_source,
    user,
    count() AS query_count,
    round(sum(query_duration_ms), 0) AS total_ms,
    round(sum(ProfileEvents['OSCPUVirtualTimeMicroseconds']) / 1e6, 2) AS total_cpu_s
FROM {{cluster_aware:system.query_log}}
WHERE type = 'QueryFinish'
  AND ${CLOUD_USER_CONDITION}
  AND user = {{drill_value:user | user}}
  AND event_time > {{time_range}}
GROUP BY activity_source, user
ORDER BY query_count DESC`,

  `-- @meta: title='Cloud Provider Query Timeline (5min)' group='Cloud Providers' interval='6 HOUR' description='Internal provider and SQL Console query rate over time'
-- @chart: type=grouped_line group_by=t value=provider_internal_queries,sql_console_queries,provider_internal_cpu_s,sql_console_cpu_s style=2d
SELECT
    toStartOfFiveMinutes(event_time) AS t,
    countIf(endsWith(user, '-internal')) AS provider_internal_queries,
    countIf(user = 'sql-console' OR startsWith(user, 'sql-console:')) AS sql_console_queries,
    round(sumIf(ProfileEvents['OSCPUVirtualTimeMicroseconds'], endsWith(user, '-internal')) / 1e6, 2) AS provider_internal_cpu_s,
    round(sumIf(ProfileEvents['OSCPUVirtualTimeMicroseconds'], user = 'sql-console' OR startsWith(user, 'sql-console:')) / 1e6, 2) AS sql_console_cpu_s
FROM {{cluster_aware:system.query_log}}
WHERE type = 'QueryFinish'
  AND ${CLOUD_USER_CONDITION}
  AND user = {{drill_value:user | user}}
  AND event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Cloud Internal vs Other Query Activity' group='Cloud Providers' interval='1 DAY' description='Each bar totals 100% of finished query activity; Cloud internal is separated from all other users, with SQL Console included in other users'
-- @chart: type=stacked_bar group_by=metric value=pct series=activity_source unit=% style=2d
SELECT
    metric,
    activity_source,
    pct
FROM (
  SELECT
      metric_order,
      metric,
      activity_source,
      if(total_value = 0, 0, round(value / total_value * 100, 2)) AS pct
  FROM (
    SELECT
        metric_order,
        metric,
        total_value,
        activity_source,
        if(activity_source = 'Cloud internal', internal_value, total_value - internal_value) AS value
    FROM (
      SELECT
          metric_order,
          metric,
          internal_value,
          total_value
      FROM (
        SELECT
            toFloat64(countIf(${CLOUD_INTERNAL_CONDITION} AND user = {{drill_value:user | user}})) AS internal_queries,
            toFloat64(count()) AS total_queries,
            toFloat64(sumIf(read_bytes, ${CLOUD_INTERNAL_CONDITION} AND user = {{drill_value:user | user}})) AS internal_read_bytes,
            toFloat64(sum(read_bytes)) AS total_read_bytes,
            toFloat64(sumIf(query_duration_ms, ${CLOUD_INTERNAL_CONDITION} AND user = {{drill_value:user | user}})) AS internal_elapsed_ms,
            toFloat64(sum(query_duration_ms)) AS total_elapsed_ms,
            toFloat64(sumIf(ProfileEvents['OSCPUVirtualTimeMicroseconds'], ${CLOUD_INTERNAL_CONDITION} AND user = {{drill_value:user | user}})) AS internal_cpu_us,
            toFloat64(sum(ProfileEvents['OSCPUVirtualTimeMicroseconds'])) AS total_cpu_us
        FROM {{cluster_aware:system.query_log}}
        WHERE type = 'QueryFinish' AND event_time > {{time_range}}
      )
      ARRAY JOIN
        [1, 2, 3, 4] AS metric_order,
        ['Queries', 'Read bytes', 'Elapsed time', 'CPU time'] AS metric,
        [internal_queries, internal_read_bytes, internal_elapsed_ms, internal_cpu_us] AS internal_value,
        [total_queries, total_read_bytes, total_elapsed_ms, total_cpu_us] AS total_value
    )
    ARRAY JOIN ['Cloud internal', 'All other users'] AS activity_source
  )
)
ORDER BY metric_order, activity_source DESC`,

  `-- @meta: title='Cloud Provider Query Cost Details' group='Cloud Providers' interval='1 DAY' description='Cost breakdown per provider-managed or SQL Console query shape'
-- @link: on=query_hash into='Cloud Provider Query Executions'
-- @cell: column=avg_memory_mb type=rag green<20 amber<100
-- @cell: column=max_memory_mb type=rag green<100 amber<500
-- @cell: column=avg_duration_ms type=rag green<100 amber<1000
SELECT
    ${CLOUD_ACTIVITY_SOURCE} AS activity_source,
    user,
    lower(hex(normalized_query_hash)) AS query_hash,
    substring(query, 1, 140) AS query_preview,
    count() AS executions,
    round(min(query_duration_ms), 1) AS min_duration_ms,
    round(avg(query_duration_ms), 1) AS avg_duration_ms,
    round(max(query_duration_ms), 1) AS max_duration_ms,
    round(sum(query_duration_ms), 0) AS total_duration_ms,
    round(min(memory_usage) / 1048576, 2) AS min_memory_mb,
    round(avg(memory_usage) / 1048576, 2) AS avg_memory_mb,
    round(max(memory_usage) / 1048576, 2) AS max_memory_mb,
    round(sum(memory_usage) / 1048576, 1) AS total_memory_mb,
    sum(read_rows) AS total_rows_read,
    sum(read_bytes) AS total_bytes_read,
    sum(result_rows) AS total_result_rows,
    sum(result_bytes) AS total_result_bytes,
    round(sum(ProfileEvents['OSCPUVirtualTimeMicroseconds']) / 1e6, 2) AS total_cpu_s
FROM {{cluster_aware:system.query_log}}
WHERE type = 'QueryFinish'
  AND ${CLOUD_USER_CONDITION}
  AND event_time > {{time_range}}
  AND {{drill:user | 1=1}}
GROUP BY activity_source, user, query_hash, query_preview
ORDER BY total_cpu_s DESC, total_duration_ms DESC`,

  `-- @meta: title='Cloud Provider Query Executions' group='Cloud Providers' interval='1 DAY' description='Recent executions for one provider-managed or SQL Console query shape'
-- @cell: column=query_duration_ms type=rag green<100 amber<1000
-- @cell: column=memory_mb type=rag green<20 amber<100
SELECT
    event_time,
    query_id,
    ${CLOUD_ACTIVITY_SOURCE} AS activity_source,
    user,
    type,
    query_duration_ms,
    round(memory_usage / 1048576, 2) AS memory_mb,
    read_rows,
    round(read_bytes / 1048576, 2) AS read_mb,
    result_rows,
    round(result_bytes / 1048576, 2) AS result_mb,
    round(ProfileEvents['OSCPUVirtualTimeMicroseconds'] / 1e6, 3) AS cpu_s,
    substring(query, 1, 220) AS query_text
FROM {{cluster_aware:system.query_log}}
WHERE type = 'QueryFinish'
  AND lower(hex(normalized_query_hash)) = {{drill_value:query_hash | ''}}
  AND event_time > {{time_range}}
ORDER BY event_time DESC
LIMIT 50`,

  `-- @meta: title='Slowest Cloud Provider Queries' group='Cloud Providers' interval='1 DAY' description='Slowest individual internal and SQL Console queries'
-- @link: on=query_id into='Advanced Dashboard#Query Detail by ID'
-- @cell: column=query_duration_ms type=rag green<100 amber<1000
SELECT
    event_time,
    query_id,
    ${CLOUD_ACTIVITY_SOURCE} AS activity_source,
    user,
    query_duration_ms,
    round(memory_usage / 1048576, 2) AS memory_mb,
    round(read_bytes / 1048576, 2) AS read_mb,
    read_rows,
    round(ProfileEvents['OSCPUVirtualTimeMicroseconds'] / 1e6, 3) AS cpu_s,
    substring(query, 1, 160) AS query_preview
FROM {{cluster_aware:system.query_log}}
WHERE type = 'QueryFinish'
  AND ${CLOUD_USER_CONDITION}
  AND event_time > {{time_range}}
  AND {{drill:user | 1=1}}
ORDER BY query_duration_ms DESC
LIMIT 50`,

  `-- @meta: title='Cloud Provider Failed Queries' group='Cloud Providers' interval='7 DAY' description='Internal and SQL Console queries that failed or threw while processing'
-- @link: on=query_id into='Advanced Dashboard#Query Detail by ID'
SELECT
    event_time,
    query_id,
    ${CLOUD_ACTIVITY_SOURCE} AS activity_source,
    user,
    type,
    exception_code,
    substring(exception, 1, 180) AS exception_preview,
    substring(query, 1, 160) AS query_preview
FROM {{cluster_aware:system.query_log}}
WHERE type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing')
  AND ${CLOUD_USER_CONDITION}
  AND event_time > {{time_range}}
  AND {{drill:user | 1=1}}
ORDER BY event_time DESC
LIMIT 50`,

  `-- @meta: title='Cloud SQL Console Queries' group='Cloud Providers' interval='1 DAY' description='Queries attributed to ClickHouse Cloud SQL Console users; useful for separating user-initiated console activity from provider internals'
-- @link: on=query_id into='Advanced Dashboard#Query Detail by ID'
-- @cell: column=query_duration_ms type=rag green<100 amber<1000
SELECT
    event_time,
    query_id,
    user,
    query_kind,
    query_duration_ms,
    round(memory_usage / 1048576, 2) AS memory_mb,
    read_rows,
    round(read_bytes / 1048576, 2) AS read_mb,
    round(ProfileEvents['OSCPUVirtualTimeMicroseconds'] / 1e6, 3) AS cpu_s,
    substring(query, 1, 220) AS query_preview
FROM {{cluster_aware:system.query_log}}
WHERE type = 'QueryFinish'
  AND (user = 'sql-console' OR startsWith(user, 'sql-console:'))
  AND event_time > {{time_range}}
  AND {{drill:user | 1=1}}
ORDER BY event_time DESC
LIMIT 100`,
];

export default queries;
