/** Insert monitoring queries — part creation, batch counts, duration, throughput. */

const queries: string[] = [
  `-- @meta: title='New Parts Created' group='Inserts' interval='2 HOUR' description='Frequency of new part creation per minute — early warning for too-many-parts'
-- @chart: type=area group_by=minute value=new_parts style=2d color=#10b981
-- Source: https://clickhouse.com/blog/monitoring-troubleshooting-insert-queries-clickhouse
SELECT
    toStartOfMinute(event_time) AS minute,
    count() AS new_parts,
    sum(rows) AS total_written_rows
FROM {{cluster_aware:system.part_log}}
WHERE event_type = 'NewPart'
  AND event_time > {{time_range}}
GROUP BY minute
ORDER BY minute ASC`,

  `-- @meta: title='Sync Insert Batches' group='Inserts' interval='3 DAY' description='Number of synchronous INSERT bulk requests per minute'
-- @chart: type=area group_by=minute value=nb_bulk_inserts style=2d color=#3b82f6
-- Source: https://clickhouse.com/blog/monitoring-troubleshooting-insert-queries-clickhouse
SELECT
    toStartOfMinute(event_time) AS minute,
    count(*) AS nb_bulk_inserts
FROM {{cluster_aware:system.query_log}}
WHERE query ILIKE '%insert%'
  AND query_kind = 'Insert'
  AND type = 'QueryFinish'
  AND event_time > {{time_range}}
GROUP BY minute
ORDER BY minute ASC`,

  `-- @meta: title='Insert Duration & Batch Count' group='Inserts' interval='2 DAY' description='Average insert duration vs batch count per minute'
-- @chart: type=area group_by=minute value=avg_duration,count_batches style=2d
-- Source: https://clickhouse.com/blog/monitoring-troubleshooting-insert-queries-clickhouse
SELECT
    toStartOfMinute(event_time) AS minute,
    count() AS count_batches,
    avg(query_duration_ms) AS avg_duration
FROM {{cluster_aware:system.query_log}}
WHERE query_kind = 'Insert'
  AND type != 'QueryStart'
  AND event_time > {{time_range}}
GROUP BY minute
ORDER BY minute ASC`,

  `-- @meta: title='Insert Duration Quantiles (hourly)' group='Inserts' interval='2 DAY' description='p50 / p95 / p99 insert duration per hour'
-- @chart: type=grouped_line group_by=hour value=p50,p95,p99 unit=ms style=2d
-- Source: https://clickhouse.com/blog/monitoring-troubleshooting-insert-queries-clickhouse
SELECT
    toStartOfHour(event_time) AS hour,
    count() AS count_batches,
    quantile(0.5)(query_duration_ms) AS p50,
    quantile(0.95)(query_duration_ms) AS p95,
    quantile(0.99)(query_duration_ms) AS p99
FROM {{cluster_aware:system.query_log}}
WHERE query_kind = 'Insert'
  AND type != 'QueryStart'
  AND event_time > {{time_range}}
GROUP BY hour
ORDER BY hour ASC`,

  `-- @meta: title='Written Rows & Bytes' group='Inserts' interval='3 DAY' description='Total rows written and bytes on disk per minute from part_log'
-- @chart: type=area group_by=minute value=total_written_rows,total_bytes_on_disk style=2d
-- Source: https://clickhouse.com/blog/monitoring-troubleshooting-insert-queries-clickhouse
SELECT
    toStartOfMinute(event_time) AS minute,
    sum(rows) AS total_written_rows,
    sum(size_in_bytes) AS total_bytes_on_disk
FROM {{cluster_aware:system.part_log}}
WHERE event_type = 'NewPart'
  AND event_time > {{time_range}}
GROUP BY minute
ORDER BY minute ASC`,

  `-- @meta: title='Top Inserts by Memory' group='Inserts' interval='1 DAY' description='Heaviest INSERT queries by memory and CPU usage'
-- @link: on=query_id into='Query Detail by ID'
-- Source: https://clickhouse.com/blog/monitoring-troubleshooting-insert-queries-clickhouse
SELECT
    event_time,
    initial_query_id AS query_id,
    formatReadableSize(memory_usage) AS memory,
    ProfileEvents['UserTimeMicroseconds'] AS user_cpu_us,
    ProfileEvents['SystemTimeMicroseconds'] AS system_cpu_us,
    substring(replaceRegexpAll(query, '\\n', ' '), 1, 120) AS query_preview
FROM {{cluster_aware:system.query_log}}
WHERE query_kind = 'Insert'
  AND type = 'QueryFinish'
  AND event_time > {{time_range}}
ORDER BY memory_usage DESC
LIMIT 20`,

  `-- @meta: title='MaxPartCountForPartition Trend' group='Inserts' interval='1 DAY' description='Trend of the highest part count in any partition — rising values signal merge pressure'
-- @chart: type=line group_by=minute value=avg_max_parts style=2d
-- Source: https://clickhouse.com/blog/monitoring-troubleshooting-insert-queries-clickhouse
SELECT
    toStartOfMinute(event_time) AS minute,
    avg(value) AS avg_max_parts
FROM {{cluster_aware:system.asynchronous_metric_log}}
WHERE metric = 'MaxPartCountForPartition'
  AND event_time > {{time_range}}
GROUP BY minute
ORDER BY minute ASC`,

  `-- @meta: title='Peak Memory by Part' group='Inserts' interval='3 DAY' description='Daily peak memory usage during part creation'
-- @chart: type=bar group_by=event_date value=max_peak_memory style=2d
-- Source: https://clickhouse.com/blog/monitoring-troubleshooting-insert-queries-clickhouse
SELECT
    event_date,
    argMax(table, peak_memory_usage) AS table,
    argMax(event_time, peak_memory_usage) AS event_time,
    formatReadableSize(max(peak_memory_usage)) AS max_peak_memory_display,
    max(peak_memory_usage) AS max_peak_memory
FROM {{cluster_aware:system.part_log}}
WHERE peak_memory_usage > 0
  AND event_date >= toDate({{time_range}})
GROUP BY event_date
ORDER BY event_date DESC`,

  `-- @meta: title='Part Errors' group='Inserts' interval='7 DAY' description='Errors during part operations — merges, mutations, inserts'
-- @rag: error_count desc 0 1
-- Source: https://clickhouse.com/blog/monitoring-troubleshooting-insert-queries-clickhouse
SELECT
    event_date,
    event_type,
    table,
    errorCodeToName(error) AS error_code,
    count() AS error_count
FROM {{cluster_aware:system.part_log}}
WHERE error > 0
  AND event_date >= toDate({{time_range}})
GROUP BY event_date, event_type, table, error_code
ORDER BY event_date DESC, error_count DESC`,
];

export default queries;
