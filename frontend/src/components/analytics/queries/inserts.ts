/** Insert monitoring queries — part creation, batch counts, duration, throughput. */

const queries: string[] = [
  `-- @meta: title='New Parts Created' group='Inserts' interval='2 HOUR' description='Frequency of new part creation per minute — early warning for too-many-parts'
-- @chart: type=line group_by=minute value=new_parts style=3d
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
-- @chart: type=line group_by=minute value=nb_bulk_inserts style=2d
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
-- @chart: type=line group_by=minute value=avg_duration style=3d
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
-- @chart: type=area group_by=minute value=total_written_rows style=2d
-- Source: https://clickhouse.com/blog/monitoring-troubleshooting-insert-queries-clickhouse
SELECT
    toStartOfMinute(event_time) AS minute,
    sum(rows) AS total_written_rows,
    formatReadableSize(sum(size_in_bytes)) AS total_bytes_on_disk
FROM {{cluster_aware:system.part_log}}
WHERE event_type = 'NewPart'
  AND event_time > {{time_range}}
GROUP BY minute
ORDER BY minute ASC`,
];

export default queries;
