/** Parts & partitions queries — part counts, hotspots, errors. */

const queries: string[] = [
  `-- @meta: title='MaxPartCountForPartition' group='Parts' interval='1 DAY' description='Trend of the highest part count per partition — early warning for too many parts'
-- @chart: type=area group_by=minute value=avg_max_parts style=2d color=#ef4444
-- @source: https://clickhouse.com/blog/monitoring-troubleshooting-insert-queries-clickhouse
SELECT
    toStartOfMinute(event_time) AS minute,
    avg(value) AS avg_max_parts
FROM {{cluster_aware:system.asynchronous_metric_log}}
WHERE event_time > {{time_range}}
  AND metric = 'MaxPartCountForPartition'
GROUP BY minute
ORDER BY minute ASC`,

  `-- @meta: title='Parts per Partition (hotspots)' group='Parts' description='Tables with more than 1 part per partition — potential merge pressure'
-- @chart: type=bar group_by=table value=parts_per_partition style=2d
SELECT
    concat(database, '.', table) AS table,
    count() AS parts_per_partition,
    partition_id
FROM (
    SELECT database, table, name, any(partition_id) AS partition_id
    FROM {{cluster_aware:system.parts}}
    WHERE active AND database != 'system'
    GROUP BY database, table, name
)
GROUP BY database, table, partition_id
HAVING parts_per_partition > 1
ORDER BY parts_per_partition DESC
LIMIT 30`,

  `-- @meta: title='Part Errors' group='Parts' interval='7 DAY' description='Errors recorded in part_log — useful for diagnosing ingestion issues'
SELECT
    event_date,
    table,
    error,
    count() AS error_count
FROM {{cluster_aware:system.part_log}}
WHERE error != ''
  AND event_date >= toDate({{time_range}})
GROUP BY event_date, table, error
ORDER BY event_date DESC, error_count DESC
LIMIT 50`,
];

export default queries;
