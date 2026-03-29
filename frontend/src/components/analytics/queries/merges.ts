/** Merge monitoring queries — active merges, throughput, errors, pool utilization. */

const queries: string[] = [
  `-- @meta: title='Active Merges' group='Merges' description='Currently running merges with progress, elapsed time, size, and memory usage'
-- @cell: column=progress_pct type=gauge max=100 unit=%
SELECT
    database,
    table,
    round(elapsed, 1) AS elapsed_sec,
    round(progress * 100, 1) AS progress_pct,
    num_parts,
    is_mutation,
    formatReadableSize(total_size_bytes_compressed) AS total_size,
    formatReadableSize(bytes_read_uncompressed) AS bytes_read,
    formatReadableSize(bytes_written_uncompressed) AS bytes_written,
    formatReadableSize(memory_usage) AS memory,
    rows_read,
    rows_written
FROM system.merges
ORDER BY elapsed DESC`,

  `-- @meta: title='Merge Throughput (bytes/sec)' group='Merges' description='Estimated merge read throughput for each active merge'
-- @chart: type=bar group_by=merge value=bytes_per_sec style=2d
-- @cell: column=progress_pct type=gauge max=100 unit=%
SELECT
    concat(database, '.', table, ' #', toString(num_parts), 'p') AS merge,
    round(elapsed, 0) AS elapsed_sec,
    round(progress * 100, 1) AS progress_pct,
    formatReadableSize(bytes_read_uncompressed) AS read,
    if(elapsed > 0, round(bytes_read_uncompressed / elapsed, 0), 0) AS bytes_per_sec,
    formatReadableSize(if(elapsed > 0, bytes_read_uncompressed / elapsed, 0)) AS throughput
FROM system.merges
ORDER BY bytes_per_sec DESC`,

  `-- @meta: title='Merge Events Over Time' group='Merges' interval='1 DAY' description='Completed merge events from part_log — duration and size per hour'
-- @chart: type=line group_by=hour value=merge_count style=2d
SELECT
    toStartOfHour(event_time) AS hour,
    count() AS merge_count,
    round(avg(duration_ms), 0) AS avg_duration_ms,
    round(quantile(0.95)(duration_ms), 0) AS p95_duration_ms,
    formatReadableSize(sum(size_in_bytes)) AS total_size
FROM {{cluster_aware:system.part_log}}
WHERE event_type = 'MergeParts'
  AND event_time > {{time_range}}
GROUP BY hour
ORDER BY hour ASC`,

  `-- @meta: title='Merge Errors' group='Merges' interval='7 DAY' description='Failed or errored merge operations from part_log'
SELECT
    event_time,
    database,
    table,
    partition_id,
    duration_ms,
    error,
    exception
FROM {{cluster_aware:system.part_log}}
WHERE event_type = 'MergeParts'
  AND exception != ''
  AND event_time > {{time_range}}
ORDER BY event_time DESC
LIMIT 50`,

  `-- @meta: title='Merge Duration by Table' group='Merges' interval='1 DAY' description='Average and p95 merge duration per table'
-- @chart: type=bar group_by=table value=avg_duration_ms style=2d
SELECT
    concat(database, '.', table) AS table,
    count() AS merge_count,
    round(avg(duration_ms), 0) AS avg_duration_ms,
    round(quantile(0.95)(duration_ms), 0) AS p95_duration_ms,
    formatReadableSize(sum(size_in_bytes)) AS total_size_merged
FROM {{cluster_aware:system.part_log}}
WHERE event_type = 'MergeParts'
  AND event_time > {{time_range}}
GROUP BY database, table
ORDER BY avg_duration_ms DESC
LIMIT 20`,

  `-- @meta: title='Background Pool Utilization' group='Merges' interval='1 HOUR' description='Background merge/mutation pool slots in use over time'
-- @chart: type=line group_by=t value=merge_slots style=2d
SELECT
    toStartOfInterval(event_time, INTERVAL 15 SECOND) AS t,
    avg(CurrentMetric_BackgroundMergesAndMutationsPoolTask) AS merge_slots,
    avg(CurrentMetric_BackgroundMergesAndMutationsPoolSize) AS pool_size
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Merges Running (trend)' group='Merges' interval='1 HOUR' description='Number of concurrent merges over time from metric_log'
-- @chart: type=line group_by=t value=merges style=2d
SELECT
    toStartOfInterval(event_time, INTERVAL 15 SECOND) AS t,
    avg(CurrentMetric_Merge) AS merges
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Merge I/O Pressure' group='Merges' interval='1 HOUR' description='Disk read/write bytes attributed to merges over time'
-- @chart: type=area group_by=t value=merge_read style=2d
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    sum(ProfileEvent_MergedRows) AS merged_rows,
    formatReadableSize(sum(ProfileEvent_MergedUncompressedBytes)) AS merged_uncompressed
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Fetch vs Merge Ratio' group='Merges' interval='1 DAY' description='Ratio of replica fetches (DownloadPart) to local merges over time — high fetch% means replicas rely on others to merge (ReplicatedMergeTree only)'
-- @chart: type=line group_by=hour value=merges,fetches style=2d
-- @cell: column=fetch_pct type=gauge max=100 unit=%
SELECT
    toStartOfHour(event_time) AS hour,
    countIf(event_type = 'MergeParts') AS merges,
    countIf(event_type = 'DownloadPart') AS fetches,
    round(countIf(event_type = 'DownloadPart') * 100.0
        / greatest(count(), 1), 1) AS fetch_pct
FROM {{cluster_aware:system.part_log}}
WHERE event_type IN ('MergeParts', 'DownloadPart')
  AND event_time > {{time_range}}
GROUP BY hour
ORDER BY hour ASC`,

  `-- @meta: title='Fetch vs Merge Duration' group='Merges' interval='1 DAY' description='Compare average and p95 duration of fetches vs local merges — fetches slower than merges may indicate network bottlenecks (ReplicatedMergeTree only)'
-- @chart: type=bar group_by=event_type value=avg_ms style=2d
SELECT
    event_type,
    count() AS ops,
    round(avg(duration_ms)) AS avg_ms,
    round(quantile(0.95)(duration_ms)) AS p95_ms,
    formatReadableSize(avg(size_in_bytes)) AS avg_size
FROM {{cluster_aware:system.part_log}}
WHERE event_type IN ('MergeParts', 'DownloadPart')
  AND event_time > {{time_range}}
GROUP BY event_type`,

  `-- @meta: title='Fetch Pool Utilization' group='Merges' interval='1 HOUR' description='Background fetch pool slots in use over time — saturation means replicas are bottlenecked on fetching parts (ReplicatedMergeTree only)'
-- @chart: type=line group_by=t value=fetch_slots,fetch_pool_size style=2d
SELECT
    toStartOfInterval(event_time, INTERVAL 15 SECOND) AS t,
    avg(CurrentMetric_BackgroundFetchesPoolTask) AS fetch_slots,
    avg(CurrentMetric_BackgroundFetchesPoolSize) AS fetch_pool_size
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Replica Merge Imbalance' group='Merges' interval='1 DAY' description='Per-replica breakdown of local merges vs fetches — asymmetry indicates one node is doing most merge work (ReplicatedMergeTree only)'
-- @chart: type=bar group_by=replica value=fetch_pct style=2d
-- @cell: column=fetch_pct type=gauge max=100 unit=%
SELECT
    hostName() AS replica,
    countIf(event_type = 'MergeParts') AS local_merges,
    countIf(event_type = 'DownloadPart') AS fetches,
    round(countIf(event_type = 'DownloadPart') * 100.0
        / greatest(count(), 1), 1) AS fetch_pct,
    formatReadableSize(sumIf(size_in_bytes, event_type = 'MergeParts')) AS merged_bytes,
    formatReadableSize(sumIf(size_in_bytes, event_type = 'DownloadPart')) AS fetched_bytes
FROM {{cluster_aware:system.part_log}}
WHERE event_type IN ('MergeParts', 'DownloadPart')
  AND event_time > {{time_range}}
GROUP BY replica
ORDER BY fetch_pct DESC`,

  `-- @meta: title='Fetch vs Merge by Table & Size' group='Merges' interval='1 DAY' description='Per-table breakdown of fetch vs local merge by part size — shows which tables and size ranges rely on fetching (ReplicatedMergeTree only)'
-- @chart: type=bar group_by=size_bucket value=fetch_pct style=2d
-- @cell: column=fetch_pct type=gauge max=100 unit=%
SELECT
    database || '.' || table AS tbl,
    multiIf(
        size_in_bytes < 1048576, '< 1 MB',
        size_in_bytes < 2097152, '1-2 MB',
        size_in_bytes < 5242880, '2-5 MB',
        size_in_bytes < 10485760, '5-10 MB',
        size_in_bytes < 104857600, '10-100 MB',
        size_in_bytes < 1073741824, '100 MB-1 GB',
        '> 1 GB'
    ) AS size_bucket,
    countIf(event_type = 'MergeParts') AS local_merges,
    countIf(event_type = 'DownloadPart') AS fetches,
    round(countIf(event_type = 'DownloadPart') * 100.0
        / greatest(count(), 1), 1) AS fetch_pct,
    formatReadableSize(sumIf(size_in_bytes, event_type = 'MergeParts')) AS merged_bytes,
    formatReadableSize(sumIf(size_in_bytes, event_type = 'DownloadPart')) AS fetched_bytes
FROM {{cluster_aware:system.part_log}}
WHERE event_type IN ('MergeParts', 'DownloadPart')
  AND event_time > {{time_range}}
GROUP BY tbl, size_bucket
HAVING fetches > 0
ORDER BY tbl, min(size_in_bytes)`,

  `-- @meta: title='Replica Merge Imbalance by Table' group='Merges' interval='1 DAY' description='Per-replica, per-table merge vs fetch breakdown — reveals which replica is the primary merger for each table (ReplicatedMergeTree only)'
-- @chart: type=bar group_by=replica value=local_merges,fetches style=2d
-- @cell: column=fetch_pct type=gauge max=100 unit=%
SELECT
    hostName() AS replica,
    database || '.' || table AS tbl,
    countIf(event_type = 'MergeParts') AS local_merges,
    countIf(event_type = 'DownloadPart') AS fetches,
    round(countIf(event_type = 'DownloadPart') * 100.0
        / greatest(count(), 1), 1) AS fetch_pct
FROM {{cluster_aware:system.part_log}}
WHERE event_type IN ('MergeParts', 'DownloadPart')
  AND database NOT LIKE 'system%'
  AND event_time > {{time_range}}
GROUP BY replica, database, table
HAVING fetches > 0
ORDER BY tbl, replica`,

  `-- @meta: title='Replication Queue Backlog' group='Merges' description='Current replication queue breakdown — GET_PART=fetch, MERGE_PARTS=merge, MUTATE_PART=mutation. Retries and errors signal cluster health issues (ReplicatedMergeTree only)'
SELECT
    type,
    count() AS queued,
    countIf(is_currently_executing) AS executing,
    countIf(num_tries > 1) AS retried,
    max(num_tries) AS max_tries,
    countIf(last_exception != '') AS with_errors
FROM {{cluster_aware:system.replication_queue}}
WHERE type IN ('GET_PART', 'MERGE_PARTS', 'MUTATE_PART')
GROUP BY type
ORDER BY queued DESC`,

];

export default queries;
