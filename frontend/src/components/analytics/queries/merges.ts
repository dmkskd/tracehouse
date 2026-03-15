/** Merge monitoring queries — active merges, throughput, errors, pool utilization. */

const queries: string[] = [
  `-- @meta: title='Active Merges' group='Merges' description='Currently running merges with progress, elapsed time, size, and memory usage'
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
-- @chart: type=bar labels=merge values=bytes_per_sec
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
-- @chart: type=line labels=hour values=merge_count style=2d
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
-- @chart: type=bar labels=table values=avg_duration_ms style=2d
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
-- @chart: type=line labels=t values=merge_slots style=2d
SELECT
    toStartOfInterval(event_time, INTERVAL 15 SECOND) AS t,
    avg(CurrentMetric_BackgroundMergesAndMutationsPoolTask) AS merge_slots,
    avg(CurrentMetric_BackgroundMergesAndMutationsPoolSize) AS pool_size
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Merges Running (trend)' group='Merges' interval='1 HOUR' description='Number of concurrent merges over time from metric_log'
-- @chart: type=line labels=t values=merges style=2d
SELECT
    toStartOfInterval(event_time, INTERVAL 15 SECOND) AS t,
    avg(CurrentMetric_Merge) AS merges
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Merge I/O Pressure' group='Merges' interval='1 HOUR' description='Disk read/write bytes attributed to merges over time'
-- @chart: type=area labels=t values=merge_read style=2d
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    sum(ProfileEvent_MergedRows) AS merged_rows,
    formatReadableSize(sum(ProfileEvent_MergedUncompressedBytes)) AS merged_uncompressed
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

];

export default queries;
