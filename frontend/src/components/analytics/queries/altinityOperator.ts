/**
 * Altinity ClickHouse Operator dashboard queries — derived from the
 * community Grafana dashboard "ClickHouse Operator" (ID 12163).
 *
 * Original dashboard: https://grafana.com/grafana/dashboards/12163
 *
 * The operator exporter surfaces three metric families:
 *   chi_clickhouse_event_*  → system.events / system.metric_log ProfileEvent_* columns
 *   chi_clickhouse_metric_* → system.metrics / system.metric_log CurrentMetric_* columns
 *   chi_clickhouse_table_*  → system.parts / system.mutations
 *
 * All metrics are available natively in ClickHouse system tables —
 * no Prometheus intermediary required.
 */

const queries: string[] = [

  // ─── Overview ────────────────────────────────────────────────────────

  `-- @meta: title='Uptime & Version' group='Grafana Imports' description='Current server uptime and ClickHouse version'
-- @source: https://grafana.com/grafana/dashboards/12163
SELECT
    uptime() AS uptime_seconds,
    formatReadableTimeDelta(uptime()) AS uptime_human,
    version() AS version`,

  `-- @meta: title='Connections (current)' group='Grafana Imports' description='Current TCP, HTTP, and interserver connections'
-- @source: https://grafana.com/grafana/dashboards/12163
SELECT
    (SELECT value FROM system.metrics WHERE metric = 'TCPConnection') AS tcp,
    (SELECT value FROM system.metrics WHERE metric = 'HTTPConnection') AS http,
    (SELECT value FROM system.metrics WHERE metric = 'InterserverConnection') AS interserver`,

  `-- @meta: title='Connections Trend' group='Grafana Imports' interval='1 HOUR' description='TCP, HTTP, and interserver connections over time'
-- @chart: type=grouped_line group_by=t value=tcp,http,interserver style=2d
-- @source: https://grafana.com/grafana/dashboards/12163
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(CurrentMetric_TCPConnection) AS tcp,
    avg(CurrentMetric_HTTPConnection) AS http,
    avg(CurrentMetric_InterserverConnection) AS interserver
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Running Queries' group='Grafana Imports' interval='1 HOUR' description='Number of currently executing queries over time'
-- @chart: type=area group_by=t value=running style=2d color=#3b82f6
-- @drill: on=t into='Grafana Imports#Queries at Time'
-- @source: https://grafana.com/grafana/dashboards/12163
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(CurrentMetric_Query) AS running
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  // ─── Errors ──────────────────────────────────────────────────────────

  `-- @meta: title='Query Errors by Type' group='Grafana Imports' interval='1 HOUR' description='Failed SELECT, INSERT, and other query errors per minute'
-- @chart: type=grouped_line group_by=t value=select_err,insert_err,other_err style=2d
-- @drill: on=t into='Grafana Imports#Failed Queries at Time'
-- @source: https://grafana.com/grafana/dashboards/12163
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    sum(ProfileEvent_FailedSelectQuery) AS select_err,
    sum(ProfileEvent_FailedInsertQuery) AS insert_err,
    sum(ProfileEvent_FailedQuery) - sum(ProfileEvent_FailedSelectQuery) - sum(ProfileEvent_FailedInsertQuery) AS other_err
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='ZooKeeper Hardware Exceptions' group='Grafana Imports' interval='1 HOUR' description='ZooKeeper hardware exceptions — network or connection failures to Keeper'
-- @chart: type=bar group_by=t value=hw_exceptions style=2d color=#ef4444
-- @cell: column=hw_exceptions type=rag green<1 amber<5
-- @source: https://grafana.com/grafana/dashboards/12163
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    sum(ProfileEvent_ZooKeeperHardwareExceptions) AS hw_exceptions
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='DNS Errors' group='Grafana Imports' interval='1 HOUR' description='DNS resolution errors — can cause connectivity issues to replicas and ZooKeeper'
-- @chart: type=bar group_by=t value=dns_errors style=2d color=#f97316
-- @source: https://grafana.com/grafana/dashboards/12163
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    sum(ProfileEvent_DNSError) AS dns_errors
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  // ─── Queries ─────────────────────────────────────────────────────────

  `-- @meta: title='Query Rate by Type' group='Grafana Imports' interval='1 HOUR' description='SELECT, INSERT, and total query rate per minute'
-- @chart: type=grouped_line group_by=t value=selects,inserts,total style=2d
-- @drill: on=t into='Grafana Imports#Queries at Time'
-- @source: https://grafana.com/grafana/dashboards/12163
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    sum(ProfileEvent_SelectQuery) AS selects,
    sum(ProfileEvent_InsertQuery) AS inserts,
    sum(ProfileEvent_Query) AS total
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Read Rows/sec' group='Grafana Imports' interval='1 HOUR' description='Read rows per second — measures read amplification across all queries'
-- @chart: type=area group_by=t value=read_rows style=2d color=#a78bfa
-- @drill: on=t into='Grafana Imports#Heavy Readers at Time'
-- @source: https://grafana.com/grafana/dashboards/12163
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(ProfileEvent_SelectedRows) AS read_rows
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Read Bytes/sec' group='Grafana Imports' interval='1 HOUR' description='Read bytes per second (compressed, from disk/network)'
-- @chart: type=area group_by=t value=read_bytes style=2d color=#8b5cf6
-- @drill: on=t into='Grafana Imports#Heavy Readers at Time'
-- @source: https://grafana.com/grafana/dashboards/12163
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(ProfileEvent_SelectedBytes) AS read_bytes
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  // ─── Inserts ─────────────────────────────────────────────────────────

  `-- @meta: title='Insert Rate' group='Grafana Imports' interval='1 HOUR' description='INSERT query rate per minute'
-- @chart: type=area group_by=t value=inserts style=2d color=#3fb950
-- @drill: on=t into='Grafana Imports#Queries at Time'
-- @source: https://grafana.com/grafana/dashboards/12163
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    sum(ProfileEvent_InsertQuery) AS inserts
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Inserted Rows/sec (Altinity)' group='Grafana Imports' interval='1 HOUR' description='Rows inserted per second across all tables'
-- @chart: type=area group_by=t value=rows_sec style=2d color=#22c55e
-- @drill: on=t into='Grafana Imports#Queries at Time'
-- @source: https://grafana.com/grafana/dashboards/12163
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(ProfileEvent_InsertedRows) AS rows_sec
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Inserted Bytes/sec (Altinity)' group='Grafana Imports' interval='1 HOUR' description='Bytes inserted per second (uncompressed)'
-- @chart: type=area group_by=t value=bytes_sec style=2d color=#10b981
-- @source: https://grafana.com/grafana/dashboards/12163
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(ProfileEvent_InsertedBytes) AS bytes_sec
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Delayed & Rejected Inserts' group='Grafana Imports' interval='1 HOUR' description='Inserts throttled (delayed) or rejected (TOO_MANY_PARTS) — indicates part pressure'
-- @chart: type=grouped_line group_by=t value=delayed,rejected style=2d
-- @cell: column=rejected type=rag green<1 amber<5
-- @drill: on=t into='Grafana Imports#Rejected Inserts at Time'
-- @source: https://grafana.com/grafana/dashboards/12163
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    sum(ProfileEvent_DelayedInserts) AS delayed,
    sum(ProfileEvent_RejectedInserts) AS rejected
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  // ─── Replication & ZooKeeper ─────────────────────────────────────────

  `-- @meta: title='Replica Lag (max)' group='Grafana Imports' interval='1 HOUR' description='Maximum absolute_delay across all replicated tables — growing lag signals replication falling behind'
-- @chart: type=area group_by=t value=max_delay style=2d color=#ef4444
-- @cell: column=max_delay type=rag green<10 amber<300
-- @source: https://grafana.com/grafana/dashboards/12163
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    max(value) AS max_delay
FROM {{cluster_aware:system.asynchronous_metric_log}}
WHERE event_time > {{time_range}}
  AND metric = 'ReplicasMaxAbsoluteDelay'
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Replication Queue Size (Altinity)' group='Grafana Imports' interval='1 HOUR' description='Total replication queue depth over time'
-- @chart: type=area group_by=t value=queue_size style=2d color=#f59e0b
-- @source: https://grafana.com/grafana/dashboards/12163
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    max(value) AS queue_size
FROM {{cluster_aware:system.asynchronous_metric_log}}
WHERE event_time > {{time_range}}
  AND metric = 'ReplicasMaxQueueSize'
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='ZooKeeper Request Rate' group='Grafana Imports' interval='1 HOUR' description='ZooKeeper transaction and watch rate per minute — high rates may indicate schema pressure'
-- @chart: type=grouped_line group_by=t value=transactions,watches style=2d
-- @source: https://grafana.com/grafana/dashboards/12163
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    sum(ProfileEvent_ZooKeeperTransactions) AS transactions,
    sum(ProfileEvent_ZooKeeperWatchResponse) AS watches
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='ZooKeeper Wait Time (avg)' group='Grafana Imports' interval='1 HOUR' description='Average ZooKeeper wait time per minute — high values indicate Keeper contention'
-- @chart: type=area group_by=t value=avg_wait_ms style=2d color=#8b5cf6 unit=ms
-- @source: https://grafana.com/grafana/dashboards/12163
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(ProfileEvent_ZooKeeperWaitMicroseconds) / 1000 AS avg_wait_ms
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='ZooKeeper Sessions' group='Grafana Imports' interval='1 HOUR' description='Active ZooKeeper sessions — should be stable; drops indicate connectivity issues'
-- @chart: type=line group_by=t value=sessions style=2d color=#06b6d4
-- @source: https://grafana.com/grafana/dashboards/12163
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(CurrentMetric_ZooKeeperSession) AS sessions
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  // ─── Merges ──────────────────────────────────────────────────────────

  `-- @meta: title='Merge Rate (Altinity)' group='Grafana Imports' interval='1 HOUR' description='Merge operations completed per minute'
-- @chart: type=area group_by=t value=merges style=2d color=#e3b341
-- @drill: on=t into='Grafana Imports#Part Activity at Time'
-- @source: https://grafana.com/grafana/dashboards/12163
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    sum(ProfileEvent_Merge) AS merges
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Merged Rows/sec (Altinity)' group='Grafana Imports' interval='1 HOUR' description='Rows processed by background merges per second'
-- @chart: type=area group_by=t value=rows_sec style=2d color=#f0883e
-- @source: https://grafana.com/grafana/dashboards/12163
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(ProfileEvent_MergedRows) AS rows_sec
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Active Merges & Mutations (Altinity)' group='Grafana Imports' interval='1 HOUR' description='Currently running merge and mutation threads'
-- @chart: type=grouped_line group_by=t value=merges,mutations style=2d
-- @source: https://grafana.com/grafana/dashboards/12163
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(CurrentMetric_Merge) AS merges,
    avg(CurrentMetric_PartMutation) AS mutations
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  // ─── Parts ───────────────────────────────────────────────────────────

  `-- @meta: title='Active Parts (Altinity)' group='Grafana Imports' interval='1 HOUR' description='Total active MergeTree parts over time — correlates with query planning cost'
-- @chart: type=area group_by=t value=parts style=2d color=#f0883e
-- @source: https://grafana.com/grafana/dashboards/12163
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(value) AS parts
FROM {{cluster_aware:system.asynchronous_metric_log}}
WHERE event_time > {{time_range}}
  AND metric = 'TotalPartsOfMergeTreeTables'
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Max Parts Per Partition (Altinity)' group='Grafana Imports' interval='1 HOUR' description='Highest part count in any single partition — above 300 triggers insert delays, above 600 causes rejections'
-- @chart: type=area group_by=t value=max_parts style=2d color=#ef4444
-- @cell: column=max_parts type=rag green<150 amber<300
-- @source: https://grafana.com/grafana/dashboards/12163
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    max(value) AS max_parts
FROM {{cluster_aware:system.asynchronous_metric_log}}
WHERE event_time > {{time_range}}
  AND metric = 'MaxPartCountForPartition'
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Parts by Table (top 10)' group='Grafana Imports' description='Active part counts per table — identifies tables contributing to merge pressure'
-- @chart: type=bar group_by=tbl value=part_count orientation=horizontal
-- @source: https://grafana.com/grafana/dashboards/12163
SELECT
    concat(database, '.', table) AS tbl,
    count() AS part_count,
    formatReadableSize(sum(bytes_on_disk)) AS disk_size,
    max(rows) AS max_rows_in_part
FROM {{cluster_aware:system.parts}}
WHERE active AND database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')
GROUP BY database, table
ORDER BY part_count DESC
LIMIT 10`,

  `-- @meta: title='Detached Parts' group='Grafana Imports' description='Detached parts — parts that failed to attach or were manually detached. Non-zero typically requires investigation.'
-- @cell: column=detached_count type=rag green<1 amber<5
-- @source: https://grafana.com/grafana/dashboards/12163
SELECT
    database,
    table,
    count() AS detached_count,
    groupArray(reason) AS reasons,
    min(disk) AS disk
FROM system.detached_parts
GROUP BY database, table
ORDER BY detached_count DESC`,

  // ─── Memory ──────────────────────────────────────────────────────────

  `-- @meta: title='Memory Tracking (Altinity)' group='Grafana Imports' interval='1 HOUR' description='Total memory tracked by ClickHouse allocator over time'
-- @chart: type=area group_by=t value=memory_bytes style=2d color=#a855f7
-- @source: https://grafana.com/grafana/dashboards/12163
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(CurrentMetric_MemoryTracking) AS memory_bytes
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Memory by Subsystem' group='Grafana Imports' interval='1 HOUR' description='Memory breakdown: queries, merges, background moves, and fetches'
-- @chart: type=grouped_line group_by=t value=queries,merges,moves,fetches style=2d
-- @source: https://grafana.com/grafana/dashboards/12163
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(CurrentMetric_MemoryTracking) - avg(CurrentMetric_MergesMutationsMemoryTracking) AS queries,
    avg(CurrentMetric_MergesMutationsMemoryTracking) AS merges,
    avg(CurrentMetric_BackgroundMovePoolTask) AS moves,
    avg(CurrentMetric_BackgroundFetchesPoolTask) AS fetches
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  // ─── Disk & Tables ───────────────────────────────────────────────────

  `-- @meta: title='Total MergeTree Bytes' group='Grafana Imports' interval='1 HOUR' description='Total disk space used by MergeTree tables over time'
-- @chart: type=area group_by=t value=total_bytes style=2d color=#06b6d4
-- @source: https://grafana.com/grafana/dashboards/12163
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(value) AS total_bytes
FROM {{cluster_aware:system.asynchronous_metric_log}}
WHERE event_time > {{time_range}}
  AND metric = 'TotalBytesOfMergeTreeTables'
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Total MergeTree Rows' group='Grafana Imports' interval='1 HOUR' description='Total rows across all MergeTree tables over time'
-- @chart: type=area group_by=t value=total_rows style=2d color=#14b8a6
-- @source: https://grafana.com/grafana/dashboards/12163
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(value) AS total_rows
FROM {{cluster_aware:system.asynchronous_metric_log}}
WHERE event_time > {{time_range}}
  AND metric = 'TotalRowsOfMergeTreeTables'
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Table Sizes (top 20)' group='Grafana Imports' description='Largest tables by disk usage — compressed and uncompressed sizes, compression ratio'
-- @chart: type=bar group_by=tbl value=compressed orientation=horizontal
-- @source: https://grafana.com/grafana/dashboards/12163
SELECT
    concat(database, '.', table) AS tbl,
    sum(bytes_on_disk) AS compressed,
    sum(data_uncompressed_bytes) AS uncompressed,
    round(sum(data_uncompressed_bytes) / greatest(sum(bytes_on_disk), 1), 2) AS compression_ratio,
    sum(rows) AS total_rows,
    count() AS parts
FROM {{cluster_aware:system.parts}}
WHERE active AND database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')
GROUP BY database, table
ORDER BY compressed DESC
LIMIT 20`,

  // ─── Background & Mutations ──────────────────────────────────────────

  `-- @meta: title='Background Pool Utilization (Altinity)' group='Grafana Imports' interval='1 HOUR' description='Background pool thread utilization — merge, move, fetch, common, and schedule pools'
-- @chart: type=grouped_line group_by=t value=merge_pool,move_pool,fetch_pool,common_pool,schedule_pool style=2d
-- @source: https://grafana.com/grafana/dashboards/12163
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(CurrentMetric_BackgroundMergesAndMutationsPoolTask) AS merge_pool,
    avg(CurrentMetric_BackgroundMovePoolTask) AS move_pool,
    avg(CurrentMetric_BackgroundFetchesPoolTask) AS fetch_pool,
    avg(CurrentMetric_BackgroundCommonPoolTask) AS common_pool,
    avg(CurrentMetric_BackgroundSchedulePoolTask) AS schedule_pool
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Active Mutations (Altinity)' group='Grafana Imports' description='Currently running and queued mutations by table'
-- @cell: column=is_done type=rag green=1 amber=0
-- @source: https://grafana.com/grafana/dashboards/12163
SELECT
    database,
    table,
    mutation_id,
    command,
    create_time,
    is_done,
    parts_to_do,
    latest_fail_reason
FROM {{cluster_aware:system.mutations}}
WHERE NOT is_done
ORDER BY create_time ASC`,

  // ─── CPU ─────────────────────────────────────────────────────────────

  `-- @meta: title='CPU User Time' group='Grafana Imports' interval='1 HOUR' description='Normalized CPU user time — fraction of available CPU cores spent in user space'
-- @chart: type=area group_by=t value=user_cpu style=2d color=#3b82f6
-- @source: https://grafana.com/grafana/dashboards/12163
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(value) AS user_cpu
FROM {{cluster_aware:system.asynchronous_metric_log}}
WHERE event_time > {{time_range}}
  AND metric = 'OSUserTimeNormalized'
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='CPU System Time' group='Grafana Imports' interval='1 HOUR' description='Normalized CPU system (kernel) time — high values may indicate I/O syscall overhead'
-- @chart: type=area group_by=t value=system_cpu style=2d color=#ef4444
-- @source: https://grafana.com/grafana/dashboards/12163
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(value) AS system_cpu
FROM {{cluster_aware:system.asynchronous_metric_log}}
WHERE event_time > {{time_range}}
  AND metric = 'OSSystemTimeNormalized'
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='CPU IO Wait' group='Grafana Imports' interval='1 HOUR' description='Normalized IO wait time — fraction of CPU time waiting for disk I/O'
-- @chart: type=area group_by=t value=iowait style=2d color=#f59e0b
-- @source: https://grafana.com/grafana/dashboards/12163
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(value) AS iowait
FROM {{cluster_aware:system.asynchronous_metric_log}}
WHERE event_time > {{time_range}}
  AND metric = 'OSIOWaitTimeNormalized'
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='CPU Breakdown' group='Grafana Imports' interval='1 HOUR' description='Full CPU time breakdown: user, system, IO wait, idle — all normalized to available cores'
-- @chart: type=area group_by=t value=value series=op style=2d
-- @source: https://grafana.com/grafana/dashboards/12163
SELECT t, avg(val) AS value, op FROM (
    SELECT toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
        'user' AS op, avg(value) AS val
    FROM {{cluster_aware:system.asynchronous_metric_log}}
    WHERE event_time > {{time_range}} AND metric = 'OSUserTimeNormalized'
    GROUP BY t
      UNION ALL
    SELECT toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
        'system' AS op, avg(value) AS val
    FROM {{cluster_aware:system.asynchronous_metric_log}}
    WHERE event_time > {{time_range}} AND metric = 'OSSystemTimeNormalized'
    GROUP BY t
      UNION ALL
    SELECT toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
        'iowait' AS op, avg(value) AS val
    FROM {{cluster_aware:system.asynchronous_metric_log}}
    WHERE event_time > {{time_range}} AND metric = 'OSIOWaitTimeNormalized'
    GROUP BY t
      UNION ALL
    SELECT toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
        'idle' AS op, avg(value) AS val
    FROM {{cluster_aware:system.asynchronous_metric_log}}
    WHERE event_time > {{time_range}} AND metric = 'OSIdleTimeNormalized'
    GROUP BY t
)
GROUP BY t, op
ORDER BY t ASC, op`,

  // ─── Network & I/O ───────────────────────────────────────────────────

  `-- @meta: title='Network Bytes (send/receive)' group='Grafana Imports' interval='1 HOUR' description='Network throughput — bytes sent and received per second'
-- @chart: type=grouped_line group_by=t value=sent,received style=2d
-- @source: https://grafana.com/grafana/dashboards/12163
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(ProfileEvent_NetworkSendBytes) AS sent,
    avg(ProfileEvent_NetworkReceiveBytes) AS received
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Disk Read/Write Bytes' group='Grafana Imports' interval='1 HOUR' description='Physical disk read and write bytes per second'
-- @chart: type=grouped_line group_by=t value=read_bytes,write_bytes style=2d
-- @source: https://grafana.com/grafana/dashboards/12163
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(ProfileEvent_OSReadBytes) AS read_bytes,
    avg(ProfileEvent_OSWriteBytes) AS write_bytes
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Disk Read/Write IOps' group='Grafana Imports' interval='1 HOUR' description='Disk I/O operations per second — read and write'
-- @chart: type=grouped_line group_by=t value=reads,writes style=2d
-- @source: https://grafana.com/grafana/dashboards/12163
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(ProfileEvent_OSReadChars) AS reads,
    avg(ProfileEvent_OSWriteChars) AS writes
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,
];

export default queries;
