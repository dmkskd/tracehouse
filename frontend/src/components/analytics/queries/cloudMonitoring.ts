/**
 * Cloud Monitoring queries — derived from the official ClickHouse
 * "Prom-Exporter Instance Dashboard v2" (clickhouse-mixin).
 *
 * Original dashboard: https://github.com/ClickHouse/clickhouse-mixin
 * Grafana Marketplace: https://grafana.com/grafana/dashboards/23415
 *
 * The Prometheus exporter surfaces three metric families:
 *   ClickHouseAsyncMetrics_*  → system.asynchronous_metric_log
 *   ClickHouseMetrics_*       → system.metric_log (CurrentMetric_* columns)
 *   ClickHouseProfileEvents_* → system.metric_log (ProfileEvent_* columns)
 *
 * All metrics are available natively in ClickHouse system tables —
 * no Prometheus intermediary required. These queries reproduce the same
 * panels using direct SQL against the source tables.
 */

const queries: string[] = [

  // ─── Server Health ───────────────────────────────────────────────────

  `-- @meta: title='CPU Usage %' group='Grafana Imports' interval='1 HOUR' description='Normalized CPU usage (user + system) as a percentage — prefers CGroup metrics (Cloud/k8s), falls back to OS metrics'
-- @chart: type=area group_by=t value=cpu_pct style=2d color=#3b82f6 unit=%
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(value) * 100 AS cpu_pct
FROM {{cluster_aware:system.asynchronous_metric_log}}
WHERE event_time > {{time_range}}
  AND metric IN ('OSUserTimeNormalized', 'OSSystemTimeNormalized')
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Memory Usage %' group='Grafana Imports' interval='1 HOUR' description='MemoryTracking as a percentage of total OS memory'
-- @chart: type=area group_by=t value=mem_pct style=2d color=#a855f7 unit=%
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(m.event_time, INTERVAL 1 MINUTE) AS t,
    avg(m.CurrentMetric_MemoryTracking) / avg(a.value) * 100 AS mem_pct
FROM {{cluster_aware:system.metric_log}} m
INNER JOIN {{cluster_aware:system.asynchronous_metric_log}} a
  ON toStartOfInterval(a.event_time, INTERVAL 1 MINUTE) = toStartOfInterval(m.event_time, INTERVAL 1 MINUTE)
  AND a.metric = 'OSMemoryTotal'
WHERE m.event_time > {{time_range}}
  AND a.event_time > {{time_range}}
  AND a.value > 0
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Failed Select %' group='Grafana Imports' interval='1 HOUR' description='Percentage of SELECT queries that failed — spikes indicate query errors or timeouts'
-- @chart: type=area group_by=t value=fail_pct style=2d color=#ef4444 unit=%
-- @cell: column=fail_pct type=rag green<1 amber<5
-- @drill: on=t into='Grafana Imports#Failed Queries at Time'
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    if(sum(ProfileEvent_SelectQuery) > 0,
       sum(ProfileEvent_FailedSelectQuery) / sum(ProfileEvent_SelectQuery) * 100, 0) AS fail_pct
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Failed Insert %' group='Grafana Imports' interval='1 HOUR' description='Percentage of INSERT queries (sync + async) that failed'
-- @chart: type=area group_by=t value=fail_pct style=2d color=#f97316 unit=%
-- @cell: column=fail_pct type=rag green<1 amber<5
-- @drill: on=t into='Grafana Imports#Failed Queries at Time'
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    if(sum(ProfileEvent_InsertQuery) + sum(ProfileEvent_AsyncInsertQuery) > 0,
       (sum(ProfileEvent_FailedInsertQuery) + sum(ProfileEvent_FailedAsyncInsertQuery))
       / (sum(ProfileEvent_InsertQuery) + sum(ProfileEvent_AsyncInsertQuery)) * 100, 0) AS fail_pct
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Error Log Rate' group='Grafana Imports' interval='1 HOUR' description='Rate of errors written to the server log — correlate with failed queries and resource pressure'
-- @chart: type=bar group_by=t value=errors style=2d color=#ef4444
-- @drill: on=t into='Grafana Imports#Failed Queries at Time'
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    sum(ProfileEvent_LogError) AS errors
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Network I/O' group='Grafana Imports' interval='1 HOUR' description='Network receive and send bytes per second'
-- @chart: type=grouped_line group_by=t value=recv_bytes,send_bytes style=2d
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(ProfileEvent_NetworkReceiveBytes) AS recv_bytes,
    avg(ProfileEvent_NetworkSendBytes) AS send_bytes
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='TCP Connections' group='Grafana Imports' interval='1 HOUR' description='Active TCP connections to the server'
-- @chart: type=area group_by=t value=connections style=2d color=#06b6d4
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(CurrentMetric_TCPConnection) AS connections
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Memory Limit Exceeded' group='Grafana Imports' interval='1 HOUR' description='Queries killed due to MEMORY_LIMIT_EXCEEDED — signals memory pressure'
-- @chart: type=bar group_by=t value=killed style=2d color=#ef4444
-- @drill: on=t into='Grafana Imports#OOM Killed Queries at Time'
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    sum(ProfileEvent_QueryMemoryLimitExceeded) AS killed
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Max Parts Per Partition' group='Grafana Imports' interval='1 HOUR' description='Maximum part count in any single partition — early warning for too-many-parts (300 = rejected inserts)'
-- @chart: type=area group_by=t value=max_parts style=2d color=#ef4444
-- @cell: column=max_parts type=rag green<100 amber<250
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    max(value) AS max_parts
FROM {{cluster_aware:system.asynchronous_metric_log}}
WHERE event_time > {{time_range}}
  AND metric = 'MaxPartCountForPartition'
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Memory Tracked (RSS)' group='Grafana Imports' interval='1 HOUR' description='Absolute memory tracked by ClickHouse (MemoryTracking metric) in bytes'
-- @chart: type=area group_by=t value=memory_bytes style=2d color=#a855f7
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(CurrentMetric_MemoryTracking) AS memory_bytes
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='CPU Cores Used' group='Grafana Imports' interval='1 HOUR' description='CPU virtual time in cores — how many cores queries and merges are consuming'
-- @chart: type=area group_by=t value=cpu_cores style=2d color=#3b82f6
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(ProfileEvent_OSCPUVirtualTimeMicroseconds) / 1000000 AS cpu_cores
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  // ─── Query Performance ───────────────────────────────────────────────

  `-- @meta: title='Total Queries/sec' group='Grafana Imports' interval='1 HOUR' description='Total query rate across all query types'
-- @chart: type=area group_by=t value=qps style=2d color=#f59e0b
-- @drill: on=t into='Grafana Imports#Queries at Time'
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(ProfileEvent_Query) AS qps
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Query Latency (avg)' group='Grafana Imports' interval='1 HOUR' description='Average query execution time in milliseconds'
-- @chart: type=line group_by=t value=avg_ms style=2d color=#8b5cf6 unit=ms
-- @drill: on=t into='Grafana Imports#Queries at Time'
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    if(sum(ProfileEvent_Query) > 0,
       sum(ProfileEvent_QueryTimeMicroseconds) / sum(ProfileEvent_Query) / 1000, 0) AS avg_ms
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Select vs Insert Rate' group='Grafana Imports' interval='1 HOUR' description='SELECT, sync INSERT, and async INSERT rates side by side'
-- @chart: type=grouped_line group_by=t value=selects,inserts,async_inserts style=2d
-- @drill: on=t into='Grafana Imports#Queries at Time'
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(ProfileEvent_SelectQuery) AS selects,
    avg(ProfileEvent_InsertQuery) AS inserts,
    avg(ProfileEvent_AsyncInsertQuery) AS async_inserts
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Failed Queries/sec' group='Grafana Imports' interval='1 HOUR' description='Total failed query rate — all query types combined'
-- @chart: type=bar group_by=t value=failed style=2d color=#ef4444
-- @drill: on=t into='Grafana Imports#Failed Queries at Time'
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    sum(ProfileEvent_FailedQuery) AS failed
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Select Latency (avg)' group='Grafana Imports' interval='1 HOUR' description='Average SELECT query latency in milliseconds'
-- @chart: type=line group_by=t value=avg_ms style=2d color=#a78bfa unit=ms
-- @drill: on=t into='Grafana Imports#Queries at Time'
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    if(sum(ProfileEvent_SelectQuery) > 0,
       sum(ProfileEvent_SelectQueryTimeMicroseconds) / sum(ProfileEvent_SelectQuery) / 1000, 0) AS avg_ms
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Insert Latency (avg)' group='Grafana Imports' interval='1 HOUR' description='Average synchronous INSERT query latency in milliseconds'
-- @chart: type=line group_by=t value=avg_ms style=2d color=#10b981 unit=ms
-- @drill: on=t into='Grafana Imports#Queries at Time'
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    if(sum(ProfileEvent_InsertQuery) > 0,
       sum(ProfileEvent_InsertQueryTimeMicroseconds) / sum(ProfileEvent_InsertQuery) / 1000, 0) AS avg_ms
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Pending Async Inserts' group='Grafana Imports' interval='1 HOUR' description='Number of async insert queries waiting to be flushed'
-- @chart: type=area group_by=t value=pending style=2d color=#f59e0b
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(CurrentMetric_PendingAsyncInsert) AS pending
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  // ─── Read Path (SELECT Internals) ────────────────────────────────────

  `-- @meta: title='Selected Parts/sec' group='Grafana Imports' interval='1 HOUR' description='Rate of parts opened for reading by SELECT queries — high values may indicate missing partition pruning'
-- @chart: type=area group_by=t value=parts_sec style=2d color=#f0883e
-- @drill: on=t into='Grafana Imports#Heavy Readers at Time'
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(ProfileEvent_SelectedParts) AS parts_sec
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Selected Ranges/sec' group='Grafana Imports' interval='1 HOUR' description='Rate of mark ranges selected — correlates with index granularity efficiency'
-- @chart: type=area group_by=t value=ranges_sec style=2d color=#e3b341
-- @drill: on=t into='Grafana Imports#Heavy Readers at Time'
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(ProfileEvent_SelectedRanges) AS ranges_sec
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Selected Marks/sec' group='Grafana Imports' interval='1 HOUR' description='Rate of marks (index granules) read — high ratios to rows indicate suboptimal ORDER BY key usage'
-- @chart: type=area group_by=t value=marks_sec style=2d color=#84cc16
-- @drill: on=t into='Grafana Imports#Heavy Readers at Time'
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(ProfileEvent_SelectedMarks) AS marks_sec
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Selected Rows/sec' group='Grafana Imports' interval='1 HOUR' description='Rate of rows read by SELECT queries'
-- @chart: type=area group_by=t value=rows_sec style=2d color=#22c55e
-- @drill: on=t into='Grafana Imports#Heavy Readers at Time'
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(ProfileEvent_SelectedRows) AS rows_sec
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Selected Bytes/sec' group='Grafana Imports' interval='1 HOUR' description='Rate of bytes read by SELECT queries'
-- @chart: type=area group_by=t value=bytes_sec style=2d color=#06b6d4
-- @drill: on=t into='Grafana Imports#Heavy Readers at Time'
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(ProfileEvent_SelectedBytes) AS bytes_sec
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  // ─── Write Path & Parts ──────────────────────────────────────────────

  `-- @meta: title='MergeTree Writer Rows/sec' group='Grafana Imports' interval='1 HOUR' description='Rate of rows written by the MergeTree data writer'
-- @chart: type=area group_by=t value=rows_sec style=2d color=#10b981
-- @drill: on=t into='Grafana Imports#Queries at Time'
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(ProfileEvent_MergeTreeDataWriterRows) AS rows_sec
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='MergeTree Writer Blocks/sec' group='Grafana Imports' interval='1 HOUR' description='Rate of blocks written — each block becomes a new part'
-- @chart: type=area group_by=t value=blocks_sec style=2d color=#14b8a6
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(ProfileEvent_MergeTreeDataWriterBlocks) AS blocks_sec
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='MergeTree Write Bytes (compressed vs uncompressed)' group='Grafana Imports' interval='1 HOUR' description='Compressed and uncompressed bytes written — gap shows compression ratio effectiveness'
-- @chart: type=grouped_line group_by=t value=uncompressed,compressed style=2d
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(ProfileEvent_MergeTreeDataWriterUncompressedBytes) AS uncompressed,
    avg(ProfileEvent_MergeTreeDataWriterCompressedBytes) AS compressed
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Rejected Inserts (TOO_MANY_PARTS)' group='Grafana Imports' interval='1 HOUR' description='Inserts rejected because partition exceeded max part count — critical production issue'
-- @chart: type=bar group_by=t value=rejected style=2d color=#ef4444
-- @cell: column=rejected type=rag green<1 amber<5
-- @drill: on=t into='Grafana Imports#Rejected Inserts at Time'
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    sum(ProfileEvent_RejectedInserts) AS rejected
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Delayed Inserts' group='Grafana Imports' interval='1 HOUR' description='Inserts throttled (delayed) due to approaching part limits — early warning before rejections'
-- @chart: type=bar group_by=t value=delayed style=2d color=#f59e0b
-- @drill: on=t into='Grafana Imports#Rejected Inserts at Time'
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    sum(ProfileEvent_DelayedInserts) AS delayed
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Delayed Insert Latency (avg)' group='Grafana Imports' interval='1 HOUR' description='Average time inserts were delayed in milliseconds'
-- @chart: type=line group_by=t value=avg_ms style=2d color=#f97316 unit=ms
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    if(sum(ProfileEvent_DelayedInserts) > 0,
       sum(ProfileEvent_DelayedInsertsMilliseconds) / sum(ProfileEvent_DelayedInserts), 0) AS avg_ms
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Inserted Rows & Bytes' group='Grafana Imports' interval='1 HOUR' description='Total inserted rows and bytes per second (all tables, all engines)'
-- @chart: type=grouped_line group_by=t value=rows_sec,bytes_sec style=2d
-- @drill: on=t into='Grafana Imports#Queries at Time'
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(ProfileEvent_InsertedRows) AS rows_sec,
    avg(ProfileEvent_InsertedBytes) AS bytes_sec
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  // ─── Parts Lifecycle ─────────────────────────────────────────────────

  `-- @meta: title='Parts by Type' group='Grafana Imports' interval='1 HOUR' description='Part counts by storage type — Wide (large), Compact (small), InMemory'
-- @chart: type=grouped_line group_by=t value=active,compact,wide style=2d
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(CurrentMetric_PartsActive) AS active,
    avg(CurrentMetric_PartsCompact) AS compact,
    avg(CurrentMetric_PartsWide) AS wide
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Parts Lifecycle' group='Grafana Imports' interval='1 HOUR' description='Part counts by lifecycle state — PreActive (being written), Outdated (pending cleanup), Deleting, Temporary'
-- @chart: type=grouped_line group_by=t value=pre_active,outdated,deleting,temporary style=2d
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(CurrentMetric_PartsPreActive) AS pre_active,
    avg(CurrentMetric_PartsOutdated) AS outdated,
    avg(CurrentMetric_PartsDeleting) AS deleting,
    avg(CurrentMetric_PartsTemporary) AS temporary
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  // ─── Merge Internals ─────────────────────────────────────────────────

  `-- @meta: title='Merge Rate' group='Grafana Imports' interval='1 HOUR' description='Number of merge operations completed per minute'
-- @chart: type=area group_by=t value=merges style=2d color=#e3b341
-- @drill: on=t into='Grafana Imports#Part Activity at Time'
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    sum(ProfileEvent_Merge) AS merges
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Active Mutations' group='Grafana Imports' interval='1 HOUR' description='Number of currently executing part mutations (ALTER TABLE operations)'
-- @chart: type=area group_by=t value=mutations style=2d color=#f0883e
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(CurrentMetric_PartMutation) AS mutations
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Merge & Mutation Memory' group='Grafana Imports' interval='1 HOUR' description='Memory consumed by background merges and mutations — competes with query memory'
-- @chart: type=area group_by=t value=memory_bytes style=2d color=#8b5cf6
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(CurrentMetric_MergesMutationsMemoryTracking) AS memory_bytes
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Merge Duration (avg)' group='Grafana Imports' interval='1 HOUR' description='Average merge duration in milliseconds'
-- @chart: type=line group_by=t value=avg_ms style=2d color=#e3b341 unit=ms
-- @drill: on=t into='Grafana Imports#Part Activity at Time'
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    if(sum(ProfileEvent_Merge) > 0,
       sum(ProfileEvent_MergeTotalMilliseconds) / sum(ProfileEvent_Merge), 0) AS avg_ms
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Merged Rows & Bytes' group='Grafana Imports' interval='1 HOUR' description='Rows and uncompressed bytes processed by merges per second'
-- @chart: type=grouped_line group_by=t value=rows_sec,bytes_sec style=2d
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(ProfileEvent_MergedRows) AS rows_sec,
    avg(ProfileEvent_MergedUncompressedBytes) AS bytes_sec
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Disk Space Reserved for Merges' group='Grafana Imports' interval='1 HOUR' description='Disk space currently reserved for in-progress merges'
-- @chart: type=area group_by=t value=reserved_bytes style=2d color=#f0883e
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(CurrentMetric_DiskSpaceReservedForMerge) AS reserved_bytes
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  // ─── Cache & I/O ─────────────────────────────────────────────────────

  `-- @meta: title='Filesystem Cache Size' group='Grafana Imports' interval='1 HOUR' description='Size of the local filesystem cache (used for S3/remote storage)'
-- @chart: type=area group_by=t value=cache_bytes style=2d color=#06b6d4
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(CurrentMetric_FilesystemCacheSize) AS cache_bytes
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Filesystem Cache Hit Rate' group='Grafana Imports' interval='1 HOUR' description='Percentage of reads served from filesystem cache vs remote storage — low rates mean excessive S3/remote reads'
-- @chart: type=line group_by=t value=hit_rate_pct style=2d color=#22c55e unit=%
-- @cell: column=hit_rate_pct type=gauge max=100 unit=%
-- @cell: column=hit_rate_pct type=rag green>80 amber>50
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    if(sum(ProfileEvent_CachedReadBufferReadFromCacheBytes) + sum(ProfileEvent_CachedReadBufferReadFromSourceBytes) > 0,
       sum(ProfileEvent_CachedReadBufferReadFromCacheBytes)
       / (sum(ProfileEvent_CachedReadBufferReadFromCacheBytes) + sum(ProfileEvent_CachedReadBufferReadFromSourceBytes)) * 100, 0) AS hit_rate_pct
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Page Cache Hit Rate' group='Grafana Imports' interval='1 HOUR' description='OS page cache effectiveness — ratio of filesystem reads to total reads (filesystem + S3)'
-- @chart: type=line group_by=t value=hit_rate_pct style=2d color=#14b8a6 unit=%
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    if(sum(ProfileEvent_OSReadChars) + sum(ProfileEvent_ReadBufferFromS3Bytes) > 0,
       sum(ProfileEvent_OSReadChars)
       / (sum(ProfileEvent_OSReadChars) + sum(ProfileEvent_ReadBufferFromS3Bytes)) * 100, 0) AS hit_rate_pct
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Mark Cache Hit Rate' group='Grafana Imports' interval='1 HOUR' description='Mark cache hit rate — marks index granule boundaries; misses mean extra disk reads for every query'
-- @chart: type=line group_by=t value=hit_rate_pct style=2d color=#84cc16 unit=%
-- @cell: column=hit_rate_pct type=gauge max=100 unit=%
-- @cell: column=hit_rate_pct type=rag green>90 amber>70
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    if(sum(ProfileEvent_MarkCacheHits) + sum(ProfileEvent_MarkCacheMisses) > 0,
       sum(ProfileEvent_MarkCacheHits)
       / (sum(ProfileEvent_MarkCacheHits) + sum(ProfileEvent_MarkCacheMisses)) * 100, 0) AS hit_rate_pct
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='S3 Read Bytes/sec' group='Grafana Imports' interval='1 HOUR' description='Bytes read from S3 (or other remote storage) per second'
-- @chart: type=area group_by=t value=bytes_sec style=2d color=#f97316
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(ProfileEvent_ReadBufferFromS3Bytes) AS bytes_sec
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Disk vs Filesystem Reads' group='Grafana Imports' interval='1 HOUR' description='Physical disk reads vs filesystem reads (includes page cache) — gap shows page cache effectiveness'
-- @chart: type=grouped_line group_by=t value=disk_bytes,fs_bytes style=2d
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(ProfileEvent_OSReadBytes) AS disk_bytes,
    avg(ProfileEvent_OSReadChars) AS fs_bytes
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  // ─── Summary stats ───────────────────────────────────────────────────

  `-- @meta: title='Database & Table Counts' group='Grafana Imports' description='Current number of databases and tables'
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    (SELECT count() FROM system.databases) AS databases,
    (SELECT count() FROM system.tables WHERE database NOT IN ('system','INFORMATION_SCHEMA','information_schema')) AS tables,
    (SELECT count() FROM system.tables WHERE database NOT IN ('system','INFORMATION_SCHEMA','information_schema') AND engine LIKE '%MergeTree%') AS mergetree_tables`,

  `-- @meta: title='Total MergeTree Data' group='Grafana Imports' description='Total bytes stored in MergeTree tables (from async metrics)'
-- @chart: type=area group_by=t value=total_bytes style=2d color=#a855f7
-- @source: https://github.com/ClickHouse/clickhouse-mixin
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(value) AS total_bytes
FROM {{cluster_aware:system.asynchronous_metric_log}}
WHERE event_time > {{time_range}}
  AND metric = 'TotalBytesOfMergeTreeTables'
GROUP BY t
ORDER BY t ASC`,
  // ─── Drill-down targets (magic sauce) ──────────────────────────────
  // These queries are not shown as panels but are navigated to when
  // a user clicks on a data point in one of the time-series panels above.

  `-- @meta: title='Queries at Time' group='Grafana Imports' description='Queries running around a clicked timestamp — drill target for query rate/latency panels'
-- @link: on=query_id into='Grafana Imports#Query Detail'
SELECT
    query_id,
    type,
    user,
    query_start_time,
    query_duration_ms,
    formatReadableSize(memory_usage) AS memory,
    formatReadableSize(read_bytes) AS read_bytes,
    read_rows,
    result_rows,
    multiIf(
      type = 'ExceptionWhileProcessing', exception,
      type = 'ExceptionBeforeStart', exception,
      ''
    ) AS error,
    replaceRegexpAll(query, '\\s+', ' ') AS query
FROM {{cluster_aware:system.query_log}}
WHERE event_time >= {{drill_value:t | now() - INTERVAL 1 MINUTE}} - INTERVAL 1 MINUTE
  AND event_time < {{drill_value:t | now()}} + INTERVAL 1 MINUTE
  AND type IN ('QueryFinish', 'ExceptionWhileProcessing', 'ExceptionBeforeStart')
ORDER BY query_start_time DESC
LIMIT 200`,

  `-- @meta: title='Query Detail' group='Grafana Imports' description='Full query detail for a specific query_id'
SELECT
    query_id,
    type,
    user,
    event_time,
    query_duration_ms,
    memory_usage,
    read_bytes,
    read_rows,
    written_bytes,
    written_rows,
    result_rows,
    result_bytes,
    ProfileEvents,
    query
FROM {{cluster_aware:system.query_log}}
WHERE query_id = {{drill_value:query_id | ''}}
  AND type IN ('QueryFinish', 'ExceptionWhileProcessing', 'ExceptionBeforeStart')
ORDER BY event_time DESC
LIMIT 1`,

  `-- @meta: title='Failed Queries at Time' group='Grafana Imports' description='Failed queries around a clicked timestamp — drill target for error panels'
-- @link: on=query_id into='Grafana Imports#Query Detail'
SELECT
    query_id,
    user,
    query_start_time,
    query_duration_ms,
    formatReadableSize(memory_usage) AS memory,
    exception_code,
    substring(exception, 1, 200) AS error,
    replaceRegexpAll(query, '\\s+', ' ') AS query
FROM {{cluster_aware:system.query_log}}
WHERE event_time >= {{drill_value:t | now() - INTERVAL 1 MINUTE}} - INTERVAL 1 MINUTE
  AND event_time < {{drill_value:t | now()}} + INTERVAL 1 MINUTE
  AND type IN ('ExceptionWhileProcessing', 'ExceptionBeforeStart')
ORDER BY query_start_time DESC
LIMIT 200`,

  `-- @meta: title='Heavy Readers at Time' group='Grafana Imports' description='Queries with highest read amplification around a clicked timestamp'
-- @link: on=query_id into='Grafana Imports#Query Detail'
SELECT
    query_id,
    user,
    query_start_time,
    query_duration_ms,
    formatReadableSize(read_bytes) AS read_bytes,
    read_rows,
    ProfileEvents['SelectedParts'] AS selected_parts,
    ProfileEvents['SelectedRanges'] AS selected_ranges,
    ProfileEvents['SelectedMarks'] AS selected_marks,
    replaceRegexpAll(query, '\\s+', ' ') AS query
FROM {{cluster_aware:system.query_log}}
WHERE event_time >= {{drill_value:t | now() - INTERVAL 1 MINUTE}} - INTERVAL 1 MINUTE
  AND event_time < {{drill_value:t | now()}} + INTERVAL 1 MINUTE
  AND type = 'QueryFinish'
ORDER BY read_rows DESC
LIMIT 100`,

  `-- @meta: title='OOM Killed Queries at Time' group='Grafana Imports' description='Queries killed by MEMORY_LIMIT_EXCEEDED around a clicked timestamp'
-- @link: on=query_id into='Grafana Imports#Query Detail'
SELECT
    query_id,
    user,
    query_start_time,
    query_duration_ms,
    formatReadableSize(memory_usage) AS peak_memory,
    exception_code,
    substring(exception, 1, 200) AS error,
    replaceRegexpAll(query, '\\s+', ' ') AS query
FROM {{cluster_aware:system.query_log}}
WHERE event_time >= {{drill_value:t | now() - INTERVAL 1 MINUTE}} - INTERVAL 1 MINUTE
  AND event_time < {{drill_value:t | now()}} + INTERVAL 1 MINUTE
  AND type IN ('ExceptionWhileProcessing', 'ExceptionBeforeStart')
  AND exception_code = 241
ORDER BY memory_usage DESC
LIMIT 100`,

  `-- @meta: title='Top Parts Per Partition' group='Grafana Imports' description='Current active parts per partition (live snapshot) — shows which partitions carry the most parts right now'
SELECT
    database,
    table,
    partition,
    count() AS part_count,
    formatReadableSize(sum(bytes_on_disk)) AS disk_size,
    min(modification_time) AS oldest_part,
    max(modification_time) AS newest_part
FROM {{cluster_aware:system.parts}}
WHERE active
GROUP BY database, table, partition
ORDER BY part_count DESC
LIMIT 50`,

  `-- @meta: title='Rejected Inserts at Time' group='Grafana Imports' description='INSERT queries rejected with TOO_MANY_PARTS around the clicked timestamp'
-- @link: on=query_id into='Grafana Imports#Query Detail'
SELECT
    query_id,
    user,
    query_start_time,
    query_duration_ms,
    formatReadableSize(memory_usage) AS memory,
    exception_code,
    substring(exception, 1, 200) AS error,
    replaceRegexpAll(query, '\\s+', ' ') AS query
FROM {{cluster_aware:system.query_log}}
WHERE event_time >= {{drill_value:t | now() - INTERVAL 1 MINUTE}} - INTERVAL 1 MINUTE
  AND event_time < {{drill_value:t | now()}} + INTERVAL 1 MINUTE
  AND type IN ('ExceptionWhileProcessing', 'ExceptionBeforeStart')
  AND exception_code = 252
ORDER BY query_start_time DESC
LIMIT 200`,

  `-- @meta: title='Part Activity at Time' group='Grafana Imports' description='Part creation, merge, and removal events around clicked timestamp — explains part count spikes'
SELECT
    database,
    table,
    partition_id AS partition,
    event_type,
    count() AS events,
    formatReadableSize(sum(size_in_bytes)) AS total_size,
    min(event_time) AS first_event,
    max(event_time) AS last_event
FROM {{cluster_aware:system.part_log}}
WHERE event_time >= {{drill_value:t | now() - INTERVAL 5 MINUTE}} - INTERVAL 5 MINUTE
  AND event_time <= {{drill_value:t | now()}} + INTERVAL 5 MINUTE
GROUP BY database, table, partition_id, event_type
ORDER BY events DESC
LIMIT 50`,
];

export default queries;
