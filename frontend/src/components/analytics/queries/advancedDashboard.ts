/**
 * Advanced Dashboard queries — adapted from system.dashboards (ClickHouse OSS).
 *
 * Source: ClickHouse server source code, Apache 2.0 License
 * https://github.com/ClickHouse/ClickHouse/blob/master/src/Storages/System/StorageSystemDashboards.cpp
 */

const queries: string[] = [
  `-- @meta: title='Queries/second' group='Advanced Dashboard' interval='1 HOUR' description='Rate of queries processed per second (from metric_log)'
-- @chart: type=area labels=t values=qps style=2d
-- @drill: on=t into='Queries at Time'
-- Source: https://github.com/ClickHouse/ClickHouse/blob/master/src/Storages/System/StorageSystemDashboards.cpp
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(ProfileEvent_Query) AS qps
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Queries at Time' group='Advanced Dashboard' description='Queries executed in a 15-second window around the selected time'
-- @link: on=query_id into='Query Detail by ID'
SELECT
    query_id,
    type,
    user,
    query_start_time,
    query_duration_ms,
    formatReadableSize(memory_usage) AS memory,
    formatReadableSize(read_bytes) AS read,
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
  AND query NOT LIKE '%-- source:Monitor%'
ORDER BY query_start_time DESC
LIMIT 100`,

  `-- @meta: title='Query Detail by ID' group='Advanced Dashboard' description='Full query detail for a specific query_id'
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
    query
FROM {{cluster_aware:system.query_log}}
WHERE query_id = {{drill_value:query_id | ''}}
  AND type IN ('QueryFinish', 'ExceptionWhileProcessing', 'ExceptionBeforeStart')
ORDER BY event_time DESC
LIMIT 1`,

  `-- @meta: title='CPU Usage (cores)' group='Advanced Dashboard' interval='1 HOUR' description='Average CPU virtual time in cores (from metric_log)'
-- @chart: type=area labels=t values=cpu_cores style=2d
-- Source: https://github.com/ClickHouse/ClickHouse/blob/master/src/Storages/System/StorageSystemDashboards.cpp
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(ProfileEvent_OSCPUVirtualTimeMicroseconds) / 1000000 AS cpu_cores
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Queries Running' group='Advanced Dashboard' interval='1 HOUR' description='Average number of concurrently running queries'
-- @chart: type=area labels=t values=running style=2d
-- Source: https://github.com/ClickHouse/ClickHouse/blob/master/src/Storages/System/StorageSystemDashboards.cpp
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(CurrentMetric_Query) AS running
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Merges Running' group='Advanced Dashboard' interval='1 HOUR' description='Average number of concurrently running merges'
-- @chart: type=area labels=t values=merges style=2d
-- Source: https://github.com/ClickHouse/ClickHouse/blob/master/src/Storages/System/StorageSystemDashboards.cpp
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(CurrentMetric_Merge) AS merges
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Selected Bytes/second' group='Advanced Dashboard' interval='1 HOUR' description='Average bytes read by SELECT queries per second'
-- @chart: type=area labels=t values=bytes_sec style=2d
-- Source: https://github.com/ClickHouse/ClickHouse/blob/master/src/Storages/System/StorageSystemDashboards.cpp
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(ProfileEvent_SelectedBytes) AS bytes_sec
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='IO Wait (seconds)' group='Advanced Dashboard' interval='1 HOUR' description='Average I/O wait time in seconds'
-- @chart: type=area labels=t values=io_wait style=2d
-- Source: https://github.com/ClickHouse/ClickHouse/blob/master/src/Storages/System/StorageSystemDashboards.cpp
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(ProfileEvent_OSIOWaitMicroseconds) / 1000000 AS io_wait
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='CPU Wait (seconds)' group='Advanced Dashboard' interval='1 HOUR' description='Average CPU wait time in seconds'
-- @chart: type=area labels=t values=cpu_wait style=2d
-- Source: https://github.com/ClickHouse/ClickHouse/blob/master/src/Storages/System/StorageSystemDashboards.cpp
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(ProfileEvent_OSCPUWaitMicroseconds) / 1000000 AS cpu_wait
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='OS CPU Usage (Userspace)' group='Advanced Dashboard' interval='1 HOUR' description='Normalized userspace CPU usage from async metrics'
-- @chart: type=area labels=t values=user_cpu style=2d
-- Source: https://github.com/ClickHouse/ClickHouse/blob/master/src/Storages/System/StorageSystemDashboards.cpp
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(value) AS user_cpu
FROM {{cluster_aware:system.asynchronous_metric_log}}
WHERE event_time > {{time_range}}
  AND metric = 'OSUserTimeNormalized'
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='OS CPU Usage (Kernel)' group='Advanced Dashboard' interval='1 HOUR' description='Normalized kernel CPU usage from async metrics'
-- @chart: type=area labels=t values=kernel_cpu style=2d
-- Source: https://github.com/ClickHouse/ClickHouse/blob/master/src/Storages/System/StorageSystemDashboards.cpp
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(value) AS kernel_cpu
FROM {{cluster_aware:system.asynchronous_metric_log}}
WHERE event_time > {{time_range}}
  AND metric = 'OSSystemTimeNormalized'
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Read From Disk (bytes/sec)' group='Advanced Dashboard' interval='1 HOUR' description='Average bytes read from disk per second'
-- @chart: type=area labels=t values=disk_read style=2d
-- Source: https://github.com/ClickHouse/ClickHouse/blob/master/src/Storages/System/StorageSystemDashboards.cpp
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(ProfileEvent_OSReadBytes) AS disk_read
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Read From Filesystem (bytes/sec)' group='Advanced Dashboard' interval='1 HOUR' description='Average bytes read from filesystem including page cache'
-- @chart: type=area labels=t values=fs_read style=2d
-- Source: https://github.com/ClickHouse/ClickHouse/blob/master/src/Storages/System/StorageSystemDashboards.cpp
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(ProfileEvent_OSReadChars) AS fs_read
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Memory Tracked (bytes)' group='Advanced Dashboard' interval='1 HOUR' description='Average tracked memory usage by ClickHouse processes'
-- @chart: type=area labels=t values=memory style=2d
-- Source: https://github.com/ClickHouse/ClickHouse/blob/master/src/Storages/System/StorageSystemDashboards.cpp
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(CurrentMetric_MemoryTracking) AS memory
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='In-Memory Caches (bytes)' group='Advanced Dashboard' interval='1 HOUR' description='Size of in-memory caches (mark, uncompressed, index, etc.)'
-- @chart: type=area labels=t values=cache_bytes style=2d
-- Source: https://github.com/ClickHouse/ClickHouse/blob/master/src/Storages/System/StorageSystemDashboards.cpp
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(
        CurrentMetric_MarkCacheBytes
        + CurrentMetric_UncompressedCacheBytes
    ) AS cache_bytes
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Load Average (15 min)' group='Advanced Dashboard' interval='1 HOUR' description='15-minute load average from async metrics'
-- @chart: type=area labels=t values=load_avg style=2d
-- Source: https://github.com/ClickHouse/ClickHouse/blob/master/src/Storages/System/StorageSystemDashboards.cpp
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(value) AS load_avg
FROM {{cluster_aware:system.asynchronous_metric_log}}
WHERE event_time > {{time_range}}
  AND metric = 'LoadAverage15'
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Selected Rows/second' group='Advanced Dashboard' interval='1 HOUR' description='Average rows read by SELECT queries per second'
-- @chart: type=area labels=t values=rows_sec style=2d
-- Source: https://github.com/ClickHouse/ClickHouse/blob/master/src/Storages/System/StorageSystemDashboards.cpp
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(ProfileEvent_SelectedRows) AS rows_sec
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Inserted Rows/second' group='Advanced Dashboard' interval='1 HOUR' description='Average rows inserted per second'
-- @chart: type=area labels=t values=rows_sec style=2d
-- Source: https://github.com/ClickHouse/ClickHouse/blob/master/src/Storages/System/StorageSystemDashboards.cpp
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(ProfileEvent_InsertedRows) AS rows_sec
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Total MergeTree Parts' group='Advanced Dashboard' interval='1 HOUR' description='Average total number of parts across all MergeTree tables'
-- @chart: type=area labels=t values=total_parts style=2d
-- Source: https://github.com/ClickHouse/ClickHouse/blob/master/src/Storages/System/StorageSystemDashboards.cpp
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(value) AS total_parts
FROM {{cluster_aware:system.asynchronous_metric_log}}
WHERE event_time > {{time_range}}
  AND metric = 'TotalPartsOfMergeTreeTables'
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Max Parts For Partition' group='Advanced Dashboard' interval='1 HOUR' description='Maximum part count in any single partition — early warning for too-many-parts'
-- @chart: type=area labels=t values=max_parts style=2d
-- Source: https://github.com/ClickHouse/ClickHouse/blob/master/src/Storages/System/StorageSystemDashboards.cpp
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    max(value) AS max_parts
FROM {{cluster_aware:system.asynchronous_metric_log}}
WHERE event_time > {{time_range}}
  AND metric = 'MaxPartCountForPartition'
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Concurrent Network Connections' group='Advanced Dashboard' interval='1 HOUR' description='TCP, MySQL, HTTP, and interserver connection counts'
-- @chart: type=grouped_line labels=t values=TCP_Connections,MySQL_Connections,HTTP_Connections,Interserver_Connections style=2d
-- Source: https://github.com/ClickHouse/ClickHouse/blob/master/src/Storages/System/StorageSystemDashboards.cpp
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    max(CurrentMetric_TCPConnection) AS TCP_Connections,
    max(CurrentMetric_MySQLConnection) AS MySQL_Connections,
    max(CurrentMetric_HTTPConnection) AS HTTP_Connections,
    max(CurrentMetric_InterserverConnection) AS Interserver_Connections
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,
];

export default queries;
