/**
 * Replication health queries — replica status, queue depth, lag, ZooKeeper health.
 *
 * Sources:
 * - ClickHouse docs: https://clickhouse.com/docs/operations/system-tables/replicas
 * - ClickHouse docs: https://clickhouse.com/docs/operations/system-tables/replication_queue
 * - Altinity KB: https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-replication-queue/
 */

const queries: string[] = [
  `-- @meta: title='Replica Status' group='Replication' description='Health overview of all replicated tables — readonly, session expired, leader status, active replicas, and delay'
-- @cell: column=is_readonly type=rag green=0
-- @cell: column=is_session_expired type=rag green=0
-- @cell: column=absolute_delay type=rag green<10 amber<300
-- @source: https://clickhouse.com/docs/operations/system-tables/replicas
SELECT
    hostname() AS node,
    database,
    table,
    is_leader,
    is_readonly,
    is_session_expired,
    absolute_delay,
    queue_size,
    inserts_in_queue,
    merges_in_queue,
    part_mutations_in_queue,
    active_replicas,
    total_replicas,
    lost_part_count,
    parts_to_check
FROM {{cluster_aware:system.replicas}}
ORDER BY absolute_delay DESC`,

  `-- @meta: title='Replication Queue Summary' group='Replication' description='Aggregated replication queue by table and operation type — spot backlogs and stuck tasks'
-- @cell: column=max_tries type=rag green<3 amber<10
-- @source: https://clickhouse.com/docs/operations/system-tables/replication_queue
-- @source: https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-replication-queue/
SELECT
    hostname() AS node,
    database,
    table,
    type,
    count() AS queue_entries,
    sum(is_currently_executing) AS executing,
    max(num_tries) AS max_tries,
    max(num_postponed) AS max_postponed,
    min(create_time) AS oldest_entry,
    countIf(last_exception != '') AS with_errors
FROM {{cluster_aware:system.replication_queue}}
GROUP BY node, database, table, type
ORDER BY queue_entries DESC`,

  `-- @meta: title='Replication Queue Errors' group='Replication' description='Replication tasks with exceptions — stuck fetches, failed merges, etc.'
-- @source: https://clickhouse.com/docs/operations/system-tables/replication_queue
-- @source: https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-replication-queue/
SELECT
    hostname() AS node,
    database,
    table,
    type,
    create_time,
    num_tries,
    num_postponed,
    postpone_reason,
    last_exception,
    last_attempt_time
FROM {{cluster_aware:system.replication_queue}}
WHERE last_exception != ''
ORDER BY num_tries DESC
LIMIT 50`,

  `-- @meta: title='Replication Lag Trend' group='Replication' interval='1 HOUR' description='Maximum absolute_delay across all replicated tables over time — early warning for growing lag'
-- @chart: type=area group_by=t value=max_delay style=2d color=#ef4444
-- @source: https://clickhouse.com/docs/operations/system-tables/replicas
SELECT
    toStartOfInterval(event_time, INTERVAL 5 MINUTE) AS t,
    max(value) AS max_delay
FROM {{cluster_aware:system.asynchronous_metric_log}}
WHERE event_time > {{time_range}}
  AND metric = 'ReplicasMaxAbsoluteDelay'
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Replication Queue Size Trend' group='Replication' interval='1 HOUR' description='Total replication queue depth over time — growing queues signal replication falling behind'
-- @chart: type=area group_by=t value=queue_size style=2d color=#f59e0b
-- @source: https://clickhouse.com/docs/operations/system-tables/replicas
SELECT
    toStartOfInterval(event_time, INTERVAL 5 MINUTE) AS t,
    max(value) AS queue_size
FROM {{cluster_aware:system.asynchronous_metric_log}}
WHERE event_time > {{time_range}}
  AND metric = 'ReplicasMaxQueueSize'
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Replication Error Trend' group='Replication' interval='1 HOUR' description='Part operation failures over time by error type — merges, fetches, mutations that failed'
-- @chart: type=stacked_bar group_by=t value=count series=error_type orientation=v style=2d
-- @source: https://clickhouse.com/docs/operations/system-tables/part_log
SELECT
    toStartOfInterval(event_time, INTERVAL 5 MINUTE) AS t,
    concat(toString(event_type), ': ', extract(exception, '\\(([A-Z_]+)\\)')) AS error_type,
    count() AS count
FROM {{cluster_aware:system.part_log}}
WHERE event_time > {{time_range}}
  AND exception != ''
GROUP BY t, error_type
ORDER BY t ASC`,

  `-- @meta: title='ZooKeeper Operations' group='Replication' interval='1 HOUR' description='ZooKeeper request rate over time — transactions, watches, bytes sent/received'
-- @chart: type=area group_by=t value=value series=op style=2d
-- @source: https://clickhouse.com/docs/operations/monitoring
SELECT
    toStartOfInterval(event_time, INTERVAL 5 MINUTE) AS t,
    op,
    sum(val) AS value
FROM (
    SELECT event_time, 'Transactions' AS op,
        ProfileEvent_ZooKeeperTransactions AS val
    FROM {{cluster_aware:system.metric_log}}
    WHERE event_time > {{time_range}}
      UNION ALL
    SELECT event_time, 'Watches' AS op,
        ProfileEvent_ZooKeeperWatchResponse AS val
    FROM {{cluster_aware:system.metric_log}}
    WHERE event_time > {{time_range}}
      UNION ALL
    SELECT event_time, 'BytesSent' AS op,
        ProfileEvent_ZooKeeperBytesSent AS val
    FROM {{cluster_aware:system.metric_log}}
    WHERE event_time > {{time_range}}
      UNION ALL
    SELECT event_time, 'BytesReceived' AS op,
        ProfileEvent_ZooKeeperBytesReceived AS val
    FROM {{cluster_aware:system.metric_log}}
    WHERE event_time > {{time_range}}
)
GROUP BY t, op
ORDER BY t ASC, op`,

  `-- @meta: title='ZooKeeper Wait Time' group='Replication' interval='1 HOUR' description='Average ZooKeeper wait time per minute — high values indicate Keeper contention or network issues'
-- @chart: type=area group_by=t value=avg_wait_ms style=2d color=#8b5cf6
-- @source: https://clickhouse.com/docs/operations/monitoring
SELECT
    toStartOfInterval(event_time, INTERVAL 5 MINUTE) AS t,
    avg(ProfileEvent_ZooKeeperWaitMicroseconds) / 1000 AS avg_wait_ms
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='ZooKeeper Sessions & Exceptions' group='Replication' interval='1 HOUR' description='Active ZooKeeper sessions and hardware exception rate over time'
-- @chart: type=grouped_line group_by=t value=sessions,hw_exceptions style=2d
-- @source: https://clickhouse.com/docs/operations/monitoring
SELECT
    toStartOfInterval(event_time, INTERVAL 5 MINUTE) AS t,
    avg(CurrentMetric_ZooKeeperSession) AS sessions,
    sum(ProfileEvent_ZooKeeperHardwareExceptions) AS hw_exceptions
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Keeper Connection Status' group='Replication' description='Current Keeper/ZooKeeper connections per node — host, port, session expiry, and API version'
-- @cell: column=is_expired type=rag green=0
-- @source: https://clickhouse.com/docs/operations/system-tables/zookeeper_connection
SELECT
    hostname() AS node,
    host,
    port,
    index,
    connected_time,
    is_expired,
    keeper_api_version
FROM {{cluster_aware:system.zookeeper_connection}}
ORDER BY node, index`,

  `-- @meta: title='Keeper Metadata per Table' group='Replication' description='ZooKeeper path stats for each replicated table — log entries, registered replicas, readonly and session status'
-- @cell: column=is_readonly type=rag green=0
-- @cell: column=is_session_expired type=rag green=0
-- @source: https://clickhouse.com/docs/operations/system-tables/replicas
SELECT
    hostname() AS node,
    database,
    table,
    zookeeper_path,
    log_max_index AS log_head,
    log_pointer,
    log_max_index - log_pointer AS log_lag,
    total_replicas,
    active_replicas,
    is_readonly,
    is_session_expired,
    replica_name
FROM {{cluster_aware:system.replicas}}
ORDER BY log_lag DESC`,

  `-- @meta: title='Distribution Queue' group='Replication' description='Pending async sends for Distributed tables — aggregated per node/table with shard count'
-- @cell: column=total_blocked type=rag green=0
-- @cell: column=total_errors type=rag green=0
-- @cell: column=total_broken_files type=rag green=0
-- @source: https://clickhouse.com/docs/operations/system-tables/distribution_queue
SELECT
    hostname() AS node,
    database,
    table,
    count() AS shards,
    sum(is_blocked) AS total_blocked,
    sum(error_count) AS total_errors,
    sum(data_files) AS total_files,
    sum(data_compressed_bytes) AS total_compressed_bytes,
    sum(broken_data_files) AS total_broken_files,
    any(last_exception) AS sample_exception
FROM {{cluster_aware:system.distribution_queue}}
GROUP BY node, database, table
ORDER BY total_compressed_bytes DESC`,

  `-- @meta: title='Distribution Files & Bytes Pending' group='Replication' interval='1 HOUR' description='Files and bytes waiting to be sent to remote shards over time — growing backlog means distribution is falling behind'
-- @chart: type=grouped_line group_by=t value=files_pending,bytes_pending_mb style=2d
-- @source: https://clickhouse.com/docs/operations/system-tables/metric_log
SELECT
    toStartOfInterval(event_time, INTERVAL 5 MINUTE) AS t,
    max(CurrentMetric_DistributedFilesToInsert) AS files_pending,
    max(CurrentMetric_DistributedBytesToInsert) / 1048576 AS bytes_pending_mb
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Distribution Send Activity' group='Replication' interval='1 HOUR' description='Active distributed sends and connection failures over time — concurrent sends and errors'
-- @chart: type=grouped_line group_by=t value=active_sends,conn_fail,conn_fail_all style=2d
-- @source: https://clickhouse.com/docs/operations/system-tables/metric_log
SELECT
    toStartOfInterval(event_time, INTERVAL 5 MINUTE) AS t,
    max(CurrentMetric_DistributedSend) AS active_sends,
    sum(ProfileEvent_DistributedConnectionFailTry) AS conn_fail,
    sum(ProfileEvent_DistributedConnectionFailAtAll) AS conn_fail_all
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Distribution Insert Pressure' group='Replication' interval='1 HOUR' description='Throttled and rejected distributed INSERTs over time — high values mean the cluster cannot keep up'
-- @chart: type=stacked_bar group_by=t value=count series=event orientation=v style=2d
-- @source: https://clickhouse.com/docs/operations/system-tables/metric_log
SELECT
    toStartOfInterval(event_time, INTERVAL 5 MINUTE) AS t,
    event,
    sum(val) AS count
FROM (
    SELECT event_time, 'Delayed' AS event,
        ProfileEvent_DistributedDelayedInserts AS val
    FROM {{cluster_aware:system.metric_log}}
    WHERE event_time > {{time_range}}
      UNION ALL
    SELECT event_time, 'Rejected' AS event,
        ProfileEvent_DistributedRejectedInserts AS val
    FROM {{cluster_aware:system.metric_log}}
    WHERE event_time > {{time_range}}
      UNION ALL
    SELECT event_time, 'Async Failures' AS event,
        ProfileEvent_DistributedAsyncInsertionFailures AS val
    FROM {{cluster_aware:system.metric_log}}
    WHERE event_time > {{time_range}}
)
GROUP BY t, event
HAVING count > 0
ORDER BY t ASC`,
];

export default queries;
