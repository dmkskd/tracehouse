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
-- @rag: column=is_readonly green=0
-- @rag: column=is_session_expired green=0
-- @rag: column=absolute_delay green<10 amber<300
-- @source: https://clickhouse.com/docs/operations/system-tables/replicas
SELECT
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
FROM system.replicas
ORDER BY absolute_delay DESC`,

  `-- @meta: title='Replication Queue Summary' group='Replication' description='Aggregated replication queue by table and operation type — spot backlogs and stuck tasks'
-- @rag: column=max_tries green<3 amber<10
-- @source: https://clickhouse.com/docs/operations/system-tables/replication_queue
-- @source: https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-replication-queue/
SELECT
    database,
    table,
    type,
    count() AS queue_entries,
    sum(is_currently_executing) AS executing,
    max(num_tries) AS max_tries,
    max(num_postponed) AS max_postponed,
    min(create_time) AS oldest_entry,
    countIf(last_exception != '') AS with_errors
FROM system.replication_queue
GROUP BY database, table, type
ORDER BY queue_entries DESC`,

  `-- @meta: title='Replication Queue Errors' group='Replication' description='Replication tasks with exceptions — stuck fetches, failed merges, etc.'
-- @source: https://clickhouse.com/docs/operations/system-tables/replication_queue
-- @source: https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-replication-queue/
SELECT
    database,
    table,
    type,
    create_time,
    num_tries,
    num_postponed,
    postpone_reason,
    last_exception,
    last_attempt_time
FROM system.replication_queue
WHERE last_exception != ''
ORDER BY num_tries DESC
LIMIT 50`,

  `-- @meta: title='Replication Lag Trend' group='Replication' interval='1 HOUR' description='Maximum absolute_delay across all replicated tables over time — early warning for growing lag'
-- @chart: type=area group_by=t value=max_delay style=2d color=#ef4444
-- @source: https://clickhouse.com/docs/operations/system-tables/replicas
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
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
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    max(value) AS queue_size
FROM {{cluster_aware:system.asynchronous_metric_log}}
WHERE event_time > {{time_range}}
  AND metric = 'ReplicasMaxQueueSize'
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='ZooKeeper Operations' group='Replication' interval='1 HOUR' description='ZooKeeper request rate over time — transactions, watches, bytes sent/received'
-- @chart: type=area group_by=t value=value series=op style=2d
-- @source: https://clickhouse.com/docs/operations/monitoring
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
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
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(ProfileEvent_ZooKeeperWaitMicroseconds) / 1000 AS avg_wait_ms
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='ZooKeeper Sessions & Exceptions' group='Replication' interval='1 HOUR' description='Active ZooKeeper sessions and hardware exception rate over time'
-- @chart: type=grouped_line group_by=t value=sessions,hw_exceptions style=2d
-- @source: https://clickhouse.com/docs/operations/monitoring
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    avg(CurrentMetric_ZooKeeperSession) AS sessions,
    sum(ProfileEvent_ZooKeeperHardwareExceptions) AS hw_exceptions
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,
];

export default queries;
