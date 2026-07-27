/**
 * Operational event exploration queries.
 *
 * The top-level Events page remains the curated, capability-aware event
 * experience. These presets expose the detector inputs in Analytics so users
 * can inspect the raw evidence, change thresholds, and clone a dashboard while
 * deciding which queries are useful for their own installation.
 */

const queries: string[] = [
  `-- @meta: title='Event Source Availability' group='Events' description='Cluster-wide coverage of the optional ClickHouse system logs used by Events. Partial coverage identifies hosts where a source is missing.'
-- @cell: column=status type=rag green=available amber=partial red=unavailable
WITH (
    SELECT groupUniqArray(hostname())
    FROM {{cluster_aware:system.one}}
) AS cluster_hosts
SELECT
    expected.source,
    expected.event_types,
    length(available.available_hosts) AS available_nodes,
    length(cluster_hosts) AS cluster_nodes,
    arrayStringConcat(
        arrayFilter(host -> NOT has(available.available_hosts, host), cluster_hosts),
        ', '
    ) AS missing_on,
    multiIf(
        available_nodes = cluster_nodes, 'available',
        available_nodes = 0, 'unavailable',
        'partial'
    ) AS status
FROM values(
        'source String, source_table String, event_types String',
        ('system.query_log', 'query_log', 'query OOM, timeout, rejection, resource failure, DDL'),
        ('system.asynchronous_insert_log', 'asynchronous_insert_log', 'asynchronous insert failure'),
        ('system.backup_log', 'backup_log', 'backup and restore outcome'),
        ('system.zookeeper_connection_log', 'zookeeper_connection_log', 'Keeper connection state'),
        ('system.asynchronous_metric_log', 'asynchronous_metric_log', 'server restart'),
        ('system.crash_log', 'crash_log', 'server crash'),
        ('system.part_log', 'part_log', 'part and replicated fetch failure'),
        ('system.background_schedule_pool_log', 'background_schedule_pool_log', 'background task failure'),
        ('system.error_log', 'error_log', 'operational error burst'),
        ('system.metric_log', 'metric_log', 'read-only replica and replication counters'),
        ('system.text_log', 'text_log', 'warning-or-higher server-log activity')
    ) AS expected
LEFT JOIN (
    SELECT
        name AS source_table,
        groupUniqArray(hostname()) AS available_hosts
    FROM {{cluster_aware:system.tables}}
    WHERE database = 'system'
    GROUP BY name
) AS available USING (source_table)
ORDER BY status DESC, expected.source`,

  `-- @meta: title='Query Resource Failures' group='Events' interval='1 HOUR' description='The query OOM, timeout, admission, quota, space, parts, and mutation-limit failures currently promoted to Events.'
-- @query_link: on=query_id
-- @source: https://clickhouse.com/docs/operations/system-tables/query_log
SELECT
    event_time AS occurred_at,
    host,
    multiIf(
        exception_code IN (173, 241), 'query OOM',
        exception_code = 159, 'query timeout',
        exception_code IN (201, 202, 252, 692), 'query rejected',
        'query resource failure'
    ) AS event_type,
    exception_code,
    query_id,
    initial_query_id,
    user,
    query_kind,
    query_duration_ms,
    formatReadableSize(memory_usage) AS memory,
    substring(exception, 1, 500) AS exception,
    substring(query, 1, 500) AS query
FROM (
    SELECT
        query_start_time_microseconds + toIntervalMillisecond(query_duration_ms) AS event_time,
        hostname() AS host,
        exception_code,
        query_id,
        initial_query_id,
        user,
        query_kind,
        query_duration_ms,
        memory_usage,
        exception,
        query
    FROM {{cluster_aware:system.query_log}}
    WHERE event_date >= toDate({{time_range}})
      AND type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing')
      AND exception_code IN (159, 173, 201, 202, 241, 243, 252, 692)
      AND query NOT LIKE '%source:TraceHouse:%'
)
WHERE event_time > {{time_range}}
ORDER BY event_time DESC
LIMIT 500`,

  `-- @meta: title='DDL Changes' group='Events' interval='1 HOUR' description='Successful schema-changing queries promoted to informational Events; replicated database worker replays are excluded.'
-- @query_link: on=query_id
-- @source: https://clickhouse.com/docs/operations/system-tables/query_log
SELECT
    event_time AS occurred_at,
    host,
    query_kind AS event_type,
    query_id,
    initial_query_id,
    user,
    databases,
    tables,
    query_duration_ms,
    substring(query, 1, 1000) AS query
FROM (
    SELECT
        query_start_time_microseconds + toIntervalMillisecond(query_duration_ms) AS event_time,
        hostname() AS host,
        query_kind,
        query_id,
        initial_query_id,
        user,
        databases,
        tables,
        query_duration_ms,
        query
    FROM {{cluster_aware:system.query_log}}
    WHERE event_date >= toDate({{time_range}})
      AND type = 'QueryFinish'
      AND query_kind IN ('Create', 'Alter', 'Drop', 'Rename', 'Truncate', 'Optimize', 'Undrop')
      AND query NOT LIKE '%source:TraceHouse:%'
      AND query NOT LIKE '/* ddl_entry=query-%'
)
WHERE event_time > {{time_range}}
ORDER BY event_time DESC
LIMIT 500`,

  `-- @meta: title='Async Insert Failures' group='Events' interval='1 HOUR' description='Terminal asynchronous insert parsing and flush errors.'
-- @query_link: on=query_id
-- @source: https://clickhouse.com/docs/operations/system-tables/asynchronous_insert_log
SELECT
    event_time AS occurred_at,
    host,
    status,
    query_id,
    flush_query_id,
    database,
    table,
    format,
    rows,
    formatReadableSize(bytes) AS bytes,
    substring(exception, 1, 1000) AS exception
FROM (
    SELECT
        event_time_microseconds AS event_time,
        hostname() AS host,
        status,
        query_id,
        flush_query_id,
        database,
        table,
        format,
        rows,
        bytes,
        exception
    FROM {{cluster_aware:system.asynchronous_insert_log}}
    WHERE event_date >= toDate({{time_range}})
      AND status IN ('ParsingError', 'FlushError')
)
WHERE event_time > {{time_range}}
ORDER BY event_time DESC
LIMIT 500`,

  `-- @meta: title='Backup & Restore Outcomes' group='Events' interval='1 DAY' description='Completed, failed, and cancelled backup or restore operations.'
-- @query_link: on=query_id
-- @source: https://clickhouse.com/docs/operations/system-tables/backup_log
SELECT
    event_time AS occurred_at,
    host,
    status,
    operation_id,
    query_id,
    storage_name,
    start_time,
    end_time,
    num_files,
    formatReadableSize(total_size) AS total_size,
    substring(error, 1, 1000) AS error
FROM (
    SELECT
        event_time_microseconds AS event_time,
        hostname() AS host,
        status,
        id AS operation_id,
        query_id,
        name AS storage_name,
        start_time,
        end_time,
        num_files,
        total_size,
        error
    FROM {{cluster_aware:system.backup_log}}
    WHERE event_date >= toDate({{time_range}})
      AND status IN (
        'BACKUP_CREATED',
        'BACKUP_FAILED',
        'RESTORED',
        'RESTORE_FAILED',
        'BACKUP_CANCELLED',
        'RESTORE_CANCELLED'
      )
)
WHERE event_time > {{time_range}}
ORDER BY event_time DESC
LIMIT 500`,

  `-- @meta: title='Keeper Connection Changes' group='Events' interval='1 HOUR' description='Historical ClickHouse Keeper or ZooKeeper connect and disconnect records.'
-- @cell: column=connection_state type=rag green=connected amber=disconnected
-- @source: https://clickhouse.com/docs/operations/system-tables/zookeeper_connection_log
SELECT
    event_time AS occurred_at,
    host,
    connection_state,
    keeper_name,
    keeper_host,
    keeper_port,
    keeper_client_id,
    reason
FROM (
    SELECT
        event_time_microseconds AS event_time,
        hostname() AS host,
        type AS connection_state,
        name AS keeper_name,
        host AS keeper_host,
        port AS keeper_port,
        toString(client_id) AS keeper_client_id,
        reason
    FROM {{cluster_aware:system.zookeeper_connection_log}}
    WHERE event_date >= toDate({{time_range}})
)
WHERE event_time > {{time_range}}
ORDER BY event_time DESC
LIMIT 500`,

  `-- @meta: title='Server Restarts' group='Events' interval='1 DAY' description='Process restarts inferred when the persisted Uptime metric drops for a host.'
-- @source: https://clickhouse.com/docs/operations/system-tables/asynchronous_metric_log
WITH samples AS (
    SELECT
        hostname() AS host,
        event_time,
        value AS uptime,
        row_number() OVER (
            PARTITION BY hostname()
            ORDER BY event_time
        ) AS sample_number,
        lagInFrame(value, 1, value) OVER (
            PARTITION BY hostname()
            ORDER BY event_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS previous_uptime
    FROM {{cluster_aware:system.asynchronous_metric_log}}
    WHERE event_date >= toDate({{time_range}} - INTERVAL 1 DAY)
      AND event_time >= {{time_range}} - INTERVAL 15 MINUTE
      AND metric = 'Uptime'
),
restarts AS (
    SELECT
        event_time - toIntervalSecond(toUInt32(uptime)) AS event_time,
        event_time AS observed_at,
        host,
        uptime,
        previous_uptime
    FROM samples
    WHERE (sample_number > 1 AND previous_uptime > uptime + 5)
       OR sample_number = 1
)
SELECT
    event_time AS occurred_at,
    observed_at,
    host,
    uptime,
    previous_uptime
FROM restarts
WHERE event_time > {{time_range}}
ORDER BY occurred_at DESC
LIMIT 500`,

  `-- @meta: title='Server Crashes' group='Events' interval='7 DAY' description='Fatal ClickHouse crashes recorded by crash_log. This table often appears only after the first crash.'
-- @query_link: on=query_id
-- @source: https://clickhouse.com/docs/operations/system-tables/crash_log
SELECT
    event_time AS occurred_at,
    hostname() AS host,
    signal,
    query_id,
    version
FROM {{cluster_aware:system.crash_log}}
WHERE event_date >= toDate({{time_range}})
  AND event_time > {{time_range}}
ORDER BY event_time DESC
LIMIT 500`,

  `-- @meta: title='Part Operation Failures' group='Events' interval='1 HOUR' description='MergeTree part operations with non-zero errors, including failed replicated fetches.'
-- @query_link: on=query_id
-- @source: https://clickhouse.com/docs/operations/system-tables/part_log
SELECT
    event_time AS occurred_at,
    host,
    event_type,
    database,
    table,
    part_name,
    partition_id,
    disk_name,
    query_id,
    duration_ms,
    error,
    substring(exception, 1, 1000) AS exception
FROM (
    SELECT
        event_time_microseconds AS event_time,
        hostname() AS host,
        event_type,
        database,
        table,
        part_name,
        partition_id,
        disk_name,
        query_id,
        duration_ms,
        error,
        exception
    FROM {{cluster_aware:system.part_log}}
    WHERE event_date >= toDate({{time_range}})
      AND error != 0
)
WHERE event_time > {{time_range}}
ORDER BY event_time DESC
LIMIT 500`,

  `-- @meta: title='Background Task Failures' group='Events' interval='1 HOUR' description='Failed scheduled background work such as distributed sends, Buffer flushes, message-broker work, and replication tasks.'
-- @query_link: on=query_id
-- @source: https://clickhouse.com/docs/operations/system-tables/background_schedule_pool_log
SELECT
    event_time AS occurred_at,
    host,
    log_name,
    database,
    table,
    query_id,
    duration_ms,
    error,
    substring(exception, 1, 1000) AS exception
FROM (
    SELECT
        event_time_microseconds AS event_time,
        hostname() AS host,
        log_name,
        database,
        table,
        query_id,
        duration_ms,
        error,
        exception
    FROM {{cluster_aware:system.background_schedule_pool_log}}
    WHERE event_date >= toDate({{time_range}})
      AND error != 0
)
WHERE event_time > {{time_range}}
ORDER BY event_time DESC
LIMIT 500`,

  `-- @meta: title='Operational Error Bursts' group='Events' interval='1 HOUR' description='The allowlisted storage, corruption, Keeper, replica, and connection error-counter deltas promoted to Events.'
-- @cell: column=value type=rag green<1 amber<5
-- @source: https://clickhouse.com/docs/operations/system-tables/error_log
SELECT
    event_time AS occurred_at,
    hostname() AS host,
    error,
    code,
    value,
    remote
FROM {{cluster_aware:system.error_log}}
WHERE event_date >= toDate({{time_range}})
  AND event_time > {{time_range}}
  AND error IN (
    'NOT_ENOUGH_SPACE',
    'CORRUPTED_DATA',
    'CHECKSUM_DOESNT_MATCH',
    'TOO_MANY_UNEXPECTED_DATA_PARTS',
    'CANNOT_FSYNC',
    'CANNOT_OPEN_FILE',
    'CANNOT_READ_FROM_FILE_DESCRIPTOR',
    'CANNOT_WRITE_TO_FILE_DESCRIPTOR',
    'CANNOT_CLOSE_FILE',
    'CANNOT_SEEK_THROUGH_FILE',
    'CANNOT_TRUNCATE_FILE',
    'KEEPER_EXCEPTION',
    'ZOOKEEPER_EXCEPTION',
    'NO_ZOOKEEPER',
    'REPLICA_IS_ALREADY_ACTIVE',
    'ALL_CONNECTION_TRIES_FAILED'
  )
  AND value > 0
ORDER BY event_time DESC
LIMIT 500`,

  `-- @meta: title='Read-only Replica Samples' group='Events' interval='1 HOUR' description='Raw persisted samples behind read-only replica episodes. Positive values are the number of read-only replicated tables on a host.'
-- @chart: type=area group_by=t value=readonly_tables series=host style=2d color=#ef4444
-- @source: https://clickhouse.com/docs/operations/system-tables/metric_log
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    hostname() AS host,
    max(toUInt64(CurrentMetric_ReadonlyReplica)) AS readonly_tables
FROM {{cluster_aware:system.metric_log}}
WHERE event_date >= toDate({{time_range}})
  AND event_time > {{time_range}}
  AND CurrentMetric_ReadonlyReplica > 0
GROUP BY t, host
ORDER BY t ASC, host`,

  `-- @meta: title='Replication Failure Counters' group='Events' interval='1 HOUR' description='Persisted ReplicatedDataLoss, failed fetch, and failed part-check deltas promoted to replication Events.'
-- @chart: type=stacked_bar group_by=t value=count series=failure_kind orientation=v style=2d
-- @source: https://clickhouse.com/docs/operations/system-tables/metric_log
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    host,
    tupleElement(failure, 1) AS failure_kind,
    sum(tupleElement(failure, 2)) AS count
FROM (
    SELECT
        hostname() AS host,
        event_time,
        arrayJoin([
            tuple('data loss', toUInt64(ProfileEvent_ReplicatedDataLoss)),
            tuple('failed fetch', toUInt64(ProfileEvent_ReplicatedPartFailedFetches)),
            tuple('failed check', toUInt64(ProfileEvent_ReplicatedPartChecksFailed))
        ]) AS failure
    FROM {{cluster_aware:system.metric_log}}
    WHERE event_date >= toDate({{time_range}})
      AND event_time > {{time_range}}
)
WHERE tupleElement(failure, 2) > 0
GROUP BY t, host, failure_kind
ORDER BY t ASC, host, failure_kind`,

  `-- @meta: title='All Query Exceptions by Type' group='Events' interval='1 HOUR' description='Broader view of every failed query grouped by exception, including types not promoted to Events.'
-- @chart: type=bar group_by=exception_name value=failures style=2d color=#f97316
-- @source: https://clickhouse.com/docs/operations/system-tables/query_log
SELECT
    errorCodeToName(exception_code) AS exception_name,
    exception_code,
    count() AS failures,
    uniqExact(query_id) AS queries,
    uniqExact(host) AS hosts,
    max(event_time) AS last_seen
FROM (
    SELECT
        event_time,
        hostname() AS host,
        query_id,
        exception_code,
        query
    FROM {{cluster_aware:system.query_log}}
    WHERE event_date >= toDate({{time_range}})
      AND event_time > {{time_range}}
      AND type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing')
      AND query NOT LIKE '%source:TraceHouse:%'
)
GROUP BY exception_code
ORDER BY failures DESC
LIMIT 50`,

  `-- @meta: title='All Error Counters by Type' group='Events' interval='1 HOUR' description='Broader view of persisted error counter deltas, including query and network classes not promoted to Events.'
-- @chart: type=bar group_by=error value=occurrences style=2d color=#f59e0b
-- @source: https://clickhouse.com/docs/operations/system-tables/error_log
SELECT
    error,
    code,
    sum(value) AS occurrences,
    uniqExact(hostname()) AS hosts,
    max(event_time) AS last_seen
FROM {{cluster_aware:system.error_log}}
WHERE event_date >= toDate({{time_range}})
  AND event_time > {{time_range}}
  AND value > 0
GROUP BY error, code
ORDER BY occurrences DESC
LIMIT 50`,

  `-- @meta: title='Warning+ Server Log Activity' group='Events' interval='1 HOUR' description='Warning-or-higher text_log volume by logger, aggregated to avoid flooding the dashboard with raw server logs.'
-- @chart: type=bar group_by=logger_name value=messages style=2d color=#d29922
-- @source: https://clickhouse.com/docs/operations/system-tables/text_log
SELECT
    logger_name,
    level,
    count() AS messages,
    uniqExact(hostname()) AS hosts,
    max(event_time) AS last_seen,
    any(substring(message, 1, 300)) AS example
FROM {{cluster_aware:system.text_log}}
WHERE event_date >= toDate({{time_range}})
  AND event_time > {{time_range}}
  AND level <= 4
  AND message NOT LIKE '%source:TraceHouse:%'
GROUP BY logger_name, level
ORDER BY messages DESC
LIMIT 50`,

  `-- @meta: title='Current Replica Problems' group='Events' description='Live view of replicas currently read-only, session-expired, delayed, undersized, or reporting ZooKeeper exceptions. This is current state, not historical evidence.'
-- @cell: column=is_readonly type=rag green=0
-- @cell: column=is_session_expired type=rag green=0
-- @cell: column=absolute_delay type=rag green<10 amber<300
-- @source: https://clickhouse.com/docs/operations/system-tables/replicas
SELECT
    hostname() AS host,
    database,
    table,
    is_readonly,
    is_session_expired,
    absolute_delay,
    queue_size,
    inserts_in_queue,
    merges_in_queue,
    lost_part_count,
    parts_to_check,
    active_replicas,
    total_replicas,
    zookeeper_exception
FROM {{cluster_aware:system.replicas}}
WHERE is_readonly
   OR is_session_expired
   OR absolute_delay >= 10
   OR queue_size > 0
   OR lost_part_count > 0
   OR parts_to_check > 0
   OR active_replicas < total_replicas
   OR zookeeper_exception != ''
ORDER BY is_readonly DESC, is_session_expired DESC, absolute_delay DESC, queue_size DESC`,
];

export default queries;
