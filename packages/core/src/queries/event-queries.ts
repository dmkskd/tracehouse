import { APP_SOURCE_LIKE } from './source-tags.js';

/**
 * Query-log events that materially change how the surrounding workload should
 * be interpreted. Both failures and successful DDL are placed at query end,
 * not query start.
 *
 * Keep occurrences individual so normalized_query_hash can reveal periodic
 * scheduled jobs in the visualization.
 */
const QUERY_EVENTS_BASE = `
SELECT
  hostname() AS host,
  toString(query_start_time_microseconds + toIntervalMillisecond(query_duration_ms)) AS occurred_at,
  type,
  query_id,
  initial_query_id,
  toString(normalized_query_hash) AS normalized_query_hash,
  user,
  query_kind,
  substring(query, 1, 1000) AS query_short,
  databases,
  tables,
  exception_code,
  substring(exception, 1, 2000) AS exception,
  query_duration_ms,
  memory_usage
FROM {{cluster_aware:system.query_log}}
WHERE event_date >= toDate({start_time}) - 1
  AND (query_start_time_microseconds + toIntervalMillisecond(query_duration_ms))
      BETWEEN {start_time} AND {end_time}
  AND (
    (
      type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing')
      AND exception_code IN (159, 173, 201, 202, 241, 243, 252, 692)
    )
    OR (
      type = 'QueryFinish'
      AND query_kind IN ('Create', 'Alter', 'Drop', 'Rename', 'Truncate', 'Optimize', 'Undrop')
    )
  )
  AND query NOT LIKE ${APP_SOURCE_LIKE}
  -- Replicated databases replay DDL through an internal queue. Repeated
  -- CREATE ... IF NOT EXISTS checks appear as successful ddl_entry queries
  -- on every replica even when they make no schema change.
  AND query NOT LIKE '/* ddl_entry=query-%'
  AND ({hostname} = '' OR hostname() = {hostname})
ORDER BY occurred_at DESC
`;

export const QUERY_EVENTS = `
${QUERY_EVENTS_BASE}
LIMIT {event_limit} BY if(type = 'QueryFinish', 'ddl', 'query_resource')
`;

export const QUERY_EVENTS_GLOBAL_LIMIT = `
${QUERY_EVENTS_BASE}
LIMIT {event_limit}
`;

/**
 * Failed asynchronous inserts. The log records terminal parsing and flush
 * outcomes, so these are exact query-correlatable point events.
 */
export const ASYNC_INSERT_FAILURE_EVENTS = `
SELECT
  hostname() AS host,
  toString(event_time_microseconds) AS occurred_at,
  query_id,
  flush_query_id,
  database,
  table,
  format,
  status,
  rows,
  bytes,
  substring(exception, 1, 2000) AS exception
FROM {{cluster_aware:system.asynchronous_insert_log}}
WHERE event_date >= toDate({start_time}) - 1
  AND event_time_microseconds BETWEEN {start_time} AND {end_time}
  AND status IN ('ParsingError', 'FlushError')
  AND ({hostname} = '' OR hostname() = {hostname})
ORDER BY event_time_microseconds DESC
LIMIT {event_limit}
`;

/**
 * Terminal backup and restore outcomes. In-progress states are omitted so a
 * long-running operation does not appear to have completed inside the range.
 */
export const BACKUP_EVENTS = `
SELECT
  hostname() AS host,
  toString(event_time_microseconds) AS occurred_at,
  id AS operation_id,
  query_id,
  name AS storage_name,
  status,
  substring(error, 1, 2000) AS error,
  toString(start_time) AS start_time,
  toString(end_time) AS end_time,
  num_files,
  total_size
FROM {{cluster_aware:system.backup_log}}
WHERE event_date >= toDate({start_time}) - 1
  AND event_time_microseconds BETWEEN {start_time} AND {end_time}
  AND status IN (
    'BACKUP_CREATED',
    'BACKUP_FAILED',
    'RESTORED',
    'RESTORE_FAILED',
    'BACKUP_CANCELLED',
    'RESTORE_CANCELLED'
  )
  AND ({hostname} = '' OR hostname() = {hostname})
ORDER BY event_time_microseconds DESC
LIMIT {event_limit}
`;

/**
 * Exact Keeper/ZooKeeper connection state changes. Unlike the live
 * zookeeper_connection table, this system log preserves disconnects and later
 * connections for historical event ranges.
 */
export const KEEPER_CONNECTION_EVENTS = `
SELECT
  hostname() AS host,
  toString(event_time_microseconds) AS occurred_at,
  type AS connection_state,
  name AS keeper_name,
  host AS keeper_host,
  port AS keeper_port,
  toString(client_id) AS keeper_client_id,
  reason
FROM {{cluster_aware:system.zookeeper_connection_log}}
WHERE event_date >= toDate({start_time}) - 1
  AND event_time_microseconds BETWEEN {start_time} AND {end_time}
  AND ({hostname} = '' OR hostname() = {hostname})
ORDER BY event_time_microseconds DESC
LIMIT {event_limit}
`;

/**
 * Detect restarts using the Uptime asynchronous metric.
 *
 * There are two cases:
 *  - an existing hostname's Uptime drops;
 *  - a recreated server/container first appears with a low Uptime value.
 *
 * A look-behind is required to compare the first sample in the requested
 * range. occurred_at is inferred as observed sample time minus Uptime.
 */
export const SERVER_RESTART_EVENTS = `
WITH samples AS (
  SELECT
    host,
    event_time,
    uptime,
    row_number() OVER (
      PARTITION BY host
      ORDER BY event_time
    ) AS sample_number,
    lagInFrame(uptime, 1, uptime) OVER (
      PARTITION BY host
      ORDER BY event_time
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS previous_uptime
  FROM (
    SELECT
      hostname() AS host,
      event_time,
      value AS uptime
    FROM {{cluster_aware:system.asynchronous_metric_log}}
    WHERE metric = 'Uptime'
      AND event_date >= toDate({start_time}) - 1
      AND event_time >= {start_time} - INTERVAL 15 MINUTE
      AND event_time <= {end_time}
      AND ({hostname} = '' OR hostname() = {hostname})
  )
)
SELECT
  host,
  toString(event_time - toIntervalSecond(toUInt32(uptime))) AS occurred_at,
  toString(event_time) AS observed_at,
  uptime,
  previous_uptime
FROM samples
WHERE (
    (sample_number > 1 AND previous_uptime > uptime + 5)
    OR sample_number = 1
  )
  AND (event_time - toIntervalSecond(toUInt32(uptime)))
      BETWEEN {start_time} AND {end_time}
ORDER BY occurred_at DESC
LIMIT {event_limit}
`;

/**
 * crash_log is created only after the first crash on many installations, so
 * this query must only run when the capability probe reports it available.
 * Use only columns that have remained stable across supported CH versions.
 */
export const SERVER_CRASH_EVENTS = `
SELECT
  hostname() AS host,
  toString(event_time) AS occurred_at,
  signal,
  query_id,
  version
FROM {{cluster_aware:system.crash_log}}
WHERE event_date >= toDate({start_time}) - 1
  AND event_time BETWEEN {start_time} AND {end_time}
  AND ({hostname} = '' OR hostname() = {hostname})
ORDER BY event_time DESC
LIMIT {event_limit}
`;

/**
 * Failed data-part operations. part_log event_time_microseconds represents the
 * operation record time; only error-bearing rows are emitted.
 */
export const PART_FAILURE_EVENTS = `
SELECT
  hostname() AS host,
  toString(event_time_microseconds) AS occurred_at,
  query_id,
  event_type,
  database,
  table,
  part_name,
  partition_id,
  disk_name,
  duration_ms,
  error,
  substring(exception, 1, 2000) AS exception
FROM {{cluster_aware:system.part_log}}
WHERE event_date >= toDate({start_time}) - 1
  AND event_time_microseconds BETWEEN {start_time} AND {end_time}
  AND error != 0
  AND ({hostname} = '' OR hostname() = {hostname})
ORDER BY event_time_microseconds DESC
LIMIT {event_limit}
`;

/**
 * Failed periodic background work, including distributed sends, Buffer
 * flushes, message-broker operations, and table-specific maintenance tasks.
 */
export const BACKGROUND_TASK_FAILURE_EVENTS = `
SELECT
  hostname() AS host,
  toString(event_time_microseconds) AS occurred_at,
  query_id,
  database,
  table,
  log_name,
  duration_ms,
  error,
  substring(exception, 1, 2000) AS exception
FROM {{cluster_aware:system.background_schedule_pool_log}}
WHERE event_date >= toDate({start_time}) - 1
  AND event_time_microseconds BETWEEN {start_time} AND {end_time}
  AND error != 0
  AND ({hostname} = '' OR hostname() = {hostname})
ORDER BY event_time_microseconds DESC
LIMIT {event_limit}
`;

/**
 * Persisted operational error-counter deltas. error_log is intentionally
 * allowlisted: unfiltered rows mostly duplicate failed user queries and
 * cancellations and would overwhelm the event timeline.
 *
 * event_time is the log flush/sample time, not necessarily the exact time of
 * every occurrence represented by value.
 */
export const OPERATIONAL_ERROR_EVENTS = `
SELECT
  hostname() AS host,
  toString(event_time) AS occurred_at,
  code,
  error,
  value,
  remote
FROM {{cluster_aware:system.error_log}}
WHERE event_date >= toDate({start_time}) - 1
  AND event_time BETWEEN {start_time} AND {end_time}
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
  AND ({hostname} = '' OR hostname() = {hostname})
ORDER BY event_time DESC
LIMIT {event_limit}
`;

/**
 * Reconstruct server-level read-only replica episodes from the persisted
 * CurrentMetric gauge. This is deliberately an aggregate host signal:
 * metric_log does not retain the database/table identity from system.replicas.
 *
 * A short look-behind lets an episode crossing the left edge remain visible.
 * The episode end is the first sampled zero after its final positive sample.
 */
export const REPLICA_READONLY_EPISODES = `
WITH samples AS (
  SELECT
    hostname() AS host,
    event_time,
    toUInt64(CurrentMetric_ReadonlyReplica) AS readonly_tables
  FROM {{cluster_aware:system.metric_log}}
  WHERE event_date >= toDate({start_time}) - 1
    AND event_time >= {start_time} - INTERVAL 5 MINUTE
    AND event_time <= {end_time}
    AND ({hostname} = '' OR hostname() = {hostname})
),
sequenced AS (
  SELECT
    host,
    event_time,
    readonly_tables,
    countIf(readonly_tables = 0) OVER (
      PARTITION BY host
      ORDER BY event_time
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS episode,
    leadInFrame(readonly_tables, 1, readonly_tables) OVER (
      PARTITION BY host
      ORDER BY event_time
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS next_readonly_tables,
    leadInFrame(event_time, 1, event_time) OVER (
      PARTITION BY host
      ORDER BY event_time
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS next_event_time
  FROM samples
),
episodes AS (
  SELECT
    host,
    episode,
    min(event_time) AS episode_start,
    max(readonly_tables) AS max_readonly_tables,
    argMax(next_readonly_tables, event_time) AS next_value,
    argMax(next_event_time, event_time) AS recovery_time
  FROM sequenced
  WHERE readonly_tables > 0
  GROUP BY host, episode
)
SELECT
  host,
  toString(greatest(episode_start, toDateTime64({start_time}, 3))) AS occurred_at,
  if(next_value = 0, toString(recovery_time), '') AS ended_at,
  max_readonly_tables
FROM episodes
WHERE episode_start <= {end_time}
  AND (next_value != 0 OR recovery_time >= {start_time})
ORDER BY occurred_at DESC
LIMIT {event_limit}
`;

/**
 * Point events from metric_log ProfileEvent deltas. These counters are
 * persisted at the log sampling cadence and preserve failures that have
 * already disappeared from the live replication queue.
 */
export const REPLICATION_FAILURE_EVENTS = `
SELECT
  host,
  toString(event_time) AS occurred_at,
  tupleElement(failure, 1) AS failure_kind,
  tupleElement(failure, 2) AS value
FROM (
  SELECT
    hostname() AS host,
    event_time,
    arrayJoin([
      tuple('data_loss', toUInt64(ProfileEvent_ReplicatedDataLoss)),
      tuple('failed_fetch', toUInt64(ProfileEvent_ReplicatedPartFailedFetches)),
      tuple('failed_check', toUInt64(ProfileEvent_ReplicatedPartChecksFailed))
    ]) AS failure
  FROM {{cluster_aware:system.metric_log}}
  WHERE event_date >= toDate({start_time}) - 1
    AND event_time BETWEEN {start_time} AND {end_time}
    AND ({hostname} = '' OR hostname() = {hostname})
)
WHERE tupleElement(failure, 2) > 0
ORDER BY event_time DESC
LIMIT {event_limit}
`;
