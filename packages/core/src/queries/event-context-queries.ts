import { APP_SOURCE_LIKE } from './source-tags.js';

/**
 * Terminal query-log rows whose execution interval contains the event time.
 * This is historical overlap, not a read of the current system.processes table.
 */
export const EVENT_CONTEXT_WORKLOAD = `
SELECT
  host,
  query_id,
  initial_query_id,
  user,
  query_kind,
  toString(query_start_time_microseconds, 'UTC') AS start_time,
  toString(query_start_time_microseconds + toIntervalMillisecond(query_duration_ms), 'UTC') AS end_time,
  query_duration_ms,
  memory_usage,
  cpu_us,
  read_rows,
  read_bytes,
  written_rows,
  written_bytes,
  toString(type) AS status,
  exception_code,
  substring(exception, 1, 2000) AS exception,
  substring(query, 1, 4000) AS query,
  query_id = {query_id} OR initial_query_id = {initial_query_id} AS is_event_query
FROM (
  SELECT
    hostname() AS host,
    query_id,
    initial_query_id,
    user,
    query_kind,
    query_start_time_microseconds,
    query_duration_ms,
    memory_usage,
    ProfileEvents['OSCPUVirtualTimeMicroseconds'] AS cpu_us,
    read_rows,
    read_bytes,
    written_rows,
    written_bytes,
    type,
    exception_code,
    exception,
    query
  FROM {{cluster_aware:system.query_log}}
  WHERE event_date >= toDate({window_start}) - 1
    AND type IN ('QueryFinish', 'ExceptionBeforeStart', 'ExceptionWhileProcessing')
    AND query_start_time_microseconds <= {event_time}
    AND query_start_time_microseconds + toIntervalMillisecond(query_duration_ms) >= {event_time}
    AND ({hostname} = '' OR hostname() = {hostname})
    -- Self-filter stays in this scope so it sees the untruncated text: tagQuery()
    -- puts the source comment at the END of the query, and the outer
    -- substring(query, 1, 4000) would cut it off for our longest queries.
    AND query NOT LIKE ${APP_SOURCE_LIKE}
)
ORDER BY is_event_query DESC, memory_usage DESC, cpu_us DESC
LIMIT {context_limit}
`;

/**
 * Five-second host samples surrounding the event. The service chooses the
 * latest sample at or before the event for each host (ASOF semantics) while
 * retaining the full before/after series for the UI.
 */
export const EVENT_CONTEXT_HOST_METRICS = `
SELECT
  hostname() AS host,
  toString(toStartOfInterval(event_time, INTERVAL 5 SECOND), 'UTC') AS sample_time,
  avg(CurrentMetric_MemoryTracking) AS memory_usage,
  avg(CurrentMetric_Query) AS active_queries,
  avg(CurrentMetric_Merge) AS active_merges,
  avg(ProfileEvent_OSCPUVirtualTimeMicroseconds) / 1000000 AS cpu_cores
FROM {{cluster_aware:system.metric_log}}
WHERE event_date >= toDate({window_start}) - 1
  AND event_time BETWEEN toDateTime64({window_start}, 3)
                     AND toDateTime64({window_end}, 3)
  AND ({hostname} = '' OR hostname() = {hostname})
GROUP BY host, sample_time
ORDER BY host, sample_time
`;

/**
 * Exact query breadcrumbs plus warning-or-higher server messages on the same
 * host and within the selected context window. When the event carries an
 * error code name, exception reports for it ("Code: N. ... (NAME)") are kept
 * at any level and ranked first, so they are not crowded out by unrelated
 * warnings. The pattern requires a digit right after "Code: ", which this
 * query's own text (logged by executeQuery, and analyzer-folded on shards)
 * can never contain, so the query does not match its own echo.
 */
export const EVENT_CONTEXT_SERVER_LOGS = `
SELECT
  hostname() AS host,
  toString(event_time_microseconds, 'UTC') AS occurred_at,
  toString(level) AS level,
  logger_name,
  substring(message, 1, 4000) AS message,
  query_id,
  thread_name,
  ({query_id} != '' AND (query_id = {query_id} OR query_id = {initial_query_id})) AS is_event_query,
  ({exception_name} != '' AND match(message, concat('(?s)Code: [0-9]+[.] .*[(]', {exception_name}, '[)]'))) AS is_event_error
FROM {{cluster_aware:system.text_log}}
WHERE event_date >= toDate({window_start}) - 1
  AND event_time_microseconds BETWEEN toDateTime64({window_start}, 6)
                                  AND toDateTime64({window_end}, 6)
  AND ({hostname} = '' OR hostname() = {hostname})
  AND (
    ({query_id} != '' AND (query_id = {query_id} OR query_id = {initial_query_id}))
    OR ({exception_name} != '' AND match(message, concat('(?s)Code: [0-9]+[.] .*[(]', {exception_name}, '[)]')))
    OR level IN ('Fatal', 'Critical', 'Error', 'Warning')
  )
ORDER BY
  is_event_query DESC,
  is_event_error DESC,
  abs(dateDiff('millisecond', event_time_microseconds, toDateTime64({event_time}, 6))),
  event_time_microseconds
LIMIT {context_limit}
`;

/**
 * Live error counter for the event's error on each host, plus that host's
 * current process start. Aggregating over the whole table returns one row per
 * reachable host even when it has no counter for the error. system.errors is
 * in memory and resets on restart, so process_started_at tells the caller
 * whether these counters can cover the event at all.
 *
 * Output aliases must not reuse column names: an alias named last_error_time
 * would shadow the column inside argMaxIf and nest aggregates.
 *
 * Uses only columns present on every supported checkpoint (23.8.2.7 through
 * 26.3); last_error_format_string and query_id are newer and must not be used.
 */
export const EVENT_ERROR_COUNTER_STATE = `
SELECT
  hostname() AS host,
  toString(now() - toIntervalSecond(uptime()), 'UTC') AS process_started_at,
  countIf(name = {exception_name} AND remote = {remote}) AS has_counter,
  sumIf(value, name = {exception_name} AND remote = {remote}) AS value,
  toString(maxIf(last_error_time, name = {exception_name} AND remote = {remote}), 'UTC') AS last_seen_at,
  substring(
    argMaxIf(last_error_message, last_error_time, name = {exception_name} AND remote = {remote}),
    1, 4000
  ) AS last_message
FROM {{cluster_aware:system.errors}}
WHERE ({hostname} = '' OR hostname() = {hostname})
GROUP BY host
ORDER BY host
`;
