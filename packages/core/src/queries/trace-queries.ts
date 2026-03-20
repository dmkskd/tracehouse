/**
 * SQL queries for query tracing and EXPLAIN functionality.
 */

/**
 * Fetch trace logs for a specific query from system.text_log.
 *
 * IMPORTANT: system.text_log is partitioned by event_date. Without an
 * event_date bound, ClickHouse must open and scan every partition (all
 * historical log data) just to find rows matching query_id.
 *
 * The {event_date_bound} placeholder is resolved by the service layer via
 * eventDateBound(). When the caller knows the query date it provides a tight
 * bound; otherwise falls back to today() - 7 (matching the default TTL).
 */
export const QUERY_TRACE_LOGS = `
  SELECT
    toString(event_time) AS event_time,
    toString(event_time_microseconds) AS event_time_microseconds,
    query_id,
    level,
    message,
    logger_name AS source,
    thread_id,
    thread_name
  FROM {{cluster_aware:system.text_log}}
  WHERE query_id = {query_id}
    AND event_date >= {event_date_bound}
  ORDER BY event_time_microseconds ASC
  LIMIT 1000
`;

/**
 * Fetch trace logs with log level filter.
 * See QUERY_TRACE_LOGS for event_date rationale.
 */
export const QUERY_TRACE_LOGS_WITH_LEVELS = `
  SELECT
    toString(event_time) AS event_time,
    toString(event_time_microseconds) AS event_time_microseconds,
    query_id,
    level,
    message,
    logger_name AS source,
    thread_id,
    thread_name
  FROM {{cluster_aware:system.text_log}}
  WHERE query_id = {query_id}
    AND level IN ({log_levels})
    AND event_date >= {event_date_bound}
  ORDER BY event_time_microseconds ASC
  LIMIT 1000
`;

/** Fetch OpenTelemetry spans by query_id attribute - only top 4 levels to avoid 35k+ processor spans */
export const QUERY_OTEL_SPANS_BY_QUERY_ID = `
  WITH 
    target_trace AS (
      SELECT trace_id 
      FROM {{cluster_aware:system.opentelemetry_span_log}} 
      WHERE attribute['clickhouse.query_id'] = {query_id} 
      LIMIT 1
    ),
    spans AS (
      SELECT span_id, parent_span_id, operation_name, start_time_us, finish_time_us,
             finish_time_us - start_time_us as duration_us,
             (finish_time_us - start_time_us) / 1000 as duration_ms,
             attribute
      FROM {{cluster_aware:system.opentelemetry_span_log}}
      WHERE trace_id = (SELECT trace_id FROM target_trace)
    ),
    level1 AS (
      SELECT *, 1 as level FROM spans 
      WHERE parent_span_id NOT IN (SELECT span_id FROM spans)
    ),
    level2 AS (
      SELECT s.*, 2 as level FROM spans s
      WHERE s.parent_span_id IN (SELECT span_id FROM level1)
    ),
    level3 AS (
      SELECT s.*, 3 as level FROM spans s
      WHERE s.parent_span_id IN (SELECT span_id FROM level2)
    ),
    level4 AS (
      SELECT s.*, 4 as level FROM spans s
      WHERE s.parent_span_id IN (SELECT span_id FROM level3)
    )
  SELECT 
    toString(span_id) as span_id,
    toString(parent_span_id) as parent_span_id,
    operation_name,
    start_time_us,
    finish_time_us,
    duration_us,
    duration_ms,
    level,
    attribute
  FROM (
    SELECT * FROM level1
    UNION ALL SELECT * FROM level2
    UNION ALL SELECT * FROM level3
    UNION ALL SELECT * FROM level4
  )
  ORDER BY start_time_us ASC
`;

/**
 * Flamegraph and trace_log queries.
 *
 * system.trace_log is partitioned by event_date. Without an event_date bound,
 * querying by query_id alone forces a full scan of every partition.
 *
 * The {event_date_bound} placeholder is resolved by the service layer via
 * eventDateBound(). When the caller knows the query date it provides a tight
 * bound; otherwise falls back to today() - 7 (matching the default TTL).
 */

/** Fetch CPU profiling data from trace_log using ClickHouse's built-in flameGraph() function */
export const QUERY_FLAMEGRAPH_CPU = `
  SELECT arrayJoin(flameGraph(arrayReverse(trace))) AS line
  FROM {{cluster_aware:system.trace_log}}
  WHERE query_id = {query_id} AND trace_type = 'CPU'
    AND event_date >= {event_date_bound}
`;

/** Fetch Real (wall-clock) profiling data - shows where time is spent including I/O waits */
export const QUERY_FLAMEGRAPH_REAL = `
  SELECT arrayJoin(flameGraph(arrayReverse(trace))) AS line
  FROM {{cluster_aware:system.trace_log}}
  WHERE query_id = {query_id} AND trace_type = 'Real'
    AND event_date >= {event_date_bound}
`;

/** Fetch Memory profiling data from trace_log using ClickHouse's built-in flameGraph() function */
export const QUERY_FLAMEGRAPH_MEMORY = `
  SELECT arrayJoin(flameGraph(trace, size)) AS line
  FROM {{cluster_aware:system.trace_log}}
  WHERE query_id = {query_id} AND trace_type = 'MemorySample'
    AND event_date >= {event_date_bound}
`;

/** Legacy: Fetch CPU profiling data with manual stack building (fallback) */
export const QUERY_FLAMEGRAPH_DATA = `
  SELECT
    arrayStringConcat(arrayMap(x -> demangle(addressToSymbol(x)), trace), ';') AS stack,
    count() AS value
  FROM {{cluster_aware:system.trace_log}}
  WHERE query_id = {query_id} AND trace_type = 'CPU'
    AND event_date >= {event_date_bound}
  GROUP BY trace
`;

/** Legacy: Fetch Real profiling data with manual stack building (fallback) */
export const QUERY_FLAMEGRAPH_REAL_LEGACY = `
  SELECT
    arrayStringConcat(arrayMap(x -> demangle(addressToSymbol(x)), trace), ';') AS stack,
    count() AS value
  FROM {{cluster_aware:system.trace_log}}
  WHERE query_id = {query_id} AND trace_type = 'Real'
    AND event_date >= {event_date_bound}
  GROUP BY trace
`;

/** Legacy: Fetch Memory profiling data with manual stack building (fallback) */
export const QUERY_FLAMEGRAPH_MEMORY_LEGACY = `
  SELECT
    arrayStringConcat(arrayMap(x -> demangle(addressToSymbol(x)), trace), ';') AS stack,
    sum(abs(size)) AS value
  FROM {{cluster_aware:system.trace_log}}
  WHERE query_id = {query_id} AND trace_type = 'MemorySample'
    AND event_date >= {event_date_bound}
  GROUP BY trace
`;

/**
 * Per-second hot (leaf) functions from trace_log.
 *
 * Returns the leaf function (trace[1] = innermost frame) counted per 1-second
 * bucket, so the frontend can show "what the query was doing" at any point
 * on the X-Ray timeline.
 *
 * Time buckets are relative to the query's first trace_log event, matching
 * the same zero-origin used by processes_history samples.
 *
 * The query is interval-agnostic: always 1-second granularity. The frontend
 * aggregates buckets when the process sampler uses a coarser interval.
 */
/**
 * Lightweight probe: count CPU profiler samples per second for a query.
 * No introspection functions needed — just counts rows.
 * Used to know which seconds have flamegraph data before the user asks.
 */
export const QUERY_TRACE_SAMPLE_COUNTS = `
  SELECT
    toUInt32(dateDiff('second', toDateTime64({query_start_time}, 6), event_time_microseconds)) AS t_second,
    count() AS samples
  FROM {{cluster_aware:system.trace_log}}
  WHERE query_id = {query_id} AND trace_type = 'CPU'
    AND event_date >= {event_date_bound}
  GROUP BY t_second
  ORDER BY t_second
`;

/**
 * Time-scoped flamegraph: same as QUERY_FLAMEGRAPH_CPU but filtered to a
 * specific time window within the query. Used by X-Ray to show "what was
 * happening at this second" via the speedscope viewer.
 *
 * {from_time} and {to_time} are absolute DateTime strings (ClickHouse format,
 * e.g. '2026-03-20 13:52:17'). The service converts ISO timestamps.
 */
export const QUERY_FLAMEGRAPH_CPU_TIME_SCOPED = `
  SELECT arrayJoin(flameGraph(arrayReverse(trace))) AS line
  FROM {{cluster_aware:system.trace_log}}
  WHERE query_id = {query_id} AND trace_type = 'CPU'
    AND event_date >= {event_date_bound}
    AND event_time_microseconds >= {from_time}
    AND event_time_microseconds < {to_time}
`;

/** Legacy fallback for time-scoped flamegraph (works on clusters) */
export const QUERY_FLAMEGRAPH_CPU_TIME_SCOPED_LEGACY = `
  SELECT
    arrayStringConcat(arrayMap(x -> demangle(addressToSymbol(x)), trace), ';') AS stack,
    count() AS value
  FROM {{cluster_aware:system.trace_log}}
  WHERE query_id = {query_id} AND trace_type = 'CPU'
    AND event_date >= {event_date_bound}
    AND event_time_microseconds >= {from_time}
    AND event_time_microseconds < {to_time}
  GROUP BY trace
`;

/**
 * Fetch per-processor execution stats from system.processors_profile_log.
 * Same partition pruning rationale as trace_log above.
 */
export const QUERY_PROCESSORS_PROFILE = `
  SELECT
    name,
    sum(elapsed_us) AS elapsed_us,
    sum(input_wait_elapsed_us) AS input_wait_us,
    sum(output_wait_elapsed_us) AS output_wait_us,
    sum(input_rows) AS input_rows,
    sum(input_bytes) AS input_bytes,
    sum(output_rows) AS output_rows,
    sum(output_bytes) AS output_bytes,
    count() AS instances
  FROM {{cluster_aware:system.processors_profile_log}}
  WHERE query_id = {query_id}
    AND event_date >= {event_date_bound}
  GROUP BY name
  ORDER BY elapsed_us DESC
`;

