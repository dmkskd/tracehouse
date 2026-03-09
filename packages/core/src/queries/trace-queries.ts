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

