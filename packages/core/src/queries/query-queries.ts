/**
 * SQL query templates for query analysis.
 *
 * Extracted from backend/services/query_analyzer.py. All placeholders
 * use {param} syntax compatible with buildQuery().
 */

/** Get currently running queries from system.processes. */
export const RUNNING_QUERIES = `
  SELECT
    query_id,
    user,
    query,
    query_kind,
    elapsed,
    memory_usage,
    read_rows,
    read_bytes,
    total_rows_approx,
    IF(total_rows_approx > 0, read_rows / total_rows_approx, 0) AS progress,
    is_initial_query,
    initial_query_id,
    hostName() AS hostname
  FROM {{cluster_aware:system.processes}}
  WHERE is_cancelled = 0
  ORDER BY elapsed DESC
`;

/**
 * Options for building query history SQL
 */
export interface QueryHistorySQLOptions {
  /** Filter by specific user */
  user?: string;
  /** Filter by minimum duration in ms */
  minDurationMs?: number;
  /** Filter by minimum memory usage in bytes */
  minMemoryBytes?: number;
  /** Filter by query text (case-insensitive contains) */
  queryText?: string;
  /** Filter by query ID (case-insensitive contains) */
  queryId?: string;
}

/**
 * Build query history SQL with optional filters.
 * 
 * Always requires: start_date, start_time, end_time, limit params
 * Optional params based on options: user, min_duration_ms, min_memory_bytes, query_text, query_id
 * 
 * @example
 * ```ts
 * // Basic query
 * const sql = buildQueryHistorySQL();
 * const result = buildQuery(sql, { start_date: '2024-01-01', start_time: '...', end_time: '...', limit: 100 });
 * 
 * // With filters
 * const sql = buildQueryHistorySQL({ user: 'default', minDurationMs: 1000 });
 * const result = buildQuery(sql, { start_date: '...', ..., user: 'default', min_duration_ms: 1000 });
 * ```
 */
export function buildQueryHistorySQL(options: QueryHistorySQLOptions = {}): string {
  const filters: string[] = [
    'event_date >= {start_date}',
    'event_time >= {start_time}',
    'event_time <= {end_time}',
    "type IN ('QueryFinish', 'ExceptionWhileProcessing')",
  ];

  if (options.user !== undefined) {
    filters.push('user = {user}');
  }

  if (options.minDurationMs !== undefined) {
    filters.push('query_duration_ms >= {min_duration_ms}');
  }

  if (options.minMemoryBytes !== undefined) {
    filters.push('memory_usage >= {min_memory_bytes}');
  }

  if (options.queryText !== undefined) {
    filters.push('positionCaseInsensitive(query, {query_text}) > 0');
  }

  if (options.queryId !== undefined) {
    filters.push('positionCaseInsensitive(query_id, {query_id}) > 0');
  }

  return `
  SELECT
    query_id,
    type,
    query_start_time,
    query_duration_ms,
    read_rows,
    read_bytes,
    result_rows,
    result_bytes,
    memory_usage,
    query,
    exception,
    user,
    client_hostname,
    ProfileEvents['OSCPUVirtualTimeMicroseconds'] AS cpu_time_us,
    ProfileEvents['NetworkSendBytes'] AS network_send_bytes,
    ProfileEvents['NetworkReceiveBytes'] AS network_receive_bytes,
    ProfileEvents['ReadBufferFromFileDescriptorReadBytes'] AS disk_read_bytes,
    ProfileEvents['WriteBufferFromFileDescriptorWriteBytes'] AS disk_write_bytes,
    ProfileEvents['SelectedParts'] AS selected_parts,
    ProfileEvents['SelectedPartsTotal'] AS selected_parts_total,
    ProfileEvents['SelectedMarks'] AS selected_marks,
    ProfileEvents['SelectedMarksTotal'] AS selected_marks_total,
    ProfileEvents['SelectedRanges'] AS selected_ranges,
    ProfileEvents['MarkCacheHits'] AS mark_cache_hits,
    ProfileEvents['MarkCacheMisses'] AS mark_cache_misses,
    ProfileEvents['OSIOWaitMicroseconds'] AS io_wait_us,
    ProfileEvents['RealTimeMicroseconds'] AS real_time_us,
    ProfileEvents['UserTimeMicroseconds'] AS user_time_us,
    ProfileEvents['SystemTimeMicroseconds'] AS system_time_us,
    is_initial_query,
    initial_query_id,
    initial_address,
    hostName() AS hostname
  FROM {{cluster_aware:system.query_log}}
  WHERE ${filters.join('\n    AND ')}
  ORDER BY event_time DESC
  LIMIT {limit}
`;
}

/** Default query history SQL (no filters) */
export const QUERY_HISTORY = buildQueryHistorySQL();

/**
 * Get shard sub-queries for a distributed (coordinator) query.
 * Returns child queries that share the same initial_query_id but are not the initial query.
 *
 * Requires: initial_query_id param
 */
export const SUB_QUERIES = `
  SELECT
    query_id,
    any(hostname) AS hostname,
    any(query_duration_ms) AS query_duration_ms,
    any(memory_usage) AS memory_usage,
    any(read_rows) AS read_rows,
    any(read_bytes) AS read_bytes,
    any(query_preview) AS query_preview,
    any(exception_code) AS exception_code,
    any(exception) AS exception,
    any(query_start_time_microseconds) AS query_start_time_microseconds
  FROM (
    SELECT
      query_id,
      hostName() AS hostname,
      query_duration_ms,
      memory_usage,
      read_rows,
      read_bytes,
      substring(query, 1, 120) AS query_preview,
      exception_code,
      exception,
      query_start_time_microseconds
    FROM {{cluster_aware:system.query_log}}
    WHERE initial_query_id = {initial_query_id}
      AND is_initial_query = 0
      AND type IN ('QueryFinish', 'ExceptionWhileProcessing', 'ExceptionBeforeStart')
      AND event_date >= {event_date_bound}
  )
  GROUP BY query_id
  ORDER BY query_duration_ms DESC
  LIMIT 50
`;

/**
 * Get the set of initial_query_id values that have shard sub-queries,
 * scoped to a specific set of candidate query IDs.
 *
 * The caller injects the IN list directly (via escapeValue) so this
 * template only needs an event_date bound for partition pruning.
 *
 * Requires: caller to replace {{query_id_list}} with a parenthesised, quoted list.
 */
export const COORDINATOR_IDS = `
  SELECT DISTINCT initial_query_id
  FROM {{cluster_aware:system.query_log}}
  WHERE initial_query_id IN ({{query_id_list}})
    AND event_date >= {start_date}
    AND is_initial_query = 0
    AND type IN ('QueryFinish', 'ExceptionWhileProcessing', 'ExceptionBeforeStart')
`;

/**
 * Get the set of initial_query_id values from currently running shard sub-queries.
 * Used to tag coordinator queries in the running queries list.
 */
export const RUNNING_COORDINATOR_IDS = `
  SELECT DISTINCT initial_query_id
  FROM {{cluster_aware:system.processes}}
  WHERE is_initial_query = 0
    AND initial_query_id != ''
`;

/**
 * Get detailed information for a specific query by query_id.
 * Returns all available metadata from query_log for deep analysis.
 * 
 * Requires: query_id param
 */
export const QUERY_DETAIL = `
  SELECT
    -- Basic info
    query_id,
    type,
    query_start_time,
    query_start_time_microseconds,
    query_duration_ms,
    query,
    formatted_query,
    query_kind,
    toString(normalized_query_hash) AS normalized_query_hash,
    toString(sipHash64(query)) AS query_hash,
    user,
    current_database,
    
    -- Resource usage
    read_rows,
    read_bytes,
    written_rows,
    written_bytes,
    result_rows,
    result_bytes,
    memory_usage,
    
    -- Threading
    thread_ids,
    peak_threads_usage,
    
    -- Objects touched
    databases,
    tables,
    columns,
    partitions,
    projections,
    views,
    
    -- Functions and features used
    used_functions,
    used_aggregate_functions,
    used_aggregate_function_combinators,
    used_table_functions,
    used_storages,
    used_formats,
    used_dictionaries,
    
    -- Error info
    exception_code,
    exception,
    stack_trace,
    
    -- Client info
    client_hostname,
    client_name,
    client_version_major,
    client_version_minor,
    client_version_patch,
    interface,
    http_method,
    http_user_agent,
    
    -- Distributed query info
    is_initial_query,
    initial_user,
    initial_query_id,
    initial_address,
    initial_query_start_time,
    
    -- Settings and profile events (full maps)
    Settings,
    ProfileEvents,
    
    -- Cache usage
    query_cache_usage,
    
    -- Privileges
    used_privileges,
    missing_privileges,
    
    -- Log comment (if set)
    log_comment,
    
    -- Server hostname
    hostName() AS hostname
    
  FROM {{cluster_aware:system.query_log}}
  WHERE query_id = {query_id}
    AND type IN ('QueryFinish', 'ExceptionWhileProcessing', 'ExceptionBeforeStart')
    AND event_date >= {event_date_bound}
  ORDER BY is_initial_query DESC, event_time DESC
  LIMIT 1
`;

/**
 * Get ProfileEvents breakdown for a query.
 * Useful for detailed performance analysis.
 *
 * event_date bound added to avoid full table scan — query_log is partitioned
 * by event_date, and query_id is NOT the primary key (PK is event_date, event_time).
 * Without this, ClickHouse opens every partition to find the matching query_id.
 *
 * Requires: query_id param
 */
export const QUERY_PROFILE_EVENTS = `
  SELECT
    query_id,
    ProfileEvents
  FROM {{cluster_aware:system.query_log}}
  WHERE query_id = {query_id}
    AND type = 'QueryFinish'
    AND event_date >= {event_date_bound}
  LIMIT 1
`;

/**
 * Get the query_log flush interval from server settings.
 * Used to know how long to wait before reading from query_log after executing queries.
 */
export const QUERY_LOG_FLUSH_INTERVAL = `
  SELECT value
  FROM system.server_settings
  WHERE name = 'query_log_flush_interval_milliseconds'
  LIMIT 1
`;

/**
 * Find similar queries by normalized_query_hash.
 * Useful for finding patterns and comparing performance across similar queries.
 *
 * NOTE: This template is currently unused — getSimilarQueries() in
 * query-analyzer.ts builds its own inline SQL with richer columns.
 * Kept for reference; consider removing if confirmed dead code.
 *
 * Requires: normalized_query_hash, limit params
 */
export const SIMILAR_QUERIES = `
  SELECT
    query_id,
    query_start_time,
    query_duration_ms,
    read_rows,
    read_bytes,
    memory_usage,
    user,
    exception_code
  FROM {{cluster_aware:system.query_log}}
  WHERE normalized_query_hash = {normalized_query_hash}
    AND type IN ('QueryFinish', 'ExceptionWhileProcessing')
    AND event_date >= today() - 30
  ORDER BY event_time DESC
  LIMIT {limit}
`;

/**
 * Get query statistics aggregated by normalized_query_hash.
 * Shows avg/min/max/count for similar queries.
 * 
 * Requires: start_date, start_time, end_time, limit params
 */
export const QUERY_PATTERNS = `
  SELECT
    normalized_query_hash,
    any(query) AS sample_query,
    count() AS execution_count,
    avg(query_duration_ms) AS avg_duration_ms,
    max(query_duration_ms) AS max_duration_ms,
    min(query_duration_ms) AS min_duration_ms,
    avg(memory_usage) AS avg_memory,
    max(memory_usage) AS max_memory,
    avg(read_rows) AS avg_read_rows,
    sum(read_rows) AS total_read_rows,
    countIf(exception_code != 0) AS error_count
  FROM {{cluster_aware:system.query_log}}
  WHERE event_date >= {start_date}
    AND event_time >= {start_time}
    AND event_time <= {end_time}
    AND type IN ('QueryFinish', 'ExceptionWhileProcessing')
  GROUP BY normalized_query_hash
  ORDER BY execution_count DESC
  LIMIT {limit}
`;

/**
 * Get default values for specific settings from system.settings.
 * Used to show what the default value was before it was overridden.
 * 
 * Note: This query uses arrayJoin to filter by setting names passed as an array.
 * Requires: setting_names param (array of setting names)
 */
export const SETTINGS_DEFAULTS = `
  SELECT
    name,
    default,
    description,
    type
  FROM system.settings
  WHERE name IN ({setting_names})
  ORDER BY name
`;


/**
 * Get per-thread breakdown for a specific query from system.query_thread_log.
 * Returns one row per thread that participated in the query execution.
 *
 * Requires: query_id param
 */
export const QUERY_THREAD_BREAKDOWN = `
  SELECT
    thread_name,
    thread_id,
    query_duration_ms,
    read_rows,
    read_bytes,
    written_rows,
    written_bytes,
    memory_usage,
    peak_memory_usage,
    event_time_microseconds,
    query_start_time_microseconds,
    initial_query_start_time_microseconds,
    ProfileEvents['OSCPUVirtualTimeMicroseconds'] AS cpu_time_us,
    ProfileEvents['UserTimeMicroseconds'] AS user_time_us,
    ProfileEvents['SystemTimeMicroseconds'] AS system_time_us,
    ProfileEvents['OSIOWaitMicroseconds'] AS io_wait_us,
    ProfileEvents['RealTimeMicroseconds'] AS real_time_us,
    ProfileEvents['ReadBufferFromFileDescriptorReadBytes'] AS disk_read_bytes,
    ProfileEvents['WriteBufferFromFileDescriptorWriteBytes'] AS disk_write_bytes,
    ProfileEvents['NetworkSendBytes'] AS network_send_bytes,
    ProfileEvents['NetworkReceiveBytes'] AS network_receive_bytes
  FROM {{cluster_aware:system.query_thread_log}}
  WHERE query_id = {query_id}
    AND event_date >= {event_date_bound}
  ORDER BY peak_memory_usage DESC
`;

/**
 * Fetch human-readable descriptions for all profile events from system.events.
 * Intended to be called once at connection time and cached.
 */
export const PROFILE_EVENT_DESCRIPTIONS = `
  SELECT event, description FROM system.events ORDER BY event
`;

