/**
 * SQL queries for 3D stress surface and pattern surface visualizations.
 *
 * Stress Surface: Time × Resource dimension × normalized intensity
 * Pattern Surface: Query pattern × Time × actual duration
 *
 * All queries accept either:
 *   - hours (UInt32) for a lookback window from now()
 *   - start_time / end_time (String) for an absolute range
 * The caller passes the appropriate time filter clause via buildSurfaceTimeFilter().
 */

/** Build the event_time filter clause for surface queries. */
export function buildSurfaceTimeFilter(
  column: string,
  opts: { hours?: number; startTime?: string; endTime?: string },
): { clause: string; params: Record<string, string | number> } {
  if (opts.startTime && opts.endTime) {
    return {
      clause: `${column} BETWEEN {start_time} AND {end_time}`,
      params: { start_time: opts.startTime.replace('T', ' '), end_time: opts.endTime.replace('T', ' ') },
    };
  }
  const minutes = Math.round((opts.hours ?? 24) * 60);
  return {
    clause: `${column} > now() - INTERVAL {minutes} MINUTE`,
    params: { minutes },
  };
}

/**
 * Per-minute aggregated query stress for a specific table.
 * Each row = one time bucket with resource usage totals/averages.
 */
export const stressSurfaceQueries = (timeClause: string) => `
SELECT
    toStartOfMinute(event_time) AS ts,
    count() AS query_count,
    sum(query_duration_ms) AS total_duration_ms,
    avg(query_duration_ms) AS avg_duration_ms,
    quantile(0.95)(query_duration_ms) AS p95_duration_ms,
    sum(read_rows) AS total_read_rows,
    sum(read_bytes) AS total_read_bytes,
    sum(memory_usage) AS total_memory,
    sum(ProfileEvents['RealTimeMicroseconds']) AS total_cpu_us,
    sum(ProfileEvents['IOWaitMicroseconds']) AS total_io_wait_us,
    sum(ProfileEvents['SelectedMarks']) AS total_selected_marks
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query_kind = 'Select'
  AND ${timeClause}
  AND is_initial_query = 1
  AND has(tables, concat({database}, '.', {table_name}))
GROUP BY ts
ORDER BY ts
`;

/**
 * Insert activity per minute for a specific table.
 */
export const stressSurfaceInserts = (timeClause: string) => `
SELECT
    toStartOfMinute(event_time) AS ts,
    count() AS insert_count,
    sum(written_rows) AS inserted_rows,
    sum(written_bytes) AS inserted_bytes
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query_kind = 'Insert'
  AND ${timeClause}
  AND has(tables, concat({database}, '.', {table_name}))
GROUP BY ts
ORDER BY ts
`;

/**
 * Merge activity per minute for a specific table (from part_log).
 */
export const stressSurfaceMerges = (timeClause: string) => `
SELECT
    toStartOfMinute(event_time) AS ts,
    countIf(event_type = 'MergeParts') AS merges,
    countIf(event_type = 'NewPart') AS new_parts,
    sumIf(duration_ms, event_type = 'MergeParts') AS merge_ms
FROM system.part_log
WHERE ${timeClause}
  AND database = {database}
  AND table = {table_name}
GROUP BY ts
ORDER BY ts
`;

// ─── Resource lanes queries ─────────────────────────────────────────────

/**
 * System-level merge resource usage per (minute, table) from part_log.
 * Same shape as query lanes so they can be merged into the composite stress.
 * Only includes MergeParts events (not NewPart, MovePart, etc.).
 */
export const resourceLanesMerges = (timeClause: string) => `
SELECT
    toStartOfMinute(event_time) AS ts,
    concat(database, '.', \`table\`) AS lane_id,
    count() AS merge_count,
    sum(duration_ms) AS total_duration_ms,
    sum(read_rows) AS total_read_rows,
    sum(read_bytes) AS total_read_bytes,
    sum(peak_memory_usage) AS total_memory,
    sum(ProfileEvents['RealTimeMicroseconds']) AS total_cpu_us,
    sum(ProfileEvents['IOWaitMicroseconds']) AS total_io_wait_us
FROM system.part_log
WHERE event_type = 'MergeParts'
  AND ${timeClause}
GROUP BY ts, lane_id
ORDER BY ts, lane_id
`;

/**
 * System-wide merge totals per minute (all tables combined).
 * Used alongside query totals for normalization.
 */
export const resourceLanesMergeTotals = (timeClause: string) => `
SELECT
    toStartOfMinute(event_time) AS ts,
    count() AS merge_count,
    sum(duration_ms) AS total_duration_ms,
    sum(read_rows) AS total_read_rows,
    sum(read_bytes) AS total_read_bytes,
    sum(peak_memory_usage) AS total_memory,
    sum(ProfileEvents['RealTimeMicroseconds']) AS total_cpu_us,
    sum(ProfileEvents['IOWaitMicroseconds']) AS total_io_wait_us
FROM system.part_log
WHERE event_type = 'MergeParts'
  AND ${timeClause}
GROUP BY ts
ORDER BY ts
`;

/**
 * Table-level merge resource usage per minute for drill-down.
 * Returns a single synthetic "Merges" lane for the specified table.
 */
export const resourceLanesTableMerges = (timeClause: string) => `
SELECT
    toStartOfMinute(event_time) AS ts,
    count() AS merge_count,
    sum(duration_ms) AS total_duration_ms,
    sum(read_rows) AS total_read_rows,
    sum(read_bytes) AS total_read_bytes,
    sum(peak_memory_usage) AS total_memory,
    sum(ProfileEvents['RealTimeMicroseconds']) AS total_cpu_us,
    sum(ProfileEvents['IOWaitMicroseconds']) AS total_io_wait_us
FROM system.part_log
WHERE event_type = 'MergeParts'
  AND ${timeClause}
  AND database = {database}
  AND \`table\` = {table_name}
GROUP BY ts
ORDER BY ts
`;

/**
 * System-level resource lanes: per-minute resource usage grouped by table.
 * Returns top N tables ranked by total resource usage across the time window.
 * Each row = one (time bucket, table) pair with resource totals.
 *
 * The CTE first identifies the top tables, then the main query fetches
 * per-minute breakdown only for those tables.
 */
export const resourceLanesSystem = (timeClause: string, excludeSystemTables = true) => `
WITH top_tables AS (
    SELECT
        arrayJoin(arrayFilter(
            t -> NOT startsWith(t, '_table_function.')${excludeSystemTables ? " AND NOT startsWith(t, 'system.') AND NOT startsWith(t, 'INFORMATION_SCHEMA.') AND NOT startsWith(t, 'information_schema.')" : ''},
            tables
        )) AS full_table,
        sum(ProfileEvents['RealTimeMicroseconds']) AS total_cpu,
        sum(memory_usage) AS total_mem,
        sum(read_bytes) AS total_io,
        count() AS qcount
    FROM system.query_log
    WHERE type = 'QueryFinish'
      AND query_kind IN ('Select', 'Insert')
      AND ${timeClause}
      AND is_initial_query = 1
    GROUP BY full_table
    ORDER BY total_cpu DESC
    LIMIT {max_lanes}
)
SELECT
    toStartOfMinute(event_time) AS ts,
    ft AS lane_id,
    ft AS lane_label,
    count() AS query_count,
    sum(query_duration_ms) AS total_duration_ms,
    sum(read_rows) AS total_read_rows,
    sum(read_bytes) AS total_read_bytes,
    sum(memory_usage) AS total_memory,
    sum(ProfileEvents['RealTimeMicroseconds']) AS total_cpu_us,
    sum(ProfileEvents['IOWaitMicroseconds']) AS total_io_wait_us,
    sum(ProfileEvents['SelectedMarks']) AS total_selected_marks
FROM system.query_log
ARRAY JOIN tables AS ft
INNER JOIN top_tables tt ON ft = tt.full_table
WHERE type = 'QueryFinish'
  AND query_kind IN ('Select', 'Insert')
  AND ${timeClause}
  AND is_initial_query = 1
GROUP BY ts, lane_id, lane_label
ORDER BY ts, lane_id
`;

/**
 * System-level totals per minute (all tables combined).
 * Used as the normalization baseline so table lanes show their share
 * of system-wide resource usage, not self-referential peaks.
 */
export const resourceLanesSystemTotals = (timeClause: string) => `
SELECT
    toStartOfMinute(event_time) AS ts,
    count() AS query_count,
    sum(query_duration_ms) AS total_duration_ms,
    sum(read_rows) AS total_read_rows,
    sum(read_bytes) AS total_read_bytes,
    sum(memory_usage) AS total_memory,
    sum(ProfileEvents['RealTimeMicroseconds']) AS total_cpu_us,
    sum(ProfileEvents['IOWaitMicroseconds']) AS total_io_wait_us,
    sum(ProfileEvents['SelectedMarks']) AS total_selected_marks
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query_kind IN ('Select', 'Insert')
  AND ${timeClause}
  AND is_initial_query = 1
GROUP BY ts
ORDER BY ts
`;

/**
 * Table-level resource lanes: per-minute resource usage grouped by query pattern.
 * Drill-down from system view — shows which query patterns are driving
 * a specific table's resource consumption.
 * Top N patterns ranked by total CPU usage.
 */
export const resourceLanesTable = (timeClause: string) => `
WITH top_patterns AS (
    SELECT
        normalized_query_hash,
        sum(ProfileEvents['RealTimeMicroseconds']) AS total_cpu
    FROM system.query_log
    WHERE type = 'QueryFinish'
      AND query_kind IN ('Select', 'Insert')
      AND ${timeClause}
      AND is_initial_query = 1
      AND has(tables, concat({database}, '.', {table_name}))
    GROUP BY normalized_query_hash
    ORDER BY total_cpu DESC
    LIMIT {max_lanes}
)
SELECT
    toStartOfMinute(event_time) AS ts,
    toString(q.normalized_query_hash) AS lane_id,
    substring(any(q.query), 1, 80) AS lane_label,
    count() AS query_count,
    sum(q.query_duration_ms) AS total_duration_ms,
    sum(q.read_rows) AS total_read_rows,
    sum(q.read_bytes) AS total_read_bytes,
    sum(q.memory_usage) AS total_memory,
    sum(q.ProfileEvents['RealTimeMicroseconds']) AS total_cpu_us,
    sum(q.ProfileEvents['IOWaitMicroseconds']) AS total_io_wait_us,
    sum(q.ProfileEvents['SelectedMarks']) AS total_selected_marks
FROM system.query_log q
INNER JOIN top_patterns tp ON q.normalized_query_hash = tp.normalized_query_hash
WHERE q.type = 'QueryFinish'
  AND q.query_kind IN ('Select', 'Insert')
  AND ${timeClause}
  AND q.is_initial_query = 1
  AND has(q.tables, concat({database}, '.', {table_name}))
GROUP BY ts, q.normalized_query_hash
ORDER BY ts, q.normalized_query_hash
`;

/**
 * Per (time bucket, query pattern) average duration for pattern surface.
 * Only includes the top N most frequent patterns.
 */
export const patternSurface = (timeClause: string) => `
WITH top_patterns AS (
    SELECT normalized_query_hash, sum(query_duration_ms) AS total_duration
    FROM system.query_log
    WHERE type = 'QueryFinish'
      AND query_kind = 'Select'
      AND ${timeClause}
      AND is_initial_query = 1
      AND has(tables, concat({database}, '.', {table_name}))
    GROUP BY normalized_query_hash
    ORDER BY total_duration DESC
    LIMIT 12
)
SELECT
    toStartOfMinute(event_time) AS ts,
    toString(q.normalized_query_hash) AS normalized_query_hash,
    avg(query_duration_ms) AS avg_duration_ms,
    count() AS query_count,
    avg(memory_usage) AS avg_memory,
    any(query) AS sample_query
FROM system.query_log q
INNER JOIN top_patterns tp ON q.normalized_query_hash = tp.normalized_query_hash
WHERE q.type = 'QueryFinish'
  AND q.query_kind = 'Select'
  AND ${timeClause}
  AND q.is_initial_query = 1
  AND has(q.tables, concat({database}, '.', {table_name}))
GROUP BY ts, q.normalized_query_hash
ORDER BY ts, q.normalized_query_hash
`;
