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
): { clause: string; params: Record<string, unknown> } {
  if (opts.startTime && opts.endTime) {
    return {
      clause: `${column} BETWEEN {start_time} AND {end_time}`,
      params: { start_time: opts.startTime.replace('T', ' '), end_time: opts.endTime.replace('T', ' ') },
    };
  }
  const hours = opts.hours ?? 24;
  return {
    clause: `${column} > now() - INTERVAL {hours} HOUR`,
    params: { hours },
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
