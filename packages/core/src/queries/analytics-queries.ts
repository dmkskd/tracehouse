/**
 * SQL queries for the Analytics feature — table-level ordering key efficiency.
 *
 * Uses system.query_log.tables (Array(String), available since CH 22.8)
 * joined with system.tables for ORDER BY / sorting key metadata
 * and system.parts for row/part counts.
 */

/**
 * Per-table ordering key efficiency analysis.
 *
 * Explodes the `tables` array from query_log so each table gets attributed,
 * then joins with system.tables for sorting/primary key and system.parts for counts.
 *
 * Params: lookback_days (UInt32), min_query_count (UInt32)
 */
export const TABLE_ORDERING_KEY_EFFICIENCY = `
SELECT
    t.tbl_database AS tbl_database,
    t.tbl_name AS tbl_name,
    t.query_count,
    t.avg_pruning_pct,
    t.poor_pruning_queries,
    t.avg_parts_scanned_pct,
    t.total_rows_read,
    t.total_marks_scanned,
    t.total_marks_available,
    t.total_pk_filter_us,
    t.avg_duration_ms,
    t.total_cpu_us,
    t.avg_memory_bytes,
    tbl.sorting_key AS sorting_key,
    tbl.primary_key AS primary_key,
    p.table_rows AS table_rows,
    p.table_marks AS table_marks,
    p.active_parts AS active_parts
FROM (
    SELECT
        splitByChar('.', full_table_name)[1] AS tbl_database,
        arrayStringConcat(arraySlice(splitByChar('.', full_table_name), 2), '.') AS tbl_name,
        count() AS query_count,
        avg(
            if(ProfileEvents['SelectedMarksTotal'] > 0 OR ProfileEvents['SelectedPartsTotal'] > 0,
               (1 - if(ProfileEvents['SelectedPartsTotal'] > 0, ProfileEvents['SelectedParts'] / ProfileEvents['SelectedPartsTotal'], 1)
                  * if(ProfileEvents['SelectedMarksTotal'] > 0, ProfileEvents['SelectedMarks'] / ProfileEvents['SelectedMarksTotal'], 1)
               ) * 100,
               NULL)
        ) AS avg_pruning_pct,
        countIf(
            (ProfileEvents['SelectedMarksTotal'] > 0 OR ProfileEvents['SelectedPartsTotal'] > 0)
            AND (1 - if(ProfileEvents['SelectedPartsTotal'] > 0, ProfileEvents['SelectedParts'] / ProfileEvents['SelectedPartsTotal'], 1)
                   * if(ProfileEvents['SelectedMarksTotal'] > 0, ProfileEvents['SelectedMarks'] / ProfileEvents['SelectedMarksTotal'], 1)
            ) < 0.5
        ) AS poor_pruning_queries,
        avg(
            if(ProfileEvents['SelectedPartsTotal'] > 0,
               ProfileEvents['SelectedParts'] / ProfileEvents['SelectedPartsTotal'] * 100,
               NULL)
        ) AS avg_parts_scanned_pct,
        sum(read_rows) AS total_rows_read,
        sum(ProfileEvents['SelectedMarks']) AS total_marks_scanned,
        sum(ProfileEvents['SelectedMarksTotal']) AS total_marks_available,
        sum(ProfileEvents['FilteringMarksWithPrimaryKeyMicroseconds']) AS total_pk_filter_us,
        avg(query_duration_ms) AS avg_duration_ms,
        sum(ProfileEvents['OSCPUVirtualTimeMicroseconds']) AS total_cpu_us,
        avg(memory_usage) AS avg_memory_bytes
    FROM {{cluster_aware:system.query_log}}
    ARRAY JOIN tables AS full_table_name
    WHERE type = 'QueryFinish'
        AND query_kind = 'Select'
        AND is_initial_query = 1
        AND event_date >= today() - {lookback_days:UInt32}
    GROUP BY tbl_database, tbl_name
    HAVING query_count >= {min_query_count:UInt32}
) t
LEFT JOIN (
    SELECT database, name, any(sorting_key) AS sorting_key, any(primary_key) AS primary_key
    FROM {{cluster_metadata:system.tables}}
    GROUP BY database, name
) AS tbl
    ON tbl.database = t.tbl_database AND tbl.name = t.tbl_name
LEFT JOIN (
    SELECT
        database,
        table,
        count() AS active_parts,
        sum(part_rows) AS table_rows,
        sum(part_marks) AS table_marks
    FROM (
        SELECT database, table, name, any(rows) AS part_rows, any(marks) AS part_marks
        FROM {{cluster_metadata:system.parts}}
        WHERE active = 1
        GROUP BY database, table, name
    )
    GROUP BY database, table
) p ON p.database = t.tbl_database AND p.table = t.tbl_name
ORDER BY t.query_count DESC
`;


/**
 * Per-query-pattern (normalized_query_hash) breakdown for a specific table.
 *
 * Shows each distinct query shape hitting the table, with its own pruning stats.
 * This is the drill-down from the table-level view.
 *
 * Params: tbl_database (String), tbl_name (String), lookback_days (UInt32)
 */
export const TABLE_QUERY_PATTERNS = `
SELECT
    toString(normalized_query_hash) AS query_hash,
    any(query) AS sample_query,
    count() AS execution_count,
    avg(
        if(ProfileEvents['SelectedMarksTotal'] > 0 OR ProfileEvents['SelectedPartsTotal'] > 0,
           (1 - if(ProfileEvents['SelectedPartsTotal'] > 0, ProfileEvents['SelectedParts'] / ProfileEvents['SelectedPartsTotal'], 1)
              * if(ProfileEvents['SelectedMarksTotal'] > 0, ProfileEvents['SelectedMarks'] / ProfileEvents['SelectedMarksTotal'], 1)
           ) * 100,
           NULL)
    ) AS avg_pruning_pct,
    countIf(
        (ProfileEvents['SelectedMarksTotal'] > 0 OR ProfileEvents['SelectedPartsTotal'] > 0)
        AND (1 - if(ProfileEvents['SelectedPartsTotal'] > 0, ProfileEvents['SelectedParts'] / ProfileEvents['SelectedPartsTotal'], 1)
               * if(ProfileEvents['SelectedMarksTotal'] > 0, ProfileEvents['SelectedMarks'] / ProfileEvents['SelectedMarksTotal'], 1)
        ) < 0.5
    ) AS poor_pruning_count,
    avg(query_duration_ms) AS avg_duration_ms,
    quantile(0.50)(query_duration_ms) AS p50_duration_ms,
    quantile(0.95)(query_duration_ms) AS p95_duration_ms,
    quantile(0.99)(query_duration_ms) AS p99_duration_ms,
    avg(read_rows) AS avg_rows_read,
    sum(ProfileEvents['SelectedMarks']) AS total_marks_scanned,
    sum(ProfileEvents['SelectedMarksTotal']) AS total_marks_available,
    sum(ProfileEvents['OSCPUVirtualTimeMicroseconds']) AS total_cpu_us,
    avg(memory_usage) AS avg_memory_bytes,
    quantile(0.50)(memory_usage) AS p50_memory_bytes,
    quantile(0.95)(memory_usage) AS p95_memory_bytes,
    quantile(0.99)(memory_usage) AS p99_memory_bytes,
    min(query_start_time) AS first_seen,
    max(query_start_time) AS last_seen,
    cast(sumMap(map(user, toUInt64(1))) AS Map(String, UInt64)) AS user_breakdown
FROM {{cluster_aware:system.query_log}}
ARRAY JOIN tables AS full_table_name
WHERE type = 'QueryFinish'
    AND query_kind = 'Select'
    AND is_initial_query = 1
    AND event_date >= today() - {lookback_days:UInt32}
    AND splitByChar('.', full_table_name)[1] = {tbl_database}
    AND arrayStringConcat(arraySlice(splitByChar('.', full_table_name), 2), '.') = {tbl_name}
GROUP BY query_hash
ORDER BY execution_count DESC
LIMIT 50
`;
