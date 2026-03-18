/**
 * SQL query and row-mapping for merge samples
 * from tracehouse.merges_history.
 *
 * Tracks in-progress merge operations over time, capturing progress,
 * I/O throughput, and memory usage at each sampling interval.
 *
 * Delta fields are normalized to per-second rates regardless of the
 * sampling interval (e.g. 1s, 5s, 10s).
 *
 * Used by the frontend and validated by integration tests.
 */

// ── Types ──

export interface MergeSample {
  /** Seconds since first sample for this merge */
  t: number;
  elapsed: number;
  /** Merge progress 0..1 */
  progress: number;

  // --- Merge identity ---
  database: string;
  table: string;
  result_part_name: string;
  partition_id: string;
  num_parts: number;
  is_mutation: boolean;
  merge_type: string;
  merge_algorithm: string;

  // --- Size (totals for the merge, constant per merge) ---
  total_size_bytes_compressed: number;
  total_size_bytes_uncompressed: number;
  total_size_marks: number;

  // --- Cumulative I/O at sample time ---
  rows_read: number;
  bytes_read_uncompressed: number;
  rows_written: number;
  bytes_written_uncompressed: number;
  columns_written: number;

  // --- Resources ---
  memory_usage: number;

  // --- Per-second rates (deltas normalized by dt) ---
  /** Rows read per second */
  d_rows_read: number;
  /** MB/s read (uncompressed) */
  d_read_mb: number;
  /** Rows written per second */
  d_rows_written: number;
  /** MB/s written (uncompressed) */
  d_written_mb: number;
}

// ── SQL ──

/**
 * Build SQL to fetch merge samples for a specific merge operation,
 * identified by (database, table, result_part_name).
 *
 * When resultPartName is provided, fetches samples for that single merge.
 * When omitted, fetches all merges for the given table (partitioned by result_part_name).
 */
export function buildMergeSamplesSQL(opts: {
  database: string;
  table: string;
  resultPartName?: string;
}): string {
  const { database, table, resultPartName } = opts;
  const escDb = database.replace(/'/g, "''");
  const escTable = table.replace(/'/g, "''");

  const whereClause = resultPartName
    ? `database = '${escDb}' AND table = '${escTable}' AND result_part_name = '${resultPartName.replace(/'/g, "''")}'`
    : `database = '${escDb}' AND table = '${escTable}'`;

  const partition = resultPartName ? '' : 'PARTITION BY result_part_name';

  return `
SELECT
    result_part_name,
    t, elapsed, progress,
    database, table, partition_id,
    num_parts, is_mutation, merge_type, merge_algorithm,
    total_size_bytes_compressed, total_size_bytes_uncompressed, total_size_marks,
    rows_read, bytes_read_uncompressed, rows_written, bytes_written_uncompressed, columns_written,
    memory_usage,
    greatest(raw_d_rows_read / dt, 0)    AS d_rows_read,
    greatest(raw_d_read_mb / dt, 0)      AS d_read_mb,
    greatest(raw_d_rows_written / dt, 0) AS d_rows_written,
    greatest(raw_d_written_mb / dt, 0)   AS d_written_mb
FROM (
    SELECT
        result_part_name,
        toFloat64(dateDiff('millisecond', min_time, sample_time)) / 1000 AS t,
        elapsed, progress,
        database, table, partition_id,
        num_parts, is_mutation, merge_type, merge_algorithm,
        total_size_bytes_compressed, total_size_bytes_uncompressed, total_size_marks,
        rows_read, bytes_read_uncompressed, rows_written, bytes_written_uncompressed, columns_written,
        memory_usage,
        -- dt: seconds since previous sample (floor 0.1s to prevent div-by-zero)
        greatest(
            toFloat64(dateDiff('millisecond',
                lagInFrame(sample_time, 1, sample_time) OVER w,
                sample_time
            )) / 1000,
            0.1
        ) AS dt,
        -- raw deltas (lag defaults to self so first sample = 0)
        toFloat64(rows_read - lagInFrame(rows_read, 1, rows_read) OVER w) AS raw_d_rows_read,
        (bytes_read_uncompressed - lagInFrame(bytes_read_uncompressed, 1, bytes_read_uncompressed) OVER w) / (1024 * 1024) AS raw_d_read_mb,
        toFloat64(rows_written - lagInFrame(rows_written, 1, rows_written) OVER w) AS raw_d_rows_written,
        (bytes_written_uncompressed - lagInFrame(bytes_written_uncompressed, 1, bytes_written_uncompressed) OVER w) / (1024 * 1024) AS raw_d_written_mb
    FROM (
        SELECT
            result_part_name, sample_time,
            min(sample_time) OVER (${partition}) AS min_time,
            elapsed, progress,
            database, table, partition_id,
            num_parts, is_mutation, merge_type, merge_algorithm,
            total_size_bytes_compressed, total_size_bytes_uncompressed, total_size_marks,
            rows_read, bytes_read_uncompressed, rows_written, bytes_written_uncompressed, columns_written,
            memory_usage
        FROM tracehouse.merges_history
        WHERE ${whereClause}
        ORDER BY result_part_name, sample_time
    )
    WINDOW w AS (${partition} ORDER BY sample_time)
)
ORDER BY result_part_name, t
`;
}

// ── Row mapping ──

/**
 * Map a raw ClickHouse row (string/number values) to a typed MergeSample.
 */
export function mapMergeSampleRow(r: Record<string, unknown>): MergeSample {
  return {
    t: Number(r.t) || 0,
    elapsed: Number(r.elapsed) || 0,
    progress: Number(r.progress) || 0,

    database: String(r.database || ''),
    table: String(r.table || ''),
    result_part_name: String(r.result_part_name || ''),
    partition_id: String(r.partition_id || ''),
    num_parts: Number(r.num_parts) || 0,
    is_mutation: Number(r.is_mutation) === 1,
    merge_type: String(r.merge_type || ''),
    merge_algorithm: String(r.merge_algorithm || ''),

    total_size_bytes_compressed: Number(r.total_size_bytes_compressed) || 0,
    total_size_bytes_uncompressed: Number(r.total_size_bytes_uncompressed) || 0,
    total_size_marks: Number(r.total_size_marks) || 0,

    rows_read: Number(r.rows_read) || 0,
    bytes_read_uncompressed: Number(r.bytes_read_uncompressed) || 0,
    rows_written: Number(r.rows_written) || 0,
    bytes_written_uncompressed: Number(r.bytes_written_uncompressed) || 0,
    columns_written: Number(r.columns_written) || 0,

    memory_usage: Number(r.memory_usage) || 0,

    d_rows_read: Number(r.d_rows_read) || 0,
    d_read_mb: Number(r.d_read_mb) || 0,
    d_rows_written: Number(r.d_rows_written) || 0,
    d_written_mb: Number(r.d_written_mb) || 0,
  };
}
