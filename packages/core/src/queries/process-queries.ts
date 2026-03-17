/**
 * SQL query and row-mapping for per-second process samples
 * from tracehouse.processes_history.
 *
 * Used by the frontend useProcessSamples hook and validated
 * by integration tests.
 */

// ── Types ──

export interface ProcessSample {
  /** Seconds since query start */
  t: number;
  elapsed: number;
  /** Number of active threads */
  thread_count: number;

  // --- Cumulative (running totals at sample time) ---
  memory_mb: number;
  peak_memory_mb: number;
  read_rows: number;
  written_rows: number;
  read_bytes: number;
  /** Cumulative CPU time in microseconds */
  cpu_us: number;
  /** Cumulative I/O wait in microseconds */
  io_wait_us: number;
  /** Cumulative network send bytes */
  net_send_bytes: number;
  /** Cumulative network recv bytes */
  net_recv_bytes: number;

  // --- Per-interval deltas (between consecutive samples) ---
  /** CPU cores used in this interval (delta of OSCPUVirtualTimeMicroseconds / 1e6) */
  d_cpu_cores: number;
  /** I/O wait seconds in this interval */
  d_io_wait_s: number;
  /** MB read in this interval */
  d_read_mb: number;
  /** Rows read in this interval */
  d_read_rows: number;
  /** Rows written in this interval */
  d_written_rows: number;
  /** Network send KB in this interval */
  d_net_send_kb: number;
  /** Network recv KB in this interval */
  d_net_recv_kb: number;
}

// ── SQL ──

/**
 * Query that returns both cumulative and per-interval delta values
 * for a given query_id (or initial_query_id for distributed sub-queries).
 *
 * Placeholder: {qid:String} — the query ID to filter on.
 */
export const PROCESS_SAMPLES_SQL = `
SELECT
    toFloat64(dateDiff('millisecond', min_time, sample_time)) / 1000 AS t,
    elapsed,
    thread_count,

    -- cumulative
    memory_usage / (1024 * 1024) AS memory_mb,
    peak_memory_usage / (1024 * 1024) AS peak_memory_mb,
    read_rows,
    written_rows,
    read_bytes,
    pe_cpu AS cpu_us,
    pe_io_wait AS io_wait_us,
    pe_net_send AS net_send_bytes,
    pe_net_recv AS net_recv_bytes,

    -- per-interval deltas
    greatest(
      (pe_cpu - lagInFrame(pe_cpu, 1, 0) OVER (ORDER BY sample_time)) / 1000000,
      0
    ) AS d_cpu_cores,
    greatest(
      (pe_io_wait - lagInFrame(pe_io_wait, 1, 0) OVER (ORDER BY sample_time)) / 1000000,
      0
    ) AS d_io_wait_s,
    greatest(
      (read_bytes - lagInFrame(read_bytes, 1, 0) OVER (ORDER BY sample_time)) / (1024 * 1024),
      0
    ) AS d_read_mb,
    greatest(
      read_rows - lagInFrame(read_rows, 1, 0) OVER (ORDER BY sample_time),
      0
    ) AS d_read_rows,
    greatest(
      written_rows - lagInFrame(written_rows, 1, 0) OVER (ORDER BY sample_time),
      0
    ) AS d_written_rows,
    greatest(
      (pe_net_send - lagInFrame(pe_net_send, 1, 0) OVER (ORDER BY sample_time)) / 1024,
      0
    ) AS d_net_send_kb,
    greatest(
      (pe_net_recv - lagInFrame(pe_net_recv, 1, 0) OVER (ORDER BY sample_time)) / 1024,
      0
    ) AS d_net_recv_kb
FROM (
    SELECT
        sample_time,
        min(sample_time) OVER () AS min_time,
        elapsed,
        memory_usage,
        peak_memory_usage,
        read_bytes,
        read_rows,
        written_rows,
        length(thread_ids) AS thread_count,
        ProfileEvents['OSCPUVirtualTimeMicroseconds'] AS pe_cpu,
        ProfileEvents['OSCPUWaitMicroseconds'] AS pe_io_wait,
        ProfileEvents['NetworkSendBytes'] AS pe_net_send,
        ProfileEvents['NetworkReceiveBytes'] AS pe_net_recv
    FROM tracehouse.processes_history
    WHERE query_id = {qid:String}
       OR initial_query_id = {qid:String}
    ORDER BY sample_time
)
ORDER BY sample_time
`;

// ── Row mapping ──

/**
 * Map a raw ClickHouse row (string/number values) to a typed ProcessSample.
 */
export function mapProcessSampleRow(r: Record<string, unknown>): ProcessSample {
  return {
    t: Number(r.t) || 0,
    elapsed: Number(r.elapsed) || 0,
    thread_count: Number(r.thread_count) || 0,
    // cumulative
    memory_mb: Number(r.memory_mb) || 0,
    peak_memory_mb: Number(r.peak_memory_mb) || 0,
    read_rows: Number(r.read_rows) || 0,
    written_rows: Number(r.written_rows) || 0,
    read_bytes: Number(r.read_bytes) || 0,
    cpu_us: Number(r.cpu_us) || 0,
    io_wait_us: Number(r.io_wait_us) || 0,
    net_send_bytes: Number(r.net_send_bytes) || 0,
    net_recv_bytes: Number(r.net_recv_bytes) || 0,
    // deltas
    d_cpu_cores: Number(r.d_cpu_cores) || 0,
    d_io_wait_s: Number(r.d_io_wait_s) || 0,
    d_read_mb: Number(r.d_read_mb) || 0,
    d_read_rows: Number(r.d_read_rows) || 0,
    d_written_rows: Number(r.d_written_rows) || 0,
    d_net_send_kb: Number(r.d_net_send_kb) || 0,
    d_net_recv_kb: Number(r.d_net_recv_kb) || 0,
  };
}
