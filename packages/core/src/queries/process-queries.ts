/**
 * SQL query and row-mapping for process samples
 * from tracehouse.processes_history.
 *
 * Delta fields are normalized to per-second rates regardless of the
 * sampling interval (e.g. 0.5s, 1s, 10s).
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

  // --- Per-second rates (deltas normalized by dt between consecutive samples) ---
  /** CPU cores (delta µs / 1e6 / dt) */
  d_cpu_cores: number;
  /** I/O wait seconds per second of wall time */
  d_io_wait_s: number;
  /** MB/s read throughput */
  d_read_mb: number;
  /** Rows/s read */
  d_read_rows: number;
  /** Rows/s written */
  d_written_rows: number;
  /** Network send KB/s */
  d_net_send_kb: number;
  /** Network recv KB/s */
  d_net_recv_kb: number;
}

// ── SQL ──

/**
 * Build SQL to fetch process samples for one or more query IDs.
 * For multiple IDs, rows are tagged with query_id and window functions
 * are partitioned per query so each query's time is relative to its own start.
 *
 * Single-query mode: pass one ID, returns ProcessSample rows.
 * Multi-query mode: pass N IDs, returns TaggedProcessSample rows (with query_id column).
 */
export function buildProcessSamplesSQL(queryIds: string[]): string {
  const multi = queryIds.length > 1;
  const escaped = queryIds.map(id => `'${id.replace(/'/g, "''")}'`);
  const whereClause = multi
    ? `query_id IN (${escaped.join(', ')}) OR initial_query_id IN (${escaped.join(', ')})`
    : `query_id = ${escaped[0]} OR initial_query_id = ${escaped[0]}`;
  const partition = multi ? 'PARTITION BY initial_query_id' : '';
  return `
SELECT
    ${multi ? 'query_id,' : ''}
    t, elapsed, thread_count,
    memory_mb, peak_memory_mb,
    read_rows, written_rows, read_bytes,
    cpu_us, io_wait_us, net_send_bytes, net_recv_bytes,
    greatest(raw_d_cpu / dt, 0) AS d_cpu_cores,
    greatest(raw_d_io / dt, 0) AS d_io_wait_s,
    greatest(raw_d_read_mb / dt, 0) AS d_read_mb,
    greatest(raw_d_read_rows / dt, 0) AS d_read_rows,
    greatest(raw_d_written_rows / dt, 0) AS d_written_rows,
    greatest(raw_d_net_send / dt, 0) AS d_net_send_kb,
    greatest(raw_d_net_recv / dt, 0) AS d_net_recv_kb
FROM (
    SELECT
        ${multi ? 'initial_query_id AS query_id,' : ''}
        toFloat64(dateDiff('millisecond', min_time, sample_time)) / 1000 AS t,
        elapsed, length(thread_ids) AS thread_count,
        memory_usage / (1024 * 1024) AS memory_mb,
        peak_memory_usage / (1024 * 1024) AS peak_memory_mb,
        read_rows, written_rows, read_bytes,
        pe_cpu AS cpu_us, pe_io_wait AS io_wait_us,
        pe_net_send AS net_send_bytes, pe_net_recv AS net_recv_bytes,
        -- dt: seconds since previous sample (floor 0.1s to prevent div-by-zero)
        greatest(
            toFloat64(dateDiff('millisecond',
                lagInFrame(sample_time, 1, sample_time) OVER w,
                sample_time
            )) / 1000,
            0.1
        ) AS dt,
        -- raw deltas (lag defaults to self so first sample = 0)
        (pe_cpu - lagInFrame(pe_cpu, 1, pe_cpu) OVER w) / 1000000 AS raw_d_cpu,
        (pe_io_wait - lagInFrame(pe_io_wait, 1, pe_io_wait) OVER w) / 1000000 AS raw_d_io,
        (read_bytes - lagInFrame(read_bytes, 1, read_bytes) OVER w) / (1024 * 1024) AS raw_d_read_mb,
        toFloat64(read_rows - lagInFrame(read_rows, 1, read_rows) OVER w) AS raw_d_read_rows,
        toFloat64(written_rows - lagInFrame(written_rows, 1, written_rows) OVER w) AS raw_d_written_rows,
        (pe_net_send - lagInFrame(pe_net_send, 1, pe_net_send) OVER w) / 1024 AS raw_d_net_send,
        (pe_net_recv - lagInFrame(pe_net_recv, 1, pe_net_recv) OVER w) / 1024 AS raw_d_net_recv
    FROM (
        SELECT
            ${multi ? 'initial_query_id,' : ''} sample_time,
            min(sample_time) OVER (${partition}) AS min_time,
            elapsed, memory_usage, peak_memory_usage,
            read_bytes, read_rows, written_rows, thread_ids,
            ProfileEvents['OSCPUVirtualTimeMicroseconds'] AS pe_cpu,
            ProfileEvents['OSCPUWaitMicroseconds'] AS pe_io_wait,
            ProfileEvents['NetworkSendBytes'] AS pe_net_send,
            ProfileEvents['NetworkReceiveBytes'] AS pe_net_recv
        FROM tracehouse.processes_history
        WHERE ${whereClause}
        ORDER BY ${multi ? 'initial_query_id, ' : ''}sample_time
    )
    WINDOW w AS (${partition} ORDER BY sample_time)
)
ORDER BY ${multi ? 'query_id, ' : ''}t
`;
}

/** @deprecated Use buildProcessSamplesSQL([queryId]) instead */
export const PROCESS_SAMPLES_SQL = '/* use buildProcessSamplesSQL */';

export interface TaggedProcessSample extends ProcessSample {
  query_id: string;
}

export function mapTaggedProcessSampleRow(r: Record<string, unknown>): TaggedProcessSample {
  return {
    query_id: String(r.query_id || ''),
    ...mapProcessSampleRow(r),
  };
}

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

// ── Timeline comparison data builder ──

export interface TimelineMetricLine {
  key: keyof ProcessSample;
  suffix: string;         // appended to line label, e.g. " send"
  strokeDasharray?: string; // dashed for secondary lines
}

export interface TimelineMetric {
  /** Chart ID — used as key for the chart container */
  id: string;
  label: string;
  unit: string;
  formatter: (v: number) => string;
  /** One or more data lines to draw on this chart */
  lines: TimelineMetricLine[];
}

export const TIMELINE_METRICS: TimelineMetric[] = [
  { id: 'd_cpu_cores', label: 'CPU Cores', unit: 'cores', formatter: v => `${v.toFixed(2)} cores`,
    lines: [{ key: 'd_cpu_cores', suffix: '' }] },
  { id: 'memory_mb', label: 'Memory', unit: 'MB', formatter: v => `${v.toFixed(1)} MB`,
    lines: [{ key: 'memory_mb', suffix: '' }] },
  { id: 'd_read_mb', label: 'read_bytes', unit: 'MB/s', formatter: v => `${v.toFixed(2)} MB/s`,
    lines: [{ key: 'd_read_mb', suffix: '' }] },
  { id: 'd_io_wait_s', label: 'I/O Wait', unit: 's', formatter: v => `${v.toFixed(3)} s`,
    lines: [{ key: 'd_io_wait_s', suffix: '' }] },
  { id: 'network', label: 'Network', unit: 'KB/s', formatter: v => `${v.toFixed(1)} KB/s`,
    lines: [
      { key: 'd_net_send_kb', suffix: ' send' },
      { key: 'd_net_recv_kb', suffix: ' recv', strokeDasharray: '4 2' },
    ] },
];

export interface TimelineChartPoint {
  t: number;
  [metricQueryKey: string]: number | null;
}

export interface TimelineChartData {
  /** Per-query sample arrays, keyed by query_id */
  perQuery: Map<string, TaggedProcessSample[]>;
  /** Unified time-axis data points with metric_queryIdx keys */
  points: TimelineChartPoint[];
  /** Metrics that have at least one non-zero value */
  activeMetrics: TimelineMetric[];
}

/**
 * Transform raw tagged samples into chart-ready data for N-query timeline overlay.
 * Pure function — no React dependency.
 *
 * @param samples - Flat array of TaggedProcessSample from buildProcessSamplesSQL
 * @param queryIds - Ordered list of query IDs (index determines the suffix _0, _1, etc.)
 * @param metrics - Which metrics to include (defaults to TIMELINE_METRICS)
 */
export function buildTimelineChartData(
  samples: TaggedProcessSample[],
  queryIds: string[],
  metrics: TimelineMetric[] = TIMELINE_METRICS,
): TimelineChartData {
  // Group samples by query_id
  const perQuery = new Map<string, TaggedProcessSample[]>();
  for (const s of samples) {
    let arr = perQuery.get(s.query_id);
    if (!arr) {
      arr = [];
      perQuery.set(s.query_id, arr);
    }
    arr.push(s);
  }

  // Collect all unique time points (rounded to 0.1s)
  const allTimes = new Set<number>();
  for (const arr of perQuery.values()) {
    for (const s of arr) allTimes.add(Math.round(s.t * 10) / 10);
  }
  const sortedTimes = Array.from(allTimes).sort((a, b) => a - b);

  // Adaptive match tolerance based on actual sample spacing
  const tolerance = sortedTimes.length >= 2
    ? (sortedTimes[sortedTimes.length - 1] - sortedTimes[0]) / (sortedTimes.length - 1) * 0.6
    : 0.6;

  // Build chart points
  const points: TimelineChartPoint[] = sortedTimes.map(t => {
    const point: TimelineChartPoint = { t };
    queryIds.forEach((qid, idx) => {
      const qSamples = perQuery.get(qid);
      const match = qSamples?.find(s => Math.abs(Math.round(s.t * 10) / 10 - t) < tolerance);
      for (const metric of metrics) {
        for (const line of metric.lines) {
          point[`${line.key}_${idx}`] = match ? Number(match[line.key]) : null;
        }
      }
    });
    return point;
  });

  // Filter to metrics with at least one non-zero value across any line
  const activeMetrics = metrics.filter(metric =>
    metric.lines.some(line =>
      points.some(point =>
        queryIds.some((_, idx) => {
          const v = point[`${line.key}_${idx}`];
          return v !== null && v !== undefined && v > 0;
        })
      )
    )
  );

  return { perQuery, points, activeMetrics };
}
