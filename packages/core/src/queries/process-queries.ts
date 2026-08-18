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

import { escapeValue } from './builder.js';

// ── Types ──

export interface ProcessSample {
  /** Seconds since query start */
  t: number;
  elapsed: number;
  /**
   * length(thread_ids) — "the list of identifiers of all threads which
   * participated in this query" (ClickHouse docs). Cumulative, not concurrency:
   * ClickHouse pools threads and re-attaches them, so pooled workers
   * (VFSRead, Reader, ParquetDecoder) stay in the list after finishing, and the
   * value climbs monotonically through a query.
   *
   * Use peak_threads_usage for concurrency — "maximum count of simultaneous
   * threads executing the query". Never present this as "threads running now",
   * and never derive per-interval concurrency from it.
   */
  thread_count: number;

  // --- Cumulative (running totals at sample time) ---
  memory_mb: number;
  peak_memory_mb: number;
  read_rows: number;
  written_rows: number;
  read_bytes: number;
  /** Cumulative CPU time in microseconds */
  cpu_us: number;
  /** Cumulative block-device I/O wait in microseconds (OSIOWaitMicroseconds) */
  io_wait_us: number;
  /** Cumulative CPU run-queue wait in microseconds (OSCPUWaitMicroseconds) */
  cpu_wait_us: number;
  /** Cumulative time blocked reading from sockets (NetworkReceiveElapsedMicroseconds) */
  net_recv_wait_us: number;
  /** Cumulative time blocked writing to sockets (NetworkSendElapsedMicroseconds) */
  net_send_wait_us: number;
  /** Cumulative network send bytes */
  net_send_bytes: number;
  /** Cumulative network recv bytes */
  net_recv_bytes: number;

  // --- Per-second rates (deltas normalized by dt between consecutive samples) ---
  /**
   * Concurrency-style wait/work rates: delta µs / 1e6 / dt. A value of 1.0 means
   * one thread's worth of the sampling window was spent this way, so these are
   * directly comparable to each other and to d_cpu_cores.
   */
  /** CPU cores (delta µs / 1e6 / dt) */
  d_cpu_cores: number;
  /** Threads-worth of blocking on real disk I/O */
  d_io_wait_s: number;
  /** Threads-worth of waiting for a free CPU (run-queue contention) */
  d_cpu_wait_s: number;
  /** Threads-worth of blocking on socket reads */
  d_net_recv_wait_s: number;
  /** Threads-worth of blocking on socket writes */
  d_net_send_wait_s: number;
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

  /**
   * True when a thread-time rate in this interval exceeded what its thread
   * count could produce and was clamped. Caused by a thread detaching and
   * merging its accumulated counters into one interval — the displayed rate is
   * a floor, not the raw counter delta.
   */
  rate_clamped: boolean;
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
  const escaped = queryIds.map(id => `'${escapeValue(id)}'`);
  const whereClause = multi
    ? `query_id IN (${escaped.join(', ')}) OR initial_query_id IN (${escaped.join(', ')})`
    : `query_id = ${escaped[0]} OR initial_query_id = ${escaped[0]}`;
  const minTimePartition = multi ? 'PARTITION BY initial_query_id' : '';
  const windowPartition = multi ? 'PARTITION BY initial_query_id, query_id' : 'PARTITION BY query_id';
  const groupBy = multi ? 'initial_query_id, t' : 't';
  return `
SELECT
    ${multi ? 'initial_query_id AS query_id,' : ''}
    t,
    max(elapsed) AS elapsed,
    sum(thread_count) AS thread_count,
    sum(memory_mb) AS memory_mb,
    max(peak_memory_mb) AS peak_memory_mb,
    sum(read_rows) AS read_rows,
    sum(written_rows) AS written_rows,
    sum(read_bytes) AS read_bytes,
    sum(cpu_us) AS cpu_us,
    sum(io_wait_us) AS io_wait_us,
    sum(cpu_wait_us) AS cpu_wait_us,
    sum(net_recv_wait_us) AS net_recv_wait_us,
    sum(net_send_wait_us) AS net_send_wait_us,
    sum(net_send_bytes) AS net_send_bytes,
    sum(net_recv_bytes) AS net_recv_bytes,
    -- Thread-time rates are clamped to the interval's thread bound: a process
    -- cannot spend more than N threads-worth of a second with N threads. See
    -- thread_bound below for why the raw counters can exceed it.
    sum(least(greatest(raw_d_cpu / dt, 0), thread_bound)) AS d_cpu_cores,
    sum(least(greatest(raw_d_io / dt, 0), thread_bound)) AS d_io_wait_s,
    sum(least(greatest(raw_d_cpu_wait / dt, 0), thread_bound)) AS d_cpu_wait_s,
    sum(least(greatest(raw_d_net_recv_wait / dt, 0), thread_bound)) AS d_net_recv_wait_s,
    sum(least(greatest(raw_d_net_send_wait / dt, 0), thread_bound)) AS d_net_send_wait_s,
    max(greatest(raw_d_cpu, raw_d_io, raw_d_cpu_wait, raw_d_net_recv_wait, raw_d_net_send_wait) / dt
        > thread_bound) AS rate_clamped,
    sum(greatest(raw_d_read_mb / dt, 0)) AS d_read_mb,
    sum(greatest(raw_d_read_rows / dt, 0)) AS d_read_rows,
    sum(greatest(raw_d_written_rows / dt, 0)) AS d_written_rows,
    sum(greatest(raw_d_net_send / dt, 0)) AS d_net_send_kb,
    sum(greatest(raw_d_net_recv / dt, 0)) AS d_net_recv_kb
FROM (
    SELECT
        ${multi ? 'initial_query_id,' : ''} query_id,
        toFloat64(dateDiff('millisecond', min_time, sample_time)) / 1000 AS t,
        elapsed, length(thread_ids) AS thread_count, peak_threads_usage,
        memory_usage / (1024 * 1024) AS memory_mb,
        peak_memory_usage / (1024 * 1024) AS peak_memory_mb,
        read_rows, written_rows, read_bytes,
        pe_cpu AS cpu_us, pe_io_wait AS io_wait_us, pe_cpu_wait AS cpu_wait_us,
        pe_net_recv_wait AS net_recv_wait_us, pe_net_send_wait AS net_send_wait_us,
        pe_net_send AS net_send_bytes, pe_net_recv AS net_recv_bytes,
        -- dt: seconds since previous sample (floor 0.1s to prevent div-by-zero)
        greatest(
            toFloat64(dateDiff('millisecond',
                lagInFrame(sample_time, 1, sample_time) OVER w,
                sample_time
            )) / 1000,
            0.1
        ) AS dt,
        -- Upper bound on thread-time rates for this interval: the largest number
        -- of threads that ran this query at once.
        --
        -- peak_threads_usage, not length(thread_ids). The docs are explicit:
        -- thread_ids is "the list of identifiers of all threads which
        -- participated in this query" — cumulative — while peak_threads_usage is
        -- "maximum count of simultaneous threads executing the query". ClickHouse
        -- pools and re-attaches threads (one query showed 424 query_thread_log
        -- rows across 38 distinct OS threads), so the cumulative list climbs all
        -- query long and gave a ceiling roughly double the real one.
        --
        -- Falls back to the cumulative count when peak_threads_usage is absent —
        -- some managed providers strip it — which is loose but never clamps a
        -- legitimate rate.
        --
        -- The bound is needed because thread counters are merged into the query
        -- total when a thread *detaches*, and pooled threads detach constantly,
        -- so any interval catching a detach absorbs that thread's whole
        -- accumulated wait: observed as +75s of OSCPUWaitMicroseconds inside
        -- 0.9s on a 4-thread process. The time is real, but attributing it to one
        -- interval yields a rate no number of threads could produce.
        toFloat64(greatest(
            if(peak_threads_usage > 0, peak_threads_usage, length(thread_ids)),
            lagInFrame(if(peak_threads_usage > 0, peak_threads_usage, length(thread_ids)), 1,
                       if(peak_threads_usage > 0, peak_threads_usage, length(thread_ids))) OVER w
        )) AS thread_bound,
        -- raw deltas (lag defaults to self so first sample = 0)
        (pe_cpu - lagInFrame(pe_cpu, 1, pe_cpu) OVER w) / 1000000 AS raw_d_cpu,
        (pe_io_wait - lagInFrame(pe_io_wait, 1, pe_io_wait) OVER w) / 1000000 AS raw_d_io,
        (pe_cpu_wait - lagInFrame(pe_cpu_wait, 1, pe_cpu_wait) OVER w) / 1000000 AS raw_d_cpu_wait,
        (pe_net_recv_wait - lagInFrame(pe_net_recv_wait, 1, pe_net_recv_wait) OVER w) / 1000000 AS raw_d_net_recv_wait,
        (pe_net_send_wait - lagInFrame(pe_net_send_wait, 1, pe_net_send_wait) OVER w) / 1000000 AS raw_d_net_send_wait,
        (read_bytes - lagInFrame(read_bytes, 1, read_bytes) OVER w) / (1024 * 1024) AS raw_d_read_mb,
        toFloat64(read_rows - lagInFrame(read_rows, 1, read_rows) OVER w) AS raw_d_read_rows,
        toFloat64(written_rows - lagInFrame(written_rows, 1, written_rows) OVER w) AS raw_d_written_rows,
        (pe_net_send - lagInFrame(pe_net_send, 1, pe_net_send) OVER w) / 1024 AS raw_d_net_send,
        (pe_net_recv - lagInFrame(pe_net_recv, 1, pe_net_recv) OVER w) / 1024 AS raw_d_net_recv
    FROM (
        SELECT
            ${multi ? 'initial_query_id,' : ''} query_id, sample_time,
            min(sample_time) OVER (${minTimePartition}) AS min_time,
            elapsed, memory_usage, peak_memory_usage,
            read_bytes, read_rows, written_rows, thread_ids, peak_threads_usage,
            ProfileEvents['OSCPUVirtualTimeMicroseconds'] AS pe_cpu,
            -- OSIOWaitMicroseconds: blocked on a block device (real disk reads).
            -- OSCPUWaitMicroseconds: runnable but no free CPU (run-queue contention).
            -- These are different stalls with opposite remedies; keep them apart.
            -- OSIOWaitMicroseconds needs procfs/taskstats access and reads 0 in
            -- some containerised deployments, so we sample both rather than
            -- substituting one for the other.
            ProfileEvents['OSIOWaitMicroseconds'] AS pe_io_wait,
            ProfileEvents['OSCPUWaitMicroseconds'] AS pe_cpu_wait,
            -- Socket blocking is invisible to every OS-level counter above: a
            -- thread parked in recv() burns no CPU, holds no run-queue slot and
            -- issues no block I/O. ClickHouse times its own socket calls instead.
            ProfileEvents['NetworkReceiveElapsedMicroseconds'] AS pe_net_recv_wait,
            ProfileEvents['NetworkSendElapsedMicroseconds'] AS pe_net_send_wait,
            ProfileEvents['NetworkSendBytes'] AS pe_net_send,
            ProfileEvents['NetworkReceiveBytes'] AS pe_net_recv
        FROM {{cluster_aware:tracehouse.processes_history}}
        WHERE ${whereClause}
        ORDER BY ${multi ? 'initial_query_id, ' : ''}query_id, sample_time
    )
    WINDOW w AS (${windowPartition} ORDER BY sample_time)
)
GROUP BY ${groupBy}
ORDER BY ${multi ? 'query_id, ' : ''}t
`;
}

/**
 * Build SQL to fetch process samples for a single query, grouped by hostname.
 * Each host's deltas are computed independently (window partitioned by hostname).
 * Returns HostProcessSample rows — one time series per host.
 */
export function buildHostProcessSamplesSQL(queryId: string): string {
  const escaped = `'${escapeValue(queryId)}'`;
  return `
SELECT
    hostname, t,
    max(elapsed) AS elapsed,
    sum(thread_count) AS thread_count,
    sum(memory_mb) AS memory_mb,
    max(peak_memory_mb) AS peak_memory_mb,
    sum(read_rows) AS read_rows,
    sum(written_rows) AS written_rows,
    sum(read_bytes) AS read_bytes,
    sum(cpu_us) AS cpu_us,
    sum(io_wait_us) AS io_wait_us,
    sum(cpu_wait_us) AS cpu_wait_us,
    sum(net_recv_wait_us) AS net_recv_wait_us,
    sum(net_send_wait_us) AS net_send_wait_us,
    sum(net_send_bytes) AS net_send_bytes,
    sum(net_recv_bytes) AS net_recv_bytes,
    -- Thread-time rates are clamped to the interval's thread bound: a process
    -- cannot spend more than N threads-worth of a second with N threads. See
    -- thread_bound below for why the raw counters can exceed it.
    sum(least(greatest(raw_d_cpu / dt, 0), thread_bound)) AS d_cpu_cores,
    sum(least(greatest(raw_d_io / dt, 0), thread_bound)) AS d_io_wait_s,
    sum(least(greatest(raw_d_cpu_wait / dt, 0), thread_bound)) AS d_cpu_wait_s,
    sum(least(greatest(raw_d_net_recv_wait / dt, 0), thread_bound)) AS d_net_recv_wait_s,
    sum(least(greatest(raw_d_net_send_wait / dt, 0), thread_bound)) AS d_net_send_wait_s,
    max(greatest(raw_d_cpu, raw_d_io, raw_d_cpu_wait, raw_d_net_recv_wait, raw_d_net_send_wait) / dt
        > thread_bound) AS rate_clamped,
    sum(greatest(raw_d_read_mb / dt, 0)) AS d_read_mb,
    sum(greatest(raw_d_read_rows / dt, 0)) AS d_read_rows,
    sum(greatest(raw_d_written_rows / dt, 0)) AS d_written_rows,
    sum(greatest(raw_d_net_send / dt, 0)) AS d_net_send_kb,
    sum(greatest(raw_d_net_recv / dt, 0)) AS d_net_recv_kb
FROM (
    SELECT
        hostname, query_id,
        toFloat64(dateDiff('millisecond', min_time, sample_time)) / 1000 AS t,
        elapsed, length(thread_ids) AS thread_count, peak_threads_usage,
        memory_usage / (1024 * 1024) AS memory_mb,
        peak_memory_usage / (1024 * 1024) AS peak_memory_mb,
        read_rows, written_rows, read_bytes,
        pe_cpu AS cpu_us, pe_io_wait AS io_wait_us, pe_cpu_wait AS cpu_wait_us,
        pe_net_recv_wait AS net_recv_wait_us, pe_net_send_wait AS net_send_wait_us,
        pe_net_send AS net_send_bytes, pe_net_recv AS net_recv_bytes,
        greatest(
            toFloat64(dateDiff('millisecond',
                lagInFrame(sample_time, 1, sample_time) OVER w,
                sample_time
            )) / 1000,
            0.1
        ) AS dt,
        -- See buildProcessSamplesSQL for why thread-time rates need this bound.
        toFloat64(greatest(
            if(peak_threads_usage > 0, peak_threads_usage, length(thread_ids)),
            lagInFrame(if(peak_threads_usage > 0, peak_threads_usage, length(thread_ids)), 1,
                       if(peak_threads_usage > 0, peak_threads_usage, length(thread_ids))) OVER w
        )) AS thread_bound,
        (pe_cpu - lagInFrame(pe_cpu, 1, pe_cpu) OVER w) / 1000000 AS raw_d_cpu,
        (pe_io_wait - lagInFrame(pe_io_wait, 1, pe_io_wait) OVER w) / 1000000 AS raw_d_io,
        (pe_cpu_wait - lagInFrame(pe_cpu_wait, 1, pe_cpu_wait) OVER w) / 1000000 AS raw_d_cpu_wait,
        (pe_net_recv_wait - lagInFrame(pe_net_recv_wait, 1, pe_net_recv_wait) OVER w) / 1000000 AS raw_d_net_recv_wait,
        (pe_net_send_wait - lagInFrame(pe_net_send_wait, 1, pe_net_send_wait) OVER w) / 1000000 AS raw_d_net_send_wait,
        (read_bytes - lagInFrame(read_bytes, 1, read_bytes) OVER w) / (1024 * 1024) AS raw_d_read_mb,
        toFloat64(read_rows - lagInFrame(read_rows, 1, read_rows) OVER w) AS raw_d_read_rows,
        toFloat64(written_rows - lagInFrame(written_rows, 1, written_rows) OVER w) AS raw_d_written_rows,
        (pe_net_send - lagInFrame(pe_net_send, 1, pe_net_send) OVER w) / 1024 AS raw_d_net_send,
        (pe_net_recv - lagInFrame(pe_net_recv, 1, pe_net_recv) OVER w) / 1024 AS raw_d_net_recv
    FROM (
        SELECT
            hostname, query_id, sample_time,
            min(sample_time) OVER (PARTITION BY hostname) AS min_time,
            elapsed, memory_usage, peak_memory_usage,
            read_bytes, read_rows, written_rows, thread_ids, peak_threads_usage,
            ProfileEvents['OSCPUVirtualTimeMicroseconds'] AS pe_cpu,
            -- OSIOWaitMicroseconds: blocked on a block device (real disk reads).
            -- OSCPUWaitMicroseconds: runnable but no free CPU (run-queue contention).
            -- These are different stalls with opposite remedies; keep them apart.
            -- OSIOWaitMicroseconds needs procfs/taskstats access and reads 0 in
            -- some containerised deployments, so we sample both rather than
            -- substituting one for the other.
            ProfileEvents['OSIOWaitMicroseconds'] AS pe_io_wait,
            ProfileEvents['OSCPUWaitMicroseconds'] AS pe_cpu_wait,
            -- Socket blocking is invisible to every OS-level counter above: a
            -- thread parked in recv() burns no CPU, holds no run-queue slot and
            -- issues no block I/O. ClickHouse times its own socket calls instead.
            ProfileEvents['NetworkReceiveElapsedMicroseconds'] AS pe_net_recv_wait,
            ProfileEvents['NetworkSendElapsedMicroseconds'] AS pe_net_send_wait,
            ProfileEvents['NetworkSendBytes'] AS pe_net_send,
            ProfileEvents['NetworkReceiveBytes'] AS pe_net_recv
        FROM {{cluster_aware:tracehouse.processes_history}}
        WHERE query_id = ${escaped} OR initial_query_id = ${escaped}
        ORDER BY hostname, query_id, sample_time
    )
    WINDOW w AS (PARTITION BY hostname, query_id ORDER BY sample_time)
)
GROUP BY hostname, t
ORDER BY hostname, t
`;
}

export function mapHostProcessSampleRow(r: Record<string, unknown>): HostProcessSample {
  return {
    hostname: String(r.hostname || ''),
    ...mapProcessSampleRow(r),
  };
}

/** @deprecated Use buildProcessSamplesSQL([queryId]) instead */
export const PROCESS_SAMPLES_SQL = '/* use buildProcessSamplesSQL */';

export interface HostProcessSample extends ProcessSample {
  hostname: string;
}

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
    cpu_wait_us: Number(r.cpu_wait_us) || 0,
    net_recv_wait_us: Number(r.net_recv_wait_us) || 0,
    net_send_wait_us: Number(r.net_send_wait_us) || 0,
    net_send_bytes: Number(r.net_send_bytes) || 0,
    net_recv_bytes: Number(r.net_recv_bytes) || 0,
    // deltas
    d_cpu_cores: Number(r.d_cpu_cores) || 0,
    d_io_wait_s: Number(r.d_io_wait_s) || 0,
    d_cpu_wait_s: Number(r.d_cpu_wait_s) || 0,
    d_net_recv_wait_s: Number(r.d_net_recv_wait_s) || 0,
    d_net_send_wait_s: Number(r.d_net_send_wait_s) || 0,
    d_read_mb: Number(r.d_read_mb) || 0,
    d_read_rows: Number(r.d_read_rows) || 0,
    d_written_rows: Number(r.d_written_rows) || 0,
    d_net_send_kb: Number(r.d_net_send_kb) || 0,
    d_net_recv_kb: Number(r.d_net_recv_kb) || 0,
    rate_clamped: Number(r.rate_clamped) > 0,
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
  { id: 'd_io_wait_s', label: 'Disk I/O Wait', unit: 'threads', formatter: v => `${v.toFixed(2)} threads`,
    lines: [{ key: 'd_io_wait_s', suffix: '' }] },
  { id: 'd_cpu_wait_s', label: 'CPU Wait', unit: 'threads', formatter: v => `${v.toFixed(2)} threads`,
    lines: [{ key: 'd_cpu_wait_s', suffix: '' }] },
  { id: 'network_wait', label: 'Network Wait', unit: 'threads', formatter: v => `${v.toFixed(2)} threads`,
    lines: [
      { key: 'd_net_recv_wait_s', suffix: ' recv' },
      { key: 'd_net_send_wait_s', suffix: ' send', strokeDasharray: '4 2' },
    ] },
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
