/**
 * SQL builder for the X-Ray "Series" dashboard overlays.
 *
 * Produces one row per (query_id, second) with a per-second metric from a
 * tracehouse.processes_history-shaped source:
 *   - CPU / read / I/O are cumulative counters → per-second RATES via windowed
 *     deltas (value − previous value) normalized by the inter-sample interval dt.
 *   - Memory is an instantaneous GAUGE → read directly (averaged per second).
 *
 * Windows are PARTITION BY query_id so deltas never leak across queries, and use
 * lagInFrame(x, 1, x) — the third argument (self) makes each query's FIRST sample
 * a zero delta instead of subtracting the type default. This mirrors
 * buildProcessSamplesSQL in process-queries.ts.
 *
 * The frontend X-Ray dashboard (frontend/src/components/analytics/queries/xray.ts)
 * builds its panels from this so the delta math has one source of truth and is
 * covered by xray-overlay.integration.test.ts.
 */

export type XRayOverlayMetric = 'cpu_cores' | 'read_mb_s' | 'io_wait_s' | 'mem_mb';

interface RateSpec {
  /** Cumulative counter expression on the source row. */
  counter: string;
  /** Divisor turning the raw delta into the metric's unit (µs→s: 1e6, bytes→MB: 1048576). */
  scale: string;
  /** Decimal places for the rounded output. */
  round: number;
}

const RATE_SPECS: Record<Exclude<XRayOverlayMetric, 'mem_mb'>, RateSpec> = {
  cpu_cores: { counter: "ProfileEvents['OSCPUVirtualTimeMicroseconds']", scale: '1e6', round: 2 },
  read_mb_s: { counter: 'read_bytes', scale: '1048576', round: 1 },
  io_wait_s: { counter: "ProfileEvents['OSCPUWaitMicroseconds']", scale: '1e6', round: 3 },
};

/**
 * Build the per-second overlay SELECT for one metric.
 *
 * @param metric      which per-second series to compute
 * @param source      table expression to read from (a bare name like
 *                    'tracehouse.processes_history', or a '{{cluster_aware:…}}' template)
 * @param whereClause full WHERE clause applied to the source (membership / filters);
 *                    pass '' for none
 */
export function buildXRayOverlaySQL(metric: XRayOverlayMetric, source: string, whereClause = ''): string {
  if (metric === 'mem_mb') {
    return `SELECT
    t,
    query_id,
    round(avg(memory_usage) / 1048576, 1) AS mem_mb
FROM
(
    SELECT
        query_id, memory_usage,
        toStartOfInterval(sample_time, INTERVAL 1 SECOND) AS t
    FROM ${source}
    ${whereClause}
)
GROUP BY query_id, t
ORDER BY t, query_id`;
  }

  const { counter, scale, round } = RATE_SPECS[metric];
  return `SELECT
    t,
    query_id,
    round(avg(${metric}), ${round}) AS ${metric}
FROM
(
    SELECT
        query_id,
        toStartOfInterval(sample_time, INTERVAL 1 SECOND)                       AS t,
        greatest((cnt - lagInFrame(cnt, 1, cnt) OVER w) / ${scale} / dt, 0)     AS ${metric}
    FROM
    (
        SELECT
            query_id, sample_time,
            ${counter} AS cnt,
            -- seconds since previous sample of this query (floored at 0.1s); the
            -- lagInFrame default (self) makes the first sample's dt 0 → clamped to 0.1
            greatest(dateDiff('millisecond', lagInFrame(sample_time, 1, sample_time) OVER w0, sample_time) / 1000, 0.1) AS dt
        FROM ${source}
        ${whereClause}
        WINDOW w0 AS (PARTITION BY query_id ORDER BY sample_time)
    )
    WINDOW w AS (PARTITION BY query_id ORDER BY sample_time)
)
GROUP BY query_id, t
ORDER BY t, query_id`;
}
