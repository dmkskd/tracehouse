import { buildXRayWindowSamplesSQL } from '../../queries/xray-window-queries.js';
import { buildSelectedXRayOverlaySQL } from '../../queries/xray-overlay-queries.js';
/**
 * Integration tests for the system.query_metric_log X-Ray source.
 *
 * These execute a real query and read back its own metric log, because the
 * defects this source is prone to are semantic, not structural: every one of
 * them produced perfectly well-formed SQL.
 *
 * Four regressions are pinned here, all of which shipped at some point during
 * development and none of which a SQL-string assertion could see:
 *
 *   1. `ProfileEvent_*` are PER-INTERVAL DELTAS, not cumulative counters.
 *      Differencing them again yields noise around zero — the series looked
 *      plausible while average CPU read ~6x low.
 *   2. The first sample's dt fell back to the 0.01s floor, dividing a whole
 *      interval's work by 10ms and manufacturing a spike that hit the clamp.
 *   3. `argMax(peak_threads_usage, event_time)` ties on second-resolution
 *      timestamps and can return the QueryStart zero, silently disabling the
 *      rate clamp.
 *   4. A fixed 0.5s roll-up bucket collapses several samples of the SAME query
 *      when query_metric_log_interval is sub-second, multiplying memory and
 *      every rate by the number of samples swallowed.
 *
 * The oracle throughout is `system.query_log`, which holds the authoritative
 * totals for a finished query.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { startClickHouse, stopClickHouse, type TestClickHouseContext } from './setup/clickhouse-container.js';
import { configuredClickHouseIsBefore } from './setup/constants.js';
import {
  buildHostQueryMetricLogSamplesSQL,
  buildQueryMetricLogSamplesSQL,
} from '../../queries/query-metric-log-queries.js';
import { mapHostProcessSampleRow, mapTaggedProcessSampleRow } from '../../queries/process-queries.js';
import { tagQuery } from '../../queries/builder.js';
import { sourceTag, TAB_INTERNAL } from '../../queries/source-tags.js';

const CONTAINER_TIMEOUT = 180_000;
const q = (sql: string) => tagQuery(sql, sourceTag(TAB_INTERNAL, 'queryMetricLogXrayIntegration'));

/** system.query_metric_log landed in 24.10. */
const describeWithQueryMetricLog =
  configuredClickHouseIsBefore(24, 10) ? describe.skip : describe;

/**
 * CPU-bound work sized to run for several seconds, so a 1s sampling interval
 * still collects a useful series. Hashing rather than summing keeps the runtime
 * dominated by CPU per row, which makes the duration far less sensitive to how
 * fast the host can stream integers: ~7s on a developer machine at 4 threads.
 */
const WORKLOAD = 'SELECT sum(sipHash64(number)) FROM numbers(1000000000)';

/** Below this the 1s-interval series is too short to say anything about shape. */
const MIN_SAMPLES = 4;

interface QueryLogTotals {
  cpu_us: number;
  peak_threads_usage: number;
  duration_ms: number;
}

describeWithQueryMetricLog('query_metric_log X-Ray source', { tags: ['query-analysis'] }, () => {
  let ctx: TestClickHouseContext;
  /** query_id → the authoritative totals system.query_log recorded for it. */
  const totals = new Map<string, QueryLogTotals>();

  /**
   * Run a workload with a known query id and sampling interval, then wait for
   * both logs to flush and capture query_log's totals as the oracle.
   */
  async function runWorkload(queryId: string, intervalMs: number): Promise<QueryLogTotals> {
    await ctx.rawAdapter.executeQuery(
      `${WORKLOAD} SETTINGS query_metric_log_interval = ${intervalMs}, max_threads = 4`,
      { queryId },
    );
    await ctx.rawAdapter.executeCommand('SYSTEM FLUSH LOGS');
    const rows = await ctx.rawAdapter.executeQuery<{
      cpu_us: string; peak_threads_usage: string; duration_ms: string;
    }>(q(`
      SELECT ProfileEvents['OSCPUVirtualTimeMicroseconds'] AS cpu_us,
             peak_threads_usage,
             query_duration_ms AS duration_ms
      FROM system.query_log
      WHERE query_id = '${queryId}' AND type = 'QueryFinish'
    `));
    expect(rows.length, 'query_log must have a QueryFinish row').toBe(1);
    const t: QueryLogTotals = {
      cpu_us: Number(rows[0].cpu_us),
      peak_threads_usage: Number(rows[0].peak_threads_usage),
      duration_ms: Number(rows[0].duration_ms),
    };
    totals.set(queryId, t);
    return t;
  }

  async function samplesFor(queryId: string) {
    const rows = await ctx.adapter.executeQuery<Record<string, unknown>>(
      q(buildHostQueryMetricLogSamplesSQL(queryId)),
    );
    return rows.map(mapHostProcessSampleRow);
  }

  // Unique per run: against an external instance (CH_TEST_URL) a fixed id would
  // match the previous run's rows still sitting in query_log.
  const RUN = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
  const SECOND_SCALE_ID = `xray-qml-1s-${RUN}`;
  const SUB_SECOND_ID = `xray-qml-100ms-${RUN}`;

  /**
   * Sub-second queries, whose QueryStart and QueryFinish rows share a
   * second-resolution event_time. Several of them, because whether a given
   * short query straddles a second boundary is luck.
   */
  const SHORT_IDS = [0, 1, 2, 3, 4].map(i => `xray-qml-short-${RUN}-${i}`);
  const SHORT_WORKLOAD = 'SELECT sum(sipHash64(number)) FROM numbers(20000000)';

  beforeAll(async () => {
    ctx = await startClickHouse();
    await runWorkload(SECOND_SCALE_ID, 1000);
    await runWorkload(SUB_SECOND_ID, 100);
    for (const id of SHORT_IDS) {
      await ctx.rawAdapter.executeQuery(
        `${SHORT_WORKLOAD} SETTINGS query_metric_log_interval = 50, max_threads = 4`,
        { queryId: id },
      );
    }
    await ctx.rawAdapter.executeCommand('SYSTEM FLUSH LOGS');
  }, CONTAINER_TIMEOUT);

  afterAll(async () => {
    if (ctx) await stopClickHouse(ctx);
  });

  it('reconstructs the query total CPU that query_log recorded', async () => {
    // Catches treating the delta columns as cumulative: differencing them again
    // discards most of the work and this total collapses.
    const samples = await samplesFor(SECOND_SCALE_ID);
    expect(samples.length).toBeGreaterThanOrEqual(MIN_SAMPLES);

    const cumulativeCpuUs = Math.max(...samples.map(s => s.cpu_us));
    const expected = totals.get(SECOND_SCALE_ID)!.cpu_us;
    expect(cumulativeCpuUs / expected).toBeGreaterThan(0.98);
    expect(cumulativeCpuUs / expected).toBeLessThan(1.02);
  });

  it('produces a rate curve whose area is the work query_log recorded', async () => {
    // The assertion that pins the delta semantics. Integrating d_cpu_cores over
    // time must recover the query's CPU seconds: differencing an already-delta
    // column subtracts two independent intervals, so roughly half the values go
    // negative, get floored to zero, and the area collapses.
    const samples = await samplesFor(SECOND_SCALE_ID);
    let areaSeconds = 0;
    for (let i = 0; i < samples.length; i++) {
      const dt = i === 0 ? samples[0].elapsed : samples[i].t - samples[i - 1].t;
      areaSeconds += samples[i].d_cpu_cores * dt;
    }
    const expectedSeconds = totals.get(SECOND_SCALE_ID)!.cpu_us / 1e6;
    // Generous band: the teardown interval is clamped to peak concurrency, so
    // the area is a slight undercount by construction.
    expect(areaSeconds / expectedSeconds).toBeGreaterThan(0.8);
    expect(areaSeconds / expectedSeconds).toBeLessThan(1.2);
  });

  it('never reports more cores than the query had threads', async () => {
    // Catches both a disabled clamp (argMax tie) and a first-sample dt of 0.01s:
    // either produces a rate no thread count could sustain.
    const samples = await samplesFor(SECOND_SCALE_ID);
    const bound = totals.get(SECOND_SCALE_ID)!.peak_threads_usage;
    expect(bound).toBeGreaterThan(0);
    for (const s of samples) {
      expect(s.d_cpu_cores).toBeLessThanOrEqual(bound + 0.001);
    }
  });

  it('does not treat the first sample as an outlier', async () => {
    // The first row of a delta series carries a full interval of real work, so
    // its rate must sit in the same range as the rest of the query.
    const samples = await samplesFor(SECOND_SCALE_ID);
    const rest = samples.slice(1).map(s => s.d_cpu_cores);
    const median = rest.sort((a, b) => a - b)[Math.floor(rest.length / 2)];
    expect(median).toBeGreaterThan(0);
    expect(samples[0].d_cpu_cores).toBeLessThan(median * 4);
  });

  it('reports a thread ceiling for a finished query, so rates stay clamped', async () => {
    // peak_threads_usage is only non-zero on the QueryFinish row; reading it
    // with argMax over a second-resolution timestamp can return the start row.
    const samples = await samplesFor(SECOND_SCALE_ID);
    expect(samples.some(s => s.rate_unclamped)).toBe(false);
  });

  it('finds the thread ceiling for sub-second queries, whose query_log rows tie on event_time', async () => {
    // system.query_log.event_time is second-resolution, so a short query's
    // QueryStart and QueryFinish rows carry the same timestamp. Reading
    // peak_threads_usage with argMax over it can return the QueryStart zero,
    // which disables the rate clamp without any visible error.
    let checked = 0;
    for (const id of SHORT_IDS) {
      const samples = await samplesFor(id);
      if (samples.length === 0) continue;
      checked++;
      expect(samples.some(s => s.rate_unclamped), `query ${id} lost its thread ceiling`).toBe(false);
    }
    expect(checked, 'no short query produced samples to check').toBeGreaterThan(0);
  });

  it('measures elapsed from query start rather than from the first sample', async () => {
    const samples = await samplesFor(SECOND_SCALE_ID);
    expect(samples[0].t).toBe(0);
    expect(samples[0].elapsed).toBeGreaterThan(0);
    const durationS = totals.get(SECOND_SCALE_ID)!.duration_ms / 1000;
    expect(Math.max(...samples.map(s => s.elapsed))).toBeLessThanOrEqual(durationS + 1);
  });

  it('keeps every sample when the interval is finer than the roll-up bucket', async () => {
    // A fixed 0.5s bucket would fold ~5 samples of this query into one row and
    // sum them, inflating memory and every rate.
    const raw = await ctx.rawAdapter.executeQuery<{ n: string }>(q(`
      SELECT count() AS n FROM system.query_metric_log WHERE query_id = '${SUB_SECOND_ID}'
    `));
    const rawCount = Number(raw[0].n);
    expect(rawCount).toBeGreaterThan(5);

    const rolledUp = (await ctx.adapter.executeQuery<Record<string, unknown>>(
      q(buildQueryMetricLogSamplesSQL([SUB_SECOND_ID])),
    )).map(mapTaggedProcessSampleRow);
    expect(rolledUp.length).toBe(rawCount);
  });

  it('does not inflate memory through the roll-up', async () => {
    const peak = await ctx.rawAdapter.executeQuery<{ mb: string }>(q(`
      SELECT max(memory_usage) / 1048576 AS mb
      FROM system.query_metric_log WHERE query_id = '${SUB_SECOND_ID}'
    `));
    const rolledUp = (await ctx.adapter.executeQuery<Record<string, unknown>>(
      q(buildQueryMetricLogSamplesSQL([SUB_SECOND_ID])),
    )).map(mapTaggedProcessSampleRow);

    const reported = Math.max(...rolledUp.map(s => s.memory_mb));
    expect(reported).toBeCloseTo(Number(peak[0].mb), 1);
  });

  it('tags each query when several are requested together', async () => {
    const rows = (await ctx.adapter.executeQuery<Record<string, unknown>>(
      q(buildQueryMetricLogSamplesSQL([SECOND_SCALE_ID, SUB_SECOND_ID])),
    )).map(mapTaggedProcessSampleRow);

    const ids = new Set(rows.map(r => r.query_id));
    expect(ids).toEqual(new Set([SECOND_SCALE_ID, SUB_SECOND_ID]));
    // Each query's series must be ordered: an ORDER BY inside a UNION ALL arm
    // does not order the union.
    for (const id of ids) {
      const ts = rows.filter(r => r.query_id === id).map(r => r.t);
      expect([...ts].sort((a, b) => a - b)).toEqual(ts);
    }
  });
  it('preserves the same real-query CPU totals in the Time Travel and Series window path', async () => {
    const selection = { availability: { processesHistory: false, queryMetricLog: true }, preference: 'query_metric_log' as const };
    for (const id of [SECOND_SCALE_ID, SUB_SECOND_ID]) {
      const opts = { start: 'now() - INTERVAL 2 DAY', end: 'now()', queryIdsSQL: `SELECT '${id}'` };
      const intervals = await ctx.adapter.executeQuery<Record<string, unknown>>(q(buildXRayWindowSamplesSQL(selection, opts)));
      const cpuUs = intervals.reduce((sum, row) => sum + Number(row.cpu_us), 0);
      const expected = totals.get(id)!.cpu_us;
      expect(cpuUs / expected).toBeGreaterThan(0.98);
      expect(cpuUs / expected).toBeLessThan(1.02);
      const overlays = await ctx.adapter.executeQuery<Record<string, unknown>>(q(buildSelectedXRayOverlaySQL(selection, 'cpu_cores', opts)));
      expect(overlays.length).toBeGreaterThan(0);
      expect(overlays.every(row => Number(row.cpu_cores) > 0 && Number.isFinite(Number(row.cpu_cores)))).toBe(true);
    }
  });

});
