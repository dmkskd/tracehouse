import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { startClickHouse, stopClickHouse, type TestClickHouseContext } from './setup/clickhouse-container.js';
import { buildXRayWindowSamplesSQL, type XRayWindowOptions } from '../../queries/xray-window-queries.js';
import { buildSelectedXRayOverlaySQL } from '../../queries/xray-overlay-queries.js';
import { tagQuery } from '../../queries/builder.js';
import { sourceTag, TAB_INTERNAL } from '../../queries/source-tags.js';

const db = 'test_xray_window_sources';
const availability = { processesHistory: true, queryMetricLog: true };
const events = ['OSCPUVirtualTimeMicroseconds', 'OSCPUWaitMicroseconds', 'OSIOWaitMicroseconds', 'NetworkReceiveElapsedMicroseconds', 'NetworkSendBytes', 'NetworkReceiveBytes'];
const options: XRayWindowOptions = {
  start: "toDateTime64('2026-09-18 12:00:00.300', 6, 'UTC')",
  end: "toDateTime64('2026-09-18 12:00:02', 6, 'UTC')",
  identityStart: "toDateTime64('2026-09-18 12:00:00', 6, 'UTC')",
  queryIdsSQL: "SELECT 'root'",
};

/** Identical executions represented once as counters, once as interval deltas. */
describe('X-Ray window source parity', () => {
  let ctx: TestClickHouseContext;
  const execute = async (sql: string) => ctx.adapter.executeQuery<Record<string, unknown>>(tagQuery(
    sql.replaceAll('{{cluster_aware:system.query_log}}', `${db}.identities`)
      .replaceAll('{{cluster_aware:system.query_metric_log}}', `${db}.metrics`)
      .replaceAll('{{cluster_aware:tracehouse.processes_history}}', `${db}.processes`),
    sourceTag(TAB_INTERNAL, 'xrayWindowParity'),
  ));

  beforeAll(async () => {
    ctx = await startClickHouse();
    await ctx.client.command({ query: `CREATE DATABASE ${db}` });
    await ctx.client.command({ query: `CREATE TABLE ${db}.identities (
      query_id String, initial_query_id String, query String,
      event_date Date, query_start_time_microseconds DateTime64(6, 'UTC')
    ) ENGINE = Memory` });
    await ctx.client.command({ query: `CREATE TABLE ${db}.metrics (
      query_id String, event_date Date, event_time_microseconds DateTime64(6, 'UTC'), memory_usage UInt64,
      ${events.map(event => `ProfileEvent_${event} UInt64`).join(', ')}
    ) ENGINE = Memory` });
    await ctx.client.command({ query: `CREATE TABLE ${db}.processes (
      query_id String, initial_query_id String, query String, sample_time DateTime64(6, 'UTC'),
      memory_usage UInt64, ProfileEvents Map(String, UInt64), read_bytes UInt64, written_bytes UInt64
    ) ENGINE = Memory` });
    for (const [id, factor] of [['root', 1], ['child', 2]] as const) {
      await ctx.client.insert({ table: `${db}.identities`, format: 'JSONEachRow', values: [{ query_id: id, initial_query_id: 'root', query: 'SELECT 1', event_date: '2026-09-18', query_start_time_microseconds: '2026-09-18 12:00:00.000000' }] });
      const time = (ms: number) => `2026-09-18 12:00:${String(Math.floor(ms / 1000)).padStart(2, '0')}.${String(ms % 1000).padStart(3, '0')}000`;
      const ticks = [0, 250, 500, 1000, 2000];
      await ctx.client.insert({ table: `${db}.processes`, format: 'JSONEachRow', values: ticks.map(ms => ({
        query_id: id, initial_query_id: 'root', query: 'SELECT 1', sample_time: time(ms), memory_usage: factor * 10 * 1048576,
        ProfileEvents: Object.fromEntries(events.map(event => [event, factor * ms * 1000])), read_bytes: factor * ms * 1000, written_bytes: 0,
      })) });
      await ctx.client.insert({ table: `${db}.metrics`, format: 'JSONEachRow', values: ticks.slice(1).map((ms, index) => ({
        query_id: id, event_date: '2026-09-18', event_time_microseconds: time(ms), memory_usage: factor * 10 * 1048576,
        ...Object.fromEntries(events.map(event => [`ProfileEvent_${event}`, factor * (ms - ticks[index]) * 1000])),
      })) });
    }
  }, 180_000);
  afterAll(async () => {
    if (ctx) {
      await ctx.client.command({ query: `DROP DATABASE IF EXISTS ${db}` });
      await stopClickHouse(ctx);
    }
  });

  it('retains predecessor intervals at the viewport boundary and keeps local executions separate', async () => {
    const sampler = await execute(buildXRayWindowSamplesSQL({ availability, preference: 'processes_history' }, options));
    const metrics = await execute(buildXRayWindowSamplesSQL({ availability, preference: 'query_metric_log' }, options));
    for (const rows of [sampler, metrics]) {
      expect(rows).toHaveLength(6);
      expect(new Set(rows.map(row => row.query_id))).toEqual(new Set(['root']));
      expect(new Set(rows.map(row => row.sample_query_id))).toEqual(new Set(['root', 'child']));
    }
    expect(metrics.map(row => [row.sample_query_id, row.ts_ms, row.dt, row.cpu_us, row.memory_usage, row.net_send_bytes]))
      .toEqual(sampler.map(row => [row.sample_query_id, row.ts_ms, row.dt, row.cpu_us, row.memory_usage, row.net_send_bytes]));
  });

  it('produces equal CPU, memory, and wait overlays without inflating sub-second samples', async () => {
    for (const metric of ['cpu_cores', 'mem_mb', 'cpu_wait_s', 'io_wait_s', 'net_wait_s'] as const) {
      const sampler = await execute(buildSelectedXRayOverlaySQL({ availability, preference: 'processes_history' }, metric, options));
      const metrics = await execute(buildSelectedXRayOverlaySQL({ availability, preference: 'query_metric_log' }, metric, options));
      expect(metrics).toEqual(sampler);
      expect(metrics).toHaveLength(3);
      expect(metrics.map(row => Number(row[metric]))).toEqual([metric === 'mem_mb' ? 30 : 3, metric === 'mem_mb' ? 30 : 3, metric === 'mem_mb' ? 30 : 3]);
    }
  });

  it('preserves the first metric-log interval instead of differencing away its work', async () => {
    const rows = await execute(buildXRayWindowSamplesSQL({ availability, preference: 'query_metric_log' }, { ...options, start: options.identityStart! }));
    const first = rows.find(row => row.sample_query_id === 'root')!;
    expect(Number(first.dt)).toBe(0.25);
    expect(Number(first.cpu_us)).toBe(250000);
  });

  it('honours host filtering and excludes unrelated root ids', async () => {
    for (const preference of ['processes_history', 'query_metric_log'] as const) {
      expect(await execute(buildXRayWindowSamplesSQL({ availability, preference }, { ...options, hostname: 'not-a-host' }))).toEqual([]);
      expect(await execute(buildXRayWindowSamplesSQL({ availability, preference }, { ...options, queryIdsSQL: "SELECT 'other-root'" }))).toEqual([]);
    }
  });

  it('resolves ranked CTE membership and returns empty overlays when no query is ranked', async () => {
    const rankedOptions = {
      ...options,
      queryIdsSQL: 'SELECT query_id FROM top_q',
      identityStart: `if((SELECT count() FROM top_q) = 0, ${options.start}, (SELECT min(query_start) FROM top_q))`,
    };
    for (const preference of ['processes_history', 'query_metric_log'] as const) {
      const overlay = buildSelectedXRayOverlaySQL({ availability, preference }, 'cpu_cores', rankedOptions);
      const cte = `WITH top_q AS (SELECT 'root' AS query_id, ${options.identityStart} AS query_start`;
      expect(await execute(`${cte}) ${overlay}`)).toHaveLength(3);
      expect(await execute(`${cte} WHERE 0) ${overlay}`)).toEqual([]);
    }
  });

  it('provides progress-byte overlays only when the sampler can supply them', async () => {
    const rows = await execute(buildSelectedXRayOverlaySQL({ availability, preference: 'processes_history' }, 'read_mb_s', options));
    expect(rows).toHaveLength(3);
    expect(rows.every(row => Number(row.read_mb_s) > 0)).toBe(true);
    expect(() => buildSelectedXRayOverlaySQL({ availability, preference: 'query_metric_log' }, 'read_mb_s', options)).toThrow(/unavailable/);
  });
});
