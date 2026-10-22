import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { startClickHouse, stopClickHouse, type TestClickHouseContext } from './setup/clickhouse-container.js';
import {
  buildHostQueryMetricLogSamplesSQL,
  buildQueryMetricLogSamplesSQL,
  QUERY_METRIC_LOG_PROFILE_EVENT_COLUMNS,
} from '../../queries/query-metric-log-queries.js';
import { mapHostProcessSampleRow, mapTaggedProcessSampleRow } from '../../queries/process-queries.js';

const DB = `qml_precision_${Math.random().toString(36).slice(2, 10)}`;
const ID = 'precision-query';
const OTHER = 'other-query';
const opts = { startedAt: '2026-01-01 00:00:00' };

describe('query_metric_log microsecond precision', { tags: ['query-analysis'] }, () => {
  let ctx: TestClickHouseContext;

  function fixtureSQL(sql: string): string {
    return sql.replaceAll('{{cluster_aware:system.query_metric_log}}', `${DB}.metrics`)
      .replaceAll('{{cluster_aware:system.query_log}}', `${DB}.queries`);
  }

  beforeAll(async () => {
    ctx = await startClickHouse();
    await ctx.client.command({ query: `CREATE DATABASE ${DB}` });
    await ctx.client.command({ query: `CREATE TABLE ${DB}.queries (
      query_id String, initial_query_id String DEFAULT query_id,
      peak_threads_usage UInt64 DEFAULT 4,
      event_time_microseconds DateTime64(6), event_date Date DEFAULT toDate(event_time_microseconds)
    ) ENGINE = Memory` });
    await ctx.client.command({ query: `CREATE TABLE ${DB}.metrics (
      hostname String DEFAULT 'host-a', query_id String, event_time_microseconds DateTime64(6),
      memory_usage UInt64, peak_memory_usage UInt64 DEFAULT memory_usage,
      ${QUERY_METRIC_LOG_PROFILE_EVENT_COLUMNS.map(column => `${column} UInt64 DEFAULT 0`).join(',\n')}
    ) ENGINE = Memory` });
    await ctx.client.insert({ table: `${DB}.queries`, format: 'JSONEachRow', values:
      [ID, OTHER].map(query_id => ({ query_id, event_time_microseconds: '2026-01-01 00:00:00.000000' })) });
    // The final two intervals are 250 us and 1 us. These rows must not be
    // rounded onto the preceding 100 ms sample or have their gauges summed.
    await ctx.client.insert({ table: `${DB}.metrics`, format: 'JSONEachRow', values: [
      ...['100000', '200000', '200250', '200251'].map((fraction, index) => ({
        query_id: ID, event_time_microseconds: `2026-01-01 00:00:00.${fraction}`,
        memory_usage: (index + 1) * 1048576,
        ProfileEvent_OSCPUVirtualTimeMicroseconds: [1000, 2000, 3, 4][index],
      })),
      { query_id: OTHER, event_time_microseconds: '2026-01-01 00:00:00.100000', memory_usage: 5 * 1048576 },
    ] });
  }, 120_000);

  afterAll(async () => {
    if (ctx) {
      try { await ctx.client.command({ query: `DROP DATABASE IF EXISTS ${DB}` }); }
      finally { await stopClickHouse(ctx); }
    }
  });

  it('preserves sub-millisecond samples and their memory and cumulative CPU in the rollup', async () => {
    const samples = (await ctx.rawAdapter.executeQuery(fixtureSQL(buildQueryMetricLogSamplesSQL([ID], opts))))
      .map(mapTaggedProcessSampleRow);
    expect(samples).toHaveLength(4);
    expect(samples.map(sample => sample.memory_mb)).toEqual([1, 2, 3, 4]);
    expect(samples.map(sample => sample.cpu_us)).toEqual([1000, 3000, 3003, 3007]);
    for (const [index, expected] of [0, 0.1, 0.10025, 0.100251].entries()) {
      expect(samples[index].t).toBeCloseTo(expected, 6);
    }
  });

  it('preserves sub-millisecond samples in the host series', async () => {
    const samples = (await ctx.rawAdapter.executeQuery(fixtureSQL(buildHostQueryMetricLogSamplesSQL(ID, opts))))
      .map(mapHostProcessSampleRow);
    expect(samples).toHaveLength(4);
    expect(samples.map(sample => sample.memory_mb)).toEqual([1, 2, 3, 4]);
    expect(samples[3].t - samples[2].t).toBeCloseTo(0.000001, 9);
    expect(samples[3].elapsed).toBeCloseTo(0.200251, 6);
    expect(samples[3].cpu_us).toBe(3007);
  });

  it('keeps each query intact when several series are requested', async () => {
    const samples = (await ctx.rawAdapter.executeQuery(fixtureSQL(buildQueryMetricLogSamplesSQL([ID, OTHER], opts))))
      .map(mapTaggedProcessSampleRow);
    expect(samples.filter(sample => sample.query_id === ID).map(sample => sample.memory_mb)).toEqual([1, 2, 3, 4]);
    expect(samples.filter(sample => sample.query_id === OTHER).map(sample => sample.memory_mb)).toEqual([5]);
  });
});
