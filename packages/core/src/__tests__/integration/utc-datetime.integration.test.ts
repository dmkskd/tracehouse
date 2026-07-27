import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { buildQuery, utcDateTime, utcDateTime64 } from '../../queries/builder.js';
import {
  startClickHouse,
  stopClickHouse,
  type TestClickHouseContext,
} from './setup/clickhouse-container.js';

describe('explicit UTC datetime integration', { tags: ['query-analysis'] }, () => {
  let ctx: TestClickHouseContext;

  beforeAll(async () => {
    ctx = await startClickHouse();
  }, 120_000);

  afterAll(async () => {
    if (ctx) await stopClickHouse(ctx);
  }, 30_000);

  it('is independent of a non-UTC ClickHouse session timezone', async () => {
    const sql = buildQuery(`
      SELECT
        timezone() AS session_timezone,
        toUnixTimestamp({explicit_time}) AS explicit_epoch,
        toUnixTimestamp(toDateTime('2026-07-27 13:13:00')) AS implicit_epoch,
        toUnixTimestamp64Milli({explicit_time64}) AS explicit_epoch_ms
      SETTINGS session_timezone = 'Europe/Berlin'
    `, {
      explicit_time: utcDateTime('2026-07-27T14:13:00+01:00'),
      explicit_time64: utcDateTime64('2026-07-27T14:13:00.125+01:00'),
    });

    const result = await ctx.client.query({ query: sql, format: 'JSONEachRow' });
    const rows = await result.json<{
      session_timezone: string;
      explicit_epoch: number;
      implicit_epoch: number;
      explicit_epoch_ms: number;
    }>();

    expect(rows[0].session_timezone).toBe('Europe/Berlin');
    expect(Number(rows[0].explicit_epoch)).toBe(
      Date.parse('2026-07-27T13:13:00.000Z') / 1000,
    );
    expect(Number(rows[0].explicit_epoch_ms)).toBe(
      Date.parse('2026-07-27T13:13:00.125Z'),
    );
    expect(Number(rows[0].implicit_epoch)).not.toBe(Number(rows[0].explicit_epoch));
  });
});
