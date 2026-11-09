import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { HttpAdapter } from '../../adapters/http-adapter.js';
import { InteractiveQueryService } from '../../services/interactive-query-service.js';
import { AnalyticsService } from '../../services/analytics-service.js';
import type { IClickHouseAdapter } from '../../adapters/types.js';
import { tagQuery } from '../../queries/builder.js';
import { describeResultTypesQuery } from '../../queries/result-type-queries.js';
import { queryTimezoneFixture } from './fixtures/query-timezone-fixture.js';
import { startClickHouse, stopClickHouse, type TestClickHouseContext } from './setup/clickhouse-container.js';

describe('Analytics typed timestamps', { tags: ['analytics'] }, () => {
  let ctx: TestClickHouseContext;
  let adapter: HttpAdapter;
  beforeAll(async () => {
    ctx = await startClickHouse({ timezone: 'Europe/Copenhagen' });
    const url = new URL(process.env.CH_TEST_URL ?? ctx.container!.getConnectionUrl());
    adapter = new HttpAdapter({ host: url.hostname, port: Number(url.port), user: decodeURIComponent(url.username) || 'default',
      password: decodeURIComponent(url.password), secure: url.protocol === 'https:', database: 'default', connect_timeout: 10, send_receive_timeout: 30 });
  }, 120_000);
  afterAll(async () => { await adapter?.close(); if (ctx) await stopClickHouse(ctx); });

  it.each([
    'SELECT now() AS t;\n-- note',
    'SELECT now() AS t FORMAT JSON',
    "SELECT now() AS t, '; FORMAT JSON' AS text FORMAT JSON;\n-- note",
    'SELECT now() AS t FORMAT JSON SETTINGS max_threads=1',
  ])('describes native types for valid authored SQL: %s', async sql => {
    const rows = await ctx.rawAdapter.executeQuery(tagQuery(describeResultTypesQuery(sql), 'TraceHouse:Analytics:nativeTypeRegression'));
    expect(rows.find(row => row.name === 't')?.type).toContain('DateTime');
  });

  for (const zone of ['UTC', 'Europe/Copenhagen', 'America/New_York']) {
    it(`${zone}: table query-pattern clocks use the same UTC contract`, async () => {
      const epoch = Math.floor(Date.now() / 1000) - 60;
      const fixtureAdapter: IClickHouseAdapter = { executeQuery: sql => ctx.rawAdapter.executeQuery(sql.replace(
        /\{\{cluster_aware:system\.([a-z_]+)\}\}/g,
        (_, table: string) => queryTimezoneFixture(table, epoch, zone).replace("'Create' AS query_kind", "'Select' AS query_kind"),
      )) };
      const rows = await new AnalyticsService(fixtureAdapter).getTableQueryPatterns('default', 'fixture');
      expect(rows).toHaveLength(1);
      expect(Date.parse(rows[0].first_seen)).toBe((epoch - 10) * 1000);
      expect(rows[0].last_seen).toBe(rows[0].first_seen);
    });
    it(`${zone}: a pattern drill-down uses the actual query start, not the finish clock`, async () => {
      const epoch = Date.parse('2026-11-08T00:30:00Z') / 1000;
      const fixtureAdapter: IClickHouseAdapter = { executeQuery: sql => ctx.rawAdapter.executeQuery(sql.replace(
        /\{\{cluster_aware:system\.([a-z_]+)\}\}/g,
        (_, table: string) => queryTimezoneFixture(table, epoch, zone),
      )) };
      const result = await new AnalyticsService(fixtureAdapter).getLatestQueryForPattern('42');
      expect(result).not.toBeNull();
      expect(Date.parse(result!.start_time)).toBe((epoch - 10) * 1000 + 123);
      expect(Date.parse(result!.end_time)).toBe(epoch * 1000 + 123);
      expect(result).toMatchObject({ query_id: 'fixture-query', duration_ms: 10000, peak_memory: 2097152 });
    });
    it(`${zone}: authored SQL returns absolute clocks without rewriting dates, strings or numbers`, async () => {
      const sql = `SELECT
        toDateTime(1794157200, '${zone}') AS clock,
        fromUnixTimestamp64Micro(1794157200123456, '${zone}') AS micro,
        toDate('2026-11-08') AS calendar, '2026-11-08 18:00:00' AS text,
        toUInt64(42) AS count, CAST(NULL AS Nullable(DateTime)) AS missing`;
      const baseline = await adapter.executeQuery(tagQuery(sql, 'TraceHouse:Analytics:timezoneBaseline'));
      const rows = await new InteractiveQueryService(adapter).run(sql, 'TraceHouse:Analytics:timezoneRegression');
      expect(rows).toHaveLength(1);
      expect(rows[0].clock).toBe('2026-11-08T17:00:00Z');
      expect(rows[0].micro).toBe('2026-11-08T17:00:00.123456Z');
      expect(rows[0]).toMatchObject({ calendar: '2026-11-08', text: '2026-11-08 18:00:00', count: baseline[0].count, missing: null });
    });
  }
});
