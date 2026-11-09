import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { IClickHouseAdapter } from '../../adapters/types.js';
import { QueryAnalyzer } from '../../services/query-analyzer.js';
import { TraceService } from '../../services/trace-service.js';
import { buildQuery, eventDateBound, tagQuery } from '../../queries/builder.js';
import { DISTRIBUTED_TOPOLOGY_EXECUTIONS, DISTRIBUTED_TOPOLOGY_EXECUTIONS_BY_QUERY_IDS, DISTRIBUTED_TOPOLOGY_TEXT_LOGS, DISTRIBUTED_TOPOLOGY_ASYNC_INSERT_LOGS, QUERY_HISTORY } from '../../queries/query-queries.js';
import { utcDateTime } from '../../queries/builder.js';
import { parseAbsoluteTimestamp } from '../../utils/absolute-timestamp.js';
import { queryTimezoneFixture } from './fixtures/query-timezone-fixture.js';
import { startClickHouse, stopClickHouse, type TestClickHouseContext } from './setup/clickhouse-container.js';

describe('Query history, detail and trace clocks', { tags: ['query-analysis'] }, () => {
  let ctx: TestClickHouseContext;
  beforeAll(async () => { ctx = await startClickHouse({ timezone: 'Europe/Copenhagen' }); }, 120_000);
  afterAll(async () => { if (ctx) await stopClickHouse(ctx); });

  for (const zone of ['UTC', 'Europe/Copenhagen', 'America/New_York']) {
    it.each([0, 1])(`${zone}: similar-query aggregation supports alias preference %s`, async preference => {
      const epoch = Math.floor(Date.now() / 1000) - 60;
      const adapter: IClickHouseAdapter = { executeQuery: sql => ctx.rawAdapter.executeQuery(sql.replace(
        /\{\{cluster_aware:system\.([a-z_]+)\}\}/g,
        (_, table: string) => queryTimezoneFixture(table, epoch, zone),
      ) + ` SETTINGS prefer_column_name_to_alias=${preference}`) };
      const rows = await new QueryAnalyzer(adapter).getSimilarQueries('42', 1);
      expect(rows).toHaveLength(1);
      expect(Date.parse(rows[0].query_start_time)).toBe((epoch - 10) * 1000);
    });
    it(`${zone}: related executions, thread clocks and server overlays align`, async () => {
      const epoch = Math.floor(Date.now() / 1000) - 60;
      const instant = new Date(epoch * 1000).toISOString();
      const adapter: IClickHouseAdapter = { executeQuery: sql => ctx.rawAdapter.executeQuery(sql.replace(
        /\{\{cluster_aware:system\.([a-z_]+)\}\}/g,
        (_, table: string) => queryTimezoneFixture(table, epoch, zone).replace('toUInt8(1) AS is_initial_query', 'toUInt8(0) AS is_initial_query'),
      )) };
      const analyzer = new QueryAnalyzer(adapter);
      const similar = await analyzer.getSimilarQueries('42', 1);
      expect(similar).toHaveLength(1);
      expect(Date.parse(similar[0].query_start_time)).toBe((epoch - 10) * 1000);
      const sub = await analyzer.getSubQueries('fixture-query', instant);
      const batch = await analyzer.getSubQueriesForInitialQueries(['fixture-query'], instant, 1);
      for (const row of [sub[0], batch.get('fixture-query')![0]]) {
        expect(parseAbsoluteTimestamp(row.query_start_time_microseconds)).toBe((epoch - 10) * 1000 + 123);
        expect(row.query_duration_ms).toBe(10000);
      }
      expect(await analyzer.getCoordinatorIds(['fixture-query'], instant.slice(0, 10))).toEqual(new Set(['fixture-query']));
      const threads = await analyzer.getQueryThreadBreakdown('fixture-query', instant);
      expect(threads).toHaveLength(1);
      expect(parseAbsoluteTimestamp(threads[0].event_time_us) - parseAbsoluteTimestamp(threads[0].initial_query_start_time_us)).toBe(10000);
      const start = new Date((epoch - 10) * 1000).toISOString();
      const end = new Date((epoch + 10) * 1000).toISOString();
      const cpu = await analyzer.getServerCpuForRange(start, end);
      const mem = await analyzer.getServerMemoryForRange(start, end);
      expect(cpu).toHaveLength(3);
      expect(mem).toHaveLength(3);
      expect(cpu.map(p => Date.parse(p.t))).toEqual([-5, 0, 5].map(delta => (epoch + delta) * 1000));
      expect(mem.map(p => p.t)).toEqual(cpu.map(p => p.t));
      expect(cpu.map(p => p.cpu_pct)).toEqual([5, 5, 5]);
      expect(mem.map(p => p.mem_pct)).toEqual([10, 10, 10]);
      for (const template of [DISTRIBUTED_TOPOLOGY_EXECUTIONS, DISTRIBUTED_TOPOLOGY_EXECUTIONS_BY_QUERY_IDS, DISTRIBUTED_TOPOLOGY_TEXT_LOGS, DISTRIBUTED_TOPOLOGY_ASYNC_INSERT_LOGS]) {
        const sql = buildQuery(template.replace('{event_date_bound}', eventDateBound(instant)).replaceAll('{{query_id_list}}', "'fixture-query'"), { initial_query_id: 'fixture-query' });
        const rows = await adapter.executeQuery(tagQuery(sql, 'TraceHouse:Queries:timezoneRegression'));
        expect(rows).toHaveLength(1);
        const clock = rows[0].query_start_time_microseconds ?? rows[0].event_time_microseconds;
        expect(parseAbsoluteTimestamp(String(clock))).toBe((epoch + ('query_start_time_microseconds' in rows[0] ? -10 : 0)) * 1000 + 123);
      }
      const historySql = buildQuery(QUERY_HISTORY, { start_date: instant.slice(0, 10), start_time: utcDateTime(start), end_time: utcDateTime(end), limit: 1 });
      expect(parseAbsoluteTimestamp(String((await adapter.executeQuery(tagQuery(historySql, 'TraceHouse:Queries:timezoneRegression')))[0].query_start_time))).toBe((epoch - 10) * 1000);
    });
    for (const instant of ['2026-11-08T00:30:00Z', '2026-07-05T12:00:00Z', '2026-10-25T00:30:00Z', '2026-10-25T01:30:00Z']) {
      it(`${zone}: history/detail/logs retain the same instant at ${instant}`, async () => {
        const epoch = Date.parse(instant) / 1000;
        const adapter: IClickHouseAdapter = {
          executeQuery: sql => ctx.rawAdapter.executeQuery(sql.replace(
            /\{\{cluster_aware:system\.([a-z_]+)\}\}/g,
            (_, table: string) => queryTimezoneFixture(table, epoch, zone),
          )),
        };
        const analyzer = new QueryAnalyzer(adapter);
        const history = await analyzer.getQueryHistory({
          start_date: instant.slice(0, 10), start_time: new Date((epoch - 20) * 1000).toISOString(),
          end_time: new Date((epoch + 20) * 1000).toISOString(), limit: 1,
        });
        expect(history).toHaveLength(1);
        expect(Date.parse(history[0].query_start_time)).toBe((epoch - 10) * 1000);
        expect(history[0]).toMatchObject({ query_id: 'fixture-query', query_duration_ms: 10000, read_rows: 3, result_rows: 7, memory_usage: 2097152 });
        const detail = await analyzer.getQueryDetail('fixture-query', instant);
        expect(detail).not.toBeNull();
        expect(parseAbsoluteTimestamp(detail!.query_start_time)).toBe((epoch - 10) * 1000);
        expect(parseAbsoluteTimestamp(detail!.query_start_time_microseconds)).toBe((epoch - 10) * 1000 + 123);
        expect(detail!.query_start_time_microseconds).toContain('.123456');
        for (const levels of [undefined, ['Error']]) {
          const logs = await new TraceService(adapter).getQueryLogs('fixture-query', levels, instant, 1);
          expect(logs).toHaveLength(1);
          expect(parseAbsoluteTimestamp(logs[0].event_time_microseconds) - parseAbsoluteTimestamp(detail!.query_start_time_microseconds)).toBe(10000);
          expect(logs[0].event_time_microseconds).toContain('.123456');
          expect(logs[0].message).toBe('fixture message');
        }
      });
    }
  }
});
