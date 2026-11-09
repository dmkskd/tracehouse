import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { buildQuery } from '../../queries/builder.js';
import { GET_CORE_TIMELINE, GET_CORE_TIMELINE_FALLBACK, GET_CORE_TIMELINE_NO_CPU_ID, GET_CORE_TIMELINE_NO_CPU_ID_NO_THREAD_NAME, GET_DICTIONARIES } from '../../queries/engine-internals-queries.js';
import { GET_QPS_HISTORY, GET_RECENT_MERGE_CPU, GET_RECENT_MUTATION_CPU } from '../../queries/overview-queries.js';
import { parseTimeValue } from '../../utils/time.js';
import { queryTimezoneFixture } from './fixtures/query-timezone-fixture.js';
import { startClickHouse, stopClickHouse, type TestClickHouseContext } from './setup/clickhouse-container.js';

describe('Engine and Overview timestamp projections', { tags: ['observability'] }, () => {
  let ctx: TestClickHouseContext;
  beforeAll(async () => { ctx = await startClickHouse({ timezone: 'Europe/Copenhagen' }); }, 120_000);
  afterAll(async () => { if (ctx) await stopClickHouse(ctx); });
  for (const zone of ['UTC', 'Europe/Copenhagen', 'America/New_York']) {
    it(`${zone}: every engine fallback retains absolute 100ms slots`, async () => {
      const epoch = Date.parse('2026-07-05T12:00:00Z') / 1000;
      const fixture = `(SELECT *, toUInt32(1) AS cpu_id, 'CPU' AS trace_type FROM ${queryTimezoneFixture('trace_log', epoch, zone)})`;
      for (const template of [GET_CORE_TIMELINE, GET_CORE_TIMELINE_FALLBACK, GET_CORE_TIMELINE_NO_CPU_ID, GET_CORE_TIMELINE_NO_CPU_ID_NO_THREAD_NAME]) {
        const sql = buildQuery(template, { window_seconds: 60, core_count: 2 })
          .replaceAll('now()', `toDateTime(${epoch + 1}, 'UTC')`)
          .replace('{{cluster_aware:system.trace_log}}', fixture);
        const rows = await ctx.rawAdapter.executeQuery<{ slot: string; samples: number }>(sql);
        expect(rows).toHaveLength(1);
        expect(parseTimeValue(rows[0].slot).timeMs).toBe(epoch * 1000 + 100);
        expect(Number(rows[0].samples)).toBe(1);
      }
    });

    it(`${zone}: dictionary update time is absolute`, async () => {
      const epoch = 1794157200;
      const fixture = `(SELECT 'fixture' AS name, 'Hashed' AS type, toUInt64(10) AS bytes_allocated,
        toUInt64(2) AS element_count, toFloat64(0.5) AS load_factor, 'fixture' AS source,
        'LOADED' AS status, toDateTime(${epoch}, '${zone}') AS last_successful_update_time)`;
      const rows = await ctx.rawAdapter.executeQuery<{ last_successful_update_time: string }>(GET_DICTIONARIES.replace('{{cluster_aware:system.dictionaries}}', fixture));
      expect(Date.parse(rows[0].last_successful_update_time)).toBe(epoch * 1000);
    });

    it(`${zone}: QPS retains samples across Copenhagen midnight`, async () => {
      const now = Date.parse('2026-11-08T23:05:00Z') / 1000;
      const sample = now - 600;
      const fixture = `(SELECT *, toUInt64(7) AS ProfileEvent_Query FROM ${queryTimezoneFixture('metric_log', sample, zone)})`;
      const sql = GET_QPS_HISTORY.replaceAll('now()', `toDateTime(${now}, 'Europe/Copenhagen')`)
        .replaceAll('today()', `toDate(toDateTime(${now}, 'Europe/Copenhagen'))`)
        .replace('{{cluster_aware:system.metric_log}}', fixture);
      const rows = await ctx.rawAdapter.executeQuery<{ t: string; qps: number }>(sql);
      expect(rows.length).toBeGreaterThan(0);
      expect(parseTimeValue(rows[0].t).timeMs).toBe(Math.floor((sample - 5) / 15) * 15000);
      expect(Number(rows[0].qps)).toBe(7);
    });

    it(`${zone}: CPU pruning follows the requested window, including multiple days`, async () => {
      const now = Date.parse('2026-11-08T23:05:00Z') / 1000;
      for (const seconds of [900, 172800]) {
        const sample = now - seconds + 60;
        for (const [template, kind] of [[GET_RECENT_MERGE_CPU, 'MergeParts'], [GET_RECENT_MUTATION_CPU, 'MutatePart']]) {
          const fixture = queryTimezoneFixture('part_log', sample, zone).replace("'MergeParts' AS event_type", `'${kind}' AS event_type`)
            .replace("map('OSCPUVirtualTimeMicroseconds', toUInt64(123)) AS ProfileEvents", "map('UserTimeMicroseconds', toUInt64(7), 'SystemTimeMicroseconds', toUInt64(3)) AS ProfileEvents");
          const sql = buildQuery(template, { window_seconds: seconds })
            .replaceAll('now()', `toDateTime(${now}, 'Europe/Copenhagen')`)
            .replaceAll('today()', `toDate(toDateTime(${now}, 'Europe/Copenhagen'))`)
            .replace('{{cluster_aware:system.part_log}}', fixture);
          const rows = await ctx.rawAdapter.executeQuery<{ user_time_us: number; system_time_us: number }>(sql);
          expect(Number(rows[0].user_time_us)).toBe(7);
          expect(Number(rows[0].system_time_us)).toBe(3);
        }
      }
    });
  }
});
