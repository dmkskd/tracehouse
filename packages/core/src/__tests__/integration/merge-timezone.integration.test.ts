import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { IClickHouseAdapter } from '../../adapters/types.js';
import { buildQuery, eventDateBound, tagQuery } from '../../queries/builder.js';
import { GET_DATABASE_MUTATION_HISTORY, GET_TABLE_MUTATION_HISTORY, withMutationKilledCapability, GET_MERGE_TEXT_LOGS_BY_QUERY_ID, GET_MERGE_TEXT_LOGS_BY_QUERY_ID_HOST, GET_MERGE_TEXT_LOGS_BY_PART_SUFFIX, GET_MERGE_TEXT_LOGS_BY_PART_SUFFIX_HOST } from '../../queries/merge-queries.js';
import { parseAbsoluteTimestamp } from '../../utils/absolute-timestamp.js';
import { MergeTracker } from '../../services/merge-tracker.js';
import { mergeTimezoneFixture } from './fixtures/merge-timezone-fixture.js';
import { startClickHouse, stopClickHouse, type TestClickHouseContext } from './setup/clickhouse-container.js';

describe('Merge timezone identities', { tags: ['merge-engine'] }, () => {
  let ctx: TestClickHouseContext;
  beforeAll(async () => { ctx = await startClickHouse({ timezone: 'Europe/Copenhagen' }); }, 120_000);
  afterAll(async () => { if (ctx) await stopClickHouse(ctx); });
  for (const zone of ['UTC', 'Europe/Copenhagen', 'America/New_York']) {
    it.each([0, 1])(`${zone}: mutation clocks with prefer_column_name_to_alias=%s`, async preferColumns => {
      const epoch = Date.parse('2026-11-08T00:30:00Z') / 1000;
      let active = false;
      const adapter: IClickHouseAdapter = { executeQuery: sql => ctx.rawAdapter.executeQuery(sql.replace(
        /\{\{cluster_aware:system\.([a-z_]+)\}\}/g,
        (_, table: string) => {
          const fixture = mergeTimezoneFixture(table, epoch, zone);
          return active ? fixture.replace('toUInt8(1) AS is_done', 'toUInt8(0) AS is_done') : fixture;
        },
      ) + ` SETTINGS prefer_column_name_to_alias=${preferColumns}`) };
      const tracker = new MergeTracker(adapter);
      const history = await tracker.getMutationHistory({ limit: 1 });
      expect(history).toHaveLength(1);
      active = true;
      const current = await tracker.getMutations();
      expect(current).toHaveLength(1);
      for (const row of [history[0], current[0]]) {
        expect(Date.parse(row.create_time)).toBe(epoch * 1000);
        expect(Date.parse(row.latest_fail_time)).toBe((epoch + 2) * 1000);
        expect(row.mutation_id).toBe('mutation_1');
      }
      active = false;
      for (const template of [GET_DATABASE_MUTATION_HISTORY, GET_TABLE_MUTATION_HISTORY]) {
        const sql = buildQuery(withMutationKilledCapability(template, true), { database: 'default', table: 'fixture', limit: 1 });
        const rows = await adapter.executeQuery(tagQuery(sql, 'TraceHouse:Merges:mutationAliasRegression'));
        expect(rows).toHaveLength(1);
        expect(parseAbsoluteTimestamp(String(rows[0].create_time_utc))).toBe(epoch * 1000);
        expect(parseAbsoluteTimestamp(String(rows[0].latest_fail_time_utc))).toBe((epoch + 2) * 1000);
      }
    });
    for (const instant of ['2026-11-08T00:30:00Z', '2026-07-05T12:00:00Z']) {
      it(`${zone}: list/exact lookup retain two attempts one microsecond apart at ${instant}`, async () => {
        const epoch = Date.parse(instant) / 1000;
        const adapter: IClickHouseAdapter = { executeQuery: sql => ctx.rawAdapter.executeQuery(sql.replace(
          /\{\{cluster_aware:system\.([a-z_]+)\}\}/g,
          (_, table: string) => mergeTimezoneFixture(table, epoch, zone),
        )) };
        const tracker = new MergeTracker(adapter);
        const rows = await tracker.getMergeHistory({ limit: 2 });
        expect(rows).toHaveLength(2);
        expect(new Set(rows.map(row => row.event_time_microseconds)).size).toBe(2);
        for (const row of rows) {
          expect(Date.parse(row.event_time)).toBe(epoch * 1000);
          expect(row.event_time_microseconds).toMatch(/\.12345[67]Z$/);
          const exact = await tracker.getMergeHistoryByPartName('default', 'fixture', 'part-fixture', row);
          expect(exact).not.toBeNull();
          expect(exact!.event_time_microseconds).toBe(row.event_time_microseconds);
          expect(exact).toMatchObject({ part_name: 'part-fixture', duration_ms: 10000, size_in_bytes: 400, source_part_names: ['source_1', 'source_2'], error: 241 });
        }
        // Old unzoned keys match the original source text; never reinterpret as UTC.
        const legacyKey = await ctx.rawAdapter.executeQuery<{ legacy: string }>(`SELECT toString(fromUnixTimestamp64Micro(toInt64(${epoch}) * 1000000 + 123456, '${zone}')) AS legacy`);
        const legacy = await tracker.getMergeHistoryByPartName('default', 'fixture', 'part-fixture', {
          ...rows[0], event_time_microseconds: legacyKey[0].legacy,
        });
        expect(legacy!.event_time_microseconds).toContain('.123456Z');
        for (const template of [GET_MERGE_TEXT_LOGS_BY_QUERY_ID, GET_MERGE_TEXT_LOGS_BY_QUERY_ID_HOST, GET_MERGE_TEXT_LOGS_BY_PART_SUFFIX, GET_MERGE_TEXT_LOGS_BY_PART_SUFFIX_HOST]) {
          const sql = buildQuery(template.replace('{event_date_bound}', eventDateBound(instant)), {
            query_id: 'fixture-query', query_id_suffix: '%fixture-query', hostname: rows[0].hostname!,
          });
          const logs = await adapter.executeQuery(tagQuery(sql, 'TraceHouse:Merges:timezoneRegression'));
          expect(logs).toHaveLength(2);
          expect(parseAbsoluteTimestamp(String(logs[0].event_time_microseconds))).toBe(epoch * 1000 + 123);
        }
        const nearest = await tracker.getMergeHistoryByPartName('default', 'fixture', 'part-fixture', {
          hostname: rows[0].hostname, event_type: 'MergeParts', event_time: instant,
        });
        expect(nearest!.event_time_microseconds).toContain('.123457Z');
      });
    }
  }
  for (const [zone, instant] of [['Europe/Copenhagen', '2026-10-25T00:30:00Z'], ['America/New_York', '2026-11-01T05:30:00Z']]) {
    it(`${zone}: an ambiguous legacy repeated-hour key never selects another attempt`, async () => {
      const epoch = Date.parse(instant) / 1000;
      const adapter: IClickHouseAdapter = { executeQuery: sql => ctx.rawAdapter.executeQuery(sql.replace(
        /\{\{cluster_aware:system\.([a-z_]+)\}\}/g,
        (_, table: string) => mergeTimezoneFixture(table, epoch, zone)
          .replace(`toDateTime(${epoch},`, `toDateTime(${epoch} + toUInt32(number) * 3600,`)
          .replace('+ 123456 + number,', '+ 123456,'),
      )) };
      const tracker = new MergeTracker(adapter);
      const rows = await tracker.getMergeHistory({ limit: 2 });
      expect(rows).toHaveLength(2);
      expect(new Set(rows.map(row => row.event_time_microseconds)).size).toBe(2);
      const oldKey = await ctx.rawAdapter.executeQuery<{ legacy: string }>(`SELECT toString(fromUnixTimestamp64Micro(toInt64(${epoch}) * 1000000 + 123456, '${zone}')) AS legacy`);
      expect(await tracker.getMergeHistoryByPartName('default', 'fixture', 'part-fixture', { ...rows[0], event_time_microseconds: oldKey[0].legacy })).toBeNull();
      for (const row of rows) {
        expect((await tracker.getMergeHistoryByPartName('default', 'fixture', 'part-fixture', row))!.event_time_microseconds).toBe(row.event_time_microseconds);
      }
    });
  }

});
