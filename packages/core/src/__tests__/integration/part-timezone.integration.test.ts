import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { IClickHouseAdapter } from '../../adapters/types.js';
import { DatabaseExplorer } from '../../services/database-explorer.js';
import { partTimezoneFixture } from './fixtures/part-timezone-fixture.js';
import { startClickHouse, stopClickHouse, type TestClickHouseContext } from './setup/clickhouse-container.js';

describe('Part and lineage timezone clocks', { tags: ['storage'] }, () => {
  let ctx: TestClickHouseContext;
  beforeAll(async () => { ctx = await startClickHouse({ timezone: 'Europe/Copenhagen' }); }, 120_000);
  afterAll(async () => { if (ctx) await stopClickHouse(ctx); });
  for (const zone of ['UTC', 'Europe/Copenhagen', 'America/New_York']) {
    it(`${zone}: part list/detail and lineage retain their absolute clocks`, async () => {
      const epoch = Date.parse('2026-11-08T00:30:00Z') / 1000;
      const adapter: IClickHouseAdapter = { executeQuery: sql => ctx.rawAdapter.executeQuery(sql.replace(
        /\{\{cluster_aware:system\.([a-z_]+)\}\}/g,
        (_, table: string) => partTimezoneFixture(table, epoch, zone),
      )) };
      const explorer = new DatabaseExplorer(adapter);
      const parts = await explorer.getTableParts('default', 'fixture');
      expect(parts).toHaveLength(1);
      expect(Date.parse(parts[0].modification_time)).toBe(epoch * 1000);
      const detail = await explorer.getPartDetail('default', 'fixture', 'fixture_1_2_1');
      expect(Date.parse(detail!.modification_time)).toBe(epoch * 1000);
      expect(detail).toMatchObject({ rows: 7, bytes_on_disk: 400, min_date: '2026-11-08', max_date: '2026-11-08' });
      expect(detail!.columns).toHaveLength(1);
      const lineage = await explorer.getPartLineage('default', 'fixture', 'fixture_1_2_1');
      expect(Date.parse(lineage.root.event_time!)).toBe(epoch * 1000);
      expect(lineage.root.children.map(child => child.part_name)).toEqual(['fixture_1_1_0', 'fixture_2_2_0']);
    });
  }
});
