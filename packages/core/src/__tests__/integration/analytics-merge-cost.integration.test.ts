import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { startClickHouse, stopClickHouse, type TestClickHouseContext } from './setup/clickhouse-container.js';
import queries from '@frontend-queries/mergeCost';
import { resolveDrillParams, resolveTimeRange } from '@frontend-analytics/templateResolution';

const DB = 'analytics_merge_cost_test';
const GiB = 1073741824;

describe('Merge cost attribution', { tags: ['analytics'] }, () => {
  let ctx: TestClickHouseContext;

  async function run(title: string, params: Record<string, string> = { tbl: 'app.events' }) {
    const raw = queries.find(q => q.includes(`title='${title}'`));
    if (!raw) throw new Error(`Missing query: ${title}`);
    const sql = resolveDrillParams(resolveTimeRange(raw, '1 DAY'), params)
      .replaceAll('{{cluster_aware:system.part_log}}', `${DB}.events`);
    return ctx.rawAdapter.executeQuery<Record<string, unknown>>(sql);
  }

  beforeAll(async () => {
    ctx = await startClickHouse();
    await ctx.client.command({ query: `CREATE DATABASE ${DB}` });
    await ctx.client.command({ query: `CREATE TABLE ${DB}.events (
      database String DEFAULT 'app', table String DEFAULT 'events',
      event_type String DEFAULT 'MergeParts', merge_reason String DEFAULT 'RegularMerge',
      error UInt16 DEFAULT 0, event_date Date DEFAULT today(), event_time DateTime DEFAULT now(),
      part_name String, merged_from Array(String), size_in_bytes UInt64 DEFAULT 1024,
      read_bytes UInt64 DEFAULT 1073741824, duration_ms UInt64,
      ProfileEvents Map(String, UInt64)
    ) ENGINE = Memory` });
    const first = { part_name: 'all_1_2_1', merged_from: ['all_1_1_0', 'all_2_2_0'], duration_ms: 4000, ProfileEvents: { OSCPUVirtualTimeMicroseconds: 3e6 } };
    await ctx.client.insert({ table: `${DB}.events`, format: 'JSONEachRow', values: [
      first,
      { ...first, part_name: 'all_3_4_1', duration_ms: 2000, ProfileEvents: { OSCPUVirtualTimeMicroseconds: 1e6 } },
      { ...first, part_name: 'all_1_8_3', merged_from: ['all_1_4_2', 'all_5_5_0'], size_in_bytes: GiB, read_bytes: 2 * GiB, duration_ms: 3000, ProfileEvents: { OSCPUVirtualTimeMicroseconds: 6e6 } },
      { ...first, part_name: 'all_1_4_2_99', merged_from: ['all_1_2_1_99', 'all_3_4_1_99'], size_in_bytes: 10485760, duration_ms: 1000, merge_reason: 'TTLRecompressMerge', ProfileEvents: { UserTimeMicroseconds: 1.5e6, SystemTimeMicroseconds: 0.5e6 } },
      { ...first, part_name: 'unrecognized', merged_from: ['unrecognized'], duration_ms: 5000, ProfileEvents: {} },
      { ...first, part_name: 'all_7_8_1', duration_ms: 0, ProfileEvents: { OSCPUVirtualTimeMicroseconds: 0 } },
      { ...first, table: 'other', ProfileEvents: { OSCPUVirtualTimeMicroseconds: 100e6 } },
      { ...first, error: 1 },
      { ...first, event_type: 'MutatePart' },
      { ...first, event_type: 'MergePartsStart' },
      { ...first, merge_reason: 'NotAMerge' },
      { ...first, database: 'system' },
      { ...first, event_date: '2000-01-01', event_time: '2000-01-01 00:00:00' },
    ] });
  }, 120_000);

  afterAll(async () => {
    if (ctx) {
      try { await ctx.client.command({ query: `DROP DATABASE IF EXISTS ${DB}` }); }
      finally { await stopClickHouse(ctx); }
    }
  });

  it('counts each merge once, sums core-seconds separately from elapsed, and scopes tables', async () => {
    const rows = await run('Merge CPU by Table');
    expect(rows).toHaveLength(1);
    const row = rows[0];
    expect(row.tbl).toBe('app.events');
    expect(Number(row.merge_count)).toBe(6);
    expect(Number(row.cpu_profiled_merges)).toBe(5);
    expect(Number(row.cpu_seconds)).toBe(12);
    expect(Number(row.elapsed_seconds)).toBe(15);
    expect(Number(row.cpu_seconds_per_input_gib)).toBe(2);
    expect(Number(row.cpu_coverage_pct)).toBe(83.3);
    const all = await run('Merge CPU by Table', {});
    expect(all.map(r => r.tbl)).toEqual(['app.other', 'app.events']);
    expect(Number(all[1].cpu_share_pct)).toBeCloseTo(12 / 112 * 100, 2);
  });

  it('distinguishes first-level, mixed-level, and mutated source part names', async () => {
    const rows = await run('Merge CPU by Level Transition');
    const byLevel = Object.fromEntries(rows.map(r => [String(r.level_transition), r]));
    expect(Number(byLevel['0 → 1'].merge_count)).toBe(3);
    expect(Number(byLevel['0 → 1'].cpu_seconds)).toBe(4);
    expect(Number(byLevel['0-2 → 3'].cpu_seconds)).toBe(6);
    expect(Number(byLevel['1 → 2'].cpu_seconds)).toBe(2);
    expect(byLevel.Unknown.cpu_seconds).toBeNull();
    expect(Number(byLevel.Unknown.elapsed_seconds)).toBe(5);
    expect(Number(byLevel.Unknown.cpu_coverage_pct)).toBe(0);
    expect(Number(byLevel['0-2 → 3'].cpu_share_pct)).toBe(50);
  });

  it('attributes result-size and reason totals and preserves table drill context', async () => {
    const sizes = await run('Merge CPU by Result Size');
    expect(Number(sizes.find(r => r.size_bucket === '1-10 GiB')?.cpu_seconds)).toBe(6);
    expect(Number(sizes.find(r => r.size_bucket === '10-100 MiB')?.cpu_seconds)).toBe(2);
    const reasons = await run('Merge Cost by Reason');
    expect(Number(reasons.find(r => r.reason === 'TTLRecompressMerge')?.cpu_seconds)).toBe(2);
    const details = await run('Table Merge Cost Breakdown');
    expect(details.reduce((sum, r) => sum + Number(r.cpu_seconds), 0)).toBe(12);
    expect(details.reduce((sum, r) => sum + Number(r.merge_count), 0)).toBe(6);
  });

  it('runs every cost panel, including elapsed rankings and empty selections', async () => {
    for (const query of queries) {
      const title = query.match(/title='([^']+)'/)![1];
      expect((await run(title)).length).toBeGreaterThan(0);
      expect(await run(title, { tbl: 'app.missing' })).toEqual([]);
    }
  });
});
