/** Validate frontend analytics presets against ClickHouse, outside the core library build. */
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { startClickHouse, stopClickHouse, type TestClickHouseContext } from './setup/clickhouse-container.js';
import queries from '@frontend-queries/mergeCost';
import analyticsQueries from '@frontend-queries/mergeAnalytics';
import { resolveDrillParams, resolveTimeRange } from '@frontend-analytics/templateResolution';

const DB = 'analytics_merge_cost_test';
const GiB = 1073741824;

describe('Merge cost attribution', { tags: ['analytics'] }, () => {
  let ctx: TestClickHouseContext;

  async function run(title: string, params: Record<string, string> = { tbl: 'app.events' }, interval = '1 DAY', clock?: string) {
    const raw = analyticsQueries.find(q => q.includes(`title='${title}'`));
    if (!raw) throw new Error(`Missing query: ${title}`);
    let sql = resolveDrillParams(resolveTimeRange(raw, '1 DAY', interval), params)
      .replaceAll('{{cluster_aware:system.part_log}}', `${DB}.events`)
      .replaceAll('{{cluster_aware:system.mutations}}', `${DB}.mutations`);
    if (clock) sql = sql.replaceAll('now()', `toDateTime('${clock}', 'UTC')`);
    return ctx.rawAdapter.executeQuery<Record<string, unknown>>(sql);
  }

  beforeAll(async () => {
    ctx = await startClickHouse();
    await ctx.client.command({ query: `CREATE DATABASE ${DB}` });
    await ctx.client.command({ query: `CREATE TABLE ${DB}.events (
      database String DEFAULT 'app', table String DEFAULT 'events',
      hostname String DEFAULT 'replica_a',
      event_type String DEFAULT 'MergeParts', merge_reason String DEFAULT 'RegularMerge',
      error UInt16 DEFAULT 0, event_date Date DEFAULT today(), event_time DateTime DEFAULT now(),
      part_name String, merged_from Array(String), size_in_bytes UInt64 DEFAULT 1024,
      read_bytes UInt64 DEFAULT 1073741824, duration_ms UInt64, rows UInt64 DEFAULT 0,
      ProfileEvents Map(String, UInt64), partition_id String DEFAULT 'all'
    ) ENGINE = Memory` });
    // Stand-in for system.mutations: block_numbers maps partition -> mutation version.
    await ctx.client.command({ query: `CREATE TABLE ${DB}.mutations (
      database String DEFAULT 'app', table String DEFAULT 'events', mutation_id String, command String,
      \`block_numbers.partition_id\` Array(String), \`block_numbers.number\` Array(Int64)
    ) ENGINE = Memory` });
  }, 120_000);

  const first = { part_name: 'all_1_2_1', merged_from: ['all_1_1_0', 'all_2_2_0'], duration_ms: 4000, ProfileEvents: { UserTimeMicroseconds: 3e6 } };

  async function seed(rows: Record<string, unknown>[]) {
    await ctx.client.command({ query: `TRUNCATE TABLE ${DB}.events` });
    await ctx.client.insert({ table: `${DB}.events`, format: 'JSONEachRow', values: rows });
  }

  beforeEach(async () => {
    await seed([
      first,
      { ...first, part_name: 'all_3_4_1', duration_ms: 2000, ProfileEvents: { UserTimeMicroseconds: 1e6 } },
      { ...first, part_name: 'all_1_8_3', merged_from: ['all_1_4_2', 'all_5_5_0'], size_in_bytes: GiB, read_bytes: 2 * GiB, duration_ms: 3000, ProfileEvents: { UserTimeMicroseconds: 6e6 } },
      { ...first, part_name: 'all_1_4_2_99', merged_from: ['all_1_2_1_99', 'all_3_4_1_99'], size_in_bytes: 10485760, duration_ms: 1000, merge_reason: 'TTLRecompressMerge', ProfileEvents: { UserTimeMicroseconds: 1.5e6, SystemTimeMicroseconds: 0.5e6 } },
      { ...first, part_name: 'unrecognized', merged_from: ['unrecognized'], duration_ms: 5000, ProfileEvents: {} },
      { ...first, part_name: 'all_7_8_1', duration_ms: 0, ProfileEvents: { UserTimeMicroseconds: 0 } },
      { ...first, table: 'other', ProfileEvents: { UserTimeMicroseconds: 100e6 } },
      { ...first, error: 1 },
      { ...first, event_type: 'MutatePart' },
      { ...first, event_type: 'MergePartsStart' },
      { ...first, database: 'system' },
      { ...first, event_date: '2000-01-01', event_time: '2000-01-01 00:00:00' },
      { ...first, event_time: '2000-01-01 00:00:00' }, // event_time filter independent of event_date
    ]);
  });

  afterAll(async () => {
    if (ctx) {
      try { await ctx.client.command({ query: `DROP DATABASE IF EXISTS ${DB}` }); }
      finally { await stopClickHouse(ctx); }
    }
  });

  it('attributes mutation cost to its ALTER command for non-replicated and replicated version records', async () => {
    const mutated = (part: string, partition: string, cpuUs: number) => ({
      ...first, event_type: 'MutatePart', part_name: part, merged_from: [], partition_id: partition,
      duration_ms: 1000, ProfileEvents: { UserTimeMicroseconds: cpuUs },
    });
    await seed([
      mutated('all_1_5_2_7', 'all', 4e6),            // version 7: non-replicated record (partition '')
      mutated('all_6_9_1_7', 'all', 2e6),
      mutated('202601_1_3_1_12', '202601', 1e6),     // version 12: replicated record, per partition
      mutated('202602_1_3_1_12', '202602', 1e6),     // same version, but no record for this partition
      mutated('all_10_11_1_3', 'all', 0.5e6),        // version 3: no longer in system.mutations
    ]);
    await ctx.client.command({ query: `TRUNCATE TABLE ${DB}.mutations` });
    await ctx.client.insert({ table: `${DB}.mutations`, format: 'JSONEachRow', values: [
      { mutation_id: 'mutation_7.txt', command: 'DELETE WHERE x = 1', 'block_numbers.partition_id': [''], 'block_numbers.number': [7] },
      // Two replicas report the same replicated mutation: must not double the rows.
      { mutation_id: '0000000001', command: 'UPDATE y = 2 WHERE 1', 'block_numbers.partition_id': ['202601'], 'block_numbers.number': [12] },
      { mutation_id: '0000000001', command: 'UPDATE y = 2 WHERE 1', 'block_numbers.partition_id': ['202601'], 'block_numbers.number': [12] },
    ] });

    const rows = await run('Mutation Cost by Command');
    const byVersion = Object.fromEntries(rows.map(r => [Number(r.mutation_version), r]));
    expect(byVersion[7]).toMatchObject({ command: 'DELETE WHERE x = 1' });
    expect(Number(byVersion[7].parts_rewritten)).toBe(2);
    expect(Number(byVersion[7].cpu_seconds)).toBe(6);
    expect(byVersion[12]).toMatchObject({ command: 'UPDATE y = 2 WHERE 1' });
    expect(Number(byVersion[12].parts_rewritten)).toBe(2);
    expect(Number(byVersion[12].cpu_seconds)).toBe(2);
    expect(byVersion[3]).toMatchObject({ command: '(no longer in system.mutations)' });
    expect(rows.map(r => Number(r.mutation_version))).toEqual([7, 12, 3]); // by CPU
  });

  it('attributes completed mutations by table, separately from merges', async () => {
    // The seed's one MutatePart row: 3s CPU, 4s, on app.events.
    const cpu = await run('Mutation CPU by Table');
    expect(cpu).toHaveLength(1);
    expect(cpu[0]).toMatchObject({ tbl: 'app.events' });
    expect(Number(cpu[0].parts_rewritten)).toBe(1);
    expect(Number(cpu[0].cpu_seconds)).toBe(3);
    expect(Number(cpu[0].elapsed_seconds)).toBe(4);
    expect(Number(cpu[0].total_parts_rewritten)).toBe(1);

    const elapsed = await run('Mutation Elapsed Time by Table');
    expect(Number(elapsed[0].total_elapsed_seconds)).toBe(4);
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

  it('uses measured totals for CPU intensity and the correct populations for averages and shares', async () => {
    // 8 CPU seconds over 3 measured input GiB = 2.667 s/GiB.
    // Averaging the per-merge rates gives 2.5; including unprofiled input gives 8/103.
    await seed([
      { ...first, duration_ms: 1000, ProfileEvents: { UserTimeMicroseconds: 2e6 } },
      { ...first, duration_ms: 3000, read_bytes: 2 * GiB, ProfileEvents: { UserTimeMicroseconds: 6e6 } },
      { ...first, part_name: 'all_1_4_2', merged_from: ['all_1_2_1', 'all_3_4_1'], size_in_bytes: GiB,
        read_bytes: 100 * GiB, merge_reason: 'TTLRecompressMerge', duration_ms: 6000, ProfileEvents: {} },
    ]);
    const [total] = await run('Merge CPU by Table');
    for (const [key, expected] of Object.entries({
      merge_count: 3, cpu_profiled_merges: 2, cpu_coverage_pct: 66.7,
      cpu_seconds: 8, elapsed_seconds: 10, avg_cpu_seconds: 4,
      avg_elapsed_seconds: 3.333, p95_elapsed_seconds: 5.7,
      cpu_seconds_per_input_gib: 2.667, cpu_share_pct: 100,
      elapsed_share_pct: 100, merge_share_pct: 100,
    })) expect(Number(total[key]), key).toBeCloseTo(expected, 3);

    const levels = await run('Merge CPU by Level Transition');
    const initial = levels.find(row => row.level_transition === '0 → 1')!;
    expect(Number(initial.cpu_share_pct)).toBe(100);
    expect(Number(initial.elapsed_share_pct)).toBe(40);
    expect(Number(initial.merge_share_pct)).toBe(66.67);
    const unprofiled = levels.find(row => row.level_transition === '1 → 2')!;
    expect(unprofiled.cpu_seconds).toBeNull();
    expect(Number(unprofiled.elapsed_share_pct)).toBe(60);
    expect(Number(unprofiled.merge_share_pct)).toBe(33.33);
  });

  it('counts CPU as user + system time, treating an omitted counter as 0 and only an empty map as unknown', async () => {
    // ClickHouse leaves zero counters out of ProfileEvents, so a missing key is 0, not unknown.
    await seed([
      { ...first, ProfileEvents: { UserTimeMicroseconds: 3e6, SystemTimeMicroseconds: 1e6 } }, // 4
      { ...first, ProfileEvents: { UserTimeMicroseconds: 2e6 } },                                // 2: system omitted
      { ...first, ProfileEvents: { SystemTimeMicroseconds: 1e6 } },                              // 1: user omitted
      { ...first, ProfileEvents: { OSCPUVirtualTimeMicroseconds: 9e6 } },                        // 0: no user/system time
      { ...first, ProfileEvents: {} },                                                           // unknown
    ]);
    const [row] = await run('Merge CPU by Table');
    expect(Number(row.cpu_seconds)).toBe(7);
    expect(Number(row.cpu_profiled_merges)).toBe(4);
    expect(Number(row.cpu_coverage_pct)).toBe(80);
    expect(Number(row.avg_cpu_seconds)).toBe(1.75);
  });

  it.each(['missing', 'zero'] as const)('handles entirely %s CPU without NaN, infinity, or invented percentages', async kind => {
    await seed([{ ...first, read_bytes: 0, duration_ms: 0,
      ProfileEvents: kind === 'missing' ? {} : { UserTimeMicroseconds: 0 } }]);
    const [row] = await run('Merge CPU by Table');
    expect(row.cpu_seconds).toBe(kind === 'missing' ? null : 0);
    expect(row.total_cpu_seconds).toBe(kind === 'missing' ? null : 0);
    expect(row.avg_cpu_seconds).toBe(kind === 'missing' ? null : 0);
    expect(Number(row.cpu_coverage_pct)).toBe(kind === 'missing' ? 0 : 100);
    expect(row.cpu_share_pct).toBeNull();
    expect(row.elapsed_share_pct).toBeNull();
    expect(row.cpu_seconds_per_input_gib).toBeNull();
    expect(Number(row.merge_share_pct)).toBe(100);
    for (const value of Object.values(row)) {
      if (typeof value === 'number') expect(Number.isFinite(value)).toBe(true);
    }
  });

  it('calculates percentages from unrounded CPU values', async () => {
    await seed([
      { ...first, ProfileEvents: { UserTimeMicroseconds: 1 } },
      { ...first, part_name: 'all_1_4_2', merged_from: ['all_1_2_1'], ProfileEvents: { UserTimeMicroseconds: 3 } },
    ]);
    const rows = await run('Merge CPU by Level Transition');
    expect(rows.map(row => Number(row.cpu_seconds))).toEqual([0, 0]);
    expect(Number(rows.find(row => row.level_transition === '0 → 1')!.cpu_share_pct)).toBe(25);
    expect(Number(rows.find(row => row.level_transition === '1 → 2')!.cpu_share_pct)).toBe(75);
  });

  it('uses the full population for shares before the top-30 table limit', async () => {
    await seed(Array.from({ length: 31 }, (_, index) => ({
      ...first, table: `table_${index.toString().padStart(2, '0')}`,
      duration_ms: 1000, ProfileEvents: { UserTimeMicroseconds: 1e6 },
    })));
    for (const title of ['Merge CPU by Table', 'Merge Elapsed Time by Table']) {
      const rows = await run(title, {});
      expect(rows).toHaveLength(30);
      for (const row of rows) {
        expect(Number(row.total_merge_count)).toBe(31);
        expect(Number(row.total_elapsed_seconds)).toBe(31);
        expect(Number(row.total_cpu_seconds)).toBe(31);
        expect(row).not.toHaveProperty('chart_summary');
        expect(row).not.toHaveProperty('chart_detail');
        for (const metric of ['cpu_share_pct', 'elapsed_share_pct', 'merge_share_pct']) {
          expect(Number(row[metric]), metric).toBe(3.23); // 100 / 31, not 100 / 30
        }
      }
    }
  });

  it('conserves counts, CPU, and duration across every grouping and the table drilldown', async () => {
    const [total] = await run('Merge CPU by Table');
    for (const title of ['Merge CPU by Level Transition', 'Merge Elapsed Time by Level Transition',
      'Merge CPU by Result Size', 'Merge Elapsed Time by Result Size', 'Merge Cost by Reason', 'Table Merge Cost Breakdown']) {
      const rows = await run(title);
      for (const row of rows) {
        expect(Number(row.total_merge_count), title).toBe(Number(total.merge_count));
        expect(Number(row.total_elapsed_seconds), title).toBe(Number(total.elapsed_seconds));
        expect(Number(row.total_cpu_seconds), title).toBe(Number(total.cpu_seconds));
      }
      for (const metric of ['merge_count', 'cpu_profiled_merges', 'cpu_seconds', 'elapsed_seconds']) {
        expect(rows.reduce((sum, row) => sum + Number(row[metric]), 0), `${title}: ${metric}`).toBe(Number(total[metric]));
      }
      for (const metric of ['cpu_share_pct', 'elapsed_share_pct', 'merge_share_pct']) {
        expect(rows.reduce((sum, row) => sum + Number(row[metric]), 0), `${title}: ${metric}`).toBeCloseTo(100, 1);
      }
    }
  });

  it('preserves quoted UInt64 and Int64 values under explicit JSONEachRow integer settings', async () => {
    const [row] = await ctx.rawAdapter.executeQuery(`SELECT
      toUInt64('18446744073709551615') AS large_count,
      toInt64('-9223372036854775808') AS signed_count,
      toUInt64(454) AS merge_count
      SETTINGS output_format_json_quote_64bit_integers = 1`);
    expect(row).toEqual({ large_count: '18446744073709551615', signed_count: '-9223372036854775808', merge_count: '454' });
  });

  it('keeps original throughput and duration arithmetic in MB/s and seconds', async () => {
    await seed([
      { ...first, size_in_bytes: 1e6, rows: 100, duration_ms: 1000 },
      { ...first, size_in_bytes: 9e6, rows: 900, duration_ms: 3000 },
    ]);
    const [throughput] = await run('Merge Throughput by Table');
    expect(Number(throughput.merge_count)).toBe(2);
    expect(Number(throughput.avg_mb_per_sec)).toBe(2); // mean of 1 and 3 MB/s
    expect(Number(throughput.avg_rows_per_sec)).toBe(200);
    const [duration] = await run('Merge Duration by Table');
    expect(Number(duration.avg_duration_sec)).toBe(2);
    expect(Number(duration.p95_duration_sec)).toBe(2.9);
  });

  it('uses compressed result bytes at every size-bucket boundary', async () => {
    const cases: [number, string][] = [
      [0, '< 1 MiB'], [1048575, '< 1 MiB'], [1048576, '1-10 MiB'],
      [10485759, '1-10 MiB'], [10485760, '10-100 MiB'],
      [104857599, '10-100 MiB'], [104857600, '100 MiB-1 GiB'],
      [GiB - 1, '100 MiB-1 GiB'], [GiB, '1-10 GiB'],
      [10 * GiB - 1, '1-10 GiB'], [10 * GiB, '10-100 GiB'],
      [100 * GiB - 1, '10-100 GiB'], [100 * GiB, '≥ 100 GiB'],
    ];
    await seed(cases.map(([size], index) => ({
      ...first, size_in_bytes: size, read_bytes: 200 * GiB,
      ProfileEvents: { UserTimeMicroseconds: (index + 1) * 1e6 },
    })));
    const rows = await run('Merge CPU by Result Size');
    for (const bucket of new Set(cases.map(([, bucket]) => bucket))) {
      const matching = cases.map(([, label], index) => ({ label, cpu: index + 1 })).filter(row => row.label === bucket);
      const row = rows.find(row => row.size_bucket === bucket)!;
      expect(Number(row.merge_count), bucket).toBe(matching.length);
      expect(Number(row.cpu_seconds), bucket).toBe(matching.reduce((sum, row) => sum + row.cpu, 0));
    }
  });

  it('counts work on each replica and allows CPU core-seconds to exceed merge duration', async () => {
    // Same result part and completion time on two hosts: both consumed resources.
    await seed([
      { ...first, hostname: 'replica_a', duration_ms: 2000, ProfileEvents: { UserTimeMicroseconds: 6e6 } },
      { ...first, hostname: 'replica_b', duration_ms: 2000, ProfileEvents: { UserTimeMicroseconds: 4e6 } },
    ]);
    const [row] = await run('Merge CPU by Table');
    expect(Number(row.merge_count)).toBe(2);
    expect(Number(row.cpu_seconds)).toBe(10);
    expect(Number(row.elapsed_seconds)).toBe(4);
    expect(Number(row.avg_cpu_seconds)).toBe(5);
    expect(Number(row.avg_elapsed_seconds)).toBe(2);
    expect(Number(row.cpu_seconds_per_input_gib)).toBe(5);
  });

  it('ranks CPU and elapsed panels by their respective costs', async () => {
    await seed([
      { ...first, table: 'cpu_heavy', duration_ms: 1000, ProfileEvents: { UserTimeMicroseconds: 10e6 } },
      { ...first, table: 'slow', duration_ms: 10000, ProfileEvents: { UserTimeMicroseconds: 1e6 } },
    ]);
    const cpu = await run('Merge CPU by Table', {});
    const elapsed = await run('Merge Elapsed Time by Table', {});
    expect(cpu.map(row => row.tbl)).toEqual(['app.cpu_heavy', 'app.slow']);
    expect(elapsed.map(row => row.tbl)).toEqual(['app.slow', 'app.cpu_heavy']);
    expect(Number(cpu[0].cpu_share_pct)).toBe(90.91);
    expect(Number(cpu[0].elapsed_share_pct)).toBe(9.09);
    expect(Number(elapsed[0].elapsed_share_pct)).toBe(90.91);
  });

  it('preserves unclassifiable input levels without losing their cost', async () => {
    await seed([
      { ...first, merged_from: [] },
      { ...first, part_name: 'unrecognized' },
      { ...first, merged_from: ['all_1_1_0', 'unrecognized'] },
      { ...first, part_name: '202610_1_4_11_200', merged_from: ['202610_1_2_10_99', '202610_3_4_10_200'] },
      { ...first, part_name: 'all_1_6_3', merged_from: ['all_1_2_2', 'all_3_4_0', 'all_5_6_1'] },
    ]);
    const rows = await run('Merge CPU by Level Transition');
    const unknown = rows.find(row => row.level_transition === 'Unknown')!;
    expect(Number(unknown.merge_count)).toBe(3);
    expect(Number(unknown.cpu_seconds)).toBe(9);
    expect(Number(rows.find(row => row.level_transition === '10 → 11')!.cpu_seconds)).toBe(3);
    expect(Number(rows.find(row => row.level_transition === '0-2 → 3')!.cpu_seconds)).toBe(3);
    expect(rows.reduce((sum, row) => sum + Number(row.cpu_seconds), 0)).toBe(15);
  });

  it('keeps the drilldown percentage denominator intact beyond its 200-row limit', async () => {
    await seed(Array.from({ length: 400 }, (_, level) => ({
      ...first, part_name: `all_1_2_${level + 1}`, merged_from: [`all_1_1_${level}`],
      ProfileEvents: { UserTimeMicroseconds: 1e6 },
    })));
    const [total] = await run('Merge CPU by Table');
    expect(Number(total.merge_count)).toBe(400);
    const rows = await run('Table Merge Cost Breakdown');
    expect(rows).toHaveLength(200);
    for (const row of rows) {
      for (const metric of ['cpu_share_pct', 'elapsed_share_pct', 'merge_share_pct']) {
        expect(Number(row[metric]), metric).toBe(0.25); // 100 / 400, not 100 / 200
      }
    }
  });

  it('returns hand-calculated 15m, 1h, and 6h totals across midnight for every cost breakdown', async () => {
    const clock = '2026-10-22 00:30:00';
    const clockMs = Date.parse('2026-10-22T00:30:00Z');
    function event(minutesAgo: number, durationMs: number, cpuUs: number, extra: Record<string, unknown> = {}) {
      const timestamp = new Date(clockMs - minutesAgo * 60000).toISOString();
      return { ...first, event_time: timestamp.slice(0, 19).replace('T', ' '), event_date: timestamp.slice(0, 10),
        duration_ms: durationMs, ProfileEvents: { UserTimeMicroseconds: cpuUs }, ...extra };
    }
    await seed([
      event(5, 120000, 90e6),
      event(10, 180000, 120e6, { size_in_bytes: 20 * 1048576 }),
      event(30, 240000, 180e6, { part_name: 'all_1_4_2', merged_from: ['all_1_2_1'], size_in_bytes: 200 * 1048576 }),
      event(120, 600000, 450e6, { part_name: 'all_1_8_3', merged_from: ['all_1_4_2', 'all_5_5_0'], size_in_bytes: 2 * GiB, merge_reason: 'TTLRecompressMerge' }),
      event(420, 900000, 600e6), // outside all three windows
      event(5, 900000, 600e6, { table: 'other' }), // outside the selected table
      // Mutations: only in the mutation charts, never in the merge totals above.
      event(5, 60000, 30e6, { event_type: 'MutatePart', part_name: 'all_1_2_1_7', merged_from: ['all_1_2_1'] }),
      event(120, 90000, 45e6, { event_type: 'MutatePart', part_name: 'all_3_4_1_7', merged_from: ['all_3_4_1'] }),
    ]);
    // CPU is microseconds → seconds; duration is milliseconds → seconds.
    // These exact totals also catch an extra /1000 or a default 1 DAY leaking through.
    const expectations = [
      { interval: '15 MINUTE', count: 2, cpu: 210, elapsed: 300, mutations: { parts_rewritten: 1, cpu_seconds: 30, elapsed_seconds: 60 } },
      { interval: '1 HOUR', count: 3, cpu: 390, elapsed: 540, mutations: { parts_rewritten: 1, cpu_seconds: 30, elapsed_seconds: 60 } },
      { interval: '6 HOUR', count: 4, cpu: 840, elapsed: 1140, mutations: { parts_rewritten: 2, cpu_seconds: 75, elapsed_seconds: 150 } },
    ];
    for (const expected of expectations) {
      for (const query of queries) {
        const title = query.match(/title='([^']+)'/)![1];
        const rows = await run(title, { tbl: 'app.events' }, expected.interval, clock);
        const totals = title.startsWith('Mutation ')
          ? expected.mutations
          : { merge_count: expected.count, cpu_seconds: expected.cpu, elapsed_seconds: expected.elapsed };
        for (const [metric, value] of Object.entries(totals)) {
          expect(rows.reduce((sum, row) => sum + Number(row[metric]), 0), `${expected.interval} / ${title} / ${metric}`).toBe(value);
        }
      }
    }
  });

  it('applies the exact lower cutoff independently for 15m, 1h, and 6h', async () => {
    const clock = '2026-10-22 12:00:00';
    const clockMs = Date.parse('2026-10-22T12:00:00Z');
    const minutes = [15, 60, 360];
    await seed(minutes.flatMap((age, index) => [-1, 0, 1].map(offset => ({
      ...first, duration_ms: 1000, ProfileEvents: { UserTimeMicroseconds: 1e6 },
      table: `boundary_${index}`,
      event_time: new Date(clockMs - age * 60000 + offset * 1000).toISOString().slice(0, 19).replace('T', ' '),
      event_date: '2026-10-22',
    }))));
    for (const [index, interval] of ['15 MINUTE', '1 HOUR', '6 HOUR'].entries()) {
      const [row] = await run('Merge CPU by Table', { tbl: `app.boundary_${index}` }, interval, clock);
      expect(Number(row.merge_count), interval).toBe(1); // only the sample one second inside the window
      expect(Number(row.cpu_seconds), interval).toBe(1);
      expect(Number(row.elapsed_seconds), interval).toBe(1);
    }
  });
});
