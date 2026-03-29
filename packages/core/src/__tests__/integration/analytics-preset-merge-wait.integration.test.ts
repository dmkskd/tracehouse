/**
 * Targeted integration tests for merge analytics wait-time queries.
 *
 * Creates a realistic merge scenario:
 *   1. Disables background merges
 *   2. Inserts multiple batches (parts pile up)
 *   3. Waits a known duration (parts sit idle)
 *   4. Re-enables merges and forces OPTIMIZE
 *   5. Verifies wait times reflect the actual idle period
 *
 * This catches:
 *   - Negative wait times (bad join producing wrong event_time pairs)
 *   - Inflated wait times (joining duplicate part_log entries)
 *   - Drill chain correctness: by-table → by-size → timeline
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import {
  startClickHouse,
  stopClickHouse,
  type TestClickHouseContext,
} from './setup/clickhouse-container.js';
import { resolveTimeRange, resolveDrillParams } from '@frontend-analytics/templateResolution';
import mergeAnalyticsQueries from '@frontend-queries/mergeAnalytics';

const CONTAINER_TIMEOUT = 120_000;
const TEST_TIMEOUT = 30_000;
const DB = 'merge_analytics_test';
const TABLE = `${DB}.events`;

/** Pause in ms — parts will sit idle for at least this long. */
const IDLE_PAUSE_MS = 5_000;

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

/** Resolve a query the same way the DashboardViewer does at runtime. */
function resolve(rawSql: string, drillParams: Record<string, string> = {}): string {
  let sql = resolveTimeRange(rawSql, '1 HOUR');
  sql = resolveDrillParams(sql, drillParams);
  return sql;
}

/** Find a query by title from the merge analytics module. */
function findQuery(title: string): string {
  const q = mergeAnalyticsQueries.find(s => s.includes(`title='${title}'`));
  if (!q) throw new Error(`Query not found: ${title}`);
  return q;
}

describe('Merge analytics wait-time queries', { tags: ['analytics'] }, () => {
  let ctx: TestClickHouseContext;

  beforeAll(async () => {
    ctx = await startClickHouse();

    await ctx.client.command({ query: `CREATE DATABASE IF NOT EXISTS ${DB}` });
    await ctx.client.command({
      query: `
        CREATE TABLE IF NOT EXISTS ${TABLE} (
          id UInt64,
          ts DateTime DEFAULT now(),
          category LowCardinality(String) DEFAULT ['web','api','mobile','batch','cron'][rand() % 5 + 1],
          status UInt16 DEFAULT rand() % 500,
          value Float64,
          payload String DEFAULT randomPrintableASCII(256),
          tags Array(String) DEFAULT [randomPrintableASCII(16), randomPrintableASCII(16)]
        ) ENGINE = MergeTree()
        ORDER BY (ts, id)
      `,
    });

    // 1. Stop background merges so parts accumulate
    await ctx.client.command({ query: `SYSTEM STOP MERGES ${TABLE}` });

    // 2. Insert many batches — each creates a separate part with ~2KB+ per row
    for (let i = 0; i < 15; i++) {
      await ctx.client.command({
        query: `INSERT INTO ${TABLE} (id, value)
                SELECT number + ${i * 5000}, rand() FROM numbers(5000)`,
      });
    }

    // 3. Let parts sit idle for a known duration
    await sleep(IDLE_PAUSE_MS);

    // 4. Re-enable merges and force them
    await ctx.client.command({ query: `SYSTEM START MERGES ${TABLE}` });
    await ctx.client.command({ query: `OPTIMIZE TABLE ${TABLE} FINAL` });

    // 5. Flush logs so part_log has the merge events
    await ctx.client.command({ query: `SYSTEM FLUSH LOGS` });
  }, CONTAINER_TIMEOUT);

  afterAll(async () => {
    if (ctx) {
      await ctx.client.command({ query: `DROP DATABASE IF EXISTS ${DB}` });
      await stopClickHouse(ctx);
    }
  }, 30_000);

  it('Part Wait Time by Table: wait times are non-negative and reflect idle period', async () => {
    const sql = resolve(findQuery('Part Wait Time by Table'), { tbl: TABLE });
    const rows = await ctx.adapter.executeQuery<{
      tbl: string;
      parts_merged: string;
      avg_wait_sec: string;
      p95_wait_sec: string;
    }>(sql);

    expect(rows.length).toBeGreaterThanOrEqual(1);
    const row = rows.find(r => r.tbl === TABLE)!;
    expect(row).toBeDefined();

    const avg = Number(row.avg_wait_sec);
    const p95 = Number(row.p95_wait_sec);

    // Must be non-negative
    expect(avg).toBeGreaterThanOrEqual(0);
    expect(p95).toBeGreaterThanOrEqual(0);

    // Should reflect the idle pause (at least ~3s, since event_time has 1s precision)
    expect(avg).toBeGreaterThanOrEqual(IDLE_PAUSE_MS / 1000 - 2);

    // Should not be wildly inflated (the old buggy join produced 7000+ seconds)
    expect(avg).toBeLessThan(60);
    expect(p95).toBeLessThan(60);
  }, TEST_TIMEOUT);

  it('Part Wait Time by Size: non-negative and plausible per bucket', async () => {
    const sql = resolve(findQuery('Part Wait Time by Size'), { tbl: TABLE });
    const rows = await ctx.adapter.executeQuery<{
      size_bucket: string;
      parts_merged: string;
      avg_wait_sec: string;
      p95_wait_sec: string;
    }>(sql);

    expect(rows.length).toBeGreaterThanOrEqual(1);

    for (const row of rows) {
      const avg = Number(row.avg_wait_sec);
      const p95 = Number(row.p95_wait_sec);
      expect(avg).toBeGreaterThanOrEqual(0);
      expect(p95).toBeGreaterThanOrEqual(0);
      expect(avg).toBeLessThan(60);
      expect(Number(row.parts_merged)).toBeGreaterThan(0);
    }
  }, TEST_TIMEOUT);

  it('Part Wait Timeline: non-negative per-event data', async () => {
    const sql = resolve(findQuery('Part Wait Timeline'), { tbl: TABLE });
    const rows = await ctx.adapter.executeQuery<{
      t: string;
      src_part: string;
      wait_sec: string;
    }>(sql);

    expect(rows.length).toBeGreaterThanOrEqual(1);

    for (const row of rows) {
      expect(Number(row.wait_sec)).toBeGreaterThanOrEqual(0);
      expect(Number(row.wait_sec)).toBeLessThan(60);
      expect(row.src_part).toBeTruthy();
    }
  }, TEST_TIMEOUT);

  it('drill chain: table → size bucket → timeline', async () => {
    // Level 1: by table
    const tableSql = resolve(findQuery('Part Wait Time by Table'), { tbl: TABLE });
    const tableRows = await ctx.adapter.executeQuery<{ tbl: string }>(tableSql);
    expect(tableRows.length).toBeGreaterThanOrEqual(1);

    // Level 2: drill into size buckets for this table
    const sizeSql = resolve(findQuery('Part Wait Time by Size'), { tbl: TABLE });
    const sizeRows = await ctx.adapter.executeQuery<{ size_bucket: string }>(sizeSql);
    expect(sizeRows.length).toBeGreaterThanOrEqual(1);
    const bucket = sizeRows[0].size_bucket;

    // Level 3: drill into timeline for this table + bucket
    const timelineSql = resolve(findQuery('Part Wait Timeline'), {
      tbl: TABLE,
      size_bucket: bucket,
    });
    const timelineRows = await ctx.adapter.executeQuery<{
      t: string;
      wait_sec: string;
    }>(timelineSql);

    expect(timelineRows.length).toBeGreaterThanOrEqual(1);
    for (const row of timelineRows) {
      expect(Number(row.wait_sec)).toBeGreaterThanOrEqual(0);
    }
  }, TEST_TIMEOUT);

  it('Merge Throughput by Table: returns positive throughput', async () => {
    const sql = resolve(findQuery('Merge Throughput by Table'), { tbl: TABLE });
    const rows = await ctx.adapter.executeQuery<{
      tbl: string;
      merge_count: string;
      avg_mb_per_sec: string;
    }>(sql);

    expect(rows.length).toBeGreaterThanOrEqual(1);
    const row = rows.find(r => r.tbl === TABLE)!;
    expect(row).toBeDefined();
    expect(Number(row.merge_count)).toBeGreaterThan(0);
    expect(Number(row.avg_mb_per_sec)).toBeGreaterThanOrEqual(0);
  }, TEST_TIMEOUT);

  it('Merge Duration by Table: returns positive durations', async () => {
    const sql = resolve(findQuery('Merge Duration by Table'), { tbl: TABLE });
    const rows = await ctx.adapter.executeQuery<{
      tbl: string;
      merge_count: string;
      avg_duration_sec: string;
    }>(sql);

    expect(rows.length).toBeGreaterThanOrEqual(1);
    const row = rows.find(r => r.tbl === TABLE)!;
    expect(row).toBeDefined();
    expect(Number(row.merge_count)).toBeGreaterThan(0);
    expect(Number(row.avg_duration_sec)).toBeGreaterThanOrEqual(0);
  }, TEST_TIMEOUT);
});
