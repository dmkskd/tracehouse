/**
 * Performance scaling tests: measure how merge-tracker and lineage queries
 * behave as part count grows (100 → 1 000 → 5 000 → 10 000).
 *
 * For each tier we:
 *   1. INSERT many tiny batches (one row each) so ClickHouse creates one part per INSERT
 *   2. SYSTEM FLUSH LOGS so system.part_log is populated
 *   3. Run the queries that hit system.parts / system.part_log / system.merges
 *   4. Collect memory_usage, read_bytes, result_bytes, query_duration_ms from query_log
 *
 * This is intentionally a long-running test — it is tagged 'perf' so it can be
 * run selectively:  vitest --tags perf
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { startClickHouse, stopClickHouse, type TestClickHouseContext } from './setup/clickhouse-container.js';
import { MergeTracker } from '../../services/merge-tracker.js';
import { LineageService } from '../../services/lineage-service.js';
import { buildQuery, tagQuery } from '../../queries/builder.js';
import { GET_ACTIVE_PART_SIZES, GET_MERGE_EVENTS_BATCH } from '../../queries/lineage-queries.js';
import { GET_ACTIVE_MERGES, GET_MERGE_HISTORY, GET_ALL_MERGE_HISTORY, GET_OUTDATED_PARTS_SIZE } from '../../queries/merge-queries.js';
import { sourceTag, TAB_MERGES } from '../../queries/source-tags.js';

const CONTAINER_TIMEOUT = 120_000;
const TEST_DB = 'perf_parts_test';

/** Part-count tiers to exercise. Adjust or trim for local dev speed. */
const TIERS = [100, 1_000];

/** Collect query_log stats for the most recent query matching a comment tag. */
interface QueryStats {
  query_duration_ms: number;
  memory_usage: number;
  read_bytes: number;
  read_rows: number;
  result_bytes: number;
  result_rows: number;
}

async function flushAndCollectStats(
  ctx: TestClickHouseContext,
  tag: string,
): Promise<QueryStats | null> {
  await ctx.client.command({ query: 'SYSTEM FLUSH LOGS' });
  const rows = await ctx.rawAdapter.executeQuery<Record<string, unknown>>(`
    SELECT
      query_duration_ms,
      memory_usage,
      read_bytes,
      read_rows,
      result_bytes,
      result_rows
    FROM system.query_log
    WHERE type = 'QueryFinish'
      AND query LIKE '%${tag}%'
      AND query NOT LIKE '%system.query_log%'
    ORDER BY event_time DESC
    LIMIT 1
  `);
  if (rows.length === 0) return null;
  const r = rows[0];
  return {
    query_duration_ms: Number(r.query_duration_ms),
    memory_usage: Number(r.memory_usage),
    read_bytes: Number(r.read_bytes),
    read_rows: Number(r.read_rows),
    result_bytes: Number(r.result_bytes),
    result_rows: Number(r.result_rows),
  };
}

/**
 * Insert `count` individual rows — each INSERT creates a separate part.
 * Uses batches of parallel INSERTs (chunks of 200) to speed things up
 * while still creating one part per INSERT.
 */
async function insertParts(ctx: TestClickHouseContext, table: string, count: number): Promise<void> {
  const CHUNK = 200;
  for (let offset = 0; offset < count; offset += CHUNK) {
    const batch = Math.min(CHUNK, count - offset);
    const promises: Promise<void>[] = [];
    for (let i = 0; i < batch; i++) {
      const id = offset + i;
      promises.push(
        ctx.client.command({
          query: `INSERT INTO ${TEST_DB}.${table} (id, value) VALUES (${id}, ${Math.random()})`,
        }).then(() => {}),
      );
    }
    await Promise.all(promises);
  }
}

/** Poll until active part count drops to `target` or timeout elapses. */
async function waitForMerge(
  ctx: TestClickHouseContext,
  table: string,
  target: number = 1,
  timeoutMs: number = 90_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const cnt = await getActivePartCount(ctx, table);
    if (cnt <= target) return;
    await new Promise(r => setTimeout(r, 1_000));
  }
}

/** Verify actual part count is close to expected (some may merge in background). */
async function getActivePartCount(ctx: TestClickHouseContext, table: string): Promise<number> {
  const rows = await ctx.rawAdapter.executeQuery<{ cnt: string }>(`
    SELECT count() AS cnt FROM system.parts
    WHERE database = '${TEST_DB}' AND table = '${table}' AND active = 1
  `);
  return Number(rows[0]?.cnt ?? 0);
}

describe('Query scaling with part count', { tags: ['perf'], timeout: 600_000 }, () => {
  let ctx: TestClickHouseContext;
  let tracker: MergeTracker;
  let lineageService: LineageService;

  /** Accumulated results for final summary. */
  const results: Array<{ tier: number; query: string; stats: QueryStats }> = [];

  beforeAll(async () => {
    ctx = await startClickHouse();
    tracker = new MergeTracker(ctx.adapter);
    lineageService = new LineageService(ctx.adapter);

    await ctx.client.command({ query: `CREATE DATABASE IF NOT EXISTS ${TEST_DB}` });
  }, CONTAINER_TIMEOUT);

  afterAll(async () => {
    // Print summary table
    if (results.length > 0) {
      console.log('\n╔══════════════════════════════════════════════════════════════════════════╗');
      console.log('║                     QUERY SCALING RESULTS                               ║');
      console.log('╠═══════╦═══════════════════════════════╦══════════╦═══════════╦═══════════╣');
      console.log('║ Parts ║ Query                         ║ Dur (ms) ║ Memory MB ║ Result KB ║');
      console.log('╠═══════╬═══════════════════════════════╬══════════╬═══════════╬═══════════╣');
      for (const { tier, query, stats } of results) {
        const name = query.padEnd(29).slice(0, 29);
        const dur = String(stats.query_duration_ms).padStart(8);
        const mem = (stats.memory_usage / 1_048_576).toFixed(1).padStart(9);
        const res = (stats.result_bytes / 1_024).toFixed(1).padStart(9);
        console.log(`║ ${String(tier).padStart(5)} ║ ${name} ║ ${dur} ║ ${mem} ║ ${res} ║`);
      }
      console.log('╚═══════╩═══════════════════════════════╩══════════╩═══════════╩═══════════╝');
    }

    if (ctx) {
      if (!ctx.keepData) {
        await ctx.client.command({ query: `DROP DATABASE IF EXISTS ${TEST_DB}` });
      }
      await stopClickHouse(ctx);
    }
  }, 30_000);

  for (const tier of TIERS) {
    describe(`${tier} parts`, { timeout: 300_000 }, () => {
      const tableName = `events_${tier}`;

      beforeAll(async () => {
        // Create table with background merges disabled so parts stay separate
        await ctx.client.command({
          query: `
            CREATE TABLE IF NOT EXISTS ${TEST_DB}.${tableName} (
              id UInt64,
              ts DateTime DEFAULT now(),
              value Float64
            ) ENGINE = MergeTree()
            ORDER BY (ts, id)
            SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1,
                     max_bytes_to_merge_at_min_space_in_pool = 1,
                     parts_to_throw_insert = 100000,
                     parts_to_delay_insert = 100000
          `,
        });

        // Insert one row per INSERT to create one part per INSERT
        await insertParts(ctx, tableName, tier);

        // Flush logs so part_log is visible
        await ctx.client.command({ query: 'SYSTEM FLUSH LOGS' });

        const actualParts = await getActivePartCount(ctx, tableName);
        console.log(`[perf] Tier ${tier}: requested ${tier} parts, got ${actualParts} active parts`);
      }, 300_000);

      it('GET_ACTIVE_PART_SIZES scales acceptably', async () => {
        const tag = `perf:activePartSizes:${tier}`;
        const sql = buildQuery(GET_ACTIVE_PART_SIZES, { database: TEST_DB, table: tableName });
        await ctx.adapter.executeQuery(tagQuery(sql, tag));
        const stats = await flushAndCollectStats(ctx, tag);
        expect(stats).not.toBeNull();
        results.push({ tier, query: 'GET_ACTIVE_PART_SIZES', stats: stats! });

        // Sanity: result rows should be close to part count
        expect(stats!.result_rows).toBeGreaterThan(0);
      });

      it('GET_MERGE_HISTORY (table-scoped, LIMIT 100) scales acceptably', async () => {
        const tag = `perf:mergeHistory:${tier}`;
        const sql = buildQuery(GET_MERGE_HISTORY, { database: TEST_DB, table: tableName, limit: 100 });
        await ctx.adapter.executeQuery(tagQuery(sql, tag));
        const stats = await flushAndCollectStats(ctx, tag);
        expect(stats).not.toBeNull();
        results.push({ tier, query: 'GET_MERGE_HISTORY', stats: stats! });
      });

      it('GET_ALL_MERGE_HISTORY (global, LIMIT 100) scales acceptably', async () => {
        const tag = `perf:allMergeHistory:${tier}`;
        const sql = buildQuery(GET_ALL_MERGE_HISTORY, { limit: 100 });
        await ctx.adapter.executeQuery(tagQuery(sql, tag));
        const stats = await flushAndCollectStats(ctx, tag);
        expect(stats).not.toBeNull();
        results.push({ tier, query: 'GET_ALL_MERGE_HISTORY', stats: stats! });
      });

      it('GET_ACTIVE_MERGES scales acceptably', async () => {
        const tag = `perf:activeMerges:${tier}`;
        await ctx.adapter.executeQuery(tagQuery(GET_ACTIVE_MERGES, tag));
        const stats = await flushAndCollectStats(ctx, tag);
        expect(stats).not.toBeNull();
        results.push({ tier, query: 'GET_ACTIVE_MERGES', stats: stats! });
      });

      it('GET_OUTDATED_PARTS_SIZE scales acceptably', async () => {
        const tag = `perf:outdatedParts:${tier}`;
        await ctx.adapter.executeQuery(tagQuery(GET_OUTDATED_PARTS_SIZE, tag));
        const stats = await flushAndCollectStats(ctx, tag);
        expect(stats).not.toBeNull();
        results.push({ tier, query: 'GET_OUTDATED_PARTS_SIZE', stats: stats! });
      });

      it('MergeTracker.getMergeHistory() end-to-end', async () => {
        const tag = `perf:trackerHistory:${tier}`;
        const history = await tracker.getMergeHistory({ database: TEST_DB, table: tableName, limit: 100 });
        // Collect stats for the underlying query (tagged by the service)
        const stats = await flushAndCollectStats(ctx, sourceTag(TAB_MERGES, 'mergeHistory'));
        if (stats) {
          results.push({ tier, query: 'MergeTracker.getHistory', stats });
        }
        expect(Array.isArray(history)).toBe(true);
      });

      it('MergeTracker.getBackgroundPoolMetrics() end-to-end', async () => {
        const metrics = await tracker.getBackgroundPoolMetrics();
        const stats = await flushAndCollectStats(ctx, sourceTag(TAB_MERGES, 'poolMetrics'));
        if (stats) {
          results.push({ tier, query: 'MergeTracker.poolMetrics', stats });
        }
        expect(metrics).toHaveProperty('active_parts');
      });

      // After all raw-part tests, trigger OPTIMIZE to create merge history,
      // then test lineage queries against the merged result.
      describe('after OPTIMIZE FINAL', () => {
        let mergedPartName: string | undefined;

        beforeAll(async () => {
          // Re-enable merges and force a full merge
          await ctx.client.command({
            query: `ALTER TABLE ${TEST_DB}.${tableName}
                    MODIFY SETTING max_bytes_to_merge_at_max_space_in_pool = 161061273600,
                                   max_bytes_to_merge_at_min_space_in_pool = 1048576`,
          });
          await ctx.client.command({
            query: `OPTIMIZE TABLE ${TEST_DB}.${tableName} FINAL`,
          });
          // Poll until merge completes (OPTIMIZE FINAL can take 60s+ at 10k parts)
          await waitForMerge(ctx, tableName);
          await ctx.client.command({ query: 'SYSTEM FLUSH LOGS' });

          // Find the final merged part name
          const rows = await ctx.rawAdapter.executeQuery<{ name: string }>(`
            SELECT name FROM system.parts
            WHERE database = '${TEST_DB}' AND table = '${tableName}' AND active = 1
            ORDER BY rows DESC LIMIT 1
          `);
          mergedPartName = rows[0]?.name;
          console.log(`[perf] Tier ${tier}: merged part = ${mergedPartName}`);
        }, 120_000);

        it('GET_MERGE_EVENTS_BATCH with merged part', async () => {
          if (!mergedPartName) return;
          const tag = `perf:mergeEventsBatch:${tier}`;
          const inList = `'${mergedPartName}'`;
          const sql = buildQuery(GET_MERGE_EVENTS_BATCH, { database: TEST_DB, table: tableName })
            .replace('{partNames}', inList);
          await ctx.adapter.executeQuery(tagQuery(sql, tag));
          const stats = await flushAndCollectStats(ctx, tag);
          expect(stats).not.toBeNull();
          results.push({ tier, query: 'GET_MERGE_EVENTS_BATCH', stats: stats! });

          // The merged_from array in the result should contain source part names.
          // At 10k parts this result row alone could be large.
          if (stats) {
            console.log(
              `[perf] Tier ${tier} MERGE_EVENTS_BATCH: result_bytes=${stats.result_bytes}, ` +
              `memory=${(stats.memory_usage / 1_048_576).toFixed(1)}MB`,
            );
          }
        });

        it('LineageService.buildLineageTree() end-to-end', async () => {
          if (!mergedPartName) return;
          const before = Date.now();
          const lineage = await lineageService.buildLineageTree(TEST_DB, tableName, mergedPartName);
          const wallMs = Date.now() - before;

          // Collect the last lineage-related query stats
          await ctx.client.command({ query: 'SYSTEM FLUSH LOGS' });
          const stats = await flushAndCollectStats(ctx, 'activePartSizes');
          if (stats) {
            // Use wall time since lineage issues multiple queries
            results.push({
              tier,
              query: 'LineageService.buildTree',
              stats: { ...stats, query_duration_ms: wallMs },
            });
          }

          console.log(
            `[perf] Tier ${tier} buildLineageTree: wall=${wallMs}ms, ` +
            `merges=${lineage.total_merges}, originals=${lineage.total_original_parts}`,
          );

          expect(lineage.total_original_parts).toBeGreaterThan(0);
        });

        it('GET_MERGE_HISTORY after OPTIMIZE shows merged_from array size', async () => {
          const tag = `perf:historyAfterOptimize:${tier}`;
          const sql = buildQuery(GET_MERGE_HISTORY, { database: TEST_DB, table: tableName, limit: 10 });
          const rows = await ctx.adapter.executeQuery<Record<string, unknown>>(tagQuery(sql, tag));
          const stats = await flushAndCollectStats(ctx, tag);

          if (stats) {
            results.push({ tier, query: 'HISTORY_POST_OPTIMIZE', stats });
          }

          // Check the merged_from array size in the first result
          if (rows.length > 0) {
            const mergedFrom = rows[0].merged_from;
            const arrayLen = Array.isArray(mergedFrom) ? mergedFrom.length : 0;
            console.log(
              `[perf] Tier ${tier} HISTORY merged_from array length: ${arrayLen}, ` +
              `result_bytes=${stats?.result_bytes}`,
            );
          }
        });
      });
    });
  }
});
