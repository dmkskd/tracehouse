/**
 * Performance scaling tests for lineage queries.
 *
 * With lots of parts (e.g. after frequent inserts + TTL deletes in a demo env),
 * the lineage queries can OOM because:
 *   - GET_ACTIVE_PART_SIZES has no LIMIT and fetches every active part
 *   - GET_MERGE_EVENTS_BATCH returns rows whose merged_from arrays can be huge
 *   - GET_L0_PART_SIZES can fetch thousands of part names in IN clauses
 *   - LineageService.buildLineageTree() builds an in-memory tree of all of that
 *
 * For each tier we:
 *   1. INSERT many tiny batches (one row each) so ClickHouse creates one part per INSERT
 *   2. SYSTEM FLUSH LOGS so system.part_log is populated
 *   3. Run the lineage queries and collect query_log stats
 *   4. After OPTIMIZE FINAL, test buildLineageTree() end-to-end
 *
 * Tagged 'perf' — run selectively:  vitest --tags perf
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { startClickHouse, stopClickHouse, type TestClickHouseContext } from './setup/clickhouse-container.js';
import { LineageService } from '../../services/lineage-service.js';
import { buildQuery, tagQuery } from '../../queries/builder.js';
import { GET_ACTIVE_PART_SIZES, GET_MERGE_EVENTS_BATCH, GET_L0_PART_SIZES } from '../../queries/lineage-queries.js';

const CONTAINER_TIMEOUT = 120_000;
const TEST_DB = 'perf_lineage_test';

/** Part-count tiers to exercise. */
const TIERS = [100, 1_000, 5_000];

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
 * Uses batches of parallel INSERTs (chunks of 200) to speed things up.
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

async function getActivePartCount(ctx: TestClickHouseContext, table: string): Promise<number> {
  const rows = await ctx.rawAdapter.executeQuery<{ cnt: string }>(`
    SELECT count() AS cnt FROM system.parts
    WHERE database = '${TEST_DB}' AND table = '${table}' AND active = 1
  `);
  return Number(rows[0]?.cnt ?? 0);
}

async function waitForMerge(
  ctx: TestClickHouseContext,
  table: string,
  target: number = 1,
  timeoutMs: number = 120_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const cnt = await getActivePartCount(ctx, table);
    if (cnt <= target) return;
    await new Promise(r => setTimeout(r, 1_000));
  }
}

describe('Lineage query scaling with part count', { tags: ['perf'], timeout: 600_000 }, () => {
  let ctx: TestClickHouseContext;
  let lineageService: LineageService;

  const results: Array<{ tier: number; query: string; stats: QueryStats }> = [];

  beforeAll(async () => {
    ctx = await startClickHouse();
    lineageService = new LineageService(ctx.adapter);
    await ctx.client.command({ query: `CREATE DATABASE IF NOT EXISTS ${TEST_DB}` });
  }, CONTAINER_TIMEOUT);

  afterAll(async () => {
    if (results.length > 0) {
      console.log('\n╔═══════════════════════════════════════════════════════════════════════════════╗');
      console.log('║                  LINEAGE QUERY SCALING RESULTS                               ║');
      console.log('╠═══════╦═══════════════════════════════╦══════════╦═══════════╦════════════════╣');
      console.log('║ Parts ║ Query                         ║ Dur (ms) ║ Memory MB ║ Result KB      ║');
      console.log('╠═══════╬═══════════════════════════════╬══════════╬═══════════╬════════════════╣');
      for (const { tier, query, stats } of results) {
        const name = query.padEnd(29).slice(0, 29);
        const dur = String(stats.query_duration_ms).padStart(8);
        const mem = (stats.memory_usage / 1_048_576).toFixed(1).padStart(9);
        const res = (stats.result_bytes / 1_024).toFixed(1).padStart(14);
        console.log(`║ ${String(tier).padStart(5)} ║ ${name} ║ ${dur} ║ ${mem} ║ ${res} ║`);
      }
      console.log('╚═══════╩═══════════════════════════════╩══════════╩═══════════╩════════════════╝');
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
      const tableName = `lineage_${tier}`;

      beforeAll(async () => {
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

        await insertParts(ctx, tableName, tier);
        await ctx.client.command({ query: 'SYSTEM FLUSH LOGS' });

        const actualParts = await getActivePartCount(ctx, tableName);
        console.log(`[lineage-perf] Tier ${tier}: requested ${tier} parts, got ${actualParts} active parts`);
      }, 300_000);

      // ── Raw query tests (before OPTIMIZE) ─────────────────────────────

      it('GET_ACTIVE_PART_SIZES scales acceptably', async () => {
        const tag = `perf:lineage:activePartSizes:${tier}`;
        const sql = buildQuery(GET_ACTIVE_PART_SIZES, { database: TEST_DB, table: tableName });
        await ctx.adapter.executeQuery(tagQuery(sql, tag));
        const stats = await flushAndCollectStats(ctx, tag);
        expect(stats).not.toBeNull();
        results.push({ tier, query: 'GET_ACTIVE_PART_SIZES', stats: stats! });

        console.log(
          `[lineage-perf] Tier ${tier} ACTIVE_PART_SIZES: ` +
          `rows=${stats!.result_rows}, result_bytes=${stats!.result_bytes}, ` +
          `memory=${(stats!.memory_usage / 1_048_576).toFixed(1)}MB`,
        );
      });

      it('GET_L0_PART_SIZES with all parts', async () => {
        // Simulate the worst case: query all part names via IN clause
        const partRows = await ctx.rawAdapter.executeQuery<{ name: string }>(`
          SELECT name FROM system.parts
          WHERE database = '${TEST_DB}' AND table = '${tableName}' AND active = 1
          LIMIT 2000
        `);
        const partNames = partRows.map(r => r.name);
        if (partNames.length === 0) return;

        const tag = `perf:lineage:l0Sizes:${tier}`;
        const inList = partNames.map(p => `'${p}'`).join(',');
        const sql = buildQuery(GET_L0_PART_SIZES, { database: TEST_DB, table: tableName })
          .replace('{partNames}', inList);
        await ctx.adapter.executeQuery(tagQuery(sql, tag));
        const stats = await flushAndCollectStats(ctx, tag);
        expect(stats).not.toBeNull();
        results.push({ tier, query: 'GET_L0_PART_SIZES', stats: stats! });

        console.log(
          `[lineage-perf] Tier ${tier} L0_PART_SIZES: ` +
          `rows=${stats!.result_rows}, result_bytes=${stats!.result_bytes}, ` +
          `memory=${(stats!.memory_usage / 1_048_576).toFixed(1)}MB`,
        );
      });

      // ── After OPTIMIZE: test the full lineage tree build ──────────────

      describe('after OPTIMIZE FINAL', () => {
        let mergedPartName: string | undefined;

        beforeAll(async () => {
          await ctx.client.command({
            query: `ALTER TABLE ${TEST_DB}.${tableName}
                    MODIFY SETTING max_bytes_to_merge_at_max_space_in_pool = 161061273600,
                                   max_bytes_to_merge_at_min_space_in_pool = 1048576`,
          });
          await ctx.client.command({
            query: `OPTIMIZE TABLE ${TEST_DB}.${tableName} FINAL`,
          });
          await waitForMerge(ctx, tableName);
          await ctx.client.command({ query: 'SYSTEM FLUSH LOGS' });

          const rows = await ctx.rawAdapter.executeQuery<{ name: string }>(`
            SELECT name FROM system.parts
            WHERE database = '${TEST_DB}' AND table = '${tableName}' AND active = 1
            ORDER BY rows DESC LIMIT 1
          `);
          mergedPartName = rows[0]?.name;
          console.log(`[lineage-perf] Tier ${tier}: merged part = ${mergedPartName}`);
        }, 180_000);

        it('GET_MERGE_EVENTS_BATCH with merged part', async () => {
          if (!mergedPartName) return;
          const tag = `perf:lineage:mergeEventsBatch:${tier}`;
          const inList = `'${mergedPartName}'`;
          const sql = buildQuery(GET_MERGE_EVENTS_BATCH, { database: TEST_DB, table: tableName })
            .replace('{partNames}', inList);
          await ctx.adapter.executeQuery(tagQuery(sql, tag));
          const stats = await flushAndCollectStats(ctx, tag);
          expect(stats).not.toBeNull();
          results.push({ tier, query: 'GET_MERGE_EVENTS_BATCH', stats: stats! });

          console.log(
            `[lineage-perf] Tier ${tier} MERGE_EVENTS_BATCH: ` +
            `result_bytes=${stats!.result_bytes}, ` +
            `memory=${(stats!.memory_usage / 1_048_576).toFixed(1)}MB`,
          );
        });

        it('LineageService.buildLineageTree() end-to-end', async () => {
          if (!mergedPartName) return;
          const before = Date.now();
          const lineage = await lineageService.buildLineageTree(TEST_DB, tableName, mergedPartName);
          const wallMs = Date.now() - before;

          await ctx.client.command({ query: 'SYSTEM FLUSH LOGS' });
          // Use the last activePartSizes tag (first query in buildLineageTree)
          const stats = await flushAndCollectStats(ctx, 'activePartSizes');

          results.push({
            tier,
            query: 'buildLineageTree (wall)',
            stats: stats
              ? { ...stats, query_duration_ms: wallMs }
              : {
                  query_duration_ms: wallMs,
                  memory_usage: 0,
                  read_bytes: 0,
                  read_rows: 0,
                  result_bytes: 0,
                  result_rows: 0,
                },
          });

          console.log(
            `[lineage-perf] Tier ${tier} buildLineageTree: wall=${wallMs}ms, ` +
            `merges=${lineage.total_merges}, originals=${lineage.total_original_parts}`,
          );

          expect(lineage.total_original_parts).toBeGreaterThan(0);
        });

        it('merged_from array size in part_log', async () => {
          if (!mergedPartName) return;
          // Check how large the merged_from array is — this is the main OOM vector
          const rows = await ctx.rawAdapter.executeQuery<{
            part_name: string;
            merged_from_len: string;
            size_in_bytes: string;
          }>(`
            SELECT
              part_name,
              length(merged_from) AS merged_from_len,
              size_in_bytes
            FROM system.part_log
            WHERE database = '${TEST_DB}' AND table = '${tableName}'
              AND event_type = 'MergeParts'
            ORDER BY length(merged_from) DESC
            LIMIT 5
          `);

          for (const r of rows) {
            console.log(
              `[lineage-perf] Tier ${tier} part_log entry: ` +
              `part=${r.part_name}, merged_from_len=${r.merged_from_len}, ` +
              `size=${r.size_in_bytes}`,
            );
          }

          // At high part counts, merged_from can be very large
          if (rows.length > 0) {
            const maxLen = Number(rows[0].merged_from_len);
            console.log(`[lineage-perf] Tier ${tier}: largest merged_from array = ${maxLen}`);
          }
        });
      });
    });
  }
});
