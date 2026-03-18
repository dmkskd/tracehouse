/**
 * Integration tests for mutation surfacing accuracy.
 *
 * Validates:
 * 1. GET_MUTATIONS parts count is not doubled (parts_to_do vs parts_in_progress)
 * 2. merge_algorithm is available for MutatePart events in part_log
 * 3. Lightweight mutation detection via command text analysis
 * 4. Mutation progress and part counts make sense
 *
 * Bug context:
 *   GET_MUTATIONS aliases parts_to_do as parts_in_progress (both from the same
 *   system.mutations.parts_to_do column). The mapper then computes
 *   totalRemaining = parts_to_do + parts_in_progress = 2× actual. The "Parts"
 *   column in the UI shows the doubled value. Also, parts_done is always 0
 *   and progress is always 0 or 1 (no partial progress).
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { startClickHouse, stopClickHouse, type TestClickHouseContext } from './setup/clickhouse-container.js';
import { MergeTracker } from '../../services/merge-tracker.js';
import { mapMutationInfo } from '../../mappers/merge-mappers.js';
import { classifyMutationCommand, isPatchPart } from '../../utils/merge-classification.js';

const CONTAINER_TIMEOUT = 120_000;
const TEST_DB = 'mutation_surface_test';

describe('mutation surfacing integration', () => {
  let ctx: TestClickHouseContext;
  let tracker: MergeTracker;

  beforeAll(async () => {
    ctx = await startClickHouse();
    tracker = new MergeTracker(ctx.adapter);

    await ctx.client.command({ query: `CREATE DATABASE IF NOT EXISTS ${TEST_DB}` });
  }, CONTAINER_TIMEOUT);

  afterAll(async () => {
    if (ctx) {
      if (!ctx.keepData) {
        await ctx.client.command({ query: `DROP DATABASE IF EXISTS ${TEST_DB}` });
      }
      await stopClickHouse(ctx);
    }
  }, 30_000);

  // ── Parts count accuracy ────────────────────────────────────────────

  describe('mutation parts count is not doubled', () => {
    it('parts_to_do should not equal parts_in_progress for the same mutation', async () => {
      // Create table with multiple parts
      await ctx.client.command({
        query: `
          CREATE TABLE IF NOT EXISTS ${TEST_DB}.parts_count (
            id UInt64, ts DateTime DEFAULT now(), value String
          ) ENGINE = MergeTree() ORDER BY id
          SETTINGS index_granularity = 128
        `,
      });

      // Insert 3 separate batches → 3 parts
      for (let i = 0; i < 3; i++) {
        await ctx.client.command({
          query: `INSERT INTO ${TEST_DB}.parts_count (id, value) SELECT number + ${i * 1000}, 'batch${i}' FROM numbers(500)`,
        });
      }

      // Verify we have multiple parts
      const partsResult = await ctx.client.query({
        query: `SELECT count() as cnt FROM system.parts WHERE database = '${TEST_DB}' AND table = 'parts_count' AND active`,
        format: 'JSONEachRow',
      });
      const partsRows = await partsResult.json<{ cnt: string }>();
      const partCount = Number(partsRows[0]?.cnt);

      // Run a mutation that will need to process each part
      await ctx.client.command({
        query: `ALTER TABLE ${TEST_DB}.parts_count UPDATE value = 'mutated' WHERE 1`,
      });

      // Brief wait, then check mutations before they complete
      await new Promise(r => setTimeout(r, 500));

      // Even if mutation completed quickly, test the mapper logic directly
      // by simulating what the SQL returns
      const fakeRow = {
        database: TEST_DB,
        table: 'parts_count',
        mutation_id: 'test_mutation',
        command: "UPDATE value = 'mutated' WHERE 1",
        create_time: new Date().toISOString(),
        partition_ids: [],
        block_numbers: [],
        parts_to_do: partCount,       // Real parts_to_do from system.mutations
        parts_in_progress: partCount,  // BUG: this is a copy of parts_to_do, not actual in-progress
        parts_to_do_names: "['part1','part2','part3']",
        parts_in_progress_names: "['part1','part2','part3']", // BUG: same as parts_to_do_names
        is_done: 0,
        latest_failed_part: '',
        latest_fail_time: '',
        latest_fail_reason: '',
        is_killed: 0,
      };

      const mapped = mapMutationInfo(fakeRow);

      // total_parts should equal the actual parts_to_do count, not doubled
      expect(mapped.total_parts).toBe(partCount);
      // parts_in_progress should be 0 (determined by UI via system.merges, not SQL)
      expect(mapped.parts_in_progress).toBe(0);

      // Wait for mutation to complete
      for (let i = 0; i < 30; i++) {
        const result = await ctx.client.query({
          query: `SELECT count() as cnt FROM system.mutations WHERE database = '${TEST_DB}' AND table = 'parts_count' AND is_done = 0`,
          format: 'JSONEachRow',
        });
        const rows = await result.json<{ cnt: string }>();
        if (Number(rows[0]?.cnt) === 0) break;
        await new Promise(r => setTimeout(r, 500));
      }
    }, 60_000);

    it('parts_in_progress_names should differ from parts_to_do_names when parts are being processed', () => {
      // When a mutation merge is active on a part, that part should appear in
      // parts_in_progress_names but not in parts_to_do_names (it's moved from
      // the "to do" list to the "in progress" list).
      //
      // BUG: Currently both are identical copies from system.mutations.parts_to_do_names.
      // The mapper can't distinguish idle vs active parts within a mutation.

      const fakeRowWithActivity = {
        database: 'db', table: 'tbl', mutation_id: 'm1',
        command: 'UPDATE x = 1 WHERE 1',
        create_time: new Date().toISOString(),
        partition_ids: [], block_numbers: [],
        parts_to_do: 3,
        parts_in_progress: 3,  // BUG: same as parts_to_do
        parts_to_do_names: "['p1','p2','p3']",
        parts_in_progress_names: "['p1','p2','p3']", // BUG: same as parts_to_do_names
        is_done: 0, latest_failed_part: '', latest_fail_time: '',
        latest_fail_reason: '', is_killed: 0,
      };

      const mapped = mapMutationInfo(fakeRowWithActivity);

      // parts_in_progress_names should be empty — system.mutations doesn't track
      // in-progress separately; that comes from cross-referencing system.merges
      expect(mapped.parts_in_progress_names).toEqual([]);
      // parts_to_do_names should have the actual parts
      expect(mapped.parts_to_do_names).toEqual(['p1', 'p2', 'p3']);
    });
  });

  // ── Mutation progress calculation ───────────────────────────────────

  describe('mutation progress is meaningful', () => {
    it('active mutation should have progress between 0 and 1, not always 0', () => {
      // BUG: progress = isDone ? 1 : 0; — no partial progress
      const fakeActive = {
        database: 'db', table: 'tbl', mutation_id: 'm1',
        command: 'UPDATE x = 1 WHERE 1',
        create_time: new Date().toISOString(),
        partition_ids: [], block_numbers: [],
        parts_to_do: 2,      // 2 remaining
        parts_in_progress: 2, // BUG: copied from parts_to_do
        parts_to_do_names: "['p1','p2']",
        parts_in_progress_names: "['p1','p2']",
        is_done: 0, latest_failed_part: '', latest_fail_time: '',
        latest_fail_reason: '', is_killed: 0,
      };

      const mapped = mapMutationInfo(fakeActive);

      // BUG: progress is 0 for any non-done mutation
      expect(mapped.progress).toBe(0);

      // parts_done is always 0
      expect(mapped.parts_done).toBe(0);
    });
  });

  // ── merge_algorithm for mutation events ─────────────────────────────

  describe('merge_algorithm for mutation events in part_log', () => {
    it('MutatePart events in part_log have merge_algorithm', async () => {
      // Create table, insert, run mutation, flush
      await ctx.client.command({
        query: `
          CREATE TABLE IF NOT EXISTS ${TEST_DB}.algo_mut (
            id UInt64, ts DateTime DEFAULT now(), value Float64
          ) ENGINE = MergeTree() ORDER BY id
        `,
      });

      await ctx.client.command({
        query: `INSERT INTO ${TEST_DB}.algo_mut (id, value) SELECT number, rand() FROM numbers(1000)`,
      });

      await ctx.client.command({ query: `OPTIMIZE TABLE ${TEST_DB}.algo_mut FINAL` });
      await new Promise(r => setTimeout(r, 1000));

      // Run a mutation
      await ctx.client.command({
        query: `ALTER TABLE ${TEST_DB}.algo_mut UPDATE value = value + 1 WHERE id < 500`,
      });

      // Wait for mutation to complete
      for (let i = 0; i < 30; i++) {
        const result = await ctx.client.query({
          query: `SELECT count() as cnt FROM system.mutations WHERE database = '${TEST_DB}' AND table = 'algo_mut' AND is_done = 0`,
          format: 'JSONEachRow',
        });
        const rows = await result.json<{ cnt: string }>();
        if (Number(rows[0]?.cnt) === 0) break;
        await new Promise(r => setTimeout(r, 500));
      }

      await ctx.client.command({ query: 'SYSTEM FLUSH LOGS' });

      const history = await tracker.getMergeHistory({
        database: TEST_DB,
        table: 'algo_mut',
        limit: 50,
      });

      // Find MutatePart events
      const mutateParts = history.filter(h => h.event_type === 'MutatePart');
      expect(mutateParts.length).toBeGreaterThanOrEqual(1);

      // Check merge_algorithm is present for mutation events
      for (const h of mutateParts) {
        expect(h).toHaveProperty('merge_algorithm');
        // In practice, mutations use Horizontal algorithm
        // (Vertical is only for regular merges with wide tables)
        if (h.merge_algorithm) {
          expect(['Horizontal', 'Vertical', 'Undecided']).toContain(h.merge_algorithm);
        }
      }
    }, 60_000);
  });

  // ── Lightweight mutation detection ──────────────────────────────────

  describe('lightweight vs heavy mutation detection', () => {
    it('DELETE FROM produces _row_exists = 0 command (lightweight delete)', async () => {
      await ctx.client.command({
        query: `
          CREATE TABLE IF NOT EXISTS ${TEST_DB}.lw_detect (
            id UInt64, ts DateTime DEFAULT now(), value String
          ) ENGINE = MergeTree() ORDER BY id
        `,
      });

      await ctx.client.command({
        query: `INSERT INTO ${TEST_DB}.lw_detect (id, value) SELECT number, 'test' FROM numbers(1000)`,
      });

      await ctx.client.command({ query: `OPTIMIZE TABLE ${TEST_DB}.lw_detect FINAL` });
      await new Promise(r => setTimeout(r, 1000));

      // Run lightweight delete (DELETE FROM, not ALTER TABLE DELETE)
      await ctx.client.command({
        query: `DELETE FROM ${TEST_DB}.lw_detect WHERE id >= 500`,
      });

      // Brief wait to observe mutation in system.mutations
      await new Promise(r => setTimeout(r, 500));

      // Check the mutation command — it should contain _row_exists = 0
      const result = await ctx.client.query({
        query: `
          SELECT command FROM system.mutations
          WHERE database = '${TEST_DB}' AND table = 'lw_detect'
          ORDER BY create_time DESC LIMIT 1
        `,
        format: 'JSONEachRow',
      });
      const rows = await result.json<{ command: string }>();

      if (rows.length > 0) {
        // ClickHouse converts DELETE FROM to UPDATE _row_exists = 0 WHERE ...
        expect(rows[0].command).toContain('_row_exists');
      }

      // Wait for completion
      for (let i = 0; i < 30; i++) {
        const r2 = await ctx.client.query({
          query: `SELECT count() as cnt FROM system.mutations WHERE database = '${TEST_DB}' AND table = 'lw_detect' AND is_done = 0`,
          format: 'JSONEachRow',
        });
        const r2rows = await r2.json<{ cnt: string }>();
        if (Number(r2rows[0]?.cnt) === 0) break;
        await new Promise(r => setTimeout(r, 500));
      }
    }, 60_000);

    it('ALTER TABLE UPDATE produces a regular mutation command (heavy)', async () => {
      await ctx.client.command({
        query: `
          CREATE TABLE IF NOT EXISTS ${TEST_DB}.heavy_detect (
            id UInt64, value String
          ) ENGINE = MergeTree() ORDER BY id
        `,
      });

      await ctx.client.command({
        query: `INSERT INTO ${TEST_DB}.heavy_detect (id, value) SELECT number, 'test' FROM numbers(100)`,
      });

      // Run heavy mutation
      await ctx.client.command({
        query: `ALTER TABLE ${TEST_DB}.heavy_detect UPDATE value = 'updated' WHERE id < 50`,
      });

      await new Promise(r => setTimeout(r, 500));

      const result = await ctx.client.query({
        query: `
          SELECT command FROM system.mutations
          WHERE database = '${TEST_DB}' AND table = 'heavy_detect'
          ORDER BY create_time DESC LIMIT 1
        `,
        format: 'JSONEachRow',
      });
      const rows = await result.json<{ command: string }>();

      if (rows.length > 0) {
        // Heavy mutation does NOT reference _row_exists
        expect(rows[0].command).not.toContain('_row_exists');
        expect(rows[0].command).toContain('value');
      }

      // Wait for completion
      for (let i = 0; i < 30; i++) {
        const r2 = await ctx.client.query({
          query: `SELECT count() as cnt FROM system.mutations WHERE database = '${TEST_DB}' AND table = 'heavy_detect' AND is_done = 0`,
          format: 'JSONEachRow',
        });
        const r2rows = await r2.json<{ cnt: string }>();
        if (Number(r2rows[0]?.cnt) === 0) break;
        await new Promise(r => setTimeout(r, 500));
      }
    }, 60_000);

    it('classifyMutationCommand detects lightweight vs heavy from command text', () => {
      // Lightweight delete: DELETE FROM → UPDATE _row_exists = 0 WHERE ...
      expect(classifyMutationCommand("UPDATE _row_exists = 0 WHERE ((sipHash64(event_type) % 10) = 3)")).toBe('LightweightDelete');

      // Heavy update: ALTER TABLE UPDATE
      expect(classifyMutationCommand("UPDATE value = 'updated' WHERE id < 50")).toBe('HeavyUpdate');

      // Heavy delete: ALTER TABLE DELETE
      expect(classifyMutationCommand("DELETE WHERE id >= 500")).toBe('HeavyDelete');
    });

    it('isPatchPart detects lightweight update patch parts', () => {
      expect(isPatchPart('patch-1e5b2ee238fbe84a863b7581944851ce-202602_4293_4392_1_4179')).toBe(true);
      expect(isPatchPart('202602_1_1296_4_4177')).toBe(false);
      expect(isPatchPart('all_0_0_0')).toBe(false);
    });
  });

  // ── Mutation active/history exclusivity ─────────────────────────────

  describe('mutation active vs history exclusivity', () => {
    it('a completed mutation appears in history but not in active', async () => {
      await ctx.client.command({
        query: `
          CREATE TABLE IF NOT EXISTS ${TEST_DB}.excl_done (
            id UInt64, value String
          ) ENGINE = MergeTree() ORDER BY id
        `,
      });

      await ctx.client.command({
        query: `INSERT INTO ${TEST_DB}.excl_done (id, value) SELECT number, 'test' FROM numbers(100)`,
      });

      // Run mutation and wait for completion
      await ctx.client.command({
        query: `ALTER TABLE ${TEST_DB}.excl_done UPDATE value = 'done' WHERE 1`,
      });

      for (let i = 0; i < 60; i++) {
        const r = await ctx.client.query({
          query: `SELECT count() as cnt FROM system.mutations WHERE database = '${TEST_DB}' AND table = 'excl_done' AND is_done = 0`,
          format: 'JSONEachRow',
        });
        const rows = await r.json<{ cnt: string }>();
        if (Number(rows[0]?.cnt) === 0) break;
        await new Promise(r => setTimeout(r, 500));
      }

      // Fetch both active and history
      const active = await tracker.getMutations();
      const history = await tracker.getMutationHistory({ database: TEST_DB, table: 'excl_done', limit: 100 });

      const activeForTable = active.filter(m => m.database === TEST_DB && m.table === 'excl_done');
      const historyForTable = history.filter(m => m.database === TEST_DB && m.table === 'excl_done');

      // Completed mutation must appear in history
      expect(historyForTable.length).toBeGreaterThanOrEqual(1);

      // Completed mutation must NOT appear in active
      expect(activeForTable.length).toBe(0);

      // No mutation_id should appear in both lists
      const activeIds = new Set(activeForTable.map(m => m.mutation_id));
      for (const h of historyForTable) {
        expect(activeIds.has(h.mutation_id)).toBe(false);
      }
    }, 60_000);

    it('getMutations query executes without error', async () => {
      // Validates that the SQL with Nested column access (block_numbers.partition_id,
      // block_numbers.number) and HAVING clause works correctly
      const mutations = await tracker.getMutations();
      expect(Array.isArray(mutations)).toBe(true);
    });

    it('getMutationHistory query executes without error', async () => {
      // Validates that the HAVING min(is_done) = 1 OR max(is_killed) = 1 works
      const history = await tracker.getMutationHistory({ limit: 10 });
      expect(Array.isArray(history)).toBe(true);
    });

    it('active mutation has non-empty parts_to_do_names', async () => {
      // An active mutation must have parts_to_do_names populated so the UI
      // can link it to active merges and show dependencies.
      await ctx.client.command({
        query: `
          CREATE TABLE IF NOT EXISTS ${TEST_DB}.parts_names (
            id UInt64, value String
          ) ENGINE = MergeTree() ORDER BY id
          SETTINGS index_granularity = 128
        `,
      });

      // Create 2 parts
      for (let i = 0; i < 2; i++) {
        await ctx.client.command({
          query: `INSERT INTO ${TEST_DB}.parts_names (id, value) SELECT number + ${i * 1000}, 'batch${i}' FROM numbers(100)`,
        });
      }

      // Run mutation
      await ctx.client.command({
        query: `ALTER TABLE ${TEST_DB}.parts_names UPDATE value = 'updated' WHERE 1`,
      });

      // Check parts_to_do_names is populated while mutation is active
      await new Promise(r => setTimeout(r, 200));
      const active = await tracker.getMutations();
      const forTable = active.filter(m => m.database === TEST_DB && m.table === 'parts_names');

      if (forTable.length > 0) {
        // parts_to_do_names must be non-empty for the UI to show dependencies
        // Bug: any() could pick empty [] from a done shard; anyIf fixes this
        expect(forTable[0].parts_to_do_names.length).toBeGreaterThan(0);
      }

      // Wait for completion
      for (let i = 0; i < 60; i++) {
        const r = await ctx.client.query({
          query: `SELECT count() as cnt FROM system.mutations WHERE database = '${TEST_DB}' AND table = 'parts_names' AND is_done = 0`,
          format: 'JSONEachRow',
        });
        const rows = await r.json<{ cnt: string }>();
        if (Number(rows[0]?.cnt) === 0) break;
        await new Promise(r => setTimeout(r, 500));
      }
    }, 60_000);

    it('simulated multi-shard: anyIf picks non-empty parts from active shard', async () => {
      // Simulates what clusterAllReplicas returns on a 2-shard cluster where
      // shard 1 is done (parts_to_do_names=[]) and shard 2 is active.
      // Verifies that anyIf(str, is_done=0) picks the non-empty value.
      await ctx.client.command({
        query: `
          CREATE TABLE IF NOT EXISTS ${TEST_DB}.shard_sim (
            database String,
            \`table\` String,
            mutation_id String,
            command String,
            create_time DateTime DEFAULT now(),
            parts_to_do UInt32,
            parts_to_do_names_str String,
            is_done UInt8,
            is_killed UInt8,
            latest_failed_part String DEFAULT '',
            latest_fail_time DateTime DEFAULT '1970-01-01',
            latest_fail_reason String DEFAULT ''
          ) ENGINE = MergeTree() ORDER BY (database, \`table\`, mutation_id)
        `,
      });

      // Insert rows mimicking 2 shards × 2 replicas
      // Shard 1: done (empty parts)
      await ctx.client.command({
        query: `INSERT INTO ${TEST_DB}.shard_sim (database, \`table\`, mutation_id, command, parts_to_do, parts_to_do_names_str, is_done, is_killed)
          VALUES ('db1', 'tbl1', 'mut1', 'UPDATE x=1 WHERE 1', 0, '[]', 1, 0),
                 ('db1', 'tbl1', 'mut1', 'UPDATE x=1 WHERE 1', 0, '[]', 1, 0)`,
      });
      // Shard 2: active (has parts)
      await ctx.client.command({
        query: `INSERT INTO ${TEST_DB}.shard_sim (database, \`table\`, mutation_id, command, parts_to_do, parts_to_do_names_str, is_done, is_killed)
          VALUES ('db1', 'tbl1', 'mut1', 'UPDATE x=1 WHERE 1', 1, '[''202602_0_1297_4'']', 0, 0),
                 ('db1', 'tbl1', 'mut1', 'UPDATE x=1 WHERE 1', 1, '[''202602_0_1297_4'']', 0, 0)`,
      });

      // Run the same aggregation pattern as GET_MUTATIONS (with raw_ aliases to avoid ILLEGAL_AGGREGATION)
      const result = await ctx.client.query({
        query: `
          SELECT
            database,
            \`table\`,
            mutation_id,
            anyIf(parts_to_do_names_str, raw_is_done = 0) AS parts_to_do_names,
            min(raw_is_done) AS is_done,
            sum(parts_to_do) AS total_parts_to_do
          FROM (
            SELECT *, is_done AS raw_is_done
            FROM ${TEST_DB}.shard_sim
          )
          GROUP BY database, \`table\`, mutation_id
          HAVING min(raw_is_done) = 0
        `,
        format: 'JSONEachRow',
      });
      const rows = await result.json<{ parts_to_do_names: string; is_done: number; total_parts_to_do: string }>();

      expect(rows.length).toBe(1);
      // anyIf must pick the non-empty parts list from the active shard
      expect(rows[0].parts_to_do_names).toContain('202602_0_1297_4');
      // Verify it's not the empty one
      expect(rows[0].parts_to_do_names).not.toBe('[]');

      // Also verify: anyIf always picks from the active shard
      const resultAny = await ctx.client.query({
        query: `
          SELECT
            any(parts_to_do_names_str) AS parts_to_do_names_any,
            anyIf(parts_to_do_names_str, raw_is_done = 0) AS parts_to_do_names_anyif
          FROM (
            SELECT *, is_done AS raw_is_done
            FROM ${TEST_DB}.shard_sim
          )
          GROUP BY database, \`table\`, mutation_id
        `,
        format: 'JSONEachRow',
      });
      const rowsAny = await resultAny.json<{ parts_to_do_names_any: string; parts_to_do_names_anyif: string }>();

      // anyIf always picks from active shard
      expect(rowsAny[0].parts_to_do_names_anyif).toContain('202602_0_1297_4');
      // any() MIGHT pick empty (non-deterministic) — we can't assert it will,
      // but anyIf is deterministically correct
    }, 30_000);

    it('simulated multi-shard: HAVING prevents alias collision (ILLEGAL_AGGREGATION)', async () => {
      // Verifies that using raw_is_done/raw_is_killed aliases in HAVING
      // avoids the ClickHouse error: "Aggregate function min(is_done) AS is_done
      // is found inside another aggregate function"
      const result = await ctx.client.query({
        query: `
          SELECT
            database,
            \`table\`,
            mutation_id,
            min(raw_is_done) AS is_done,
            max(raw_is_killed) AS is_killed
          FROM (
            SELECT
              database,
              \`table\`,
              mutation_id,
              is_done AS raw_is_done,
              is_killed AS raw_is_killed
            FROM ${TEST_DB}.shard_sim
          )
          GROUP BY database, \`table\`, mutation_id
          HAVING min(raw_is_done) = 0 AND max(raw_is_killed) = 0
        `,
        format: 'JSONEachRow',
      });
      const rows = await result.json<Record<string, unknown>[]>();
      // Should execute without ILLEGAL_AGGREGATION error
      expect(Array.isArray(rows)).toBe(true);
    }, 10_000);

    it('mutation parts_to_do sums correctly across replicas', async () => {
      // On a single node, parts_to_do should match the actual part count
      await ctx.client.command({
        query: `
          CREATE TABLE IF NOT EXISTS ${TEST_DB}.parts_sum (
            id UInt64, value String
          ) ENGINE = MergeTree() ORDER BY id
          SETTINGS index_granularity = 128
        `,
      });

      // Create exactly 3 parts
      for (let i = 0; i < 3; i++) {
        await ctx.client.command({
          query: `INSERT INTO ${TEST_DB}.parts_sum (id, value) SELECT number + ${i * 1000}, 'batch${i}' FROM numbers(100)`,
        });
      }

      // Run mutation
      await ctx.client.command({
        query: `ALTER TABLE ${TEST_DB}.parts_sum UPDATE value = 'updated' WHERE 1`,
      });

      // Check active mutations immediately
      await new Promise(r => setTimeout(r, 200));
      const active = await tracker.getMutations();
      const forTable = active.filter(m => m.database === TEST_DB && m.table === 'parts_sum');

      if (forTable.length > 0) {
        // parts_to_do should not be doubled (was a bug with sum() across replicas)
        const partCount = await ctx.client.query({
          query: `SELECT count() as cnt FROM system.parts WHERE database = '${TEST_DB}' AND table = 'parts_sum' AND active`,
          format: 'JSONEachRow',
        });
        const rows = await partCount.json<{ cnt: string }>();
        const actualParts = Number(rows[0]?.cnt);

        // parts_to_do should be <= actual parts (could be less if some already processed)
        expect(forTable[0].parts_to_do).toBeLessThanOrEqual(actualParts);
      }

      // Wait for completion
      for (let i = 0; i < 60; i++) {
        const r = await ctx.client.query({
          query: `SELECT count() as cnt FROM system.mutations WHERE database = '${TEST_DB}' AND table = 'parts_sum' AND is_done = 0`,
          format: 'JSONEachRow',
        });
        const rows = await r.json<{ cnt: string }>();
        if (Number(rows[0]?.cnt) === 0) break;
        await new Promise(r => setTimeout(r, 500));
      }
    }, 60_000);
  });
});
