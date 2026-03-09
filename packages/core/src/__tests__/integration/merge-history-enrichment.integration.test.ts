/**
 * Integration tests for merge history enrichment features.
 *
 * Validates the full pipeline from SQL queries through the service layer:
 * - merge_algorithm (Vertical / Horizontal) in merge history
 * - disk_name / path_on_disk fields in merge history
 * - Storage policy volume resolution (getStoragePolicyVolumes)
 * - TTL merge classification (TTLDelete, TTLMove via classifyMergeHistory)
 * - merge_type surfacing in active merges
 * - merge_reason classification through the service layer
 * - rows_diff: net row change per merge/mutation event
 * - Lightweight delete lifecycle (DELETE FROM → MutatePart → LightweightDelete merge)
 *
 * ┌─────────────────────┬──────────────────┬──────────────────┬──────────┬──────────┬──────────────────────────────────────────────┐
 * │ Category            │ CH event_type    │ CH merge_reason  │ rows_diff│ size_diff│ What happened                                │
 * ├─────────────────────┼──────────────────┼──────────────────┼──────────┼──────────┼──────────────────────────────────────────────┤
 * │ Regular             │ MergeParts       │ RegularMerge     │    0     │  ≤ 0     │ Parts combined, no rows lost                 │
 * │ TTLDelete           │ MergeParts       │ TTLDeleteMerge   │  < 0     │  < 0     │ Expired rows removed during merge            │
 * │ TTLRecompress       │ MergeParts       │ TTLRecompressMerge│   0     │  varies  │ Codec changed on aged data, rows preserved   │
 * │ TTLMove             │ MovePart         │ (n/a)            │    0     │    0     │ Part relocated to different volume/disk       │
 * │ Mutation            │ MutatePart       │ NotAMerge        │ ≤ 0      │  varies  │ Part rewrite (see notes on sources + chaining)│
 * │ LightweightDelete   │ MergeParts       │ RegularMerge     │  < 0     │  < 0     │ Regular merge cleaned up DELETE FROM masks   │
 * └─────────────────────┴──────────────────┴──────────────────┴──────────┴──────────┴──────────────────────────────────────────────┘
 *
 * Notes:
 * - rows_diff = rows (output) − read_rows (input). Negative means rows were removed.
 * - size_diff = bytes_uncompressed (output) − read_bytes (input). Negative means data shrank.
 * - TTLDelete merges are triggered by `TTL <expr> DELETE` rules. ClickHouse schedules
 *   them via TTLMergeSelector; the merge itself drops expired rows while combining parts.
 * - TTLMove is NOT a merge — it's a file relocation. No data is rewritten.
 * - TTLRecompress changes the compression codec but preserves all rows.
 * - Regular merges never remove rows — they only combine parts.
 *
 * Mutation sources (all appear as MutatePart / NotAMerge in part_log):
 *   1. ALTER TABLE DELETE WHERE ...  — heavy delete, rewrites parts, rows_diff < 0
 *   2. ALTER TABLE UPDATE SET ...    — heavy update, rewrites parts, rows_diff = 0
 *   3. DELETE FROM table WHERE ...   — lightweight delete. ClickHouse internally
 *      converts this to UPDATE _row_exists = 0 WHERE ..., which goes through the
 *      mutation pipeline and produces MutatePart events. The rows are masked but
 *      not physically removed. Chained mutations on the same part may show rows = 0
 *      and read_rows = 0 (mask-only rewrite — only the _row_exists column is
 *      updated, no actual data rows are read or written). The actual row removal
 *      happens during a subsequent regular merge → classified as LightweightDelete
 *      (rows_diff < 0).
 *
 * LightweightDelete lifecycle (two phases):
 *   Phase 1 — Mask: DELETE FROM → MutatePart events (Mutation badge in UI).
 *             Each part is rewritten to add/update the _row_exists column.
 *   Phase 2 — Cleanup: A regular background merge combines parts and drops
 *             masked rows → MergeParts + RegularMerge with rows_diff < 0
 *             → classified as LightweightDelete.
 *   This behavior is a ClickHouse implementation detail that may change in
 *   future versions. These tests pin the current behavior so we detect changes.
 *
 * Gaps / not yet covered:
 * - ALTER UPDATE mutations (rows_diff = 0 but data changes) — not tested yet
 * - Lightweight UPDATE (UPDATE table SET ... WHERE ...) — requires
 *   enable_block_number_column=1 and enable_block_offset_column=1 on the table.
 *   As of CH 26.1 this is beta and fails with internal column resolution errors
 *   on consecutive updates before patch materialization. When it works:
 *     Phase 1: Creates many small patch-<hash>-* parts (one per block)
 *     Phase 2: Regular merge materializes patches (MergeParts, rows_diff=0)
 *     Phase 3: Old patch parts removed (RemovePart, not shown in our UI)
 *   Currently shows as Regular merge — no special classification yet.
 *   Known issue: failed lightweight updates can leave orphaned patch parts
 *   whose cleanup merge shows rows_diff < 0 → false LightweightDelete badge.
 * - TTLRecompress end-to-end — classification is tested but no real CH table test
 *   (requires multi-codec TTL rule which is complex to set up in containers)
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { startClickHouse, stopClickHouse, type TestClickHouseContext } from './setup/clickhouse-container.js';
import { MergeTracker } from '../../services/merge-tracker.js';
import { classifyMergeHistory, classifyActiveMerge, refineCategoryWithRowDiff, ALL_MERGE_CATEGORIES, MERGE_CATEGORIES } from '../../utils/merge-classification.js';
import type { MergeCategory } from '../../utils/merge-classification.js';

const CONTAINER_TIMEOUT = 120_000;
const TEST_DB = 'merge_enrich_test';

describe('merge history enrichment integration', () => {
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

  // ── merge_algorithm in merge history ──────────────────────────────

  describe('merge_algorithm in merge history', () => {
    it('system.part_log has merge_algorithm column', async () => {
      // Ensure part_log exists by flushing logs first (table is lazy-created)
      await ctx.client.command({ query: 'SYSTEM FLUSH LOGS' });
      const rows = await ctx.rawAdapter.executeQuery<{ merge_algorithm: string }>(
        'SELECT merge_algorithm FROM system.part_log LIMIT 0'
      );
      expect(Array.isArray(rows)).toBe(true);
    });

    it('merge history records include merge_algorithm field', async () => {
      // Create table, insert data, trigger merge, flush logs
      await ctx.client.command({
        query: `
          CREATE TABLE IF NOT EXISTS ${TEST_DB}.algo_test (
            id UInt64, ts DateTime DEFAULT now(), value Float64
          ) ENGINE = MergeTree() ORDER BY (ts, id)
        `,
      });

      for (let i = 0; i < 5; i++) {
        await ctx.client.command({
          query: `INSERT INTO ${TEST_DB}.algo_test (id, value) SELECT number + ${i * 500}, rand() FROM numbers(500)`,
        });
      }

      await ctx.client.command({ query: `OPTIMIZE TABLE ${TEST_DB}.algo_test FINAL` });
      await new Promise(r => setTimeout(r, 2000));
      await ctx.client.command({ query: 'SYSTEM FLUSH LOGS' });

      const history = await tracker.getMergeHistory({
        database: TEST_DB,
        table: 'algo_test',
        limit: 50,
      });

      expect(history.length).toBeGreaterThanOrEqual(1);

      for (const h of history) {
        expect(h).toHaveProperty('merge_algorithm');
        // merge_algorithm is optional (undefined for MovePart events)
        if (h.merge_algorithm) {
          expect(typeof h.merge_algorithm).toBe('string');
          expect(['Horizontal', 'Vertical', 'Undecided']).toContain(h.merge_algorithm);
        }
      }
    });

    it('MergeParts events have a non-empty merge_algorithm', async () => {
      await ctx.client.command({ query: 'SYSTEM FLUSH LOGS' });

      const history = await tracker.getMergeHistory({
        database: TEST_DB,
        table: 'algo_test',
        limit: 50,
      });

      // Filter to actual merge events (not mutations or moves)
      const mergeEvents = history.filter(h => h.event_type === 'MergeParts');
      if (mergeEvents.length > 0) {
        for (const h of mergeEvents) {
          expect(h.merge_algorithm).toBeDefined();
          expect(h.merge_algorithm!.length).toBeGreaterThan(0);
        }
      }
    });
  });

  // ── merge_algorithm in active merges ──────────────────────────────

  describe('merge_algorithm in active merges', () => {
    it('system.merges has merge_algorithm column', async () => {
      const rows = await ctx.rawAdapter.executeQuery<{ merge_algorithm: string }>(
        'SELECT merge_algorithm FROM system.merges LIMIT 0'
      );
      expect(Array.isArray(rows)).toBe(true);
    });

    it('active merges include merge_algorithm in MergeInfo shape', async () => {
      const merges = await tracker.getActiveMerges();
      expect(Array.isArray(merges)).toBe(true);
      for (const m of merges) {
        expect(m).toHaveProperty('merge_algorithm');
        expect(typeof m.merge_algorithm).toBe('string');
      }
    });
  });

  // ── disk_name and path_on_disk ────────────────────────────────────

  describe('disk_name and path_on_disk in merge history', () => {
    it('merge history records include disk_name', async () => {
      const history = await tracker.getMergeHistory({ database: TEST_DB, limit: 50 });
      expect(history.length).toBeGreaterThanOrEqual(1);
      for (const h of history) {
        expect(h).toHaveProperty('disk_name');
        if (h.disk_name) {
          expect(typeof h.disk_name).toBe('string');
        }
      }
    });

    it('merge history records include path_on_disk', async () => {
      const history = await tracker.getMergeHistory({ database: TEST_DB, limit: 50 });
      for (const h of history) {
        expect(h).toHaveProperty('path_on_disk');
        if (h.path_on_disk) {
          expect(typeof h.path_on_disk).toBe('string');
          // Path should look like a filesystem path
          expect(h.path_on_disk).toMatch(/^\//);
        }
      }
    });

    it('regular merges on default disk report disk_name as "default"', async () => {
      const history = await tracker.getMergeHistory({
        database: TEST_DB,
        table: 'algo_test',
        limit: 50,
      });

      const withDisk = history.filter(h => h.disk_name);
      if (withDisk.length > 0) {
        // Single-disk container should use "default"
        expect(withDisk.some(h => h.disk_name === 'default')).toBe(true);
      }
    });
  });

  // ── Storage policy volumes ────────────────────────────────────────

  describe('getStoragePolicyVolumes', () => {
    it('returns volumes with expected shape', async () => {
      const volumes = await tracker.getStoragePolicyVolumes();
      expect(Array.isArray(volumes)).toBe(true);
      expect(volumes.length).toBeGreaterThanOrEqual(1);

      for (const v of volumes) {
        expect(v).toHaveProperty('policyName');
        expect(v).toHaveProperty('volumeName');
        expect(v).toHaveProperty('disks');
        expect(typeof v.policyName).toBe('string');
        expect(typeof v.volumeName).toBe('string');
        expect(Array.isArray(v.disks)).toBe(true);
        expect(v.disks.length).toBeGreaterThanOrEqual(1);
      }
    });

    it('default policy contains default disk', async () => {
      const volumes = await tracker.getStoragePolicyVolumes();
      const defaultVol = volumes.find(v => v.policyName === 'default');
      expect(defaultVol).toBeDefined();
      expect(defaultVol!.disks).toContain('default');
    });

    it('can resolve disk_name from merge history to a volume', async () => {
      const [volumes, history] = await Promise.all([
        tracker.getStoragePolicyVolumes(),
        tracker.getMergeHistory({ database: TEST_DB, limit: 50 }),
      ]);

      const withDisk = history.filter(h => h.disk_name);
      if (withDisk.length > 0) {
        const diskName = withDisk[0].disk_name!;
        const matchingVol = volumes.find(v => v.disks.includes(diskName));
        expect(matchingVol).toBeDefined();
        expect(matchingVol!.policyName.length).toBeGreaterThan(0);
        expect(matchingVol!.volumeName.length).toBeGreaterThan(0);
      }
    });
  });

  // ── TTL merge classification ──────────────────────────────────────

  describe('TTL merge classification through service layer', () => {
    it('TTL DELETE table produces TTLDelete-classified history', async () => {
      await ctx.client.command({
        query: `
          CREATE TABLE IF NOT EXISTS ${TEST_DB}.ttl_del (
            id UInt64, ts DateTime DEFAULT now(), value Float64
          ) ENGINE = MergeTree()
          ORDER BY (ts, id)
          TTL ts + INTERVAL 1 SECOND DELETE
          SETTINGS merge_with_ttl_timeout = 1, old_parts_lifetime = 1
        `,
      });

      // Insert with already-expired timestamps
      await ctx.client.command({
        query: `
          INSERT INTO ${TEST_DB}.ttl_del (id, ts, value)
          SELECT number, now() - toIntervalSecond(10), rand()
          FROM numbers(200)
        `,
      });

      await new Promise(r => setTimeout(r, 5000));
      await ctx.client.command({ query: `OPTIMIZE TABLE ${TEST_DB}.ttl_del FINAL` });
      await new Promise(r => setTimeout(r, 2000));
      await ctx.client.command({ query: 'SYSTEM FLUSH LOGS' });

      const history = await tracker.getMergeHistory({
        database: TEST_DB,
        table: 'ttl_del',
        limit: 50,
      });

      expect(history.length).toBeGreaterThanOrEqual(1);

      // At least one event should be classified as TTLDelete
      const reasons = history.map(h => h.merge_reason);
      expect(reasons).toContain('TTLDelete');
    });

    it('regular merges are classified as Regular', async () => {
      const history = await tracker.getMergeHistory({
        database: TEST_DB,
        table: 'algo_test',
        limit: 50,
      });

      const mergeEvents = history.filter(h => h.event_type === 'MergeParts');
      if (mergeEvents.length > 0) {
        // algo_test has no TTL, so merges should be Regular
        expect(mergeEvents.some(h => h.merge_reason === 'Regular')).toBe(true);
      }
    });
  });

  // ── classifyMergeHistory unit checks (run inside integration suite) ─

  describe('classifyMergeHistory classification logic', () => {
    it('MovePart → TTLMove regardless of merge_reason', () => {
      expect(classifyMergeHistory('MovePart', '')).toBe('TTLMove');
      expect(classifyMergeHistory('MovePart', 'RegularMerge')).toBe('TTLMove');
      expect(classifyMergeHistory('MovePart', 'TTLDeleteMerge')).toBe('TTLMove');
    });

    it('MutatePart → Mutation', () => {
      expect(classifyMergeHistory('MutatePart', '')).toBe('Mutation');
      expect(classifyMergeHistory('MutatePart', 'NotAMerge')).toBe('Mutation');
    });

    it('MergeParts with TTL reasons → TTLDelete or TTLRecompress', () => {
      expect(classifyMergeHistory('MergeParts', 'TTLDeleteMerge')).toBe('TTLDelete');
      expect(classifyMergeHistory('MergeParts', 'TTLDropMerge')).toBe('TTLDelete');
      expect(classifyMergeHistory('MergeParts', 'TTLRecompressMerge')).toBe('TTLRecompress');
      expect(classifyMergeHistory('MergeParts', 'TTLMerge')).toBe('TTLDelete');
    });

    it('MergeParts with RegularMerge → Regular', () => {
      expect(classifyMergeHistory('MergeParts', 'RegularMerge')).toBe('Regular');
      expect(classifyMergeHistory('MergeParts', '')).toBe('Regular');
    });

    it('MergeParts with NotAMerge → Regular (failed fetch, not a mutation)', () => {
      // When a MergeParts operation fails (e.g. NO_REPLICA_HAS_PART), ClickHouse
      // sets merge_reason = 'NotAMerge' because no merge actually happened.
      // This must NOT be classified as Mutation — only MutatePart event_type means mutation.
      expect(classifyMergeHistory('MergeParts', 'NotAMerge')).toBe('Regular');
    });

    it('unknown merge_reason falls back to Regular', () => {
      expect(classifyMergeHistory('MergeParts', 'SomeFutureReason')).toBe('Regular');
    });
  });

  describe('refineCategoryWithRowDiff — lightweight delete inference', () => {
    it('Regular + rows_diff < 0 → LightweightDelete', () => {
      expect(refineCategoryWithRowDiff('Regular', -100)).toBe('LightweightDelete');
      expect(refineCategoryWithRowDiff('Regular', -1)).toBe('LightweightDelete');
    });

    it('Regular + rows_diff >= 0 stays Regular', () => {
      expect(refineCategoryWithRowDiff('Regular', 0)).toBe('Regular');
      expect(refineCategoryWithRowDiff('Regular', 100)).toBe('Regular');
    });

    it('TTLDelete + rows_diff < 0 stays TTLDelete (not reclassified)', () => {
      expect(refineCategoryWithRowDiff('TTLDelete', -500)).toBe('TTLDelete');
    });

    it('Mutation + rows_diff < 0 stays Mutation', () => {
      expect(refineCategoryWithRowDiff('Mutation', -200)).toBe('Mutation');
    });

    it('TTLMove is never reclassified', () => {
      expect(refineCategoryWithRowDiff('TTLMove', 0)).toBe('TTLMove');
    });
  });

  describe('classifyActiveMerge classification logic', () => {
    it('mutation flag takes precedence', () => {
      expect(classifyActiveMerge('Regular', true)).toBe('Mutation');
      expect(classifyActiveMerge('TTLDelete', true)).toBe('Mutation');
    });

    it('maps known merge_type values', () => {
      expect(classifyActiveMerge('Regular', false)).toBe('Regular');
      expect(classifyActiveMerge('TTLDelete', false)).toBe('TTLDelete');
      expect(classifyActiveMerge('TTLRecompress', false)).toBe('TTLRecompress');
    });

    it('unknown merge_type falls back to Regular', () => {
      expect(classifyActiveMerge('SomeFutureType', false)).toBe('Regular');
    });
  });

  // ── MERGE_CATEGORIES metadata ─────────────────────────────────────

  describe('MERGE_CATEGORIES metadata', () => {
    it('every category in ALL_MERGE_CATEGORIES has metadata', () => {
      for (const cat of ALL_MERGE_CATEGORIES) {
        const info = MERGE_CATEGORIES[cat as MergeCategory];
        expect(info).toBeDefined();
        expect(info.label.length).toBeGreaterThan(0);
        expect(info.color).toMatch(/^#/);
        expect(info.description.length).toBeGreaterThan(0);
      }
    });
  });

  // ── getMergeHistory query variants ────────────────────────────────

  describe('getMergeHistory query routing', () => {
    it('database+table filter returns only matching rows', async () => {
      const history = await tracker.getMergeHistory({
        database: TEST_DB,
        table: 'algo_test',
        limit: 50,
      });
      for (const h of history) {
        expect(h.database).toBe(TEST_DB);
        expect(h.table).toBe('algo_test');
      }
    });

    it('database-only filter returns only matching database', async () => {
      const history = await tracker.getMergeHistory({
        database: TEST_DB,
        limit: 50,
      });
      for (const h of history) {
        expect(h.database).toBe(TEST_DB);
      }
    });

    it('no filter returns results from any database', async () => {
      const history = await tracker.getMergeHistory({ limit: 10 });
      expect(Array.isArray(history)).toBe(true);
      // Should have at least our test data
      expect(history.length).toBeGreaterThanOrEqual(1);
    });

    it('all query variants include merge_algorithm', async () => {
      // Test all 3 query paths return merge_algorithm
      const [byTable, byDb, all] = await Promise.all([
        tracker.getMergeHistory({ database: TEST_DB, table: 'algo_test', limit: 5 }),
        tracker.getMergeHistory({ database: TEST_DB, limit: 5 }),
        tracker.getMergeHistory({ limit: 5 }),
      ]);

      for (const history of [byTable, byDb, all]) {
        for (const h of history) {
          expect(h).toHaveProperty('merge_algorithm');
          expect(h).toHaveProperty('disk_name');
          expect(h).toHaveProperty('path_on_disk');
        }
      }
    });
  });

  // ── rows_diff: net row change per merge/mutation ──────────────────

  describe('rows_diff — net row change tracking', () => {
    it('merge history records include read_rows and rows_diff fields', async () => {
      const history = await tracker.getMergeHistory({
        database: TEST_DB,
        table: 'algo_test',
        limit: 50,
      });

      expect(history.length).toBeGreaterThanOrEqual(1);
      for (const h of history) {
        expect(h).toHaveProperty('read_rows');
        expect(h).toHaveProperty('rows_diff');
        expect(typeof h.read_rows).toBe('number');
        expect(typeof h.rows_diff).toBe('number');
      }
    });

    it('regular merges have rows_diff === 0 (no rows removed)', async () => {
      const history = await tracker.getMergeHistory({
        database: TEST_DB,
        table: 'algo_test',
        limit: 50,
      });

      const regularMerges = history.filter(h => h.merge_reason === 'Regular');
      for (const h of regularMerges) {
        // Regular merges combine parts without dropping rows
        expect(h.rows_diff).toBe(0);
        expect(h.read_rows).toBeGreaterThan(0);
        expect(h.rows).toBe(h.read_rows);
      }
    });

    it('TTL DELETE merge shows negative rows_diff', async () => {
      // ttl_del table was created earlier with TTL ts + INTERVAL 1 SECOND DELETE
      // and populated with already-expired rows
      const history = await tracker.getMergeHistory({
        database: TEST_DB,
        table: 'ttl_del',
        limit: 50,
      });

      const ttlMerges = history.filter(h => h.merge_reason === 'TTLDelete');
      expect(ttlMerges.length).toBeGreaterThanOrEqual(1);

      // At least one TTL delete merge should have removed rows
      const withRowDrop = ttlMerges.filter(h => h.rows_diff < 0);
      expect(withRowDrop.length).toBeGreaterThanOrEqual(1);

      for (const h of withRowDrop) {
        expect(h.read_rows).toBeGreaterThan(h.rows);
        expect(h.rows_diff).toBe(h.rows - h.read_rows);
      }
    });

    it('ALTER TABLE DELETE mutation shows negative rows_diff in part_log', async () => {
      // Create a table, insert data, then delete some rows via mutation
      await ctx.client.command({
        query: `
          CREATE TABLE IF NOT EXISTS ${TEST_DB}.mut_del (
            id UInt64, ts DateTime DEFAULT now(), value Float64
          ) ENGINE = MergeTree() ORDER BY id
        `,
      });

      await ctx.client.command({
        query: `INSERT INTO ${TEST_DB}.mut_del (id, value) SELECT number, rand() FROM numbers(1000)`,
      });

      // Force merge so we have a single part
      await ctx.client.command({ query: `OPTIMIZE TABLE ${TEST_DB}.mut_del FINAL` });
      await new Promise(r => setTimeout(r, 1000));

      // Delete ~half the rows via mutation
      await ctx.client.command({
        query: `ALTER TABLE ${TEST_DB}.mut_del DELETE WHERE id >= 500`,
      });

      // Wait for mutation to complete
      for (let i = 0; i < 20; i++) {
        const result = await ctx.client.query({
          query: `SELECT is_done FROM system.mutations WHERE database = '${TEST_DB}' AND table = 'mut_del' AND is_done = 0`,
          format: 'JSONEachRow',
        });
        const rows = await result.json<{ is_done: number }>();
        if (rows.length === 0) break;
        await new Promise(r => setTimeout(r, 500));
      }

      await ctx.client.command({ query: 'SYSTEM FLUSH LOGS' });

      const history = await tracker.getMergeHistory({
        database: TEST_DB,
        table: 'mut_del',
        limit: 50,
      });

      // Find the MutatePart event
      const mutations = history.filter(h => h.merge_reason === 'Mutation');
      expect(mutations.length).toBeGreaterThanOrEqual(1);

      // The mutation should show rows were removed
      const withRowDrop = mutations.filter(h => h.rows_diff < 0);
      expect(withRowDrop.length).toBeGreaterThanOrEqual(1);

      for (const h of withRowDrop) {
        expect(h.read_rows).toBeGreaterThan(0);
        expect(h.rows).toBeLessThan(h.read_rows);
      }
    });

    it('all query variants include read_rows and rows_diff', async () => {
      const [byTable, byDb, all] = await Promise.all([
        tracker.getMergeHistory({ database: TEST_DB, table: 'algo_test', limit: 5 }),
        tracker.getMergeHistory({ database: TEST_DB, limit: 5 }),
        tracker.getMergeHistory({ limit: 5 }),
      ]);

      for (const history of [byTable, byDb, all]) {
        for (const h of history) {
          expect(h).toHaveProperty('read_rows');
          expect(h).toHaveProperty('rows_diff');
          expect(typeof h.read_rows).toBe('number');
          expect(typeof h.rows_diff).toBe('number');
        }
      }
    });
  });

  // ── Lightweight delete lifecycle (DELETE FROM → MutatePart → LightweightDelete merge) ──

  describe('lightweight delete lifecycle', () => {
    /**
     * Pins the two-phase behaviour of DELETE FROM in ClickHouse:
     *
     * Phase 1 — Mask:
     *   DELETE FROM internally becomes UPDATE _row_exists = 0 WHERE ...
     *   This goes through the mutation pipeline and produces MutatePart
     *   events in part_log. Rows are masked, not physically removed.
     *
     * Phase 2 — Cleanup:
     *   A subsequent regular merge combines parts and drops masked rows.
     *   This appears as MergeParts + RegularMerge with rows_diff < 0,
     *   which our pipeline classifies as LightweightDelete.
     *
     * This is a ClickHouse implementation detail that may change.
     * The test pins current behaviour so we detect regressions.
     */

    const LW_TABLE = 'lw_del_lifecycle';

    it('Phase 1: DELETE FROM produces MutatePart events (mask setting)', async () => {
      // Create table and insert data
      await ctx.client.command({
        query: `
          CREATE TABLE IF NOT EXISTS ${TEST_DB}.${LW_TABLE} (
            id UInt64, ts DateTime DEFAULT now(), value Float64
          ) ENGINE = MergeTree() ORDER BY id
        `,
      });

      await ctx.client.command({
        query: `INSERT INTO ${TEST_DB}.${LW_TABLE} (id, value) SELECT number, rand() FROM numbers(2000)`,
      });

      // Force merge to a single part so the DELETE affects exactly one part
      await ctx.client.command({ query: `OPTIMIZE TABLE ${TEST_DB}.${LW_TABLE} FINAL` });
      await new Promise(r => setTimeout(r, 1000));

      // Run lightweight delete — DELETE FROM (not ALTER TABLE DELETE)
      await ctx.client.command({
        query: `DELETE FROM ${TEST_DB}.${LW_TABLE} WHERE id >= 1000`,
      });

      // Wait for mutation to complete
      for (let i = 0; i < 30; i++) {
        const result = await ctx.client.query({
          query: `SELECT count() as cnt FROM system.mutations WHERE database = '${TEST_DB}' AND table = '${LW_TABLE}' AND is_done = 0`,
          format: 'JSONEachRow',
        });
        const rows = await result.json<{ cnt: string }>();
        if (Number(rows[0]?.cnt) === 0) break;
        await new Promise(r => setTimeout(r, 500));
      }

      await ctx.client.command({ query: 'SYSTEM FLUSH LOGS' });

      const history = await tracker.getMergeHistory({
        database: TEST_DB,
        table: LW_TABLE,
        limit: 100,
      });

      // Phase 1: should see MutatePart events (classified as Mutation)
      const mutateParts = history.filter(h => h.event_type === 'MutatePart');
      expect(mutateParts.length).toBeGreaterThanOrEqual(1);
      for (const h of mutateParts) {
        expect(h.merge_reason).toBe('Mutation');
      }
    }, 60_000);

    it('Phase 2: subsequent merge cleans up masked rows → LightweightDelete', async () => {
      // Insert a second batch so there are multiple parts to merge
      // (OPTIMIZE FINAL on a single part may be a no-op)
      await ctx.client.command({
        query: `INSERT INTO ${TEST_DB}.${LW_TABLE} (id, value) SELECT number + 10000, rand() FROM numbers(500)`,
      });

      // Force merge to trigger cleanup of masked rows
      await ctx.client.command({ query: `OPTIMIZE TABLE ${TEST_DB}.${LW_TABLE} FINAL` });
      await new Promise(r => setTimeout(r, 3000));
      await ctx.client.command({ query: 'SYSTEM FLUSH LOGS' });

      const history = await tracker.getMergeHistory({
        database: TEST_DB,
        table: LW_TABLE,
        limit: 100,
      });

      // Phase 2: should see a MergeParts event with rows_diff < 0
      // classified as LightweightDelete by our pipeline
      const lwDeletes = history.filter(h => h.merge_reason === 'LightweightDelete');
      expect(lwDeletes.length).toBeGreaterThanOrEqual(1);

      for (const h of lwDeletes) {
        // It's a regular merge under the hood
        expect(h.event_type).toBe('MergeParts');
        // Rows were removed during merge
        expect(h.rows_diff).toBeLessThan(0);
        expect(h.read_rows).toBeGreaterThan(h.rows);
      }
    }, 60_000);

    it('full lifecycle: row count reflects deletion after cleanup merge', async () => {
      // After both phases, the table should have fewer rows than originally inserted
      // Original: 2000 rows, deleted id >= 1000 (1000 rows), added 500 more = 1500 expected
      const result = await ctx.client.query({
        query: `SELECT count() as cnt FROM ${TEST_DB}.${LW_TABLE}`,
        format: 'JSONEachRow',
      });
      const rows = await result.json<{ cnt: string }>();
      const count = Number(rows[0]?.cnt);

      // We inserted 2000, deleted 1000 (id >= 1000), then inserted 500 more
      // Expected: 1000 + 500 = 1500
      expect(count).toBe(1500);
    });
  });
});
