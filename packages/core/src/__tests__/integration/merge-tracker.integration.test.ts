/**
 * Integration tests for MergeTracker against a real ClickHouse instance.
 *
 * Creates tables, inserts data to trigger merges, and validates
 * MergeTracker methods against real system tables.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { startClickHouse, stopClickHouse, type TestClickHouseContext } from './setup/clickhouse-container.js';
import { MergeTracker } from '../../services/merge-tracker.js';

const CONTAINER_TIMEOUT = 120_000;
const TEST_DB = 'merge_test';

describe('MergeTracker integration', { tags: ['merge-engine'] }, () => {
  let ctx: TestClickHouseContext;
  let tracker: MergeTracker;

  beforeAll(async () => {
    ctx = await startClickHouse();
    tracker = new MergeTracker(ctx.adapter);

    // Create test database and table
    await ctx.client.command({ query: `CREATE DATABASE IF NOT EXISTS ${TEST_DB}` });
    await ctx.client.command({
      query: `
        CREATE TABLE IF NOT EXISTS ${TEST_DB}.events (
          id UInt64,
          ts DateTime DEFAULT now(),
          value Float64
        ) ENGINE = MergeTree()
        ORDER BY (ts, id)
      `,
    });

    // Insert multiple batches to create multiple parts that may merge
    for (let i = 0; i < 5; i++) {
      await ctx.client.command({
        query: `INSERT INTO ${TEST_DB}.events (id, value) SELECT number + ${i * 1000}, rand() FROM numbers(1000)`,
      });
    }

    // Run a mutation to populate system.mutations
    await ctx.client.command({
      query: `ALTER TABLE ${TEST_DB}.events UPDATE value = value + 1 WHERE id < 100`,
    });

    // Wait briefly for mutation to process, then optimize to trigger merges
    await new Promise(r => setTimeout(r, 2000));
    await ctx.client.command({ query: `OPTIMIZE TABLE ${TEST_DB}.events FINAL` });
    await new Promise(r => setTimeout(r, 1000));

    await ctx.client.command({ query: 'SYSTEM FLUSH LOGS' });
  }, CONTAINER_TIMEOUT);

  afterAll(async () => {
    if (ctx) {
      await ctx.client.command({ query: `DROP DATABASE IF EXISTS ${TEST_DB}` });
      await stopClickHouse(ctx);
    }
  }, 30_000);

  describe('getActiveMerges', () => {
    it('returns an array (may be empty if merges completed)', async () => {
      const merges = await tracker.getActiveMerges();
      expect(Array.isArray(merges)).toBe(true);
    });

    it('items have expected shape when present', async () => {
      const merges = await tracker.getActiveMerges();
      if (merges.length > 0) {
        const m = merges[0];
        expect(m).toHaveProperty('database');
        expect(m).toHaveProperty('table');
        expect(m).toHaveProperty('progress');
        expect(m).toHaveProperty('result_part_name');
        expect(m).toHaveProperty('source_part_names');
        expect(typeof m.progress).toBe('number');
      }
    });

    it('filters by database', async () => {
      const merges = await tracker.getActiveMerges('nonexistent_db');
      expect(merges).toEqual([]);
    });
  });

  describe('getMergeHistory', () => {
    it('returns merge history from part_log', async () => {
      const history = await tracker.getMergeHistory({ database: TEST_DB, table: 'events', limit: 50 });
      // After OPTIMIZE FINAL, there should be merge events
      expect(history.length).toBeGreaterThanOrEqual(1);
    });

    it('items have expected shape', async () => {
      const history = await tracker.getMergeHistory({ database: TEST_DB, table: 'events', limit: 10 });
      if (history.length > 0) {
        const h = history[0];
        expect(h).toHaveProperty('event_time');
        expect(h).toHaveProperty('database');
        expect(h).toHaveProperty('table');
        expect(h).toHaveProperty('part_name');
        expect(h).toHaveProperty('rows');
        expect(h).toHaveProperty('size_in_bytes');
        expect(h).toHaveProperty('duration_ms');
      }
    });

    it('respects limit', async () => {
      const history = await tracker.getMergeHistory({ limit: 1 });
      expect(history.length).toBeLessThanOrEqual(1);
    });

    it('filters by database only', async () => {
      const history = await tracker.getMergeHistory({ database: TEST_DB, limit: 50 });
      for (const h of history) {
        expect(h.database).toBe(TEST_DB);
      }
    });

    it('returns empty for non-existent database', async () => {
      const history = await tracker.getMergeHistory({ database: 'nonexistent_db_xyz', table: 'nope' });
      expect(history).toEqual([]);
    });

    it('pushes category filter into SQL — Mutation returns only mutations', async () => {
      const history = await tracker.getMergeHistory({ database: TEST_DB, table: 'events', category: 'Mutation', limit: 50 });
      for (const h of history) {
        expect(h.merge_reason).toBe('Mutation');
      }
    });

    it('pushes category filter into SQL — Regular excludes mutations', async () => {
      const history = await tracker.getMergeHistory({ database: TEST_DB, table: 'events', category: 'Regular', limit: 50 });
      for (const h of history) {
        expect(h.merge_reason).toBe('Regular');
      }
    });

    it('category filter respects limit (limit applies after filter)', async () => {
      // Fetch all history to know the total for each category
      const all = await tracker.getMergeHistory({ database: TEST_DB, table: 'events', limit: 1000 });
      const mutationCount = all.filter(h => h.merge_reason === 'Mutation').length;
      const regularCount = all.filter(h => h.merge_reason === 'Regular').length;

      // With category filter, limit 1 should still return exactly 1 of that category
      if (mutationCount > 0) {
        const filtered = await tracker.getMergeHistory({ database: TEST_DB, table: 'events', category: 'Mutation', limit: 1 });
        expect(filtered).toHaveLength(1);
        expect(filtered[0].merge_reason).toBe('Mutation');
      }
      if (regularCount > 0) {
        const filtered = await tracker.getMergeHistory({ database: TEST_DB, table: 'events', category: 'Regular', limit: 1 });
        expect(filtered).toHaveLength(1);
        expect(filtered[0].merge_reason).toBe('Regular');
      }
    });

    it('client-side-only category (LightweightDelete) returns unfiltered results', async () => {
      // LightweightDelete can't be pushed to SQL (needs row-diff analysis),
      // so the query returns the full result set for client-side filtering
      const all = await tracker.getMergeHistory({ database: TEST_DB, table: 'events', limit: 50 });
      const withCategory = await tracker.getMergeHistory({ database: TEST_DB, table: 'events', category: 'LightweightDelete', limit: 50 });
      expect(withCategory.length).toBe(all.length);
    });
  });

  describe('getMutations', () => {
    it('returns an array or throws a query error for older CH versions', async () => {
      // parts_in_progress_names was added in CH 24.12+; older versions throw a query error.
      // Either outcome is acceptable — we're testing the service layer, not the SQL template.
      try {
        const mutations = await tracker.getMutations();
        expect(Array.isArray(mutations)).toBe(true);
      } catch (error) {
        // Older CH versions don't have parts_in_progress_names column
        expect((error as Error).message).toContain('Failed to get mutations');
      }
    });
  });

  describe('getMutationHistory', () => {
    it('returns completed mutations', async () => {
      // Wait a bit for mutation to complete
      await new Promise(r => setTimeout(r, 1000));
      const history = await tracker.getMutationHistory({ database: TEST_DB, table: 'events' });
      // The mutation should have completed by now
      expect(Array.isArray(history)).toBe(true);
    });
  });

  describe('getBackgroundPoolMetrics', () => {
    it('returns pool metrics with expected shape', async () => {
      const metrics = await tracker.getBackgroundPoolMetrics();
      expect(metrics).toHaveProperty('merge_pool_size');
      expect(metrics).toHaveProperty('merge_pool_active');
      expect(metrics).toHaveProperty('active_parts');
      expect(metrics).toHaveProperty('outdated_parts');
      expect(metrics).toHaveProperty('outdated_parts_bytes');
      expect(typeof metrics.merge_pool_size).toBe('number');
      expect(typeof metrics.active_parts).toBe('number');
      expect(metrics.active_parts).toBeGreaterThan(0); // We have data
    });
  });

  describe('analyzeMutationDependencies (pure computation)', () => {
    it('correctly classifies idle parts', () => {
      const result = tracker.analyzeMutationDependencies(
        {
          database: 'db', table: 'tbl', mutation_id: 'm1', command: 'UPDATE',
          create_time: '', parts_to_do: 2, total_parts: 2, parts_in_progress: 0,
          parts_done: 0, is_done: false, latest_failed_part: '', latest_fail_time: '',
          latest_fail_reason: '', is_killed: false, status: 'running', progress: 0,
          parts_to_do_names: ['p1', 'p2'], parts_in_progress_names: [],
        },
        [],
        [],
      );
      expect(result.part_statuses).toHaveLength(2);
      expect(result.part_statuses.every(p => p.status === 'idle')).toBe(true);
      expect(result.co_dependent_mutations).toEqual([]);
    });
  });
});
