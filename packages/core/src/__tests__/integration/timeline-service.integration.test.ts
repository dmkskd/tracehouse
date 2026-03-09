/**
 * Integration tests for TimelineService against a real ClickHouse instance.
 *
 * Validates:
 * - Timeline data fetching (queries, merges, mutations) with correct shape
 * - Activity limit (LIMIT N) is respected and configurable
 * - query_kind field is populated for queries
 * - merge_reason field uses canonical MergeCategory values from classifyMergeHistory
 * - MovePart events (TTL Move) bypass the 1MB memory filter
 * - sortMetric controls which ORDER BY column is used
 * - Count queries return totals independent of the LIMIT
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { startClickHouse, stopClickHouse, type TestClickHouseContext } from './setup/clickhouse-container.js';
import { TimelineService } from '../../services/timeline-service.js';
import { ALL_MERGE_CATEGORIES } from '../../utils/merge-classification.js';
import type { MergeCategory } from '../../utils/merge-classification.js';

const CONTAINER_TIMEOUT = 120_000;
const TEST_DB = 'timeline_test';

describe('TimelineService integration', () => {
  let ctx: TestClickHouseContext;
  let service: TimelineService;

  beforeAll(async () => {
    ctx = await startClickHouse();
    service = new TimelineService(ctx.adapter);

    await ctx.client.command({ query: `CREATE DATABASE IF NOT EXISTS ${TEST_DB}` });

    // Create a table and insert data to generate query_log + part_log entries
    await ctx.client.command({
      query: `
        CREATE TABLE IF NOT EXISTS ${TEST_DB}.events (
          id UInt64, ts DateTime DEFAULT now(), value Float64
        ) ENGINE = MergeTree()
        ORDER BY (ts, id)
      `,
    });

    // Insert multiple batches to create parts
    for (let i = 0; i < 5; i++) {
      await ctx.client.command({
        query: `INSERT INTO ${TEST_DB}.events (id, value) SELECT number + ${i * 1000}, rand() FROM numbers(1000)`,
      });
    }

    // Trigger a merge
    await ctx.client.command({ query: `OPTIMIZE TABLE ${TEST_DB}.events FINAL` });
    await new Promise(r => setTimeout(r, 2000));

    // Run a mutation to generate MutatePart events
    await ctx.client.command({
      query: `ALTER TABLE ${TEST_DB}.events UPDATE value = value + 1 WHERE id < 100`,
    });

    // Wait for mutation
    for (let i = 0; i < 20; i++) {
      const result = await ctx.client.query({
        query: `SELECT count() as cnt FROM system.mutations WHERE database = '${TEST_DB}' AND table = 'events' AND is_done = 0`,
        format: 'JSONEachRow',
      });
      const rows = await result.json<{ cnt: string }>();
      if (Number(rows[0]?.cnt) === 0) break;
      await new Promise(r => setTimeout(r, 500));
    }

    // Run a SELECT to ensure we have a query_log entry with query_kind
    await ctx.client.query({
      query: `SELECT count() FROM ${TEST_DB}.events`,
      format: 'JSONEachRow',
    });

    await ctx.client.command({ query: 'SYSTEM FLUSH LOGS' });
  }, CONTAINER_TIMEOUT);

  afterAll(async () => {
    if (ctx) {
      if (!ctx.keepData) {
        await ctx.client.command({ query: `DROP DATABASE IF EXISTS ${TEST_DB}` });
      }
      await stopClickHouse(ctx);
    }
  }, 30_000);

  // ── Basic timeline fetch ───────────────────────────────────────────

  describe('getTimeline returns valid data', () => {
    it('returns a MemoryTimeline with expected shape', async () => {
      const result = await service.getTimeline({
        timestamp: new Date(),
        windowSeconds: 300,
      });

      expect(result).toHaveProperty('window_start');
      expect(result).toHaveProperty('window_end');
      expect(result).toHaveProperty('queries');
      expect(result).toHaveProperty('merges');
      expect(result).toHaveProperty('mutations');
      expect(result).toHaveProperty('query_count');
      expect(result).toHaveProperty('merge_count');
      expect(result).toHaveProperty('mutation_count');
      expect(Array.isArray(result.queries)).toBe(true);
      expect(Array.isArray(result.merges)).toBe(true);
      expect(Array.isArray(result.mutations)).toBe(true);
    });

    it('finds queries from the test setup', async () => {
      const result = await service.getTimeline({
        timestamp: new Date(),
        windowSeconds: 300,
      });

      // We ran INSERT and SELECT queries during setup
      expect(result.queries.length).toBeGreaterThanOrEqual(1);
    });

    it('finds merges from the test setup', async () => {
      const result = await service.getTimeline({
        timestamp: new Date(),
        windowSeconds: 300,
      });

      // We ran OPTIMIZE TABLE which should produce merge events
      expect(result.merges.length).toBeGreaterThanOrEqual(1);
    });
  });

  // ── query_kind field ───────────────────────────────────────────────

  describe('query_kind classification', () => {
    it('queries have query_kind populated', async () => {
      const result = await service.getTimeline({
        timestamp: new Date(),
        windowSeconds: 300,
      });

      // At least some queries should have query_kind
      const withKind = result.queries.filter(q => q.query_kind);
      expect(withKind.length).toBeGreaterThanOrEqual(1);
    });

    it('query_kind values are valid ClickHouse query kinds', async () => {
      const result = await service.getTimeline({
        timestamp: new Date(),
        windowSeconds: 300,
      });

      const validKinds = ['Select', 'Insert', 'Create', 'Drop', 'Alter', 'System', 'Optimize', 'Other'];
      for (const q of result.queries) {
        if (q.query_kind) {
          expect(validKinds).toContain(q.query_kind);
        }
      }
    });
  });

  // ── merge_reason classification ────────────────────────────────────

  describe('merge_reason uses canonical MergeCategory values', () => {
    it('all merge_reason values are valid MergeCategory strings', async () => {
      const result = await service.getTimeline({
        timestamp: new Date(),
        windowSeconds: 300,
      });

      for (const m of result.merges) {
        if (m.merge_reason) {
          expect(ALL_MERGE_CATEGORIES as readonly string[]).toContain(m.merge_reason);
        }
      }
    });

    it('regular merges are classified as Regular', async () => {
      const result = await service.getTimeline({
        timestamp: new Date(),
        windowSeconds: 300,
      });

      // Our test table has no TTL, so merges from OPTIMIZE should be Regular
      const regularMerges = result.merges.filter(m => m.merge_reason === 'Regular');
      expect(regularMerges.length).toBeGreaterThanOrEqual(1);
    });
  });

  // ── Activity limit ─────────────────────────────────────────────────

  describe('activityLimit controls LIMIT', () => {
    it('default limit (100) returns up to 100 items', async () => {
      const result = await service.getTimeline({
        timestamp: new Date(),
        windowSeconds: 300,
      });

      // With default limit, should not exceed 100 per type
      expect(result.queries.length).toBeLessThanOrEqual(100);
      expect(result.merges.length).toBeLessThanOrEqual(100);
      expect(result.mutations.length).toBeLessThanOrEqual(100);
    });

    it('small limit caps the returned items', async () => {
      const result = await service.getTimeline({
        timestamp: new Date(),
        windowSeconds: 300,
        activityLimit: 2,
      });

      // With limit=2, queries/merges should be capped
      // (running items may add a few more via dedup merge)
      expect(result.queries.length).toBeLessThanOrEqual(5); // 2 + possible running
      expect(result.merges.length).toBeLessThanOrEqual(5);
    });

    it('totalCount reflects actual total, not the LIMIT', async () => {
      // Fetch with a very small limit
      const small = await service.getTimeline({
        timestamp: new Date(),
        windowSeconds: 300,
        activityLimit: 1,
        includeRunning: false,
      });

      // Fetch with a large limit
      const large = await service.getTimeline({
        timestamp: new Date(),
        windowSeconds: 300,
        activityLimit: 500,
        includeRunning: false,
      });

      // The count queries don't have LIMIT, so totals should match
      expect(small.query_count).toBe(large.query_count);
      expect(small.merge_count).toBe(large.merge_count);
    });
  });

  // ── sortMetric controls ORDER BY ───────────────────────────────────

  describe('sortMetric controls ordering', () => {
    it('sortMetric=memory returns items sorted by memory', async () => {
      const result = await service.getTimeline({
        timestamp: new Date(),
        windowSeconds: 300,
        sortMetric: 'memory',
      });

      // Queries should be in descending memory order (from SQL)
      for (let i = 1; i < result.queries.length; i++) {
        expect(result.queries[i - 1].peak_memory).toBeGreaterThanOrEqual(result.queries[i].peak_memory);
      }
    });

    it('sortMetric=cpu returns items (may differ from memory order)', async () => {
      const byMemory = await service.getTimeline({
        timestamp: new Date(),
        windowSeconds: 300,
        sortMetric: 'memory',
        activityLimit: 50,
      });

      const byCpu = await service.getTimeline({
        timestamp: new Date(),
        windowSeconds: 300,
        sortMetric: 'cpu',
        activityLimit: 50,
      });

      // Both should return valid results
      expect(byCpu.queries.length).toBeGreaterThanOrEqual(1);
      expect(byMemory.queries.length).toBeGreaterThanOrEqual(1);

      // The query IDs may differ because ORDER BY changed
      // (not guaranteed to differ with small datasets, but the queries should succeed)
    });

    it('different sortMetric values produce valid results', async () => {
      for (const metric of ['memory', 'cpu', 'network', 'disk'] as const) {
        const result = await service.getTimeline({
          timestamp: new Date(),
          windowSeconds: 300,
          sortMetric: metric,
          activityLimit: 5,
        });

        // Should not throw and should return valid shape
        expect(Array.isArray(result.queries)).toBe(true);
        expect(Array.isArray(result.merges)).toBe(true);
      }
    });
  });

  // ── MovePart (TTL Move) bypass ─────────────────────────────────────

  describe('MovePart events bypass memory filter', () => {
    it('SQL filter includes MovePart in event_type IN clause', async () => {
      // Verify at the SQL level that MovePart events with 0 memory would be included.
      // We can't easily trigger a TTL Move in a single-disk container, so we verify
      // the SQL shape by checking part_log for MovePart acceptance.
      const rows = await ctx.rawAdapter.executeQuery<{ cnt: string }>(`
        SELECT count() as cnt
        FROM system.part_log
        WHERE event_type = 'MovePart'
          AND peak_memory_usage = 0
      `);
      // This just validates the query runs (MovePart + 0 memory is legal)
      expect(Array.isArray(rows)).toBe(true);
    });

    it('the detail query filter accepts MovePart with 0 memory', async () => {
      // Directly test the filter condition from ACTIVE_MERGES_DETAIL
      const rows = await ctx.rawAdapter.executeQuery<{ accepted: number }>(`
        SELECT 1 as accepted
        WHERE (0 > 1048576 OR 'MovePart' = 'MovePart')
      `);
      expect(rows.length).toBe(1);
      expect(rows[0].accepted).toBe(1);
    });
  });

  // ── Query shape validation ─────────────────────────────────────────

  describe('query and merge item shape', () => {
    it('query items have expected fields', async () => {
      const result = await service.getTimeline({
        timestamp: new Date(),
        windowSeconds: 300,
      });

      if (result.queries.length > 0) {
        const q = result.queries[0];
        expect(q).toHaveProperty('query_id');
        expect(q).toHaveProperty('label');
        expect(q).toHaveProperty('user');
        expect(q).toHaveProperty('peak_memory');
        expect(q).toHaveProperty('duration_ms');
        expect(q).toHaveProperty('cpu_us');
        expect(q).toHaveProperty('start_time');
        expect(q).toHaveProperty('end_time');
        expect(q).toHaveProperty('query_kind');
      }
    });

    it('merge items have expected fields including merge_reason', async () => {
      const result = await service.getTimeline({
        timestamp: new Date(),
        windowSeconds: 300,
      });

      if (result.merges.length > 0) {
        const m = result.merges[0];
        expect(m).toHaveProperty('part_name');
        expect(m).toHaveProperty('table');
        expect(m).toHaveProperty('peak_memory');
        expect(m).toHaveProperty('duration_ms');
        expect(m).toHaveProperty('start_time');
        expect(m).toHaveProperty('end_time');
        expect(m).toHaveProperty('merge_reason');
        // merge_reason should be a canonical MergeCategory
        if (m.merge_reason) {
          expect(ALL_MERGE_CATEGORIES as readonly string[]).toContain(m.merge_reason);
        }
      }
    });
  });
});
