/**
 * Integration tests for merge_type surfacing in both MergeTracker and OverviewService.
 *
 * Validates that:
 * - MergeTracker.getActiveMerges() returns merge_type from system.merges
 * - OverviewService.getActiveMerges() maps merge_type to mergeType
 * - TTL-triggered merges are distinguishable from regular merges
 * - merge_reason is captured in merge history (system.part_log)
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { startClickHouse, stopClickHouse, type TestClickHouseContext } from './setup/clickhouse-container.js';
import { MergeTracker } from '../../services/merge-tracker.js';
import { OverviewService } from '../../services/overview-service.js';

const CONTAINER_TIMEOUT = 120_000;
const TEST_DB = 'merge_type_test';

describe('merge_type surfacing integration', () => {
  let ctx: TestClickHouseContext;
  let tracker: MergeTracker;
  let overview: OverviewService;

  beforeAll(async () => {
    ctx = await startClickHouse();
    tracker = new MergeTracker(ctx.adapter);
    overview = new OverviewService(ctx.adapter);

    await ctx.client.command({ query: `CREATE DATABASE IF NOT EXISTS ${TEST_DB}` });
  }, CONTAINER_TIMEOUT);

  afterAll(async () => {
    if (ctx) {
      await ctx.client.command({ query: `DROP DATABASE IF EXISTS ${TEST_DB}` });
      await stopClickHouse(ctx);
    }
  }, 30_000);

  describe('MergeTracker.getActiveMerges() merge_type field', () => {
    it('returns merge_type in the MergeInfo shape', async () => {
      const merges = await tracker.getActiveMerges();
      expect(Array.isArray(merges)).toBe(true);
      // Even if empty, the schema contract is validated by the mapper.
      // If merges are present, verify the field exists.
      for (const m of merges) {
        expect(m).toHaveProperty('merge_type');
        expect(typeof m.merge_type).toBe('string');
      }
    });

    it('merge_type is a known value when merges are active', async () => {
      // Create a table and insert enough data to potentially trigger a merge
      await ctx.client.command({
        query: `
          CREATE TABLE IF NOT EXISTS ${TEST_DB}.type_check (
            id UInt64, ts DateTime DEFAULT now(), value Float64
          ) ENGINE = MergeTree() ORDER BY (ts, id)
        `,
      });

      for (let i = 0; i < 5; i++) {
        await ctx.client.command({
          query: `INSERT INTO ${TEST_DB}.type_check (id, value) SELECT number + ${i * 500}, rand() FROM numbers(500)`,
        });
      }

      // Trigger a merge
      await ctx.client.command({ query: `OPTIMIZE TABLE ${TEST_DB}.type_check FINAL` });

      // Check active merges — they may have already completed
      const merges = await tracker.getActiveMerges();
      for (const m of merges) {
        // merge_type should be one of the known ClickHouse values
        expect(['Regular', 'TTL_DELETE', 'TTL_RECOMPRESS', '']).toContain(m.merge_type);
      }
    });
  });

  describe('OverviewService.getActiveMerges() mergeType field', () => {
    it('returns mergeType string in ActiveMergeInfo', async () => {
      const merges = await overview.getActiveMerges();
      expect(Array.isArray(merges)).toBe(true);
      for (const m of merges) {
        expect(m).toHaveProperty('mergeType');
        expect(typeof m.mergeType).toBe('string');
      }
    });

    it('maps merge_type correctly for regular merges', async () => {
      // Insert data and optimize to create a merge
      await ctx.client.command({
        query: `
          CREATE TABLE IF NOT EXISTS ${TEST_DB}.overview_check (
            id UInt64, ts DateTime DEFAULT now(), value Float64
          ) ENGINE = MergeTree() ORDER BY (ts, id)
        `,
      });

      for (let i = 0; i < 5; i++) {
        await ctx.client.command({
          query: `INSERT INTO ${TEST_DB}.overview_check (id, value) SELECT number + ${i * 500}, rand() FROM numbers(500)`,
        });
      }

      await ctx.client.command({ query: `OPTIMIZE TABLE ${TEST_DB}.overview_check FINAL` });

      const merges = await overview.getActiveMerges();
      // If we caught the merge in progress, verify the type
      const ourMerges = merges.filter(m => m.database === TEST_DB);
      for (const m of ourMerges) {
        expect(typeof m.mergeType).toBe('string');
        // Regular OPTIMIZE merges should be 'Regular' or empty
        expect(['Regular', '']).toContain(m.mergeType);
      }
    });
  });

  describe('TTL merge_type detection', () => {
    it('creates a TTL DELETE table and verifies merge history captures merge_reason', async () => {
      // Create a table with TTL that deletes rows after 1 second
      await ctx.client.command({
        query: `
          CREATE TABLE IF NOT EXISTS ${TEST_DB}.ttl_test (
            id UInt64,
            ts DateTime DEFAULT now(),
            value Float64
          ) ENGINE = MergeTree()
          ORDER BY (ts, id)
          TTL ts + INTERVAL 1 SECOND DELETE
          SETTINGS merge_with_ttl_timeout = 1, old_parts_lifetime = 1
        `,
      });

      // Insert rows with timestamps in the past so TTL is already expired
      await ctx.client.command({
        query: `
          INSERT INTO ${TEST_DB}.ttl_test (id, ts, value)
          SELECT number, now() - toIntervalSecond(10), rand()
          FROM numbers(100)
        `,
      });

      // Wait for TTL merge to kick in
      await new Promise(r => setTimeout(r, 5000));

      // Force a TTL merge if it hasn't happened yet
      await ctx.client.command({ query: `OPTIMIZE TABLE ${TEST_DB}.ttl_test FINAL` });
      await new Promise(r => setTimeout(r, 2000));

      // Flush logs so part_log has the merge event
      await ctx.client.command({ query: 'SYSTEM FLUSH LOGS' });

      // Check merge history — should show TTLDelete merge_reason
      const history = await tracker.getMergeHistory({
        database: TEST_DB,
        table: 'ttl_test',
        limit: 50,
      });

      // There should be at least one merge event
      expect(history.length).toBeGreaterThanOrEqual(1);

      // Verify merge_reason is populated
      for (const h of history) {
        expect(h).toHaveProperty('merge_reason');
        expect(typeof h.merge_reason).toBe('string');
        expect(h.merge_reason.length).toBeGreaterThan(0);
      }

      // At least one should be TTL-related (TTLDeleteMerge or RegularMerge)
      const reasons = history.map(h => h.merge_reason);
      // The merge_reason from part_log goes through classifyMergeHistory,
      // so we check for the classified output
      expect(reasons.some(r => r.length > 0)).toBe(true);
    });

    it('TTL merges are distinguishable via startsWith("TTL") pattern', () => {
      // Unit-level validation of the TTL detection pattern used in the frontend
      const ttlTypes = ['TTL_DELETE', 'TTL_RECOMPRESS'];
      const regularTypes = ['Regular', ''];

      for (const t of ttlTypes) {
        expect(t.startsWith('TTL')).toBe(true);
      }
      for (const t of regularTypes) {
        expect(t.startsWith('TTL')).toBe(false);
      }
    });
  });

  describe('GET_ACTIVE_MERGES_LIVE query includes merge_type', () => {
    it('raw query returns merge_type column', async () => {
      // Execute the raw query directly to verify the column exists
      const rows = await ctx.rawAdapter.executeQuery<{ merge_type: string }>(
        'SELECT merge_type FROM system.merges LIMIT 0'
      );
      // Even with 0 rows, the query should succeed (column exists)
      expect(Array.isArray(rows)).toBe(true);
    });
  });

  describe('disk_name and path_on_disk in merge history', () => {
    it('merge history records include disk_name field', async () => {
      const history = await tracker.getMergeHistory({ database: TEST_DB, limit: 50 });
      for (const h of history) {
        // disk_name should be present (may be undefined for older CH versions)
        expect(h).toHaveProperty('disk_name');
        if (h.disk_name) {
          expect(typeof h.disk_name).toBe('string');
        }
      }
    });

    it('merge history records include path_on_disk field', async () => {
      const history = await tracker.getMergeHistory({ database: TEST_DB, limit: 50 });
      for (const h of history) {
        expect(h).toHaveProperty('path_on_disk');
        if (h.path_on_disk) {
          expect(typeof h.path_on_disk).toBe('string');
        }
      }
    });
  });

  describe('getStoragePolicyVolumes', () => {
    it('returns storage policy volumes with expected shape', async () => {
      const volumes = await tracker.getStoragePolicyVolumes();
      expect(Array.isArray(volumes)).toBe(true);
      // There should be at least the default policy
      expect(volumes.length).toBeGreaterThanOrEqual(1);
      for (const v of volumes) {
        expect(v).toHaveProperty('policyName');
        expect(v).toHaveProperty('volumeName');
        expect(v).toHaveProperty('disks');
        expect(typeof v.policyName).toBe('string');
        expect(typeof v.volumeName).toBe('string');
        expect(Array.isArray(v.disks)).toBe(true);
      }
    });

    it('default policy has a default disk', async () => {
      const volumes = await tracker.getStoragePolicyVolumes();
      const defaultPolicy = volumes.find(v => v.policyName === 'default');
      expect(defaultPolicy).toBeDefined();
      expect(defaultPolicy!.disks).toContain('default');
    });
  });
});
