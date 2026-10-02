/**
 * Integration test for OverviewService.getRecentActivity (resource arena backfill).
 * Runs a real query and a real merge, flushes the logs, and checks that both
 * come back inside the lookback window with sane end/elapsed values.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { startClickHouse, stopClickHouse, type TestClickHouseContext } from './setup/clickhouse-container.js';
import { OverviewService } from '../../services/overview-service.js';

const CONTAINER_TIMEOUT = 120_000;
const TEST_DB = 'recent_activity_test';

describe('OverviewService.getRecentActivity', { tags: ['merge-engine'] }, () => {
  let ctx: TestClickHouseContext;
  let overview: OverviewService;

  beforeAll(async () => {
    ctx = await startClickHouse();
    overview = new OverviewService(ctx.adapter);

    const cmd = (query: string) => ctx.client.command({ query });
    await cmd(`CREATE DATABASE IF NOT EXISTS ${TEST_DB}`);
    await cmd(`CREATE TABLE ${TEST_DB}.t (id UInt64) ENGINE = MergeTree ORDER BY id`);
    await cmd(`INSERT INTO ${TEST_DB}.t SELECT number FROM numbers(1000)`);
    await cmd(`INSERT INTO ${TEST_DB}.t SELECT number FROM numbers(1000)`);
    await cmd(`OPTIMIZE TABLE ${TEST_DB}.t FINAL`);
    await cmd(`SELECT sleep(1.2), count() FROM ${TEST_DB}.t`);
    await cmd(`SELECT count() FROM ${TEST_DB}.t WHERE id = 1`);
    await cmd('SYSTEM FLUSH LOGS');
  }, CONTAINER_TIMEOUT);

  afterAll(async () => {
    if (ctx) {
      await ctx.client.command({ query: `DROP DATABASE IF EXISTS ${TEST_DB}` });
      await stopClickHouse(ctx);
    }
  }, 30_000);

  it('drops sub-second operations by default and keeps slow ones', async () => {
    const windowSeconds = 120;
    const { queries, merges } = await overview.getRecentActivity(windowSeconds);

    const slow = queries.find(q => q.query.includes('sleep(1.2)'));
    expect(slow).toBeDefined();
    expect(slow!.elapsed).toBeGreaterThanOrEqual(1);
    expect(slow!.endedAgoSec).toBeGreaterThanOrEqual(0);
    expect(slow!.endedAgoSec).toBeLessThanOrEqual(windowSeconds);

    expect(queries.some(q => q.query.includes('WHERE id = 1'))).toBe(false);
    expect(queries.every(q => q.elapsed >= 1)).toBe(true);
    expect(merges.some(m => m.database === TEST_DB)).toBe(false);
  });

  it('returns fast queries and the merge when the minimum duration is 0', async () => {
    const windowSeconds = 120;
    const { queries, merges } = await overview.getRecentActivity(windowSeconds, { minDurationMs: 0 });

    expect(queries.some(q => q.query.includes('WHERE id = 1'))).toBe(true);

    const merge = merges.find(m => m.database === TEST_DB && m.table === 't' && !m.isMutation);
    expect(merge).toBeDefined();
    expect(merge!.numParts).toBe(2);
    // same units as the live system.merges row: progress 0..1, merge_type naming, uncompressed bytes
    expect(merge!.progress).toBe(1);
    expect(merge!.mergeType).toBe('Regular');
    expect(merge!.writeBytesPerSec).toBeGreaterThan(0);
    expect(merge!.endedAgoSec).toBeLessThanOrEqual(windowSeconds);
  });
});
