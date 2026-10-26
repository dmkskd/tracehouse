/**
 * Integration tests for LinkResolver: runs the running / pending-flush lookups
 * against a real ClickHouse and checks each state is detected.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { startClickHouse, stopClickHouse, type TestClickHouseContext } from './setup/clickhouse-container.js';
import { LinkResolver, LINK_RESOLUTION_CONFIG } from '../../services/link-resolver.js';
import { MergeTracker } from '../../services/merge-tracker.js';

const CONTAINER_TIMEOUT = 120_000;
const TEST_DB = 'link_resolver_test';

describe('LinkResolver integration', () => {
  let ctx: TestClickHouseContext;
  let resolver: LinkResolver;

  beforeAll(async () => {
    ctx = await startClickHouse();
    resolver = new LinkResolver(ctx.adapter);
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

  describe('queries', () => {
    it('detects a running query', async () => {
      const queryId = `link-running-${Date.now()}`;
      const running = ctx.client.command({ query: 'SELECT sleep(3)', query_id: queryId });
      // Give the server a moment to register the query in system.processes.
      await new Promise(r => setTimeout(r, 500));
      const status = await resolver.diagnoseQuery(queryId);
      await running;
      expect(status).toMatchObject({ state: 'running' });
      if (status.state === 'running') expect(status.elapsedSec).toBeGreaterThan(0);
    });

    it('reports pending flush when only the QueryStart row exists', async () => {
      const queryId = `link-started-${Date.now()}`;
      await ctx.client.command({ query: 'SELECT 1', query_id: queryId });
      await ctx.client.command({ query: 'SYSTEM FLUSH LOGS' });
      // The finish row exists too here; diagnoseQuery is only called after the
      // finish lookup missed, so this exercises the QueryStart lookup itself.
      expect(await resolver.diagnoseQuery(queryId)).toMatchObject({ state: 'pending_flush' });
    });

    it('uses the end-time hint and falls back to not_found', async () => {
      const unknownId = `link-unknown-${Date.now()}`;
      expect(await resolver.diagnoseQuery(unknownId, Date.now() - 1_000)).toEqual({ state: 'pending_flush' });
      expect(await resolver.diagnoseQuery(unknownId, Date.now() - 600_000)).toEqual({ state: 'not_found' });
      expect(await resolver.diagnoseQuery(unknownId)).toEqual({ state: 'not_found' });
    });
  });

  describe('merges', () => {
    beforeAll(async () => {
      await ctx.client.command({
        query: `CREATE TABLE IF NOT EXISTS ${TEST_DB}.merged (id UInt64) ENGINE = MergeTree ORDER BY id`,
      });
      await ctx.client.command({ query: `SYSTEM STOP MERGES ${TEST_DB}.merged` });
      await ctx.client.command({ query: `INSERT INTO ${TEST_DB}.merged SELECT number FROM numbers(10)` });
      await ctx.client.command({ query: `INSERT INTO ${TEST_DB}.merged SELECT number FROM numbers(10)` });
      await ctx.client.command({ query: `SYSTEM START MERGES ${TEST_DB}.merged` });
      await ctx.client.command({ query: `OPTIMIZE TABLE ${TEST_DB}.merged FINAL` });
    });

    async function resultPartName(): Promise<string> {
      const result = await ctx.client.query({
        query: `SELECT name FROM system.parts WHERE database = '${TEST_DB}' AND table = 'merged' AND active`,
        format: 'JSONEachRow',
      });
      const rows = await result.json<{ name: string }>();
      expect(rows).toHaveLength(1);
      return rows[0].name;
    }

    it('reports pending flush for a freshly written result part', async () => {
      const partName = await resultPartName();
      const status = await resolver.diagnoseMerge({ database: TEST_DB, table: 'merged', partName });
      expect(status).toMatchObject({ state: 'pending_flush' });
    });

    it('scopes diagnosis and the part_log lookup to the requested host', async () => {
      const partName = await resultPartName();
      const hostResult = await ctx.client.query({ query: 'SELECT hostName() AS h', format: 'JSONEachRow' });
      const [{ h: host }] = await hostResult.json<{ h: string }>();
      const ref = { database: TEST_DB, table: 'merged', partName };

      expect(await resolver.diagnoseMerge({ ...ref, hostname: host })).toMatchObject({ state: 'pending_flush', hostname: host });
      expect(await resolver.diagnoseMerge({ ...ref, hostname: 'other-replica' })).toEqual({ state: 'not_found' });

      await ctx.client.command({ query: 'SYSTEM FLUSH LOGS' });
      const tracker = new MergeTracker(ctx.adapter);
      const onHost = await tracker.getMergeHistoryByPartName(TEST_DB, 'merged', partName, { hostname: host });
      expect(onHost).toMatchObject({ part_name: partName, hostname: host });
      expect(await tracker.getMergeHistoryByPartName(TEST_DB, 'merged', partName, { hostname: 'other-replica' })).toBeNull();
    });

    it('reports not_found for an unknown part', async () => {
      const status = await resolver.diagnoseMerge({ database: TEST_DB, table: 'merged', partName: 'all_999_999_1' });
      expect(status).toEqual({ state: 'not_found' });
    });

    it('skips the extra checks when they are switched off', async () => {
      const partName = await resultPartName();
      const off = new LinkResolver(ctx.adapter, { ...LINK_RESOLUTION_CONFIG, checkRunning: false, checkPendingFlush: false });
      expect(await off.diagnoseMerge({ database: TEST_DB, table: 'merged', partName })).toEqual({ state: 'not_found' });
    });
  });
});
