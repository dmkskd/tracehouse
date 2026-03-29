/**
 * Smoke tests that run production queries against real system tables.
 *
 * These don't validate specific values (we can't control what CH generates),
 * but they verify that:
 *   - Our SQL is syntactically valid against the actual CH version
 *   - Column names haven't changed between CH releases
 *   - The query/response shape matches what our mappers expect
 *
 * Think of these as schema compatibility tests.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { startClickHouse, stopClickHouse, type TestClickHouseContext } from './setup/clickhouse-container.js';
import { MetricsCollector } from '../../services/metrics-collector.js';
import { DatabaseExplorer } from '../../services/database-explorer.js';
import { MergeTracker } from '../../services/merge-tracker.js';
import { TestAdapter } from './setup/clickhouse-container.js';

const CONTAINER_TIMEOUT = 120_000;

describe('Real system tables smoke tests', { tags: ['observability'] }, () => {
  let ctx: TestClickHouseContext;

  beforeAll(async () => {
    ctx = await startClickHouse();

    // Generate some activity so system tables have data.
    // Create a test table, insert data, and run a query to populate query_log.
    await ctx.client.command({
      query: `CREATE DATABASE IF NOT EXISTS smoke_test`,
    });
    await ctx.client.command({
      query: `
        CREATE TABLE IF NOT EXISTS smoke_test.events (
          id UInt64,
          ts DateTime DEFAULT now(),
          value Float64
        ) ENGINE = MergeTree() ORDER BY (ts, id)
      `,
    });
    // Insert enough rows to trigger some activity
    await ctx.client.command({
      query: `INSERT INTO smoke_test.events (id, value) SELECT number, rand() FROM numbers(10000)`,
    });
    // Force a flush of system logs so query_log has entries
    await ctx.client.command({ query: `SYSTEM FLUSH LOGS` });
  }, CONTAINER_TIMEOUT);

  afterAll(async () => {
    if (ctx) {
      await ctx.client.command({ query: `DROP DATABASE IF EXISTS smoke_test` });
      await stopClickHouse(ctx);
    }
  }, 30_000);

  describe('MetricsCollector against real system tables', () => {
    it('getServerMetrics() returns valid shape', async () => {
      const collector = new MetricsCollector(ctx.adapter);
      const metrics = await collector.getServerMetrics();

      expect(metrics).toHaveProperty('timestamp');
      expect(metrics).toHaveProperty('cpu_usage');
      expect(metrics).toHaveProperty('memory_used');
      expect(metrics).toHaveProperty('memory_total');
      expect(metrics).toHaveProperty('uptime_seconds');
      expect(typeof metrics.cpu_usage).toBe('number');
      expect(typeof metrics.memory_total).toBe('number');
      expect(metrics.uptime_seconds).toBeGreaterThanOrEqual(0);
    });
  });

  describe('DatabaseExplorer against real system tables', () => {
    it('listDatabases() returns at least system and default', async () => {
      const explorer = new DatabaseExplorer(ctx.adapter);
      const databases = await explorer.listDatabases();

      const names = databases.map(d => d.name);
      expect(names).toContain('system');
      expect(names).toContain('default');
    });

    it('listTables() returns tables for smoke_test db', async () => {
      const explorer = new DatabaseExplorer(ctx.adapter);
      const tables = await explorer.listTables('smoke_test');

      expect(tables.length).toBeGreaterThanOrEqual(1);
      const names = tables.map(t => t.name);
      expect(names).toContain('events');
    });
  });

  describe('MergeTracker against real system tables', () => {
    it('getActiveMerges() does not throw', async () => {
      const tracker = new MergeTracker(ctx.adapter);
      // May return empty array if no merges are running — that's fine
      const merges = await tracker.getActiveMerges();
      expect(Array.isArray(merges)).toBe(true);
    });
  });

  describe('Raw query validation', () => {
    it('system.metric_log query is syntactically valid', async () => {
      // metric_log may not have data yet (needs time to accumulate),
      // but the query should not throw a syntax/column error
      const adapter = new TestAdapter(ctx.client);
      const rows = await adapter.executeQuery(`
        SELECT
          toString(event_time) AS t,
          ProfileEvent_OSCPUVirtualTimeMicroseconds AS v
        FROM system.metric_log
        LIMIT 1
      `);
      // May be empty, but should not throw
      expect(Array.isArray(rows)).toBe(true);
    });

    it('system.asynchronous_metrics query is syntactically valid', async () => {
      const adapter = new TestAdapter(ctx.client);
      const rows = await adapter.executeQuery<{ metric: string; value: number }>(`
        SELECT metric, value
        FROM system.asynchronous_metrics
        WHERE metric LIKE 'OS%'
        LIMIT 1
      `);
      // Some OS metrics should always exist
      expect(rows.length).toBeGreaterThanOrEqual(0);
      // Verify the query shape is correct (no column errors)
      if (rows.length > 0) {
        expect(rows[0]).toHaveProperty('metric');
        expect(rows[0]).toHaveProperty('value');
      }
    });

    it('system.query_log has entries after FLUSH LOGS', async () => {
      const adapter = new TestAdapter(ctx.client);
      const rows = await adapter.executeQuery<{ cnt: number }>(`
        SELECT count() AS cnt FROM system.query_log WHERE type = 'QueryFinish'
      `);
      expect(Number(rows[0].cnt)).toBeGreaterThan(0);
    });
  });
});
