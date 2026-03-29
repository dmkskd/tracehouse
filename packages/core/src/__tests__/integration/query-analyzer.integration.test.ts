/**
 * Integration tests for QueryAnalyzer against a real ClickHouse instance.
 *
 * Runs queries to populate system.processes and system.query_log,
 * then validates QueryAnalyzer methods return correct results.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { startClickHouse, stopClickHouse, type TestClickHouseContext } from './setup/clickhouse-container.js';
import { QueryAnalyzer } from '../../services/query-analyzer.js';

const CONTAINER_TIMEOUT = 120_000;

describe('QueryAnalyzer integration', { tags: ['query-analysis'] }, () => {
  let ctx: TestClickHouseContext;
  let analyzer: QueryAnalyzer;

  beforeAll(async () => {
    ctx = await startClickHouse();
    analyzer = new QueryAnalyzer(ctx.adapter);

    // Generate query_log entries by running some queries
    await ctx.client.command({ query: 'CREATE DATABASE IF NOT EXISTS qa_test' });
    await ctx.client.command({
      query: `
        CREATE TABLE IF NOT EXISTS qa_test.data (
          id UInt64, value Float64
        ) ENGINE = MergeTree() ORDER BY id
      `,
    });
    await ctx.client.command({
      query: `INSERT INTO qa_test.data SELECT number, rand() FROM numbers(10000)`,
    });
    // Run a SELECT to generate a QueryFinish entry
    await ctx.adapter.executeQuery('SELECT count() FROM qa_test.data');
    // Run a query that will fail to generate an ExceptionWhileProcessing entry
    try {
      await ctx.adapter.executeQuery('SELECT nonexistent_column FROM qa_test.data');
    } catch { /* expected */ }

    await ctx.client.command({ query: 'SYSTEM FLUSH LOGS' });
  }, CONTAINER_TIMEOUT);

  afterAll(async () => {
    if (ctx) {
      await ctx.client.command({ query: 'DROP DATABASE IF EXISTS qa_test' });
      await stopClickHouse(ctx);
    }
  }, 30_000);

  describe('getRunningQueries', () => {
    it('returns an array (may be empty if no long queries)', async () => {
      const queries = await analyzer.getRunningQueries();
      expect(Array.isArray(queries)).toBe(true);
      // Our own query should appear briefly, but it may have finished
      // The important thing is the SQL is valid and returns the right shape
    });

    it('returned items have expected shape when present', async () => {
      const queries = await analyzer.getRunningQueries();
      if (queries.length > 0) {
        const q = queries[0];
        expect(q).toHaveProperty('query_id');
        expect(q).toHaveProperty('user');
        expect(q).toHaveProperty('query');
        expect(q).toHaveProperty('elapsed_seconds');
        expect(q).toHaveProperty('memory_usage');
      }
    });
  });

  describe('getQueryHistory', () => {
    it('returns finished queries from query_log', async () => {
      const now = new Date();
      const oneHourAgo = new Date(now.getTime() - 3600_000);

      const history = await analyzer.getQueryHistory({
        start_date: oneHourAgo.toISOString().split('T')[0],
        start_time: oneHourAgo.toISOString(),
        end_time: now.toISOString(),
        limit: 50,
      });

      expect(history.length).toBeGreaterThan(0);
    });

    it('returned items have expected shape', async () => {
      const now = new Date();
      const oneHourAgo = new Date(now.getTime() - 3600_000);

      const history = await analyzer.getQueryHistory({
        start_date: oneHourAgo.toISOString().split('T')[0],
        start_time: oneHourAgo.toISOString(),
        end_time: now.toISOString(),
        limit: 10,
      });

      expect(history.length).toBeGreaterThan(0);
      const item = history[0];
      expect(item).toHaveProperty('query_id');
      expect(item).toHaveProperty('query_duration_ms');
      expect(item).toHaveProperty('read_rows');
      expect(item).toHaveProperty('memory_usage');
      expect(item).toHaveProperty('query');
      expect(item).toHaveProperty('user');
    });

    it('respects limit parameter', async () => {
      const now = new Date();
      const oneHourAgo = new Date(now.getTime() - 3600_000);

      const history = await analyzer.getQueryHistory({
        start_date: oneHourAgo.toISOString().split('T')[0],
        start_time: oneHourAgo.toISOString(),
        end_time: now.toISOString(),
        limit: 2,
      });

      expect(history.length).toBeLessThanOrEqual(2);
    });

    it('returns empty for future time range', async () => {
      const history = await analyzer.getQueryHistory({
        start_date: '2099-01-01',
        start_time: '2099-01-01T00:00:00Z',
        end_time: '2099-01-02T00:00:00Z',
      });

      expect(history).toEqual([]);
    });
  });

  describe('killQuery', () => {
    it('does not throw for a non-existent query id', async () => {
      // KILL QUERY WHERE query_id = '...' is a no-op if no match — should not throw
      await expect(analyzer.killQuery('nonexistent-query-id-xyz')).resolves.not.toThrow();
    });
  });

  describe('getQueryDetail', () => {
    it('returns detail for a known query', async () => {
      // First get a query_id from history
      const now = new Date();
      const oneHourAgo = new Date(now.getTime() - 3600_000);
      const history = await analyzer.getQueryHistory({
        start_date: oneHourAgo.toISOString().split('T')[0],
        start_time: oneHourAgo.toISOString(),
        end_time: now.toISOString(),
        limit: 1,
      });

      if (history.length > 0) {
        const detail = await analyzer.getQueryDetail(history[0].query_id);
        expect(detail).not.toBeNull();
        expect(detail!.query_id).toBe(history[0].query_id);
        expect(detail!).toHaveProperty('query_duration_ms');
        expect(detail!).toHaveProperty('read_rows');
        expect(detail!).toHaveProperty('memory_usage');
      }
    });

    it('returns null for non-existent query id', async () => {
      const detail = await analyzer.getQueryDetail('nonexistent-id-xyz');
      expect(detail).toBeNull();
    });
  });

  describe('getCoordinatorIds', () => {
    it('returns empty set for empty input', async () => {
      const result = await analyzer.getCoordinatorIds([], '2020-01-01');
      expect(result).toEqual(new Set());
    });

    it('returns empty set when no sub-queries exist for given IDs', async () => {
      // Plain queries on a single node have no shard sub-queries
      const now = new Date();
      const oneHourAgo = new Date(now.getTime() - 3600_000);
      const history = await analyzer.getQueryHistory({
        start_date: oneHourAgo.toISOString().split('T')[0],
        start_time: oneHourAgo.toISOString(),
        end_time: now.toISOString(),
        limit: 10,
      });

      const queryIds = history.map(q => q.query_id);
      const startDate = oneHourAgo.toISOString().split('T')[0]!;
      const result = await analyzer.getCoordinatorIds(queryIds, startDate);
      expect(result).toBeInstanceOf(Set);
      expect(result.size).toBe(0);
    });

    it('returns empty set for non-existent query IDs', async () => {
      const result = await analyzer.getCoordinatorIds(
        ['00000000-0000-0000-0000-000000000000'],
        new Date().toISOString().split('T')[0]!,
      );
      expect(result).toEqual(new Set());
    });
  });

  describe('getSettingsDefaults', () => {
    it('returns defaults for known settings', async () => {
      const defaults = await analyzer.getSettingsDefaults(['max_threads', 'max_memory_usage']);
      expect(defaults.length).toBeGreaterThan(0);
      const names = defaults.map(d => d.name);
      expect(names).toContain('max_threads');
    });

    it('returns empty for empty input', async () => {
      const defaults = await analyzer.getSettingsDefaults([]);
      expect(defaults).toEqual([]);
    });
  });
});
