/**
 * Integration tests for service-layer error wrapping against a real ClickHouse instance.
 *
 * Instead of injecting a failing mock adapter, we trigger real errors by
 * sending invalid SQL to a real ClickHouse container and verifying that
 * each service wraps the AdapterError in its domain-specific error type.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { startClickHouse, stopClickHouse, type TestClickHouseContext } from './setup/clickhouse-container.js';
import { DatabaseExplorer, DatabaseExplorerError } from '../../services/database-explorer.js';
import { QueryAnalyzer, QueryAnalysisError } from '../../services/query-analyzer.js';
import { MetricsCollector, MetricsCollectionError } from '../../services/metrics-collector.js';
import { MergeTracker, MergeTrackerError } from '../../services/merge-tracker.js';
import type { IClickHouseAdapter } from '../../adapters/types.js';
import { AdapterError } from '../../adapters/types.js';

const CONTAINER_TIMEOUT = 120_000;

/**
 * Adapter that wraps a real adapter but corrupts all SQL to trigger query errors.
 * This lets us test error wrapping with a real ClickHouse connection.
 */
class CorruptingAdapter implements IClickHouseAdapter {
  constructor(private inner: IClickHouseAdapter) {}

  async executeQuery<T extends Record<string, unknown>>(sql: string): Promise<T[]> {
    // Prepend garbage to make any SQL invalid
    try {
      return await this.inner.executeQuery<T>('INVALID_SYNTAX_XYZ ' + sql);
    } catch (error) {
      // Re-throw as AdapterError (which is what real adapters do)
      const msg = error instanceof Error ? error.message : String(error);
      throw new AdapterError(msg, 'query', error instanceof Error ? error : undefined);
    }
  }

  async executeCommand(sql: string): Promise<void> {
    // Not needed for these tests
  }
}

describe('Service error wrapping integration', { tags: ['connectivity'] }, () => {
  let ctx: TestClickHouseContext;
  let corruptAdapter: CorruptingAdapter;

  beforeAll(async () => {
    ctx = await startClickHouse();
    corruptAdapter = new CorruptingAdapter(ctx.adapter);
  }, CONTAINER_TIMEOUT);

  afterAll(async () => {
    if (ctx) {
      await stopClickHouse(ctx);
    }
  }, 30_000);

  describe('DatabaseExplorer wraps errors in DatabaseExplorerError', () => {
    it('listDatabases', async () => {
      const explorer = new DatabaseExplorer(corruptAdapter);
      await expect(explorer.listDatabases()).rejects.toThrow(DatabaseExplorerError);
    });

    it('listTables', async () => {
      const explorer = new DatabaseExplorer(corruptAdapter);
      await expect(explorer.listTables('default')).rejects.toThrow(DatabaseExplorerError);
    });

    it('getTableSchema', async () => {
      const explorer = new DatabaseExplorer(corruptAdapter);
      await expect(explorer.getTableSchema('default', 'test')).rejects.toThrow(DatabaseExplorerError);
    });

    it('getTableParts', async () => {
      const explorer = new DatabaseExplorer(corruptAdapter);
      await expect(explorer.getTableParts('default', 'test')).rejects.toThrow(DatabaseExplorerError);
    });

    it('getPartDetail', async () => {
      const explorer = new DatabaseExplorer(corruptAdapter);
      await expect(explorer.getPartDetail('default', 'test', 'part')).rejects.toThrow(DatabaseExplorerError);
    });

    it('getPartLineage', async () => {
      const explorer = new DatabaseExplorer(corruptAdapter);
      await expect(explorer.getPartLineage('default', 'test', 'part')).rejects.toThrow(DatabaseExplorerError);
    });

    it('preserves cause chain', async () => {
      const explorer = new DatabaseExplorer(corruptAdapter);
      try {
        await explorer.listDatabases();
        expect.unreachable('should have thrown');
      } catch (e) {
        expect(e).toBeInstanceOf(DatabaseExplorerError);
        expect((e as DatabaseExplorerError).cause).toBeInstanceOf(AdapterError);
      }
    });
  });

  describe('QueryAnalyzer wraps errors in QueryAnalysisError', () => {
    it('getRunningQueries', async () => {
      const analyzer = new QueryAnalyzer(corruptAdapter);
      await expect(analyzer.getRunningQueries()).rejects.toThrow(QueryAnalysisError);
    });

    it('getQueryHistory', async () => {
      const analyzer = new QueryAnalyzer(corruptAdapter);
      await expect(analyzer.getQueryHistory({
        start_date: '2024-01-01',
        start_time: '2024-01-01T00:00:00Z',
        end_time: '2024-01-02T00:00:00Z',
      })).rejects.toThrow(QueryAnalysisError);
    });

    it('killQuery', async () => {
      const analyzer = new QueryAnalyzer(corruptAdapter);
      await expect(analyzer.killQuery('some-id')).rejects.toThrow(QueryAnalysisError);
    });

    it('preserves cause chain', async () => {
      const analyzer = new QueryAnalyzer(corruptAdapter);
      try {
        await analyzer.getRunningQueries();
        expect.unreachable('should have thrown');
      } catch (e) {
        expect(e).toBeInstanceOf(QueryAnalysisError);
        expect((e as QueryAnalysisError).cause).toBeInstanceOf(AdapterError);
      }
    });
  });

  describe('MetricsCollector wraps errors in MetricsCollectionError', () => {
    it('getServerMetrics', async () => {
      const collector = new MetricsCollector(corruptAdapter);
      await expect(collector.getServerMetrics()).rejects.toThrow(MetricsCollectionError);
    });

    it('preserves cause chain', async () => {
      const collector = new MetricsCollector(corruptAdapter);
      try {
        await collector.getServerMetrics();
        expect.unreachable('should have thrown');
      } catch (e) {
        expect(e).toBeInstanceOf(MetricsCollectionError);
        expect((e as MetricsCollectionError).cause).toBeInstanceOf(AdapterError);
      }
    });
  });

  describe('MergeTracker wraps errors in MergeTrackerError', () => {
    it('getActiveMerges', async () => {
      const tracker = new MergeTracker(corruptAdapter);
      await expect(tracker.getActiveMerges()).rejects.toThrow(MergeTrackerError);
    });

    it('getMergeHistory', async () => {
      const tracker = new MergeTracker(corruptAdapter);
      await expect(tracker.getMergeHistory({ database: 'db', table: 'tbl' })).rejects.toThrow(MergeTrackerError);
    });

    it('getMutations', async () => {
      const tracker = new MergeTracker(corruptAdapter);
      await expect(tracker.getMutations()).rejects.toThrow(MergeTrackerError);
    });

    it('getBackgroundPoolMetrics', async () => {
      const tracker = new MergeTracker(corruptAdapter);
      await expect(tracker.getBackgroundPoolMetrics()).rejects.toThrow(MergeTrackerError);
    });

    it('preserves cause chain', async () => {
      const tracker = new MergeTracker(corruptAdapter);
      try {
        await tracker.getActiveMerges();
        expect.unreachable('should have thrown');
      } catch (e) {
        expect(e).toBeInstanceOf(MergeTrackerError);
        expect((e as MergeTrackerError).cause).toBeInstanceOf(AdapterError);
      }
    });
  });
});
