/**
 * Integration tests for TraceService.getFlamegraphFoldedForTimeRange().
 *
 * Runs a CPU-intensive query to generate system.trace_log entries,
 * then validates the time-scoped flamegraph query returns correct shape.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { startClickHouse, stopClickHouse, type TestClickHouseContext } from './setup/clickhouse-container.js';
import { TraceService } from '../../services/trace-service.js';
import { createClient } from '@clickhouse/client';
import { ClusterAwareAdapter } from '../../adapters/cluster-adapter.js';
import { TestAdapter } from './setup/clickhouse-container.js';

const CONTAINER_TIMEOUT = 120_000;

describe('TraceService.getFlamegraphFoldedForTimeRange', () => {
  let ctx: TestClickHouseContext;
  let traceService: TraceService;
  let queryStartTime: string;

  beforeAll(async () => {
    ctx = await startClickHouse();

    // Create a separate client with allow_introspection_functions enabled
    const url = ctx.container
      ? ctx.container.getConnectionUrl()
      : process.env.CH_TEST_URL!;
    const introClient = createClient({
      url,
      clickhouse_settings: { allow_introspection_functions: 1 },
    });
    const introAdapter = new ClusterAwareAdapter(new TestAdapter(introClient));
    traceService = new TraceService(introAdapter);

    // Record the time before running the query
    queryStartTime = new Date().toISOString();

    // Run a CPU-intensive query to generate trace_log entries.
    try {
      await ctx.client.query({
        query: `SELECT count() FROM numbers(50000000) WHERE sipHash64(number) % 2 = 0`,
        query_id: 'time-scoped-fg-test-' + Date.now(),
        format: 'JSONEachRow',
      });
    } catch { /* query itself doesn't matter */ }

    // Flush logs so trace_log is populated
    await ctx.client.command({ query: 'SYSTEM FLUSH LOGS' });
  }, CONTAINER_TIMEOUT);

  afterAll(async () => {
    if (ctx) await stopClickHouse(ctx);
  }, 30_000);

  it('should return folded-stack text for a valid time range', async () => {
    const fromTime = queryStartTime;
    const toTime = new Date(Date.now() + 60_000).toISOString(); // 1 minute ahead

    const result = await traceService.getFlamegraphFoldedForTimeRange(
      'time-scoped-fg-test-*', // won't match, but SQL should be valid
      fromTime,
      toTime,
    );

    // Should not throw — empty folded is fine (no matching query_id)
    expect(typeof result.folded).toBe('string');
    expect(result.unavailableReason).toBeUndefined();
  });

  it('should not throw on ISO timestamps with Z suffix', async () => {
    // Regression: ISO format '2026-03-20T13:52:17.000Z' must be converted
    // to ClickHouse-compatible format without the Z/milliseconds.
    const result = await traceService.getFlamegraphFoldedForTimeRange(
      'any-query-id',
      '2026-03-20T13:52:17.000Z',
      '2026-03-20T13:52:18.000Z',
      '2026-03-20',
    );
    expect(typeof result.folded).toBe('string');
  });

  it('should return empty folded for non-existent query', async () => {
    const result = await traceService.getFlamegraphFoldedForTimeRange(
      'non-existent-query-id',
      '2026-01-01 00:00:00',
      '2026-01-01 00:01:00',
      '2026-01-01',
    );
    expect(result.folded).toBe('');
  });
});
