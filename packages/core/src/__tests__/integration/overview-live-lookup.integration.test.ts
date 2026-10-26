/**
 * Integration tests for the single-operation lookups behind the arena detail
 * panel's 1s refresh: OverviewService.getRunningQuery / getActiveMerge.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { startClickHouse, stopClickHouse, type TestClickHouseContext } from './setup/clickhouse-container.js';
import { OverviewService } from '../../services/overview-service.js';

const CONTAINER_TIMEOUT = 120_000;

describe('OverviewService live lookups', () => {
  let ctx: TestClickHouseContext;
  let service: OverviewService;

  beforeAll(async () => {
    ctx = await startClickHouse();
    service = new OverviewService(ctx.adapter);
  }, CONTAINER_TIMEOUT);

  afterAll(async () => {
    if (ctx) await stopClickHouse(ctx);
  }, 30_000);

  it('returns a running query by id, mapped like the full poll', async () => {
    const queryId = `live-lookup-${Date.now()}`;
    const running = ctx.client.command({
      query: 'SELECT count() FROM numbers(1e10) WHERE sleepEachRow(0) = 0 SETTINGS max_block_size = 65536, max_execution_time = 3, timeout_overflow_mode = \'break\'',
      query_id: queryId,
    });
    await new Promise(r => setTimeout(r, 1000));
    const q = await service.getRunningQuery(queryId);
    await running;

    expect(q).not.toBeNull();
    expect(q!.queryId).toBe(queryId);
    expect(q!.elapsed).toBeGreaterThan(0);
    expect(q!.rowsRead).toBeGreaterThan(0);
    expect(q!.progress).toBeGreaterThan(0);
    expect(q!.progress).toBeLessThanOrEqual(100);
  });

  it('returns null once the query is gone, and for an unknown merge', async () => {
    expect(await service.getRunningQuery(`missing-${Date.now()}`)).toBeNull();
    expect(await service.getActiveMerge('default', 'no_such_table', 'all_1_2_1')).toBeNull();
    expect(await service.getActiveMerge('default', 'no_such_table', 'all_1_2_1', 'some-host')).toBeNull();
  });
});
