/**
 * The 'Worst Waiting Queries' drill must land on something for every shape the
 * 'Where Query Time Goes' panel offers.
 *
 * That panel ranks every system.query_log row, shard children included. This
 * one lists coordinators only, because a shard child cannot be opened from the
 * UI. Drilling a shard-child shape therefore used to return zero rows, which on
 * a distributed workload is most of the panel — the failure looked identical to
 * "no data in this time range".
 *
 * Runs a real distributed query across 2 shards so the shard-child rewrites are
 * genuine ClickHouse output rather than hand-written text.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import {
  startSamplingCluster,
  stopSamplingCluster,
  type SamplingClusterContext,
} from './setup/sampling-cluster-container.js';
import { configuredClickHouseIsBefore } from './setup/constants.js';
import { RAW_QUERIES } from '@frontend-queries/index';
import { parseQueryMetadata } from '@frontend-analytics/metaLanguage';
import { resolveTimeRange, resolveDrillParams } from '@frontend-analytics/templateResolution';
import { ClusterService } from '../../services/cluster-service.js';

const CLUSTER_TIMEOUT = 180_000;
const describeCluster = configuredClickHouseIsBefore(24, 9) ? describe.skip : describe;

/** The panel under test, found by title so a reorder does not silently retarget. */
function worstWaitingQueriesSQL(): string {
  const sql = RAW_QUERIES.find(q => parseQueryMetadata(q)?.name === 'Worst Waiting Queries');
  if (!sql) throw new Error('Worst Waiting Queries panel not found in RAW_QUERIES');
  return sql;
}

/**
 * Resolve the panel exactly as the app does.
 *
 * An earlier version of this test substituted the placeholders with its own
 * regexes. It passed while the real drill returned nothing, because a
 * hand-rolled resolver cannot reproduce what the production one emits — the
 * bug lived in the gap between them. Everything here now goes through the
 * same functions the dashboard calls.
 */
function resolve(sql: string, shape: string, clusterName: string | null = 'test'): string {
  const interval = parseQueryMetadata(sql)?.directives.meta?.interval ?? '1 HOUR';
  let resolved = resolveTimeRange(sql, interval);
  resolved = resolveDrillParams(resolved, { query_shape: shape });
  return ClusterService.resolveTableRefs(resolved, clusterName);
}

describeCluster('Wait Breakdown drill reaches shard-child shapes', { tags: ['analytics'] }, () => {
  let ctx: SamplingClusterContext;
  const COORDINATOR_QID = `drill-test-${Date.now()}`;

  beforeAll(async () => {
    ctx = await startSamplingCluster({ nodes: 2, shards: 2, clusterName: 'test', samplingIntervalSec: 1 });

    // A distributed SELECT: ClickHouse rewrites it per shard, and those
    // rewrites are the rows the drill has to resolve back to this coordinator.
    // Slow enough to clear the panel's query_duration_ms > 100 floor.
    // Fans out to ch2 only, so ch1 holds the coordinator row and no child row.
    // With remote('ch1,ch2') the coordinator node also runs a child, and a
    // node-local subquery still finds one — which hid the real bug.
    await ctx.clients[0].command({
      query: `SELECT count(), sum(sleepEachRow(0.05))
              FROM remote('ch2', 'system', 'numbers', 'default', 'test')
              WHERE number < 4
              SETTINGS max_block_size = 1`,
      query_id: COORDINATOR_QID,
    });

    for (const client of ctx.clients) {
      await client.command({ query: 'SYSTEM FLUSH LOGS' });
    }
  }, CLUSTER_TIMEOUT);

  afterAll(async () => {
    if (ctx) {
      for (const client of ctx.clients) {
        try { await client.command({ query: 'DROP DATABASE IF EXISTS tracehouse' }); } catch {}
      }
      await stopSamplingCluster(ctx);
    }
  }, 60_000);

  /**
   * The shard-child shapes this query produced, restricted to those the panel
   * can actually list.
   *
   * A distributed SELECT also logs incidental children — `DESC TABLE
   * system.numbers` here, at 0ms — which the panel's own 100ms floor excludes.
   * Asserting the drill returns rows for those would be asserting against the
   * panel's design.
   */
  async function childShapes(): Promise<string[]> {
    const rows = await ctx.clients[0].query({
      query: `SELECT DISTINCT substring(normalizeQuery(query), 1, 60) AS shape
              FROM clusterAllReplicas('test', system.query_log)
              WHERE type = 'QueryFinish'
                AND is_initial_query = 0
                AND query_duration_ms > 100
                AND initial_query_id = {qid:String}`,
      query_params: { qid: COORDINATOR_QID },
      format: 'JSONEachRow',
    });
    return (await rows.json<{ shape: string }>()).map(r => r.shape);
  }

  it('produces shard children whose shape differs from the coordinator', async () => {
    const shapes = await childShapes();
    // Guards the two tests below: an empty list would make their loops vacuous.
    expect(shapes.length).toBeGreaterThan(0);
  });

  it('drilling a shard-child shape returns those shard executions', async () => {
    // The point of the drill: you clicked a bar, you get the executions that
    // make up that bar. Returning their coordinators instead produced rows
    // whose composition looked nothing like the bar clicked.
    const shapes = await childShapes();
    const sql = worstWaitingQueriesSQL();

    for (const shape of shapes) {
      const result = await ctx.clients[0].query({
        query: resolve(sql, shape),
        format: 'JSONEachRow',
      });
      const rows = await result.json<{ query_id: string; segment: string; ms: number }>();
      expect(rows.length).toBeGreaterThan(0);

      // The reason this drill exists. A shard execution's wait is largely
      // network — streaming results back — while its coordinator's is parked.
      // Clicking a bar with a network segment and landing on rows without one
      // is the bug the coordinator-matching version shipped.
      expect(rows.map(r => r.segment)).toContain('network');

      // Every row must be an execution of the clicked shape, not something
      // merely related to it.
      const shapesBack = await ctx.clients[0].query({
        query: `SELECT DISTINCT substring(normalizeQuery(query), 1, 60) AS shape
                FROM clusterAllReplicas('test', system.query_log)
                WHERE type = 'QueryFinish' AND query_id IN ({ids:Array(String)})`,
        query_params: { ids: rows.map(r => r.query_id) },
        format: 'JSONEachRow',
      });
      expect((await shapesBack.json<{ shape: string }>()).map(r => r.shape)).toEqual([shape]);

      // The coordinator has a different shape, so it must not appear.
      expect(rows.map(r => r.query_id)).not.toContain(COORDINATOR_QID);
    }
  }, 60_000);

  it('still matches client-submitted shapes on their own text', async () => {
    const result = await ctx.clients[0].query({
      query: `SELECT substring(normalizeQuery(query), 1, 60) AS shape
              FROM system.query_log
              WHERE type = 'QueryFinish' AND query_id = {qid:String} AND is_initial_query = 1
              LIMIT 1`,
      query_params: { qid: COORDINATOR_QID },
      format: 'JSONEachRow',
    });
    const [own] = await result.json<{ shape: string }>();
    expect(own).toBeDefined();

    const drilled = await ctx.clients[0].query({
      query: resolve(worstWaitingQueriesSQL(), own.shape),
      format: 'JSONEachRow',
    });
    const ids = (await drilled.json<{ query_id: string }>()).map(r => r.query_id);
    expect(ids).toContain(COORDINATOR_QID);
  }, 60_000);
});
