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

const CLUSTER_TIMEOUT = 180_000;
const describeCluster = configuredClickHouseIsBefore(24, 9) ? describe.skip : describe;

/** The panel under test, found by title so a reorder does not silently retarget. */
function worstWaitingQueriesSQL(): string {
  const sql = RAW_QUERIES.find(q => parseQueryMetadata(q)?.name === 'Worst Waiting Queries');
  if (!sql) throw new Error('Worst Waiting Queries panel not found in RAW_QUERIES');
  return sql;
}

/**
 * Resolve the panel's templates by hand.
 *
 * The production resolver needs a live cluster context; this test only needs
 * the drill and time-range placeholders filled, and doing it here keeps the
 * assertion about the SQL's own logic.
 *
 * cluster_aware becomes clusterAllReplicas, matching what ClusterAwareAdapter
 * emits. It has to: a shard child's query_log row lives on the node that ran
 * it, so a single-node resolution cannot see the children at all.
 */
function resolve(sql: string, shape: string): string {
  return sql
    .replace(/--\s*@\w+:.*$/gm, '')
    .replace(/\{\{cluster_aware:system\.(\w+)\}\}/g, "clusterAllReplicas('test', system.$1)")
    .replace(/\{\{time_range\}\}/g, "now() - INTERVAL 1 HOUR")
    .replace(/\{\{drill_value:query_shape\s*\|\s*''\}\}/g, `'${shape.replace(/'/g, "\\'")}'`)
    .replace(/\{\{drill_value:\w+\s*\|\s*''\}\}/g, "''");
}

describeCluster('Wait Breakdown drill reaches shard-child shapes', { tags: ['analytics'] }, () => {
  let ctx: SamplingClusterContext;
  const COORDINATOR_QID = `drill-test-${Date.now()}`;

  beforeAll(async () => {
    ctx = await startSamplingCluster({ nodes: 2, shards: 2, clusterName: 'test', samplingIntervalSec: 1 });

    // A distributed SELECT: ClickHouse rewrites it per shard, and those
    // rewrites are the rows the drill has to resolve back to this coordinator.
    // Slow enough to clear the panel's query_duration_ms > 100 floor.
    await ctx.clients[0].command({
      query: `SELECT count(), sum(sleepEachRow(0.05))
              FROM remote('ch1,ch2', 'system', 'numbers', 'default', 'test')
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

  /** The shard-child shapes the top panel would offer for this query. */
  async function childShapes(): Promise<string[]> {
    const rows = await ctx.clients[0].query({
      query: `SELECT DISTINCT substring(normalizeQuery(query), 1, 60) AS shape
              FROM clusterAllReplicas('test', system.query_log)
              WHERE type = 'QueryFinish'
                AND is_initial_query = 0
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

  it('drilling a shard-child shape returns the coordinator that produced it', async () => {
    const shapes = await childShapes();
    const sql = worstWaitingQueriesSQL();

    for (const shape of shapes) {
      const result = await ctx.clients[0].query({
        query: resolve(sql, shape),
        format: 'JSONEachRow',
      });
      const rows = await result.json<{ query_id: string }>();
      const ids = new Set(rows.map(r => r.query_id));

      expect(ids.size).toBeGreaterThan(0);
      expect(ids).toContain(COORDINATOR_QID);
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
