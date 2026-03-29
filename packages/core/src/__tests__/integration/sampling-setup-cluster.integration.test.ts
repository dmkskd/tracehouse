/**
 * Integration tests for infra/scripts/setup_sampling.sh on a multi-node cluster.
 *
 * Spins up a 4-node ClickHouse cluster (2 shards × 2 replicas) that mimics
 * the Altinity operator topology (chop-generated-remote_servers.xml).
 *
 * These tests verify that the setup script correctly propagates the database
 * and tables to ALL nodes via ON CLUSTER DDL.
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { startAltinityCluster, stopAltinityCluster, type AltinityClusterContext } from './setup/index.js';
import { execSync } from 'node:child_process';
import { resolve } from 'node:path';

const CLUSTER_TIMEOUT = 180_000;

const SCRIPT_PATH = resolve(__dirname, '../../../../../infra/scripts/setup_sampling.sh');

// The setup script takes ~10-20s per run (DDL + 3s live sample wait)
const TEST_TIMEOUT = 120_000;

describe('setup_sampling.sh on Altinity-like cluster (2s×2r)', { tags: ['cluster'] }, () => {
  let ctx: AltinityClusterContext;
  let ch1Host: string;
  let ch1NativePort: number;

  beforeAll(async () => {
    ctx = await startAltinityCluster();
    // Connect to ch1 (shard 1, replica 1) — same as k8s setup.sh does
    ch1Host = ctx.ch1.getHost();
    ch1NativePort = ctx.ch1.getMappedPort(9000);
  }, CLUSTER_TIMEOUT);

  afterAll(async () => {
    if (ctx) {
      // Clean up tracehouse database on all nodes
      for (const client of ctx.clients) {
        try {
          await client.command({ query: 'DROP DATABASE IF EXISTS tracehouse' });
        } catch { /* node may not have it */ }
      }
      await stopAltinityCluster(ctx);
    }
  }, 60_000);

  beforeEach(async () => {
    // Clean up any previous run on all nodes
    for (const client of ctx.clients) {
      try {
        await client.command({ query: 'DROP DATABASE IF EXISTS tracehouse' });
      } catch { /* ignore */ }
    }
  });

  /** Run the setup script targeting ch1 with given arguments */
  function runScript(args: string): string {
    const cmd = `bash "${SCRIPT_PATH}" --host ${ch1Host} --port ${ch1NativePort} --user default --password test --yes ${args}`;
    return execSync(cmd, {
      encoding: 'utf-8',
      timeout: 60_000,
      env: { ...process.env, PATH: process.env.PATH },
    });
  }

  /** Check if a database exists on a specific node */
  async function hasDatabaseOnNode(nodeIdx: number, dbName: string): Promise<boolean> {
    const result = await ctx.clients[nodeIdx].query({
      query: `SELECT count() AS cnt FROM system.databases WHERE name = '${dbName}'`,
      format: 'JSONEachRow',
    });
    const rows = await result.json<{ cnt: string }>();
    return Number(rows[0].cnt) > 0;
  }

  /** Get list of tables in tracehouse database on a specific node */
  async function getTablesOnNode(nodeIdx: number): Promise<string[]> {
    try {
      const result = await ctx.clients[nodeIdx].query({
        query: `SELECT name FROM system.tables WHERE database = 'tracehouse' ORDER BY name`,
        format: 'JSONEachRow',
      });
      const rows = await result.json<{ name: string }>();
      return rows.map(r => r.name);
    } catch {
      return []; // database doesn't exist on this node
    }
  }

  // -----------------------------------------------------------------------
  // Cluster topology sanity checks
  // -----------------------------------------------------------------------

  describe('cluster topology', () => {
    it('dev cluster has 4 nodes (2 shards × 2 replicas)', async () => {
      const result = await ctx.clients[0].query({
        query: `SELECT count() AS cnt FROM system.clusters WHERE cluster = 'dev'`,
        format: 'JSONEachRow',
      });
      const rows = await result.json<{ cnt: string }>();
      expect(Number(rows[0].cnt)).toBe(4);
    });

    it('all-sharded cluster has 4 nodes', async () => {
      const result = await ctx.clients[0].query({
        query: `SELECT count() AS cnt FROM system.clusters WHERE cluster = 'all-sharded'`,
        format: 'JSONEachRow',
      });
      const rows = await result.json<{ cnt: string }>();
      expect(Number(rows[0].cnt)).toBe(4);
    });

    it('all 4 nodes are reachable', async () => {
      for (let i = 0; i < 4; i++) {
        const result = await ctx.clients[i].query({
          query: 'SELECT 1 AS ok',
          format: 'JSONEachRow',
        });
        const rows = await result.json<{ ok: string }>();
        expect(Number(rows[0].ok)).toBe(1);
      }
    });
  });

  // -----------------------------------------------------------------------
  // DDL propagation with --cluster dev (the main cluster, all 4 nodes)
  // -----------------------------------------------------------------------

  describe('DDL propagation with --cluster dev', () => {
    it('creates tracehouse database on ALL 4 nodes', { timeout: TEST_TIMEOUT }, async () => {
      runScript('--cluster dev');

      const results = await Promise.all([
        hasDatabaseOnNode(0, 'tracehouse'),
        hasDatabaseOnNode(1, 'tracehouse'),
        hasDatabaseOnNode(2, 'tracehouse'),
        hasDatabaseOnNode(3, 'tracehouse'),
      ]);

      expect(results).toEqual([true, true, true, true]);
    });

    it('creates all tables on ALL 4 nodes', { timeout: TEST_TIMEOUT }, async () => {
      runScript('--cluster dev');

      const expectedTables = [
        'merges_history',
        'merges_history_buffer',
        'merges_sampler',
        'processes_history',
        'processes_history_buffer',
        'processes_sampler',
      ];

      for (let i = 0; i < 4; i++) {
        const tables = await getTablesOnNode(i);
        for (const expected of expectedTables) {
          expect(tables, `node ${i + 1} (ch${i + 1}) missing table ${expected}`).toContain(expected);
        }
      }
    });

    it('samplers are running on ALL 4 nodes', { timeout: TEST_TIMEOUT }, async () => {
      runScript('--cluster dev');

      // Wait a moment for samplers to register
      await new Promise(r => setTimeout(r, 3000));

      for (let i = 0; i < 4; i++) {
        try {
          const result = await ctx.clients[i].query({
            query: `SELECT view FROM system.view_refreshes WHERE database = 'tracehouse' ORDER BY view`,
            format: 'JSONEachRow',
          });
          const views = (await result.json<{ view: string }>()).map(r => r.view);
          expect(views, `node ${i + 1} (ch${i + 1}) missing samplers`).toContain('processes_sampler');
          expect(views, `node ${i + 1} (ch${i + 1}) missing samplers`).toContain('merges_sampler');
        } catch (e) {
          throw new Error(`node ${i + 1} (ch${i + 1}): ${e instanceof Error ? e.message : e}`);
        }
      }
    });
  });

  // -----------------------------------------------------------------------
  // --on-cluster flag
  // -----------------------------------------------------------------------

  describe('--on-cluster', () => {
    it('defaults to yes — DDL contains ON CLUSTER', { timeout: TEST_TIMEOUT }, () => {
      const output = runScript('--cluster dev --dry-run');

      expect(output).toContain('ON CLUSTER: yes');
      expect(output).toMatch(/ON CLUSTER 'dev'/);
    });

    it('can be disabled — DDL does not contain ON CLUSTER', { timeout: TEST_TIMEOUT }, () => {
      const output = runScript('--cluster dev --on-cluster no --dry-run');

      expect(output).toContain('ON CLUSTER: no');
      expect(output).not.toMatch(/ON CLUSTER 'dev'/);
    });

    it('when disabled, DDL is not propagated to other nodes', { timeout: TEST_TIMEOUT }, async () => {
      runScript('--cluster dev --on-cluster no');

      // Database and tables should exist on the connected node (ch1)
      expect(await hasDatabaseOnNode(0, 'tracehouse')).toBe(true);
      const tables = await getTablesOnNode(0);
      for (const expected of ['processes_history', 'merges_history', 'processes_sampler', 'merges_sampler']) {
        expect(tables, `ch1 missing table ${expected}`).toContain(expected);
      }

      // Other nodes should NOT have the database
      for (let i = 1; i < 4; i++) {
        expect(
          await hasDatabaseOnNode(i, 'tracehouse'),
          `ch${i + 1} should NOT have tracehouse database`,
        ).toBe(false);
      }
    });
  });

  // -----------------------------------------------------------------------
  // Auto-detection tests
  // -----------------------------------------------------------------------

  describe('auto-detection (no --cluster flag)', () => {
    it('auto-detects topology and selects a cluster', { timeout: TEST_TIMEOUT }, () => {
      // Without --cluster, the script should detect the topology and pick one
      const output = runScript('');
      // It should succeed and create tables
      expect(output).toContain('Setup complete');
    });
  });
});
