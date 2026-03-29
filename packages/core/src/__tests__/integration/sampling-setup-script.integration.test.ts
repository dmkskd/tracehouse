/**
 * Integration tests for infra/scripts/setup_sampling.sh.
 *
 * Runs the actual shell script against a real ClickHouse container
 * and verifies that the correct tables, buffer tables, and refreshable
 * materialized views are created.
 *
 * Tests all three --target modes: processes, merges, and all.
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { startClickHouse, stopClickHouse, type TestClickHouseContext } from './setup/clickhouse-container.js';
import { execSync } from 'node:child_process';
import { resolve } from 'node:path';

const CONTAINER_TIMEOUT = 120_000;

// Resolve script path relative to this file
// This file is at packages/core/src/__tests__/integration/
// Script is at infra/scripts/setup_sampling.sh
const SCRIPT_PATH = resolve(__dirname, '../../../../../infra/scripts/setup_sampling.sh');

describe('setup_sampling.sh script integration', { tags: ['setup'] }, () => {
  let ctx: TestClickHouseContext;
  let httpPort: number;
  let nativePort: number;
  let host: string;
  let username: string;
  let password: string;

  beforeAll(async () => {
    ctx = await startClickHouse();

    // Extract connection info from the container
    if (ctx.container) {
      host = ctx.container.getHost();
      nativePort = ctx.container.getMappedPort(9000);
      httpPort = ctx.container.getMappedPort(8123);
      // @testcontainers/clickhouse defaults to user=test, password=test
      username = ctx.container.getUsername();
      password = ctx.container.getPassword();
    } else {
      // External instance — parse from CH_TEST_URL
      const url = new URL(process.env.CH_TEST_URL!);
      host = url.hostname;
      httpPort = Number(url.port) || 8123;
      // Assume native port is 9000 for external instances
      nativePort = 9000;
      username = url.username || '';
      password = url.password || '';
    }
  }, CONTAINER_TIMEOUT);

  afterAll(async () => {
    if (ctx) {
      if (!ctx.keepData) {
        await ctx.client.command({ query: `DROP DATABASE IF EXISTS tracehouse` });
      }
      await stopClickHouse(ctx);
    }
  }, 30_000);

  beforeEach(async () => {
    // Clean up any previous run
    await ctx.client.command({ query: `DROP DATABASE IF EXISTS tracehouse` });
  });

  /** Run the setup script with given arguments */
  function runScript(args: string): string {
    const authArgs = [
      username ? `--user ${username}` : '',
      password ? `--password ${password}` : '',
    ].filter(Boolean).join(' ');
    const cmd = `bash "${SCRIPT_PATH}" --host ${host} --port ${nativePort} ${authArgs} --yes ${args}`;
    return execSync(cmd, {
      encoding: 'utf-8',
      timeout: 30_000,
      env: { ...process.env, PATH: process.env.PATH },
    });
  }

  /** Get list of tables in tracehouse database */
  async function getTracehouseTables(): Promise<{ name: string; engine: string }[]> {
    const result = await ctx.client.query({
      query: `SELECT name, engine FROM system.tables WHERE database = 'tracehouse' ORDER BY name`,
      format: 'JSONEachRow',
    });
    return result.json<{ name: string; engine: string }>();
  }

  /** Get list of refreshable MVs in tracehouse */
  async function getRefreshableViews(): Promise<{ view: string }[]> {
    const result = await ctx.client.query({
      query: `SELECT view FROM system.view_refreshes WHERE database = 'tracehouse' ORDER BY view`,
      format: 'JSONEachRow',
    });
    return result.json<{ view: string }>();
  }

  /** Get column names for a table */
  async function getTableColumns(tableName: string): Promise<string[]> {
    const result = await ctx.client.query({
      query: `SELECT name FROM system.columns WHERE database = 'tracehouse' AND table = '${tableName}' ORDER BY position`,
      format: 'JSONEachRow',
    });
    const rows = await result.json<{ name: string }>();
    return rows.map(r => r.name);
  }

  // -----------------------------------------------------------------------
  // --target all (default)
  // -----------------------------------------------------------------------

  describe('--target all (default)', () => {
    it('creates both processes_history and merges_history tables', async () => {
      runScript('');

      const tables = await getTracehouseTables();
      const tableNames = tables.map(t => t.name);

      // Processes tables
      expect(tableNames).toContain('processes_history');
      expect(tableNames).toContain('processes_history_buffer');
      expect(tableNames).toContain('processes_sampler');

      // Merges tables
      expect(tableNames).toContain('merges_history');
      expect(tableNames).toContain('merges_history_buffer');
      expect(tableNames).toContain('merges_sampler');
    });

    it('creates tables with correct engines', async () => {
      runScript('');

      const tables = await getTracehouseTables();
      const byName = Object.fromEntries(tables.map(t => [t.name, t.engine]));

      expect(byName['processes_history']).toBe('MergeTree');
      expect(byName['processes_history_buffer']).toBe('Buffer');
      expect(byName['processes_sampler']).toBe('MaterializedView');

      expect(byName['merges_history']).toBe('MergeTree');
      expect(byName['merges_history_buffer']).toBe('Buffer');
      expect(byName['merges_sampler']).toBe('MaterializedView');
    });

    it('creates refreshable materialized views', async () => {
      runScript('');

      const views = await getRefreshableViews();
      const viewNames = views.map(v => v.view);

      expect(viewNames).toContain('processes_sampler');
      expect(viewNames).toContain('merges_sampler');
    });
  });

  // -----------------------------------------------------------------------
  // --target processes
  // -----------------------------------------------------------------------

  describe('--target processes', () => {
    it('creates only processes_history tables', async () => {
      runScript('--target processes');

      const tables = await getTracehouseTables();
      const tableNames = tables.map(t => t.name);

      expect(tableNames).toContain('processes_history');
      expect(tableNames).toContain('processes_history_buffer');
      expect(tableNames).toContain('processes_sampler');

      expect(tableNames).not.toContain('merges_history');
      expect(tableNames).not.toContain('merges_history_buffer');
      expect(tableNames).not.toContain('merges_sampler');
    });
  });

  // -----------------------------------------------------------------------
  // --target merges
  // -----------------------------------------------------------------------

  describe('--target merges', () => {
    it('creates only merges_history tables', async () => {
      runScript('--target merges');

      const tables = await getTracehouseTables();
      const tableNames = tables.map(t => t.name);

      expect(tableNames).toContain('merges_history');
      expect(tableNames).toContain('merges_history_buffer');
      expect(tableNames).toContain('merges_sampler');

      expect(tableNames).not.toContain('processes_history');
      expect(tableNames).not.toContain('processes_history_buffer');
      expect(tableNames).not.toContain('processes_sampler');
    });
  });

  // -----------------------------------------------------------------------
  // Schema validation
  // -----------------------------------------------------------------------

  describe('schema validation', () => {
    it('processes_history has expected columns', async () => {
      runScript('--target processes');

      const columns = await getTableColumns('processes_history');

      expect(columns).toContain('hostname');
      expect(columns).toContain('sample_time');
      expect(columns).toContain('query_id');
      expect(columns).toContain('initial_query_id');
      expect(columns).toContain('elapsed');
      expect(columns).toContain('memory_usage');
      expect(columns).toContain('peak_memory_usage');
      expect(columns).toContain('read_rows');
      expect(columns).toContain('read_bytes');
      expect(columns).toContain('written_rows');
      expect(columns).toContain('thread_ids');
      expect(columns).toContain('ProfileEvents');
      expect(columns).toContain('Settings');
    });

    it('merges_history has expected columns', async () => {
      runScript('--target merges');

      const columns = await getTableColumns('merges_history');

      expect(columns).toContain('hostname');
      expect(columns).toContain('sample_time');
      expect(columns).toContain('database');
      expect(columns).toContain('table');
      expect(columns).toContain('result_part_name');
      expect(columns).toContain('partition_id');
      expect(columns).toContain('elapsed');
      expect(columns).toContain('progress');
      expect(columns).toContain('num_parts');
      expect(columns).toContain('is_mutation');
      expect(columns).toContain('merge_type');
      expect(columns).toContain('merge_algorithm');
      expect(columns).toContain('total_size_bytes_compressed');
      expect(columns).toContain('total_size_bytes_uncompressed');
      expect(columns).toContain('rows_read');
      expect(columns).toContain('bytes_read_uncompressed');
      expect(columns).toContain('rows_written');
      expect(columns).toContain('bytes_written_uncompressed');
      expect(columns).toContain('columns_written');
      expect(columns).toContain('memory_usage');
      expect(columns).toContain('thread_id');
    });
  });

  // -----------------------------------------------------------------------
  // --dry-run mode
  // -----------------------------------------------------------------------

  describe('--dry-run mode', () => {
    it('outputs SQL without creating any tables', async () => {
      const output = runScript('--dry-run');

      // Should contain SQL statements
      expect(output).toContain('CREATE DATABASE');
      expect(output).toContain('CREATE TABLE');
      expect(output).toContain('CREATE MATERIALIZED VIEW');
      expect(output).toContain('processes_history');
      expect(output).toContain('merges_history');

      // But no tables should actually be created
      const result = await ctx.client.query({
        query: `SELECT count() AS cnt FROM system.databases WHERE name = 'tracehouse'`,
        format: 'JSONEachRow',
      });
      const rows = await result.json<{ cnt: string }>();
      expect(Number(rows[0].cnt)).toBe(0);
    });

    it('--dry-run respects --target', async () => {
      const output = runScript('--dry-run --target merges');

      expect(output).toContain('merges_history');
      expect(output).toContain('merges_sampler');
      expect(output).not.toContain('processes_history');
      expect(output).not.toContain('processes_sampler');
    });
  });

  // -----------------------------------------------------------------------
  // Idempotency
  // -----------------------------------------------------------------------

  describe('idempotency', () => {
    it('can be run twice without error', async () => {
      runScript('');
      // Second run should succeed (IF NOT EXISTS)
      expect(() => runScript('')).not.toThrow();

      const tables = await getTracehouseTables();
      const tableNames = tables.map(t => t.name);

      expect(tableNames).toContain('processes_history');
      expect(tableNames).toContain('merges_history');
    });
  });

  // -----------------------------------------------------------------------
  // TTL configuration
  // -----------------------------------------------------------------------

  describe('--ttl option', () => {
    it('creates tables with TTL when specified', async () => {
      runScript('--ttl 14');

      const result = await ctx.client.query({
        query: `SELECT engine_full FROM system.tables WHERE database = 'tracehouse' AND name = 'processes_history'`,
        format: 'JSONEachRow',
      });
      const rows = await result.json<{ engine_full: string }>();
      expect(rows[0].engine_full).toContain('TTL');
    });

    it('creates tables without TTL when --ttl 0', async () => {
      runScript('--ttl 0');

      const result = await ctx.client.query({
        query: `SELECT engine_full FROM system.tables WHERE database = 'tracehouse' AND name = 'processes_history'`,
        format: 'JSONEachRow',
      });
      const rows = await result.json<{ engine_full: string }>();
      expect(rows[0].engine_full).not.toContain('TTL');
    });
  });

  // -----------------------------------------------------------------------
  // Invalid arguments
  // -----------------------------------------------------------------------

  describe('invalid arguments', () => {
    it('rejects invalid --target', () => {
      expect(() => runScript('--target invalid')).toThrow();
    });

    it('rejects invalid --interval', () => {
      expect(() => runScript('--interval 0')).toThrow();
      expect(() => runScript('--interval -1')).toThrow();
      expect(() => runScript('--interval abc')).toThrow();
    });

    it('rejects invalid --ttl', () => {
      expect(() => runScript('--ttl -1')).toThrow();
      expect(() => runScript('--ttl abc')).toThrow();
    });
  });
});

// ---------------------------------------------------------------------------
// Cluster detection function tests (pure bash, no ClickHouse needed)
// ---------------------------------------------------------------------------

describe('cluster detection functions', { tags: ['setup'] }, () => {
  /**
   * Source setup_sampling.sh in function-only mode and call a cluster
   * selection function with the given input.
   */
  function callBashFn(fnName: string, input: string): string {
    const cmd = `SETUP_SAMPLING_SOURCE_ONLY=1 source "${SCRIPT_PATH}" && echo "$(${fnName} "${input}")"`;
    return execSync(`bash -c '${cmd}'`, { encoding: 'utf-8' }).trim();
  }

  // --- Typical 3-shard × 2-replica + all-sharded ---
  describe('typical topology (3s×2r + all-sharded)', () => {
    const clusters = 'mycluster\t6\t3\t2\nall-sharded\t6\t6\t1';

    it('selects replicated cluster', () => {
      expect(callBashFn('select_replicated_cluster', clusters)).toBe('mycluster');
    });

    it('selects sharded cluster', () => {
      expect(callBashFn('select_sharded_cluster', clusters)).toBe('all-sharded');
    });
  });

  // --- Altinity topology (all 2-node) ---
  describe('Altinity topology (all-replicated 1s×2r + all-sharded 2s×1r)', () => {
    const clusters = 'dev\t2\t2\t1\nall-sharded\t2\t2\t1\nall-clusters\t2\t2\t1\nall-replicated\t2\t1\t2';

    it('selects replicated cluster', () => {
      expect(callBashFn('select_replicated_cluster', clusters)).toBe('all-replicated');
    });

    it('selects sharded cluster', () => {
      expect(callBashFn('select_sharded_cluster', clusters)).toBe('dev');
    });
  });

  // --- Single replicated cluster (1 shard, 3 replicas) ---
  describe('single replicated cluster (1s×3r)', () => {
    const clusters = 'prod\t3\t1\t3';

    it('selects replicated cluster', () => {
      expect(callBashFn('select_replicated_cluster', clusters)).toBe('prod');
    });

    it('returns empty for sharded cluster', () => {
      expect(callBashFn('select_sharded_cluster', clusters)).toBe('');
    });
  });

  // --- Single all-sharded cluster ---
  describe('single all-sharded cluster (3s×1r)', () => {
    const clusters = 'shards\t3\t3\t1';

    it('returns empty for replicated cluster', () => {
      expect(callBashFn('select_replicated_cluster', clusters)).toBe('');
    });

    it('selects sharded cluster', () => {
      expect(callBashFn('select_sharded_cluster', clusters)).toBe('shards');
    });
  });

  // --- Mixed large topology ---
  describe('mixed topology (2s×3r preferred over 6s×1r)', () => {
    const clusters = 'big-sharded\t6\t6\t1\nreplicated\t6\t2\t3\nsmall\t2\t2\t1';

    it('selects replicated cluster', () => {
      expect(callBashFn('select_replicated_cluster', clusters)).toBe('replicated');
    });

    it('selects sharded cluster', () => {
      expect(callBashFn('select_sharded_cluster', clusters)).toBe('big-sharded');
    });
  });

  // --- Only all-sharded clusters (no replicated) ---
  describe('only all-sharded clusters', () => {
    const clusters = 'a\t4\t4\t1\nb\t2\t2\t1';

    it('returns empty for replicated cluster', () => {
      expect(callBashFn('select_replicated_cluster', clusters)).toBe('');
    });

    it('selects first sharded cluster', () => {
      expect(callBashFn('select_sharded_cluster', clusters)).toBe('a');
    });
  });
});
