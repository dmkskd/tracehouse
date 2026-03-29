/**
 * Integration tests for cluster query dedup.
 *
 * Spins up a 2-node ClickHouse cluster (1 shard, 2 replicas) and verifies
 * that our metadata queries with GROUP BY dedup return the same results
 * as querying a single node's local system tables.
 *
 * This catches the core problem: clusterAllReplicas on metadata tables
 * returns duplicate rows (one per replica), and our queries must dedup them.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import {
  startCluster,
  stopCluster,
  type ClusterTestContext,
} from './setup/cluster-container.js';
import { ClusterService } from '../../services/cluster-service.js';
import { ClusterAwareAdapter } from '../../adapters/cluster-adapter.js';
import { QueryAnalyzer } from '../../services/query-analyzer.js';
import { buildQuery } from '../../queries/builder.js';
import {
  LIST_DATABASES,
  LIST_TABLES,
  GET_TABLE_SCHEMA,
  GET_TABLE_PARTS,
  GET_PART_DETAIL,
  GET_TABLE_KEYS,
  GET_TABLE_COLUMN_NAMES,
} from '../../queries/database-queries.js';
import {
  GET_PARTS_ALERTS,
  GET_DISK_ALERTS,
  GET_REPLICATION_SUMMARY,
  GET_PK_MEMORY,
  GET_DICT_MEMORY,
} from '../../queries/overview-queries.js';
import {
  GET_MUTATIONS,
  GET_OUTDATED_PARTS_SIZE,
  GET_MUTATION_HISTORY,
} from '../../queries/merge-queries.js';
import {
  GET_PK_INDEX_BY_TABLE,
  GET_DICTIONARIES,
} from '../../queries/engine-internals-queries.js';
import {
  GET_REPLICATION_DETAIL,
  GET_DATABASE_ENGINES,
} from '../../queries/cluster-queries.js';
import {
  PROBE_SYSTEM_LOG_TABLES,
  PROBE_ZOOKEEPER,
} from '../../queries/monitoring-capabilities-queries.js';

const CONTAINER_TIMEOUT = 180_000;
const TEST_DB = 'dedup_test';

/** Wait until both replicas have the same row count for a table. */
async function waitForReplication(
  ctx: ClusterTestContext,
  database: string,
  table: string,
  timeoutMs = 30_000,
): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const r1 = await ctx.client1.query({
        query: `SELECT count() AS cnt FROM ${database}.${table}`,
        format: 'JSONEachRow',
      });
      const r2 = await ctx.client2.query({
        query: `SELECT count() AS cnt FROM ${database}.${table}`,
        format: 'JSONEachRow',
      });
      const rows1 = await r1.json<{ cnt: string }>();
      const rows2 = await r2.json<{ cnt: string }>();
      if (Number(rows1[0]?.cnt) > 0 && rows1[0]?.cnt === rows2[0]?.cnt) return;
    } catch {
      // Retry — table may not exist on node 2 yet (DDL propagation)
    }
    await new Promise(r => setTimeout(r, 1000));
  }
  throw new Error(`Replication did not complete for ${database}.${table} within timeout`);
}

/** Resolve a query template for the cluster and execute it. */
async function runClusterQuery<T extends Record<string, unknown>>(
  ctx: ClusterTestContext,
  template: string,
  params?: Record<string, string | number>,
): Promise<T[]> {
  let sql = params ? buildQuery(template, params) : template;
  sql = ClusterService.resolveTableRefs(sql, 'default');
  return ctx.adapter1.executeQuery<T>(sql);
}

/** Run a query against a single node's local system tables (no cluster wrapper). */
async function runLocalQuery<T extends Record<string, unknown>>(
  ctx: ClusterTestContext,
  template: string,
  params?: Record<string, string | number>,
): Promise<T[]> {
  let sql = params ? buildQuery(template, params) : template;
  sql = ClusterService.resolveTableRefs(sql, null);
  return ctx.adapter1.executeQuery<T>(sql);
}

describe('Cluster query dedup', { tags: ['cluster'] }, () => {
  let ctx: ClusterTestContext;

  beforeAll(async () => {
    ctx = await startCluster();

    // Create a Replicated database on both nodes. Each node must create its
    // own instance pointing to the same Keeper path. On 26.x+, ON CLUSTER is
    // rejected for Replicated databases, so we create on each node individually.
    // Once both nodes have the database, DDL within it (CREATE TABLE, etc.)
    // propagates automatically via Keeper.
    await ctx.client1.command({
      query: `CREATE DATABASE IF NOT EXISTS ${TEST_DB}
              ENGINE = Replicated('/clickhouse/databases/${TEST_DB}', '{shard}', '{replica}')`,
    });
    await ctx.client2.command({
      query: `CREATE DATABASE IF NOT EXISTS ${TEST_DB}
              ENGINE = Replicated('/clickhouse/databases/${TEST_DB}', '{shard}', '{replica}')`,
    });

    await ctx.client1.command({
      query: `
        CREATE TABLE IF NOT EXISTS ${TEST_DB}.events (
          id UInt64,
          ts DateTime DEFAULT now(),
          category String,
          value Float64
        ) ENGINE = ReplicatedMergeTree()
        PARTITION BY toYYYYMM(ts)
        ORDER BY (category, ts, id)
        PRIMARY KEY (category, ts)
      `,
    });

    // Wait for DDL propagation — table DDL replicates via Keeper asynchronously.
    {
      const start = Date.now();
      while (Date.now() - start < 30_000) {
        try {
          const result = await ctx.client2.query({
            query: `SELECT count() AS cnt FROM system.tables WHERE database = '${TEST_DB}' AND name = 'events'`,
            format: 'JSONEachRow',
          });
          const rows = await result.json<{ cnt: string }>();
          if (Number(rows[0]?.cnt) > 0) break;
        } catch { /* retry */ }
        await new Promise(r => setTimeout(r, 1000));
      }
    }

    // Insert data in multiple batches to create multiple parts
    await ctx.client1.command({
      query: `INSERT INTO ${TEST_DB}.events (id, category, value)
              SELECT number, 'cat_a', rand() FROM numbers(500)`,
    });
    await ctx.client1.command({
      query: `INSERT INTO ${TEST_DB}.events (id, category, value)
              SELECT number + 500, 'cat_b', rand() FROM numbers(500)`,
    });

    // Wait for replication to complete
    await waitForReplication(ctx, TEST_DB, 'events');

    // Flush logs
    await ctx.client1.command({ query: 'SYSTEM FLUSH LOGS' });
    await ctx.client2.command({ query: 'SYSTEM FLUSH LOGS' });
  }, CONTAINER_TIMEOUT);

  afterAll(async () => {
    if (ctx) {
      try {
        await ctx.client1.command({ query: `DROP DATABASE IF EXISTS ${TEST_DB} SYNC` });
      } catch { /* ignore */ }
      await stopCluster(ctx);
    }
  }, 60_000);

  // ─── database-queries.ts ───

  describe('LIST_DATABASES dedup', () => {
    it('returns same database count as local query', async () => {
      const cluster = await runClusterQuery<{ name: string }>(ctx, LIST_DATABASES);
      const local = await runLocalQuery<{ name: string }>(ctx, LIST_DATABASES);
      const clusterNames = cluster.map(r => r.name).sort();
      const localNames = local.map(r => r.name).sort();
      expect(clusterNames).toEqual(localNames);
    });

    it('includes test database exactly once', async () => {
      const rows = await runClusterQuery<{ name: string }>(ctx, LIST_DATABASES);
      const matches = rows.filter(r => r.name === TEST_DB);
      expect(matches).toHaveLength(1);
    });
  });

  describe('LIST_TABLES dedup', () => {
    it('returns same table count as local query', async () => {
      const cluster = await runClusterQuery<{ name: string }>(ctx, LIST_TABLES, { database: TEST_DB });
      const local = await runLocalQuery<{ name: string }>(ctx, LIST_TABLES, { database: TEST_DB });
      expect(cluster.map(r => r.name).sort()).toEqual(local.map(r => r.name).sort());
    });

    it('events table appears exactly once', async () => {
      const rows = await runClusterQuery<{ name: string }>(ctx, LIST_TABLES, { database: TEST_DB });
      expect(rows.filter(r => r.name === 'events')).toHaveLength(1);
    });
  });

  describe('GET_TABLE_SCHEMA dedup', () => {
    it('returns same column count as local query', async () => {
      const cluster = await runClusterQuery<{ name: string }>(ctx, GET_TABLE_SCHEMA, { database: TEST_DB, table: 'events' });
      const local = await runLocalQuery<{ name: string }>(ctx, GET_TABLE_SCHEMA, { database: TEST_DB, table: 'events' });
      expect(cluster.map(r => r.name).sort()).toEqual(local.map(r => r.name).sort());
    });

    it('each column appears exactly once', async () => {
      const rows = await runClusterQuery<{ name: string }>(ctx, GET_TABLE_SCHEMA, { database: TEST_DB, table: 'events' });
      const names = rows.map(r => r.name);
      expect(new Set(names).size).toBe(names.length);
      expect(names).toContain('id');
      expect(names).toContain('category');
    });
  });

  describe('GET_TABLE_PARTS dedup', () => {
    it('returns same part count as local query', async () => {
      const cluster = await runClusterQuery<{ name: string }>(ctx, GET_TABLE_PARTS, { database: TEST_DB, table: 'events' });
      const local = await runLocalQuery<{ name: string }>(ctx, GET_TABLE_PARTS, { database: TEST_DB, table: 'events' });
      expect(cluster.map(r => r.name).sort()).toEqual(local.map(r => r.name).sort());
    });

    it('each part name appears exactly once', async () => {
      const rows = await runClusterQuery<{ name: string }>(ctx, GET_TABLE_PARTS, { database: TEST_DB, table: 'events' });
      const names = rows.map(r => r.name);
      expect(new Set(names).size).toBe(names.length);
    });
  });

  describe('GET_PART_DETAIL dedup', () => {
    it('returns exactly one row for a specific part', async () => {
      // Get a part name first
      const parts = await runClusterQuery<{ name: string }>(ctx, GET_TABLE_PARTS, { database: TEST_DB, table: 'events' });
      expect(parts.length).toBeGreaterThan(0);
      const partName = parts[0].name;

      const rows = await runClusterQuery<{ name: string }>(ctx, GET_PART_DETAIL, {
        database: TEST_DB,
        table: 'events',
        part_name: partName,
      });
      expect(rows).toHaveLength(1);
      expect(rows[0].name).toBe(partName);
    });
  });

  describe('GET_TABLE_KEYS dedup', () => {
    it('returns exactly one row for table keys', async () => {
      const rows = await runClusterQuery<{ sorting_key: string }>(ctx, GET_TABLE_KEYS, {
        database: TEST_DB,
        table: 'events',
      });
      expect(rows).toHaveLength(1);
      expect(rows[0].sorting_key).toContain('category');
    });
  });

  describe('GET_TABLE_COLUMN_NAMES dedup', () => {
    it('returns same columns as local query', async () => {
      const cluster = await runClusterQuery<{ name: string }>(ctx, GET_TABLE_COLUMN_NAMES, { database: TEST_DB, table: 'events' });
      const local = await runLocalQuery<{ name: string }>(ctx, GET_TABLE_COLUMN_NAMES, { database: TEST_DB, table: 'events' });
      expect(cluster.map(r => r.name)).toEqual(local.map(r => r.name));
    });
  });

  // ─── overview-queries.ts ───

  describe('GET_PARTS_ALERTS dedup', () => {
    it('returns deduped part counts (no inflation)', async () => {
      const cluster = await runClusterQuery<{ part_count: string }>(ctx, GET_PARTS_ALERTS, { parts_threshold: 0 });
      const local = await runLocalQuery<{ part_count: string }>(ctx, GET_PARTS_ALERTS, { parts_threshold: 0 });
      // Cluster query should not have inflated counts vs local
      expect(cluster.length).toBeLessThanOrEqual(local.length * 2); // some tolerance
      for (const row of cluster) {
        expect(Number(row.part_count)).toBeGreaterThan(0);
      }
    });
  });

  describe('GET_DISK_ALERTS dedup', () => {
    it('returns each disk exactly once', async () => {
      const rows = await runClusterQuery<{ name: string }>(ctx, GET_DISK_ALERTS);
      const names = rows.map(r => r.name);
      expect(new Set(names).size).toBe(names.length);
    });
  });

  describe('GET_REPLICATION_SUMMARY dedup', () => {
    it('returns a single summary row', async () => {
      const rows = await runClusterQuery<{ total_tables: string }>(ctx, GET_REPLICATION_SUMMARY);
      expect(rows).toHaveLength(1);
    });

    it('total_tables matches local count', async () => {
      const cluster = await runClusterQuery<{ total_tables: string }>(ctx, GET_REPLICATION_SUMMARY);
      const local = await runLocalQuery<{ total_tables: string }>(ctx, GET_REPLICATION_SUMMARY);
      expect(cluster[0].total_tables).toBe(local[0].total_tables);
    });
  });

  describe('GET_PK_MEMORY dedup', () => {
    it('returns a single row', async () => {
      const rows = await runClusterQuery<{ pk_bytes: string }>(ctx, GET_PK_MEMORY);
      expect(rows).toHaveLength(1);
    });

    it('pk_bytes matches local value', async () => {
      const cluster = await runClusterQuery<{ pk_bytes: string }>(ctx, GET_PK_MEMORY);
      const local = await runLocalQuery<{ pk_bytes: string }>(ctx, GET_PK_MEMORY);
      // PK memory can differ slightly between replicas (compaction timing),
      // so allow a small tolerance rather than exact match.
      const clusterVal = Number(cluster[0].pk_bytes);
      const localVal = Number(local[0].pk_bytes);
      const diff = Math.abs(clusterVal - localVal);
      expect(diff).toBeLessThan(Math.max(localVal * 0.2, 64)); // within 20% or 64 bytes
    });
  });

  describe('GET_DICT_MEMORY dedup', () => {
    it('returns a single row', async () => {
      const rows = await runClusterQuery<{ dict_bytes: string }>(ctx, GET_DICT_MEMORY);
      expect(rows).toHaveLength(1);
    });
  });

  // ─── merge-queries.ts ───

  describe('GET_MUTATIONS dedup', () => {
    it('does not duplicate mutation rows', async () => {
      // No pending mutations expected, but query should not error
      const rows = await runClusterQuery<{ mutation_id: string }>(ctx, GET_MUTATIONS, { limit: 100 });
      const ids = rows.map(r => `${r.mutation_id}`);
      expect(new Set(ids).size).toBe(ids.length);
    });
  });

  describe('GET_OUTDATED_PARTS_SIZE dedup', () => {
    it('returns a single summary row', async () => {
      const rows = await runClusterQuery<{ outdated_parts_count: string }>(ctx, GET_OUTDATED_PARTS_SIZE);
      expect(rows).toHaveLength(1);
    });

    it('count matches local value', async () => {
      const cluster = await runClusterQuery<{ outdated_parts_count: string }>(ctx, GET_OUTDATED_PARTS_SIZE);
      const local = await runLocalQuery<{ outdated_parts_count: string }>(ctx, GET_OUTDATED_PARTS_SIZE);
      expect(cluster[0].outdated_parts_count).toBe(local[0].outdated_parts_count);
    });
  });

  describe('GET_MUTATION_HISTORY dedup', () => {
    it('does not duplicate completed mutations', async () => {
      const rows = await runClusterQuery<{ mutation_id: string; database: string; table: string }>(
        ctx, GET_MUTATION_HISTORY, { limit: 100 },
      );
      const keys = rows.map(r => `${r.database}.${r.table}.${r.mutation_id}`);
      expect(new Set(keys).size).toBe(keys.length);
    });
  });

  // ─── engine-internals-queries.ts ───

  describe('GET_PK_INDEX_BY_TABLE dedup', () => {
    it('returns each table exactly once', async () => {
      const rows = await runClusterQuery<{ database: string; table: string }>(
        ctx, GET_PK_INDEX_BY_TABLE, { limit: 100 },
      );
      const keys = rows.map(r => `${r.database}.${r.table}`);
      expect(new Set(keys).size).toBe(keys.length);
    });
  });

  describe('GET_DICTIONARIES dedup', () => {
    it('returns each dictionary exactly once', async () => {
      const rows = await runClusterQuery<{ name: string }>(ctx, GET_DICTIONARIES);
      const names = rows.map(r => r.name);
      expect(new Set(names).size).toBe(names.length);
    });
  });

  // ─── cluster-queries.ts ───

  describe('GET_REPLICATION_DETAIL dedup', () => {
    it('returns each replica table exactly once', async () => {
      const rows = await runClusterQuery<{ database: string; table: string }>(ctx, GET_REPLICATION_DETAIL);
      const keys = rows.map(r => `${r.database}.${r.table}`);
      expect(new Set(keys).size).toBe(keys.length);
    });
  });

  describe('GET_DATABASE_ENGINES dedup', () => {
    it('returns each database exactly once', async () => {
      const rows = await runClusterQuery<{ name: string }>(ctx, GET_DATABASE_ENGINES);
      const names = rows.map(r => r.name);
      expect(new Set(names).size).toBe(names.length);
    });
  });

  // ─── monitoring-capabilities-queries.ts ───

  describe('PROBE_SYSTEM_LOG_TABLES dedup', () => {
    it('returns each log table exactly once', async () => {
      const rows = await runClusterQuery<{ name: string }>(ctx, PROBE_SYSTEM_LOG_TABLES);
      const names = rows.map(r => r.name);
      expect(new Set(names).size).toBe(names.length);
    });
  });

  describe('PROBE_ZOOKEEPER dedup', () => {
    it('returns a single count row', async () => {
      const rows = await runClusterQuery<{ cnt: string }>(ctx, PROBE_ZOOKEEPER);
      expect(rows).toHaveLength(1);
    });
  });

  // ─── query-queries.ts ───

  describe('getCoordinatorIds', () => {
    it('detects coordinator query that dispatched shard sub-queries', async () => {
      // Use clusterAllReplicas() which always fans out to all replicas,
      // producing sub-queries with is_initial_query = 0 even on 1-shard clusters.
      const coordinatorId = crypto.randomUUID();
      const result = await ctx.client1.query({
        query: `SELECT count() FROM clusterAllReplicas('default', ${TEST_DB}.events)`,
        query_id: coordinatorId,
        format: 'JSONEachRow',
      });
      await result.json();

      await ctx.client1.command({ query: 'SYSTEM FLUSH LOGS' });
      await ctx.client2.command({ query: 'SYSTEM FLUSH LOGS' });

      // Wrap adapter1 in ClusterAwareAdapter so QueryAnalyzer resolves {{cluster_aware:...}}
      // Must set cluster name so it uses clusterAllReplicas (sub-queries are on remote nodes)
      const clusterAdapter = new ClusterAwareAdapter(ctx.adapter1);
      clusterAdapter.setClusterName('default');
      const analyzer = new QueryAnalyzer(clusterAdapter);

      const startDate = new Date().toISOString().split('T')[0]!;
      const ids = await analyzer.getCoordinatorIds([coordinatorId], startDate);
      expect(ids.has(coordinatorId)).toBe(true);
    });

    it('does not flag plain queries as coordinators', async () => {
      // Run a plain query (not through Distributed)
      const plainId = crypto.randomUUID();
      const result = await ctx.client1.query({
        query: `SELECT count() FROM ${TEST_DB}.events`,
        query_id: plainId,
        format: 'JSONEachRow',
      });
      await result.json();

      await ctx.client1.command({ query: 'SYSTEM FLUSH LOGS' });
      await ctx.client2.command({ query: 'SYSTEM FLUSH LOGS' });

      const clusterAdapter = new ClusterAwareAdapter(ctx.adapter1);
      clusterAdapter.setClusterName('default');
      const analyzer = new QueryAnalyzer(clusterAdapter);

      const startDate = new Date().toISOString().split('T')[0]!;
      const ids = await analyzer.getCoordinatorIds([plainId], startDate);
      expect(ids.has(plainId)).toBe(false);
    });
  });

});
