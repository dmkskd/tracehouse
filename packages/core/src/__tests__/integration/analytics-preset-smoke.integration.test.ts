/**
 * Smoke tests for all preset analytics queries.
 *
 * Runs every query against a bare ClickHouse container to catch SQL syntax
 * errors, missing columns, and bad joins. No test data setup — queries are
 * expected to return 0+ rows on a fresh instance.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import {
  startClickHouse,
  stopClickHouse,
  type TestClickHouseContext,
} from './setup/clickhouse-container.js';
import { runTracehouseSetup } from './setup/tracehouse-setup.js';
import { RAW_QUERIES } from '@frontend-queries/index';
import { resolveTimeRange, resolveDrillParams } from '@frontend-analytics/templateResolution';
import { parseQueryMetadata } from '@frontend-analytics/metaLanguage';
import {
  isClickHouseVersionAtLeast,
  parseClickHouseVersion,
} from '../../utils/clickhouse-version.js';

const CONTAINER_TIMEOUT = 120_000;
const CLUSTER_SYSTEM_TABLE_RE = /\{\{cluster_aware:system\.([A-Za-z0-9_]+)\}\}/g;
const CLUSTER_TRACEHOUSE_TABLE_RE =
  /\{\{cluster_aware:tracehouse\.([A-Za-z0-9_]+)\}\}/g;

function referencedSystemTables(sql: string): string[] {
  return [...sql.matchAll(CLUSTER_SYSTEM_TABLE_RE)].map(match => match[1]);
}

function referencedTracehouseTables(sql: string): string[] {
  return [...sql.matchAll(CLUSTER_TRACEHOUSE_TABLE_RE)].map(match => match[1]);
}

// Queries that need infrastructure not available in a basic single-node container.
const KNOWN_FAILURES = new Set([
  'Part Errors',                     // error column is UInt16, query compares to ''
  'ZooKeeper Operations',            // ProfileEvent_ZooKeeperWatch removed in CH 26.1
  'ZooKeeper Wait Time',             // requires ZK tables
  'ZooKeeper Sessions & Exceptions', // requires ZK tables
  'Keeper Connection Status',        // system.zookeeper_connection only exists with Keeper configured
]);

describe('Preset analytics query smoke tests', { tags: ['analytics'] }, () => {
  let ctx: TestClickHouseContext;
  let availableSystemTables: Set<string>;
  let availableTracehouseTables: Set<string>;
  let serverVersion: string;

  beforeAll(async () => {
    ctx = await startClickHouse({ withKeeper: true });

    // Minimal activity so system.part_log / system.query_log have entries.
    await ctx.client.command({ query: `CREATE DATABASE IF NOT EXISTS smoke_test` });
    await ctx.client.command({
      query: `CREATE TABLE IF NOT EXISTS smoke_test.t (id UInt64) ENGINE = MergeTree() ORDER BY id`,
    });
    for (let i = 0; i < 3; i++) {
      await ctx.client.command({
        query: `INSERT INTO smoke_test.t SELECT number + ${i * 100} FROM numbers(100)`,
      });
    }
    await ctx.client.command({ query: `OPTIMIZE TABLE smoke_test.t FINAL` });

    const versionRows = await ctx.rawAdapter.executeQuery<{ version: string }>(
      'SELECT version() AS version',
    );
    serverVersion = versionRows[0]?.version ?? '';

    // APPEND refreshable materialized views were added in ClickHouse 24.9.
    // Older versions can still exercise every preset that does not depend on
    // TraceHouse's sampled history tables.
    if (isClickHouseVersionAtLeast(serverVersion, 24, 9)) {
      await runTracehouseSetup(ctx);
    }

    await ctx.client.command({ query: `SYSTEM FLUSH LOGS` });

    const tableRows = await ctx.rawAdapter.executeQuery<{ name: string }>(`
      SELECT name
      FROM system.tables
      WHERE database = 'system'
    `);
    availableSystemTables = new Set(tableRows.map(row => row.name));
    const tracehouseTableRows = await ctx.rawAdapter.executeQuery<{ name: string }>(`
      SELECT name
      FROM system.tables
      WHERE database = 'tracehouse'
    `);
    availableTracehouseTables = new Set(tracehouseTableRows.map(row => row.name));
  }, CONTAINER_TIMEOUT);

  afterAll(async () => {
    if (ctx) {
      await ctx.client.command({ query: `DROP DATABASE IF EXISTS smoke_test` });
      await ctx.client.command({ query: `DROP DATABASE IF EXISTS tracehouse` });
      await stopClickHouse(ctx);
    }
  }, 30_000);

  for (const rawSql of RAW_QUERIES) {
    const parsed = parseQueryMetadata(rawSql, 'preset');
    const title = parsed?.name ?? '(untitled)';

    if (KNOWN_FAILURES.has(title)) {
      it.skip(`query: ${title} (known failure)`, () => {});
      continue;
    }

    it(`query: ${title}`, async ({ skip }) => {
      const minimumVersion = parsed?.directives.requires?.clickhouseMinVersion;
      if (minimumVersion) {
        const parsedMinimum = parseClickHouseVersion(minimumVersion);
        if (
          parsedMinimum
          && !isClickHouseVersionAtLeast(
            serverVersion,
            parsedMinimum.major,
            parsedMinimum.minor,
            parsedMinimum.patch,
          )
        ) {
          skip(`Requires ClickHouse >= ${minimumVersion}; running ${serverVersion}`);
        }
      }

      const missingSystemTables = referencedSystemTables(rawSql)
        .filter(table => !availableSystemTables.has(table));
      if (missingSystemTables.length > 0) {
        skip(`Unavailable system tables: ${missingSystemTables.join(', ')}`);
      }

      const missingTracehouseTables = referencedTracehouseTables(rawSql)
        .filter(table => !availableTracehouseTables.has(table));
      if (missingTracehouseTables.length > 0) {
        skip(`Unavailable TraceHouse tables: ${missingTracehouseTables.join(', ')}`);
      }

      let sql = resolveTimeRange(rawSql, parsed?.directives.meta?.interval ?? '1 HOUR');
      sql = resolveDrillParams(sql, {});
      const rows = await ctx.adapter.executeQuery(sql);
      expect(Array.isArray(rows)).toBe(true);
    });
  }
});
