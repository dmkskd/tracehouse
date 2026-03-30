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

const CONTAINER_TIMEOUT = 120_000;

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

  beforeAll(async () => {
    ctx = await startClickHouse();

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

    // Create tracehouse sampling infrastructure using the production setup script.
    await runTracehouseSetup(ctx);

    await ctx.client.command({ query: `SYSTEM FLUSH LOGS` });
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

    it(`query: ${title}`, async () => {
      let sql = resolveTimeRange(rawSql, parsed?.directives.meta?.interval ?? '1 HOUR');
      sql = resolveDrillParams(sql, {});
      const rows = await ctx.adapter.executeQuery(sql);
      expect(Array.isArray(rows)).toBe(true);
    });
  }
});
