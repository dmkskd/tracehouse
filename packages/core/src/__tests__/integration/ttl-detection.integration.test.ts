/**
 * Integration tests for TTL detection from system.tables.create_table_query.
 *
 * Verifies that parseTTL correctly extracts TTL from real ClickHouse DDL
 * by creating tables with various TTL configurations, then reading back
 * the DDL from system.tables and parsing it.
 *
 * Also tests ALTER TABLE MODIFY TTL to prove we detect post-creation changes.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import {
  startClickHouse,
  stopClickHouse,
  type TestClickHouseContext,
} from './setup/clickhouse-container.js';
import { parseTTL } from '../../utils/ttl-parser.js';

const CONTAINER_TIMEOUT = 120_000;
const TEST_DB = 'test_ttl_detection';

describe('TTL detection from system.tables', () => {
  let ctx: TestClickHouseContext;

  beforeAll(async () => {
    ctx = await startClickHouse();
    await ctx.client.command({ query: `CREATE DATABASE IF NOT EXISTS ${TEST_DB}` });
  }, CONTAINER_TIMEOUT);

  afterAll(async () => {
    if (ctx) {
      if (!ctx.keepData) {
        await ctx.client.command({ query: `DROP DATABASE IF EXISTS ${TEST_DB}` });
      }
      await stopClickHouse(ctx);
    }
  }, 30_000);

  async function getCreateTableQuery(table: string): Promise<string> {
    const rows = await ctx.rawAdapter.executeQuery<{ create_table_query: string }>(
      `SELECT create_table_query FROM system.tables WHERE database = '${TEST_DB}' AND name = '${table}'`
    );
    return rows[0]?.create_table_query ?? '';
  }

  // ── toIntervalDay ──

  it('detects TTL with toIntervalDay(30)', async () => {
    await ctx.client.command({
      query: `
        CREATE TABLE ${TEST_DB}.ttl_30d (
          event_date Date,
          event_time DateTime DEFAULT now(),
          data String
        ) ENGINE = MergeTree()
        ORDER BY event_time
        TTL event_date + toIntervalDay(30) DELETE
      `,
    });

    const ddl = await getCreateTableQuery('ttl_30d');
    expect(parseTTL(ddl)).toBe('30 days');
  });

  // ── toIntervalDay(7) ──

  it('detects TTL with toIntervalDay(7)', async () => {
    await ctx.client.command({
      query: `
        CREATE TABLE ${TEST_DB}.ttl_7d (
          event_date Date,
          event_time DateTime DEFAULT now(),
          data String
        ) ENGINE = MergeTree()
        ORDER BY event_time
        TTL event_date + toIntervalDay(7) DELETE
      `,
    });

    const ddl = await getCreateTableQuery('ttl_7d');
    expect(parseTTL(ddl)).toBe('7 days');
  });

  // ── toIntervalMonth ──

  it('detects TTL with toIntervalMonth(3)', async () => {
    await ctx.client.command({
      query: `
        CREATE TABLE ${TEST_DB}.ttl_3m (
          event_date Date,
          event_time DateTime DEFAULT now(),
          data String
        ) ENGINE = MergeTree()
        ORDER BY event_time
        TTL event_date + toIntervalMonth(3) DELETE
      `,
    });

    const ddl = await getCreateTableQuery('ttl_3m');
    expect(parseTTL(ddl)).toBe('3 months');
  });

  // ── toIntervalHour ──

  it('detects TTL with toIntervalHour(24)', async () => {
    await ctx.client.command({
      query: `
        CREATE TABLE ${TEST_DB}.ttl_24h (
          event_date Date,
          event_time DateTime DEFAULT now(),
          data String
        ) ENGINE = MergeTree()
        ORDER BY event_time
        TTL event_time + toIntervalHour(24) DELETE
      `,
    });

    const ddl = await getCreateTableQuery('ttl_24h');
    expect(parseTTL(ddl)).toBe('24 hours');
  });

  // ── INTERVAL N DAY syntax ──

  it('detects TTL with INTERVAL 14 DAY syntax', async () => {
    await ctx.client.command({
      query: `
        CREATE TABLE ${TEST_DB}.ttl_interval_14d (
          event_date Date,
          event_time DateTime DEFAULT now(),
          data String
        ) ENGINE = MergeTree()
        ORDER BY event_time
        TTL event_date + INTERVAL 14 DAY DELETE
      `,
    });

    const ddl = await getCreateTableQuery('ttl_interval_14d');
    // ClickHouse may normalize to toIntervalDay() internally
    const result = parseTTL(ddl);
    expect(result).toBe('14 days');
  });

  // ── No TTL ──

  it('returns null for table without TTL (unlimited retention)', async () => {
    await ctx.client.command({
      query: `
        CREATE TABLE ${TEST_DB}.no_ttl (
          event_date Date,
          event_time DateTime DEFAULT now(),
          data String
        ) ENGINE = MergeTree()
        ORDER BY event_time
      `,
    });

    const ddl = await getCreateTableQuery('no_ttl');
    expect(parseTTL(ddl)).toBeNull();
  });

  // ── ALTER TABLE MODIFY TTL ──

  it('detects TTL change after ALTER TABLE MODIFY TTL', async () => {
    // Start with 30 days
    await ctx.client.command({
      query: `
        CREATE TABLE ${TEST_DB}.ttl_alter (
          event_date Date,
          event_time DateTime DEFAULT now(),
          data String
        ) ENGINE = MergeTree()
        ORDER BY event_time
        TTL event_date + toIntervalDay(30) DELETE
      `,
    });

    const ddlBefore = await getCreateTableQuery('ttl_alter');
    expect(parseTTL(ddlBefore)).toBe('30 days');

    // Change to 7 days
    await ctx.client.command({
      query: `ALTER TABLE ${TEST_DB}.ttl_alter MODIFY TTL event_date + toIntervalDay(7) DELETE`,
    });

    const ddlAfter = await getCreateTableQuery('ttl_alter');
    expect(parseTTL(ddlAfter)).toBe('7 days');
  });

  // ── ALTER TABLE to add TTL on a table that didn't have one ──

  it('detects TTL added to a table that had none', async () => {
    await ctx.client.command({
      query: `
        CREATE TABLE ${TEST_DB}.ttl_add (
          event_date Date,
          event_time DateTime DEFAULT now(),
          data String
        ) ENGINE = MergeTree()
        ORDER BY event_time
      `,
    });

    // Initially no TTL
    const ddlBefore = await getCreateTableQuery('ttl_add');
    expect(parseTTL(ddlBefore)).toBeNull();

    // Add TTL
    await ctx.client.command({
      query: `ALTER TABLE ${TEST_DB}.ttl_add MODIFY TTL event_date + toIntervalDay(14) DELETE`,
    });

    const ddlAfter = await getCreateTableQuery('ttl_add');
    expect(parseTTL(ddlAfter)).toBe('14 days');
  });

  // ── Real system tables ──

  it('can parse TTL from all system MergeTree tables without errors', async () => {
    const rows = await ctx.rawAdapter.executeQuery<{ name: string; create_table_query: string }>(
      `SELECT name, create_table_query FROM system.tables WHERE database = 'system' AND engine LIKE '%MergeTree%'`
    );

    for (const row of rows) {
      // Should never throw, regardless of DDL shape
      const result = parseTTL(row.create_table_query);
      expect(result === null || typeof result === 'string').toBe(true);
    }
  });

  it('detects TTL change on a real system.query_log via ALTER TABLE', async () => {
    // Read the original DDL so we can restore it
    const originalRows = await ctx.rawAdapter.executeQuery<{ create_table_query: string }>(
      `SELECT create_table_query FROM system.tables WHERE database = 'system' AND name = 'query_log'`
    );
    if (originalRows.length === 0) return; // query_log doesn't exist in this config

    const originalDDL = originalRows[0].create_table_query;
    const originalTTL = parseTTL(originalDDL);

    try {
      // Set a known TTL on the real system.query_log
      await ctx.client.command({
        query: `ALTER TABLE system.query_log MODIFY TTL event_date + toIntervalDay(42) DELETE`,
      });

      const modifiedRows = await ctx.rawAdapter.executeQuery<{ create_table_query: string }>(
        `SELECT create_table_query FROM system.tables WHERE database = 'system' AND name = 'query_log'`
      );

      expect(parseTTL(modifiedRows[0].create_table_query)).toBe('42 days');
    } finally {
      // Restore original state
      if (originalTTL) {
        // Had a TTL — restore it. We need to reconstruct the TTL clause from the original DDL.
        const ttlClauseMatch = originalDDL.match(/\bTTL\s+(.*?)(?:\s+SETTINGS\b|$)/i);
        if (ttlClauseMatch) {
          await ctx.client.command({
            query: `ALTER TABLE system.query_log MODIFY TTL ${ttlClauseMatch[1]}`,
          });
        }
      } else {
        // Had no TTL — ClickHouse doesn't support REMOVE TTL, so set a very long one
        // to approximate the original. In a testcontainer this doesn't matter since
        // it'll be destroyed anyway.
        await ctx.client.command({
          query: `ALTER TABLE system.query_log MODIFY TTL event_date + toIntervalYear(100) DELETE`,
        });
      }
    }
  });
});
