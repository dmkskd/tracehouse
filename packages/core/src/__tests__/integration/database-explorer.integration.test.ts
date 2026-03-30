/**
 * Integration tests for DatabaseExplorer against a real ClickHouse instance.
 *
 * Creates a test database with tables, inserts data, and validates that
 * all DatabaseExplorer methods return correct results from real system tables.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { startClickHouse, stopClickHouse, type TestClickHouseContext } from './setup/clickhouse-container.js';
import { DatabaseExplorer } from '../../services/database-explorer.js';

const CONTAINER_TIMEOUT = 120_000;
const TEST_DB = 'explorer_test';

describe('DatabaseExplorer integration', { tags: ['storage'] }, () => {
  let ctx: TestClickHouseContext;
  let explorer: DatabaseExplorer;

  beforeAll(async () => {
    ctx = await startClickHouse();
    explorer = new DatabaseExplorer(ctx.adapter);

    // Create test database and tables
    await ctx.client.command({ query: `CREATE DATABASE IF NOT EXISTS ${TEST_DB}` });
    await ctx.client.command({
      query: `
        CREATE TABLE IF NOT EXISTS ${TEST_DB}.events (
          id UInt64,
          ts DateTime DEFAULT now(),
          category String,
          value Float64
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(ts)
        ORDER BY (category, ts, id)
        PRIMARY KEY (category, ts)
      `,
    });
    await ctx.client.command({
      query: `
        CREATE TABLE IF NOT EXISTS ${TEST_DB}.users (
          user_id UInt32,
          name String,
          created_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY user_id
      `,
    });

    // Insert data to create parts
    await ctx.client.command({
      query: `INSERT INTO ${TEST_DB}.events (id, category, value) SELECT number, 'cat_a', rand() FROM numbers(1000)`,
    });
    await ctx.client.command({
      query: `INSERT INTO ${TEST_DB}.users (user_id, name) SELECT number, concat('user_', toString(number)) FROM numbers(100)`,
    });

    // Flush logs so part_log has entries
    await ctx.client.command({ query: 'SYSTEM FLUSH LOGS' });
  }, CONTAINER_TIMEOUT);

  afterAll(async () => {
    if (ctx) {
      await ctx.client.command({ query: `DROP DATABASE IF EXISTS ${TEST_DB}` });
      await stopClickHouse(ctx);
    }
  }, 30_000);

  describe('listDatabases', () => {
    it('returns system, default, and our test database', async () => {
      const databases = await explorer.listDatabases();
      const names = databases.map(d => d.name);
      expect(names).toContain('system');
      expect(names).toContain('default');
      expect(names).toContain(TEST_DB);
    });

    it('returns engine info for each database', async () => {
      const databases = await explorer.listDatabases();
      for (const db of databases) {
        expect(db).toHaveProperty('name');
        expect(db).toHaveProperty('engine');
        expect(typeof db.name).toBe('string');
      }
    });

    it('includes table_count for test database', async () => {
      const databases = await explorer.listDatabases();
      const testDb = databases.find(d => d.name === TEST_DB);
      expect(testDb).toBeDefined();
      expect(testDb!.table_count).toBeGreaterThanOrEqual(2);
    });
  });

  describe('listTables', () => {
    it('returns tables in test database', async () => {
      const tables = await explorer.listTables(TEST_DB);
      const names = tables.map(t => t.name);
      expect(names).toContain('events');
      expect(names).toContain('users');
    });

    it('returns row counts and byte sizes', async () => {
      const tables = await explorer.listTables(TEST_DB);
      const events = tables.find(t => t.name === 'events');
      expect(events).toBeDefined();
      expect(events!.total_rows).toBeGreaterThanOrEqual(1000);
      expect(events!.total_bytes).toBeGreaterThan(0);
    });

    it('returns engine and key info', async () => {
      const tables = await explorer.listTables(TEST_DB);
      const events = tables.find(t => t.name === 'events');
      expect(events!.engine).toBe('MergeTree');
    });

    it('returns empty array for non-existent database', async () => {
      const tables = await explorer.listTables('nonexistent_db_xyz');
      expect(tables).toEqual([]);
    });
  });

  describe('getTableSchema', () => {
    it('returns column definitions for events table', async () => {
      const schema = await explorer.getTableSchema(TEST_DB, 'events');
      const colNames = schema.map(c => c.name);
      expect(colNames).toContain('id');
      expect(colNames).toContain('ts');
      expect(colNames).toContain('category');
      expect(colNames).toContain('value');
    });

    it('returns correct types', async () => {
      const schema = await explorer.getTableSchema(TEST_DB, 'events');
      const idCol = schema.find(c => c.name === 'id');
      expect(idCol!.type).toBe('UInt64');
      const valCol = schema.find(c => c.name === 'value');
      expect(valCol!.type).toBe('Float64');
    });

    it('includes key membership flags', async () => {
      const schema = await explorer.getTableSchema(TEST_DB, 'events');
      const catCol = schema.find(c => c.name === 'category');
      // category is in sorting_key and primary_key
      expect(catCol!.is_in_sorting_key).toBe(true);
      expect(catCol!.is_in_primary_key).toBe(true);
    });
  });

  describe('getTableParts', () => {
    it('returns at least one active part for events table', async () => {
      const parts = await explorer.getTableParts(TEST_DB, 'events');
      expect(parts.length).toBeGreaterThanOrEqual(1);
    });

    it('parts have expected shape', async () => {
      const parts = await explorer.getTableParts(TEST_DB, 'events');
      const part = parts[0];
      expect(part).toHaveProperty('name');
      expect(part).toHaveProperty('rows');
      expect(part).toHaveProperty('bytes_on_disk');
      expect(part).toHaveProperty('level');
      expect(part.rows).toBeGreaterThan(0);
      expect(part.bytes_on_disk).toBeGreaterThan(0);
    });
  });

  describe('getPartDetail', () => {
    it('returns detail for an existing part', async () => {
      const parts = await explorer.getTableParts(TEST_DB, 'events');
      const partName = parts[0].name;

      const detail = await explorer.getPartDetail(TEST_DB, 'events', partName);
      expect(detail).not.toBeNull();
      expect(detail!.name).toBe(partName);
      expect(detail!.rows).toBeGreaterThan(0);
      expect(detail!.data_compressed_bytes).toBeGreaterThan(0);
    });

    it('returns null for non-existent part', async () => {
      const detail = await explorer.getPartDetail(TEST_DB, 'events', 'nonexistent_part_999');
      expect(detail).toBeNull();
    });

    it('includes column breakdown', async () => {
      const parts = await explorer.getTableParts(TEST_DB, 'events');
      const partName = parts[0].name;

      const detail = await explorer.getPartDetail(TEST_DB, 'events', partName);
      expect(detail!.columns.length).toBeGreaterThan(0);
      const col = detail!.columns[0];
      expect(col).toHaveProperty('column_name');
      expect(col).toHaveProperty('compressed_bytes');
      expect(col).toHaveProperty('uncompressed_bytes');
    });
  });

  describe('getPartLineage', () => {
    it('returns a lineage tree for an existing part', async () => {
      const parts = await explorer.getTableParts(TEST_DB, 'events');
      const partName = parts[0].name;

      const lineage = await explorer.getPartLineage(TEST_DB, 'events', partName);
      expect(lineage).toHaveProperty('root');
      expect(lineage).toHaveProperty('total_merges');
      expect(lineage).toHaveProperty('total_original_parts');
      expect(lineage.root.part_name).toBe(partName);
      expect(lineage.total_original_parts).toBeGreaterThanOrEqual(1);
    });
  });

  describe('getPartData', () => {
    it('returns sample rows from a part', async () => {
      const parts = await explorer.getTableParts(TEST_DB, 'events');
      const partName = parts[0].name;

      const data = await explorer.getPartData(TEST_DB, 'events', partName, 10);
      expect(data.columns.length).toBeGreaterThan(0);
      expect(data.columns).toContain('id');
      expect(data.returned_rows).toBeGreaterThan(0);
      expect(data.returned_rows).toBeLessThanOrEqual(10);
      expect(data.total_rows_in_part).toBeGreaterThan(0);
    });
  });

  describe('getPartColumnMinMax', () => {
    const DATE_TABLE = `${TEST_DB}.date_minmax`;

    beforeAll(async () => {
      await ctx.client.command({
        query: `
          CREATE TABLE IF NOT EXISTS ${DATE_TABLE} (
            id        UInt32,
            d         Date,
            dt        DateTime,
            dt64      DateTime64(3),
            n         UInt32
          ) ENGINE = MergeTree()
          ORDER BY id
        `,
      });
      // Insert two rows with known, non-epoch dates
      await ctx.client.command({
        query: `
          INSERT INTO ${DATE_TABLE} (id, d, dt, dt64, n) VALUES
            (1, '2024-03-01', '2024-03-01 08:00:00', '2024-03-01 08:00:00.000', 10),
            (2, '2024-06-30', '2024-06-30 20:00:00', '2024-06-30 20:00:00.500', 99)
        `,
      });
    });

    it('returns correctly formatted min/max for Date and DateTime columns', async () => {
      const parts = await explorer.getTableParts(TEST_DB, 'date_minmax');
      expect(parts.length).toBeGreaterThanOrEqual(1);
      const partName = parts[0].name;

      const columns = [
        { column_name: 'd',    type: 'Date' },
        { column_name: 'dt',   type: 'DateTime' },
        { column_name: 'dt64', type: 'DateTime64(3)' },
        { column_name: 'n',    type: 'UInt32' },
      ];

      const mm = await explorer.getPartColumnMinMax(TEST_DB, 'date_minmax', partName, columns);

      // Date column — expect "YYYY-MM-DD", not "1970-01-01"
      expect(mm.get('d')?.min).toBe('2024-03-01');
      expect(mm.get('d')?.max).toBe('2024-06-30');

      // DateTime column — expect "YYYY-MM-DD HH:MM:SS"
      expect(mm.get('dt')?.min).toBe('2024-03-01 08:00:00');
      expect(mm.get('dt')?.max).toBe('2024-06-30 20:00:00');

      // DateTime64 — toString includes sub-second precision
      expect(mm.get('dt64')?.min).toMatch(/^2024-03-01/);
      expect(mm.get('dt64')?.max).toMatch(/^2024-06-30/);

      // Numeric column still works
      expect(mm.get('n')?.min).toBe('10');
      expect(mm.get('n')?.max).toBe('99');
    });

    it('returns empty map for a non-existent part name', async () => {
      const columns = [{ column_name: 'd', type: 'Date' }];
      const mm = await explorer.getPartColumnMinMax(TEST_DB, 'date_minmax', 'nonexistent_part_0_0_0', columns);
      expect(mm.size).toBe(0);
    });
  });
});
