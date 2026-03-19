/**
 * Integration tests for buildMergeSamplesSQL + mapMergeSampleRow
 * from packages/core/src/queries/merge-sample-queries.ts.
 *
 * Creates a tracehouse.merges_history table in a real ClickHouse container,
 * seeds it with known synthetic data, runs the exact production SQL, maps
 * via the production row mapper, and asserts correctness.
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { startClickHouse, stopClickHouse, type TestClickHouseContext } from './setup/clickhouse-container.js';
import { buildMergeSamplesSQL, mapMergeSampleRow, type MergeSample } from '../../queries/merge-sample-queries.js';

const CONTAINER_TIMEOUT = 120_000;

interface MergeSampleRow {
  sample_time: string;
  database: string;
  table: string;
  result_part_name: string;
  partition_id: string;
  elapsed: number;
  progress: number;
  num_parts: number;
  is_mutation: number;
  merge_type: string;
  merge_algorithm: string;
  total_size_bytes_compressed: number;
  total_size_bytes_uncompressed: number;
  total_size_marks: number;
  rows_read: number;
  bytes_read_uncompressed: number;
  rows_written: number;
  bytes_written_uncompressed: number;
  columns_written: number;
  memory_usage: number;
  thread_id: number;
}

function buildInsertValues(rows: MergeSampleRow[]): string {
  return rows.map(r =>
    `('${r.sample_time}', '${r.database}', '${r.table}', '${r.result_part_name}', '${r.partition_id}', ` +
    `${r.elapsed}, ${r.progress}, ${r.num_parts}, ${r.is_mutation}, '${r.merge_type}', '${r.merge_algorithm}', ` +
    `${r.total_size_bytes_compressed}, ${r.total_size_bytes_uncompressed}, ${r.total_size_marks}, ` +
    `${r.rows_read}, ${r.bytes_read_uncompressed}, ${r.rows_written}, ${r.bytes_written_uncompressed}, ` +
    `${r.columns_written}, ${r.memory_usage}, ${r.thread_id})`
  ).join(',\n');
}

describe('merge samples integration (delta calculations)', () => {
  let ctx: TestClickHouseContext;

  beforeAll(async () => {
    ctx = await startClickHouse();

    await ctx.client.command({
      query: `CREATE DATABASE IF NOT EXISTS tracehouse ENGINE = Atomic`,
    });

    await ctx.client.command({
      query: `
        CREATE TABLE IF NOT EXISTS tracehouse.merges_history
        (
          hostname            LowCardinality(String) DEFAULT hostName(),
          sample_time         DateTime64(3) DEFAULT now64(3),
          database            LowCardinality(String),
          table               LowCardinality(String),
          result_part_name    String,
          partition_id        LowCardinality(String),
          elapsed             Float64,
          progress            Float64,
          num_parts           UInt64,
          is_mutation         UInt8,
          merge_type          LowCardinality(String),
          merge_algorithm     LowCardinality(String),
          total_size_bytes_compressed   UInt64,
          total_size_bytes_uncompressed UInt64,
          total_size_marks              UInt64,
          rows_read                     UInt64,
          bytes_read_uncompressed       UInt64,
          rows_written                  UInt64,
          bytes_written_uncompressed    UInt64,
          columns_written               UInt64,
          memory_usage        UInt64,
          thread_id           UInt64
        )
        ENGINE = MergeTree
        ORDER BY (database, table, result_part_name, sample_time)
      `,
    });
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
    await ctx.client.command({ query: `TRUNCATE TABLE tracehouse.merges_history` });
  });

  /** Seed rows, run the production SQL, and map through the production mapper. */
  async function seedAndQuery(
    rows: MergeSampleRow[],
    opts: { database: string; table: string; resultPartName?: string },
  ): Promise<MergeSample[]> {
    const values = buildInsertValues(rows);
    await ctx.client.command({
      query: `INSERT INTO tracehouse.merges_history
        (sample_time, database, table, result_part_name, partition_id,
         elapsed, progress, num_parts, is_mutation, merge_type, merge_algorithm,
         total_size_bytes_compressed, total_size_bytes_uncompressed, total_size_marks,
         rows_read, bytes_read_uncompressed, rows_written, bytes_written_uncompressed,
         columns_written, memory_usage, thread_id)
        VALUES ${values}`,
    });

    const raw = await ctx.adapter.executeQuery<Record<string, unknown>>(buildMergeSamplesSQL(opts));
    return raw.map(mapMergeSampleRow);
  }

  /**
   * Helper to build a sequence of merge sample rows with increasing cumulative values.
   */
  function makeMergeSamples(opts: {
    count: number;
    startTime?: string;
    database?: string;
    table?: string;
    resultPartName?: string;
    intervalMs?: number;
    rowsReadPerSec?: number;
    bytesReadPerSec?: number;
    rowsWrittenPerSec?: number;
    bytesWrittenPerSec?: number;
    totalSizeCompressed?: number;
    totalSizeUncompressed?: number;
    numParts?: number;
    isMutation?: boolean;
    mergeType?: string;
    mergeAlgorithm?: string;
    memoryUsage?: number;
  }): MergeSampleRow[] {
    const {
      count,
      startTime = '2025-06-01 12:00:00.000',
      database = 'default',
      table = 'test_table',
      resultPartName = 'all_0_1_1',
      intervalMs = 1000,
      rowsReadPerSec = 0,
      bytesReadPerSec = 0,
      rowsWrittenPerSec = 0,
      bytesWrittenPerSec = 0,
      totalSizeCompressed = 1_000_000,
      totalSizeUncompressed = 5_000_000,
      numParts = 3,
      isMutation = false,
      mergeType = 'Regular',
      mergeAlgorithm = 'Horizontal',
      memoryUsage = 0,
    } = opts;

    const base = new Date(startTime.replace(' ', 'T') + 'Z');
    const intervalSec = intervalMs / 1000;
    const rows: MergeSampleRow[] = [];

    for (let i = 0; i < count; i++) {
      const ts = new Date(base.getTime() + i * intervalMs);
      const timeStr = ts.toISOString().replace('T', ' ').replace('Z', '');
      const elapsedSec = i * intervalSec;
      const progress = count > 1 ? i / (count - 1) : 0;

      rows.push({
        sample_time: timeStr,
        database,
        table,
        result_part_name: resultPartName,
        partition_id: 'all',
        elapsed: elapsedSec,
        progress,
        num_parts: numParts,
        is_mutation: isMutation ? 1 : 0,
        merge_type: mergeType,
        merge_algorithm: mergeAlgorithm,
        total_size_bytes_compressed: totalSizeCompressed,
        total_size_bytes_uncompressed: totalSizeUncompressed,
        total_size_marks: 100,
        rows_read: Math.round(rowsReadPerSec * elapsedSec),
        bytes_read_uncompressed: Math.round(bytesReadPerSec * elapsedSec),
        rows_written: Math.round(rowsWrittenPerSec * elapsedSec),
        bytes_written_uncompressed: Math.round(bytesWrittenPerSec * elapsedSec),
        columns_written: 0,
        memory_usage: memoryUsage,
        thread_id: 42,
      });
    }
    return rows;
  }

  // -----------------------------------------------------------------------
  // Time offset (t)
  // -----------------------------------------------------------------------

  describe('time offset (t)', () => {
    it('computes seconds since first sample', async () => {
      const results = await seedAndQuery(
        makeMergeSamples({ count: 4, rowsReadPerSec: 1000 }),
        { database: 'default', table: 'test_table', resultPartName: 'all_0_1_1' },
      );

      expect(results).toHaveLength(4);
      expect(results[0].t).toBeCloseTo(0, 1);
      expect(results[1].t).toBeCloseTo(1, 1);
      expect(results[2].t).toBeCloseTo(2, 1);
      expect(results[3].t).toBeCloseTo(3, 1);
    });
  });

  // -----------------------------------------------------------------------
  // Cumulative passthrough
  // -----------------------------------------------------------------------

  describe('cumulative passthrough', () => {
    it('passes through merge identity fields', async () => {
      const results = await seedAndQuery(
        makeMergeSamples({
          count: 2,
          numParts: 5,
          mergeType: 'Regular',
          mergeAlgorithm: 'Vertical',
        }),
        { database: 'default', table: 'test_table', resultPartName: 'all_0_1_1' },
      );

      for (const r of results) {
        expect(r.database).toBe('default');
        expect(r.table).toBe('test_table');
        expect(r.result_part_name).toBe('all_0_1_1');
        expect(r.num_parts).toBe(5);
        expect(r.merge_type).toBe('Regular');
        expect(r.merge_algorithm).toBe('Vertical');
        expect(r.is_mutation).toBe(false);
      }
    });

    it('passes through progress', async () => {
      const results = await seedAndQuery(
        makeMergeSamples({ count: 3 }),
        { database: 'default', table: 'test_table', resultPartName: 'all_0_1_1' },
      );

      expect(results[0].progress).toBeCloseTo(0, 2);
      expect(results[1].progress).toBeCloseTo(0.5, 2);
      expect(results[2].progress).toBeCloseTo(1, 2);
    });

    it('passes through cumulative rows_read and rows_written', async () => {
      const results = await seedAndQuery(
        makeMergeSamples({ count: 3, rowsReadPerSec: 10000, rowsWrittenPerSec: 8000 }),
        { database: 'default', table: 'test_table', resultPartName: 'all_0_1_1' },
      );

      expect(results[0].rows_read).toBe(0);
      expect(results[1].rows_read).toBe(10000);
      expect(results[2].rows_read).toBe(20000);

      expect(results[0].rows_written).toBe(0);
      expect(results[1].rows_written).toBe(8000);
      expect(results[2].rows_written).toBe(16000);
    });

    it('passes through size fields', async () => {
      const results = await seedAndQuery(
        makeMergeSamples({
          count: 2,
          totalSizeCompressed: 1_000_000,
          totalSizeUncompressed: 5_000_000,
        }),
        { database: 'default', table: 'test_table', resultPartName: 'all_0_1_1' },
      );

      for (const r of results) {
        expect(r.total_size_bytes_compressed).toBe(1_000_000);
        expect(r.total_size_bytes_uncompressed).toBe(5_000_000);
      }
    });

    it('passes through memory_usage', async () => {
      const results = await seedAndQuery(
        makeMergeSamples({ count: 2, memoryUsage: 52_428_800 }),
        { database: 'default', table: 'test_table', resultPartName: 'all_0_1_1' },
      );

      for (const r of results) {
        expect(r.memory_usage).toBe(52_428_800);
      }
    });

    it('passes through elapsed', async () => {
      const results = await seedAndQuery(
        makeMergeSamples({ count: 3 }),
        { database: 'default', table: 'test_table', resultPartName: 'all_0_1_1' },
      );

      expect(results[0].elapsed).toBe(0);
      expect(results[1].elapsed).toBe(1);
      expect(results[2].elapsed).toBe(2);
    });

    it('correctly maps is_mutation', async () => {
      const results = await seedAndQuery(
        makeMergeSamples({ count: 2, isMutation: true, mergeType: '' }),
        { database: 'default', table: 'test_table', resultPartName: 'all_0_1_1' },
      );

      for (const r of results) {
        expect(r.is_mutation).toBe(true);
      }
    });
  });

  // -----------------------------------------------------------------------
  // Delta calculations
  // -----------------------------------------------------------------------

  describe('row read delta (d_rows_read)', () => {
    it('computes per-second read rows correctly (1s interval)', async () => {
      const results = await seedAndQuery(
        makeMergeSamples({ count: 4, rowsReadPerSec: 50000 }),
        { database: 'default', table: 'test_table', resultPartName: 'all_0_1_1' },
      );

      expect(results[0].d_rows_read).toBeCloseTo(0, 5);
      expect(results[1].d_rows_read).toBeCloseTo(50000, 0);
      expect(results[2].d_rows_read).toBeCloseTo(50000, 0);
      expect(results[3].d_rows_read).toBeCloseTo(50000, 0);
    });
  });

  describe('read MB delta (d_read_mb)', () => {
    it('computes per-second read MB', async () => {
      const bytesPerSec = 10 * 1024 * 1024; // 10 MiB/s
      const results = await seedAndQuery(
        makeMergeSamples({ count: 3, bytesReadPerSec: bytesPerSec }),
        { database: 'default', table: 'test_table', resultPartName: 'all_0_1_1' },
      );

      expect(results[0].d_read_mb).toBeCloseTo(0, 1);
      expect(results[1].d_read_mb).toBeCloseTo(10, 1);
      expect(results[2].d_read_mb).toBeCloseTo(10, 1);
    });
  });

  describe('write deltas (d_rows_written, d_written_mb)', () => {
    it('computes per-second write rates', async () => {
      const bytesPerSec = 5 * 1024 * 1024; // 5 MiB/s
      const results = await seedAndQuery(
        makeMergeSamples({ count: 3, rowsWrittenPerSec: 30000, bytesWrittenPerSec: bytesPerSec }),
        { database: 'default', table: 'test_table', resultPartName: 'all_0_1_1' },
      );

      expect(results[0].d_rows_written).toBeCloseTo(0, 5);
      expect(results[1].d_rows_written).toBeCloseTo(30000, 0);
      expect(results[2].d_rows_written).toBeCloseTo(30000, 0);

      expect(results[0].d_written_mb).toBeCloseTo(0, 1);
      expect(results[1].d_written_mb).toBeCloseTo(5, 1);
      expect(results[2].d_written_mb).toBeCloseTo(5, 1);
    });
  });

  // -----------------------------------------------------------------------
  // No negative deltas (greatest clamp)
  // -----------------------------------------------------------------------

  describe('no negative deltas', () => {
    it('clamps to 0 when cumulative counters decrease', async () => {
      const rows: MergeSampleRow[] = [
        {
          sample_time: '2025-06-01 12:00:00.000',
          database: 'default', table: 'test_table', result_part_name: 'all_0_1_1',
          partition_id: 'all', elapsed: 0, progress: 0, num_parts: 3, is_mutation: 0,
          merge_type: 'Regular', merge_algorithm: 'Horizontal',
          total_size_bytes_compressed: 1000, total_size_bytes_uncompressed: 5000, total_size_marks: 10,
          rows_read: 0, bytes_read_uncompressed: 0, rows_written: 0, bytes_written_uncompressed: 0,
          columns_written: 0, memory_usage: 0, thread_id: 42,
        },
        {
          sample_time: '2025-06-01 12:00:01.000',
          database: 'default', table: 'test_table', result_part_name: 'all_0_1_1',
          partition_id: 'all', elapsed: 1, progress: 0.5, num_parts: 3, is_mutation: 0,
          merge_type: 'Regular', merge_algorithm: 'Horizontal',
          total_size_bytes_compressed: 1000, total_size_bytes_uncompressed: 5000, total_size_marks: 10,
          rows_read: 10000, bytes_read_uncompressed: 10_485_760, rows_written: 8000,
          bytes_written_uncompressed: 8_388_608, columns_written: 0, memory_usage: 0, thread_id: 42,
        },
        {
          // counters decrease — simulate anomaly
          sample_time: '2025-06-01 12:00:02.000',
          database: 'default', table: 'test_table', result_part_name: 'all_0_1_1',
          partition_id: 'all', elapsed: 2, progress: 1, num_parts: 3, is_mutation: 0,
          merge_type: 'Regular', merge_algorithm: 'Horizontal',
          total_size_bytes_compressed: 1000, total_size_bytes_uncompressed: 5000, total_size_marks: 10,
          rows_read: 5000, bytes_read_uncompressed: 5_242_880, rows_written: 4000,
          bytes_written_uncompressed: 4_194_304, columns_written: 0, memory_usage: 0, thread_id: 42,
        },
      ];

      const results = await seedAndQuery(rows, {
        database: 'default', table: 'test_table', resultPartName: 'all_0_1_1',
      });
      expect(results).toHaveLength(3);

      // Third sample has lower counters → clamped to 0
      expect(results[2].d_rows_read).toBe(0);
      expect(results[2].d_read_mb).toBe(0);
      expect(results[2].d_rows_written).toBe(0);
      expect(results[2].d_written_mb).toBe(0);
    });
  });

  // -----------------------------------------------------------------------
  // Single sample edge case
  // -----------------------------------------------------------------------

  describe('single sample', () => {
    it('returns one row with t=0 and all deltas = 0', async () => {
      const rows: MergeSampleRow[] = [{
        sample_time: '2025-06-01 12:00:00.000',
        database: 'default', table: 'test_table', result_part_name: 'all_0_1_1',
        partition_id: 'all', elapsed: 5.5, progress: 0.75, num_parts: 4, is_mutation: 0,
        merge_type: 'Regular', merge_algorithm: 'Horizontal',
        total_size_bytes_compressed: 1_000_000, total_size_bytes_uncompressed: 5_000_000,
        total_size_marks: 100,
        rows_read: 50000, bytes_read_uncompressed: 52_428_800,
        rows_written: 40000, bytes_written_uncompressed: 41_943_040,
        columns_written: 5, memory_usage: 10_485_760, thread_id: 42,
      }];

      const results = await seedAndQuery(rows, {
        database: 'default', table: 'test_table', resultPartName: 'all_0_1_1',
      });
      expect(results).toHaveLength(1);

      const r = results[0];
      expect(r.t).toBe(0);
      expect(r.elapsed).toBeCloseTo(5.5, 1);
      expect(r.progress).toBeCloseTo(0.75, 2);
      expect(r.num_parts).toBe(4);
      expect(r.rows_read).toBe(50000);
      expect(r.memory_usage).toBe(10_485_760);

      // lag defaults to self → first sample raw delta = 0
      expect(r.d_rows_read).toBe(0);
      expect(r.d_read_mb).toBe(0);
      expect(r.d_rows_written).toBe(0);
      expect(r.d_written_mb).toBe(0);
    });
  });

  // -----------------------------------------------------------------------
  // Multi-merge isolation (table-level query without resultPartName)
  // -----------------------------------------------------------------------

  describe('multi-merge query', () => {
    it('partitions deltas by result_part_name when querying all merges for a table', async () => {
      const mergeA = makeMergeSamples({
        count: 3, resultPartName: 'all_0_1_1', rowsReadPerSec: 10000,
      });
      const mergeB = makeMergeSamples({
        count: 3, resultPartName: 'all_2_3_1', rowsReadPerSec: 50000,
      });

      const allRows = [...mergeA, ...mergeB];
      const values = buildInsertValues(allRows);
      await ctx.client.command({
        query: `INSERT INTO tracehouse.merges_history
          (sample_time, database, table, result_part_name, partition_id,
           elapsed, progress, num_parts, is_mutation, merge_type, merge_algorithm,
           total_size_bytes_compressed, total_size_bytes_uncompressed, total_size_marks,
           rows_read, bytes_read_uncompressed, rows_written, bytes_written_uncompressed,
           columns_written, memory_usage, thread_id)
          VALUES ${values}`,
      });

      const results = (await ctx.adapter.executeQuery<Record<string, unknown>>(buildMergeSamplesSQL({ database: 'default', table: 'test_table' }))).map(mapMergeSampleRow);

      expect(results).toHaveLength(6);

      // Filter by part name
      const partA = results.filter(r => r.result_part_name === 'all_0_1_1');
      const partB = results.filter(r => r.result_part_name === 'all_2_3_1');

      expect(partA).toHaveLength(3);
      expect(partB).toHaveLength(3);

      // Each merge's deltas are computed independently
      expect(partA[1].d_rows_read).toBeCloseTo(10000, 0);
      expect(partB[1].d_rows_read).toBeCloseTo(50000, 0);
    });
  });

  // -----------------------------------------------------------------------
  // Non-uniform intervals
  // -----------------------------------------------------------------------

  describe('non-uniform intervals', () => {
    it('normalizes deltas by dt so rates are per-second', async () => {
      const rows: MergeSampleRow[] = [
        {
          sample_time: '2025-06-01 14:00:00.000',
          database: 'default', table: 'test_table', result_part_name: 'all_0_1_1',
          partition_id: 'all', elapsed: 0, progress: 0, num_parts: 3, is_mutation: 0,
          merge_type: 'Regular', merge_algorithm: 'Horizontal',
          total_size_bytes_compressed: 1000, total_size_bytes_uncompressed: 5000, total_size_marks: 10,
          rows_read: 0, bytes_read_uncompressed: 0, rows_written: 0, bytes_written_uncompressed: 0,
          columns_written: 0, memory_usage: 0, thread_id: 42,
        },
        {
          sample_time: '2025-06-01 14:00:01.000',
          database: 'default', table: 'test_table', result_part_name: 'all_0_1_1',
          partition_id: 'all', elapsed: 1, progress: 0.2, num_parts: 3, is_mutation: 0,
          merge_type: 'Regular', merge_algorithm: 'Horizontal',
          total_size_bytes_compressed: 1000, total_size_bytes_uncompressed: 5000, total_size_marks: 10,
          rows_read: 10000, bytes_read_uncompressed: 0, rows_written: 0, bytes_written_uncompressed: 0,
          columns_written: 0, memory_usage: 0, thread_id: 42,
        },
        {
          // 4-second gap: raw rows_read delta = 80000, dt = 4s → 20000 rows/s
          sample_time: '2025-06-01 14:00:05.000',
          database: 'default', table: 'test_table', result_part_name: 'all_0_1_1',
          partition_id: 'all', elapsed: 5, progress: 1, num_parts: 3, is_mutation: 0,
          merge_type: 'Regular', merge_algorithm: 'Horizontal',
          total_size_bytes_compressed: 1000, total_size_bytes_uncompressed: 5000, total_size_marks: 10,
          rows_read: 90000, bytes_read_uncompressed: 0, rows_written: 0, bytes_written_uncompressed: 0,
          columns_written: 0, memory_usage: 0, thread_id: 42,
        },
      ];

      const results = await seedAndQuery(rows, {
        database: 'default', table: 'test_table', resultPartName: 'all_0_1_1',
      });
      expect(results).toHaveLength(3);

      expect(results[0].t).toBeCloseTo(0, 1);
      expect(results[1].t).toBeCloseTo(1, 1);
      expect(results[2].t).toBeCloseTo(5, 1);

      // dt=1s: 10000/1 = 10000 rows/s
      expect(results[1].d_rows_read).toBeCloseTo(10000, 0);
      // dt=4s: 80000/4 = 20000 rows/s (normalized rate)
      expect(results[2].d_rows_read).toBeCloseTo(20000, 0);
    });
  });

  // -----------------------------------------------------------------------
  // Variable sampling intervals
  // -----------------------------------------------------------------------

  describe('variable sampling intervals', () => {
    it('produces correct rates with 500ms interval', async () => {
      const results = await seedAndQuery(
        makeMergeSamples({
          count: 4, intervalMs: 500, rowsReadPerSec: 100000,
          bytesReadPerSec: 10 * 1024 * 1024,
        }),
        { database: 'default', table: 'test_table', resultPartName: 'all_0_1_1' },
      );

      expect(results[0].d_rows_read).toBeCloseTo(0, 5); // first sample
      // dt=0.5s, raw delta = 50000, 50000/0.5 = 100000 rows/s
      expect(results[1].d_rows_read).toBeCloseTo(100000, 0);
      expect(results[2].d_rows_read).toBeCloseTo(100000, 0);

      // dt=0.5s, raw read = 5 MiB, 5/0.5 = 10 MB/s
      expect(results[1].d_read_mb).toBeCloseTo(10, 0);
    });

    it('produces correct rates with 10s interval', async () => {
      const results = await seedAndQuery(
        makeMergeSamples({
          count: 4, intervalMs: 10_000, rowsReadPerSec: 50000,
        }),
        { database: 'default', table: 'test_table', resultPartName: 'all_0_1_1' },
      );

      expect(results[0].d_rows_read).toBeCloseTo(0, 5); // first sample
      // dt=10s, raw delta = 500000, 500000/10 = 50000 rows/s
      expect(results[1].d_rows_read).toBeCloseTo(50000, 0);
      expect(results[2].d_rows_read).toBeCloseTo(50000, 0);
    });
  });
});
