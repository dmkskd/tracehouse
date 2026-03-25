/**
 * Integration tests for PROCESS_SAMPLES_SQL + mapProcessSampleRow
 * from packages/core/src/queries/process-queries.ts.
 *
 * Creates a tracehouse.processes_history table in a real ClickHouse container,
 * seeds it with known synthetic data, runs the exact production SQL, maps
 * via the production row mapper, and asserts correctness.
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { startClickHouse, stopClickHouse, type TestClickHouseContext } from './setup/clickhouse-container.js';
import { runTracehouseSetup } from './setup/tracehouse-setup.js';
import { buildProcessSamplesSQL, mapProcessSampleRow, type ProcessSample } from '../../queries/process-queries.js';

const CONTAINER_TIMEOUT = 120_000;

interface SampleRow {
  sample_time: string;
  query_id: string;
  initial_query_id: string;
  elapsed: number;
  memory_usage: number;
  peak_memory_usage: number;
  read_rows: number;
  read_bytes: number;
  written_rows: number;
  thread_ids: number[];
  profile_events: Record<string, number>;
}

function buildInsertValues(rows: SampleRow[]): string {
  return rows.map(r => {
    const pe = Object.entries(r.profile_events)
      .map(([k, v]) => `'${k}', ${v}`)
      .join(', ');
    const threadArr = `[${r.thread_ids.join(', ')}]`;
    return `('${r.sample_time}', '${r.query_id}', '${r.initial_query_id}', ${r.elapsed}, ${r.memory_usage}, ${r.peak_memory_usage}, ${r.read_rows}, ${r.read_bytes}, ${r.written_rows}, ${threadArr}, map(${pe}))`;
  }).join(',\n');
}

describe('PROCESS_SAMPLES_SQL integration (delta calculations)', () => {
  let ctx: TestClickHouseContext;
  const TEST_QID = 'test-query-001';

  beforeAll(async () => {
    ctx = await startClickHouse();
    await runTracehouseSetup(ctx, { target: 'processes', tablesOnly: true });
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
    await ctx.client.command({ query: `TRUNCATE TABLE tracehouse.processes_history` });
  });

  /** Seed rows, run the production SQL, and map through the production mapper. */
  async function seedAndQuery(rows: SampleRow[], qid: string = TEST_QID): Promise<ProcessSample[]> {
    const values = buildInsertValues(rows);
    await ctx.client.command({
      query: `INSERT INTO tracehouse.processes_history
        (sample_time, query_id, initial_query_id, elapsed, memory_usage, peak_memory_usage,
         read_rows, read_bytes, written_rows, thread_ids, ProfileEvents)
        VALUES ${values}`,
    });

    const raw = await ctx.adapter.executeQuery<Record<string, unknown>>(buildProcessSamplesSQL([qid]));
    return raw.map(mapProcessSampleRow);
  }

  /**
   * Helper to build a sequence of sample rows with increasing cumulative values.
   * `intervalMs` controls the spacing between samples (default 1000ms).
   * Rate params (cpuPerSec, etc.) are always per-second regardless of interval.
   */
  function makeSamples(opts: {
    count: number;
    startTime?: string;
    qid?: string;
    intervalMs?: number;
    cpuPerSec?: number;
    ioWaitPerSec?: number;
    readBytesPerSec?: number;
    readRowsPerSec?: number;
    writtenRowsPerSec?: number;
    netSendPerSec?: number;
    netRecvPerSec?: number;
    memoryUsage?: number;
    peakMemoryUsage?: number;
    threadsPerSample?: number;
  }): SampleRow[] {
    const {
      count,
      startTime = '2025-06-01 12:00:00.000',
      qid = TEST_QID,
      intervalMs = 1000,
      cpuPerSec = 0,
      ioWaitPerSec = 0,
      readBytesPerSec = 0,
      readRowsPerSec = 0,
      writtenRowsPerSec = 0,
      netSendPerSec = 0,
      netRecvPerSec = 0,
      memoryUsage = 0,
      peakMemoryUsage = 0,
      threadsPerSample = 1,
    } = opts;

    const base = new Date(startTime.replace(' ', 'T') + 'Z');
    const intervalSec = intervalMs / 1000;
    const rows: SampleRow[] = [];

    for (let i = 0; i < count; i++) {
      const ts = new Date(base.getTime() + i * intervalMs);
      const timeStr = ts.toISOString().replace('T', ' ').replace('Z', '');
      const elapsedSec = i * intervalSec;

      rows.push({
        sample_time: timeStr,
        query_id: qid,
        initial_query_id: qid,
        elapsed: elapsedSec,
        memory_usage: memoryUsage,
        peak_memory_usage: peakMemoryUsage,
        read_rows: Math.round(readRowsPerSec * elapsedSec),
        read_bytes: Math.round(readBytesPerSec * elapsedSec),
        written_rows: Math.round(writtenRowsPerSec * elapsedSec),
        thread_ids: Array.from({ length: threadsPerSample }, (_, t) => t + 1),
        profile_events: {
          OSCPUVirtualTimeMicroseconds: Math.round(cpuPerSec * elapsedSec),
          OSCPUWaitMicroseconds: Math.round(ioWaitPerSec * elapsedSec),
          NetworkSendBytes: Math.round(netSendPerSec * elapsedSec),
          NetworkReceiveBytes: Math.round(netRecvPerSec * elapsedSec),
        },
      });
    }
    return rows;
  }

  // -----------------------------------------------------------------------
  // Time offset (t)
  // -----------------------------------------------------------------------

  describe('time offset (t)', () => {
    it('computes seconds since first sample', async () => {
      const results = await seedAndQuery(makeSamples({ count: 4 }));

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
    it('passes through memory_mb and peak_memory_mb correctly', async () => {
      const results = await seedAndQuery(makeSamples({
        count: 3,
        memoryUsage: 104_857_600,      // 100 MiB
        peakMemoryUsage: 209_715_200,  // 200 MiB
      }));

      for (const r of results) {
        expect(r.memory_mb).toBeCloseTo(100, 0);
        expect(r.peak_memory_mb).toBeCloseTo(200, 0);
      }
    });

    it('passes through cumulative read_rows and written_rows', async () => {
      const results = await seedAndQuery(makeSamples({ count: 3, readRowsPerSec: 1000, writtenRowsPerSec: 500 }));

      expect(results[0].read_rows).toBe(0);
      expect(results[1].read_rows).toBe(1000);
      expect(results[2].read_rows).toBe(2000);

      expect(results[0].written_rows).toBe(0);
      expect(results[1].written_rows).toBe(500);
      expect(results[2].written_rows).toBe(1000);
    });

    it('passes through cumulative cpu_us and io_wait_us', async () => {
      const results = await seedAndQuery(makeSamples({ count: 3, cpuPerSec: 500_000, ioWaitPerSec: 100_000 }));

      expect(results[0].cpu_us).toBe(0);
      expect(results[1].cpu_us).toBe(500_000);
      expect(results[2].cpu_us).toBe(1_000_000);

      expect(results[0].io_wait_us).toBe(0);
      expect(results[1].io_wait_us).toBe(100_000);
      expect(results[2].io_wait_us).toBe(200_000);
    });

    it('passes through cumulative net_send_bytes and net_recv_bytes', async () => {
      const results = await seedAndQuery(makeSamples({ count: 3, netSendPerSec: 1024, netRecvPerSec: 2048 }));

      expect(results[1].net_send_bytes).toBe(1024);
      expect(results[2].net_send_bytes).toBe(2048);
      expect(results[1].net_recv_bytes).toBe(2048);
      expect(results[2].net_recv_bytes).toBe(4096);
    });

    it('passes through thread_count from thread_ids length', async () => {
      const results = await seedAndQuery(makeSamples({ count: 2, threadsPerSample: 8 }));

      for (const r of results) {
        expect(r.thread_count).toBe(8);
      }
    });

    it('passes through elapsed', async () => {
      const results = await seedAndQuery(makeSamples({ count: 3 }));

      expect(results[0].elapsed).toBe(0);
      expect(results[1].elapsed).toBe(1);
      expect(results[2].elapsed).toBe(2);
    });
  });

  // -----------------------------------------------------------------------
  // Delta calculations
  // -----------------------------------------------------------------------

  describe('CPU delta (d_cpu_cores)', () => {
    it('computes per-second CPU cores correctly (1s interval)', async () => {
      // 2_000_000 µs per second = 2 cores worth of CPU time
      const results = await seedAndQuery(makeSamples({ count: 4, cpuPerSec: 2_000_000 }));

      // i=0: first sample, lag defaults to self → delta=0
      expect(results[0].d_cpu_cores).toBeCloseTo(0, 5);
      // i=1+: dt=1s, raw_delta=2M µs, d_cpu = 2M/1e6/1 = 2 cores
      expect(results[1].d_cpu_cores).toBeCloseTo(2, 5);
      expect(results[2].d_cpu_cores).toBeCloseTo(2, 5);
      expect(results[3].d_cpu_cores).toBeCloseTo(2, 5);
    });
  });

  describe('I/O wait delta (d_io_wait_s)', () => {
    it('computes per-interval I/O wait in seconds', async () => {
      // 500_000 µs per second = 0.5 seconds of I/O wait
      const results = await seedAndQuery(makeSamples({ count: 3, ioWaitPerSec: 500_000 }));

      expect(results[0].d_io_wait_s).toBeCloseTo(0, 5);
      expect(results[1].d_io_wait_s).toBeCloseTo(0.5, 5);
      expect(results[2].d_io_wait_s).toBeCloseTo(0.5, 5);
    });
  });

  describe('read delta (d_read_mb)', () => {
    it('computes per-interval read MB', async () => {
      const bytesPerSec = 10 * 1024 * 1024; // 10 MiB
      const results = await seedAndQuery(makeSamples({ count: 3, readBytesPerSec: bytesPerSec }));

      expect(results[0].d_read_mb).toBeCloseTo(0, 1);
      expect(results[1].d_read_mb).toBeCloseTo(10, 1);
      expect(results[2].d_read_mb).toBeCloseTo(10, 1);
    });
  });

  describe('row deltas (d_read_rows, d_written_rows)', () => {
    it('computes per-interval row deltas', async () => {
      const results = await seedAndQuery(makeSamples({ count: 3, readRowsPerSec: 5000, writtenRowsPerSec: 200 }));

      expect(results[0].d_read_rows).toBe(0);
      expect(results[1].d_read_rows).toBe(5000);
      expect(results[2].d_read_rows).toBe(5000);

      expect(results[0].d_written_rows).toBe(0);
      expect(results[1].d_written_rows).toBe(200);
      expect(results[2].d_written_rows).toBe(200);
    });
  });

  describe('network deltas (d_net_send_kb, d_net_recv_kb)', () => {
    it('computes separate send and recv deltas in KB', async () => {
      const results = await seedAndQuery(makeSamples({ count: 3, netSendPerSec: 10240, netRecvPerSec: 20480 }));

      expect(results[0].d_net_send_kb).toBeCloseTo(0, 1);
      expect(results[1].d_net_send_kb).toBeCloseTo(10, 1);
      expect(results[2].d_net_send_kb).toBeCloseTo(10, 1);

      expect(results[0].d_net_recv_kb).toBeCloseTo(0, 1);
      expect(results[1].d_net_recv_kb).toBeCloseTo(20, 1);
      expect(results[2].d_net_recv_kb).toBeCloseTo(20, 1);
    });
  });

  // -----------------------------------------------------------------------
  // No negative deltas (greatest clamp)
  // -----------------------------------------------------------------------

  describe('no negative deltas', () => {
    it('clamps to 0 when cumulative counters decrease (counter reset)', async () => {
      const rows: SampleRow[] = [
        {
          sample_time: '2025-06-01 12:00:00.000',
          query_id: TEST_QID, initial_query_id: TEST_QID,
          elapsed: 0, memory_usage: 0, peak_memory_usage: 0,
          read_rows: 0, read_bytes: 0, written_rows: 0,
          thread_ids: [1],
          profile_events: {
            OSCPUVirtualTimeMicroseconds: 0, OSCPUWaitMicroseconds: 0,
            NetworkSendBytes: 0, NetworkReceiveBytes: 0,
          },
        },
        {
          sample_time: '2025-06-01 12:00:01.000',
          query_id: TEST_QID, initial_query_id: TEST_QID,
          elapsed: 1, memory_usage: 0, peak_memory_usage: 0,
          read_rows: 1000, read_bytes: 1_048_576, written_rows: 500,
          thread_ids: [1],
          profile_events: {
            OSCPUVirtualTimeMicroseconds: 2_000_000, OSCPUWaitMicroseconds: 500_000,
            NetworkSendBytes: 10240, NetworkReceiveBytes: 20480,
          },
        },
        {
          // counters decrease — simulate counter reset
          sample_time: '2025-06-01 12:00:02.000',
          query_id: TEST_QID, initial_query_id: TEST_QID,
          elapsed: 2, memory_usage: 0, peak_memory_usage: 0,
          read_rows: 500, read_bytes: 524_288, written_rows: 200,
          thread_ids: [1],
          profile_events: {
            OSCPUVirtualTimeMicroseconds: 1_000_000, OSCPUWaitMicroseconds: 200_000,
            NetworkSendBytes: 5120, NetworkReceiveBytes: 10240,
          },
        },
      ];

      const results = await seedAndQuery(rows);
      expect(results).toHaveLength(3);

      // Third sample has lower counters than second → clamped to 0
      expect(results[2].d_cpu_cores).toBe(0);
      expect(results[2].d_io_wait_s).toBe(0);
      expect(results[2].d_read_mb).toBe(0);
      expect(results[2].d_read_rows).toBe(0);
      expect(results[2].d_written_rows).toBe(0);
      expect(results[2].d_net_send_kb).toBe(0);
      expect(results[2].d_net_recv_kb).toBe(0);
    });
  });

  // -----------------------------------------------------------------------
  // Single sample edge case
  // -----------------------------------------------------------------------

  describe('single sample', () => {
    it('returns one row with t=0 and all deltas = 0 (first sample has no previous)', async () => {
      const rows: SampleRow[] = [{
        sample_time: '2025-06-01 12:00:00.000',
        query_id: TEST_QID, initial_query_id: TEST_QID,
        elapsed: 5.5, memory_usage: 52_428_800, peak_memory_usage: 104_857_600,
        read_rows: 10000, read_bytes: 10_485_760, written_rows: 100,
        thread_ids: [1, 2, 3, 4],
        profile_events: {
          OSCPUVirtualTimeMicroseconds: 3_000_000, OSCPUWaitMicroseconds: 500_000,
          NetworkSendBytes: 51200, NetworkReceiveBytes: 102400,
        },
      }];

      const results = await seedAndQuery(rows);
      expect(results).toHaveLength(1);

      const r = results[0];
      expect(r.t).toBe(0);
      expect(r.elapsed).toBeCloseTo(5.5, 1);
      expect(r.thread_count).toBe(4);
      expect(r.memory_mb).toBeCloseTo(50, 0);
      expect(r.peak_memory_mb).toBeCloseTo(100, 0);
      expect(r.read_rows).toBe(10000);
      expect(r.read_bytes).toBe(10_485_760);
      expect(r.cpu_us).toBe(3_000_000);

      // lag defaults to self → first sample raw delta = 0
      expect(r.d_cpu_cores).toBe(0);
      expect(r.d_io_wait_s).toBe(0);
      expect(r.d_read_mb).toBe(0);
      expect(r.d_read_rows).toBe(0);
      expect(r.d_written_rows).toBe(0);
      expect(r.d_net_send_kb).toBe(0);
      expect(r.d_net_recv_kb).toBe(0);
    });
  });

  // -----------------------------------------------------------------------
  // Multi-query filter isolation
  // -----------------------------------------------------------------------

  describe('multi-query filter isolation', () => {
    it('only returns samples for the requested query_id', async () => {
      const wanted = makeSamples({ count: 3, qid: 'query-A', cpuPerSec: 1_000_000 });
      const other = makeSamples({ count: 3, qid: 'query-B', cpuPerSec: 9_000_000 });

      const allRows = [...wanted, ...other];
      const values = buildInsertValues(allRows);
      await ctx.client.command({
        query: `INSERT INTO tracehouse.processes_history
          (sample_time, query_id, initial_query_id, elapsed, memory_usage, peak_memory_usage,
           read_rows, read_bytes, written_rows, thread_ids, ProfileEvents)
          VALUES ${values}`,
      });

      const results = (await ctx.adapter.executeQuery<Record<string, unknown>>(buildProcessSamplesSQL(['query-A']))).map(mapProcessSampleRow);

      expect(results).toHaveLength(3);
      expect(results[1].d_cpu_cores).toBeCloseTo(1, 5);
    });

    it('matches rows by initial_query_id (distributed sub-queries)', async () => {
      const parentRow: SampleRow = {
        sample_time: '2025-06-01 13:00:00.000',
        query_id: 'parent-Q', initial_query_id: 'parent-Q',
        elapsed: 0, memory_usage: 0, peak_memory_usage: 0,
        read_rows: 0, read_bytes: 0, written_rows: 0,
        thread_ids: [1],
        profile_events: { OSCPUVirtualTimeMicroseconds: 0, OSCPUWaitMicroseconds: 0, NetworkSendBytes: 0, NetworkReceiveBytes: 0 },
      };
      const childRow: SampleRow = {
        sample_time: '2025-06-01 13:00:01.000',
        query_id: 'child-sub-1', initial_query_id: 'parent-Q',
        elapsed: 1, memory_usage: 0, peak_memory_usage: 0,
        read_rows: 5000, read_bytes: 0, written_rows: 0,
        thread_ids: [1, 2],
        profile_events: { OSCPUVirtualTimeMicroseconds: 1_000_000, OSCPUWaitMicroseconds: 0, NetworkSendBytes: 0, NetworkReceiveBytes: 0 },
      };

      const values = buildInsertValues([parentRow, childRow]);
      await ctx.client.command({
        query: `INSERT INTO tracehouse.processes_history
          (sample_time, query_id, initial_query_id, elapsed, memory_usage, peak_memory_usage,
           read_rows, read_bytes, written_rows, thread_ids, ProfileEvents)
          VALUES ${values}`,
      });

      const results = (await ctx.adapter.executeQuery<Record<string, unknown>>(buildProcessSamplesSQL(['parent-Q']))).map(mapProcessSampleRow);

      expect(results).toHaveLength(2);
    });
  });

  // -----------------------------------------------------------------------
  // Non-uniform intervals
  // -----------------------------------------------------------------------

  describe('non-uniform intervals', () => {
    it('normalizes deltas by dt so rates are per-second', async () => {
      const rows: SampleRow[] = [
        {
          sample_time: '2025-06-01 14:00:00.000',
          query_id: TEST_QID, initial_query_id: TEST_QID,
          elapsed: 0, memory_usage: 0, peak_memory_usage: 0,
          read_rows: 0, read_bytes: 0, written_rows: 0,
          thread_ids: [1],
          profile_events: { OSCPUVirtualTimeMicroseconds: 0, OSCPUWaitMicroseconds: 0, NetworkSendBytes: 0, NetworkReceiveBytes: 0 },
        },
        {
          sample_time: '2025-06-01 14:00:01.000',
          query_id: TEST_QID, initial_query_id: TEST_QID,
          elapsed: 1, memory_usage: 0, peak_memory_usage: 0,
          read_rows: 1000, read_bytes: 0, written_rows: 0,
          thread_ids: [1],
          profile_events: { OSCPUVirtualTimeMicroseconds: 2_000_000, OSCPUWaitMicroseconds: 0, NetworkSendBytes: 0, NetworkReceiveBytes: 0 },
        },
        {
          // 4-second gap: raw CPU delta = 8M µs, dt = 4s → 2 cores/s
          sample_time: '2025-06-01 14:00:05.000',
          query_id: TEST_QID, initial_query_id: TEST_QID,
          elapsed: 5, memory_usage: 0, peak_memory_usage: 0,
          read_rows: 9000, read_bytes: 0, written_rows: 0,
          thread_ids: [1],
          profile_events: { OSCPUVirtualTimeMicroseconds: 10_000_000, OSCPUWaitMicroseconds: 0, NetworkSendBytes: 0, NetworkReceiveBytes: 0 },
        },
      ];

      const results = await seedAndQuery(rows);
      expect(results).toHaveLength(3);

      expect(results[0].t).toBeCloseTo(0, 1);
      expect(results[1].t).toBeCloseTo(1, 1);
      expect(results[2].t).toBeCloseTo(5, 1);

      // dt=1s: raw=2M µs → 2M/1e6/1 = 2 cores
      expect(results[1].d_cpu_cores).toBeCloseTo(2, 5);
      // dt=4s: raw=8M µs → 8M/1e6/4 = 2 cores (normalized rate, not raw delta)
      expect(results[2].d_cpu_cores).toBeCloseTo(2, 5);

      // dt=1s: 1000 rows/1s = 1000 rows/s
      expect(results[1].d_read_rows).toBeCloseTo(1000, 0);
      // dt=4s: 8000 rows/4s = 2000 rows/s
      expect(results[2].d_read_rows).toBeCloseTo(2000, 0);
    });
  });

  // -----------------------------------------------------------------------
  // Variable sampling intervals
  // -----------------------------------------------------------------------

  describe('variable sampling intervals', () => {
    it('produces correct rates with 500ms interval', async () => {
      // 2 CPU cores at 500ms intervals
      const results = await seedAndQuery(makeSamples({
        count: 4, intervalMs: 500, cpuPerSec: 2_000_000,
        readBytesPerSec: 10 * 1024 * 1024,
      }));

      expect(results[0].d_cpu_cores).toBeCloseTo(0, 5); // first sample
      // dt=0.5s, raw CPU delta = 1M µs, 1M/1e6/0.5 = 2 cores
      expect(results[1].d_cpu_cores).toBeCloseTo(2, 4);
      expect(results[2].d_cpu_cores).toBeCloseTo(2, 4);
      expect(results[3].d_cpu_cores).toBeCloseTo(2, 4);

      // dt=0.5s, raw read = 5 MiB, 5/0.5 = 10 MB/s
      expect(results[1].d_read_mb).toBeCloseTo(10, 0);
    });

    it('produces correct rates with 10s interval', async () => {
      // 2 CPU cores at 10s intervals
      const results = await seedAndQuery(makeSamples({
        count: 4, intervalMs: 10_000, cpuPerSec: 2_000_000,
        readRowsPerSec: 5000,
      }));

      expect(results[0].d_cpu_cores).toBeCloseTo(0, 5); // first sample
      // dt=10s, raw CPU delta = 20M µs, 20M/1e6/10 = 2 cores
      expect(results[1].d_cpu_cores).toBeCloseTo(2, 4);
      expect(results[2].d_cpu_cores).toBeCloseTo(2, 4);

      // dt=10s, raw rows = 50000, 50000/10 = 5000 rows/s
      expect(results[1].d_read_rows).toBeCloseTo(5000, 0);
    });
  });
});
