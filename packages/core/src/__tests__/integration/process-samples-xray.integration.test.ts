/**
 * Integration tests for buildHostProcessSamplesSQL (X-Ray view).
 *
 * Two test suites:
 *
 * 1. **Single-node (synthetic data)** — Seeds tracehouse.processes_history with
 *    known synthetic rows to verify delta computation is correct when multiple
 *    processes (coordinator + shard sub-queries) coexist on the same host.
 *
 * 2. **Cluster (organic sampling)** — Spins up a 2-shard cluster with the
 *    processes_sampler MV running, executes a real distributed query that takes
 *    several seconds, then reads the organically populated processes_history
 *    and verifies CPU values are reasonable (not inflated).
 *
 * The fix being tested: window functions must partition by (hostname, query_id)
 * — not just hostname — to avoid computing lag() across different processes,
 * which inflates cumulative counters.
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { startClickHouse, stopClickHouse, type TestClickHouseContext } from './setup/clickhouse-container.js';
import { runTracehouseSetup } from './setup/tracehouse-setup.js';
import {
  startSamplingCluster,
  stopSamplingCluster,
  type SamplingClusterContext,
} from './setup/sampling-cluster-container.js';
import {
  buildHostProcessSamplesSQL,
  mapHostProcessSampleRow,
  type HostProcessSample,
} from '../../queries/process-queries.js';
import { ClusterAwareAdapter } from '../../adapters/cluster-adapter.js';
import { ClusterTestAdapter } from './setup/cluster-container.js';

const CONTAINER_TIMEOUT = 120_000;

// ─────────────────────────────────────────────────────────────────────────────
// Shared helpers
// ─────────────────────────────────────────────────────────────────────────────

interface SampleRow {
  hostname: string;
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
    return `('${r.hostname}', '${r.sample_time}', '${r.query_id}', '${r.initial_query_id}', ${r.elapsed}, ${r.memory_usage}, ${r.peak_memory_usage}, ${r.read_rows}, ${r.read_bytes}, ${r.written_rows}, ${threadArr}, map(${pe}))`;
  }).join(',\n');
}

const PE_ZERO = {
  OSCPUVirtualTimeMicroseconds: 0,
  OSCPUWaitMicroseconds: 0,
  NetworkSendBytes: 0,
  NetworkReceiveBytes: 0,
};

// ─────────────────────────────────────────────────────────────────────────────
// Suite 1: Single-node with synthetic data
// ─────────────────────────────────────────────────────────────────────────────

describe('X-Ray: single-node synthetic data', () => {
  let ctx: TestClickHouseContext;
  const INITIAL_QID = 'dist-query-001';

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

  async function seedAndQuery(rows: SampleRow[], qid: string = INITIAL_QID): Promise<HostProcessSample[]> {
    const values = buildInsertValues(rows);
    await ctx.client.command({
      query: `INSERT INTO tracehouse.processes_history
        (hostname, sample_time, query_id, initial_query_id, elapsed, memory_usage, peak_memory_usage,
         read_rows, read_bytes, written_rows, thread_ids, ProfileEvents)
        VALUES ${values}`,
    });

    const raw = await ctx.adapter.executeQuery<Record<string, unknown>>(buildHostProcessSamplesSQL(qid));
    return raw.map(mapHostProcessSampleRow);
  }

  // ── Single process baseline ──

  describe('single process on one host', () => {
    it('computes correct CPU cores', async () => {
      const rows: SampleRow[] = [0, 1, 2, 3].map(i => ({
        hostname: 'host-a',
        sample_time: `2025-06-01 12:00:0${i}.000`,
        query_id: INITIAL_QID,
        initial_query_id: INITIAL_QID,
        elapsed: i,
        memory_usage: 50 * 1024 * 1024,
        peak_memory_usage: 100 * 1024 * 1024,
        read_rows: i * 1000,
        read_bytes: i * 1024 * 1024,
        written_rows: 0,
        thread_ids: [1, 2, 3, 4],
        profile_events: {
          OSCPUVirtualTimeMicroseconds: i * 2_000_000,
          OSCPUWaitMicroseconds: i * 500_000,
          NetworkSendBytes: i * 10240,
          NetworkReceiveBytes: i * 20480,
        },
      }));

      const results = await seedAndQuery(rows);
      expect(results).toHaveLength(4);
      expect(results[0].hostname).toBe('host-a');
      expect(results[0].d_cpu_cores).toBeCloseTo(0, 5);
      expect(results[1].d_cpu_cores).toBeCloseTo(2, 4);
      expect(results[2].d_cpu_cores).toBeCloseTo(2, 4);
      expect(results[3].d_cpu_cores).toBeCloseTo(2, 4);
      expect(results[1].d_io_wait_s).toBeCloseTo(0.5, 4);
      expect(results[1].thread_count).toBe(4);
      expect(results[1].memory_mb).toBeCloseTo(50, 0);
    });
  });

  // ── Cross-process delta isolation (the bug) ──

  describe('cross-process delta isolation', () => {
    it('does not inflate CPU when coordinator + sub-query coexist on same host', async () => {
      // coordinator: 2 CPU cores, sub-query: 3 CPU cores, both on host-a
      // Before fix: cross-process lag produced 55+ cores
      // After fix: correctly sums to 5 cores per sample
      const coordinator: SampleRow[] = [0, 1, 2, 3].map(i => ({
        hostname: 'host-a',
        sample_time: `2025-06-01 12:00:0${i}.000`,
        query_id: INITIAL_QID,
        initial_query_id: INITIAL_QID,
        elapsed: i,
        memory_usage: 10 * 1024 * 1024,
        peak_memory_usage: 20 * 1024 * 1024,
        read_rows: 0, read_bytes: 0, written_rows: 0,
        thread_ids: [1, 2],
        profile_events: {
          OSCPUVirtualTimeMicroseconds: i * 2_000_000,
          OSCPUWaitMicroseconds: i * 100_000,
          NetworkSendBytes: i * 1024,
          NetworkReceiveBytes: i * 2048,
        },
      }));

      const subQuery: SampleRow[] = [0, 1, 2, 3].map(i => ({
        hostname: 'host-a',
        sample_time: `2025-06-01 12:00:0${i}.000`,
        query_id: 'child-sub-1',
        initial_query_id: INITIAL_QID,
        elapsed: i,
        memory_usage: 40 * 1024 * 1024,
        peak_memory_usage: 80 * 1024 * 1024,
        read_rows: i * 5000, read_bytes: i * 10 * 1024 * 1024, written_rows: 0,
        thread_ids: [10, 11, 12, 13, 14, 15],
        profile_events: {
          OSCPUVirtualTimeMicroseconds: i * 3_000_000,
          OSCPUWaitMicroseconds: i * 200_000,
          NetworkSendBytes: i * 5120,
          NetworkReceiveBytes: i * 10240,
        },
      }));

      const results = await seedAndQuery([...coordinator, ...subQuery]);
      expect(results).toHaveLength(4);

      expect(results[0].d_cpu_cores).toBeCloseTo(0, 5);
      // 2 + 3 = 5 cores per interval
      expect(results[1].d_cpu_cores).toBeCloseTo(5, 4);
      expect(results[2].d_cpu_cores).toBeCloseTo(5, 4);
      expect(results[3].d_cpu_cores).toBeCloseTo(5, 4);

      // IO wait: 0.1 + 0.2 = 0.3
      expect(results[1].d_io_wait_s).toBeCloseTo(0.3, 4);
      // Threads: 2 + 6 = 8
      expect(results[1].thread_count).toBe(8);
      // Memory: 10 + 40 = 50 MB
      expect(results[1].memory_mb).toBeCloseTo(50, 0);
    });

    it('handles three processes on same host without inflation', async () => {
      const makeProcess = (qid: string, cpuPerSample: number): SampleRow[] =>
        [0, 1, 2].map(i => ({
          hostname: 'host-a',
          sample_time: `2025-06-01 12:00:0${i}.000`,
          query_id: qid,
          initial_query_id: INITIAL_QID,
          elapsed: i,
          memory_usage: 10 * 1024 * 1024,
          peak_memory_usage: 10 * 1024 * 1024,
          read_rows: i * 1000, read_bytes: 0, written_rows: 0,
          thread_ids: [1, 2],
          profile_events: { ...PE_ZERO, OSCPUVirtualTimeMicroseconds: i * cpuPerSample },
        }));

      const rows = [
        ...makeProcess(INITIAL_QID, 1_000_000),
        ...makeProcess('child-sub-1', 1_000_000),
        ...makeProcess('child-sub-2', 1_000_000),
      ];

      const results = await seedAndQuery(rows);
      expect(results).toHaveLength(3);
      expect(results[0].d_cpu_cores).toBeCloseTo(0, 5);
      // 1 + 1 + 1 = 3 cores (not 9 or 27)
      expect(results[1].d_cpu_cores).toBeCloseTo(3, 4);
      expect(results[2].d_cpu_cores).toBeCloseTo(3, 4);
      expect(results[1].thread_count).toBe(6);
      expect(results[1].memory_mb).toBeCloseTo(30, 0);
    });

    it('sub-query with different cumulative values does not corrupt coordinator deltas', async () => {
      // Exact scenario that caused the original bug:
      // Coordinator has high cumulative CPU, sub-query has low.
      // Without per-process windowing, lag from sub-query to coordinator
      // produces a huge positive delta.
      const rows: SampleRow[] = [
        // coordinator t=0: cumulative CPU = 10M µs
        { hostname: 'host-a', sample_time: '2025-06-01 12:00:00.000',
          query_id: INITIAL_QID, initial_query_id: INITIAL_QID,
          elapsed: 0, memory_usage: 0, peak_memory_usage: 0,
          read_rows: 0, read_bytes: 0, written_rows: 0,
          thread_ids: [1],
          profile_events: { ...PE_ZERO, OSCPUVirtualTimeMicroseconds: 10_000_000 } },

        // sub-query t=0.5: cumulative CPU = 500K µs (much lower)
        { hostname: 'host-a', sample_time: '2025-06-01 12:00:00.500',
          query_id: 'child-sub-1', initial_query_id: INITIAL_QID,
          elapsed: 0.5, memory_usage: 0, peak_memory_usage: 0,
          read_rows: 0, read_bytes: 0, written_rows: 0,
          thread_ids: [10],
          profile_events: { ...PE_ZERO, OSCPUVirtualTimeMicroseconds: 500_000 } },

        // coordinator t=1: cumulative CPU = 12M µs
        // BUG: lag sees 500K (sub-query) → 12M = 11.5 cores!
        // FIX: lag sees 10M (own prev) → 12M = 2 cores ✓
        { hostname: 'host-a', sample_time: '2025-06-01 12:00:01.000',
          query_id: INITIAL_QID, initial_query_id: INITIAL_QID,
          elapsed: 1, memory_usage: 0, peak_memory_usage: 0,
          read_rows: 0, read_bytes: 0, written_rows: 0,
          thread_ids: [1],
          profile_events: { ...PE_ZERO, OSCPUVirtualTimeMicroseconds: 12_000_000 } },

        // sub-query t=1.5: cumulative CPU = 1.5M µs
        { hostname: 'host-a', sample_time: '2025-06-01 12:00:01.500',
          query_id: 'child-sub-1', initial_query_id: INITIAL_QID,
          elapsed: 1.5, memory_usage: 0, peak_memory_usage: 0,
          read_rows: 0, read_bytes: 0, written_rows: 0,
          thread_ids: [10],
          profile_events: { ...PE_ZERO, OSCPUVirtualTimeMicroseconds: 1_500_000 } },
      ];

      const results = await seedAndQuery(rows);

      for (const r of results) {
        // Must never exceed 3 cores (coordinator=2, sub-query=1)
        expect(r.d_cpu_cores).toBeLessThanOrEqual(3);
        expect(r.d_cpu_cores).toBeGreaterThanOrEqual(0);
      }
    });
  });

  // ── Multi-host ──

  describe('multi-host distributed query', () => {
    it('separates metrics per host correctly', async () => {
      const hostA: SampleRow[] = [0, 1, 2].map(i => ({
        hostname: 'host-a',
        sample_time: `2025-06-01 12:00:0${i}.000`,
        query_id: INITIAL_QID, initial_query_id: INITIAL_QID,
        elapsed: i, memory_usage: 10 * 1024 * 1024, peak_memory_usage: 20 * 1024 * 1024,
        read_rows: 0, read_bytes: 0, written_rows: 0,
        thread_ids: [1],
        profile_events: { ...PE_ZERO, OSCPUVirtualTimeMicroseconds: i * 1_000_000 },
      }));

      const hostB: SampleRow[] = [0, 1, 2].map(i => ({
        hostname: 'host-b',
        sample_time: `2025-06-01 12:00:0${i}.000`,
        query_id: 'child-host-b', initial_query_id: INITIAL_QID,
        elapsed: i, memory_usage: 40 * 1024 * 1024, peak_memory_usage: 80 * 1024 * 1024,
        read_rows: i * 10000, read_bytes: i * 50 * 1024 * 1024, written_rows: 0,
        thread_ids: [10, 11, 12, 13],
        profile_events: { ...PE_ZERO, OSCPUVirtualTimeMicroseconds: i * 3_000_000 },
      }));

      const results = await seedAndQuery([...hostA, ...hostB]);

      const byHost = new Map<string, HostProcessSample[]>();
      for (const r of results) {
        const arr = byHost.get(r.hostname) || [];
        arr.push(r);
        byHost.set(r.hostname, arr);
      }

      expect(byHost.size).toBe(2);

      const a = byHost.get('host-a')!;
      expect(a[1].d_cpu_cores).toBeCloseTo(1, 4);
      expect(a[1].thread_count).toBe(1);

      const b = byHost.get('host-b')!;
      expect(b[1].d_cpu_cores).toBeCloseTo(3, 4);
      expect(b[1].thread_count).toBe(4);
    });

    it('sums processes per host when multiple processes share a host', async () => {
      // host-a: coordinator (1 core) + local sub-query (2 cores) = 3 cores
      // host-b: remote sub-query (4 cores)
      const hostACoord: SampleRow[] = [0, 1, 2].map(i => ({
        hostname: 'host-a',
        sample_time: `2025-06-01 12:00:0${i}.000`,
        query_id: INITIAL_QID, initial_query_id: INITIAL_QID,
        elapsed: i, memory_usage: 5 * 1024 * 1024, peak_memory_usage: 10 * 1024 * 1024,
        read_rows: 0, read_bytes: 0, written_rows: 0,
        thread_ids: [1],
        profile_events: { ...PE_ZERO, OSCPUVirtualTimeMicroseconds: i * 1_000_000 },
      }));

      const hostASub: SampleRow[] = [0, 1, 2].map(i => ({
        hostname: 'host-a',
        sample_time: `2025-06-01 12:00:0${i}.000`,
        query_id: 'child-local', initial_query_id: INITIAL_QID,
        elapsed: i, memory_usage: 20 * 1024 * 1024, peak_memory_usage: 40 * 1024 * 1024,
        read_rows: i * 3000, read_bytes: 0, written_rows: 0,
        thread_ids: [10, 11, 12],
        profile_events: { ...PE_ZERO, OSCPUVirtualTimeMicroseconds: i * 2_000_000 },
      }));

      const hostBSub: SampleRow[] = [0, 1, 2].map(i => ({
        hostname: 'host-b',
        sample_time: `2025-06-01 12:00:0${i}.000`,
        query_id: 'child-remote', initial_query_id: INITIAL_QID,
        elapsed: i, memory_usage: 30 * 1024 * 1024, peak_memory_usage: 60 * 1024 * 1024,
        read_rows: i * 8000, read_bytes: 0, written_rows: 0,
        thread_ids: [20, 21, 22, 23, 24, 25],
        profile_events: { ...PE_ZERO, OSCPUVirtualTimeMicroseconds: i * 4_000_000 },
      }));

      const results = await seedAndQuery([...hostACoord, ...hostASub, ...hostBSub]);

      const byHost = new Map<string, HostProcessSample[]>();
      for (const r of results) {
        const arr = byHost.get(r.hostname) || [];
        arr.push(r);
        byHost.set(r.hostname, arr);
      }

      const a = byHost.get('host-a')!;
      expect(a[1].d_cpu_cores).toBeCloseTo(3, 4);   // 1 + 2
      expect(a[1].thread_count).toBe(4);             // 1 + 3
      expect(a[1].memory_mb).toBeCloseTo(25, 0);    // 5 + 20

      const b = byHost.get('host-b')!;
      expect(b[1].d_cpu_cores).toBeCloseTo(4, 4);
      expect(b[1].thread_count).toBe(6);
      expect(b[1].memory_mb).toBeCloseTo(30, 0);
    });
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// Suite 2: Cluster with organic sampling
// ─────────────────────────────────────────────────────────────────────────────

describe('X-Ray: cluster with organic sampling', () => {
  let ctx: SamplingClusterContext;
  let adapter: ClusterAwareAdapter;

  beforeAll(async () => {
    // 2 shards × 1 replica — distributed queries fan out to both nodes
    ctx = await startSamplingCluster({
      nodes: 2,
      shards: 2,
      clusterName: 'test',
      samplingIntervalSec: 1,
    });

    // Create a cluster-aware adapter pointed at ch1 (the coordinator node)
    const rawAdapter = new ClusterTestAdapter(ctx.clients[0]);
    adapter = new ClusterAwareAdapter(rawAdapter);
    adapter.setClusterName('test');
  }, 180_000);

  afterAll(async () => {
    if (ctx) {
      // Drop tracehouse on all nodes
      for (const client of ctx.clients) {
        try { await client.command({ query: 'DROP DATABASE IF EXISTS tracehouse' }); } catch {}
      }
      await stopSamplingCluster(ctx);
    }
  }, 60_000);

  /**
   * Start a distributed query that takes ~durationSec seconds using sleepEachRow.
   * Returns the query_id and a promise that resolves when the query completes.
   * The query is NOT awaited — it runs in the background so the sampler can
   * capture it in system.processes.
   */
  function startDistributedSlowQuery(durationSec: number): { queryId: string; done: Promise<unknown> } {
    const queryId = `xray-test-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

    // Run a distributed query across both shards using remote().
    // sleepEachRow(1) sleeps 1 second per row — with max_block_size=1,
    // each row is processed individually for ~durationSec wall time.
    const done = ctx.clients[0].command({
      query: `SELECT sleepEachRow(1)
              FROM remote('ch1,ch2', 'system', 'numbers', 'default', 'test')
              WHERE number < ${durationSec}
              SETTINGS max_block_size = 1`,
      query_id: queryId,
    });

    return { queryId, done };
  }

  /**
   * Query processes_history for a given query_id using the production
   * buildHostProcessSamplesSQL, resolved through ClusterAwareAdapter.
   */
  async function queryXRaySamples(queryId: string): Promise<HostProcessSample[]> {
    const sql = buildHostProcessSamplesSQL(queryId);
    const rows = await adapter.executeQuery<Record<string, unknown>>(sql);
    return rows.map(mapHostProcessSampleRow);
  }

  /**
   * Wait for processes_history to accumulate at least minSamples rows for a query.
   * The sampler MV fires every 1s, but there can be a short delay before rows appear.
   */
  async function waitForSamples(
    queryId: string,
    minSamples: number,
    timeoutMs = 15_000,
  ): Promise<void> {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      const result = await ctx.clients[0].query({
        query: `SELECT count() AS cnt
                FROM clusterAllReplicas('test', tracehouse.processes_history)
                WHERE query_id = '${queryId}' OR initial_query_id = '${queryId}'`,
        format: 'JSONEachRow',
      });
      const rows = await result.json<{ cnt: string }>();
      if (Number(rows[0].cnt) >= minSamples) return;
      // Force buffer table flush so samples become visible in processes_history
      for (const client of ctx.clients) {
        await client.command({ query: `OPTIMIZE TABLE tracehouse.processes_history_buffer` }).catch(() => {});
      }
      await new Promise(r => setTimeout(r, 1000));
    }
    throw new Error(`Timed out waiting for ${minSamples} samples for ${queryId}`);
  }

  it('captures samples from a distributed query running across 2 shards', async () => {
    const { queryId, done } = startDistributedSlowQuery(6);

    // Wait for samples while query runs in the background
    await waitForSamples(queryId, 2);

    const results = await queryXRaySamples(queryId);

    expect(results.length).toBeGreaterThan(0);
    for (const r of results) {
      expect(r.hostname).toBeTruthy();
    }

    await done;
  }, 60_000);

  it('CPU cores are reasonable (not inflated by cross-process lag)', async () => {
    const { queryId, done } = startDistributedSlowQuery(8);

    await waitForSamples(queryId, 3);

    const results = await queryXRaySamples(queryId);
    expect(results.length).toBeGreaterThanOrEqual(2);

    // On a 2-shard cluster, a distributed SELECT creates:
    // - coordinator process on ch1
    // - sub-query process on ch1 (local shard)
    // - sub-query process on ch2 (remote shard)
    //
    // sleepEachRow does very little CPU work — it mostly sleeps.
    // CPU per process should be well under 1 core.
    // With the bug, cross-process lag corruption could produce 50+ cores.
    for (const r of results) {
      expect(r.d_cpu_cores).toBeLessThan(2);
      expect(r.d_cpu_cores).toBeGreaterThanOrEqual(0);
    }

    await done;
  }, 60_000);

  it('produces samples from multiple hosts', async () => {
    const { queryId, done } = startDistributedSlowQuery(8);

    await waitForSamples(queryId, 4);

    const results = await queryXRaySamples(queryId);

    const hostnames = new Set(results.map(r => r.hostname));
    // With 2 shards, we expect samples from both ch1 and ch2
    expect(hostnames.size).toBe(2);

    await done;
  }, 60_000);

  it('elapsed time progresses across samples', async () => {
    const { queryId, done } = startDistributedSlowQuery(8);

    await waitForSamples(queryId, 3);

    const results = await queryXRaySamples(queryId);

    // Group by host and check that t increases
    const byHost = new Map<string, HostProcessSample[]>();
    for (const r of results) {
      const arr = byHost.get(r.hostname) || [];
      arr.push(r);
      byHost.set(r.hostname, arr);
    }

    for (const [_host, samples] of byHost) {
      if (samples.length < 2) continue;
      for (let i = 1; i < samples.length; i++) {
        expect(samples[i].t).toBeGreaterThan(samples[i - 1].t);
      }
    }

    await done;
  }, 60_000);
});
