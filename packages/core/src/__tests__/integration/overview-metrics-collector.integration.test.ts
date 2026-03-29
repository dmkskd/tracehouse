/**
 * Integration tests for MetricsCollector against a real ClickHouse instance.
 *
 * Uses testcontainers to spin up CH, creates shadow tables mirroring
 * system.metric_log / system.asynchronous_metric_log, seeds them with
 * known data, and validates the math in getHistoricalMetrics().
 *
 * These tests verify the CPU formula used by CLUSTER_CPU_TIMESERIES:
 *   The query reads OSUserTime + OSSystemTime (seconds) from asynchronous_metric_log,
 *   multiplies by 1_000_000 to get microseconds, then:
 *   CPU%  = (v / (cores × 1_000_000 × interval_s)) × 100
 *   Mem%  = (MemoryTracking / OSMemoryTotal) × 100
 *   Disk  = OSReadBytes / interval_s, OSWriteBytes / interval_s
 *   Net   = NetworkSendBytes / interval_s, NetworkReceiveBytes / interval_s
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { startClickHouse, stopClickHouse, type TestClickHouseContext } from './setup/clickhouse-container.js';
import {
  createShadowDatabase,
  dropShadowDatabase,
  truncateShadowTables,
  seedMetricLog,
  seedAsyncMetricLog,
} from './setup/shadow-tables.js';
import { ShadowAdapter } from './setup/shadow-adapter.js';
import { MetricsCollector } from '../../services/metrics-collector.js';

// Container startup can take 15-30s on first pull
const CONTAINER_TIMEOUT = 120_000;

describe('MetricsCollector integration (shadow tables)', { tags: ['observability'] }, () => {
  let ctx: TestClickHouseContext;
  let collector: MetricsCollector;

  beforeAll(async () => {
    ctx = await startClickHouse();
    await createShadowDatabase(ctx.client);

    // Wire up the collector with a ShadowAdapter so production queries
    // transparently hit test_shadow.* instead of system.*
    const shadowAdapter = new ShadowAdapter(ctx.client);
    collector = new MetricsCollector(shadowAdapter);
  }, CONTAINER_TIMEOUT);

  afterAll(async () => {
    if (ctx) {
      await dropShadowDatabase(ctx.client);
      await stopClickHouse(ctx);
    }
  }, 30_000);

  /**
   * Helper: seed asynchronous_metric_log with OSUserTime/OSSystemTime rows
   * that produce a known CPU microsecond value per interval.
   *
   * The CLUSTER_CPU_TIMESERIES query sums (OSUserTime + OSSystemTime) per event_time
   * then multiplies by 1_000_000. So to get `targetUs` microseconds of CPU time
   * for a given timestamp, we seed OSUserTime = targetUs / 1_000_000 (in seconds).
   */
  async function seedCpuTime(times: string[], targetUs: number): Promise<void> {
    const rows = times.flatMap(t => [
      { event_time: t, metric: 'OSUserTime', value: targetUs / 1_000_000 },
      { event_time: t, metric: 'OSSystemTime', value: 0 },
    ]);
    await seedAsyncMetricLog(ctx.client, rows);
  }

  // -----------------------------------------------------------------------
  // CPU calculation
  // -----------------------------------------------------------------------

  describe('CPU usage calculation', () => {
    it('computes correct CPU% for known values (50% usage on 4 cores)', async () => {
      await truncateShadowTables(ctx.client);

      // Scenario: 4 cores, 1-second intervals (hardcoded), 2_000_000 µs CPU time per tick
      // Expected: (2_000_000 / (4 × 1_000_000 × 1)) × 100 = 50%
      const times = Array.from({ length: 5 }, (_, i) => {
        const sec = String(i).padStart(2, '0');
        return `2025-06-01 12:00:${sec}`;
      });

      // Seed CPU time into asynchronous_metric_log (OSUserTime/OSSystemTime)
      await seedCpuTime(times, 2_000_000);

      // Seed metric_log for memory/disk/network (non-CPU columns)
      await seedMetricLog(ctx.client, times.map(t => ({
        event_time: t,
        memory_tracking: 1_073_741_824, // 1 GiB
        disk_read: 10_000_000,
        disk_write: 5_000_000,
        net_send: 1_000_000,
        net_recv: 2_000_000,
      })));

      // Seed CPU cores = 4 and total RAM = 16 GiB
      await seedAsyncMetricLog(ctx.client, [
        { event_time: times[0], metric: 'NumberOfCPUCores', value: 4 },
        { event_time: times[0], metric: 'OSMemoryTotal', value: 17_179_869_184 },
      ]);

      const from = new Date('2025-06-01T11:59:50Z');
      const to = new Date('2025-06-01T12:00:10Z');
      const points = await collector.getHistoricalMetrics(from, to);

      expect(points.length).toBeGreaterThan(0);

      for (const p of points) {
        expect(p.cpu_usage).toBeCloseTo(50, 0);
      }
    });

    it('clamps CPU to 100% when virtual time exceeds wall time (containerized)', async () => {
      await truncateShadowTables(ctx.client);

      // Scenario: 2 cores, 1-second intervals, 3_000_000 µs CPU time
      // Raw: (3_000_000 / (2 × 1_000_000 × 1)) × 100 = 150%
      // Clamped to 100%
      const times = Array.from({ length: 4 }, (_, i) => {
        const sec = String(i).padStart(2, '0');
        return `2025-06-01 13:00:${sec}`;
      });

      await seedCpuTime(times, 3_000_000);

      await seedAsyncMetricLog(ctx.client, [
        { event_time: times[0], metric: 'NumberOfCPUCores', value: 2 },
        { event_time: times[0], metric: 'OSMemoryTotal', value: 8_589_934_592 },
      ]);

      const points = await collector.getHistoricalMetrics(
        new Date('2025-06-01T12:59:50Z'),
        new Date('2025-06-01T13:00:10Z'),
      );

      expect(points.length).toBeGreaterThan(0);
      for (const p of points) {
        expect(p.cpu_usage).toBe(100);
      }
    });

    it('handles single-core scenario correctly', async () => {
      await truncateShadowTables(ctx.client);

      // 1 core, 500_000 µs → 50%
      const times = Array.from({ length: 3 }, (_, i) => {
        const sec = String(i).padStart(2, '0');
        return `2025-06-01 14:00:${sec}`;
      });

      await seedCpuTime(times, 500_000);

      await seedAsyncMetricLog(ctx.client, [
        { event_time: times[0], metric: 'NumberOfCPUCores', value: 1 },
        { event_time: times[0], metric: 'OSMemoryTotal', value: 4_294_967_296 },
      ]);

      const points = await collector.getHistoricalMetrics(
        new Date('2025-06-01T13:59:50Z'),
        new Date('2025-06-01T14:00:10Z'),
      );

      expect(points.length).toBeGreaterThan(0);
      for (const p of points) {
        expect(p.cpu_usage).toBeCloseTo(50, 0);
      }
    });

    it('prefers CGroupMaxCPU over NumberOfCPUCores in k8s/containers', async () => {
      await truncateShadowTables(ctx.client);

      // Scenario: host has 12 cores, but cgroup limit is 3 cores
      // CPU time = 1_500_000 µs per tick (1-second intervals)
      // With host cores (12): (1_500_000 / (12 × 1_000_000)) × 100 = 12.5% — WRONG
      // With cgroup cores (3): (1_500_000 / (3 × 1_000_000)) × 100 = 50% — CORRECT
      const times = Array.from({ length: 4 }, (_, i) => {
        const sec = String(i).padStart(2, '0');
        return `2025-06-01 20:00:${sec}`;
      });

      await seedCpuTime(times, 1_500_000);

      // Seed metric_log for memory
      await seedMetricLog(ctx.client, times.map(t => ({
        event_time: t,
        memory_tracking: 1_073_741_824,
      })));

      await seedAsyncMetricLog(ctx.client, [
        { event_time: times[0], metric: 'NumberOfCPUCores', value: 12 },
        { event_time: times[0], metric: 'CGroupMaxCPU', value: 3 },
        { event_time: times[0], metric: 'OSMemoryTotal', value: 17_179_869_184 },
      ]);

      const points = await collector.getHistoricalMetrics(
        new Date('2025-06-01T19:59:50Z'),
        new Date('2025-06-01T20:00:10Z'),
      );

      const stablePoints = points.slice(1);
      expect(stablePoints.length).toBeGreaterThan(0);
      for (const p of stablePoints) {
        // Should be ~50% (cgroup 3 cores), not ~12.5% (host 12 cores)
        expect(p.cpu_usage).toBeCloseTo(50, 0);
      }
    });

    it('falls back to NumberOfCPUCores when CGroupMaxCPU is absent', async () => {
      await truncateShadowTables(ctx.client);

      // Scenario: bare-metal, no cgroup limit — 4 cores, 2_000_000 µs → 50%
      const times = Array.from({ length: 4 }, (_, i) => {
        const sec = String(i).padStart(2, '0');
        return `2025-06-01 21:00:${sec}`;
      });

      await seedCpuTime(times, 2_000_000);

      await seedAsyncMetricLog(ctx.client, [
        { event_time: times[0], metric: 'NumberOfCPUCores', value: 4 },
        { event_time: times[0], metric: 'OSMemoryTotal', value: 8_589_934_592 },
        // No CGroupMaxCPU seeded
      ]);

      const points = await collector.getHistoricalMetrics(
        new Date('2025-06-01T20:59:50Z'),
        new Date('2025-06-01T21:00:10Z'),
      );

      expect(points.length).toBeGreaterThan(0);
      for (const p of points) {
        expect(p.cpu_usage).toBeCloseTo(50, 0);
      }
    });

    it('ignores CGroupMaxCPU when it exceeds NumberOfCPUCores', async () => {
      await truncateShadowTables(ctx.client);

      // Scenario: CGroupMaxCPU reports larger than host cores (no real limit)
      // Should fall back to NumberOfCPUCores = 4
      // CPU time = 2_000_000 µs → 50%
      const times = Array.from({ length: 4 }, (_, i) => {
        const sec = String(i).padStart(2, '0');
        return `2025-06-01 22:00:${sec}`;
      });

      await seedCpuTime(times, 2_000_000);

      await seedAsyncMetricLog(ctx.client, [
        { event_time: times[0], metric: 'NumberOfCPUCores', value: 4 },
        { event_time: times[0], metric: 'CGroupMaxCPU', value: 16 }, // larger than host cores
        { event_time: times[0], metric: 'OSMemoryTotal', value: 8_589_934_592 },
      ]);

      const points = await collector.getHistoricalMetrics(
        new Date('2025-06-01T21:59:50Z'),
        new Date('2025-06-01T22:00:10Z'),
      );

      expect(points.length).toBeGreaterThan(0);
      for (const p of points) {
        // Should use 4 cores (host), not 16 (bogus cgroup)
        expect(p.cpu_usage).toBeCloseTo(50, 0);
      }
    });
  });

  // -----------------------------------------------------------------------
  // Memory values
  // -----------------------------------------------------------------------

  describe('Memory tracking', () => {
    it('returns correct memory_used and memory_total', async () => {
      await truncateShadowTables(ctx.client);

      const time = '2025-06-01 15:00:00';
      const memUsed = 4_294_967_296; // 4 GiB
      const memTotal = 17_179_869_184; // 16 GiB

      await seedMetricLog(ctx.client, [
        { event_time: time, memory_tracking: memUsed },
      ]);

      // Seed CPU time so cpuRows is the base (not memoryRows)
      await seedCpuTime([time], 0);

      await seedAsyncMetricLog(ctx.client, [
        { event_time: time, metric: 'OSMemoryTotal', value: memTotal },
        { event_time: time, metric: 'NumberOfCPUCores', value: 4 },
      ]);

      const points = await collector.getHistoricalMetrics(
        new Date('2025-06-01T14:59:50Z'),
        new Date('2025-06-01T15:00:10Z'),
      );

      expect(points.length).toBe(1);
      expect(points[0].memory_used).toBe(memUsed);
      expect(points[0].memory_total).toBe(memTotal);
    });

    it('memory percentage can be derived: (MemoryTracking / OSMemoryTotal) × 100', async () => {
      await truncateShadowTables(ctx.client);

      // Scenario: 4 GiB used out of 16 GiB total → 25%
      const time = '2025-06-01 15:30:00';
      const memUsed = 4_294_967_296;   // 4 GiB
      const memTotal = 17_179_869_184; // 16 GiB

      await seedMetricLog(ctx.client, [
        { event_time: time, memory_tracking: memUsed },
      ]);

      await seedCpuTime([time], 0);

      await seedAsyncMetricLog(ctx.client, [
        { event_time: time, metric: 'OSMemoryTotal', value: memTotal },
        { event_time: time, metric: 'NumberOfCPUCores', value: 4 },
      ]);

      const points = await collector.getHistoricalMetrics(
        new Date('2025-06-01T15:29:50Z'),
        new Date('2025-06-01T15:30:10Z'),
      );

      expect(points.length).toBe(1);
      // Verify the raw values allow correct percentage derivation
      const memPct = (points[0].memory_used / points[0].memory_total) * 100;
      expect(memPct).toBeCloseTo(25, 0);
    });
  });

  // -----------------------------------------------------------------------
  // Disk I/O
  // -----------------------------------------------------------------------

  describe('Disk I/O rates', () => {
    it('returns raw per-interval disk bytes', async () => {
      await truncateShadowTables(ctx.client);

      const times = ['2025-06-01 16:00:00', '2025-06-01 16:00:01'];
      const diskRead = 52_428_800; // 50 MiB
      const diskWrite = 10_485_760; // 10 MiB

      await seedMetricLog(ctx.client, times.map(t => ({
        event_time: t,
        disk_read: diskRead,
        disk_write: diskWrite,
      })));

      await seedCpuTime(times, 1_000_000);

      await seedAsyncMetricLog(ctx.client, [
        { event_time: times[0], metric: 'NumberOfCPUCores', value: 4 },
        { event_time: times[0], metric: 'OSMemoryTotal', value: 17_179_869_184 },
      ]);

      const points = await collector.getHistoricalMetrics(
        new Date('2025-06-01T15:59:50Z'),
        new Date('2025-06-01T16:00:10Z'),
      );

      expect(points.length).toBe(2);
      // metric_log stores per-interval values; MetricsCollector returns them as rates
      for (const p of points) {
        expect(p.disk_read_rate).toBe(diskRead);
        expect(p.disk_write_rate).toBe(diskWrite);
      }
    });
  });

  // -----------------------------------------------------------------------
  // Network I/O
  // -----------------------------------------------------------------------

  describe('Network I/O rates', () => {
    it('returns raw per-interval network bytes', async () => {
      await truncateShadowTables(ctx.client);

      const times = ['2025-06-01 17:00:00', '2025-06-01 17:00:01'];
      const netSend = 1_048_576;
      const netRecv = 2_097_152;

      await seedMetricLog(ctx.client, times.map(t => ({
        event_time: t,
        net_send: netSend,
        net_recv: netRecv,
      })));

      await seedCpuTime(times, 1_000_000);

      await seedAsyncMetricLog(ctx.client, [
        { event_time: times[0], metric: 'NumberOfCPUCores', value: 4 },
        { event_time: times[0], metric: 'OSMemoryTotal', value: 17_179_869_184 },
      ]);

      const points = await collector.getHistoricalMetrics(
        new Date('2025-06-01T16:59:50Z'),
        new Date('2025-06-01T17:00:10Z'),
      );

      expect(points.length).toBe(2);
      for (const p of points) {
        expect(p.network_send_rate).toBe(netSend);
        expect(p.network_recv_rate).toBe(netRecv);
      }
    });
  });

  // -----------------------------------------------------------------------
  // Edge cases
  // -----------------------------------------------------------------------

  describe('Edge cases', () => {
    it('returns empty array when no data in time range', async () => {
      await truncateShadowTables(ctx.client);

      const points = await collector.getHistoricalMetrics(
        new Date('2099-01-01T00:00:00Z'),
        new Date('2099-01-01T01:00:00Z'),
      );

      expect(points).toEqual([]);
    });

    it('handles zero CPU cores gracefully (defaults to 1)', async () => {
      await truncateShadowTables(ctx.client);

      const time = '2025-06-01 18:00:00';

      // Seed CPU time = 1_000_000 µs via asynchronous_metric_log
      await seedCpuTime([time], 1_000_000);

      // Seed OSMemoryTotal but NO NumberOfCPUCores anywhere —
      // collector should exhaust all fallbacks and default to 1
      await seedAsyncMetricLog(ctx.client, [
        { event_time: time, metric: 'OSMemoryTotal', value: 8_589_934_592 },
      ]);

      const points = await collector.getHistoricalMetrics(
        new Date('2025-06-01T17:59:50Z'),
        new Date('2025-06-01T18:00:10Z'),
      );

      // Should not throw, should produce a result
      expect(points.length).toBe(1);
      // With fallback to 1 core: (1_000_000 / (1 × 1_000_000 × 1)) × 100 = 100%
      expect(points[0].cpu_usage).toBeCloseTo(100, 0);
    });

    it('first row uses default 1s interval when interval_ms is 0', async () => {
      await truncateShadowTables(ctx.client);

      // Single row — interval_ms is hardcoded to 1000ms in the query
      // With 4 cores and 2_000_000 µs: (2_000_000 / (4 × 1_000_000 × 1)) × 100 = 50%
      const time = '2025-06-01 19:00:00';

      await seedCpuTime([time], 2_000_000);

      await seedAsyncMetricLog(ctx.client, [
        { event_time: time, metric: 'NumberOfCPUCores', value: 4 },
        { event_time: time, metric: 'OSMemoryTotal', value: 8_589_934_592 },
      ]);

      const points = await collector.getHistoricalMetrics(
        new Date('2025-06-01T18:59:50Z'),
        new Date('2025-06-01T19:00:10Z'),
      );

      expect(points.length).toBe(1);
      // interval_ms is hardcoded to 1000 → 50% CPU
      expect(points[0].cpu_usage).toBeCloseTo(50, 0);
    });
  });

  // -----------------------------------------------------------------------
  // getServerMetrics (snapshot / real-time mode)
  // -----------------------------------------------------------------------

  describe('getServerMetrics (snapshot mode against real system tables)', () => {
    it('returns valid shape with real system data', async () => {
      // Use the real adapter (not shadow) to hit actual system tables
      const realCollector = new MetricsCollector(ctx.adapter);
      const metrics = await realCollector.getServerMetrics();

      expect(metrics).toHaveProperty('timestamp');
      expect(metrics).toHaveProperty('cpu_usage');
      expect(metrics).toHaveProperty('memory_used');
      expect(metrics).toHaveProperty('memory_total');
      expect(metrics).toHaveProperty('uptime_seconds');
      expect(typeof metrics.cpu_usage).toBe('number');
      expect(metrics.cpu_usage).toBeGreaterThanOrEqual(0);
      expect(metrics.memory_total).toBeGreaterThan(0);
      expect(metrics.uptime_seconds).toBeGreaterThanOrEqual(0);
    });

    it('cpu_usage is between 0 and a reasonable upper bound', async () => {
      const realCollector = new MetricsCollector(ctx.adapter);
      const metrics = await realCollector.getServerMetrics();

      // Snapshot CPU uses LoadAverage1 / cores, capped at 100
      expect(metrics.cpu_usage).toBeGreaterThanOrEqual(0);
      expect(metrics.cpu_usage).toBeLessThanOrEqual(100);
    });

    it('memory_used <= memory_total', async () => {
      const realCollector = new MetricsCollector(ctx.adapter);
      const metrics = await realCollector.getServerMetrics();

      expect(metrics.memory_used).toBeLessThanOrEqual(metrics.memory_total);
    });
  });
});
