import { describe, expect, it } from 'vitest';
import type { IClickHouseAdapter, TaggedQuery } from '../../adapters/types.js';
import { buildQuery } from '../../queries/builder.js';
import { buildSurfaceTimeFilter } from '../../queries/surface-queries.js';
import { MetricsCollector } from '../metrics-collector.js';
import { TimelineService } from '../timeline-service.js';

class RecordingAdapter implements IClickHouseAdapter {
  readonly queries: string[] = [];

  async executeQuery<T extends Record<string, unknown>>(sql: TaggedQuery): Promise<T[]> {
    this.queries.push(sql);
    return [];
  }
}

class HeterogeneousCapacityAdapter implements IClickHouseAdapter {
  readonly queries: string[] = [];

  async executeQuery<T extends Record<string, unknown>>(sql: TaggedQuery): Promise<T[]> {
    this.queries.push(sql);
    if (sql.includes('TimeTravel:serverMemory')) {
      return [
        { t: '2026-07-27 14:00:00', v: 40 * 1024 ** 3 },
      ] as unknown as T[];
    }
    if (sql.includes('TimeTravel:serverCpu')) {
      return [
        { t: '2026-07-27 14:00:00', v: 10_000_000, interval_ms: 1000 },
      ] as unknown as T[];
    }
    if (sql.includes('TimeTravel:perHostCpu')) {
      return [
        { host: 'host-a', t: '2026-07-27 14:00:00', v: 3_000_000, interval_ms: 1000 },
        { host: 'host-b', t: '2026-07-27 14:00:00', v: 7_000_000, interval_ms: 1000 },
      ] as unknown as T[];
    }
    if (sql.includes('TimeTravel:totalRam')) {
      return [
        { host: 'host-a', value: 12 * 1024 ** 3 },
        { host: 'host-b', value: 32 * 1024 ** 3 },
      ] as unknown as T[];
    }
    if (sql.includes('TimeTravel:cpuCapacityHistorical')) {
      return [
        { host: 'host-a', cgroup_max_cpu: 3, reported_cores: 12, os_cpu_count: 12 },
        { host: 'host-b', cgroup_max_cpu: 8, reported_cores: 32, os_cpu_count: 32 },
      ] as unknown as T[];
    }
    return [];
  }
}

class FourHostCgroupCapacityAdapter implements IClickHouseAdapter {
  readonly queries: string[] = [];

  async executeQuery<T extends Record<string, unknown>>(sql: TaggedQuery): Promise<T[]> {
    this.queries.push(sql);
    if (sql.includes('TimeTravel:totalRam')) {
      return ['host-a', 'host-b', 'host-c', 'host-d'].map(host => ({
        host,
        value: 12 * 1024 ** 3,
      })) as unknown as T[];
    }
    if (sql.includes('TimeTravel:cpuCapacityHistorical')) {
      return ['host-a', 'host-b', 'host-c', 'host-d'].map(host => ({
        host,
        cgroup_max_cpu: 3,
        reported_cores: 0,
        os_cpu_count: 12,
      })) as unknown as T[];
    }
    return [];
  }
}

class CurrentCapacityFallbackAdapter implements IClickHouseAdapter {
  async executeQuery<T extends Record<string, unknown>>(sql: TaggedQuery): Promise<T[]> {
    if (sql.includes('TimeTravel:totalRam')) {
      return [
        { host: 'host-a', value: 12 * 1024 ** 3 },
        { host: 'host-b', value: 12 * 1024 ** 3 },
      ] as unknown as T[];
    }
    if (sql.includes('TimeTravel:cpuCapacityHistorical')) {
      return [
        { host: 'host-a', cgroup_max_cpu: 2.5, reported_cores: 12, os_cpu_count: 12 },
      ] as unknown as T[];
    }
    if (sql.includes('TimeTravel:cpuCapacityCurrent')) {
      return [
        { host: 'host-a', cgroup_max_cpu: 4, reported_cores: 12, os_cpu_count: 12 },
        { host: 'host-b', cgroup_max_cpu: 4, reported_cores: 16, os_cpu_count: 16 },
      ] as unknown as T[];
    }
    return [];
  }
}

class IncompleteCapacityAdapter implements IClickHouseAdapter {
  async executeQuery<T extends Record<string, unknown>>(sql: TaggedQuery): Promise<T[]> {
    if (sql.includes('TimeTravel:serverCpu')) {
      return [
        { t: '2026-07-27 14:00:00', v: 7_000_000, interval_ms: 1000 },
      ] as unknown as T[];
    }
    if (sql.includes('TimeTravel:totalRam')) {
      return [
        { host: 'host-a', value: 12 * 1024 ** 3 },
        { host: 'host-b', value: 12 * 1024 ** 3 },
      ] as unknown as T[];
    }
    if (sql.includes('TimeTravel:cpuCapacityHistorical')) {
      return [
        { host: 'host-a', cgroup_max_cpu: 3, reported_cores: 12, os_cpu_count: 12 },
      ] as unknown as T[];
    }
    return [];
  }
}

class ClusterDetectionTransitionAdapter implements IClickHouseAdapter {
  clusterDetected = false;
  totalRamQueries = 0;

  async executeQuery<T extends Record<string, unknown>>(sql: TaggedQuery): Promise<T[]> {
    const hosts = this.clusterDetected
      ? ['host-a', 'host-b', 'host-c', 'host-d']
      : ['host-a'];

    if (sql.includes('TimeTravel:totalRam')) {
      this.totalRamQueries += 1;
      return hosts.map(host => ({
        host,
        value: 12 * 1024 ** 3,
      })) as unknown as T[];
    }
    if (sql.includes('TimeTravel:cpuCapacityHistorical')) {
      return hosts.map(host => ({
        host,
        cgroup_max_cpu: 3,
        reported_cores: 0,
        os_cpu_count: 12,
      })) as unknown as T[];
    }
    if (sql.includes('TimeTravel:perHostCpu')) {
      return hosts.map(host => ({
        host,
        t: '2026-07-27 14:00:00',
        v: 1_000_000,
        interval_ms: 1000,
      })) as unknown as T[];
    }
    return [];
  }
}

class IncompleteRamCapacityAdapter implements IClickHouseAdapter {
  async executeQuery<T extends Record<string, unknown>>(sql: TaggedQuery): Promise<T[]> {
    if (sql.includes('TimeTravel:serverMemory')) {
      return [
        { t: '2026-07-27 14:00:00', v: 20 * 1024 ** 3 },
      ] as unknown as T[];
    }
    if (sql.includes('TimeTravel:totalRam')) {
      return [
        { host: 'host-a', value: 12 * 1024 ** 3 },
      ] as unknown as T[];
    }
    if (sql.includes('TimeTravel:cpuCapacityHistorical')) {
      return [
        { host: 'host-a', cgroup_max_cpu: 3, reported_cores: 12, os_cpu_count: 12 },
        { host: 'host-b', cgroup_max_cpu: 3, reported_cores: 12, os_cpu_count: 12 },
      ] as unknown as T[];
    }
    return [];
  }
}

class HeterogeneousSpikeAdapter implements IClickHouseAdapter {
  readonly queries: string[] = [];

  async executeQuery<T extends Record<string, unknown>>(sql: TaggedQuery): Promise<T[]> {
    this.queries.push(sql);
    if (sql.includes('TimeTravel:cpuSpikes')) {
      return [
        { t: '2026-07-27 14:00:00', cpu_us: 24_000_000, interval_ms: 1000 },
      ] as unknown as T[];
    }
    if (sql.includes('TimeTravel:cpuCapacityHistorical')) {
      return [
        { host: 'host-a', cgroup_max_cpu: 0, reported_cores: 8, os_cpu_count: 8 },
        { host: 'host-b', cgroup_max_cpu: 0, reported_cores: 32, os_cpu_count: 32 },
      ] as unknown as T[];
    }
    return [];
  }
}

describe('UTC boundaries across timeline consumers', () => {
  it('renders Analytics Surface custom bounds explicitly in UTC', () => {
    const filter = buildSurfaceTimeFilter('event_time', {
      startTime: '2026-07-27T14:13:00+01:00',
      endTime: '2026-07-27T16:05:00+02:00',
    });

    expect(buildQuery(`WHERE ${filter.clause}`, filter.params)).toBe(
      "WHERE event_time BETWEEN toDateTime('2026-07-27 13:13:00', 'UTC') AND toDateTime('2026-07-27 14:05:00', 'UTC')",
    );
  });

  it('renders Time Travel bounds explicitly in UTC', async () => {
    const adapter = new RecordingAdapter();
    const service = new TimelineService(adapter);

    await service.getTimeline({
      timestamp: new Date('2026-07-27T14:00:00.000Z'),
      windowSeconds: 60,
      includeRunning: false,
    });

    const timeBoundQueries = adapter.queries.filter(sql =>
      sql.includes("toDateTime('2026-07-27 13:59:00', 'UTC')"),
    );
    expect(timeBoundQueries.length).toBeGreaterThan(0);
    for (const sql of timeBoundQueries) {
      expect(sql).toContain("toDateTime('2026-07-27 13:59:00', 'UTC')");
      expect(sql).toContain("toDateTime('2026-07-27 14:01:00', 'UTC')");
    }
  });

  it('filters Time Travel queries to every selected host', async () => {
    const adapter = new RecordingAdapter();
    const service = new TimelineService(adapter);

    await service.getTimeline({
      timestamp: new Date('2026-07-27T14:00:00.000Z'),
      windowSeconds: 60,
      includeRunning: false,
      hostname: ['host-a', 'host-b'],
    });

    expect(adapter.queries.some(sql =>
      sql.includes("hostname() IN ('host-a', 'host-b')"),
    )).toBe(true);
  });

  it('reports selected-host capacity totals without changing per-host chart references', async () => {
    const service = new TimelineService(new HeterogeneousCapacityAdapter());

    const result = await service.getTimeline({
      timestamp: new Date('2026-07-27T14:00:00.000Z'),
      windowSeconds: 60,
      includeRunning: false,
    });

    expect(result.host_count).toBe(2);
    expect(result.server_total_ram).toBe(12 * 1024 ** 3);
    expect(result.total_ram).toBe(44 * 1024 ** 3);
    expect(result.cpu_cores).toBe(3);
    expect(result.total_cpu_cores).toBe(11);
  });

  it('uses sum/sum semantics for Overall usage on heterogeneous hosts', async () => {
    const adapter = new HeterogeneousCapacityAdapter();
    const service = new TimelineService(adapter);

    const memoryResult = await service.getTimeline({
      timestamp: new Date('2026-07-27T14:00:00.000Z'),
      windowSeconds: 60,
      includeRunning: false,
      activeMetric: 'memory',
    });
    const cpuResult = await service.getTimeline({
      timestamp: new Date('2026-07-27T14:00:00.000Z'),
      windowSeconds: 60,
      includeRunning: false,
      activeMetric: 'cpu',
    });

    expect(memoryResult.server_memory[0]?.v).toBe(40 * 1024 ** 3);
    expect(cpuResult.server_cpu[0]?.v).toBe(10_000_000);
    expect(cpuResult.total_cpu_cores).toBe(11);
    expect(cpuResult.per_host_cpu_cores).toEqual({
      'host-a': 3,
      'host-b': 8,
    });
    expect(adapter.queries.some(sql =>
      sql.includes('sum(CurrentMetric_MemoryTracking) AS v'),
    )).toBe(true);
    expect(adapter.queries.some(sql =>
      sql.includes('sum(v) AS v'),
    )).toBe(true);
  });

  it('sums historical CGroupMaxCPU for every host when NumberOfCPUCores is absent', async () => {
    const adapter = new FourHostCgroupCapacityAdapter();
    const service = new TimelineService(adapter);

    const result = await service.getTimeline({
      timestamp: new Date('2026-07-27T14:00:00.000Z'),
      windowSeconds: 60,
      includeRunning: false,
      activeMetric: 'cpu',
    });

    expect(result.host_count).toBe(4);
    expect(result.cpu_cores).toBe(3);
    expect(result.total_cpu_cores).toBe(12);
    expect(result.cpu_capacity_complete).toBe(true);
    expect(result.cpu_capacity_missing_hosts).toBeUndefined();
    expect(adapter.queries.some(sql =>
      sql.includes("metric = 'CGroupMaxCPU'"),
    )).toBe(true);
    expect(adapter.queries.some(sql =>
      sql.includes('TimeTravel:cpuCapacityCurrent'),
    )).toBe(false);
  });

  it('refreshes one-host metadata cached before cluster detection', async () => {
    const adapter = new ClusterDetectionTransitionAdapter();
    const service = new TimelineService(adapter);
    const options = {
      timestamp: new Date('2026-07-27T14:00:00.000Z'),
      windowSeconds: 60,
      includeRunning: false,
      activeMetric: 'cpu' as const,
    };

    const localResult = await service.getTimeline(options);
    expect(localResult.host_count).toBe(1);
    expect(localResult.total_ram).toBe(12 * 1024 ** 3);
    expect(localResult.total_cpu_cores).toBe(3);

    adapter.clusterDetected = true;
    const clusterResult = await service.getTimeline(options);

    expect(adapter.totalRamQueries).toBe(2);
    expect(clusterResult.host_count).toBe(4);
    expect(clusterResult.total_ram).toBe(48 * 1024 ** 3);
    expect(clusterResult.total_cpu_cores).toBe(12);
    expect(clusterResult.cpu_capacity_complete).toBe(true);
  });

  it('omits the RAM denominator when any selected host capacity is missing', async () => {
    const result = await new TimelineService(new IncompleteRamCapacityAdapter()).getTimeline({
      timestamp: new Date('2026-07-27T14:00:00.000Z'),
      windowSeconds: 60,
      includeRunning: false,
      activeMetric: 'memory',
    });

    expect(result.server_memory[0]?.v).toBe(20 * 1024 ** 3);
    expect(result.server_total_ram).toBe(0);
    expect(result.total_ram).toBeUndefined();
    expect(result.ram_capacity_complete).toBe(false);
    expect(result.ram_capacity_missing_hosts).toEqual(['host-b']);
    expect(result.host_count).toBe(2);
  });

  it('uses total usage over total capacity for heterogeneous CPU spike analysis', async () => {
    const adapter = new HeterogeneousSpikeAdapter();
    const result = await new TimelineService(adapter)
      .getCpuSpikeAnalysis(
        new Date('2026-07-27T13:59:00.000Z'),
        new Date('2026-07-27T14:01:00.000Z'),
      );

    expect(result.cpu_cores).toBe(40);
    expect(result.overall_peak_pct).toBe(60);
    expect(result.points_above_100).toBe(0);
    expect(adapter.queries.some(sql =>
      sql.includes('sum(cpu_us) AS cpu_us'),
    )).toBe(true);
  });

  it('preserves fractional historical cgroup capacity and fills missing hosts from current metrics', async () => {
    const service = new TimelineService(new CurrentCapacityFallbackAdapter());

    const result = await service.getTimeline({
      timestamp: new Date('2026-07-27T14:00:00.000Z'),
      windowSeconds: 60,
      includeRunning: false,
      activeMetric: 'cpu',
    });

    expect(result.per_host_cpu_cores).toBeUndefined();
    expect(result.cpu_cores).toBe(2.5);
    expect(result.total_cpu_cores).toBe(6.5);
    expect(result.cpu_capacity_complete).toBe(true);
    expect(result.cpu_capacity_approximate).toBe(true);
  });

  it('uses reported cores before the OS per-core count, then falls back to the OS count', async () => {
    class BareMetalCapacityAdapter extends FourHostCgroupCapacityAdapter {
      override async executeQuery<T extends Record<string, unknown>>(sql: TaggedQuery): Promise<T[]> {
        if (sql.includes('TimeTravel:cpuCapacityHistorical')) {
          return [
            { host: 'host-a', cgroup_max_cpu: 0, reported_cores: 10, os_cpu_count: 12 },
            { host: 'host-b', cgroup_max_cpu: 0, reported_cores: 16, os_cpu_count: 16 },
            { host: 'host-c', cgroup_max_cpu: 0, reported_cores: 0, os_cpu_count: 8 },
            { host: 'host-d', cgroup_max_cpu: 0, reported_cores: 0, os_cpu_count: 4 },
          ] as unknown as T[];
        }
        return super.executeQuery<T>(sql);
      }
    }

    const result = await new TimelineService(new BareMetalCapacityAdapter()).getTimeline({
      timestamp: new Date('2026-07-27T14:00:00.000Z'),
      windowSeconds: 60,
      includeRunning: false,
      activeMetric: 'cpu',
    });

    expect(result.cpu_cores).toBe(4);
    expect(result.total_cpu_cores).toBe(38);
    expect(result.cpu_capacity_complete).toBe(true);
  });

  it('does not clamp or expose a percentage denominator when any host capacity is missing', async () => {
    const result = await new TimelineService(new IncompleteCapacityAdapter()).getTimeline({
      timestamp: new Date('2026-07-27T14:00:00.000Z'),
      windowSeconds: 60,
      includeRunning: false,
      activeMetric: 'cpu',
    });

    expect(result.server_cpu[0]?.v).toBe(7_000_000);
    expect(result.cpu_cores).toBe(0);
    expect(result.total_cpu_cores).toBeUndefined();
    expect(result.cpu_capacity_complete).toBe(false);
    expect(result.cpu_capacity_missing_hosts).toEqual(['host-b']);
  });

  it('renders historical Metrics bounds explicitly in UTC', async () => {
    const adapter = new RecordingAdapter();
    const service = new MetricsCollector(adapter);

    await service.getClusterHistoricalMetrics(
      new Date('2026-01-27T14:13:00.000Z'),
      new Date('2026-01-27T15:05:00.000Z'),
    );

    const timeBoundQueries = adapter.queries.filter(sql =>
      sql.includes("toDateTime('2026-01-27 14:13:00', 'UTC')"),
    );
    expect(timeBoundQueries.length).toBeGreaterThan(0);
    for (const sql of timeBoundQueries) {
      expect(sql).toContain("toDateTime('2026-01-27 14:13:00', 'UTC')");
      expect(sql).toContain("toDateTime('2026-01-27 15:05:00', 'UTC')");
    }
  });
});
