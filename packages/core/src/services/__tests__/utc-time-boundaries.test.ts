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
  async executeQuery<T extends Record<string, unknown>>(sql: TaggedQuery): Promise<T[]> {
    if (sql.includes('TimeTravel:totalRam')) {
      return [
        { host: 'host-a', value: 12 * 1024 ** 3 },
        { host: 'host-b', value: 32 * 1024 ** 3 },
      ] as unknown as T[];
    }
    if (sql.includes('TimeTravel:cpuCores')) {
      return [
        { host: 'host-a', value: 3 },
        { host: 'host-b', value: 8 },
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
