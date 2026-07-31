import { describe, expect, it } from 'vitest';
import type { IClickHouseAdapter, TaggedQuery } from '../../adapters/types.js';
import { TimelineService } from '../timeline-service.js';

class NavigatorAdapter implements IClickHouseAdapter {
  readonly queries: string[] = [];

  async executeQuery<T extends Record<string, unknown>>(sql: TaggedQuery): Promise<T[]> {
    this.queries.push(sql);
    return [
      {
        t: '2026-07-31 08:00:00',
        average_v: 4_000_000,
        peak_v: 5_000_000,
      },
      {
        t: '2026-07-31 08:00:03',
        average_v: 5_000_000,
        peak_v: 7_000_000,
      },
    ] as unknown as T[];
  }
}

describe('TimelineService navigator metric', { tags: ['observability'] }, () => {
  it('uses one bucketed metric query with a sanitized host filter', async () => {
    const adapter = new NavigatorAdapter();
    const service = new TimelineService(adapter);

    const result = await service.getNavigatorMetric({
      startTime: new Date('2026-07-31T08:00:00Z'),
      endTime: new Date('2026-07-31T09:00:00Z'),
      metric: 'cpu',
      bucketSeconds: 3,
      hostname: ['host-a', 'host-b'],
    });

    expect(adapter.queries).toHaveLength(1);
    expect(adapter.queries[0]).toContain('toIntervalSecond(3)');
    expect(adapter.queries[0]).toContain("hostname() IN ('host-a', 'host-b')");
    expect(adapter.queries[0]).toContain('AS average_v');
    expect(adapter.queries[0]).toContain('AS peak_v');
    expect(adapter.queries[0]).toContain('source:TraceHouse:TimeTravel:navigatorCpu');
    expect(result).toEqual({
      window_start: '2026-07-31T08:00:00.000Z',
      window_end: '2026-07-31T09:00:00.000Z',
      bucket_seconds: 3,
      points: [
        {
          t: '2026-07-31 08:00:00',
          average_v: 4_000_000,
          peak_v: 5_000_000,
        },
        {
          t: '2026-07-31 08:00:03',
          average_v: 5_000_000,
          peak_v: 7_000_000,
        },
      ],
    });
  });

  it('rejects an invalid time range before querying ClickHouse', async () => {
    const adapter = new NavigatorAdapter();
    const service = new TimelineService(adapter);

    await expect(service.getNavigatorMetric({
      startTime: new Date('2026-07-31T09:00:00Z'),
      endTime: new Date('2026-07-31T08:00:00Z'),
      metric: 'memory',
      bucketSeconds: 1,
    })).rejects.toThrow('Invalid navigator time range');
    expect(adapter.queries).toHaveLength(0);
  });
});
