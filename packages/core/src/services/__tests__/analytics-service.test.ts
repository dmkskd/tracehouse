import { describe, expect, it, vi } from 'vitest';
import type { IClickHouseAdapter } from '../../adapters/types.js';
import { AnalyticsService } from '../analytics-service.js';

function adapter(rows: Record<string, unknown>[]): IClickHouseAdapter {
  return {
    executeQuery: vi.fn().mockResolvedValue(rows),
  };
}

describe('AnalyticsService', { tags: ['query-analysis'] }, () => {
  it('returns MergeTree database names through a tagged core query', async () => {
    const mock = adapter([{ db: 'default' }, { db: 'analytics' }]);
    const service = new AnalyticsService(mock);

    await expect(service.getMergeTreeDatabases()).resolves.toEqual(['default', 'analytics']);
    expect(mock.executeQuery).toHaveBeenCalledWith(expect.stringContaining('source:TraceHouse:Analytics:mergeTreeDatabases'));
  });

  it('maps the latest pattern query to QuerySeries', async () => {
    const mock = adapter([{
      query_id: 'q1',
      query: 'SELECT 1',
      user: 'default',
      event_time: '2026-06-08 10:00:00',
      query_duration_ms: 25,
      memory_usage: 1024,
      cpu_us: 500,
      net_send: 10,
      net_recv: 20,
      disk_read: 30,
      disk_write: 40,
      status: 'QueryFinish',
    }]);
    const service = new AnalyticsService(mock);

    const result = await service.getLatestQueryForPattern('abc123');

    expect(result).toMatchObject({
      query_id: 'q1',
      label: 'SELECT 1',
      duration_ms: 25,
      peak_memory: 1024,
      cpu_us: 500,
      points: [],
    });
    expect(result?.start_time).toBe('2026-06-08T10:00:00.000Z');
    expect(result?.end_time).toBe('2026-06-08T10:00:00.025Z');
    expect(mock.executeQuery).toHaveBeenCalledWith(expect.stringContaining('source:TraceHouse:Analytics:latestPatternQuery'));
  });

  it('returns null for non-numeric pattern hashes', async () => {
    const mock = adapter([]);
    const service = new AnalyticsService(mock);

    await expect(service.getLatestQueryForPattern('not-a-hash')).resolves.toBeNull();
    expect(mock.executeQuery).not.toHaveBeenCalled();
  });
});
