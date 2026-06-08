import { describe, expect, it, vi } from 'vitest';
import type { IClickHouseAdapter } from '../../adapters/types.js';
import { ObservabilityMapService } from '../observability-map-service.js';

function adapter(rows: Record<string, unknown>[]): IClickHouseAdapter {
  return {
    executeQuery: vi.fn().mockResolvedValue(rows),
  };
}

describe('ObservabilityMapService', { tags: ['observability'] }, () => {
  it('maps system table metadata by qualified table name', async () => {
    const mock = adapter([{ name: 'query_log', sorting_key: 'event_date', primary_key: 'event_time' }]);
    const service = new ObservabilityMapService(mock);

    const result = await service.getSystemTables();

    expect(result.get('system.query_log')).toEqual({
      name: 'system.query_log',
      sorting_key: 'event_date',
      primary_key: 'event_time',
    });
    expect(mock.executeQuery).toHaveBeenCalledWith(expect.stringContaining('source:TraceHouse:Overview:observabilityTables'));
  });

  it('maps column comments by qualified column name', async () => {
    const mock = adapter([{ table: 'query_log', name: 'query_id', comment: 'Query identifier' }]);
    const service = new ObservabilityMapService(mock);

    const result = await service.getColumnComments();

    expect(result.get('system.query_log.query_id')).toBe('Query identifier');
    expect(mock.executeQuery).toHaveBeenCalledWith(expect.stringContaining('source:TraceHouse:Overview:observabilityColumnComments'));
  });
});
