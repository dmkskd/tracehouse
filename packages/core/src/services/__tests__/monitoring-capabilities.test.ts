import { describe, expect, it, vi } from 'vitest';
import type {
  IClickHouseAdapter,
  TaggedQuery,
} from '../../adapters/types.js';
import { MonitoringCapabilitiesService } from '../monitoring-capabilities.js';

function adapterWithLogCoverage(
  availableHosts: number,
  expectedHosts: number,
): IClickHouseAdapter {
  const executeQuery = vi.fn(async (sql: TaggedQuery) => {
    if (sql.includes('source:TraceHouse:Internal:serverVersion')) {
      return [{ version: '26.7.1.0' }];
    }
    if (sql.includes('source:TraceHouse:Internal:logTables')) {
      return [{
        name: 'asynchronous_insert_log',
        engine: 'MergeTree',
        total_rows: 12,
        total_bytes: 1024,
        create_table_query: '',
        available_hosts: availableHosts,
        expected_hosts: expectedHosts,
      }];
    }
    return [];
  });

  return {
    executeQuery: executeQuery as IClickHouseAdapter['executeQuery'],
  };
}

describe('MonitoringCapabilitiesService cluster log coverage', () => {
  it('marks a system log unavailable when it is missing on one replica', async () => {
    const service = new MonitoringCapabilitiesService(
      adapterWithLogCoverage(1, 2),
    );

    const result = await service.probe();
    const capability = result.capabilities.find(
      item => item.id === 'asynchronous_insert_log',
    );

    expect(capability).toMatchObject({
      available: false,
      detail: 'Partial cluster coverage · 1/2 hosts',
    });
  });

  it('marks a system log available when every replica reports it', async () => {
    const service = new MonitoringCapabilitiesService(
      adapterWithLogCoverage(2, 2),
    );

    const result = await service.probe();
    const capability = result.capabilities.find(
      item => item.id === 'asynchronous_insert_log',
    );

    expect(capability).toMatchObject({
      available: true,
      detail: 'MergeTree · 12 rows · 2/2 hosts',
    });
  });
});
