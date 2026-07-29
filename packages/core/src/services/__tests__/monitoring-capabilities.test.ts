import { describe, expect, it, vi } from 'vitest';
import type {
  IClickHouseAdapter,
  TaggedQuery,
} from '../../adapters/types.js';
import { MonitoringCapabilitiesService } from '../monitoring-capabilities.js';

function adapterWithLogCoverage(
  availableHosts: number,
  expectedHosts: number,
  distributedLimitByError?: Error,
): IClickHouseAdapter {
  const executeQuery = vi.fn(async (sql: TaggedQuery) => {
    if (sql.includes('source:TraceHouse:Internal:serverVersion')) {
      return [{ version: '26.7.1.0' }];
    }
    if (sql.includes('source:TraceHouse:Internal:distributedLimitBy')) {
      if (distributedLimitByError) throw distributedLimitByError;
      return [{ dummy: 0 }];
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

  it('reports distributed LIMIT BY when the behavioral probe succeeds', async () => {
    const service = new MonitoringCapabilitiesService(
      adapterWithLogCoverage(2, 2),
    );

    const result = await service.probe();
    expect(result.capabilities.find(
      item => item.id === 'distributed_limit_by',
    )).toMatchObject({
      available: true,
      detail: 'Supported by the active query path',
    });
  });

  it('disables distributed LIMIT BY when the planner probe fails', async () => {
    const service = new MonitoringCapabilitiesService(
      adapterWithLogCoverage(
        2,
        2,
        new Error('Code: 8. Cannot find column in source stream (THERE_IS_NO_COLUMN)'),
      ),
    );

    const result = await service.probe();
    expect(result.capabilities.find(
      item => item.id === 'distributed_limit_by',
    )).toMatchObject({
      available: false,
      detail: 'Distributed LIMIT BY planner bug detected',
    });
  });
});
