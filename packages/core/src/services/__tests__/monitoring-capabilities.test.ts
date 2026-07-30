import { describe, expect, it, vi } from 'vitest';
import type {
  IClickHouseAdapter,
  TaggedQuery,
} from '../../adapters/types.js';
import { MonitoringCapabilitiesService } from '../monitoring-capabilities.js';

interface AdapterOptions {
  availableHosts?: number;
  expectedHosts?: number;
  serverVersion?: string;
  systemTables?: string[];
  introspectionEnabled?: boolean;
}

function adapterForCapabilities(options: AdapterOptions = {}): IClickHouseAdapter {
  const {
    availableHosts = 2,
    expectedHosts = 2,
    serverVersion = '26.7.1.0',
    systemTables = ['merges', 'distributed_ddl_queue', 'zookeeper'],
    introspectionEnabled = false,
  } = options;

  const executeQuery = vi.fn(async (sql: TaggedQuery) => {
    if (sql.includes('source:TraceHouse:Internal:capabilitySnapshot')) {
      return [{
        version: serverVersion,
        introspection_functions_present: 1,
        introspection_enabled: introspectionEnabled ? 1 : 0,
        system_tables: systemTables,
      }];
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

describe('MonitoringCapabilitiesService', () => {
  it('marks a system log unavailable when it is missing on one replica', async () => {
    const service = new MonitoringCapabilitiesService(
      adapterForCapabilities({ availableHosts: 1, expectedHosts: 2 }),
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
      adapterForCapabilities(),
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

  it('enables distributed LIMIT BY on ClickHouse 24.1 and newer', async () => {
    const result = await new MonitoringCapabilitiesService(
      adapterForCapabilities({ serverVersion: '24.1.1.1' }),
    ).probe();

    expect(result.capabilities.find(
      item => item.id === 'distributed_limit_by',
    )).toMatchObject({
      available: true,
      detail: 'Supported by ClickHouse 24.1.1.1',
    });
  });

  it('uses the global LIMIT fallback on ClickHouse 23.8', async () => {
    const result = await new MonitoringCapabilitiesService(
      adapterForCapabilities({ serverVersion: '23.8.2.7' }),
    ).probe();

    expect(result.capabilities.find(
      item => item.id === 'distributed_limit_by',
    )).toMatchObject({
      available: false,
      detail: 'Disabled for ClickHouse 23.8.2.7; requires 24.1+',
    });
  });

  it('derives Keeper and system-table capabilities from metadata', async () => {
    const result = await new MonitoringCapabilitiesService(
      adapterForCapabilities({
        systemTables: ['merges', 'distributed_ddl_queue'],
      }),
    ).probe();

    expect(result.capabilities.find(item => item.id === 'zookeeper'))
      .toMatchObject({ available: false });
    expect(result.capabilities.find(
      item => item.id === 'system_merges',
    )).toMatchObject({
      available: true,
      detail: 'Present · access verified when used',
    });
    expect(result.capabilities.find(
      item => item.id === 'system_distributed_ddl_queue',
    )).toMatchObject({
      available: false,
      detail: 'Present · Keeper not configured',
    });
  });

  it('uses one successful snapshot and never runs intentional failure probes', async () => {
    const adapter = adapterForCapabilities();

    await new MonitoringCapabilitiesService(adapter).probe();

    const executeQuery = vi.mocked(adapter.executeQuery);
    const queries = executeQuery.mock.calls.map(([sql]) => String(sql));
    expect(queries).toHaveLength(6);
    expect(queries.filter(query =>
      query.includes('source:TraceHouse:Internal:capabilitySnapshot'),
    )).toHaveLength(1);
    expect(queries.some(query => query.includes("demangle('')"))).toBe(false);
    expect(queries.some(query =>
      query.includes('FROM system.distributed_ddl_queue'),
    )).toBe(false);
    expect(queries.some(query =>
      query.includes('source:TraceHouse:Internal:probe_'),
    )).toBe(false);
  });
});
