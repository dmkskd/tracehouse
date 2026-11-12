import { describe, expect, it, vi } from 'vitest';
import type {
  IClickHouseAdapter,
  TaggedQuery,
} from '../../adapters/types.js';
import { MonitoringCapabilitiesService } from '../monitoring-capabilities.js';
import { ClusterAwareAdapter } from '../../adapters/cluster-adapter.js';
import { processorProfileCompatibilityFromMonitoringCapabilities } from '../distributed-query-topology.js';

interface AdapterOptions {
  availableHosts?: number;
  expectedHosts?: number;
  serverVersion?: string;
  systemTables?: string[];
  introspectionEnabled?: boolean;
  processorProfileSchema?: 'full' | 'legacy' | 'missing' | 'error';
  extraLogTables?: string[];
  readErrors?: Record<string, string>;
}

function adapterForCapabilities(options: AdapterOptions = {}): IClickHouseAdapter {
  const {
    availableHosts = 2,
    expectedHosts = 2,
    serverVersion = '26.7.1.0',
    systemTables = ['merges', 'distributed_ddl_queue', 'zookeeper'],
    introspectionEnabled = false,
    processorProfileSchema = 'missing',
    extraLogTables = [],
    readErrors = {},
  } = options;

  const executeQuery = vi.fn(async (sql: TaggedQuery) => {
    if (sql.includes('source:TraceHouse:Internal:tableSelectGrant')) {
      for (const [table, error] of Object.entries(readErrors)) {
        if (sql.includes(`CHECK GRANT SELECT ON ${table}`)) {
          if (/ACCESS_DENIED|privileges/.test(error)) return [{ result: 0 }];
          throw new Error(error);
        }
      }
      return [{ result: 1 }];
    }
    if (sql.includes('source:TraceHouse:Internal:tableReadAccess')) {
      for (const [table, error] of Object.entries(readErrors)) {
        if (sql.includes(`{{cluster_aware:${table}}}`)) throw new Error(error);
      }
      return [];
    }
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
      }, ...extraLogTables.map(name => ({
        name,
        engine: 'MergeTree',
        total_rows: 0,
        total_bytes: 0,
        create_table_query: '',
        available_hosts: availableHosts,
        expected_hosts: expectedHosts,
      })), ...(processorProfileSchema === 'missing' || processorProfileSchema === 'error'
        ? []
        : [{
            name: 'processors_profile_log',
            engine: 'MergeTree',
            total_rows: 20,
            total_bytes: 2048,
            create_table_query: '',
            available_hosts: availableHosts,
            expected_hosts: expectedHosts,
          }])];
    }
    if (sql.includes('source:TraceHouse:Internal:processorProfileSchema')) {
      if (processorProfileSchema === 'error') {
        throw new Error('system.columns denied');
      }
      return [{
        host_count: expectedHosts,
        base_host_count: processorProfileSchema === 'missing' ? 0 : availableHosts,
        plan_step_host_count: processorProfileSchema === 'full' ? availableHosts : 0,
      }];
    }
    return [];
  });

  return {
    executeQuery: executeQuery as IClickHouseAdapter['executeQuery'],
    executeRawQuery: vi.fn(async (sql: TaggedQuery) => {
      const rows = await executeQuery(sql);
      return rows.map(row => String(Object.values(row)[0]));
    }),
  };
}

describe('MonitoringCapabilitiesService', () => {
  it('falls back to zero-row reads when the wrapped cluster adapter lacks raw-query support', async () => {
    const inner = adapterForCapabilities({ extraLogTables: ['query_log'] });
    delete inner.executeRawQuery;
    const adapter = new ClusterAwareAdapter(inner);
    adapter.setClusterName('default');
    expect(adapter.executeRawQuery).toBeUndefined();

    const result = await new MonitoringCapabilitiesService(adapter).probe();

    expect(result.capabilities.find(item => item.id === 'query_log')).toMatchObject({ available: true });
    const queries = vi.mocked(inner.executeQuery).mock.calls.map(([sql]) => String(sql));
    expect(queries.some(sql => sql.includes('CHECK GRANT'))).toBe(false);
    expect(queries.some(sql => sql.includes("SELECT * FROM clusterAllReplicas('default', system.query_log) WHERE 0"))).toBe(true);
  });

  it('disables visible tables without SELECT permission before event collection', async () => {
    const mock = adapterForCapabilities({
      extraLogTables: ['zookeeper_connection_log', 'metric_log', 'query_metric_log'],
      readErrors: {
        'system.zookeeper_connection_log': 'Code: 497. Not enough privileges (ACCESS_DENIED)',
        'system.metric_log': 'Not enough privileges',
        'system.query_metric_log': 'Not enough privileges',
        'system.merges': 'Not enough privileges',
      },
    });
    const result = await new MonitoringCapabilitiesService(mock).probe();
    for (const id of ['zookeeper_connection_log', 'metric_log', 'query_metric_log', 'system_merges']) {
      expect(result.capabilities.find(item => item.id === id)).toMatchObject({
        available: false,
        unavailableReason: 'grant',
        detail: expect.stringContaining('Missing SELECT permission on system.'),
        probeError: expect.stringContaining('privileges'),
      });
    }
    for (const id of ['metric_log_replication_state', 'metric_log_replication_failures', 'query_metric_log_xray']) {
      expect(result.capabilities.find(item => item.id === id)?.available).toBe(false);
    }
    expect(result.capabilities.find(item => item.id === 'asynchronous_insert_log')?.available).toBe(true);
    const queries = vi.mocked(mock.executeQuery).mock.calls.map(([sql]) => String(sql));
    expect(queries.some(sql => sql.includes('CHECK GRANT SELECT ON system.zookeeper_connection_log'))).toBe(true);
    expect(queries.some(sql => sql.includes('SELECT * FROM {{cluster_aware:system.zookeeper_connection_log}}'))).toBe(false);
  });

  it('retains zero-row permission probes on older servers without CHECK GRANT', async () => {
    const mock = adapterForCapabilities({ serverVersion: '23.8.2.7' });
    await new MonitoringCapabilitiesService(mock).probe();
    const queries = vi.mocked(mock.executeQuery).mock.calls.map(([sql]) => String(sql));
    expect(queries.some(sql => sql.includes('CHECK GRANT'))).toBe(false);
    expect(queries.some(sql => sql.includes('SELECT * FROM {{cluster_aware:system.asynchronous_insert_log}} WHERE 0'))).toBe(true);
  });

  it('fails closed on a read probe error and rechecks when capabilities are refreshed', async () => {
    const readErrors = { 'system.asynchronous_insert_log': 'Network timeout' };
    const mock = adapterForCapabilities({ readErrors });
    const service = new MonitoringCapabilitiesService(mock);
    const failed = await service.probe();
    expect(failed.capabilities.find(item => item.id === 'asynchronous_insert_log')).toMatchObject({
      available: false,
      unavailableReason: 'config',
      probeError: 'Network timeout',
    });
    delete (readErrors as Record<string, string>)['system.asynchronous_insert_log'];
    const refreshed = await service.probe();
    expect(refreshed.capabilities.find(item => item.id === 'asynchronous_insert_log')?.available).toBe(true);
  });

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
      detail: 'Available (v24.1.1.1)',
      minVersion: '24.1',
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
      detail: 'Requires ClickHouse 24.1+ (current: v23.8.2.7)',
      minVersion: '24.1',
      unavailableReason: 'version',
    });
  });

  it('gates analytics presets on their tested minimum version', async () => {
    const result = await new MonitoringCapabilitiesService(
      adapterForCapabilities({ serverVersion: '24.3.18.7' }),
    ).probe();
    const byId = (id: string) => result.capabilities.find(item => item.id === id);

    // 24.3 has the JSON and async-insert boundaries but not the merge ones.
    expect(byId('json_subcolumn_analysis')).toMatchObject({ available: true });
    expect(byId('async_insert_log_data_kind')).toMatchObject({ available: true });
    expect(byId('merge_duration_metric')).toMatchObject({
      available: false,
      unavailableReason: 'version',
    });
    expect(byId('merge_wait_analytics')).toMatchObject({ available: false });
  });

  it('reports the sampler as unsupported, not uninstalled, below the DDL floor', async () => {
    const result = await new MonitoringCapabilitiesService(
      adapterForCapabilities({ serverVersion: '24.8.14.39' }),
    ).probe();
    const processes = result.capabilities.find(
      item => item.id === 'tracehouse_processes_history',
    );

    expect(processes).toMatchObject({
      available: false,
      unavailableReason: 'ddl',
    });
    expect(processes?.detail).toContain('cannot create refreshable materialized views');
  });

  it('points at the setup script when the version supports the sampler DDL', async () => {
    const result = await new MonitoringCapabilitiesService(
      adapterForCapabilities({ serverVersion: '25.3.14.14' }),
    ).probe();
    const processes = result.capabilities.find(
      item => item.id === 'tracehouse_processes_history',
    );

    expect(processes).toMatchObject({
      available: false,
      unavailableReason: 'config',
    });
    expect(processes?.detail).toContain('setup_sampling.sh');
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
      detail: 'Present · read access verified',
    });
    expect(result.capabilities.find(
      item => item.id === 'system_distributed_ddl_queue',
    )).toMatchObject({
      available: false,
      detail: 'Present · Keeper not configured',
    });
  });

  it('runs read checks in a bounded pool that starts slow tables first', async () => {
    const adapter = adapterForCapabilities({
      systemTables: [
        'merges', 'mutations', 'clusters', 'replicas', 'parts',
        'databases', 'processes', 'distributed_ddl_queue', 'zookeeper',
      ],
    });
    const base = adapter.executeQuery.bind(adapter);
    const started: string[] = [];
    let inFlight = 0;
    let peak = 0;
    let releaseSlow!: () => void;
    const slow = new Promise<void>(resolve => { releaseSlow = resolve; });
    adapter.executeQuery = vi.fn(async (sql: TaggedQuery) => {
      if (!sql.includes('source:TraceHouse:Internal:tableReadAccess')) return base(sql);
      const table = /\{\{cluster_aware:([\w.]+)\}\}/.exec(sql)![1];
      started.push(table);
      peak = Math.max(peak, ++inFlight);
      try {
        await (table === 'system.distributed_ddl_queue'
          ? slow
          : new Promise(resolve => setTimeout(resolve, 5)));
        return [];
      } finally {
        inFlight--;
      }
    }) as IClickHouseAdapter['executeQuery'];

    const progress: string[] = [];
    const probing = new MonitoringCapabilitiesService(adapter).probe(
      update => progress.push(`${update.stage}:${update.checked}/${update.total}`),
    );
    // Every other table is checked while the slow one is still running.
    await vi.waitFor(() => {
      expect(started).toHaveLength(9);
      expect(inFlight).toBe(1);
    });
    expect(started[0]).toBe('system.distributed_ddl_queue');
    expect(peak).toBe(4);
    expect(progress.at(-1)).toBe('tableAccess:8/9');
    releaseSlow();
    await probing;
    expect(progress[0]).toBe('metadata:0/0');
    expect(progress).toContain('tableAccess:0/9');
    expect(progress.at(-1)).toBe('tableAccess:9/9');
  });

  it('uses one snapshot and checks read access only for present, configured tables', async () => {
    const adapter = adapterForCapabilities();

    await new MonitoringCapabilitiesService(adapter).probe();

    const executeQuery = vi.mocked(adapter.executeQuery);
    const queries = executeQuery.mock.calls.map(([sql]) => String(sql));
    // snapshot, log tables, settings, cloud, tracehouse tables,
    // metric_log replication columns, query_metric_log columns, processor schema
    expect(queries).toHaveLength(14);
    const reads = queries.filter(query => query.includes('source:TraceHouse:Internal:tableReadAccess'));
    expect(reads).toHaveLength(3);
    expect(reads.every(query => query.includes('WHERE 0'))).toBe(true);
    expect(reads.some(query => query.includes('system.zookeeper}}'))).toBe(false);
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

  it('detects the full processor plan-step schema at connection time', async () => {
    const result = await new MonitoringCapabilitiesService(
      adapterForCapabilities({ processorProfileSchema: 'full' }),
    ).probe();

    expect(result.capabilities.find(
      item => item.id === 'processors_profile_log',
    )).toMatchObject({ available: true });
    expect(result.capabilities.find(
      item => item.id === 'processors_profile_log_plan_steps',
    )).toMatchObject({
      available: true,
      detail: 'Full schema · 2/2 hosts',
    });
    expect(processorProfileCompatibilityFromMonitoringCapabilities(result))
      .toMatchObject({ mode: 'full', reason: 'full_schema' });
  });

  it('keeps the processor log available but marks plan steps unavailable on a legacy schema', async () => {
    const result = await new MonitoringCapabilitiesService(
      adapterForCapabilities({ processorProfileSchema: 'legacy' }),
    ).probe();

    expect(result.capabilities.find(
      item => item.id === 'processors_profile_log',
    )).toMatchObject({ available: true });
    expect(result.capabilities.find(
      item => item.id === 'processors_profile_log_plan_steps',
    )).toMatchObject({
      available: false,
      detail: 'Legacy schema · plan-step columns on 0/2 hosts',
    });
    expect(processorProfileCompatibilityFromMonitoringCapabilities(result))
      .toMatchObject({ mode: 'legacy', reason: 'legacy_schema' });
  });

  it('retains a processor schema probe error in connection capabilities', async () => {
    const result = await new MonitoringCapabilitiesService(
      adapterForCapabilities({ processorProfileSchema: 'error' }),
    ).probe();

    expect(result.capabilities.find(
      item => item.id === 'processors_profile_log',
    )).toMatchObject({
      available: false,
      detail: 'Schema probe failed · system.columns denied',
      probeError: 'system.columns denied',
    });
    expect(result.capabilities.find(
      item => item.id === 'processors_profile_log_plan_steps',
    )).toMatchObject({
      available: false,
      probeError: 'system.columns denied',
    });
    expect(processorProfileCompatibilityFromMonitoringCapabilities(result))
      .toMatchObject({
        mode: 'unavailable',
        reason: 'schema_probe_failed',
        detail: 'system.columns denied',
      });
  });
});
