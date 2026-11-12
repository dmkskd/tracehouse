/**
 * Integration tests for the capability read-access probe on a 2-node cluster.
 *
 * The probe must deny exactly what feature reads would deny, on every
 * replica, without reading table data. The adapter here has no raw
 * transport, so CHECK GRANT is skipped and the SELECT probe is the only
 * check. That matches servers before 25.1, and the SELECT probe is the only
 * per-replica check on every version.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { createClient, type ClickHouseClient } from '@clickhouse/client';
import {
  startCluster,
  stopCluster,
  ClusterTestAdapter,
  type ClusterTestContext,
} from './setup/cluster-container.js';
import { ClusterAwareAdapter } from '../../adapters/cluster-adapter.js';
import { MonitoringCapabilitiesService } from '../../services/monitoring-capabilities.js';
import type { MonitoringCapabilities } from '../../types/monitoring-capabilities.js';

const PROBE_USER = 'probe_user';
const PROBE_PASSWORD = 'probe_pass';

describe('capability read-access probe on a cluster', { tags: ['cluster', 'security'] }, () => {
  let ctx: ClusterTestContext;
  let probeClient: ClickHouseClient;
  let clusterResult: MonitoringCapabilities;
  let localResult: MonitoringCapabilities;
  let probeStartedAt: string;

  beforeAll(async () => {
    ctx = await startCluster();

    // Users live in each node's local access storage, so grants can differ
    // between replicas. The cluster secret runs remote legs as this user.
    const nodeGrants: Array<[ClickHouseClient, string[]]> = [
      [ctx.client1, []],
      [ctx.client2, [`REVOKE SELECT ON system.replicas FROM ${PROBE_USER}`]],
    ];
    for (const [client, extra] of nodeGrants) {
      for (const query of [
        `CREATE USER ${PROBE_USER} IDENTIFIED BY '${PROBE_PASSWORD}'`,
        `GRANT SELECT, REMOTE, CREATE TEMPORARY TABLE ON *.* TO ${PROBE_USER}`,
        `REVOKE SELECT ON system.mutations FROM ${PROBE_USER}`,
        `REVOKE SELECT ON system.parts FROM ${PROBE_USER}`,
        `GRANT SELECT(database, table) ON system.parts TO ${PROBE_USER}`,
        ...extra,
      ]) {
        await client.command({ query });
      }
    }

    probeClient = createClient({
      url: `http://${ctx.ch1.getHost()}:${ctx.ch1.getMappedPort(8123)}`,
      username: PROBE_USER,
      password: PROBE_PASSWORD,
    });
    const clusterAdapter = new ClusterAwareAdapter(new ClusterTestAdapter(probeClient));
    clusterAdapter.setClusterName('default');
    // Single-node mode sends the same probe without clusterAllReplicas.
    const localAdapter = new ClusterAwareAdapter(new ClusterTestAdapter(probeClient));

    const [{ now }] = await ctx.adapter1.executeQuery<{ now: string }>(
      'SELECT toString(now64(6)) AS now',
    );
    probeStartedAt = now;
    clusterResult = await new MonitoringCapabilitiesService(clusterAdapter).probe();
    localResult = await new MonitoringCapabilitiesService(localAdapter).probe();
    await ctx.client1.command({ query: 'SYSTEM FLUSH LOGS' });
    await ctx.client2.command({ query: 'SYSTEM FLUSH LOGS' });
  }, 180_000);

  afterAll(async () => {
    await probeClient?.close();
    if (ctx) await stopCluster(ctx);
  }, 60_000);

  const capability = (result: MonitoringCapabilities, id: string) =>
    result.capabilities.find(item => item.id === id);

  it('marks a table available only when every replica grants SELECT on all its columns', () => {
    expect(capability(clusterResult, 'system_merges')).toMatchObject({ available: true });
    // A full revoke also hides the table from system.tables.
    expect(capability(clusterResult, 'system_mutations')).toMatchObject({ available: false });
    expect(capability(clusterResult, 'system_parts')).toMatchObject({ available: false, unavailableReason: 'grant' });
    expect(capability(clusterResult, 'system_replicas')).toMatchObject({ available: false, unavailableReason: 'grant' });
  });

  it('denies a column-only grant in single-node mode', () => {
    expect(capability(localResult, 'system_merges')).toMatchObject({ available: true });
    expect(capability(localResult, 'system_parts')).toMatchObject({ available: false, unavailableReason: 'grant' });
  });

  it('reaches every replica and reads no rows', async () => {
    const initial = await ctx.adapter1.executeQuery<{ query_id: string; query: string; read_rows: string }>(`
      SELECT query_id, query, toString(read_rows) AS read_rows
      FROM system.query_log
      WHERE type = 'QueryFinish'
        AND is_initial_query = 1
        AND user = '${PROBE_USER}'
        AND event_time_microseconds >= toDateTime64('${probeStartedAt}', 6)
        AND query LIKE '%source:TraceHouse:Internal:tableReadAccess%'
    `);
    expect(initial.length).toBeGreaterThan(0);
    expect(initial.filter(row => Number(row.read_rows) !== 0)).toEqual([]);

    const routed = initial.filter(row => row.query.includes('clusterAllReplicas'));
    expect(routed.length).toBeGreaterThan(0);
    const remote = await ctx.adapter2.executeQuery<{ initial_query_id: string }>(`
      SELECT DISTINCT initial_query_id
      FROM system.query_log
      WHERE type IN ('QueryFinish', 'ExceptionBeforeStart', 'ExceptionWhileProcessing')
        AND is_initial_query = 0
        AND initial_query_id IN (${routed.map(row => `'${row.query_id}'`).join(', ')})
    `);
    const reached = new Set(remote.map(row => row.initial_query_id));
    expect(routed.filter(row => !reached.has(row.query_id)).map(row => row.query_id)).toEqual([]);
  });
});
