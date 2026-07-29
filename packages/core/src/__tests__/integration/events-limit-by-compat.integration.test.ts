import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { ClusterAwareAdapter } from '../../adapters/cluster-adapter.js';
import { EventsService } from '../../services/events-service.js';
import { MonitoringCapabilitiesService } from '../../services/monitoring-capabilities.js';
import { isClickHouseVersionAtLeast } from '../../utils/clickhouse-version.js';
import {
  startCluster,
  stopCluster,
  type ClusterTestContext,
} from './setup/cluster-container.js';

const TEST_DB = 'events_limit_by_compat';
const CONTAINER_TIMEOUT = 180_000;

describe('Events distributed LIMIT BY compatibility', { tags: ['cluster'] }, () => {
  let ctx: ClusterTestContext;
  let adapter: ClusterAwareAdapter;
  let startTime: string;

  beforeAll(async () => {
    ctx = await startCluster();
    adapter = new ClusterAwareAdapter(ctx.adapter1);
    adapter.setClusterName('default');
    startTime = new Date(Date.now() - 60_000).toISOString();

    await Promise.all([
      ctx.client1.command({
        query: `CREATE DATABASE IF NOT EXISTS ${TEST_DB}`,
      }),
      ctx.client2.command({
        query: `CREATE DATABASE IF NOT EXISTS ${TEST_DB}`,
      }),
    ]);
    await Promise.all([
      ctx.client1.command({ query: 'SYSTEM FLUSH LOGS' }),
      ctx.client2.command({ query: 'SYSTEM FLUSH LOGS' }),
    ]);
  }, CONTAINER_TIMEOUT);

  afterAll(async () => {
    if (!ctx) return;
    await Promise.allSettled([
      ctx.client1.command({ query: `DROP DATABASE IF EXISTS ${TEST_DB}` }),
      ctx.client2.command({ query: `DROP DATABASE IF EXISTS ${TEST_DB}` }),
    ]);
    await stopCluster(ctx);
  }, 60_000);

  it('detects the planner capability and keeps query-log events available', async () => {
    const capabilityResult = await new MonitoringCapabilitiesService(
      adapter,
    ).probe();
    const distributedLimitBy = capabilityResult.capabilities.find(
      capability => capability.id === 'distributed_limit_by',
    );
    expect(distributedLimitBy).toBeDefined();

    if (capabilityResult.serverVersion === '23.8.2.7') {
      expect(distributedLimitBy).toMatchObject({
        available: false,
        detail: 'Distributed LIMIT BY planner bug detected',
      });
    } else if (isClickHouseVersionAtLeast(
      capabilityResult.serverVersion,
      24,
      1,
    )) {
      expect(distributedLimitBy).toMatchObject({
        available: true,
        detail: 'Supported by the active query path',
      });
    }

    const availableCapabilities = capabilityResult.capabilities
      .filter(capability => capability.available)
      .map(capability => capability.id);
    const result = await new EventsService(adapter).getEvents({
      startTime,
      endTime: new Date(Date.now() + 60_000).toISOString(),
      availableCapabilities,
      limit: 100,
    });

    expect(result.coverage.find(
      source => source.capability === 'query_log',
    )).toMatchObject({
      status: 'loaded',
    });
    expect(result.events.some(event => event.kind === 'ddl')).toBe(true);
  });
});
