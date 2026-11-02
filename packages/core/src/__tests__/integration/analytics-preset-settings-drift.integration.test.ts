/**
 * 'Settings Drift Across Hosts' must report settings that really differ between
 * nodes, and nothing else.
 *
 * Runs on a 2-node cluster whose users config is made to differ per node, so
 * the drift comes from ClickHouse's own system tables rather than hand-written
 * rows. Session settings cannot be used for this: the fan-out sends the
 * initiator's values to every node.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { ClickHouseClient } from '@clickhouse/client';
import {
  startCluster,
  stopCluster,
  type ClusterTestContext,
} from './setup/cluster-container.js';
import { RAW_QUERIES } from '@frontend-queries/index';
import { parseQueryMetadata } from '@frontend-analytics/metaLanguage';
import { ClusterService } from '../../services/cluster-service.js';

const CLUSTER_TIMEOUT = 180_000;

interface DriftRow {
  scope: string;
  name: string;
  most_common_value: string;
  on_hosts: string;
  other_values: string;
  missing_on: string;
  set_in_config: string;
}

function settingsDriftSQL(): string {
  const sql = RAW_QUERIES.find(q => parseQueryMetadata(q)?.name === 'Settings Drift Across Hosts');
  if (!sql) throw new Error('Settings Drift Across Hosts panel not found in RAW_QUERIES');
  return ClusterService.resolveTableRefs(sql, 'default');
}

function profileConfig(settings: Record<string, number>): string {
  const body = Object.entries(settings).map(([k, v]) => `<${k}>${v}</${k}>`).join('');
  return `<clickhouse><profiles><default>${body}</default></profiles></clickhouse>`;
}

async function hostName(client: ClickHouseClient): Promise<string> {
  const result = await client.query({ query: 'SELECT hostName() AS host', format: 'JSONEachRow' });
  return (await result.json<{ host: string }>())[0].host;
}

/** users.d is reloaded without a restart; wait until the node reports the setting. */
async function waitForProfileSetting(client: ClickHouseClient, name: string, timeoutMs = 30_000): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const result = await client.query({
      query: `SELECT count() AS n FROM system.settings_profile_elements
              WHERE profile_name = 'default' AND setting_name = '${name}'`,
      format: 'JSONEachRow',
    });
    if (Number((await result.json<{ n: string }>())[0].n) > 0) return;
    await new Promise(r => setTimeout(r, 500));
  }
  throw new Error(`Profile setting ${name} was not reloaded within ${timeoutMs}ms`);
}

describe('Settings Drift Across Hosts', { tags: ['analytics'] }, () => {
  let ctx: ClusterTestContext;
  let host1: string;
  let host2: string;
  let rows: DriftRow[];

  beforeAll(async () => {
    ctx = await startCluster();
    host1 = await hostName(ctx.client1);
    host2 = await hostName(ctx.client2);

    // max_threads differs between the nodes; max_insert_threads exists only on
    // ch2; max_final_threads is identical and must not be reported.
    await ctx.ch1.copyContentToContainer([{
      content: profileConfig({ max_threads: 3, max_final_threads: 4 }),
      target: '/etc/clickhouse-server/users.d/zz-drift-test.xml',
    }]);
    await ctx.ch2.copyContentToContainer([{
      content: profileConfig({ max_threads: 5, max_final_threads: 4, max_insert_threads: 2 }),
      target: '/etc/clickhouse-server/users.d/zz-drift-test.xml',
    }]);
    await waitForProfileSetting(ctx.client1, 'max_threads');
    await waitForProfileSetting(ctx.client2, 'max_insert_threads');

    rows = await ctx.adapter1.executeQuery<DriftRow & Record<string, unknown>>(settingsDriftSQL());
  }, CLUSTER_TIMEOUT);

  afterAll(async () => {
    if (ctx) await stopCluster(ctx);
  }, CLUSTER_TIMEOUT);

  const profileRow = (name: string) => rows.find(r => r.scope === 'profile default' && r.name === name);

  it('reports a setting configured with different values, with the hosts holding each', () => {
    const row = profileRow('max_threads');
    expect(row).toBeDefined();
    expect(row!.on_hosts).toBe('1 of 2');
    expect(row!.set_in_config).toBe('2 of 2');
    // Tie broken by value: '3' sorts first, so ch2's '5' is the other value.
    expect(row!.most_common_value).toBe('3');
    expect(row!.other_values).toBe(`5 on ${host2}`);
    expect(row!.missing_on).toBe('');
  });

  it('reports a setting configured on only some hosts, and skips identical ones', () => {
    const row = profileRow('max_insert_threads');
    expect(row).toBeDefined();
    expect(row!.on_hosts).toBe('1 of 2');
    expect(row!.set_in_config).toBe('1 of 2');
    expect(row!.missing_on).toBe(host1);

    expect(profileRow('max_final_threads')).toBeUndefined();
  });
});
