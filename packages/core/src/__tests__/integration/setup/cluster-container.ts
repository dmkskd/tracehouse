/**
 * 2-node ClickHouse cluster with Keeper for integration tests.
 *
 * Topology: 1 shard, 2 replicas + 1 ClickHouse Keeper node.
 * All on a shared Docker network so they can discover each other.
 */

import { GenericContainer, Network, Wait, type StartedNetwork, type StartedTestContainer } from 'testcontainers';
import { createClient, type ClickHouseClient } from '@clickhouse/client';
import type { IClickHouseAdapter } from '../../../adapters/types.js';
import { AdapterError } from '../../../adapters/types.js';

const CH_IMAGE = 'clickhouse/clickhouse-server:26.1-alpine';

const CH_USER = 'default';
const CH_PASSWORD = 'test';

// ── XML configs injected into each container ──

const KEEPER_CONFIG = `<?xml version="1.0"?>
<clickhouse>
  <listen_host>0.0.0.0</listen_host>
  <keeper_server>
    <tcp_port>9181</tcp_port>
    <server_id>1</server_id>
    <raft_configuration>
      <server>
        <id>1</id>
        <hostname>keeper</hostname>
        <port>9234</port>
      </server>
    </raft_configuration>
  </keeper_server>
</clickhouse>`;

function chConfig(replicaName: string): string {
  return `<?xml version="1.0"?>
<clickhouse>
  <listen_host>0.0.0.0</listen_host>
  <interserver_http_host>${replicaName}</interserver_http_host>
  <interserver_http_credentials>
    <user>interserver</user>
    <password>interserver_secret</password>
  </interserver_http_credentials>
  <remote_servers>
    <default>
      <secret>cluster_secret</secret>
      <shard>
        <replica>
          <host>ch1</host>
          <port>9000</port>
        </replica>
        <replica>
          <host>ch2</host>
          <port>9000</port>
        </replica>
      </shard>
    </default>
  </remote_servers>
  <zookeeper>
    <node>
      <host>keeper</host>
      <port>9181</port>
    </node>
  </zookeeper>
  <macros>
    <shard>01</shard>
    <replica>${replicaName}</replica>
    <cluster>default</cluster>
  </macros>
</clickhouse>`;
}

/** Adapter wrapping @clickhouse/client for cluster integration tests. */
export class ClusterTestAdapter implements IClickHouseAdapter {
  constructor(private client: ClickHouseClient) {}

  async executeQuery<T extends Record<string, unknown>>(sql: string): Promise<T[]> {
    try {
      const result = await this.client.query({ query: sql, format: 'JSONEachRow' });
      return await result.json<T>();
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error);
      throw new AdapterError(msg, 'query', error instanceof Error ? error : undefined);
    }
  }

  async executeCommand(sql: string): Promise<void> {
    await this.client.command({ query: sql });
  }
}

export interface ClusterTestContext {
  network: StartedNetwork;
  keeper: StartedTestContainer;
  ch1: StartedTestContainer;
  ch2: StartedTestContainer;
  /** Client connected to ch1 */
  client1: ClickHouseClient;
  /** Client connected to ch2 */
  client2: ClickHouseClient;
  /** Adapter connected to ch1 */
  adapter1: ClusterTestAdapter;
  /** Adapter connected to ch2 */
  adapter2: ClusterTestAdapter;
}

/**
 * Start a 2-node ClickHouse cluster with Keeper.
 * Returns clients/adapters for both nodes.
 */
export async function startCluster(): Promise<ClusterTestContext> {
  const network = await new Network().start();

  // Start Keeper first — uses the same CH image with embedded Keeper.
  // The server logs go to a file (not stdout), so we wait on HTTP /ping instead.
  const keeper = await new GenericContainer(CH_IMAGE)
    .withNetwork(network)
    .withNetworkAliases('keeper')
    .withEnvironment({ CLICKHOUSE_USER: CH_USER, CLICKHOUSE_PASSWORD: CH_PASSWORD })
    .withCopyContentToContainer([{
      content: KEEPER_CONFIG,
      target: '/etc/clickhouse-server/config.d/keeper.xml',
    }])
    .withExposedPorts(8123, 9181)
    .withWaitStrategy(Wait.forHttp('/ping', 8123).forStatusCode(200))
    .withStartupTimeout(60_000)
    .start();

  // Start CH nodes in parallel
  const [ch1, ch2] = await Promise.all([
    new GenericContainer(CH_IMAGE)
      .withNetwork(network)
      .withNetworkAliases('ch1')
      .withEnvironment({ CLICKHOUSE_USER: CH_USER, CLICKHOUSE_PASSWORD: CH_PASSWORD })
      .withCopyContentToContainer([{
        content: chConfig('ch1'),
        target: '/etc/clickhouse-server/config.d/cluster.xml',
      }])
      .withExposedPorts(8123, 9000)
      .withWaitStrategy(Wait.forHttp('/ping', 8123).forStatusCode(200))
      .withStartupTimeout(60_000)
      .start(),
    new GenericContainer(CH_IMAGE)
      .withNetwork(network)
      .withNetworkAliases('ch2')
      .withEnvironment({ CLICKHOUSE_USER: CH_USER, CLICKHOUSE_PASSWORD: CH_PASSWORD })
      .withCopyContentToContainer([{
        content: chConfig('ch2'),
        target: '/etc/clickhouse-server/config.d/cluster.xml',
      }])
      .withExposedPorts(8123, 9000)
      .withWaitStrategy(Wait.forHttp('/ping', 8123).forStatusCode(200))
      .withStartupTimeout(60_000)
      .start(),
  ]);

  const client1 = createClient({
    url: `http://${ch1.getHost()}:${ch1.getMappedPort(8123)}`,
    username: CH_USER,
    password: CH_PASSWORD,
  });
  const client2 = createClient({
    url: `http://${ch2.getHost()}:${ch2.getMappedPort(8123)}`,
    username: CH_USER,
    password: CH_PASSWORD,
  });

  // Wait for cluster to be ready (both nodes see each other)
  await waitForCluster(client1);

  return {
    network,
    keeper,
    ch1,
    ch2,
    client1,
    client2,
    adapter1: new ClusterTestAdapter(client1),
    adapter2: new ClusterTestAdapter(client2),
  };
}

/** Wait until system.clusters shows 2 replicas for 'default' cluster. */
async function waitForCluster(client: ClickHouseClient, timeoutMs = 60_000): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const result = await client.query({
        query: `SELECT cluster, count() AS cnt FROM system.clusters GROUP BY cluster`,
        format: 'JSONEachRow',
      });
      const rows = await result.json<{ cluster: string; cnt: string }>();
      const defaultCluster = rows.find(r => r.cluster === 'default');
      if (defaultCluster && Number(defaultCluster.cnt) >= 2) return;
    } catch {
      // Retry — server may still be starting
    }
    await new Promise(r => setTimeout(r, 2000));
  }
  throw new Error('Cluster did not become ready within timeout');
}

/** Tear down the cluster. */
export async function stopCluster(ctx: ClusterTestContext): Promise<void> {
  await ctx.client1.close();
  await ctx.client2.close();
  await ctx.ch1.stop();
  await ctx.ch2.stop();
  await ctx.keeper.stop();
  await ctx.network.stop();
}
