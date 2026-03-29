/**
 * Configurable multi-node ClickHouse cluster with sampling infrastructure.
 *
 * Spins up N ClickHouse nodes + 1 Keeper, configures a cluster with S shards,
 * and creates the tracehouse sampling tables (processes_history + sampler MV)
 * on all nodes via ON CLUSTER DDL.
 *
 * Default topology: 2 nodes, 2 shards (1 replica each) — ideal for testing
 * distributed queries where the coordinator fans out to a remote shard.
 *
 * Usage:
 *   const ctx = await startSamplingCluster();           // 2-node, 2-shard
 *   const ctx = await startSamplingCluster({ nodes: 4, shards: 2 }); // 4-node, 2×2
 */

import { GenericContainer, Network, Wait, type StartedNetwork, type StartedTestContainer } from 'testcontainers';
import { createClient, type ClickHouseClient } from '@clickhouse/client';
import { execSync } from 'node:child_process';
import { resolve } from 'node:path';
import { ClusterTestAdapter } from './cluster-container.js';
import { CH_IMAGE } from './constants.js';

const SETUP_SCRIPT = resolve(__dirname, '../../../../../../infra/scripts/setup_sampling.sh');
const CH_USER = 'default';
const CH_PASSWORD = 'test';

// ── Options ──

export interface SamplingClusterOptions {
  /** Total number of ClickHouse nodes (default: 2) */
  nodes?: number;
  /** Number of shards (default: same as nodes — 1 replica per shard) */
  shards?: number;
  /** Cluster name (default: 'test') */
  clusterName?: string;
  /** Sampling interval in seconds (default: 1) */
  samplingIntervalSec?: number;
}

// ── Context ──

export interface SamplingClusterContext {
  network: StartedNetwork;
  keeper: StartedTestContainer;
  containers: StartedTestContainer[];
  clients: ClickHouseClient[];
  adapters: ClusterTestAdapter[];
  clusterName: string;
  /** Hostnames of nodes as seen inside the Docker network (ch1, ch2, ...) */
  nodeNames: string[];
}

// ── Config generation ──

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

/**
 * Build cluster XML config for a node.
 *
 * @param nodeName    Docker network alias for this node (e.g. "ch1")
 * @param shardIdx    0-based shard index this node belongs to
 * @param clusterName Name of the cluster
 * @param topology    Array of { shard: number; nodes: string[] } describing the full cluster
 */
function buildNodeConfig(
  nodeName: string,
  shardIdx: number,
  clusterName: string,
  topology: { shard: number; nodes: string[] }[],
): string {
  const shardXml = topology.map(s => {
    const replicas = s.nodes
      .map(n => `          <replica><host>${n}</host><port>9000</port></replica>`)
      .join('\n');
    return `      <shard>
        <internal_replication>true</internal_replication>
${replicas}
      </shard>`;
  }).join('\n');

  const shardMacro = String(shardIdx + 1).padStart(2, '0');

  return `<?xml version="1.0"?>
<clickhouse>
  <listen_host>0.0.0.0</listen_host>
  <interserver_http_host>${nodeName}</interserver_http_host>
  <interserver_http_credentials>
    <user>interserver</user>
    <password>interserver_secret</password>
  </interserver_http_credentials>
  <remote_servers>
    <${clusterName}>
      <secret>cluster_secret</secret>
${shardXml}
    </${clusterName}>
  </remote_servers>
  <zookeeper>
    <node>
      <host>keeper</host>
      <port>9181</port>
    </node>
  </zookeeper>
  <macros>
    <shard>${shardMacro}</shard>
    <replica>${nodeName}</replica>
    <cluster>${clusterName}</cluster>
  </macros>
</clickhouse>`;
}

// ── Sampling setup ──

/**
 * Run the production setup_sampling.sh script against a ClickHouse node.
 * Creates the full tracehouse infrastructure (tables, buffers, refreshable MVs).
 */
function runSamplingSetup(
  host: string,
  nativePort: number,
  opts: { user?: string; password?: string; clusterName?: string; intervalSec?: number } = {},
): void {
  const authArgs = [
    opts.user ? `--user ${opts.user}` : '',
    opts.password ? `--password ${opts.password}` : '',
  ].filter(Boolean).join(' ');
  const clusterArg = opts.clusterName ? `--cluster ${opts.clusterName}` : '';
  const intervalArg = opts.intervalSec ? `--interval ${opts.intervalSec}` : '';
  const cmd = `bash "${SETUP_SCRIPT}" --host ${host} --port ${nativePort} ${authArgs} ${clusterArg} ${intervalArg} --yes`;
  execSync(cmd, {
    encoding: 'utf-8',
    timeout: 30_000,
    env: { ...process.env, PATH: process.env.PATH },
  });
}

// ── Lifecycle ──

/**
 * Start a multi-node ClickHouse cluster with sampling infrastructure.
 *
 * @param opts - Topology and sampling options (all optional, sane defaults)
 */
export async function startSamplingCluster(
  opts: SamplingClusterOptions = {},
): Promise<SamplingClusterContext> {
  const {
    nodes: nodeCount = 2,
    shards: shardCount = nodeCount,
    clusterName = 'test',
    samplingIntervalSec = 1,
  } = opts;

  if (nodeCount < 1) throw new Error('nodes must be >= 1');
  if (shardCount < 1 || shardCount > nodeCount) throw new Error('shards must be between 1 and nodes');
  if (nodeCount % shardCount !== 0) throw new Error('nodes must be evenly divisible by shards');

  // Build topology: assign nodes to shards round-robin
  const replicasPerShard = nodeCount / shardCount;
  const nodeNames: string[] = [];
  const topology: { shard: number; nodes: string[] }[] = [];

  for (let s = 0; s < shardCount; s++) {
    const shardNodes: string[] = [];
    for (let r = 0; r < replicasPerShard; r++) {
      const name = `ch${s * replicasPerShard + r + 1}`;
      shardNodes.push(name);
      nodeNames.push(name);
    }
    topology.push({ shard: s, nodes: shardNodes });
  }

  // Determine which shard each node belongs to
  const nodeShardMap = new Map<string, number>();
  for (const s of topology) {
    for (const n of s.nodes) {
      nodeShardMap.set(n, s.shard);
    }
  }

  // Start network + keeper
  const network = await new Network().start();

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
  const containers = await Promise.all(
    nodeNames.map(name =>
      new GenericContainer(CH_IMAGE)
        .withNetwork(network)
        .withNetworkAliases(name)
        .withEnvironment({ CLICKHOUSE_USER: CH_USER, CLICKHOUSE_PASSWORD: CH_PASSWORD })
        .withCopyContentToContainer([{
          content: buildNodeConfig(name, nodeShardMap.get(name)!, clusterName, topology),
          target: '/etc/clickhouse-server/config.d/cluster.xml',
        }])
        .withExposedPorts(8123, 9000)
        .withWaitStrategy(Wait.forHttp('/ping', 8123).forStatusCode(200))
        .withStartupTimeout(60_000)
        .start()
    ),
  );

  const clients = containers.map(c => createClient({
    url: `http://${c.getHost()}:${c.getMappedPort(8123)}`,
    username: CH_USER,
    password: CH_PASSWORD,
  }));
  const adapters = clients.map(c => new ClusterTestAdapter(c));

  // Wait for cluster to be ready
  await waitForCluster(clients[0], clusterName, nodeCount);

  // Create sampling infrastructure using the production setup script.
  // Connects to the first node; ON CLUSTER DDL propagates to all nodes.
  const firstContainer = containers[0];
  runSamplingSetup(
    firstContainer.getHost(),
    firstContainer.getMappedPort(9000),
    { user: CH_USER, password: CH_PASSWORD, clusterName, intervalSec: samplingIntervalSec },
  );

  // Wait for the sampler MV to register on all nodes
  await waitForSamplers(clients);

  return { network, keeper, containers, clients, adapters, clusterName, nodeNames };
}

/** Wait until system.clusters shows expected node count. */
async function waitForCluster(
  client: ClickHouseClient,
  clusterName: string,
  expectedNodes: number,
  timeoutMs = 60_000,
): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const result = await client.query({
        query: `SELECT count() AS cnt FROM system.clusters WHERE cluster = '${clusterName}'`,
        format: 'JSONEachRow',
      });
      const rows = await result.json<{ cnt: string }>();
      if (rows.length > 0 && Number(rows[0].cnt) >= expectedNodes) return;
    } catch {
      // Retry
    }
    await new Promise(r => setTimeout(r, 2000));
  }
  throw new Error(`Cluster '${clusterName}' did not become ready within ${timeoutMs}ms`);
}

/** Wait for processes_sampler to appear in system.view_refreshes on all nodes. */
async function waitForSamplers(clients: ClickHouseClient[], timeoutMs = 30_000): Promise<void> {
  const start = Date.now();
  for (let i = 0; i < clients.length; i++) {
    while (Date.now() - start < timeoutMs) {
      try {
        const result = await clients[i].query({
          query: `SELECT view FROM system.view_refreshes WHERE database = 'tracehouse' AND view = 'processes_sampler'`,
          format: 'JSONEachRow',
        });
        const rows = await result.json<{ view: string }>();
        if (rows.length > 0) break;
      } catch {
        // Retry
      }
      await new Promise(r => setTimeout(r, 1000));
    }
  }
}

/** Tear down the sampling cluster. */
export async function stopSamplingCluster(ctx: SamplingClusterContext): Promise<void> {
  for (const client of ctx.clients) await client.close();
  for (const c of ctx.containers) await c.stop();
  await ctx.keeper.stop();
  await ctx.network.stop();
}
