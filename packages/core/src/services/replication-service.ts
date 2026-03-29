/**
 * ReplicationService — assembles replication topology data from raw query results.
 * All business logic for shard grouping, engine parsing, and health classification lives here.
 */
import type { IClickHouseAdapter } from '../adapters/types.js';
import type {
  ReplicaInfo,
  ReplicaPartStats,
  ShardPartitionDist,
  TopologyShard,
  TopologyReplica,
  TopologyQueueEntry,
  ReplicationTopologyData,
  TableEngineInfo,
  ReplicaHealth,
  ReplicaHealthStatus,
} from '../types/replication-topology.js';
import { GET_REPLICATION_QUEUE } from '../queries/cluster-queries.js';
import {
  GET_REPLICA_TOPOLOGY,
  GET_REPLICA_PARTS,
  GET_SHARD_DISTRIBUTION,
  GET_TABLE_ENGINE_INFO,
  GET_DISTRIBUTED_FOR_TABLE,
} from '../queries/replication-queries.js';
import { buildQuery, tagQuery } from '../queries/builder.js';
import { sourceTag } from '../queries/source-tags.js';
import {
  mapReplicaInfo,
  mapReplicaPartStats,
  mapShardPartitionDist,
  mapQueueEntry,
} from '../mappers/replication-mappers.js';
import type { RawRow } from '../mappers/helpers.js';
import { toStr } from '../mappers/helpers.js';

const TAB = 'replication';

export class ReplicationServiceError extends Error {
  constructor(message: string, public readonly cause?: Error) {
    super(message);
    this.name = 'ReplicationServiceError';
  }
}

// ── Pure functions (exported for testing) ──

/** Extract the shard identifier from a ZooKeeper path (last path segment). */
export function extractShardId(zkPath: string): string {
  const parts = zkPath.replace(/\/$/, '').split('/');
  return parts[parts.length - 1] || '01';
}

/** Parse shard ID string to number. */
export function shardIdToNum(id: string): number {
  const n = parseInt(id, 10);
  return isNaN(n) ? 0 : n;
}

/** Group replica rows by ZooKeeper path into shards with aggregate stats. */
export function assembleShards(
  replicaRows: ReplicaInfo[],
  partsByHost: Map<string, ReplicaPartStats>,
): { shards: TopologyShard[]; logHead: number } {
  const shardMap = new Map<string, TopologyReplica[]>();
  let maxLogIndex = 0;

  for (const r of replicaRows) {
    const zkPath = r.zookeeper_path;
    if (!shardMap.has(zkPath)) shardMap.set(zkPath, []);
    shardMap.get(zkPath)!.push({
      info: r,
      parts: partsByHost.get(r.hostname) || null,
      shardNum: shardIdToNum(extractShardId(zkPath)),
    });
    maxLogIndex = Math.max(maxLogIndex, r.log_max_index);
  }

  const shards: TopologyShard[] = [...shardMap.entries()]
    .map(([zkPath, replicas]) => ({
      shardNum: replicas[0].shardNum,
      zkPath,
      replicas,
      totalBytes: replicas.reduce((s, r) => Math.max(s, r.parts ? r.parts.bytes_on_disk : 0), 0),
      totalRows: replicas.reduce((s, r) => Math.max(s, r.parts ? r.parts.total_rows : 0), 0),
      totalParts: replicas.reduce((s, r) => Math.max(s, r.parts ? r.parts.part_count : 0), 0),
      allLeaders: replicas.length > 1 && replicas.every(r => r.info.is_leader === 1),
    }))
    .sort((a, b) => a.shardNum - b.shardNum);

  return { shards, logHead: maxLogIndex };
}

/** Extract sharding key from a Distributed engine_full string.
 *  Format: Distributed('cluster', 'db', 'table', <sharding_expr>)
 *  The sharding expr may contain nested parens (e.g. sipHash64(domain)),
 *  so we find the last closing paren and take everything between the 4th comma and it. */
export function parseShardingKey(engineFull: string): string | null {
  // Find the opening "Distributed(" prefix
  const prefix = 'Distributed(';
  const start = engineFull.indexOf(prefix);
  if (start === -1) return null;
  // Find the content between Distributed( ... ) — match the outermost closing paren
  const inner = engineFull.substring(start + prefix.length);
  // Walk past 3 commas (cluster, db, table) respecting quotes
  let commaCount = 0;
  let inQuote = false;
  let depth = 0;
  let i = 0;
  for (; i < inner.length && commaCount < 3; i++) {
    const ch = inner[i];
    if (ch === "'" && (i === 0 || inner[i - 1] !== '\\')) inQuote = !inQuote;
    if (!inQuote) {
      if (ch === '(') depth++;
      if (ch === ')') depth--;
      if (ch === ',' && depth === 0) commaCount++;
    }
  }
  if (commaCount < 3) return null;
  // Everything from here to the last ')' at depth 0 is the sharding key
  const rest = inner.substring(i);
  // Find the closing ')' that matches the Distributed( opening
  let endIdx = -1;
  depth = 0;
  for (let j = 0; j < rest.length; j++) {
    const ch = rest[j];
    if (ch === '(') depth++;
    if (ch === ')') {
      if (depth === 0) { endIdx = j; break; }
      depth--;
    }
  }
  if (endIdx === -1) return null;
  const key = rest.substring(0, endIdx).trim();
  return key || null;
}

/** Build TableEngineInfo from raw query results. */
export function buildEngineInfo(
  engineRows: RawRow[],
  distTableRows: RawRow[],
): TableEngineInfo | null {
  if (engineRows.length === 0) return null;
  const e = engineRows[0];
  let shardingKey: string | null = null;
  let distributedTable: string | null = null;
  if (distTableRows.length > 0) {
    distributedTable = toStr(distTableRows[0].dist_table);
    shardingKey = parseShardingKey(toStr(distTableRows[0].engine_full));
  }
  return {
    engine: toStr(e.engine),
    partitionKey: toStr(e.partition_key),
    sortingKey: toStr(e.sorting_key),
    distributedTable,
    shardingKey,
  };
}

/** Filter and map queue entries for a specific table. */
export function filterQueueEntries(
  queueRows: RawRow[],
  table: string,
): TopologyQueueEntry[] {
  const entries: TopologyQueueEntry[] = [];
  for (const row of queueRows) {
    const mapped = mapQueueEntry(row, table);
    if (mapped) entries.push(mapped);
  }
  return entries;
}

/** Stuck queue threshold — entries with more retries than this and an exception are stuck. */
const STUCK_QUEUE_TRIES = 5;

/** Classify a replica's health based on its state and queue entries.
 *  - error: readonly or session expired — replica cannot function
 *  - warning: has stuck queue entries (repeated failures with exceptions)
 *  - healthy: everything else (brief delays and active queue items are normal) */
export function classifyReplicaHealth(
  info: ReplicaInfo,
  queueEntries: TopologyQueueEntry[],
): ReplicaHealth {
  const reasons: string[] = [];

  // Error conditions — replica is broken
  if (info.is_session_expired === 1) {
    reasons.push('ZooKeeper session expired');
  }
  if (info.is_readonly === 1) {
    reasons.push('Replica is read-only (lost Keeper connection)');
  }
  if (reasons.length > 0) {
    return { status: 'error', reasons };
  }

  // Warning conditions — something is stuck
  const myQueue = queueEntries.filter(e => e.replica_name === info.replica_name);
  const stuckEntries = myQueue.filter(
    e => e.num_tries > STUCK_QUEUE_TRIES && e.last_exception !== '',
  );
  if (stuckEntries.length > 0) {
    reasons.push(`${stuckEntries.length} stuck queue entr${stuckEntries.length === 1 ? 'y' : 'ies'} (>${STUCK_QUEUE_TRIES} retries with errors)`);
    return { status: 'warning', reasons };
  }

  return { status: 'healthy', reasons: [] };
}

// ── Service class ──

export class ReplicationService {
  constructor(private adapter: IClickHouseAdapter) {}

  /** Fetch full topology data for a single replicated table. */
  async getTopology(database: string, table: string): Promise<ReplicationTopologyData> {
    try {
      const params = { database, table };
      const [replicaRaw, partRaw, distRaw, queueRaw, engineRaw, distTableRaw] = await Promise.all([
        this.adapter.executeQuery<RawRow>(tagQuery(buildQuery(GET_REPLICA_TOPOLOGY, params), sourceTag(TAB, 'topology'))),
        this.adapter.executeQuery<RawRow>(tagQuery(buildQuery(GET_REPLICA_PARTS, params), sourceTag(TAB, 'parts'))),
        this.adapter.executeQuery<RawRow>(tagQuery(buildQuery(GET_SHARD_DISTRIBUTION, params), sourceTag(TAB, 'dist'))),
        this.adapter.executeQuery<RawRow>(tagQuery(buildQuery(GET_REPLICATION_QUEUE, { database }), sourceTag(TAB, 'queue'))),
        this.adapter.executeQuery<RawRow>(tagQuery(buildQuery(GET_TABLE_ENGINE_INFO, params), sourceTag(TAB, 'engine'))),
        this.adapter.executeQuery<RawRow>(tagQuery(buildQuery(GET_DISTRIBUTED_FOR_TABLE, params), sourceTag(TAB, 'dist-table'))),
      ]);

      const replicaRows = replicaRaw.map(mapReplicaInfo);
      const partRows = partRaw.map(mapReplicaPartStats);
      const partitionDist = distRaw.map(mapShardPartitionDist);
      const queueEntries = filterQueueEntries(queueRaw, table);
      const engineInfo = buildEngineInfo(engineRaw, distTableRaw);

      const partsByHost = new Map(partRows.map(p => [p.hostname, p]));
      const { shards, logHead } = assembleShards(replicaRows, partsByHost);
      const totalBytes = shards.reduce((s, sh) => s + sh.totalBytes, 0);

      return {
        database,
        table,
        shards,
        logHead,
        totalBytes,
        partitionDist,
        queueEntries,
        engineInfo,
      };
    } catch (err) {
      throw new ReplicationServiceError(
        `Failed to fetch topology for ${database}.${table}`,
        err instanceof Error ? err : undefined,
      );
    }
  }
}
