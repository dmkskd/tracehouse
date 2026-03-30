/**
 * Types for the Replication Topology Map feature.
 * Shows per-table replica/shard layout with ZooKeeper connections.
 */

/** Per-replica state from system.replicas (not aggregated). */
export interface ReplicaInfo {
  hostname: string;
  database: string;
  table: string;
  replica_name: string;
  zookeeper_path: string;
  replica_path: string;
  is_leader: number;
  is_readonly: number;
  is_session_expired: number;
  absolute_delay: number;
  queue_size: number;
  inserts_in_queue: number;
  merges_in_queue: number;
  log_pointer: number;
  log_max_index: number;
  total_replicas: number;
  active_replicas: number;
  [key: string]: unknown;
}

/** Per-replica part statistics from system.parts. */
export interface ReplicaPartStats {
  hostname: string;
  part_count: number;
  bytes_on_disk: number;
  total_rows: number;
  partition_count: number;
  [key: string]: unknown;
}

/** Partition distribution per hostname — for data skew visualization. */
export interface ShardPartitionDist {
  hostname: string;
  partition_id: string;
  part_count: number;
  bytes_on_disk: number;
  total_rows: number;
  [key: string]: unknown;
}

/** A replica node enriched with part stats, positioned within a shard. */
export interface TopologyReplica {
  info: ReplicaInfo;
  parts: ReplicaPartStats | null;
  /** Derived shard number from the ZooKeeper path. */
  shardNum: number;
}

/** A shard containing its replicas and aggregate stats. */
export interface TopologyShard {
  shardNum: number;
  zkPath: string;
  replicas: TopologyReplica[];
  totalBytes: number;
  totalRows: number;
  totalParts: number;
  /** True when every replica in the shard reports is_leader=1 (normal for Replicated databases). */
  allLeaders: boolean;
}

/** Queue entry for display in the topology map. */
export interface TopologyQueueEntry {
  hostname: string;
  replica_name: string;
  type: string;
  new_part_name: string;
  is_currently_executing: number;
  num_tries: number;
  num_postponed: number;
  last_exception: string;
  create_time: string;
}

/** Table engine metadata for context display. */
export interface TableEngineInfo {
  engine: string;
  partitionKey: string;
  sortingKey: string;
  /** Name of the Distributed table that routes to this local table, if any. */
  distributedTable: string | null;
  /** Sharding expression from the Distributed table, if any. */
  shardingKey: string | null;
}

/** Health status for a single replica. */
export type ReplicaHealthStatus = 'healthy' | 'warning' | 'error';

/** Health classification result for a replica. */
export interface ReplicaHealth {
  status: ReplicaHealthStatus;
  reasons: string[];
}

/** Severity level for replication delay. */
export type DelaySeverity = 'ok' | 'lagging' | 'critical';

/** A single ZooKeeper child node — used for browsing log, mutations, etc. */
export interface ZkChildNode {
  name: string;
  value: string;
  numChildren: number;
  dataLength: number;
  ctime: string;
  mtime: string;
}

/** ZooKeeper sub-path stats for a replicated table. */
export interface ZkPathStats {
  /** Sub-path (e.g. '/clickhouse/tables/01/events/log') */
  path: string;
  /** Number of children (log entries, mutations, blocks, etc.) */
  childCount: number;
  /** Total data bytes stored in children */
  totalDataBytes: number;
}

/** Parsed Keeper metadata for a replicated table's ZK structure. */
export interface KeeperTableInfo {
  /** The table's root ZK path */
  zkPath: string;
  /** Number of entries in /log */
  logEntries: number;
  /** Number of pending mutations in /mutations */
  mutations: number;
  /** Number of dedup blocks in /blocks */
  blocks: number;
  /** Number of registered replicas in /replicas */
  registeredReplicas: number;
  /** Whether quorum path has children (active insert quorum) */
  hasQuorum: boolean;
}

/** Keeper/ZooKeeper connection info from system.zookeeper_connection. */
export interface KeeperConnection {
  host: string;
  port: number;
  index: number;
  connectedTime: string;
  isExpired: number;
  keeperApiVersion: number;
}

/** Distribution queue entry — pending sends for a Distributed table. */
export interface DistributionQueueEntry {
  database: string;
  table: string;
  isBlocked: number;
  errorCount: number;
  dataFiles: number;
  dataCompressedBytes: number;
  brokenDataFiles: number;
  brokenDataCompressedBytes: number;
  lastException: string;
}

/** Full topology data for a single replicated table. */
export interface ReplicationTopologyData {
  database: string;
  table: string;
  shards: TopologyShard[];
  /** Max log_max_index across all replicas (the "head" of the shared log). */
  logHead: number;
  /** Total bytes across all shards — for computing shard data %. */
  totalBytes: number;
  /** Partition distribution for the data skew bar. */
  partitionDist: ShardPartitionDist[];
  /** Queue entries for all replicas (from existing queue fetch). */
  queueEntries: TopologyQueueEntry[];
  /** Engine metadata for context display. */
  engineInfo: TableEngineInfo | null;
  /** Keeper metadata for this table's ZK paths. */
  keeperInfo: KeeperTableInfo | null;
  /** Keeper connection info. */
  keeperConnections: KeeperConnection[];
  /** Distribution queue entries (for Distributed tables routing to this table). */
  distributionQueue: DistributionQueueEntry[];
}
