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
}
