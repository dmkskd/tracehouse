/**
 * Mappers for replication topology raw query rows → typed interfaces.
 */
import type {
  ReplicaInfo,
  ReplicaPartStats,
  ShardPartitionDist,
  TopologyQueueEntry,
} from '../types/replication-topology.js';
import { type RawRow, toInt, toStr } from './helpers.js';

export function mapReplicaInfo(row: RawRow): ReplicaInfo {
  return {
    hostname: toStr(row.hostname),
    database: toStr(row.database),
    table: toStr(row.table),
    replica_name: toStr(row.replica_name),
    zookeeper_path: toStr(row.zookeeper_path),
    replica_path: toStr(row.replica_path),
    is_leader: toInt(row.is_leader),
    is_readonly: toInt(row.is_readonly),
    is_session_expired: toInt(row.is_session_expired),
    absolute_delay: toInt(row.absolute_delay),
    queue_size: toInt(row.queue_size),
    inserts_in_queue: toInt(row.inserts_in_queue),
    merges_in_queue: toInt(row.merges_in_queue),
    log_pointer: toInt(row.log_pointer),
    log_max_index: toInt(row.log_max_index),
    total_replicas: toInt(row.total_replicas),
    active_replicas: toInt(row.active_replicas),
  };
}

export function mapReplicaPartStats(row: RawRow): ReplicaPartStats {
  return {
    hostname: toStr(row.hostname),
    part_count: toInt(row.part_count),
    bytes_on_disk: toInt(row.bytes_on_disk),
    total_rows: toInt(row.total_rows),
    partition_count: toInt(row.partition_count),
  };
}

export function mapShardPartitionDist(row: RawRow): ShardPartitionDist {
  return {
    hostname: toStr(row.hostname),
    partition_id: toStr(row.partition_id),
    part_count: toInt(row.part_count),
    bytes_on_disk: toInt(row.bytes_on_disk),
    total_rows: toInt(row.total_rows),
  };
}

export function mapQueueEntry(row: RawRow, filterTable: string): TopologyQueueEntry | null {
  if (toStr(row.table) !== filterTable) return null;
  return {
    hostname: toStr(row.hostname),
    replica_name: toStr(row.replica_name),
    type: toStr(row.type),
    new_part_name: toStr(row.new_part_name),
    is_currently_executing: toInt(row.is_currently_executing),
    num_tries: toInt(row.num_tries),
    num_postponed: toInt(row.num_postponed),
    last_exception: toStr(row.last_exception),
    create_time: toStr(row.create_time),
  };
}
