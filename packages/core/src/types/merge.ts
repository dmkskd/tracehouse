export interface MergeInfo {
  database: string;
  table: string;
  elapsed: number;
  progress: number;
  num_parts: number;
  source_part_names: string[];
  result_part_name: string;
  total_size_bytes_compressed: number;
  rows_read: number;
  rows_written: number;
  memory_usage: number;
  merge_type: string;
  merge_algorithm: string;
  is_mutation: boolean;
  bytes_read_uncompressed: number;
  bytes_written_uncompressed: number;
  columns_written: number;
  thread_id: number;
  /** Server hostname where this merge is executing */
  hostname?: string;
}

export interface MergeHistoryRecord {
  event_time: string;
  event_type: string;
  database: string;
  table: string;
  part_name: string;
  partition_id: string;
  rows: number;
  size_in_bytes: number;
  duration_ms: number;
  merge_reason: string;
  source_part_names: string[];
  bytes_uncompressed: number;
  read_bytes: number;
  read_rows: number;
  peak_memory_usage: number;
  size_diff: number;
  size_diff_pct: number;
  /** Row count delta: output rows minus input rows. Negative = rows removed (e.g. TTL delete). */
  rows_diff: number;
  /** Server hostname where this merge was executed */
  hostname?: string;
  /** Disk where the part resides after this event (from system.part_log) */
  disk_name?: string;
  /** Physical path on disk after this event */
  path_on_disk?: string;
  /** Merge algorithm used (Vertical or Horizontal) */
  merge_algorithm?: string;
  /** query_id from system.part_log — links to query_log and text_log */
  query_id?: string;
  /** Per-operation ProfileEvents map directly from system.part_log */
  profile_events?: Record<string, number>;
  /** Error code from part_log (0 = no error) */
  error?: number;
  /** Exception message from part_log */
  exception?: string;
}

/** Storage policy volume mapping from system.storage_policies */
export interface StoragePolicyVolume {
  policyName: string;
  volumeName: string;
  disks: string[];
}

export interface MutationInfo {
  database: string;
  table: string;
  mutation_id: string;
  command: string;
  create_time: string;
  parts_to_do: number;
  total_parts: number;
  parts_in_progress: number;
  parts_done: number;
  is_done: boolean;
  latest_failed_part: string;
  latest_fail_time: string;
  latest_fail_reason: string;
  is_killed: boolean;
  status: string;
  progress: number;
  /** Part names still waiting to be mutated */
  parts_to_do_names: string[];
  /** Part names currently being mutated */
  parts_in_progress_names: string[];
}

/** Dependency analysis for a mutation — shows how merges and other mutations interact */
export interface MutationDependencyInfo {
  /** The mutation this analysis is for */
  mutation_id: string;
  database: string;
  table: string;
  /** Parts waiting, with their status relative to active merges */
  part_statuses: MutationPartStatus[];
  /** Other mutations that share parts with this one (co-dependent) */
  co_dependent_mutations: CoDependentMutation[];
  /** Summary: how many parts are covered by active merges */
  parts_covered_by_merges: number;
  /** Summary: how many distinct active merges touch this mutation's parts */
  active_merges_covering: number;
}

export interface MutationPartStatus {
  part_name: string;
  /** 'mutating' = actively being mutated, 'merging' = part is in an active merge, 'idle' = waiting */
  status: 'mutating' | 'merging' | 'idle';
  /** If merging, the result part name of the merge */
  merge_result_part?: string;
  /** If merging, the progress of that merge (0-1) */
  merge_progress?: number;
  /** If merging, elapsed seconds */
  merge_elapsed?: number;
}

export interface CoDependentMutation {
  mutation_id: string;
  command: string;
  /** How many parts are shared between the two mutations */
  shared_parts_count: number;
  /** The actual shared part names */
  shared_parts: string[];
}

export interface BackgroundPoolMetrics {
  merge_pool_size: number;
  merge_pool_active: number;
  move_pool_size: number;
  move_pool_active: number;
  fetch_pool_size: number;
  fetch_pool_active: number;
  schedule_pool_size: number;
  schedule_pool_active: number;
  common_pool_size: number;
  common_pool_active: number;
  distributed_pool_size: number;
  distributed_pool_active: number;
  active_merges: number;
  active_mutations: number;
  active_parts: number;
  outdated_parts: number;
  outdated_parts_bytes: number;
}


export interface MutationHistoryRecord {
  database: string;
  table: string;
  mutation_id: string;
  command: string;
  create_time: string;
  is_done: boolean;
  is_killed: boolean;
  latest_failed_part: string;
  latest_fail_time: string;
  latest_fail_reason: string;
}


/**
 * Text log entry for a merge/mutation operation.
 * Fetched from system.text_log by query_id or time-window correlation.
 */
export interface MergeTextLog {
  event_time: string;
  event_time_microseconds: string;
  query_id: string;
  level: string;
  message: string;
  source: string;
  thread_id: number;
  thread_name: string;
}

export interface MergeThroughputEstimate {
  merge_algorithm: string;
  merge_count: number;
  avg_bytes_per_sec: number;
  median_bytes_per_sec: number;
  avg_duration_ms: number;
  avg_size_bytes: number;
}
