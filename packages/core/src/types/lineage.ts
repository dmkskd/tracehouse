/**
 * Parsed components of a ClickHouse part name.
 *
 * Part names follow these patterns:
 * - Regular: `partition_minBlock_maxBlock_level` (e.g., `202602_1_100_3`)
 * - Mutated: `partition_minBlock_maxBlock_level_mutationVersion` (e.g., `202602_1_100_3_19118`)
 */
export interface ParsedPartName {
  /** Original part name */
  name: string;
  /** Partition ID (may contain underscores) */
  partition: string;
  /** Minimum block number */
  minBlock: number;
  /** Maximum block number */
  maxBlock: number;
  /** Merge level (0 = L0/original part) */
  level: number;
  /** Mutation version (undefined if not mutated) */
  mutationVersion?: number;
  /** Whether this part was created by a mutation */
  isMutated: boolean;
}

export interface MergeEvent {
  part_name: string;
  merged_from: string[];
  event_time: string;
  duration_ms: number;
  rows: number;
  size_in_bytes: number;
  bytes_uncompressed: number;
  read_bytes: number;
  peak_memory_usage: number;
  merge_reason: string;
  merge_algorithm: string;
  level: number;
  event_type?: 'MergeParts' | 'MutatePart';
}

export interface LineageNode {
  part_name: string;
  level: number;
  rows: number;
  size_in_bytes: number;
  event_time?: string;
  merge_event?: MergeEvent;
  children: LineageNode[];
}

export interface PartLineage {
  root: LineageNode;
  total_merges: number;
  total_original_parts: number;
  total_time_ms: number;
  original_total_size: number;
  final_size: number;
}
