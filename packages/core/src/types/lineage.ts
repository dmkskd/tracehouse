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
