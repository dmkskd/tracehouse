import type { MergeEvent } from '../types/lineage.js';
import { normalizeTimestamp } from './timestamp.js';
import { type RawRow, toInt, toStr, toStrArray } from './helpers.js';

export function mapMergeEvent(row: RawRow): MergeEvent {
  return {
    part_name: toStr(row.part_name),
    merged_from: toStrArray(row.merged_from),
    event_time: normalizeTimestamp(row.event_time),
    duration_ms: toInt(row.duration_ms),
    rows: toInt(row.rows),
    size_in_bytes: toInt(row.size_in_bytes),
    bytes_uncompressed: toInt(row.bytes_uncompressed),
    read_bytes: toInt(row.read_bytes),
    peak_memory_usage: toInt(row.peak_memory_usage),
    merge_reason: toStr(row.merge_reason),
    merge_algorithm: toStr(row.merge_algorithm),
    level: toInt(row.level),
  };
}
