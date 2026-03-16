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

/**
 * Parse the `merged_from` column which may arrive as an array, JSON string,
 * or comma-separated string depending on the adapter.
 */
export function parseMergedFrom(val: unknown): string[] {
  if (Array.isArray(val)) return val.map(String);
  if (typeof val === 'string') {
    const trimmed = val.trim();
    if (!trimmed || trimmed === '[]') return [];
    try {
      const parsed = JSON.parse(trimmed);
      if (Array.isArray(parsed)) return parsed.map(String);
      return [trimmed];
    } catch {
      // Handle ClickHouse array format: ['a','b','c'] or comma-separated
      return trimmed
        .replace(/^\[|\]$/g, '')
        .split(',')
        .map(s => s.trim().replace(/^'|'$/g, ''))
        .filter(Boolean);
    }
  }
  return [];
}
