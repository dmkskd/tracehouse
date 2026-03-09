import type { QueryMetrics, QueryHistoryItem } from '../types/query.js';
import { normalizeTimestamp } from './timestamp.js';
import { type RawRow, toInt, toStr, toFloat, shortenHostname } from './helpers.js';

export function mapQueryMetrics(row: RawRow): QueryMetrics {
  return {
    query_id: toStr(row.query_id),
    user: toStr(row.user),
    query: toStr(row.query),
    query_kind: toStr(row.query_kind),
    elapsed_seconds: toFloat(row.elapsed_seconds),
    memory_usage: toInt(row.memory_usage),
    read_rows: toInt(row.read_rows),
    read_bytes: toInt(row.read_bytes),
    total_rows_approx: toInt(row.total_rows_approx),
    progress: toFloat(row.progress),
    is_initial_query: row.is_initial_query != null ? toInt(row.is_initial_query) : undefined,
    initial_query_id: row.initial_query_id != null ? toStr(row.initial_query_id) : undefined,
    hostname: row.hostname != null ? shortenHostname(row.hostname) : undefined,
  };
}

export function mapQueryHistoryItem(row: RawRow): QueryHistoryItem {
  const readRows = toInt(row.read_rows);
  const resultRows = toInt(row.result_rows);
  const exception = row.exception != null ? toStr(row.exception) : null;
  
  // Compute efficiency score as marks pruning effectiveness (percentage of marks skipped)
  // Higher = better index usage. null when no marks data available (e.g. SELECT 1, system queries)
  const selectedMarks = row.selected_marks != null ? toInt(row.selected_marks) : undefined;
  const selectedMarksTotal = row.selected_marks_total != null ? toInt(row.selected_marks_total) : undefined;
  const efficiencyScore: number | null = (selectedMarksTotal !== undefined && selectedMarksTotal > 0 && selectedMarks !== undefined)
    ? ((selectedMarksTotal - selectedMarks) / selectedMarksTotal) * 100
    : null;
  
  // Compute type based on exception
  const type = exception ? 'error' : 'success';
  
  return {
    query_id: toStr(row.query_id),
    query_type: toStr(row.query_type),
    query_kind: toStr(row.query_kind),
    query_start_time: normalizeTimestamp(row.query_start_time),
    query_duration_ms: toInt(row.query_duration_ms),
    read_rows: readRows,
    read_bytes: toInt(row.read_bytes),
    result_rows: resultRows,
    result_bytes: toInt(row.result_bytes),
    memory_usage: toInt(row.memory_usage),
    query: toStr(row.query),
    exception,
    user: toStr(row.user),
    client_hostname: toStr(row.client_hostname),
    type,
    efficiency_score: efficiencyScore,
    // ProfileEvents metrics
    cpu_time_us: toInt(row.cpu_time_us),
    network_send_bytes: toInt(row.network_send_bytes),
    network_receive_bytes: toInt(row.network_receive_bytes),
    disk_read_bytes: toInt(row.disk_read_bytes),
    disk_write_bytes: toInt(row.disk_write_bytes),
    // Index/parts selectivity metrics
    selected_parts: row.selected_parts != null ? toInt(row.selected_parts) : undefined,
    selected_parts_total: row.selected_parts_total != null ? toInt(row.selected_parts_total) : undefined,
    selected_marks: row.selected_marks != null ? toInt(row.selected_marks) : undefined,
    selected_marks_total: row.selected_marks_total != null ? toInt(row.selected_marks_total) : undefined,
    selected_ranges: row.selected_ranges != null ? toInt(row.selected_ranges) : undefined,
    mark_cache_hits: row.mark_cache_hits != null ? toInt(row.mark_cache_hits) : undefined,
    mark_cache_misses: row.mark_cache_misses != null ? toInt(row.mark_cache_misses) : undefined,
    io_wait_us: row.io_wait_us != null ? toInt(row.io_wait_us) : undefined,
    real_time_us: row.real_time_us != null ? toInt(row.real_time_us) : undefined,
    user_time_us: row.user_time_us != null ? toInt(row.user_time_us) : undefined,
    system_time_us: row.system_time_us != null ? toInt(row.system_time_us) : undefined,
    Settings: row.Settings != null ? row.Settings as Record<string, string> : undefined,
    is_initial_query: row.is_initial_query != null ? toInt(row.is_initial_query) : undefined,
    initial_query_id: row.initial_query_id != null ? toStr(row.initial_query_id) : undefined,
    initial_address: row.initial_address != null ? toStr(row.initial_address) : undefined,
    hostname: row.hostname != null ? shortenHostname(row.hostname) : undefined,
    databases: Array.isArray(row.databases) ? (row.databases as string[]).filter(Boolean) : undefined,
    tables: Array.isArray(row.tables) ? (row.tables as string[]).filter(Boolean) : undefined,
  };
}
