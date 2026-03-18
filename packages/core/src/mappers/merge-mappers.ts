import type { MergeInfo, MergeHistoryRecord, MutationInfo, MutationHistoryRecord, BackgroundPoolMetrics, MergeTextLog } from '../types/merge.js';
import { normalizeTimestamp } from './timestamp.js';
import { type RawRow, toInt, toStr, toFloat, toBool, toStrArray } from './helpers.js';
import { classifyMergeHistory, refineCategoryWithRowDiff } from '../utils/merge-classification.js';

export function mapMergeInfo(row: RawRow): MergeInfo {
  return {
    database: toStr(row.database),
    table: toStr(row.table),
    elapsed: toFloat(row.elapsed),
    progress: toFloat(row.progress),
    num_parts: toInt(row.num_parts),
    source_part_names: toStrArray(row.source_part_names),
    result_part_name: toStr(row.result_part_name),
    total_size_bytes_compressed: toInt(row.total_size_bytes_compressed),
    rows_read: toInt(row.rows_read),
    rows_written: toInt(row.rows_written),
    memory_usage: toInt(row.memory_usage),
    merge_type: toStr(row.merge_type),
    merge_algorithm: toStr(row.merge_algorithm),
    is_mutation: toBool(row.is_mutation),
    bytes_read_uncompressed: toInt(row.bytes_read_uncompressed),
    bytes_written_uncompressed: toInt(row.bytes_written_uncompressed),
    columns_written: toInt(row.columns_written),
    thread_id: toInt(row.thread_id),
    hostname: row.hostname != null ? toStr(row.hostname) : undefined,
  };
}

export function mapMergeHistoryRecord(row: RawRow): MergeHistoryRecord {
  const sizeInBytes = toInt(row.size_in_bytes);
  const readBytes = toInt(row.read_bytes);
  const readRows = toInt(row.read_rows);
  const bytesUncompressed = toInt(row.bytes_uncompressed);
  const outputRows = toInt(row.rows);
  // Compare uncompressed output vs uncompressed input for true merge reduction.
  // size_in_bytes is compressed on disk; read_bytes is uncompressed input —
  // comparing those mixes compression ratio into the merge diff.
  const sizeDiff = bytesUncompressed - readBytes;
  const sizeDiffPct = readBytes > 0 ? (sizeDiff / readBytes) * 100 : 0;
  // Row delta: negative means rows were removed (e.g. TTL delete merge).
  const rowsDiff = readRows > 0 ? outputRows - readRows : 0;
  // Classify merge reason, then refine: a Regular merge with row loss
  // is likely a lightweight delete cleanup.
  const baseCategory = classifyMergeHistory(toStr(row.event_type), toStr(row.merge_reason));
  const category = refineCategoryWithRowDiff(baseCategory, rowsDiff);
  
  return {
    event_time: normalizeTimestamp(row.event_time),
    event_type: toStr(row.event_type),
    database: toStr(row.database),
    table: toStr(row.table),
    part_name: toStr(row.part_name),
    partition_id: toStr(row.partition_id),
    rows: outputRows,
    size_in_bytes: sizeInBytes,
    duration_ms: toInt(row.duration_ms),
    merge_reason: category,
    source_part_names: toStrArray(row.merged_from),
    bytes_uncompressed: toInt(row.bytes_uncompressed),
    read_bytes: readBytes,
    read_rows: readRows,
    peak_memory_usage: toInt(row.peak_memory_usage),
    size_diff: sizeDiff,
    size_diff_pct: sizeDiffPct,
    rows_diff: rowsDiff,
    hostname: row.hostname != null ? toStr(row.hostname) : undefined,
    disk_name: row.disk_name != null ? toStr(row.disk_name) : undefined,
    path_on_disk: row.path_on_disk != null ? toStr(row.path_on_disk) : undefined,
    merge_algorithm: row.merge_algorithm != null ? toStr(row.merge_algorithm) : undefined,
    query_id: row.query_id != null ? toStr(row.query_id) : undefined,
    profile_events: parseProfileEvents(row.ProfileEvents),
    error: row.error != null ? toInt(row.error) : undefined,
    exception: row.exception != null ? toStr(row.exception) : undefined,
  };
}

export function mapMutationInfo(row: RawRow): MutationInfo {
  const partsToDo = toInt(row.parts_to_do);
  const isDone = toBool(row.is_done);
  const isKilled = toBool(row.is_killed);
  const latestFailReason = toStr(row.latest_fail_reason);
  const latestFailedPart = toStr(row.latest_failed_part);
  const latestFailTime = row.latest_fail_time ? normalizeTimestamp(row.latest_fail_time) : '';

  // system.mutations only provides parts_to_do (parts remaining).
  // In-progress detection comes from cross-referencing with system.merges in the UI.
  // parts_in_progress_names is left empty here; the UI links mutations to active
  // merges via getMergeForMutation() in mutationDependencyHelpers.

  // Compute status based on mutation state
  let status: string;
  if (isKilled) {
    status = 'killed';
  } else if (latestFailReason) {
    status = 'failed';
  } else if (isDone) {
    status = 'done';
  } else if (partsToDo > 0) {
    status = 'running';
  } else {
    status = 'queued';
  }

  const progress = isDone ? 1 : 0;

  return {
    database: toStr(row.database),
    table: toStr(row.table),
    mutation_id: toStr(row.mutation_id),
    command: toStr(row.command),
    create_time: normalizeTimestamp(row.create_time),
    parts_to_do: partsToDo,
    total_parts: partsToDo,
    parts_in_progress: 0,
    parts_done: 0,
    is_done: isDone,
    latest_failed_part: latestFailedPart,
    latest_fail_time: latestFailTime,
    latest_fail_reason: latestFailReason,
    is_killed: isKilled,
    status,
    progress,
    parts_to_do_names: toStrArray(row.parts_to_do_names),
    parts_in_progress_names: [],
  };
}

export function mapBackgroundPoolMetrics(row: RawRow, outdatedSizeRow?: RawRow): BackgroundPoolMetrics {
  return {
    merge_pool_size: toInt(row.merge_pool_size),
    merge_pool_active: toInt(row.merge_pool_active),
    move_pool_size: toInt(row.move_pool_size),
    move_pool_active: toInt(row.move_pool_active),
    fetch_pool_size: toInt(row.fetch_pool_size),
    fetch_pool_active: toInt(row.fetch_pool_active),
    schedule_pool_size: toInt(row.schedule_pool_size),
    schedule_pool_active: toInt(row.schedule_pool_active),
    common_pool_size: toInt(row.common_pool_size),
    common_pool_active: toInt(row.common_pool_active),
    distributed_pool_size: toInt(row.distributed_pool_size),
    distributed_pool_active: toInt(row.distributed_pool_active),
    active_merges: toInt(row.active_merges),
    active_mutations: toInt(row.active_mutations),
    active_parts: toInt(row.active_parts),
    outdated_parts: toInt(row.outdated_parts),
    outdated_parts_bytes: outdatedSizeRow ? toInt(outdatedSizeRow.outdated_parts_bytes) : 0,
  };
}


export function mapMutationHistoryRecord(row: RawRow): MutationHistoryRecord {
  return {
    database: toStr(row.database),
    table: toStr(row.table),
    mutation_id: toStr(row.mutation_id),
    command: toStr(row.command),
    create_time: normalizeTimestamp(row.create_time),
    is_done: toBool(row.is_done),
    is_killed: toBool(row.is_killed),
    latest_failed_part: toStr(row.latest_failed_part),
    latest_fail_time: row.latest_fail_time ? normalizeTimestamp(row.latest_fail_time) : '',
    latest_fail_reason: toStr(row.latest_fail_reason),
  };
}


/** Parse ProfileEvents from a ClickHouse Map(String, UInt64) column. */
function parseProfileEvents(val: unknown): Record<string, number> | undefined {
  if (!val) return undefined;
  const result: Record<string, number> = {};
  if (val instanceof Map) {
    for (const [k, v] of val) {
      result[String(k)] = Number(v);
    }
  } else if (typeof val === 'object' && val !== null) {
    for (const [k, v] of Object.entries(val as Record<string, unknown>)) {
      result[k] = Number(v);
    }
  }
  // Return undefined if empty (no ProfileEvents available)
  return Object.keys(result).length > 0 ? result : undefined;
}

export function mapMergeTextLog(row: RawRow): MergeTextLog {
  return {
    event_time: toStr(row.event_time),
    event_time_microseconds: toStr(row.event_time_microseconds || row.event_time),
    query_id: toStr(row.query_id),
    level: toStr(row.level),
    message: toStr(row.message),
    source: toStr(row.source),
    thread_id: toInt(row.thread_id),
    thread_name: toStr(row.thread_name),
  };
}
