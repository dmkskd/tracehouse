import type { MergeHistoryRecord, MergeInfo } from '../../stores/mergeStore';
import type { MergeHistorySort } from '../../stores/mergeStore';
import {
  classifyActiveMerge,
  classifyMergeHistory,
  type MergeCategory,
} from '@tracehouse/core';

export const MERGE_FINALIZING_GRACE_MS = 10_000;
const MERGE_WARN_SECONDS = 10 * 60;
const MERGE_DANGER_SECONDS = 30 * 60;

export type MergeLiveStatus = 'running' | 'finalizing';

export interface MergeLiveActivity {
  merge: MergeInfo;
  status: MergeLiveStatus;
}

interface PendingMerge {
  merge: MergeInfo;
  missingSince: number;
}

export interface MergeActivityState {
  observedRunning: Map<string, MergeInfo>;
  pending: Map<string, PendingMerge>;
}

export interface MergeActivitySnapshot {
  live: MergeLiveActivity[];
  recent: MergeHistoryRecord[];
}

export type MergeActivitySource = MergeLiveStatus | 'history';
export type MergeActivityStatus = MergeLiveStatus | 'ok' | 'error';

/**
 * Common row shape for the unified merge activity table. Differences between
 * system.merges and system.part_log are resolved here rather than in TSX.
 */
export interface MergeActivityRecord {
  activityKey: string;
  activitySource: MergeActivitySource;
  status: MergeActivityStatus;
  database: string;
  table: string;
  hostname?: string;
  partName: string;
  partLabel: string;
  category: MergeCategory;
  isReplicaMerge: boolean;
  startedAt: string;
  startedAtMs: number;
  durationMs: number;
  rowsRead: number;
  rowsWritten: number;
  sizeBytes: number;
  memoryBytes: number;
  throughputBytesPerSec: number;
  progress: number | null;
  error?: number;
  exception?: string;
  isStuck: boolean;
  liveMerge?: MergeInfo;
  historyRecord?: MergeHistoryRecord;
}

export interface MergeActivityFilters {
  hideReplicaMerges?: boolean;
  excludeSystemDatabases?: boolean;
  database?: string[];
  table?: string[];
  /** Legacy active-only URL filter retained for compatible deep links. */
  liveCategory?: string;
  category?: string[];
  minDurationMs?: number;
  minSizeBytes?: number;
  status?: string[];
  hostname?: string[];
  partName?: string;
}

function includesValue(filters: string[] | undefined, value: string | undefined): boolean {
  if (!filters?.length) return true;
  const normalizedValue = (value ?? '').toLowerCase();
  return filters.some(filter => filter.toLowerCase() === normalizedValue);
}

export function createMergeActivityState(): MergeActivityState {
  return {
    observedRunning: new Map(),
    pending: new Map(),
  };
}

export function mergeActivityKey(
  merge: Pick<MergeInfo, 'database' | 'table' | 'result_part_name' | 'hostname'>,
): string {
  return `${merge.hostname ?? ''}\u0000${merge.database}\u0000${merge.table}\u0000${merge.result_part_name}`;
}

/**
 * Flag merges whose elapsed time and observed progress imply that completion
 * is unusually far away, including work stuck in ClickHouse's finalization
 * phase at effectively 100% progress.
 */
export function isMergeStuck(merge: MergeInfo): boolean {
  if (merge.elapsed < MERGE_WARN_SECONDS) return false;
  if (merge.progress >= 0.9995) return merge.elapsed > MERGE_DANGER_SECONDS;
  if (merge.progress <= 0.001) return true;
  const estimatedTotal = merge.elapsed / merge.progress;
  return estimatedTotal - merge.elapsed > MERGE_DANGER_SECONDS;
}

function historyActivityKey(
  merge: Pick<MergeHistoryRecord, 'database' | 'table' | 'part_name' | 'hostname'>,
): string {
  return `${merge.hostname ?? ''}\u0000${merge.database}\u0000${merge.table}\u0000${merge.part_name}`;
}

function liveRecord(item: MergeLiveActivity, now: number): MergeActivityRecord {
  const { merge } = item;
  const durationMs = Math.max(0, merge.elapsed * 1000);
  const startedAtMs = Math.max(0, now - durationMs);
  const bytesProcessed = merge.total_size_bytes_compressed * merge.progress;

  return {
    activityKey: mergeActivityKey(merge),
    activitySource: item.status,
    status: item.status,
    database: merge.database,
    table: merge.table,
    hostname: merge.hostname,
    partName: merge.result_part_name,
    partLabel: `${merge.num_parts} → ${merge.result_part_name}`,
    category: classifyActiveMerge(merge.merge_type, merge.is_mutation, merge.result_part_name),
    isReplicaMerge: merge.is_replica_merge ?? false,
    startedAt: new Date(startedAtMs).toISOString(),
    startedAtMs,
    durationMs,
    rowsRead: merge.rows_read,
    rowsWritten: merge.rows_written,
    sizeBytes: merge.total_size_bytes_compressed,
    memoryBytes: merge.memory_usage || 0,
    throughputBytesPerSec: merge.elapsed > 0 ? bytesProcessed / merge.elapsed : 0,
    progress: merge.progress,
    isStuck: isMergeStuck(merge),
    liveMerge: merge,
  };
}

function completedRecord(record: MergeHistoryRecord): MergeActivityRecord {
  const completedAtMs = Date.parse(record.event_time);
  const safeCompletedAtMs = Number.isFinite(completedAtMs) ? completedAtMs : 0;
  const durationMs = Math.max(0, record.duration_ms);
  const startedAtMs = Math.max(0, safeCompletedAtMs - durationMs);
  const sourceCount = record.source_part_names?.length ?? 0;

  return {
    activityKey: `${historyActivityKey(record)}\u0000${record.event_time}\u0000${record.event_type}`,
    activitySource: 'history',
    status: record.error ? 'error' : 'ok',
    database: record.database,
    table: record.table,
    hostname: record.hostname,
    partName: record.part_name,
    partLabel: sourceCount > 0 ? `${sourceCount} → ${record.part_name}` : record.part_name,
    category: classifyMergeHistory(record.event_type, record.merge_reason, record.part_name),
    isReplicaMerge: record.is_replica_merge ?? false,
    startedAt: new Date(startedAtMs).toISOString(),
    startedAtMs,
    durationMs,
    rowsRead: record.read_rows,
    rowsWritten: record.rows,
    sizeBytes: record.size_in_bytes,
    memoryBytes: record.peak_memory_usage || 0,
    throughputBytesPerSec: durationMs > 0 ? record.size_in_bytes / (durationMs / 1000) : 0,
    progress: record.error ? null : 1,
    error: record.error,
    exception: record.exception,
    isStuck: false,
    historyRecord: record,
  };
}

/**
 * Combine live and terminal merges into the activity list users perceive.
 * Live work stays visible even when it began before the selected history range.
 */
export function buildMergeActivityRecords(
  live: MergeLiveActivity[],
  history: MergeHistoryRecord[],
  now = Date.now(),
): MergeActivityRecord[] {
  return [
    ...live.map(item => liveRecord(item, now)),
    ...history.map(completedRecord),
  ];
}

/** Apply the table's single display cap after both ClickHouse sources are combined. */
export function limitMergeActivityRecords(
  activity: MergeActivityRecord[],
  limit?: number,
): MergeActivityRecord[] {
  return limit != null && limit > 0 ? activity.slice(0, limit) : activity;
}

const SYSTEM_DATABASES = new Set(['system', 'information_schema', 'INFORMATION_SCHEMA']);

function matchesCommonFilter(
  value: {
    database: string;
    table: string;
    hostname?: string;
    resultPartName: string;
    sourcePartNames?: string[];
    isReplicaMerge?: boolean;
    durationMs: number;
    sizeBytes: number;
  },
  filters: MergeActivityFilters,
): boolean {
  if (filters.hideReplicaMerges && value.isReplicaMerge) return false;
  if (filters.excludeSystemDatabases && SYSTEM_DATABASES.has(value.database)) return false;
  if (!includesValue(filters.database, value.database)) return false;
  if (!includesValue(filters.table, value.table)) return false;
  if (filters.minDurationMs != null && value.durationMs < filters.minDurationMs) return false;
  if (filters.minSizeBytes != null && value.sizeBytes < filters.minSizeBytes) return false;
  if (!includesValue(filters.hostname, value.hostname)) return false;
  if (filters.partName) {
    const query = filters.partName.toLowerCase();
    const matchesPart = value.resultPartName.toLowerCase().includes(query)
      || value.sourcePartNames?.some(part => part.toLowerCase().includes(query));
    if (!matchesPart) return false;
  }
  return true;
}

/**
 * Apply one filter contract to both ClickHouse sources before they are
 * normalized for presentation.
 */
export function filterMergeActivity(
  snapshot: MergeActivitySnapshot,
  filters: MergeActivityFilters,
): MergeActivitySnapshot {
  const requestedStatuses = new Set(filters.status?.map(status => status.toLowerCase()) ?? []);
  const live = snapshot.live.filter(({ merge }) => {
    if (requestedStatuses.size > 0 && !requestedStatuses.has('running')) return false;
    if (!matchesCommonFilter({
      database: merge.database,
      table: merge.table,
      hostname: merge.hostname,
      resultPartName: merge.result_part_name,
      sourcePartNames: merge.source_part_names,
      isReplicaMerge: merge.is_replica_merge,
      durationMs: merge.elapsed * 1000,
      sizeBytes: merge.total_size_bytes_compressed,
    }, filters)) return false;
    const category = classifyActiveMerge(merge.merge_type, merge.is_mutation, merge.result_part_name);
    if (filters.liveCategory && category !== filters.liveCategory) return false;
    if (!includesValue(filters.category, category)) return false;
    return true;
  });

  const recent = snapshot.recent.filter(record => {
    if (requestedStatuses.size > 0) {
      const status = record.error ? 'error' : 'ok';
      if (!requestedStatuses.has(status)) return false;
    }
    if (!matchesCommonFilter({
      database: record.database,
      table: record.table,
      hostname: record.hostname,
      resultPartName: record.part_name,
      sourcePartNames: record.source_part_names,
      isReplicaMerge: record.is_replica_merge,
      durationMs: record.duration_ms,
      sizeBytes: record.size_in_bytes,
    }, filters)) return false;
    const category = classifyMergeHistory(record.event_type, record.merge_reason, record.part_name);
    if (!includesValue(filters.category, category)) return false;
    return true;
  });

  return { live, recent };
}

export function mergeActivityHosts(snapshot: MergeActivitySnapshot): string[] {
  const hosts = new Set<string>();
  snapshot.live.forEach(({ merge }) => {
    if (merge.hostname) hosts.add(merge.hostname);
  });
  snapshot.recent.forEach(record => {
    if (record.hostname) hosts.add(record.hostname);
  });
  return Array.from(hosts).sort();
}

export function mergeActivityStatuses(history: MergeHistoryRecord[]): string[] {
  const statuses = new Set<string>(['Running']);
  history.forEach(record => statuses.add(record.error ? 'Error' : 'OK'));
  return ['Running', 'OK', 'Error'].filter(status => statuses.has(status));
}

export function hasReplicaMergeActivity(snapshot: MergeActivitySnapshot): boolean {
  return snapshot.live.some(({ merge }) => merge.is_replica_merge)
    || snapshot.recent.some(record => record.is_replica_merge);
}

export function isMergeActivityRecordSelected(
  record: MergeActivityRecord,
  selectedLiveMerge?: MergeInfo | null,
  selectedHistoryRecord?: MergeHistoryRecord | null,
): boolean {
  if (record.liveMerge && selectedLiveMerge) {
    return record.database === selectedLiveMerge.database
      && record.table === selectedLiveMerge.table
      && record.partName === selectedLiveMerge.result_part_name
      && (record.hostname || '') === (selectedLiveMerge.hostname || '');
  }
  if (record.historyRecord && selectedHistoryRecord) {
    return record.historyRecord.event_time === selectedHistoryRecord.event_time
      && record.partName === selectedHistoryRecord.part_name
      && (record.hostname || '') === (selectedHistoryRecord.hostname || '');
  }
  return false;
}

function sortValue(record: MergeActivityRecord, field: MergeHistorySort['field']): number {
  switch (field) {
    case 'event_time': return record.startedAtMs;
    case 'duration_ms': return record.durationMs;
    case 'rows': return record.rowsWritten;
    case 'size_in_bytes': return record.sizeBytes;
    case 'throughput': return record.throughputBytesPerSec;
  }
}

/**
 * Active work is always pinned above terminal history. Within each lifecycle
 * group, the selected column has the same meaning and direction.
 */
export function sortMergeActivityRecords(
  records: MergeActivityRecord[],
  sort: MergeHistorySort,
): MergeActivityRecord[] {
  const direction = sort.direction === 'asc' ? 1 : -1;
  const compare = (a: MergeActivityRecord, b: MergeActivityRecord) => {
    const delta = sortValue(a, sort.field) - sortValue(b, sort.field);
    if (delta !== 0) return delta * direction;
    return a.activityKey.localeCompare(b.activityKey);
  };
  const live = records.filter(record => record.activitySource !== 'history').sort(compare);
  const history = records.filter(record => record.activitySource === 'history').sort(compare);
  return [...live, ...history];
}

/**
 * Preserve a merge while system.part_log catches up with system.merges.
 */
export function reconcileMergeActivity(
  state: MergeActivityState,
  running: MergeInfo[],
  history: MergeHistoryRecord[],
  now = Date.now(),
  graceMs = MERGE_FINALIZING_GRACE_MS,
): MergeActivitySnapshot {
  const historyKeys = new Set(history.map(historyActivityKey));
  const current = new Map(running.map(merge => [mergeActivityKey(merge), merge]));

  for (const [key, previous] of state.observedRunning) {
    if (!current.has(key) && !historyKeys.has(key) && !state.pending.has(key)) {
      state.pending.set(key, { merge: previous, missingSince: now });
    }
  }

  state.observedRunning = current;

  for (const key of current.keys()) state.pending.delete(key);
  for (const key of historyKeys) state.pending.delete(key);
  for (const [key, pending] of state.pending) {
    if (now - pending.missingSince > graceMs) state.pending.delete(key);
  }

  const live: MergeLiveActivity[] = [
    ...running.map(merge => ({ merge, status: 'running' as const })),
    ...Array.from(state.pending.values(), pending => ({
      merge: pending.merge,
      status: 'finalizing' as const,
    })),
  ].sort((a, b) => b.merge.elapsed - a.merge.elapsed);

  return { live, recent: history };
}
