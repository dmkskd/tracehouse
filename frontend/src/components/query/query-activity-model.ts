import {
  sortQueryHistory,
  type QueryHistoryFilter,
  type QueryHistoryItem,
  type QueryHistorySort,
  type RunningQuery,
} from '../../stores/queryStore';
import { APP_SOURCE_PREFIX, type QuerySeries } from '@tracehouse/core';

export interface QueryActivitySnapshot {
  live: RunningQuery[];
  recent: QueryHistoryItem[];
}

export type QueryActivitySource = 'running' | 'history';

/**
 * Common row shape consumed by the activity table. Live-only derivation stays
 * here so the TSX component is concerned only with presentation and actions.
 */
export interface QueryActivityRecord extends QueryHistoryItem {
  activityKey: string;
  activitySource: QueryActivitySource;
  liveQuery?: RunningQuery;
}

export function queryActivityKey(query: Pick<RunningQuery, 'query_id' | 'hostname'>): string {
  return `${query.hostname ?? ''}\u0000${query.query_id}`;
}

function historyActivityKey(query: Pick<QueryHistoryItem, 'query_id' | 'hostname'>): string {
  return `${query.hostname ?? ''}\u0000${query.query_id}`;
}

function runningStartTime(query: RunningQuery, now: number): number {
  return now - query.elapsed_seconds * 1000;
}

function historyRecord(query: QueryHistoryItem): QueryActivityRecord {
  return {
    ...query,
    activityKey: historyActivityKey(query),
    activitySource: 'history',
  };
}

function liveRecord(query: RunningQuery, now: number): QueryActivityRecord {
  return {
    query_id: query.query_id,
    query_type: query.query_kind,
    query_kind: query.query_kind,
    query_start_time: new Date(runningStartTime(query, now)).toISOString(),
    query_duration_ms: Math.max(0, query.elapsed_seconds * 1000),
    read_rows: query.read_rows,
    read_bytes: query.read_bytes,
    result_rows: 0,
    result_bytes: 0,
    memory_usage: query.memory_usage,
    query: query.query,
    exception: null,
    user: query.user,
    client_hostname: '',
    type: 'running',
    efficiency_score: null,
    is_initial_query: query.is_initial_query,
    initial_query_id: query.initial_query_id,
    hostname: query.hostname,
    activityKey: queryActivityKey(query),
    activitySource: 'running',
    liveQuery: query,
  };
}

function matchesLiveFilter(record: QueryActivityRecord, filter: QueryHistoryFilter): boolean {
  const query = record.liveQuery;
  if (!query) return true;
  if (filter.queryId && !query.query_id.toLowerCase().includes(filter.queryId.toLowerCase())) return false;
  if (filter.user && !query.user.toLowerCase().includes(filter.user.toLowerCase())) return false;
  if (filter.hostname && !(query.hostname ?? '').toLowerCase().includes(filter.hostname.toLowerCase())) return false;
  if (filter.queryText && !query.query.toLowerCase().includes(filter.queryText.toLowerCase())) return false;
  if (filter.minDurationMs != null && record.query_duration_ms < filter.minDurationMs) return false;
  if (filter.minMemoryBytes != null && query.memory_usage < filter.minMemoryBytes) return false;
  if (filter.queryKind && query.query_kind.toLowerCase() !== filter.queryKind.toLowerCase()) return false;
  if (filter.excludeAppQueries && query.query.includes(APP_SOURCE_PREFIX)) return false;
  if (filter.status && filter.status.toLowerCase() !== 'running') return false;
  // system.processes does not expose resolved databases/tables.
  if (filter.database || filter.table) return false;
  return true;
}

/**
 * Merge live and terminal records into the one list users perceive as query
 * activity. Live rows are intentionally not constrained by the historical
 * time range: an active query must remain visible even when it began before
 * the selected history window.
 */
export function buildQueryActivityRecords(
  snapshot: QueryActivitySnapshot,
  filter: QueryHistoryFilter,
  now = Date.now(),
): QueryActivityRecord[] {
  const live = snapshot.live
    .map(query => liveRecord(query, now))
    .filter(record => matchesLiveFilter(record, filter));
  const recent = filter.status?.toLowerCase() === 'running'
    ? []
    : snapshot.recent.map(historyRecord);
  const activity = [...live, ...recent].filter(record =>
    !filter.hostname
    || (record.hostname ?? '').toLowerCase().includes(filter.hostname.toLowerCase())
  );
  return filter.limit != null && filter.limit > 0
    ? activity.slice(0, filter.limit)
    : activity;
}

/**
 * Convert an explicitly typed store selection for the query detail timeline.
 * The caller-provided source is authoritative; no field-shape inference is
 * used to decide whether a query is running.
 */
export function querySelectionToSeries(
  query: RunningQuery | QueryHistoryItem | null,
  source: 'running' | 'history' | null,
  now = Date.now(),
): QuerySeries | null {
  if (!query || !source) return null;

  if (source === 'running') {
    const live = query as RunningQuery;
    const durationMs = Math.max(0, Math.round(live.elapsed_seconds * 1000));
    return {
      query_id: live.query_id,
      user: live.user || 'default',
      label: live.query || '',
      start_time: new Date(now - durationMs).toISOString(),
      end_time: new Date(now).toISOString(),
      duration_ms: durationMs,
      peak_memory: live.memory_usage || 0,
      cpu_us: 0,
      net_send: 0,
      net_recv: 0,
      disk_read: live.read_bytes || 0,
      disk_write: 0,
      status: 'Running',
      query_kind: live.query_kind,
      is_running: true,
      points: [],
    };
  }

  const history = query as QueryHistoryItem;
  const durationMs = Math.max(0, history.query_duration_ms || 0);
  const startMs = Date.parse(history.query_start_time);
  const safeStartMs = Number.isFinite(startMs) ? startMs : 0;
  return {
    query_id: history.query_id,
    user: history.user || 'default',
    label: history.query || '',
    start_time: new Date(safeStartMs).toISOString(),
    end_time: new Date(safeStartMs + durationMs).toISOString(),
    duration_ms: durationMs,
    peak_memory: history.memory_usage || 0,
    cpu_us: history.cpu_time_us || 0,
    net_send: history.network_send_bytes || 0,
    net_recv: history.network_receive_bytes || 0,
    disk_read: history.disk_read_bytes || history.read_bytes || 0,
    disk_write: history.disk_write_bytes || 0,
    status: history.type,
    exception: history.exception ?? undefined,
    query_kind: history.query_kind,
    is_running: false,
    points: [],
  };
}

/**
 * Live work stays visible above terminal history regardless of the history
 * sort. For the default start-time sort, order live rows by elapsed time so
 * the oldest/longest-running query is the first row.
 */
export function sortQueryActivityRecords(
  records: QueryActivityRecord[],
  sort: QueryHistorySort,
): QueryActivityRecord[] {
  const live = records.filter(record => record.activitySource !== 'history');
  const history = records.filter(record => record.activitySource === 'history');
  const sortedLive = sort.field === 'query_start_time'
    ? [...live].sort((a, b) => b.query_duration_ms - a.query_duration_ms)
    : sortQueryHistory(live, sort) as QueryActivityRecord[];
  const sortedHistory = sortQueryHistory(history, sort) as QueryActivityRecord[];
  return [...sortedLive, ...sortedHistory];
}
