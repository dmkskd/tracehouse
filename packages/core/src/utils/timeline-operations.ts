/**
 * Unified operation rows for the Time Travel operations table.
 *
 * Queries, merges and mutations are three shapes of the same thing: an
 * operation that ran on a host over an interval and consumed resources.
 * This module flattens them into one row type, applies the table's scope
 * (time window vs active-at-pin) and sorts by the selected resource metric,
 * so the UI layer only renders.
 */
import type { QuerySeries, MergeSeries, MutationSeries } from '../types/timeline.js';

export type OperationKind = 'query' | 'merge' | 'mutation';

/** Kind filter of the table, including the "everything" option. */
export type OperationKindFilter = OperationKind | 'all';

/** Which operations the table lists. */
export type OperationScope = 'window' | 'pin';

export type OperationSortField = 'metric' | 'duration' | 'started' | 'kind' | 'user' | 'server';
export type OperationSortDir = 'asc' | 'desc';

/** Resource dimension driving the metric column and the default sort. */
export type OperationMetricMode = 'memory' | 'cpu' | 'network' | 'disk';

export interface OperationRow {
  kind: OperationKind;
  /** Index into the source array of its own kind — maps a row back to its chart band. */
  idx: number;
  /** Stable id: query_id for queries, part_name for merges and mutations. */
  id: string;
  /** Badge text: the query kind (SELECT, INSERT, …) for queries, MERGE / MUTATION otherwise. */
  kindLabel: string;
  /** Primary label: the query label for queries, the table for merges and mutations. */
  label: string;
  user?: string;
  hostname?: string;
  startMs: number;
  endMs: number;
  durationMs: number;
  /** Value of the selected metric mode, used for the metric column and sorting. */
  metricValue: number;
  cpuUs: number;
  peakMemory: number;
  diskBytes: number;
  /** Read and write kept apart: a merge's in/out sizes are the interesting part. */
  diskReadBytes: number;
  diskWriteBytes: number;
  netBytes: number;
  /** Merges and mutations in flight: completion 0..1. */
  progress?: number;
  isRunning: boolean;
  /**
   * True when metricValue is an estimate rather than a measurement. A running
   * merge or mutation has no CPU counters, so the timeline derives cpu_us from
   * elapsed time; any view showing that number has to say so.
   */
  metricEstimated: boolean;
  /** Queries only: matches the active normalized-hash filter. */
  matchedHash: boolean;
  /** Queries only: finished with an exception. */
  failed: boolean;
  /** Merges only: the merge reason, for the category badge. */
  mergeReason?: string;
  /** The original series, for selection handlers and detail panels. */
  source: QuerySeries | MergeSeries | MutationSeries;
}

export interface OperationRowInput {
  queries: readonly QuerySeries[];
  merges: readonly MergeSeries[];
  mutations: readonly MutationSeries[];
}

export interface OperationRowOptions {
  metricMode: OperationMetricMode;
  sortField: OperationSortField;
  sortDir: OperationSortDir;
  /** 'pin' lists only operations active at pinnedMs; falls back to 'window' when nothing is pinned. */
  scope: OperationScope;
  pinnedMs: number | null;
  /** Zoom selection [startMs, endMs] restricting the 'window' scope. */
  zoomRange: readonly [number, number] | null;
  /** A normalized-hash filter is active: matching queries float to the top. */
  hashFilterActive?: boolean;
  /** Hash filter is exclusive: drop everything that does not match. */
  hashOnly?: boolean;
  /** Free-text filter over id, label, user, host and kind. */
  search?: string;
}

export interface OperationCounts {
  all: number;
  query: number;
  merge: number;
  mutation: number;
}

/** Parse a ClickHouse datetime string as UTC when it carries no zone. */
export function parseOperationTimestamp(value: string): number {
  const normalized = value.replace(' ', 'T')
    + (value.includes('+') || value.includes('Z') || value.includes('T') ? '' : 'Z');
  return new Date(normalized).getTime();
}

/** Raw value of one resource dimension for an operation. */
export function getOperationMetric(
  item: QuerySeries | MergeSeries | MutationSeries,
  mode: OperationMetricMode,
): number {
  if (mode === 'memory') return item.peak_memory;
  if (mode === 'cpu') return item.cpu_us;
  if (mode === 'network') return item.net_send + item.net_recv;
  return item.disk_read + item.disk_write;
}

function queryKindLabel(q: QuerySeries): string {
  const kind = (q.query_kind ?? '').trim();
  return kind.length > 0 ? kind.toUpperCase() : 'QUERY';
}

function toRow(
  kind: OperationKind,
  idx: number,
  item: QuerySeries | MergeSeries | MutationSeries,
  mode: OperationMetricMode,
): OperationRow {
  const query = kind === 'query' ? (item as QuerySeries) : undefined;
  const part = kind === 'query' ? undefined : (item as MergeSeries | MutationSeries);
  const merge = kind === 'merge' ? (item as MergeSeries) : undefined;
  return {
    kind,
    idx,
    id: query ? query.query_id : part!.part_name,
    kindLabel: query ? queryKindLabel(query) : kind === 'merge' ? 'MERGE' : 'MUTATION',
    label: query ? query.label : part!.table,
    user: query?.user,
    hostname: item.hostname,
    startMs: parseOperationTimestamp(item.start_time),
    endMs: parseOperationTimestamp(item.end_time),
    durationMs: item.duration_ms,
    metricValue: getOperationMetric(item, mode),
    cpuUs: item.cpu_us,
    peakMemory: item.peak_memory,
    diskBytes: item.disk_read + item.disk_write,
    diskReadBytes: item.disk_read,
    diskWriteBytes: item.disk_write,
    netBytes: item.net_send + item.net_recv,
    progress: part?.progress,
    isRunning: item.is_running === true,
    metricEstimated: mode === 'cpu' && kind !== 'query' && item.is_running === true,
    matchedHash: query?.matched_hash === true,
    failed: query !== undefined && (query.exception_code ?? 0) !== 0,
    mergeReason: merge?.merge_reason,
    source: item,
  };
}

function inScope(row: OperationRow, options: OperationRowOptions): boolean {
  const { scope, pinnedMs, zoomRange } = options;
  if (scope === 'pin' && pinnedMs !== null) {
    return pinnedMs >= row.startMs && pinnedMs <= row.endMs;
  }
  if (zoomRange) {
    return row.startMs <= zoomRange[1] && row.endMs >= zoomRange[0];
  }
  return true;
}

/** The text a string-sorted column compares on. */
function sortText(row: OperationRow, field: OperationSortField): string {
  if (field === 'kind') return row.kindLabel;
  if (field === 'user') return row.user ?? '';
  return row.hostname ?? '';
}

function compareRows(a: OperationRow, b: OperationRow, options: OperationRowOptions): number {
  const { sortField, sortDir, hashFilterActive, hashOnly } = options;
  if (hashFilterActive && !hashOnly && a.matchedHash !== b.matchedHash) {
    return a.matchedHash ? -1 : 1;
  }
  if (sortField === 'kind' || sortField === 'user' || sortField === 'server') {
    const cmp = sortText(a, sortField).localeCompare(sortText(b, sortField));
    // Equal text keeps the heaviest operation on top, so groups stay readable.
    if (cmp !== 0) return sortDir === 'desc' ? -cmp : cmp;
    return b.metricValue - a.metricValue;
  }
  let aVal: number, bVal: number;
  if (sortField === 'metric') { aVal = a.metricValue; bVal = b.metricValue; }
  else if (sortField === 'duration') { aVal = a.durationMs; bVal = b.durationMs; }
  else { aVal = a.startMs; bVal = b.startMs; }
  return sortDir === 'desc' ? bVal - aVal : aVal - bVal;
}

/**
 * Free-text match over the fields the table shows: id, label (query text or
 * table), user, host and kind. Case-insensitive; every whitespace-separated
 * term must match somewhere, so "merge events" narrows rather than widens.
 */
export function matchesOperationSearch(row: OperationRow, search: string): boolean {
  const terms = search.toLowerCase().split(/\s+/).filter(Boolean);
  if (terms.length === 0) return true;
  const haystack = [row.id, row.label, row.user ?? '', row.hostname ?? '', row.kindLabel]
    .join(' ')
    .toLowerCase();
  return terms.every(term => haystack.includes(term));
}

/**
 * Flatten queries, merges and mutations into scoped, sorted operation rows.
 * The kind filter is applied separately so counts stay comparable across tabs.
 */
export function buildOperationRows(
  input: OperationRowInput,
  options: OperationRowOptions,
): OperationRow[] {
  const rows: OperationRow[] = [];
  input.queries.forEach((q, idx) => rows.push(toRow('query', idx, q, options.metricMode)));
  if (!(options.hashOnly && options.hashFilterActive)) {
    input.merges.forEach((m, idx) => rows.push(toRow('merge', idx, m, options.metricMode)));
    input.mutations.forEach((m, idx) => rows.push(toRow('mutation', idx, m, options.metricMode)));
  }
  const search = options.search?.trim() ?? '';
  const scoped = rows.filter(row => inScope(row, options)
    && !(options.hashOnly && options.hashFilterActive && row.kind === 'query' && !row.matchedHash)
    && (search === '' || matchesOperationSearch(row, search)));
  return scoped.sort((a, b) => compareRows(a, b, options));
}

/** Row counts per kind, taken before the kind filter so tab counts share one scope. */
export function countOperationRows(rows: readonly OperationRow[]): OperationCounts {
  const counts: OperationCounts = { all: rows.length, query: 0, merge: 0, mutation: 0 };
  for (const row of rows) counts[row.kind] += 1;
  return counts;
}

export function filterOperationRowsByKind(
  rows: readonly OperationRow[],
  kind: OperationKindFilter,
): OperationRow[] {
  return kind === 'all' ? [...rows] : rows.filter(row => row.kind === kind);
}

export interface OperationTotalsInput {
  /** Operations matching the window server-side, before the top-N activity limit. */
  queryTotal: number;
  mergeTotal: number;
  mutationTotal: number;
  /** Operations actually loaded into the timeline (the top-N cap). */
  queryLoaded: number;
  mergeLoaded: number;
  mutationLoaded: number;
}

/** Total and loaded operation counts for the active kind filter. */
export function operationTotals(
  input: OperationTotalsInput,
  kind: OperationKindFilter,
): { total: number; loaded: number } {
  if (kind === 'query') return { total: input.queryTotal, loaded: input.queryLoaded };
  if (kind === 'merge') return { total: input.mergeTotal, loaded: input.mergeLoaded };
  if (kind === 'mutation') return { total: input.mutationTotal, loaded: input.mutationLoaded };
  return {
    total: input.queryTotal + input.mergeTotal + input.mutationTotal,
    loaded: input.queryLoaded + input.mergeLoaded + input.mutationLoaded,
  };
}

export interface OperationHostSummary {
  hostname: string;
  count: number;
  /** Sum of the active metric across that host's operations. */
  metricTotal: number;
}

export interface OperationSummary {
  counts: OperationCounts;
  /** Sum of the active metric across every row in scope. */
  metricTotal: number;
  runningCount: number;
  hosts: OperationHostSummary[];
}

/**
 * Aggregate the operations in scope, for the inspector's no-selection state.
 * Hosts are ranked by metric total, so the panel leads with where the load sits.
 */
export function summarizeOperations(
  rows: readonly OperationRow[],
  hostLimit = 4,
): OperationSummary {
  const byHost = new Map<string, OperationHostSummary>();
  let metricTotal = 0;
  let runningCount = 0;
  for (const row of rows) {
    metricTotal += row.metricValue;
    if (row.isRunning) runningCount += 1;
    const hostname = row.hostname ?? '';
    if (hostname === '') continue;
    const entry = byHost.get(hostname) ?? { hostname, count: 0, metricTotal: 0 };
    entry.count += 1;
    entry.metricTotal += row.metricValue;
    byHost.set(hostname, entry);
  }
  const hosts = [...byHost.values()]
    .sort((a, b) => b.metricTotal - a.metricTotal || a.hostname.localeCompare(b.hostname))
    .slice(0, hostLimit);
  return { counts: countOperationRows(rows), metricTotal, runningCount, hosts };
}

/** Identifies one operation across data refreshes, so a selection survives a reload. */
export interface OperationSelectionKey {
  kind: OperationKind;
  id: string;
  hostname?: string;
}

/**
 * Key shared by the chart bands, the table rows and the highlight state.
 * Queries are their query_id; parts need table, part and host together,
 * because the same part name exists on every replica.
 */
export function operationHighlightKey(
  kind: OperationKind,
  item: QuerySeries | MergeSeries | MutationSeries,
): string {
  if (kind === 'query') return (item as QuerySeries).query_id;
  const part = item as MergeSeries | MutationSeries;
  return `${part.table}:${part.part_name}:${part.hostname ?? ''}`;
}

/** The same key, derived from a row the table already holds. */
export function operationRowHighlightKey(row: OperationRow): string {
  return row.kind === 'query' ? row.id : `${row.label}:${row.id}:${row.hostname ?? ''}`;
}

/** Selection key for a series, used when a chart band is clicked. */
export function operationKeyFor(
  kind: OperationKind,
  item: QuerySeries | MergeSeries | MutationSeries,
): OperationSelectionKey {
  return {
    kind,
    id: kind === 'query' ? (item as QuerySeries).query_id : (item as MergeSeries | MutationSeries).part_name,
    hostname: item.hostname,
  };
}

/**
 * Resolve a selection key against the current timeline data.
 * Returns null once the operation leaves the loaded window, so the inspector
 * drops a stale selection instead of showing data that is no longer on screen.
 */
export function findOperationRow(
  input: OperationRowInput,
  key: OperationSelectionKey | null,
  mode: OperationMetricMode,
): OperationRow | null {
  if (!key) return null;
  const items: readonly (QuerySeries | MergeSeries | MutationSeries)[] =
    key.kind === 'query' ? input.queries : key.kind === 'merge' ? input.merges : input.mutations;
  for (let idx = 0; idx < items.length; idx++) {
    const item = items[idx];
    const candidate = operationKeyFor(key.kind, item);
    if (candidate.id !== key.id) continue;
    if (key.hostname !== undefined && candidate.hostname !== key.hostname) continue;
    return toRow(key.kind, idx, item, mode);
  }
  return null;
}


/**
 * What the table's one flexible column shows for a row. Queries and merges
 * carry different facts, and a column that means "user" only for queries wastes
 * its width on every merge.
 */
export type OperationContextCell =
  | { type: 'user'; user: string }
  | { type: 'part'; reason?: string; readBytes: number; writtenBytes: number; progress?: number }
  | { type: 'none' };

export function describeOperationContext(row: OperationRow): OperationContextCell {
  if (row.kind === 'query') {
    return row.user ? { type: 'user', user: row.user } : { type: 'none' };
  }
  return {
    type: 'part',
    reason: row.kind === 'merge' ? row.mergeReason : undefined,
    readBytes: row.diskReadBytes,
    writtenBytes: row.diskWriteBytes,
    progress: row.isRunning ? row.progress : undefined,
  };
}

/**
 * Largest metric value among the rows, for scaling the in-cell bars. Returns 0
 * for an empty list so callers can skip drawing rather than divide by zero.
 */
export function maxOperationMetric(rows: readonly OperationRow[]): number {
  let max = 0;
  for (const row of rows) if (row.metricValue > max) max = row.metricValue;
  return max;
}
