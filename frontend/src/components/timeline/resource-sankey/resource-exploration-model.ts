import type { MemoryTimeline, MergeSeries, MutationSeries, QuerySeries, TimeseriesPoint, AttributionSelection, AttributionBasis, TimelineAttribution, ResourceAttributionGroup, ResourceAttributionSummary, ResourceAttributionWindow, ResourceAttributionGroupKey, AttributedOperation } from '@tracehouse/core';
import { attributeTimeline, measureTimelineSeries, selectServerSeries } from '@tracehouse/core';
import type { MetricMode } from '../timeline-constants';

type Kind = 'query' | 'merge' | 'mutation';
export interface ResourceConsumer {
  id: string;
  source: QuerySeries | MergeSeries | MutationSeries;
  label: string;
  host?: string;
  group: string;
  kind: Kind;
  tableKey: string;
  tableLabel: string;
  tables: string[];
  value: number;
  failureCode: number;
  basis: Exclude<AttributionBasis, 'unavailable'>;
  sampleMs?: number;
}

/** Presentation adapter only: all activity membership, sampling, and totals come from core. */
export function attributionConsumers(attribution: Pick<TimelineAttribution, 'operations' | 'mode'>) {
  const consumers: ResourceConsumer[] = attribution.operations.flatMap(row => {
    if (row.value === null || row.value <= 0 || row.basis === 'unavailable') return [];
    const query = row.kind === 'query' ? row.source as QuerySeries : undefined;
    const part = row.kind !== 'query' ? row.source as MergeSeries : undefined;
    const tables = row.tables;
    return [{
      id: row.id, source: row.source, kind: row.kind, host: row.source.hostname,
      group: row.activity, tables,
      tableKey: tables.length ? JSON.stringify(tables) : query?.is_running ? 'running-unreported' : query?.tables ? 'none-recorded' : 'metadata-unavailable',
      tableLabel: tables.length === 1 ? tables[0] : tables.length > 1 ? tables.join(' + ')
        : query?.is_running ? 'Not reported (running query)' : query?.tables ? 'No table recorded' : 'Table metadata unavailable',
      label: query ? `${query.query_id.slice(0, 12)} · ${query.label}` : `${part!.table} · ${part!.part_name}`,
      failureCode: row.failureCode, value: row.value, basis: row.basis, sampleMs: row.sampleMs,
    }];
  });
  return { consumers, missingMemory: attribution.mode === 'memory'
    ? attribution.operations.filter(row => row.basis === 'unavailable' || row.missingMs > 0).length : 0 };
}

export function resourceConsumers(data: MemoryTimeline, atMs: number, mode: MetricMode, host: string | readonly string[] = '', hidden?: ReadonlySet<Kind>) {
  return attributionConsumers(attributeTimeline(data, { selection: { kind: 'instant', atMs }, mode, hostname: host, hiddenKinds: hidden }));
}

export function explorationServerSeries(data: MemoryTimeline, mode: MetricMode, host: string | readonly string[]): TimeseriesPoint[] {
  return selectServerSeries(data, mode, host);
}

export function explorationMeasurement(series: TimeseriesPoint[], atMs: number) {
  const { value, sampleMs } = measureTimelineSeries(series, { kind: 'instant', atMs });
  return { value, sampleMs };
}

export interface ResourceTableGroup {
  id: string;
  activity: string;
  label: string;
  tables: string[];
  rows: ResourceConsumer[];
  value: number;
  remainder?: boolean;
  remainingTables?: number;
  remainingActivities?: number;
  visibleActivities?: number;
}

/** Presentation hierarchy; identities retain their meaning in either pivot order. */
export interface ResourceFlowGroup extends Omit<ResourceTableGroup, 'activity' | 'label'> {
  primaryId: string;
  primaryLabel: string;
  secondaryLabel: string;
  aggregates?: ResourceAttributionGroup[];
}

export const flowGroupCount = (group: ResourceFlowGroup) => group.aggregates?.reduce((sum, row) => sum + row.operationCount - row.unavailableCount, 0) ?? group.rows.length;
export const flowGroupDetailCount = (group: ResourceFlowGroup) => group.aggregates?.reduce((sum, row) => sum + row.operationCount - row.unavailableCount - row.zeroCount, 0) ?? group.rows.length;
export const flowGroupFailedCount = (group: ResourceFlowGroup) => group.aggregates?.reduce((sum, row) => sum + row.failedCount, 0) ?? group.rows.filter(row => row.failureCode).length;
export const flowGroupFailureValue = (group: ResourceFlowGroup) => group.aggregates?.reduce((sum, row) => sum + row.failureValue, 0) ?? group.rows.reduce((sum, row) => sum + (row.failureCode ? row.value : 0), 0);
export const flowGroupKind = (group: ResourceFlowGroup) => group.aggregates?.[0]?.kind ?? group.rows[0].kind;

export type ResourceTableLimits = number | Readonly<Record<string, number>>;
const tableDisplayLimit = (limits: ResourceTableLimits, primaryId: string) => typeof limits === 'number' ? limits : limits[primaryId] ?? 5;

export function summaryResourceFlow(summary: ResourceAttributionSummary, groupBy: 'activity' | 'table', tableLimit: ResourceTableLimits = 5, activityLimit?: number): ResourceFlowGroup[] {
  const groups = summary.groups.filter(group => group.value > 0).map(group => {
    const tableKey = group.tables.length ? JSON.stringify(group.tables) : group.tableState;
    const tableLabel = group.tables.length ? group.tables.join(' + ') : group.tableState === 'running' ? 'Not reported (running query)' : 'No table recorded';
    return { id: group.id, primaryId: groupBy === 'activity' ? group.activity : tableKey,
      primaryLabel: groupBy === 'activity' ? group.activity : tableLabel,
      secondaryLabel: groupBy === 'activity' ? tableLabel : group.activity,
      tables: group.tables, value: group.value, rows: [], aggregates: [group] } satisfies ResourceFlowGroup;
  });
  const totals = new Map<string, number>();
  for (const group of groups) totals.set(group.primaryId, (totals.get(group.primaryId) ?? 0) + group.value);
  const ranked = groups.sort((a,b) => (groupBy === 'table' ? totals.get(b.primaryId)! - totals.get(a.primaryId)! : 0)
    || a.primaryId.localeCompare(b.primaryId) || b.value-a.value || a.id.localeCompare(b.id));
  return limitResourceActivities(limitResourceTables(ranked, groupBy, tableLimit),groupBy,activityLimit);
}

/** Small activity types remain in the total without reserving a section each. */
function limitResourceActivities(groups: ResourceFlowGroup[], groupBy: 'activity' | 'table', limit?: number): ResourceFlowGroup[] {
  if (groupBy === 'table') return groups;
  const totals = new Map<string,number>();
  for (const group of groups) totals.set(group.primaryId,(totals.get(group.primaryId) ?? 0)+group.value);
  const total = groups.reduce((sum,group) => sum+group.value,0);
  const ranked = [...totals.keys()].sort((a,b) => totals.get(b)!-totals.get(a)! || a.localeCompare(b));
  const count = limit ?? Math.min(5,ranked.filter(key => totals.get(key)! >= total*.001).length);
  const shown = new Set(ranked.slice(0,count));
  const rest = groups.filter(group => !shown.has(group.primaryId));
  const result = ranked.slice(0,count).flatMap(key => groups.filter(group => group.primaryId === key));
  if (rest.length) {
    const remaining = new Set(rest.map(group => group.primaryId)).size;
    result.push(...rest.map(group => ({ ...group, primaryId: 'other-activities', primaryLabel: `${remaining} more activity types`,
      secondaryLabel: group.primaryLabel, remainingActivities: remaining, visibleActivities: shown.size })));
  }
  return result;
}

function limitResourceTables(ranked: ResourceFlowGroup[], groupBy: 'activity' | 'table', limits: ResourceTableLimits): ResourceFlowGroup[] {
  const combine = (rows: ResourceFlowGroup[], primaryId: string, primaryLabel: string, secondaryLabel: string): ResourceFlowGroup => ({
    id: JSON.stringify(['other-tables', primaryId, secondaryLabel]), primaryId, primaryLabel, secondaryLabel,
    tables: [...new Set(rows.flatMap(row => row.tables))], rows: rows.flatMap(row => row.rows),
    ...(rows.some(row => row.aggregates) ? { aggregates: rows.flatMap(row => row.aggregates ?? []) } : {}),
    value: rows.reduce((sum,row) => sum+row.value,0), remainder: true,
    remainingTables: groupBy === 'activity' ? rows.length : new Set(rows.map(row => row.primaryId)).size,
  });
  const result: ResourceFlowGroup[] = [];
  if (groupBy === 'activity') {
    for (const primaryId of new Set(ranked.map(row => row.primaryId))) {
      const rows = ranked.filter(row => row.primaryId === primaryId);
      const tableLimit = tableDisplayLimit(limits, primaryId);
      result.push(...rows.slice(0,tableLimit));
      if (rows.length > tableLimit) result.push(combine(rows.slice(tableLimit),primaryId,rows[0].primaryLabel,`Other tables (${rows.length-tableLimit})`));
    }
  } else {
    const tableLimit = tableDisplayLimit(limits, '*');
    const shown = new Set([...new Set(ranked.map(row => row.primaryId))].slice(0,tableLimit));
    result.push(...ranked.filter(row => shown.has(row.primaryId)));
    const rest = ranked.filter(row => !shown.has(row.primaryId));
    const label = `Other tables (${new Set(rest.map(row => row.primaryId)).size})`;
    for (const activity of new Set(rest.map(row => row.secondaryLabel)))
      result.push({ ...combine(rest.filter(row => row.secondaryLabel === activity),'other-tables',label,activity), remainingTables: new Set(rest.map(row => row.primaryId)).size });
  }
  return result;
}

/** Only combines already-aggregated rows; execution attribution remains server-side. */
export function applyResourceSummary(context: TimelineAttribution, summary: ResourceAttributionSummary, hidden?: ReadonlySet<Kind>): TimelineAttribution {
  const groups = summary.groups.filter(group => !hidden?.has(group.kind));
  const total = groups.reduce((sum,g) => sum+g.value,0);
  const duration = summary.selection.kind === 'range' ? (summary.selection.endMs-summary.selection.startMs)/1000 : 1;
  return { ...context, selection: summary.selection, operations: [], total, meanValue: summary.selection.kind === 'range' ? total/duration : null,
    work: summary.selection.kind === 'range' && summary.mode !== 'memory' ? total : null, peakValue: null,
    coverage: { active: groups.reduce((sum,g) => sum+g.operationCount,0), sampled: groups.reduce((sum,g) => sum+g.sampledCount,0),
      estimated: groups.reduce((sum,g) => sum+g.estimatedCount,0), unavailable: groups.reduce((sum,g) => sum+g.unavailableCount,0),
      sampledOperationMs: groups.reduce((sum,g) => sum+g.sampledMs,0), estimatedOperationMs: groups.reduce((sum,g) => sum+g.estimatedMs,0), missingOperationMs: groups.reduce((sum,g) => sum+g.missingMs,0) } };
}

/** Aggregate all activity types before ranking tables; never split a multi-table cost. */
export function groupResourceActivitiesByTable(consumers: ResourceConsumer[]): ResourceFlowGroup[] {
  const groups = new Map<string, ResourceFlowGroup>();
  const totals = new Map<string, number>();
  for (const c of consumers) {
    const id = JSON.stringify([c.tableKey, c.group]);
    const group = groups.get(id) ?? { id, primaryId: c.tableKey, primaryLabel: c.tableLabel,
      secondaryLabel: c.group, tables: c.tables, rows: [], value: 0 };
    group.rows.push(c); group.value += c.value;
    groups.set(id, group);
    totals.set(c.tableKey, (totals.get(c.tableKey) ?? 0) + c.value);
  }
  return [...groups.values()].sort((a, b) => (totals.get(b.primaryId)! - totals.get(a.primaryId)!)
    || a.primaryId.localeCompare(b.primaryId) || b.value - a.value || a.id.localeCompare(b.id));
}

export function groupResourceFlow(consumers: ResourceConsumer[], groupBy: 'activity' | 'table', tableLimit: ResourceTableLimits = 6, activityLimit?: number): ResourceFlowGroup[] {
  const groups = groupBy === 'table' ? groupResourceActivitiesByTable(consumers)
    : groupResourceTables(consumers, Infinity).map(({ activity, label, ...group }) => ({
      ...group, primaryId: activity, primaryLabel: activity, secondaryLabel: label,
    }));
  return limitResourceActivities(limitResourceTables(groups, groupBy, tableLimit),groupBy,activityLimit);
}

/** Every operation has exactly one branch, including multi-table and unknown queries. */
export function groupResourceTables(consumers: ResourceConsumer[], limitPerActivity = 6): ResourceTableGroup[] {
  const groups = new Map<string, ResourceTableGroup>();
  for (const c of consumers) {
    const id = JSON.stringify([c.group, c.tableKey]);
    const group = groups.get(id) ?? { id, activity: c.group, label: c.tableLabel, tables: c.tables, rows: [], value: 0 };
    group.rows.push(c); group.value += c.value;
    groups.set(id, group);
  }
  const result: ResourceTableGroup[] = [];
  for (const activity of new Set(consumers.map(c => c.group))) {
    const tables = [...groups.values()].filter(g => g.activity === activity).sort((a, b) => b.value - a.value || a.id.localeCompare(b.id));
    result.push(...tables.slice(0, limitPerActivity));
    const rest = tables.slice(limitPerActivity);
    if (rest.length) result.push({
      id: JSON.stringify([activity, 'other-tables']), activity, label: `Other table groups (${rest.length})`,
      tables: [...new Set(rest.flatMap(g => g.tables))], rows: rest.flatMap(g => g.rows),
      value: rest.reduce((sum, g) => sum + g.value, 0), remainder: true,
    });
  }
  return result;
}

export interface ResourceLoadResult {
  data: MemoryTimeline;
  samplingWarning?: string;
  contextSeries?: TimeseriesPoint[];
  summary?: ResourceAttributionSummary;
  snapshots?: ResourceAttributionWindow;
}
export type ResourceDataLoader = ((source: MemoryTimeline, mode: MetricMode, atMs: number, hostLabel?: string | readonly string[], selection?: AttributionSelection) => Promise<ResourceLoadResult>) & {
  loadDetails?: (options: { mode: MetricMode; selection: AttributionSelection; hostname?: string | readonly string[]; groups: ResourceAttributionGroupKey[]; limit: number }) => Promise<{ operations: AttributedOperation[]; warnings: string[] }>;
};

export type ResourceOperationOpen = (kind: Kind, operation: QuerySeries | MergeSeries | MutationSeries) => void;
