import type { MemoryTimeline, MergeSeries, MutationSeries, QuerySeries, TimeseriesPoint, AttributionSelection, AttributionBasis, TimelineAttribution } from '@tracehouse/core';
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
export function attributionConsumers(attribution: TimelineAttribution) {
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

export type ResourceDataLoader = (source: MemoryTimeline, mode: MetricMode, atMs: number, hostLabel?: string | readonly string[], selection?: AttributionSelection) => Promise<{
  data: MemoryTimeline;
  samplingWarning?: string;
  contextSeries?: TimeseriesPoint[];
}>;

export type ResourceOperationOpen = (kind: Kind, operation: QuerySeries | MergeSeries | MutationSeries) => void;
