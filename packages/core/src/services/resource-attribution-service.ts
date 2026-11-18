import type { IClickHouseAdapter } from '../adapters/types.js';
import type { AttributionSelection, AttributedOperation } from '../utils/timeline-attribution.js';
import type { ResourceAttributionRequest, ResourceAttributionSummary, ResourceAttributionWindow, ResourceAttributionGroup, ResourceAttributionGroupKey, ResourceAttributionDetails } from '../types/resource-attribution.js';
import type { QuerySeries, MergeSeries, MutationSeries } from '../types/timeline.js';
import { buildResourceAttributionSQL, type ResourceAggregationSQLRequest } from '../queries/resource-attribution-queries.js';
import { tagQuery } from '../queries/builder.js';

type Row = Record<string, unknown>;
const number = (value: unknown) => Number(value ?? 0);

function groupFromRow(row: Row): ResourceAttributionGroup {
  const key: ResourceAttributionGroupKey = { kind: row.kind as ResourceAttributionGroupKey['kind'], activity: String(row.activity),
    tables: row.tables as string[], tableState: row.table_state as ResourceAttributionGroupKey['tableState'] };
  return { ...key, id: JSON.stringify([key.kind,key.activity,key.tables,key.tableState]), value: number(row.value), failureValue: number(row.failure_value),
    operationCount: number(row.operation_count), failedCount: number(row.failed_count), sampledCount: number(row.sampled_count), estimatedCount: number(row.estimated_count),
    unavailableCount: number(row.unavailable_count), zeroCount: number(row.zero_count), sampledMs: number(row.sampled_ms), estimatedMs: number(row.estimated_ms), missingMs: number(row.missing_ms) };
}

/** Whole-population totals and bounded execution details. Independent of timeline display limits. */
export class ResourceAttributionService {
  private mergeHistory?: Promise<boolean>;
  constructor(private readonly adapter: IClickHouseAdapter) {}

  private async execute(options: Omit<ResourceAggregationSQLRequest, 'mergeSamplesAvailable'>) {
    let mergeSamplesAvailable = false;
    if (options.mode === 'memory') {
      this.mergeHistory ??= this.adapter.executeQuery(tagQuery("SELECT count() AS present FROM system.tables WHERE database='tracehouse' AND name='merges_history'", 'TraceHouse:Attribution:capabilities'))
        .then(rows => number(rows[0]?.present) > 0).catch(() => false);
      mergeSamplesAvailable = await this.mergeHistory;
    }
    const run = (request: ResourceAggregationSQLRequest) => this.adapter.executeQuery(tagQuery(buildResourceAttributionSQL(request), `TraceHouse:Attribution:${options.details ? 'details' : options.snapshots ? 'snapshots' : 'range'}`));
    try { return { rows: await run({ ...options, mergeSamplesAvailable }), warnings: [] as string[] }; }
    catch (error) {
      // Missing optional sampling tables must not turn into silently limited totals.
      if (!/UNKNOWN_TABLE|does not exist|Unknown table/i.test(String(error))) throw error;
      const rows = await run({ ...options, mergeSamplesAvailable: false, sampleSelection: undefined });
      return { rows, warnings: ['Operation samples are unavailable. CPU and I/O use execution averages; concurrent memory remains unavailable.'] };
    }
  }

  async getSnapshots(options: ResourceAttributionRequest & { startMs: number; endMs: number }): Promise<ResourceAttributionWindow> {
    const { rows, warnings } = await this.execute({ ...options, snapshots: true });
    const snapshots = new Map<number, ResourceAttributionSummary>();
    for (const row of rows) {
      const atMs = number(row.snapshot_ms);
      const summary = snapshots.get(atMs) ?? { selection: { kind: 'instant' as const, atMs }, mode: options.mode, groups: [], warnings };
      summary.groups.push(groupFromRow(row)); snapshots.set(atMs, summary);
    }
    return { startMs: options.startMs, endMs: options.endMs, stepMs: 1000, mode: options.mode, snapshots, warnings };
  }

  async getRange(options: ResourceAttributionRequest & { startMs: number; endMs: number }): Promise<ResourceAttributionSummary> {
    const { rows, warnings } = await this.execute({ ...options, snapshots: false });
    return { selection: { kind: 'range', startMs: options.startMs, endMs: options.endMs }, mode: options.mode, groups: rows.map(groupFromRow), warnings };
  }

  async getDetails(options: ResourceAttributionRequest & { selection: AttributionSelection; groups: ResourceAttributionGroupKey[]; limit: number }): Promise<ResourceAttributionDetails> {
    const instant = options.selection.kind === 'instant';
    const startMs = instant ? Math.floor((options.selection as { atMs: number }).atMs / 1000) * 1000 : (options.selection as { startMs: number }).startMs;
    const endMs = instant ? startMs + 1000 : (options.selection as { endMs: number }).endMs;
    const { rows, warnings } = await this.execute({ ...options, startMs, endMs, snapshots: instant, details: { groups: options.groups, limit: options.limit } });
    return { warnings, operations: rows.map((row, index): AttributedOperation => {
      const kind = row.kind as AttributedOperation['kind'];
      const metrics = { hostname: String(row.hostname), duration_ms: number(row.duration_ms), cpu_us: number(row.cpu_us), peak_memory: number(row.peak_memory),
        net_send: number(row.net_bytes), net_recv: 0, disk_read: number(row.disk_bytes), disk_write: 0,
        start_time: new Date(number(row.op_start_ms)).toISOString(), end_time: new Date(number(row.op_end_ms)).toISOString(), is_running: !!number(row.is_running) };
      const source: QuerySeries | MergeSeries | MutationSeries = kind === 'query'
        ? { ...metrics, query_id: String(row.local_id), label: String(row.label), user: String(row.user), query_kind: String(row.activity), tables: row.tables as string[], exception_code: number(row.failure_code), points: [] }
        : { ...metrics, table: (row.tables as string[])[0] ?? '', part_name: String(row.label), error: number(row.failure_code),
          event_time_microseconds: row.source_event_time_us ? String(row.source_event_time_us) : undefined,
          event_type: String(row.source_event_type || (kind === 'mutation' ? 'MutatePart' : 'MergeParts')),
          operation_id: JSON.stringify([row.source_event_type,row.source_event_time_us]) };
      const value = number(row.attributed_value), sampled = !!number(row.sampled), estimated = !!number(row.estimated);
      return { id: JSON.stringify([kind,row.hostname,row.local_id,kind === 'query' ? row.op_start_ms : row.source_event_time_us]), source, kind, index, activity: String(row.activity), tables: row.tables as string[], failureCode: number(row.failure_code),
        value, meanValue: instant ? null : value / ((endMs-startMs)/1000), basis: sampled && estimated ? 'mixed' : sampled ? 'sampled' : 'average',
        work: !instant && options.mode !== 'memory' ? value : null, activeMs: number(row.active_ms), sampledMs: number(row.sampled_ms), estimatedMs: number(row.estimated_ms), missingMs: number(row.missing_ms) };
    }) };
  }
}

/** Snapshot lookup does no I/O; the UI can scrub an already-loaded window synchronously. */
export function resourceSnapshotAt(window: ResourceAttributionWindow, atMs: number): ResourceAttributionSummary {
  const snapped = Math.floor(atMs / window.stepMs) * window.stepMs;
  if (snapped < window.startMs || snapped >= window.endMs) throw new Error('Instant is outside the prefetched attribution window');
  return window.snapshots.get(snapped) ?? { selection: { kind: 'instant', atMs: snapped }, mode: window.mode, groups: [], warnings: window.warnings };
}
