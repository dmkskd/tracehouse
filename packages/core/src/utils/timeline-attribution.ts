/**
 * Shared activity attribution for Time Travel lists, pins, and resource visualizations.
 * Selection semantics, sampling rules, and measurement coverage live here, not in a renderer.
 */
import type {
  MemoryTimeline,
  MergeSeries,
  MutationSeries,
  QuerySeries,
  TimeseriesPoint,
  ZoomSample,
} from '../types/timeline.js';
import type { OperationKind, OperationMetricMode } from './timeline-operations.js';
import { getTimelineCpuCapacity, getTimelineRamCapacity } from '../types/timeline.js';
import { operationKeyFor, parseOperationTimestamp } from './timeline-operations.js';

/** Sample lookups tolerate this much distance before reading as "no value". */
const SAMPLE_TOLERANCE_MS = 2000;

export interface InstantSegment {
  /** Grouping key: a query kind (SELECT, INSERT, …) or MERGE / MUTATION. */
  key: string;
  kind: OperationKind;
  /** Summed contribution at the instant, in the metric's own unit. */
  value: number;
  /** Fraction of capacity, or null for metrics with no capacity (disk, network). */
  shareOfCapacity: number | null;
  /** Fraction of everything shown at this instant. */
  shareOfShown: number;
  /** How many operations of this type were active. */
  count: number;
}

export interface InstantBreakdown {
  atMs: number;
  segments: InstantSegment[];
  /** Sum across all segments. */
  shownTotal: number;
  shownShareOfCapacity: number | null;
  /** Measured server value at the instant, or null when no sample is near. */
  measured: number | null;
  measuredShareOfCapacity: number | null;
  capacity: number | null;
  /** True when at least one contribution came from a sample rather than an average. */
  hasSamples: boolean;
}

const EMPTY_BREAKDOWN: InstantBreakdown = {
  atMs: 0, segments: [], shownTotal: 0, shownShareOfCapacity: null,
  measured: null, measuredShareOfCapacity: null, capacity: null, hasSamples: false,
};

/**
 * Execution-wide estimate used for historical band geometry and unsampled rate
 * intervals. Memory returns the execution peak for the historical band only;
 * attributeTimeline never uses that peak as concurrent memory attribution.
 */
export function operationAverageRate(
  item: QuerySeries | MergeSeries | MutationSeries,
  mode: OperationMetricMode,
): number {
  if (mode === 'memory') return item.peak_memory;
  const seconds = Math.max(item.duration_ms / 1000, 0.001);
  if (mode === 'cpu') return item.cpu_us / seconds;
  if (mode === 'network') return (item.net_send + item.net_recv) / seconds;
  return (item.disk_read + item.disk_write) / seconds;
}

function sampleValue(sample: ZoomSample, mode: OperationMetricMode): number {
  if (mode === 'memory') return sample.memory;
  // Zoom CPU samples are in cores; band values are µs/s, so scale to match.
  if (mode === 'cpu') return sample.cpu_cores * 1_000_000;
  if (mode === 'network') return sample.net_rate;
  return sample.disk_rate;
}

/** Nearest measured server point to `atMs`, or null when none is close enough. */
export function measuredValueAt(
  series: readonly TimeseriesPoint[],
  atMs: number,
  toleranceMs = SAMPLE_TOLERANCE_MS,
): number | null {
  let best: number | null = null;
  let bestDist = Infinity;
  for (const point of series) {
    const dist = Math.abs(parseOperationTimestamp(point.t) - atMs);
    if (dist < bestDist) { bestDist = dist; best = point.v; }
  }
  return bestDist <= toleranceMs ? best : null;
}

function queryKey(q: QuerySeries): string {
  const kind = (q.query_kind ?? '').trim();
  return kind.length > 0 ? kind.toUpperCase() : 'QUERY';
}

export interface InstantBreakdownInput {
  queries: readonly QuerySeries[];
  merges: readonly MergeSeries[];
  mutations: readonly MutationSeries[];
  /** Measured server series for the active metric. */
  serverSeries: readonly TimeseriesPoint[];
}

export interface InstantBreakdownOptions {
  atMs: number;
  mode: OperationMetricMode;
  /**
   * Full-scale value for the metric: CPU cores × 1e6 for cpu, total RAM for
   * memory. Null for disk and network, which have no capacity to divide by.
   */
  capacity: number | null;
  /** Kinds hidden from the chart, excluded so the panel matches what is drawn. */
  hiddenKinds?: ReadonlySet<OperationKind>;
  hostname?: string | readonly string[];
}

/**
 * Compose the resource stack at one instant, grouped by operation type.
 * Segments come back largest first.
 */
export function buildInstantBreakdown(
  input: InstantBreakdownInput,
  options: InstantBreakdownOptions,
): InstantBreakdown {
  const { atMs, mode, capacity, hiddenKinds } = options;
  if (!Number.isFinite(atMs)) return EMPTY_BREAKDOWN;

  const operations = attributeOperations(input, { kind: 'instant', atMs }, mode, options.hostname, hiddenKinds).operations;
  const groups = new Map<string, InstantSegment>();
  const hasSamples = operations.some(row => row.basis === 'sampled');
  for (const row of operations) {
    if (row.value === null || row.value <= 0) continue;
    const entry = groups.get(row.activity) ?? { key: row.activity, kind: row.kind, value: 0, shareOfCapacity: null, shareOfShown: 0, count: 0 };
    entry.value += row.value; entry.count++;
    groups.set(row.activity, entry);
  }

  const segments = [...groups.values()].sort((a, b) => b.value - a.value || a.key.localeCompare(b.key));
  const shownTotal = segments.reduce((sum, s) => sum + s.value, 0);
  for (const segment of segments) {
    segment.shareOfShown = shownTotal > 0 ? segment.value / shownTotal : 0;
    segment.shareOfCapacity = capacity && capacity > 0 ? segment.value / capacity : null;
  }

  const measured = measuredValueAt(input.serverSeries, atMs);
  return {
    atMs,
    segments,
    shownTotal,
    shownShareOfCapacity: capacity && capacity > 0 ? shownTotal / capacity : null,
    measured,
    measuredShareOfCapacity: measured !== null && capacity && capacity > 0 ? measured / capacity : null,
    capacity,
    hasSamples,
  };
}

/**
 * The measured server series for a metric mode. Network and disk are the sum of
 * their two directions, matching what the chart draws as the measured line.
 */
export function selectServerSeries(
  data: Pick<MemoryTimeline, 'server_memory' | 'server_cpu' | 'server_network_send' | 'server_network_recv'> & Partial<Pick<MemoryTimeline, 'server_disk_read' | 'server_disk_write' | 'host_count' | 'per_host_cpu'>>,
  mode: OperationMetricMode,
  hostname: string | readonly string[] = '',
): TimeseriesPoint[] {
  const hosts = typeof hostname === 'string' ? hostname ? [hostname] : [] : hostname;
  if (hosts.length && (data.host_count ?? 1) > 1) {
    if (mode !== 'cpu' || hosts.some(host => !data.per_host_cpu?.[host])) return [];
    const series = hosts.map(host => data.per_host_cpu![host]);
    const rest = series.slice(1).map(points => new Map(points.map(point => [parseOperationTimestamp(point.t), point.v])));
    return series[0].filter(point => rest.every(values => values.has(parseOperationTimestamp(point.t))))
      .map(point => ({ t: point.t, v: rest.reduce((sum, values) => sum + values.get(parseOperationTimestamp(point.t))!, point.v) }));
  }
  if (mode === 'memory') return data.server_memory;
  if (mode === 'cpu') return data.server_cpu;
  const first = mode === 'network' ? data.server_network_send : data.server_disk_read ?? [];
  const second = mode === 'network' ? data.server_network_recv : data.server_disk_write ?? [];
  const key = (point: TimeseriesPoint) => {
    const ms = parseOperationTimestamp(point.t);
    return Number.isFinite(ms) ? ms : point.t;
  };
  const other = new Map(second.map(point => [key(point), point.v]));
  return first.filter(point => other.has(key(point))).map(point => ({ t: point.t, v: point.v + other.get(key(point))! }));
}

export type AttributionSelection =
  | { kind: 'instant'; atMs: number }
  | { kind: 'range'; startMs: number; endMs: number };
export type AttributionBasis = 'sampled' | 'average' | 'mixed' | 'unavailable';
export type AttributedSource = QuerySeries | MergeSeries | MutationSeries;

export interface AttributedOperation {
  id: string;
  source: AttributedSource;
  kind: OperationKind;
  index: number;
  activity: string;
  tables: string[];
  failureCode: number;
  /** Instant: CPU µs/s, I/O bytes/s, memory bytes. Range totals: CPU µs, I/O bytes, memory byte-seconds. */
  value: number | null;
  /** Known contribution divided by the full range duration, in instantaneous units; null for instants. */
  meanValue: number | null;
  basis: AttributionBasis;
  sampleMs?: number;
  /** CPU time in µs or I/O bytes over the range; null for instants and memory. */
  work: number | null;
  /** Range durations indicate support of available sample buckets, not raw sampler uptime. Unknown memory is not a measured zero. */
  activeMs: number;
  sampledMs: number;
  estimatedMs: number;
  missingMs: number;
}
export interface TimelineAttribution {
  selection: AttributionSelection;
  mode: OperationMetricMode;
  operations: AttributedOperation[];
  /** Known contributions only. Range values are duration integrals, not sums of unweighted snapshots. */
  total: number;
  unit: 'cpu-us-per-second' | 'cpu-us' | 'bytes-per-second' | 'bytes' | 'byte-seconds';
  meanValue: number | null;
  work: number | null;
  /** Largest sum of concurrent known values in instantaneous units, never a sum of per-operation peaks. */
  peakValue: number | null;
  measured: { value: number | null; meanValue: number | null; sampleMs: number | null; coverageMs: number; peakValue: number | null };
  capacity: number | null;
  capacityApproximate: boolean;
  coverage: { active: number; sampled: number; estimated: number; unavailable: number; sampledOperationMs: number; estimatedOperationMs: number; missingOperationMs: number };
}
export interface AttributionOptions {
  selection: AttributionSelection;
  mode: OperationMetricMode;
  hostname?: string | readonly string[];
  hiddenKinds?: ReadonlySet<OperationKind>;
}
interface ValueSpan { start: number; end: number; value: number; basis: 'sampled' | 'average' }

function validSelection(selection: AttributionSelection): boolean {
  return selection.kind === 'instant' ? Number.isFinite(selection.atMs)
    : Number.isFinite(selection.startMs) && Number.isFinite(selection.endMs) && selection.endMs > selection.startMs;
}
function usableSamples(item: AttributedSource, kind: OperationKind, mode: OperationMetricMode): ZoomSample[] {
  if ((mode === 'cpu' && kind !== 'query') || ('zoomMissing' in item && item.zoomMissing?.includes(mode))) return [];
  return (item.zoomSamples ?? []).filter(sample => (mode === 'memory' || sample.rate_sampled !== false) && Number.isFinite(sample.ms) && Number.isFinite(sampleValue(sample, mode)) && sampleValue(sample, mode) >= 0);
}
function instantSample(item: AttributedSource, kind: OperationKind, mode: OperationMetricMode, atMs: number): ZoomSample | undefined {
  const samples = usableSamples(item, kind, mode);
  if (kind === 'query' && mode !== 'memory') return samples.find(sample => sample.ms === Math.floor(atMs / 1000) * 1000);
  return samples.reduce<ZoomSample | undefined>((best, sample) => Math.abs(sample.ms - atMs) <= SAMPLE_TOLERANCE_MS
    && (!best || Math.abs(sample.ms - atMs) < Math.abs(best.ms - atMs)) ? sample : best, undefined);
}

/** Piecewise sample support; no interpolation across missing memory captures. */
function sampledSpans(item: AttributedSource, kind: OperationKind, mode: OperationMetricMode, start: number, end: number): ValueSpan[] {
  const unique = new Map<number, ZoomSample>();
  for (const sample of usableSamples(item, kind, mode)) if (!unique.has(sample.ms)) unique.set(sample.ms, sample);
  const samples = [...unique.values()].sort((a, b) => a.ms - b.ms);
  return samples.flatMap((sample, i) => {
    const left = mode === 'memory' || kind !== 'query'
      ? Math.max(sample.ms - SAMPLE_TOLERANCE_MS, i > 0 ? (samples[i - 1].ms + sample.ms) / 2 : -Infinity) : sample.ms;
    const right = mode === 'memory' || kind !== 'query'
      ? Math.min(sample.ms + SAMPLE_TOLERANCE_MS, i + 1 < samples.length ? (sample.ms + samples[i + 1].ms) / 2 : Infinity) : sample.ms + 1000;
    const a = Math.max(start, left), b = Math.min(end, right);
    return b > a ? [{ start: a, end: b, value: sampleValue(sample, mode), basis: 'sampled' as const }] : [];
  });
}

function attributeOperations(input: Pick<InstantBreakdownInput, 'queries' | 'merges' | 'mutations'>, selection: AttributionSelection, mode: OperationMetricMode, hostname?: string | readonly string[], hidden?: ReadonlySet<OperationKind>) {
  const operations: AttributedOperation[] = [];
  const events = new Map<number, number>();
  const addEvent = (at: number, delta: number) => events.set(at, (events.get(at) ?? 0) + delta);
  if (!validSelection(selection)) return { operations, peakValue: null };
  const add = (source: AttributedSource, kind: OperationKind, index: number) => {
    const hosts = typeof hostname === 'string' ? hostname ? [hostname] : [] : hostname ?? [];
    if (hidden?.has(kind) || (hosts.length > 0 && !hosts.includes(source.hostname ?? ''))) return;
    const start = parseOperationTimestamp(source.start_time), end = parseOperationTimestamp(source.end_time);
    if (!Number.isFinite(start) || !Number.isFinite(end) || end < start) return;
    const query = kind === 'query' ? source as QuerySeries : undefined;
    const part = kind !== 'query' ? source as MergeSeries : undefined;
    const base = {
      id: JSON.stringify([operationKeyFor(kind, source), source.start_time]), source, kind, index,
      activity: query ? queryKey(query) : kind === 'merge' ? 'MERGE' : 'MUTATION',
      tables: [...new Set((query?.tables ?? (part?.table ? [part.table] : [])).filter(table => typeof table === 'string').map(table => table.trim()).filter(Boolean))].sort(),
      failureCode: (query ? query.exception_code : part?.error) ?? 0,
    };
    if (selection.kind === 'instant') {
      if (selection.atMs < start || selection.atMs > end) return;
      const sample = instantSample(source, kind, mode, selection.atMs);
      const candidate = sample ? sampleValue(sample, mode) : mode === 'memory' ? null : operationAverageRate(source, mode);
      const value = candidate !== null && Number.isFinite(candidate) && candidate >= 0 ? candidate : null;
      operations.push({ ...base, value, meanValue: null, basis: value === null ? 'unavailable' : sample ? 'sampled' : 'average', sampleMs: sample?.ms,
        work: null, activeMs: 0, sampledMs: 0, estimatedMs: 0, missingMs: 0 });
      return;
    }
    const a = Math.max(start, selection.startMs), b = Math.min(end, selection.endMs);
    if (b <= a) return;
    const spans = sampledSpans(source, kind, mode, a, b);
    const average = mode === 'memory' ? null : operationAverageRate(source, mode);
    const canEstimate = average !== null && Number.isFinite(average) && average >= 0;
    let cursor = a;
    const complete: ValueSpan[] = [];
    for (const span of spans) {
      if (span.start > cursor && canEstimate) complete.push({ start: cursor, end: span.start, value: average!, basis: 'average' });
      complete.push(span); cursor = span.end;
    }
    if (cursor < b && canEstimate) complete.push({ start: cursor, end: b, value: average!, basis: 'average' });
    let sampledMs = 0, estimatedMs = 0, integral = 0;
    for (const span of complete) {
      const duration = span.end - span.start;
      if (span.basis === 'sampled') sampledMs += duration; else estimatedMs += duration;
      integral += span.value * duration;
      addEvent(span.start, span.value); addEvent(span.end, -span.value);
    }
    const known = sampledMs + estimatedMs;
    operations.push({ ...base, value: known > 0 ? integral / 1000 : null,
      meanValue: known > 0 ? integral / (selection.endMs - selection.startMs) : null,
      work: known > 0 && mode !== 'memory' ? integral / 1000 : null,
      basis: sampledMs > 0 && estimatedMs > 0 ? 'mixed' : sampledMs > 0 ? 'sampled' : estimatedMs > 0 ? 'average' : 'unavailable',
      activeMs: b - a, sampledMs, estimatedMs, missingMs: b - a - known });
  };
  input.queries.forEach((item, i) => add(item, 'query', i));
  input.merges.forEach((item, i) => add(item, 'merge', i));
  input.mutations.forEach((item, i) => add(item, 'mutation', i));
  operations.sort((a, b) => (b.value ?? -1) - (a.value ?? -1) || a.id.localeCompare(b.id));
  let peak = 0, current = 0;
  for (const time of [...events.keys()].sort((a, b) => a - b)) { current += events.get(time)!; peak = Math.max(peak, current); }
  return { operations, peakValue: operations.some(row => row.value !== null)
    ? selection.kind === 'instant' ? operations.reduce((sum, row) => sum + (row.value ?? 0), 0) : peak : null };
}

/** Shared measurement lookup, including temporal coverage for range means. */
export function measureTimelineSeries(series: readonly TimeseriesPoint[], selection: AttributionSelection): TimelineAttribution['measured'] {
  if (!validSelection(selection)) return { value: null, meanValue: null, sampleMs: null, coverageMs: 0, peakValue: null };
  const points = series.map(point => ({ ms: parseOperationTimestamp(point.t), value: point.v }))
    .filter(point => Number.isFinite(point.ms) && Number.isFinite(point.value) && point.value >= 0).sort((a, b) => a.ms - b.ms);
  if (selection.kind === 'instant') {
    const nearest = points.reduce<typeof points[number] | undefined>((best, point) => Math.abs(point.ms - selection.atMs) <= SAMPLE_TOLERANCE_MS
      && (!best || Math.abs(point.ms - selection.atMs) < Math.abs(best.ms - selection.atMs)) ? point : best, undefined);
    return { value: nearest?.value ?? null, meanValue: null, sampleMs: nearest?.ms ?? null, coverageMs: 0, peakValue: nearest?.value ?? null };
  }
  let integral = 0, coverageMs = 0, peakValue = 0;
  for (let i = 0; i < points.length; i++) {
    const point = points[i];
    const a = Math.max(selection.startMs, point.ms - SAMPLE_TOLERANCE_MS, i > 0 ? (points[i - 1].ms + point.ms) / 2 : -Infinity);
    const b = Math.min(selection.endMs, point.ms + SAMPLE_TOLERANCE_MS, i + 1 < points.length ? (point.ms + points[i + 1].ms) / 2 : Infinity);
    if (b <= a) continue;
    coverageMs += b - a; integral += point.value * (b - a); peakValue = Math.max(peakValue, point.value);
  }
  return { value: coverageMs > 0 ? integral / 1000 : null, meanValue: coverageMs > 0 ? integral / coverageMs : null, sampleMs: null, coverageMs, peakValue: coverageMs > 0 ? peakValue : null };
}

/** No IO or presentation dependencies. Lists, grouped charts, and Sankeys consume the same snapshot. */
export function attributeTimeline(data: MemoryTimeline, options: AttributionOptions): TimelineAttribution {
  const { selection, mode, hostname, hiddenKinds } = options;
  const { operations, peakValue } = attributeOperations({ queries: data.queries, merges: data.merges, mutations: data.mutations ?? [] }, selection, mode, hostname, hiddenKinds);
  const hosts = typeof hostname === 'string' ? hostname ? [hostname] : [] : hostname ?? [];
  const scopedCores = hosts.length && data.host_count > 1
    ? hosts.every(host => data.per_host_cpu_cores?.[host] !== undefined) ? hosts.reduce((sum, host) => sum + data.per_host_cpu_cores![host], 0) : 0
    : getTimelineCpuCapacity(data);
  const capacity = mode === 'cpu' ? scopedCores * 1e6
    : mode === 'memory' && (hosts.length === 0 || data.host_count === 1) ? getTimelineRamCapacity(data) : null;
  const coverage = {
    active: operations.length,
    sampled: operations.filter(row => row.basis === 'sampled' || row.basis === 'mixed').length,
    estimated: operations.filter(row => row.basis === 'average' || row.basis === 'mixed').length,
    unavailable: operations.filter(row => row.basis === 'unavailable').length,
    sampledOperationMs: operations.reduce((sum, row) => sum + row.sampledMs, 0),
    estimatedOperationMs: operations.reduce((sum, row) => sum + row.estimatedMs, 0),
    missingOperationMs: operations.reduce((sum, row) => sum + row.missingMs, 0),
  };
  const durationSeconds = selection.kind === 'range' && validSelection(selection) ? (selection.endMs - selection.startMs) / 1000 : 1;
  const unit: TimelineAttribution['unit'] = selection.kind === 'range'
    ? mode === 'cpu' ? 'cpu-us' : mode === 'memory' ? 'byte-seconds' : 'bytes'
    : mode === 'cpu' ? 'cpu-us-per-second' : mode === 'memory' ? 'bytes' : 'bytes-per-second';
  return { selection, mode, unit, meanValue: selection.kind === 'range' ? operations.reduce((sum, row) => sum + (row.meanValue ?? 0), 0) : null,
    operations, total: operations.reduce((sum, row) => sum + (row.value ?? 0), 0),
    work: selection.kind === 'range' && mode !== 'memory' && operations.some(row => row.work !== null) ? operations.reduce((sum, row) => sum + (row.work ?? 0), 0) : null,
    peakValue, measured: measureTimelineSeries(selectServerSeries(data, mode, hostname), selection),
    capacity: capacity && capacity > 0 ? capacity * durationSeconds : null, capacityApproximate: mode === 'cpu' && !!data.cpu_capacity_approximate, coverage };
}
