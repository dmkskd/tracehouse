/**
 * Per-type resource breakdown at one instant of the Time Travel timeline.
 *
 * This answers "what is the load made of right now" without needing per-second
 * sampling: each operation contributes its own average rate across its lifetime
 * (the same value the chart stacks into its bands), or its sampled value where
 * zoom samples exist. Because the bands are averages, the stack is an estimate
 * of composition, not a measurement — `hasSamples` and the measured server value
 * are returned alongside so the UI can show both and never conflate them.
 */
import type {
  MergeSeries,
  MutationSeries,
  QuerySeries,
  TimeseriesPoint,
  ZoomSample,
} from '../types/timeline.js';
import type { OperationKind, OperationMetricMode } from './timeline-operations.js';
import { parseOperationTimestamp } from './timeline-operations.js';

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
 * The operation's average rate over its lifetime, in the metric's unit: the
 * value the timeline chart draws as that operation's band height, and the one
 * the instant breakdown stacks. Shared so the two cannot drift apart.
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

/** Nearest sample to `atMs`, or null when the closest one is too far away. */
function nearestSample(samples: readonly ZoomSample[], atMs: number): ZoomSample | null {
  let best: ZoomSample | null = null;
  let bestDist = Infinity;
  for (const sample of samples) {
    const dist = Math.abs(sample.ms - atMs);
    if (dist < bestDist) { bestDist = dist; best = sample; }
  }
  return bestDist <= SAMPLE_TOLERANCE_MS ? best : null;
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

  const groups = new Map<string, InstantSegment>();
  let hasSamples = false;

  const add = (
    key: string,
    kind: OperationKind,
    item: QuerySeries | MergeSeries | MutationSeries,
  ) => {
    const startMs = parseOperationTimestamp(item.start_time);
    const endMs = parseOperationTimestamp(item.end_time);
    let value: number;
    const sample = item.zoomSamples ? nearestSample(item.zoomSamples, atMs) : null;
    if (sample) {
      value = sampleValue(sample, mode);
      hasSamples = true;
    } else {
      if (atMs < startMs || atMs > endMs) return;
      value = operationAverageRate(item, mode);
    }
    if (value <= 0) return;
    const entry = groups.get(key) ?? { key, kind, value: 0, shareOfCapacity: null, shareOfShown: 0, count: 0 };
    entry.value += value;
    entry.count += 1;
    groups.set(key, entry);
  };

  if (!hiddenKinds?.has('query')) for (const q of input.queries) add(queryKey(q), 'query', q);
  if (!hiddenKinds?.has('merge')) for (const m of input.merges) add('MERGE', 'merge', m);
  if (!hiddenKinds?.has('mutation')) for (const m of input.mutations) add('MUTATION', 'mutation', m);

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
  data: {
    server_memory: TimeseriesPoint[];
    server_cpu: TimeseriesPoint[];
    server_network_send: TimeseriesPoint[];
    server_network_recv: TimeseriesPoint[];
    server_disk_read?: TimeseriesPoint[];
    server_disk_write?: TimeseriesPoint[];
  },
  mode: OperationMetricMode,
): TimeseriesPoint[] {
  if (mode === 'memory') return data.server_memory;
  if (mode === 'cpu') return data.server_cpu;
  if (mode === 'network') {
    return data.server_network_send.map((p, i) => ({
      t: p.t, v: p.v + (data.server_network_recv[i]?.v ?? 0),
    }));
  }
  return (data.server_disk_read ?? []).map((p, i) => ({
    t: p.t, v: p.v + (data.server_disk_write?.[i]?.v ?? 0),
  }));
}
