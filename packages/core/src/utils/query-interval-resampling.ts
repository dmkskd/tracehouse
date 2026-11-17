import type { ZoomSample } from '../types/timeline.js';

/** Normalized local execution: end timestamp in milliseconds, duration in seconds, interval counters. */
export interface QueryZoomInterval {
  query_id: string;
  sample_query_id: string;
  hostname: string;
  ts_ms: number;
  memory_usage: number;
  dt: number;
  /** False for a cumulative-counter baseline without a predecessor. Memory is still observed. */
  interval_valid?: boolean;
  cpu_us: number;
  net_bytes: number;
  disk_bytes: number;
}

/** First captured bucket at or after the start of an interval. */
function lowerBound(times: readonly number[], target: number): number {
  let lo = 0;
  let hi = times.length;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (times[mid] < target) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

/**
 * Convert irregular local-execution intervals into per-second query samples.
 * Counters are distributed uniformly within each reported interval and weighted
 * by bucket overlap. Rates use observed duration; partial coverage is not idle time.
 * Memory remains an average of gauges actually captured in that second.
 * Separate local executions are processed before rates/gauges are summed by root ID.
 * No database access, UI state, or Sankey assumptions belong in this function.
 */
export function resampleQueryIntervals(raw: readonly QueryZoomInterval[]): Map<string, ZoomSample[]> {
  // Gauges belong to their capture time. Rates are weighted by the overlap of
  // each measurement interval with a wall-clock second, not by its endpoint.
  // Otherwise a 227 ms catch-up sample can inflate an entire displayed second.
  const localBuckets = new Map<string, { queryId: string; ms: number; memory: number; count: number; dt: number; cpu: number; net: number; disk: number }>();
  const bucketTimes = new Map<string, number[]>();
  for (const row of raw) {
    if (!Number.isFinite(row.ts_ms)) continue;
    const ms = Math.floor(row.ts_ms / 1000) * 1000;
    const execution = JSON.stringify([row.query_id, row.hostname, row.sample_query_id]);
    const key = `${execution}:${ms}`;
    if (!localBuckets.has(key)) {
      const times = bucketTimes.get(execution) ?? [];
      times.push(ms); bucketTimes.set(execution, times);
    }
    const bucket = localBuckets.get(key) ?? { queryId: row.query_id, ms, memory: 0, count: 0, dt: 0, cpu: 0, net: 0, disk: 0 };
    bucket.memory += row.memory_usage; bucket.count++;
    localBuckets.set(key, bucket);
  }
  for (const times of bucketTimes.values()) times.sort((a, b) => a - b);
  for (const row of raw) {
    if (row.interval_valid === false || !Number.isFinite(row.ts_ms) || !Number.isFinite(row.dt) || row.dt <= 0) continue;
    const execution = JSON.stringify([row.query_id, row.hostname, row.sample_query_id]);
    const startMs = row.ts_ms - row.dt * 1000;
    const times = bucketTimes.get(execution) ?? [];
    const firstMs = Math.floor(startMs / 1000) * 1000;
    const firstIndex = lowerBound(times, firstMs);
    // Visit captured buckets only: a long sampler outage must not scan every missing second.
    for (let i = firstIndex; i < times.length && times[i] < row.ts_ms; i++) {
      const ms = times[i];
      const bucket = localBuckets.get(`${execution}:${ms}`);
      // A missing gauge sample remains missing; do not synthesize memory zeros.
      if (!bucket) continue;
      const overlap = Math.max(0, Math.min(row.ts_ms, ms + 1000) - Math.max(startMs, ms)) / 1000;
      const weight = overlap / row.dt;
      bucket.dt += overlap;
      bucket.cpu += row.cpu_us * weight;
      bucket.net += row.net_bytes * weight;
      bucket.disk += row.disk_bytes * weight;
    }
  }
  const grouped = new Map<string, Map<number, ZoomSample>>();
  for (const bucket of localBuckets.values()) {
    const samples = grouped.get(bucket.queryId) ?? new Map<number, ZoomSample>();
    const sample = samples.get(bucket.ms) ?? { ms: bucket.ms, memory: 0, cpu_cores: 0, net_rate: 0, disk_rate: 0 };
    sample.memory += bucket.memory / bucket.count;
    if (bucket.dt <= 0) sample.rate_sampled = false;
    sample.cpu_cores += bucket.dt > 0 ? bucket.cpu / 1e6 / bucket.dt : 0;
    sample.net_rate += bucket.dt > 0 ? bucket.net / bucket.dt : 0;
    sample.disk_rate += bucket.dt > 0 ? bucket.disk / bucket.dt : 0;
    samples.set(bucket.ms, sample); grouped.set(bucket.queryId, samples);
  }
  return new Map([...grouped].map(([id, samples]) => [id, [...samples.values()].sort((a, b) => a.ms - b.ms)]));
}
