import type { QueryParameter } from '../queries/builder.js';

/**
 * Which operations Time Travel lists. An operation is listed when it passes either
 * threshold, so a duration threshold only adds operations to the memory threshold.
 */
export interface TimelineFilters {
  /** Peak memory above this many bytes. 0 lists every operation. */
  minMemoryBytes: number;
  /** Duration of at least this many milliseconds. 0 turns the duration threshold off. */
  minDurationMs: number;
  /** Also lists queries that failed before they started (query_log ExceptionBeforeStart, e.g. ACCESS_DENIED), whatever their size. */
  includeFailedBeforeStart: boolean;
  /** Also lists the queries TraceHouse itself runs. */
  includeTraceHouse: boolean;
}

export const DEFAULT_TIMELINE_FILTERS: TimelineFilters = {
  minMemoryBytes: 1_048_576, minDurationMs: 0, includeFailedBeforeStart: false, includeTraceHouse: false,
};

/** Choices the UI offers; any non-negative value is accepted from settings and URLs. */
export const MIN_MEMORY_CHOICES = [0, 65_536, 1_048_576, 16_777_216, 134_217_728] as const;
export const MIN_DURATION_CHOICES = [0, 10, 100, 1_000, 10_000] as const;

/** URL parameters of each filter. */
export const TIMELINE_FILTER_PARAMS = { minMemoryBytes: 'tt_min_mem', minDurationMs: 'tt_min_dur', includeFailedBeforeStart: 'tt_failed_start', includeTraceHouse: 'tt_self' } as const;

const nonNegative = (value: unknown): number | undefined => {
  const number = typeof value === 'string' && value.trim() !== '' ? Number(value) : value;
  return typeof number === 'number' && Number.isFinite(number) && number >= 0 ? Math.floor(number) : undefined;
};
const flag = (value: unknown): boolean | undefined =>
  value === true || value === '1' || value === 'true' ? true : value === false || value === '0' || value === 'false' ? false : undefined;

/** The valid fields of a partial setting (admin JSON, stored preference); others are dropped. */
export function cleanTimelineFilters(value: Partial<Record<keyof TimelineFilters, unknown>> | null | undefined): Partial<TimelineFilters> {
  const clean: Partial<TimelineFilters> = {};
  const minMemoryBytes = nonNegative(value?.minMemoryBytes), minDurationMs = nonNegative(value?.minDurationMs);
  const includeFailedBeforeStart = flag(value?.includeFailedBeforeStart), includeTraceHouse = flag(value?.includeTraceHouse);
  if (minMemoryBytes !== undefined) clean.minMemoryBytes = minMemoryBytes;
  if (minDurationMs !== undefined) clean.minDurationMs = minDurationMs;
  if (includeFailedBeforeStart !== undefined) clean.includeFailedBeforeStart = includeFailedBeforeStart;
  if (includeTraceHouse !== undefined) clean.includeTraceHouse = includeTraceHouse;
  return clean;
}

/** Filters set in a URL. */
export function readTimelineFilters(get: (name: string) => string | null): Partial<TimelineFilters> {
  return cleanTimelineFilters(Object.fromEntries(Object.entries(TIMELINE_FILTER_PARAMS).map(([key, param]) => [key, get(param) ?? undefined])));
}

/** The effective filters: built-in defaults, then the admin default, the personal setting and the URL. */
export function resolveTimelineFilters(...layers: (Partial<TimelineFilters> | null | undefined)[]): TimelineFilters {
  return Object.assign({ ...DEFAULT_TIMELINE_FILTERS }, ...layers.map(layer => cleanTimelineFilters(layer)));
}

export function formatBytesThreshold(bytes: number): string {
  if (bytes === 0) return 'off';
  const units = ['B', 'KB', 'MB', 'GB'];
  let value = bytes, unit = 0;
  while (value >= 1024 && unit < units.length - 1 && value % 1024 === 0) { value /= 1024;unit++; }
  return `${value} ${units[unit]}`;
}
export function formatDurationThreshold(ms: number): string {
  if (ms === 0) return 'off';
  return ms >= 1000 && ms % 1000 === 0 ? `${ms / 1000} s` : `${ms} ms`;
}

/** One line naming what the filters list, e.g. "over 1 MB or 1 s · failed before start shown". */
export function describeTimelineFilters(filters: TimelineFilters): string {
  const thresholds = filters.minMemoryBytes === 0 ? 'all sizes'
    : `over ${formatBytesThreshold(filters.minMemoryBytes)}${filters.minDurationMs ? ` or ${formatDurationThreshold(filters.minDurationMs)}` : ''}`;
  return [thresholds, ...(filters.includeFailedBeforeStart ? ['failed before start shown'] : []), ...(filters.includeTraceHouse ? ['TraceHouse queries shown'] : [])].join(' · ');
}

/** Whether an operation passes the size thresholds, as the timeline SQL decides it. */
export function passesThresholds(filters: TimelineFilters, peakMemoryBytes: number, durationMs: number): boolean {
  return filters.minMemoryBytes === 0 || peakMemoryBytes > filters.minMemoryBytes
    || (filters.minDurationMs > 0 && durationMs >= filters.minDurationMs);
}

/** SQL parameters of the timeline queries. */
export function timelineFilterParams(filters: TimelineFilters): Record<string, QueryParameter> {
  return { min_memory_bytes: filters.minMemoryBytes, min_duration_ms: filters.minDurationMs,
    include_failed_before_start: filters.includeFailedBeforeStart ? 1 : 0, include_app: filters.includeTraceHouse ? 1 : 0 };
}

/**
 * A sentence naming what Time Travel lists, e.g. "The 100 queries, 100 merges and 100 mutations
 * with the highest CPU, using over 1 MB of memory or running over 1 s. Queries that failed before start included."
 */
export function explainListedOperations(limit: number, metricName: string, filters: TimelineFilters): string {
  const size = filters.minMemoryBytes === 0 ? 'of any size'
    : `using over ${formatBytesThreshold(filters.minMemoryBytes)} of memory${filters.minDurationMs ? ` or running over ${formatDurationThreshold(filters.minDurationMs)}` : ''}`;
  const extras = [...(filters.includeFailedBeforeStart ? ['Queries that failed before start'] : []), ...(filters.includeTraceHouse ? ["TraceHouse's own queries"] : [])];
  return `The ${limit} queries, ${limit} merges and ${limit} mutations with the highest ${metricName}, ${size}.`
    + (extras.length ? ` ${extras.join(' and ')} included.` : '');
}
