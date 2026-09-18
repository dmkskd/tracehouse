/**
 * X-Ray source selection.
 *
 * The X-Ray can be driven by either of two tables, which produce the same
 * ProcessSample shape but differ in fidelity and freshness:
 *
 *   tracehouse.processes_history  — our 1s sampler over system.processes.
 *                                   Requires the sampler to be installed.
 *                                   Carries thread counts and the real progress
 *                                   counters, and is current to the last tick.
 *   system.query_metric_log       — built in since 24.10. No install, finer
 *                                   resolution, cheaper to read. Buffered by one
 *                                   log flush interval, and missing thread counts
 *                                   and the progress counters. It does cover a
 *                                   query that is still running: rows are written
 *                                   throughout execution, and the identity join
 *                                   finds the QueryStart row.
 *
 * This module is the *only* place that decides between them. It is a pure
 * function so it can be tested without a server, and so that no component has
 * to reimplement the rules. The query layer then declares what it could not
 * provide (`missing`), and the display layer obeys that list instead of
 * hardcoding per-source knowledge.
 */

/** A concrete table the X-Ray can read from. */
export type MergeXRaySource = 'merges_history';

export type QueryXRaySource = 'processes_history' | 'query_metric_log';

/** What the user asked for. 'auto' lets the rules below decide. */
export type QueryXRaySourcePreference = 'auto' | QueryXRaySource;

/** Why the selected source was selected. Surfaced in the UI so 'auto' is explicable. */
export type QueryXRaySourceReason =
  /** Preferred source under automatic selection */
  | 'auto'
  /** The user's explicit override was honoured */
  | 'override'
  /** The requested source could not serve this query; fell back to the other */
  | 'fallback'
  /** Only one source exists on this server */
  | 'only_available';

/** Lifecycle of the query being inspected. Decides whether a buffered log can serve it. */
export type XRayQueryState = 'running' | 'finished' | 'unknown';

/** Which sources this connection actually has. Derived from MonitoringFlags. */
export interface QueryXRaySourceAvailability {
  /** tracehouse.processes_history exists (sampler installed) */
  processesHistory: boolean;
  /** system.query_metric_log exists AND carries the ProfileEvent columns we read */
  queryMetricLog: boolean;
}

export interface QueryXRaySourceSelection {
  /** The table to query, or null when neither source can serve this query. */
  source: QueryXRaySource | null;
  reason: QueryXRaySourceReason;
  /**
   * ProcessSample fields the selected source cannot populate faithfully.
   * The UI must hide or mark these rather than rendering the zeros/approximations
   * the SQL is forced to emit.
   */
  missing: readonly string[];
  /**
   * Worst-case staleness in milliseconds. 0 for the sampler (it writes through a
   * Buffer table but the app reads the same table), ~one log flush interval for
   * query_metric_log.
   */
  lagMs: number;
  /** Human-readable explanation, present whenever reason is 'fallback' or nothing is available. */
  note?: string;
}

/**
 * Stock `flush_interval_milliseconds` for system logs. query_metric_log rows
 * (and the query_log rows the identity join needs) are not visible until the
 * buffer flushes. Reported as `lagMs` so the UI can disclose it; it does not
 * disqualify the source, it just means the right edge of a live query's
 * timeline trails reality by a few seconds.
 */
export const SYSTEM_LOG_FLUSH_LAG_MS = 7500;

/**
 * Which source automatic selection prefers when both can serve the query.
 *
 * query_metric_log wins: it needs no install, samples finer, and reads far
 * cheaper (real columns instead of a ProfileEvents Map that must be read whole).
 * The cost is `missing` — no thread count, and progress counters replaced by
 * ProfileEvent approximations. Flip this constant to make the sampler the
 * default again.
 */
export const AUTO_PREFERRED_SOURCE: QueryXRaySource = 'query_metric_log';

/** Fields processes_history can always populate. */
const PROCESSES_HISTORY_MISSING: readonly string[] = [];

/**
 * Fields query_metric_log cannot populate faithfully.
 * Mirrors QUERY_METRIC_LOG_MISSING_FIELDS in queries/query-metric-log-queries.ts;
 * duplicated here so the types layer does not depend on the queries layer.
 */
const QUERY_METRIC_LOG_MISSING: readonly string[] = [
  'thread_count',
  'read_rows',
  'read_bytes',
  'written_rows',
  'd_read_rows',
  'd_read_mb',
  'd_written_rows',
];

export interface SelectQueryXRaySourceInput {
  availability: QueryXRaySourceAvailability;
  /** User override. Defaults to 'auto'. */
  preference?: QueryXRaySourcePreference;
  /** Lifecycle of the inspected query. Defaults to 'unknown' (treated as possibly running). */
  queryState?: XRayQueryState;
}

function describe(
  source: QueryXRaySource,
  reason: QueryXRaySourceReason,
  opts: { note?: string; live?: boolean } = {},
): QueryXRaySourceSelection {
  const { note } = opts;
  if (source === 'processes_history') {
    return {
      source,
      reason,
      missing: PROCESSES_HISTORY_MISSING,
      lagMs: 0,
      ...(note ? { note } : {}),
    };
  }
  return {
    source,
    reason,
    missing: QUERY_METRIC_LOG_MISSING,
    lagMs: SYSTEM_LOG_FLUSH_LAG_MS,
    ...(note ? { note } : {}),
  };
}

/**
 * Decide which table the X-Ray should read for one query.
 *
 * Rules, in order:
 *  1. An explicit override is honoured whenever that source exists. Both sources
 *     can serve a running query: query_metric_log writes rows throughout
 *     execution, and log_queries_min_type defaults to QUERY_START so the
 *     identity join finds the query before it finishes. The cost is the flush
 *     lag (reported as lagMs) and no clamp ceiling until the query ends.
 *  2. Otherwise the available source wins; when both are available,
 *     AUTO_PREFERRED_SOURCE decides for a finished query, and the sampler wins
 *     for a live one — it is fresher and carries thread counts and progress.
 */
export function selectQueryXRaySource(input: SelectQueryXRaySourceInput): QueryXRaySourceSelection {
  const { availability } = input;
  const preference = input.preference ?? 'auto';
  const queryState = input.queryState ?? 'unknown';
  const live = queryState !== 'finished';

  const liveNote = `This query has not finished: samples lag by up to ${
    (SYSTEM_LOG_FLUSH_LAG_MS / 1000).toFixed(1)
  }s and rates are unclamped until peak_threads_usage lands in query_log.`;

  if (!availability.processesHistory && !availability.queryMetricLog) {
    return {
      source: null,
      reason: 'auto',
      missing: [],
      lagMs: 0,
      note: 'No X-Ray source available: install the sampler (setup_sampling.sh) or enable system.query_metric_log (ClickHouse 24.10+).',
    };
  }

  if (preference === 'processes_history') {
    return availability.processesHistory
      ? describe('processes_history', 'override')
      : describe('query_metric_log', 'fallback', {
          live,
          note: 'tracehouse.processes_history is not installed on this connection; showing system.query_metric_log instead.',
        });
  }

  if (preference === 'query_metric_log') {
    return availability.queryMetricLog
      ? describe('query_metric_log', 'override', { live, ...(live ? { note: liveNote } : {}) })
      : describe('processes_history', 'fallback', {
          note: 'system.query_metric_log is not available on this connection; showing tracehouse.processes_history instead.',
        });
  }

  // Automatic selection.
  if (!availability.queryMetricLog) return describe('processes_history', 'only_available');
  if (!availability.processesHistory) {
    return describe('query_metric_log', 'only_available', { live, ...(live ? { note: liveNote } : {}) });
  }
  // Both exist: the sampler is strictly better for a query still in flight
  // (fresher, tighter clamp, real thread counts and progress counters).
  return live
    ? describe('processes_history', 'auto')
    : describe(AUTO_PREFERRED_SOURCE, 'auto');
}

/** Label for the source badge in the X-Ray header. */
export function xraySourceLabel(source: QueryXRaySource | null): string {
  switch (source) {
    case 'processes_history': return 'processes_history';
    case 'query_metric_log': return 'query_metric_log';
    default: return 'unavailable';
  }
}
