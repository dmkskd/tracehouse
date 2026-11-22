import type { TimelineTracking } from '../types/timeline.js';

/** URL parameters of Time Travel tracking. `nqh` predates the others and stays as is. */
export const TRACKING_PARAMS = { queryHash: 'nqh', user: 'tt_user', table: 'tt_table' } as const;
export type TrackingDimension = keyof typeof TRACKING_PARAMS;

/** URL parameters that stop with a tracked dimension. A link that tracks a user also lists that
 * user's queries that failed before start (tt_failed_start, see object browser links). */
export function trackingParamsFor(dimension: TrackingDimension): string[] {
  return [TRACKING_PARAMS[dimension], ...(dimension === 'user' ? ['tt_failed_start'] : [])];
}

/** The set dimensions of `tracking`, or null when nothing is tracked. */
export function activeTracking(tracking: TimelineTracking | null | undefined): TimelineTracking | null {
  if (!tracking) return null;
  const active = Object.fromEntries(Object.entries(tracking).filter(([, value]) => typeof value === 'string' && value !== '')) as TimelineTracking;
  return Object.keys(active).length ? active : null;
}

/** Merges and mutations carry a table only: they are tracked when the table alone is tracked. */
export function tracksParts(tracking: TimelineTracking | null): string | null {
  return tracking?.table && !tracking.user && !tracking.queryHash ? tracking.table : null;
}

export function readTracking(get: (name: string) => string | null): TimelineTracking | null {
  return activeTracking({ queryHash: get(TRACKING_PARAMS.queryHash) ?? undefined, user: get(TRACKING_PARAMS.user) ?? undefined,
    table: get(TRACKING_PARAMS.table) ?? undefined });
}

/** Label and value of each tracked dimension, in display order. */
export function describeTracking(tracking: TimelineTracking | null): { dimension: TrackingDimension; label: string; value: string }[] {
  const labels: Record<TrackingDimension, string> = { user: 'user', table: 'table', queryHash: 'query hash' };
  return (['user', 'table', 'queryHash'] as const).flatMap(dimension => {
    const value = tracking?.[dimension];
    return value ? [{ dimension, label: labels[dimension], value }] : [];
  });
}
