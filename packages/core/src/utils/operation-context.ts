/**
 * Concurrency context for one Time Travel operation.
 *
 * A base ClickHouse install records only lifetime totals per query or merge —
 * start, end, and how much it consumed — so there is no second-by-second shape
 * to draw. What can always be answered is the question that makes someone pin a
 * spike in the first place: what else was running at that moment, and how much
 * of the load was this one operation?
 */
import type { OperationMetricMode, OperationRow } from './timeline-operations.js';

export interface OperationContext {
  /** Fraction (0..1) of the metric across everything in view that this operation holds. */
  metricShare: number;
  /** Fraction (0..1) of the concurrent operations' metric that this operation holds. */
  concurrentShare: number;
  /** Metric summed over the selected operation and everything overlapping it. */
  concurrentMetricTotal: number;
  /** Operations that overlapped this one, biggest first by metric. */
  neighbors: OperationRow[];
  /** How many operations overlapped in total, before the display limit. */
  neighborCount: number;
  /** Rank of this operation among the overlapping set, 1 = largest. */
  rank: number;
}

const EMPTY: OperationContext = {
  metricShare: 0,
  concurrentShare: 0,
  concurrentMetricTotal: 0,
  neighbors: [],
  neighborCount: 0,
  rank: 1,
};

/** Two operations overlap when their intervals touch at all. */
export function operationsOverlap(a: OperationRow, b: OperationRow): boolean {
  return a.startMs <= b.endMs && a.endMs >= b.startMs;
}

function isSame(a: OperationRow, b: OperationRow): boolean {
  return a.kind === b.kind && a.id === b.id && (a.hostname ?? '') === (b.hostname ?? '');
}

/**
 * Rank the selected operation against everything that ran alongside it.
 *
 * @param rows - Every operation in view, unfiltered by kind, so the answer
 *   covers merges competing with queries rather than one tab's worth.
 * @param limit - How many neighbors to return for display.
 */
export function buildOperationContext(
  selected: OperationRow | null,
  rows: readonly OperationRow[],
  limit = 5,
): OperationContext {
  if (!selected) return EMPTY;

  let viewTotal = 0;
  const neighbors: OperationRow[] = [];
  for (const row of rows) {
    viewTotal += row.metricValue;
    if (isSame(row, selected)) continue;
    if (operationsOverlap(row, selected)) neighbors.push(row);
  }
  neighbors.sort((a, b) => b.metricValue - a.metricValue);

  const neighborMetric = neighbors.reduce((sum, row) => sum + row.metricValue, 0);
  const concurrentMetricTotal = neighborMetric + selected.metricValue;
  const rank = neighbors.filter(row => row.metricValue > selected.metricValue).length + 1;

  return {
    metricShare: viewTotal > 0 ? selected.metricValue / viewTotal : 0,
    concurrentShare: concurrentMetricTotal > 0 ? selected.metricValue / concurrentMetricTotal : 0,
    concurrentMetricTotal,
    neighbors: neighbors.slice(0, limit),
    neighborCount: neighbors.length,
    rank,
  };
}

export interface OperationSpan {
  /** Position of the operation's start within the window, 0..1. */
  startFraction: number;
  /** Position of its end within the window, 0..1. */
  endFraction: number;
  /** Position of the pin within the window, or null when nothing is pinned or it falls outside. */
  pinFraction: number | null;
  /** True when the operation began before the window opened. */
  clippedStart: boolean;
  /** True when it was still running when the window closed. */
  clippedEnd: boolean;
}

/** Where an operation sits inside the visible window, for the interval bar. */
export function buildOperationSpan(
  row: OperationRow,
  windowStartMs: number,
  windowEndMs: number,
  pinnedMs: number | null,
): OperationSpan {
  const span = Math.max(1, windowEndMs - windowStartMs);
  const clamp = (ms: number) => Math.min(1, Math.max(0, (ms - windowStartMs) / span));
  return {
    startFraction: clamp(row.startMs),
    endFraction: clamp(row.endMs),
    pinFraction: pinnedMs !== null && pinnedMs >= windowStartMs && pinnedMs <= windowEndMs
      ? clamp(pinnedMs)
      : null,
    clippedStart: row.startMs < windowStartMs,
    clippedEnd: row.endMs > windowEndMs,
  };
}

/** Metric mode is carried through so callers can label the share consistently. */
export type OperationContextMode = OperationMetricMode;
