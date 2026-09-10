/**
 * Presentation constants and helpers for the time breakdown.
 *
 * Split from TimeBreakdownBar so components and plain helpers do not share a
 * module: the Distributed timeline needs the colours and the gradient without
 * pulling in a component, and mixing the two breaks fast refresh.
 */

import type { TimeBreakdown, TimeBreakdownKey, ThreadTimeContext } from '@tracehouse/core';

/** Work reads warm, waits read cool, the unknown reads grey. */
export const SEGMENT_COLORS: Record<TimeBreakdownKey, string> = {
  cpu: '#d29922',
  disk_wait: '#7B83FF',
  cpu_wait: '#B682FF',
  network_wait: '#FF6692',
  unaccounted: '#6e7681',
};

// Kept short: these read as one line each in the popover, next to the counter
// they come from.
export const SEGMENT_HINTS: Record<TimeBreakdownKey, string> = {
  cpu: 'working',
  disk_wait: 'blocked on disk',
  cpu_wait: 'waiting for a free CPU',
  network_wait: 'blocked on a socket',
  // RealTimeMicroseconds is thread *lifetime*, not busy time, so an idle-but-
  // alive thread lands here. On short queries that is usually plain
  // over-parallelisation - threads started, then left with no work - and only
  // sometimes genuine lock contention. The two are indistinguishable in this
  // counter, so the hint names the state and the panel's thread-time line
  // carries the parallelism figure that separates them.
  //
  // A distributed initiator's wait on its shards may or may not be metered as
  // network_wait, depending on version (26.8.2 counts it, 26.7.1 did not).
  // See the module header of core's time-breakdown.ts. Either way its idle
  // sibling threads land here, so the wording keeps shards.
  unaccounted: 'thread alive but blocked — waiting on shards, pipeline, or locks',
};

/**
 * The one line that makes the shares above it readable: what the denominator
 * was, and how far it exceeds the elapsed time.
 *
 * Written for the case that sends people to this panel confused - a short
 * distributed query showing 79% Parked. With "23 threads · 335ms thread time /
 * 49ms wall · 6.8x" underneath, the segment stops looking like a measurement
 * gap and starts reading as what it is: threads alive with nothing to do.
 *
 * Every part is optional because callers know different things. The flow
 * diagram has no thread count, so it says the rest and omits that word rather
 * than printing a zero.
 */
export function threadTimeNote(context: ThreadTimeContext | undefined): string | undefined {
  if (!context) return undefined;
  const parts: string[] = [];
  if (context.threads) parts.push(`${context.threads} threads`);
  const threadMs = Math.round(context.threadTimeUs / 1000);
  parts.push(context.wallClockMs != null
    ? `${threadMs}ms thread time / ${Math.round(context.wallClockMs)}ms wall`
    : `${threadMs}ms thread time`);
  // Below ~1.5x the ratio says nothing a reader needs; above it, it is the
  // whole explanation for a wide residual.
  if (context.parallelism != null && context.parallelism >= 1.5) {
    parts.push(`${context.parallelism.toFixed(1)}x parallel`);
  }
  return parts.join(' · ');
}

export const pct = (share: number) => `${(share * 100).toFixed(share < 0.1 ? 1 : 0)}%`;

/**
 * Parked share below which the deeper layers are neither fetched nor shown.
 *
 * Low deliberately: if the bar renders a Parked segment at all, the panel
 * should be willing to explain it. A higher floor produced the confusing state
 * of showing a 9.4% segment while silently declining to say anything about it.
 * The floor exists only to skip two log-table scans for residuals too small to
 * be worth a query.
 *
 * Shared with the caller so the fetch condition and the display condition
 * cannot drift apart — the panel must never advertise a layer never queried.
 */
export const PARKED_EXPLANATION_THRESHOLD = 0.02;

/**
 * The composition as a CSS gradient, for filling a bar whose geometry is
 * already owned by something else — the Distributed timeline positions and
 * sizes its bars by wall clock, so the composition can only be the paint.
 *
 * Returns undefined when there is nothing to compose, so callers can fall back
 * to their solid colour rather than render an empty bar.
 */
export function timeBreakdownGradient(
  breakdown: TimeBreakdown,
  /**
   * 'horizontal' for a standalone composition bar.
   *
   * 'vertical' whenever the bar sits on a time axis — the Distributed Gantt.
   * Segments laid out left-to-right there read as a sequence ("CPU, then queue,
   * then parked") because the axis beneath them is time, but this is a
   * composition and the parts have no order. Stacking them across the bar's
   * height keeps horizontal meaning wall clock and gives the composition its
   * own, non-temporal direction.
   */
  direction: 'horizontal' | 'vertical' = 'horizontal',
): string | undefined {
  if (!breakdown.available || breakdown.segments.length === 0) return undefined;
  const stops: string[] = [];
  let cursor = 0;
  for (const segment of breakdown.segments) {
    const start = cursor * 100;
    cursor += segment.share;
    stops.push(`${SEGMENT_COLORS[segment.key]} ${start}%`, `${SEGMENT_COLORS[segment.key]} ${cursor * 100}%`);
  }
  return `linear-gradient(${direction === 'vertical' ? '180deg' : '90deg'}, ${stops.join(', ')})`;
}

/** Legend text for a breakdown, e.g. "cpu 27% · queue 40% · parked 33%". */
export function timeBreakdownSummary(breakdown: TimeBreakdown): string {
  return breakdown.segments.map(s => `${s.label.toLowerCase()} ${pct(s.share)}`).join(' · ');
}

/** Compact duration for per-stage wait totals, which span µs to minutes. */
export function fmtWait(us: number): string {
  const seconds = us / 1_000_000;
  if (seconds >= 60) return `${(seconds / 60).toFixed(1)}m`;
  if (seconds >= 1) return `${seconds.toFixed(1)}s`;
  return `${Math.round(us / 1000)}ms`;
}

