import { hasExplicitOffset, parseAbsoluteTimestamp } from './absolute-timestamp.js';

/** An absolute time range in epoch milliseconds. */
export interface TimeWindow {
  startMs: number;
  endMs: number;
}

/**
 * Epoch milliseconds for each x-axis label, or null when the axis is not time.
 *
 * Only explicitly zoned labels qualify: Analytics returns typed DateTime
 * columns as UTC ISO strings, while unzoned strings may be labels or local
 * calendar values and must stay on a label axis. Values must strictly
 * increase, because a time axis draws rows in time order.
 */
export function timeAxisValues(labels: readonly string[]): number[] | null {
  if (labels.length === 0) return null;
  const values: number[] = [];
  for (const label of labels) {
    if (!hasExplicitOffset(label)) return null;
    const ms = parseAbsoluteTimestamp(label);
    if (!Number.isFinite(ms)) return null;
    if (values.length > 0 && ms <= values[values.length - 1]) return null;
    values.push(ms);
  }
  return values;
}

const INTERVAL_UNIT_MS: Record<string, number> = {
  SECOND: 1000,
  MINUTE: 60_000,
  HOUR: 3_600_000,
  DAY: 86_400_000,
  WEEK: 7 * 86_400_000,
};

/** Length of a relative ClickHouse interval such as '15 MINUTE' or '7 DAY'. */
export function relativeIntervalMs(interval: string): number | null {
  const match = /^(\d+)\s+(SECOND|MINUTE|HOUR|DAY|WEEK)S?$/i.exec(interval.trim());
  if (!match) return null;
  const amount = Number(match[1]);
  if (amount <= 0) return null;
  return amount * INTERVAL_UNIT_MS[match[2].toUpperCase()];
}

/**
 * The window a dashboard shows: an absolute range when one is given,
 * otherwise the relative interval ending at nowMs (default: now). Null when
 * neither describes a window.
 */
export function resolveDashboardWindow(options: {
  interval: string | null;
  absolute: { startTime: string; endTime: string } | null;
  nowMs?: number;
}): TimeWindow | null {
  const nowMs = options.nowMs ?? Date.now();
  if (options.absolute) {
    const startMs = parseAbsoluteTimestamp(options.absolute.startTime);
    const endMs = parseAbsoluteTimestamp(options.absolute.endTime);
    return Number.isFinite(startMs) && Number.isFinite(endMs) && endMs > startMs
      ? { startMs, endMs }
      : null;
  }
  if (!options.interval) return null;
  const length = relativeIntervalMs(options.interval);
  return length === null ? null : { startMs: nowMs - length, endMs: nowMs };
}

/**
 * The x range of a time-axis chart: the dashboard window when there is one,
 * widened to include any rows outside it, otherwise the rows' own extent.
 */
export function timeAxisDomain(values: readonly number[], window: TimeWindow | null): [number, number] {
  const first = values[0];
  const last = values[values.length - 1];
  return window
    ? [Math.min(window.startMs, first), Math.max(window.endMs, last)]
    : [first, last];
}

const TICK_STEPS_MS = [
  1_000, 5_000, 15_000, 30_000,
  60_000, 5 * 60_000, 10 * 60_000, 15 * 60_000, 30 * 60_000,
  3_600_000, 3 * 3_600_000, 6 * 3_600_000, 12 * 3_600_000,
  86_400_000, 2 * 86_400_000, 7 * 86_400_000,
];

/** Ticks on round times (whole minutes, hours, days) with at most maxTicks inside the domain. */
export function timeAxisTicks(domain: readonly [number, number], maxTicks = 6): number[] {
  const [start, end] = domain;
  if (!(end > start) || maxTicks < 1) return [];
  const span = end - start;
  const step = TICK_STEPS_MS.find(candidate => span / candidate <= maxTicks)
    ?? Math.ceil(span / maxTicks / 86_400_000) * 86_400_000;
  const ticks: number[] = [];
  for (let tick = Math.ceil(start / step) * step; tick <= end; tick += step) ticks.push(tick);
  return ticks;
}

/** Shortest zoom range. Most dashboard queries bucket by the minute, so a narrower range shows no more detail. */
export const MIN_ZOOM_SPAN_MS = 60_000;

/**
 * The range selected by dragging across a time axis from anchorMs to currentMs,
 * in either direction, clamped to the axis domain and widened to whole seconds.
 * Null when the selection is shorter than minSpanMs.
 */
export function zoomSelection(
  anchorMs: number,
  currentMs: number,
  domain: readonly [number, number],
  minSpanMs = MIN_ZOOM_SPAN_MS,
): TimeWindow | null {
  if (!Number.isFinite(anchorMs) || !Number.isFinite(currentMs)) return null;
  const [domainStart, domainEnd] = domain;
  const startMs = Math.floor(Math.max(domainStart, Math.min(anchorMs, currentMs)) / 1000) * 1000;
  const endMs = Math.ceil(Math.min(domainEnd, Math.max(anchorMs, currentMs)) / 1000) * 1000;
  return endMs - startMs >= minSpanMs ? { startMs, endMs } : null;
}
