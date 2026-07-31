import type { OperationalEvent, TimeseriesPoint } from '@tracehouse/core';

export interface TimelineRange {
  startMs: number;
  endMs: number;
}

export type TimelineCoverage = TimelineRange;

const parsePointMs = (point: TimeseriesPoint): number => {
  const normalized = point.t.replace(' ', 'T')
    + (point.t.includes('Z') || point.t.includes('+') ? '' : 'Z');
  return new Date(normalized).getTime();
};

export function navigatorBucketSeconds(
  visibleSpanMs: number,
  targetPoints = 1_500,
): number {
  return Math.max(1, Math.ceil(visibleSpanMs / 1000 / targetPoints));
}

/** Pick a compact, readable percentage ceiling without clipping the data. */
export function navigatorPercentScaleCeiling(percent: number): number {
  const positive = Math.max(0, percent);
  const preferred = [1, 2, 5, 10, 25, 50, 75, 100, 125, 150, 175, 200];
  return preferred.find(value => value >= positive)
    ?? Math.ceil(positive / 50) * 50;
}

/** Pick a compact binary-byte ceiling that formats cleanly as KB/MB/GB/TB. */
export function navigatorByteScaleCeiling(bytes: number): number {
  const positive = Math.max(0, bytes);
  if (positive === 0) return 1;
  const unitPower = Math.max(0, Math.min(4, Math.floor(Math.log(positive) / Math.log(1024))));
  const unit = 1024 ** unitPower;
  const valueInUnit = positive / unit;
  const preferred = [1, 2, 5, 10, 25, 50, 75, 100, 125, 150, 175, 200, 250, 500, 750, 1024];
  const ceiling = preferred.find(value => value >= valueInUnit)
    ?? Math.ceil(valueInUnit / 1024) * 1024;
  return ceiling * unit;
}

export function navigatorChunkMs(visibleSpanMs: number): number {
  return Math.min(
    30 * 60_000,
    Math.max(5 * 60_000, Math.round(visibleSpanMs / 4)),
  );
}

export function navigatorChangePoints(
  points: TimeseriesPoint[],
  expectedBucketMs: number,
): TimeseriesPoint[] {
  const sorted = [...points].sort((left, right) => parsePointMs(left) - parsePointMs(right));
  return sorted.map((point, index) => {
    if (index === 0) return { ...point, v: 0 };
    const previous = sorted[index - 1];
    const elapsedMs = parsePointMs(point) - parsePointMs(previous);
    const contiguous = elapsedMs > 0 && elapsedMs <= expectedBucketMs * 2.5;
    return {
      t: point.t,
      v: contiguous ? point.v - previous.v : 0,
    };
  });
}

export function mergeTimelinePoints(
  current: TimeseriesPoint[],
  incoming: TimeseriesPoint[],
  bounds?: TimelineRange,
): TimeseriesPoint[] {
  const byTimestamp = new Map<number, TimeseriesPoint>();
  for (const point of [...current, ...incoming]) {
    const ms = parsePointMs(point);
    if (!Number.isFinite(ms)) continue;
    if (bounds && (ms < bounds.startMs || ms > bounds.endMs)) continue;
    byTimestamp.set(ms, point);
  }
  return [...byTimestamp.entries()]
    .sort(([left], [right]) => left - right)
    .map(([, point]) => point);
}

export function mergeTimelineEvents(
  current: OperationalEvent[],
  incoming: OperationalEvent[],
  bounds?: TimelineRange,
): OperationalEvent[] {
  const byId = new Map<string, OperationalEvent>();
  for (const event of [...current, ...incoming]) {
    const ms = new Date(event.occurred_at).getTime();
    if (!Number.isFinite(ms)) continue;
    if (bounds && (ms < bounds.startMs || ms > bounds.endMs)) continue;
    byId.set(event.id, event);
  }
  return [...byId.values()].sort(
    (left, right) => new Date(left.occurred_at).getTime() - new Date(right.occurred_at).getTime(),
  );
}

export function mergeCoverage(
  current: TimelineCoverage[],
  incoming: TimelineCoverage,
  bounds?: TimelineRange,
): TimelineCoverage[] {
  const clipped = [...current, incoming]
    .map(range => bounds
      ? {
          startMs: Math.max(range.startMs, bounds.startMs),
          endMs: Math.min(range.endMs, bounds.endMs),
        }
      : range)
    .filter(range => range.endMs > range.startMs)
    .sort((left, right) => left.startMs - right.startMs);

  const merged: TimelineCoverage[] = [];
  for (const range of clipped) {
    const previous = merged[merged.length - 1];
    if (!previous || range.startMs > previous.endMs + 1000) {
      merged.push({ ...range });
    } else {
      previous.endMs = Math.max(previous.endMs, range.endMs);
    }
  }
  return merged;
}

export function isRangeCovered(
  coverage: TimelineCoverage[],
  requested: TimelineRange,
): boolean {
  return coverage.some(
    range => range.startMs <= requested.startMs && range.endMs >= requested.endMs,
  );
}

export function uncoveredTimelineRanges(
  coverage: TimelineCoverage[],
  requested: TimelineRange,
): TimelineRange[] {
  const relevant = coverage
    .filter(range => range.endMs > requested.startMs && range.startMs < requested.endMs)
    .sort((left, right) => left.startMs - right.startMs);
  const uncovered: TimelineRange[] = [];
  let cursor = requested.startMs;

  for (const range of relevant) {
    if (range.startMs > cursor) {
      uncovered.push({
        startMs: cursor,
        endMs: Math.min(range.startMs, requested.endMs),
      });
    }
    cursor = Math.max(cursor, range.endMs);
    if (cursor >= requested.endMs) break;
  }
  if (cursor < requested.endMs) {
    uncovered.push({ startMs: cursor, endMs: requested.endMs });
  }
  return uncovered.filter(range => range.endMs > range.startMs);
}

export function panRangeToIncludeViewport(
  range: TimelineRange,
  viewport: TimelineRange,
  maxEndMs: number,
): TimelineRange {
  const spanMs = range.endMs - range.startMs;
  let startMs = range.startMs;
  let endMs = range.endMs;

  if (viewport.startMs < startMs) {
    const delta = viewport.startMs - startMs;
    startMs += delta;
    endMs += delta;
  } else if (viewport.endMs > endMs) {
    const delta = viewport.endMs - endMs;
    startMs += delta;
    endMs += delta;
  }

  if (endMs > maxEndMs) {
    endMs = maxEndMs;
    startMs = endMs - spanMs;
  }
  return { startMs, endMs };
}

export function navigatorCacheBounds(range: TimelineRange): TimelineRange {
  const spanMs = range.endMs - range.startMs;
  return {
    startMs: range.startMs - spanMs,
    endMs: range.endMs + spanMs,
  };
}

export function navigatorEdgeScrollVelocity(
  pointerX: number,
  containerWidth: number,
  rangeSpanMs: number,
  edgeZonePx = 36,
): number {
  const leftIntensity = Math.max(
    0,
    Math.min(2, (edgeZonePx - pointerX) / edgeZonePx),
  );
  const rightIntensity = Math.max(
    0,
    Math.min(2, (pointerX - (containerWidth - edgeZonePx)) / edgeZonePx),
  );
  // At full edge intensity, traverse the visible range in about nine seconds.
  // Pointer distance beyond the edge can still accelerate this up to 2x.
  return (rightIntensity - leftIntensity) * (rangeSpanMs / 9);
}

/**
 * Keep direct pointer deltas inside the navigator. Edge auto-scroll still uses
 * the raw pointer position, but off-screen distance must not accumulate and
 * then make the viewport jump when the pointer re-enters.
 */
export function clampNavigatorDragX(pointerX: number, containerWidth: number): number {
  return Math.max(0, Math.min(containerWidth, pointerX));
}
