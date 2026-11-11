import type { OperationalEvent, TimeWindow } from '@tracehouse/core';
import { clusterEvents, type EventCluster } from './event-model';
import { isStateEpisode } from './event-distribution-model';

/** A state episode drawn as a band, clipped to the visible window. */
export interface EventBand {
  event: OperationalEvent;
  startMs: number;
  endMs: number;
  /** No recovery observed: the band runs to the window end with no closing edge. */
  open: boolean;
  /** Started before the window: the band starts at the left edge with no opening edge. */
  startsBefore: boolean;
}

export interface EventLayerMarks {
  points: EventCluster[];
  bands: EventBand[];
}

/**
 * Splits events into point markers and episode bands for one chart.
 * Callers filter first (EventFilter); every event passed in is drawn.
 * Points are clustered by pixel distance over the window, which holds because
 * the chart's time axis is linear in milliseconds.
 */
export function buildEventLayerMarks(
  events: readonly OperationalEvent[],
  window: TimeWindow,
  pixelWidth: number,
): EventLayerMarks {
  const points: OperationalEvent[] = [];
  const bands: EventBand[] = [];
  for (const event of events) {
    if (!isStateEpisode(event)) {
      points.push(event);
      continue;
    }
    const startMs = Date.parse(event.started_at ?? event.occurred_at);
    const endMs = event.ended_at ? Date.parse(event.ended_at) : window.endMs;
    if (!Number.isFinite(startMs) || !Number.isFinite(endMs)) continue;
    if (endMs < window.startMs || startMs > window.endMs) continue;
    bands.push({
      event,
      startMs: Math.max(startMs, window.startMs),
      endMs: Math.min(endMs, window.endMs),
      open: !event.ended_at,
      startsBefore: startMs < window.startMs,
    });
  }
  return {
    points: clusterEvents(points, window.startMs, window.endMs, pixelWidth),
    bands,
  };
}

/** Hover text for an episode band. */
export function eventBandTitle(band: EventBand): string {
  return [band.event.title, band.event.hostname, band.event.detail].filter(Boolean).join('\n');
}
