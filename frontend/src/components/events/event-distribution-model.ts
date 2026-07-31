import type { OperationalEvent, EventCategory } from '@tracehouse/core';
import {
  EVENT_CATEGORY_LABELS,
  EVENT_CATEGORIES,
} from './event-model';

export const EVENT_DISTRIBUTION_LAYOUT = {
  labelWidth: 132,
  rightPadding: 16,
  topPadding: 38,
  activeLaneHeight: 30,
  quietLaneHeight: 19,
  focusedLaneHeight: 68,
  axisHeight: 8,
} as const;

export const EVENT_CLUSTER_MARKER_SIZE = {
  singletonRadius: 4.5,
  minimumClusterRadius: 7,
  maximumRadius: 12,
  countAtMaximum: 32,
} as const;

export const EVENT_CATEGORY_COLORS: Record<EventCategory, string> = {
  lifecycle: '#d29922',
  queries: '#f0883e',
  merges: '#3fb950',
  replication: '#a371f7',
  coordination: '#db61a2',
  storage: '#39c5cf',
  changes: '#58a6ff',
  maintenance: '#8b949e',
};

export const EVENT_CATEGORY_SYMBOLS: Record<EventCategory, string> = {
  lifecycle: '↻',
  queries: '⌁',
  merges: '⇉',
  replication: '⇄',
  coordination: '◇',
  storage: '▱',
  changes: 'Δ',
  maintenance: '⚙',
};

export interface EventDistributionLane {
  category: EventCategory;
  eventCount: number;
  laneHeight: number;
  yTop: number;
  y: number;
}

export interface EventHoverCardModel {
  categoryLabel: string;
  severityLabel: string;
  title: string;
  timeLabel: string;
  hostsLabel?: string;
  distinctTitles: string[];
  detail?: string;
  actionLabel: string;
}

/**
 * Encode count by circle area rather than radius. This keeps small clusters
 * legible while compressing large bursts into a bounded visual range.
 */
export function eventClusterMarkerRadius(eventCount: number): number {
  const {
    singletonRadius,
    minimumClusterRadius,
    maximumRadius,
    countAtMaximum,
  } = EVENT_CLUSTER_MARKER_SIZE;
  if (eventCount <= 1) return singletonRadius;

  const boundedCount = Math.min(Math.max(eventCount, 2), countAtMaximum);
  const progress = (boundedCount - 2) / (countAtMaximum - 2);
  const minimumArea = minimumClusterRadius ** 2;
  const maximumArea = maximumRadius ** 2;
  return Math.sqrt(minimumArea + progress * (maximumArea - minimumArea));
}

export function formatEventDistributionTick(ms: number, spanMs: number): string {
  const date = new Date(ms);
  if (spanMs > 36 * 60 * 60 * 1000) {
    return date.toLocaleString([], {
      month: 'short',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
    });
  }
  if (spanMs >= 12 * 60 * 60 * 1000) {
    return date.toLocaleString([], {
      day: '2-digit',
      month: 'short',
      hour: '2-digit',
      minute: '2-digit',
    });
  }
  return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
}

export function formatEventHoverTime(value: string): string {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString([], {
    day: '2-digit',
    month: 'short',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
}

export function groupEventsByCategory(
  events: readonly OperationalEvent[],
): Map<EventCategory, OperationalEvent[]> {
  const grouped = new Map<EventCategory, OperationalEvent[]>(
    EVENT_CATEGORIES.map(category => [category, []]),
  );
  for (const event of events) grouped.get(event.category)?.push(event);
  return grouped;
}

export function buildEventDistributionLanes(
  byCategory: ReadonlyMap<EventCategory, readonly OperationalEvent[]>,
  focusedCategory?: EventCategory,
): EventDistributionLane[] {
  const {
    activeLaneHeight,
    quietLaneHeight,
    focusedLaneHeight,
    topPadding,
  } = EVENT_DISTRIBUTION_LAYOUT;
  const categories = focusedCategory
    ? [focusedCategory]
    : EVENT_CATEGORIES;
  const laneHeights = categories.map(category =>
    focusedCategory
      ? focusedLaneHeight
      : (byCategory.get(category)?.length ?? 0) > 0
        ? activeLaneHeight
        : quietLaneHeight);
  return categories.map((category, index) => {
    const eventCount = byCategory.get(category)?.length ?? 0;
    const laneHeight = laneHeights[index];
    const yTop = topPadding + laneHeights
      .slice(0, index)
      .reduce((sum, height) => sum + height, 0);
    return {
      category,
      eventCount,
      laneHeight,
      yTop,
      y: yTop + laneHeight / 2,
    };
  });
}

export function isStateEpisode(event: OperationalEvent): boolean {
  const end = Date.parse(event.ended_at ?? '');
  return event.kind === 'replica_readonly'
    || event.kind === 'replica_unavailable'
    || (Number.isFinite(end) && end > Date.parse(event.occurred_at));
}

export function buildEventHoverCardModel(
  events: readonly OperationalEvent[],
  primaryEvent: OperationalEvent,
  clusterLabel: string,
): EventHoverCardModel {
  const eventTimes = events
    .map(event => Date.parse(event.occurred_at))
    .filter(Number.isFinite);
  const firstTime = eventTimes.length > 0 ? Math.min(...eventTimes) : Number.NaN;
  const lastTime = eventTimes.length > 0 ? Math.max(...eventTimes) : Number.NaN;
  const timeLabel = Number.isFinite(firstTime)
    ? firstTime === lastTime
      ? formatEventHoverTime(new Date(firstTime).toISOString())
      : `${formatEventHoverTime(new Date(firstTime).toISOString())} – ${
        formatEventHoverTime(new Date(lastTime).toISOString())
      }`
    : primaryEvent.occurred_at;
  const hosts = [...new Set(events.map(event => event.hostname).filter(Boolean))];

  return {
    categoryLabel: EVENT_CATEGORY_LABELS[primaryEvent.category],
    severityLabel: primaryEvent.severity,
    title: events.length > 1 ? clusterLabel : primaryEvent.title,
    timeLabel,
    hostsLabel: hosts.length === 0
      ? undefined
      : hosts.length === 1
        ? hosts[0]
        : `${hosts.length} hosts`,
    distinctTitles: [...new Set(events.map(event => event.title))],
    detail: events.length === 1 ? primaryEvent.detail : undefined,
    actionLabel: events.length > 1 ? 'Click to inspect these events' : 'Click to inspect event details',
  };
}
