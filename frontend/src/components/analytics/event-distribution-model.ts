import type { TimelineEvent, TimelineEventCategory } from '@tracehouse/core';
import {
  EVENT_CATEGORY_LABELS,
  TIMELINE_EVENT_CATEGORIES,
} from '../timeline/timeline-event-model';

export const EVENT_DISTRIBUTION_LAYOUT = {
  labelWidth: 124,
  rightPadding: 14,
  topPadding: 10,
  activeLaneHeight: 30,
  quietLaneHeight: 19,
  axisHeight: 25,
} as const;

export const EVENT_CATEGORY_COLORS: Record<TimelineEventCategory, string> = {
  lifecycle: '#d29922',
  queries: '#f0883e',
  replication: '#a371f7',
  coordination: '#db61a2',
  storage: '#39c5cf',
  changes: '#58a6ff',
  maintenance: '#8b949e',
};

export const EVENT_CATEGORY_SYMBOLS: Record<TimelineEventCategory, string> = {
  lifecycle: '↻',
  queries: '⌁',
  replication: '⇄',
  coordination: '◇',
  storage: '▱',
  changes: 'Δ',
  maintenance: '⚙',
};

export type EventMarkerShape = 'circle' | 'diamond' | 'square' | 'triangle';

export interface EventDistributionLane {
  category: TimelineEventCategory;
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

export function eventMarkerShape(event: TimelineEvent): EventMarkerShape {
  if (event.kind === 'server_restart') return 'diamond';
  if (event.kind === 'server_crash' || event.kind === 'query_timeout') {
    return 'triangle';
  }
  if (event.kind === 'query_oom' || event.kind === 'query_rejected') {
    return 'square';
  }
  return 'circle';
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
  events: readonly TimelineEvent[],
): Map<TimelineEventCategory, TimelineEvent[]> {
  const grouped = new Map<TimelineEventCategory, TimelineEvent[]>(
    TIMELINE_EVENT_CATEGORIES.map(category => [category, []]),
  );
  for (const event of events) grouped.get(event.category)?.push(event);
  return grouped;
}

export function buildEventDistributionLanes(
  byCategory: ReadonlyMap<TimelineEventCategory, readonly TimelineEvent[]>,
): EventDistributionLane[] {
  const {
    activeLaneHeight,
    quietLaneHeight,
    topPadding,
  } = EVENT_DISTRIBUTION_LAYOUT;
  const laneHeights = TIMELINE_EVENT_CATEGORIES.map(category =>
    (byCategory.get(category)?.length ?? 0) > 0
      ? activeLaneHeight
      : quietLaneHeight);
  return TIMELINE_EVENT_CATEGORIES.map((category, index) => {
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

export function isTimelineStateEpisode(event: TimelineEvent): boolean {
  const end = Date.parse(event.ended_at ?? '');
  return event.kind === 'replica_readonly'
    || event.kind === 'replica_unavailable'
    || (Number.isFinite(end) && end > Date.parse(event.occurred_at));
}

export function buildEventHoverCardModel(
  events: readonly TimelineEvent[],
  primaryEvent: TimelineEvent,
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
