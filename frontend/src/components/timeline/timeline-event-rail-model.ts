import type {
  TimelineEvent,
  TimelineEventCategory,
  TimelineEventKind,
  TimelineEventSeverity,
} from '@tracehouse/core';
import {
  EVENT_KIND_LABELS,
  TIMELINE_EVENT_CATEGORIES,
  TIMELINE_EVENT_SEVERITIES,
  type TimelineEventCluster,
  type TimelineEventFilter,
} from './timeline-event-model';

export function timelineEventKindLabel(kind: TimelineEventKind): string {
  return EVENT_KIND_LABELS[kind] ?? kind.replaceAll('_', ' ');
}

export function toggleSetValue<T>(
  values: ReadonlySet<T>,
  value: T,
): ReadonlySet<T> {
  const next = new Set(values);
  if (next.has(value)) next.delete(value);
  else next.add(value);
  return next;
}

export function formatTimelineEventTime(value: string): string {
  const date = new Date(value);
  return Number.isNaN(date.getTime())
    ? value
    : date.toLocaleTimeString([], {
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        fractionalSecondDigits: 3,
      });
}

export function observedTimelineEventCategories(
  events: readonly TimelineEvent[],
): TimelineEventCategory[] {
  return TIMELINE_EVENT_CATEGORIES.filter(category =>
    events.some(event => event.category === category),
  );
}

export function observedTimelineEventKinds(
  events: readonly TimelineEvent[],
): TimelineEventKind[] {
  return [...new Set(events.map(event => event.kind))]
    .sort((a, b) => timelineEventKindLabel(a).localeCompare(timelineEventKindLabel(b)));
}

export function buildSeverityPresetFilter(
  visible: ReadonlySet<TimelineEventSeverity>,
): TimelineEventFilter {
  return {
    hiddenSeverities: new Set(
      TIMELINE_EVENT_SEVERITIES.filter(severity => !visible.has(severity)),
    ),
    hiddenCategories: new Set(),
    hiddenKinds: new Set(),
  };
}

export function timelineEventMarkerTitle(cluster: TimelineEventCluster): string {
  return cluster.events.length === 1
    ? `${cluster.primaryEvent.title}\n${
      formatTimelineEventTime(cluster.primaryEvent.occurred_at)
    }`
    : `${cluster.events.length} events\n${
      cluster.events.slice(0, 8).map(event => `• ${event.title}`).join('\n')
    }`;
}
