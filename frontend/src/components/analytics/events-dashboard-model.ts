import type {
  TimelineEvent,
  TimelineEventCategory,
  TimelineEventKind,
  TimelineEventSeverity,
} from '@tracehouse/core';
import {
  EVENT_CATEGORY_LABELS,
  EVENT_KIND_LABELS,
} from '../timeline/timeline-event-model';

export interface EventMarkerSelection {
  eventIds: ReadonlySet<string>;
  startMs: number;
  endMs: number;
}

export interface EventDashboardFilters {
  search: string;
  severity: 'all' | TimelineEventSeverity;
  category: 'all' | TimelineEventCategory;
  kind: 'all' | TimelineEventKind;
}

export interface EventSeverityCounts {
  critical: number;
  error: number;
  warning: number;
  info: number;
}

export interface EventSourceExplanation {
  label: string;
  description: string;
}

export const EVENT_SOURCE_EXPLANATIONS: Record<string, EventSourceExplanation> = {
  query_log: {
    label: 'system.query_log',
    description: 'Query OOMs, timeouts, resource/admission failures, and successful DDL.',
  },
  asynchronous_metric_log: {
    label: 'system.asynchronous_metric_log',
    description: 'Server restarts inferred from persisted Uptime resets.',
  },
  crash_log: {
    label: 'system.crash_log',
    description: 'Fatal ClickHouse process crashes, signals, versions, and related query IDs.',
  },
  part_log: {
    label: 'system.part_log',
    description: 'Failed data-part operations and their table, part, disk, and error context.',
  },
  background_schedule_pool_log: {
    label: 'system.background_schedule_pool_log',
    description: 'Failures from scheduled ClickHouse background work.',
  },
  error_log: {
    label: 'system.error_log',
    description: 'Operational error bursts classified into replication, Keeper, storage, or maintenance.',
  },
  metric_log_replication_state: {
    label: 'system.metric_log · replica state',
    description: 'Host-level episodes where one or more replicated tables were read-only.',
  },
  metric_log_replication_failures: {
    label: 'system.metric_log · replication failures',
    description: 'Persisted data-loss, failed-fetch, and failed-part-check counters.',
  },
};

export function toClickHouseEventTime(date: Date): string {
  return date.toISOString().slice(0, 19).replace('T', ' ');
}

export function formatEventDateTime(value: string): string {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString([], {
    year: 'numeric',
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    fractionalSecondDigits: 3,
  });
}

export function formatEventClusterRange(startMs: number, endMs: number): string {
  const start = new Date(startMs);
  const end = new Date(endMs);
  const time = (date: Date) => date.toLocaleTimeString([], {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
  if (start.toDateString() === end.toDateString()) {
    return `${start.toLocaleDateString([], {
      day: '2-digit',
      month: 'short',
      year: 'numeric',
    })}, ${time(start)}–${time(end)}`;
  }
  return `${formatEventDateTime(start.toISOString())}–${
    formatEventDateTime(end.toISOString())
  }`;
}

export function eventKindLabel(event: TimelineEvent): string {
  return EVENT_KIND_LABELS[event.kind] ?? event.kind.replaceAll('_', ' ');
}

export function eventDetailRows(event: TimelineEvent): Array<[string, string]> {
  const rows: Array<[string, string]> = [
    ['Severity', event.severity],
    ['Event ID', event.id],
    ['Ended', event.ended_at ? formatEventDateTime(event.ended_at) : '—'],
    ['State', event.kind === 'replica_readonly' || event.kind === 'replica_unavailable'
      ? event.ended_at ? 'recovered' : 'ongoing at range end'
      : '—'],
    ['Category', EVENT_CATEGORY_LABELS[event.category]],
    ['Event type', eventKindLabel(event)],
    ['Precision', event.precision],
    ['Host', event.hostname ?? '—'],
    ['Source', event.source],
    ['Occurrences', event.count != null ? String(event.count) : '1'],
    ['Query ID', event.query_id ?? '—'],
    ['Initial query ID', event.initial_query_id ?? '—'],
    ['Normalized hash', event.normalized_query_hash ?? '—'],
    ['User', event.user ?? '—'],
    ['Query kind', event.query_kind ?? '—'],
    ['Database', event.database ?? event.databases?.join(', ') ?? '—'],
    ['Table', event.table ?? event.tables?.join(', ') ?? '—'],
    ['Part', event.part_name ?? '—'],
    ['Operation', event.operation ?? event.task_name ?? '—'],
    ['Disk', event.disk_name ?? '—'],
    ['Exception', event.exception_name
      ? `${event.exception_name}${event.exception_code != null ? ` (${event.exception_code})` : ''}`
      : event.exception_code != null ? String(event.exception_code) : '—'],
    ['Duration', event.duration_ms != null ? `${event.duration_ms.toLocaleString()} ms` : '—'],
    ['Memory', event.memory_usage != null ? `${event.memory_usage.toLocaleString()} bytes` : '—'],
    ['Server version', event.version ?? '—'],
    ['Signal', event.signal != null ? String(event.signal) : '—'],
    ['Remote error', event.remote != null ? (event.remote ? 'yes' : 'no') : '—'],
  ];
  return rows.filter(([, value]) => value !== '—');
}

export function sortAndFilterEvents(
  events: readonly TimelineEvent[],
  filters: EventDashboardFilters,
): TimelineEvent[] {
  const needle = filters.search.trim().toLowerCase();
  return sortTimelineEvents(events)
    .filter(event => {
      if (filters.severity !== 'all' && event.severity !== filters.severity) return false;
      if (filters.category !== 'all' && event.category !== filters.category) return false;
      if (filters.kind !== 'all' && event.kind !== filters.kind) return false;
      if (!needle) return true;
      return [
        event.title,
        event.detail,
        event.kind,
        event.category,
        event.hostname,
        event.source,
        event.query_id,
        event.normalized_query_hash,
        event.exception_name,
        event.database,
        event.table,
        event.query,
      ].some(value => String(value ?? '').toLowerCase().includes(needle));
    });
}

export function sortTimelineEvents(events: readonly TimelineEvent[]): TimelineEvent[] {
  return [...events].sort(
    (a, b) => Date.parse(b.occurred_at) - Date.parse(a.occurred_at),
  );
}

export function observedEventKinds(events: readonly TimelineEvent[]): TimelineEventKind[] {
  return [...new Set(events.map(event => event.kind))]
    .sort((a, b) => (EVENT_KIND_LABELS[a] ?? a).localeCompare(EVENT_KIND_LABELS[b] ?? b));
}

export function selectTimelineEvent(
  displayedEvents: readonly TimelineEvent[],
  fallbackEvents: readonly TimelineEvent[],
  selectedEventId?: string,
  selectedEventTime?: string,
): TimelineEvent | undefined {
  const exact = displayedEvents.find(event => event.id === selectedEventId);
  if (exact) return exact;
  if (selectedEventTime && displayedEvents.length > 0) {
    const target = Date.parse(selectedEventTime);
    return [...displayedEvents].sort(
      (a, b) => Math.abs(Date.parse(a.occurred_at) - target)
        - Math.abs(Date.parse(b.occurred_at) - target),
    )[0];
  }
  return displayedEvents[0] ?? fallbackEvents[0];
}

export function countEventSeverities(
  events: readonly TimelineEvent[],
): EventSeverityCounts {
  return {
    critical: events.filter(event => event.severity === 'critical').length,
    error: events.filter(event => event.severity === 'error').length,
    warning: events.filter(event => event.severity === 'warning').length,
    info: events.filter(event => event.severity === 'info').length,
  };
}

export function buildEventMarkerSelection(
  events: readonly TimelineEvent[],
  fallbackMs: number,
): EventMarkerSelection {
  const eventTimes = events
    .map(event => Date.parse(event.occurred_at))
    .filter(Number.isFinite);
  return {
    eventIds: new Set(events.map(event => event.id)),
    startMs: eventTimes.length > 0 ? Math.min(...eventTimes) : fallbackMs,
    endMs: eventTimes.length > 0 ? Math.max(...eventTimes) : fallbackMs,
  };
}
