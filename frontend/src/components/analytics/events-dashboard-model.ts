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
import { formatBytes } from '../../utils/formatters';

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

export interface EventDetailRow {
  label: string;
  value: string;
  monospace?: boolean;
}

export interface EventDetailSection {
  id: string;
  label: string;
  rows: EventDetailRow[];
}

export const EVENT_SOURCE_EXPLANATIONS: Record<string, EventSourceExplanation> = {
  query_log: {
    label: 'system.query_log',
    description: 'Query OOMs, timeouts, resource/admission failures, and successful DDL.',
  },
  asynchronous_metric_log: {
    label: 'system.asynchronous_metric_log',
    description: 'Server restarts inferred when persisted Uptime moves backwards; startup time is estimated as sample time minus Uptime.',
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

export function eventDetailLabel(event: TimelineEvent): string {
  switch (event.kind) {
    case 'server_restart':
    case 'replica_readonly':
    case 'replica_unavailable':
      return 'Detection method';
    case 'server_crash':
      return 'Crash details';
    case 'query_oom':
    case 'query_timeout':
    case 'query_rejected':
    case 'query_resource_limit':
    case 'query_failure':
    case 'part_failure':
      return 'ClickHouse error';
    case 'ddl':
      return 'Statement details';
    case 'replication_data_loss':
    case 'replication_task_failure':
      return 'Replication details';
    case 'keeper_connection':
      return 'Keeper details';
    default:
      return event.precision === 'inferred' ? 'Detection method' : 'Recorded details';
  }
}

function formatMetricValue(value: number, unit?: string): string {
  const formatted = value.toLocaleString(undefined, {
    maximumFractionDigits: 3,
  });
  return unit ? `${formatted} ${unit}` : formatted;
}

function detailRow(
  label: string,
  value: string | number | undefined,
  monospace = false,
): EventDetailRow | null {
  if (value === undefined || value === '') return null;
  return { label, value: String(value), monospace };
}

function compactRows(rows: Array<EventDetailRow | null>): EventDetailRow[] {
  return rows.filter((row): row is EventDetailRow => row !== null);
}

export function eventDetailSections(event: TimelineEvent): EventDetailSection[] {
  const exception = event.exception_name
    ? `${event.exception_name}${event.exception_code != null ? ` (${event.exception_code})` : ''}`
    : event.exception_code != null ? String(event.exception_code) : undefined;
  const replicaState = event.kind === 'replica_readonly' || event.kind === 'replica_unavailable'
    ? event.ended_at ? 'Recovered' : 'Ongoing at range end'
    : undefined;

  const sections: EventDetailSection[] = [
    {
      id: 'metadata',
      label: 'Event metadata',
      rows: compactRows([
        detailRow('Type', eventKindLabel(event)),
        detailRow('Category', EVENT_CATEGORY_LABELS[event.category]),
        detailRow('Source', event.source, true),
        detailRow('Precision', event.precision),
        detailRow('Occurrences', event.count != null ? event.count : 1),
      ]),
    },
    {
      id: 'details',
      label: 'Event details',
      rows: compactRows([
        detailRow('Host', event.hostname, true),
        detailRow('User', event.user),
        detailRow('Database', event.database ?? event.databases?.join(', '), true),
        detailRow('Table', event.table ?? event.tables?.join(', '), true),
        detailRow('Part', event.part_name, true),
        detailRow('Disk', event.disk_name, true),
        detailRow('Query kind', event.query_kind),
        detailRow('Operation', event.operation ?? event.task_name),
        detailRow('Duration', event.duration_ms != null
          ? `${event.duration_ms.toLocaleString()} ms`
          : undefined),
        detailRow('Memory', event.memory_usage != null
          ? formatBytes(event.memory_usage)
          : undefined),
        detailRow('Exception', exception, true),
        detailRow('Remote error', event.remote != null
          ? event.remote ? 'Yes' : 'No'
          : undefined),
        detailRow('Signal', event.signal),
        detailRow('Server version', event.version, true),
        detailRow('State', replicaState),
        detailRow('Ended at', event.ended_at ? formatEventDateTime(event.ended_at) : undefined),
      ]),
    },
    {
      id: 'detection',
      label: 'Detection',
      rows: compactRows([
        detailRow('Observed at', event.observed_at
          ? formatEventDateTime(event.observed_at)
          : undefined),
        detailRow('Metric', event.metric_name, true),
        detailRow('Previous sample', event.previous_metric_value != null
          ? formatMetricValue(event.previous_metric_value, event.metric_unit)
          : undefined),
        detailRow('Detected sample', event.metric_value != null
          ? formatMetricValue(event.metric_value, event.metric_unit)
          : undefined),
      ]),
    },
    {
      id: 'identifiers',
      label: 'Query identifiers',
      rows: compactRows([
        detailRow('Query ID', event.query_id, true),
        detailRow('Initial query ID', event.initial_query_id, true),
        detailRow('Normalized hash', event.normalized_query_hash, true),
      ]),
    },
  ];

  return sections.filter(section => section.rows.length > 0);
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
