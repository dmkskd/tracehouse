import type {
  EventSourceCapability,
  MonitoringCapability,
  OperationalEvent,
  EventCategory,
  EventKind,
  EventSeverity,
  EventSourceCoverage,
} from '@tracehouse/core';
import {
  EVENT_KIND_DEFINITIONS,
  EVENT_SOURCE_DEFINITIONS,
} from '@tracehouse/core';
import {
  EVENT_CATEGORY_LABELS,
  EVENT_KIND_LABELS,
} from './event-model';
import { formatBytes } from '../../utils/formatters';

export interface EventMarkerSelection {
  eventIds: ReadonlySet<string>;
  startMs: number;
  endMs: number;
}

export interface EventListCluster {
  id: string;
  events: OperationalEvent[];
  startMs: number;
  endMs: number;
}

export interface EventDashboardFilters {
  search: string;
  severity: 'all' | EventSeverity;
  category: 'all' | EventCategory;
  kind: 'all' | EventKind;
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

export type SupportedEventCategory = EventCategory | 'multiple';

export interface SupportedEventType {
  kind: EventKind;
  label: string;
  category: SupportedEventCategory;
  severity: EventSeverity;
  description: string;
  sources: readonly string[];
  capabilities: readonly EventSourceCapability[];
}

export type SupportedEventAvailability =
  | 'available'
  | 'partial'
  | 'unavailable'
  | 'failed';

export interface SupportedEventCoverage {
  availability: SupportedEventAvailability;
  availableSources: number;
  totalSources: number;
  label: string;
  warning?: string;
}

export interface SupportedEventGroup {
  category: SupportedEventCategory;
  label: string;
  events: SupportedEventType[];
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

export const EVENT_SOURCE_EXPLANATIONS = Object.fromEntries(
  EVENT_SOURCE_DEFINITIONS.map(source => [
    source.capability,
    {
      label: (source.coverageLabel ?? source.source)
        .replace(' (', ' · ')
        .replace(/\)$/, ''),
      description: source.description,
    },
  ]),
) as Readonly<Record<EventSourceCapability, EventSourceExplanation>>;

export function eventSourceExplanation(
  capability: string,
): EventSourceExplanation | undefined {
  return EVENT_SOURCE_EXPLANATIONS[
    capability as EventSourceCapability
  ];
}

export function eventSourceStatusDetail(
  source: EventSourceCoverage,
  capabilities: readonly MonitoringCapability[],
): string | undefined {
  if (source.status !== 'unavailable') return source.detail;
  return capabilities.find(capability => capability.id === source.capability)?.detail
    ?? source.detail;
}

/**
 * Event kinds currently emitted by EventsService.
 * Keep future taxonomy values out of this catalog until a source implements
 * them, so "supported" remains an accurate product claim.
 */
const supportedEventKinds = new Set<EventKind>(
  EVENT_SOURCE_DEFINITIONS.flatMap(source => [...source.kinds]),
);

export const SUPPORTED_EVENT_TYPES: readonly SupportedEventType[] = (
  Object.keys(EVENT_KIND_DEFINITIONS) as EventKind[]
)
  .filter(kind => supportedEventKinds.has(kind))
  .map(kind => {
    const definition = EVENT_KIND_DEFINITIONS[kind];
    const sources = EVENT_SOURCE_DEFINITIONS.filter(source =>
      source.kinds.includes(kind as never),
    );
    return {
      kind,
      label: definition.label,
      category: definition.categories.length === 1
        ? definition.categories[0]
        : 'multiple',
      severity: definition.severities[0],
      description: definition.description,
      sources: [...new Set(sources.map(source =>
        source.source.replace(/ \(.*\)$/, ''),
      ))],
      capabilities: [...new Set(sources.map(source => source.capability))],
    };
  });

export function supportedEventAvailability(
  eventType: SupportedEventType,
  coverage: readonly EventSourceCoverage[],
): SupportedEventAvailability {
  return supportedEventCoverage(eventType, coverage).availability;
}

export function supportedEventCoverage(
  eventType: SupportedEventType,
  coverage: readonly EventSourceCoverage[],
): SupportedEventCoverage {
  const statuses = eventType.capabilities.map(capability => (
    coverage.find(item => item.capability === capability)?.status
    ?? 'unavailable'
  ));
  const availableSources = statuses.filter(status => status === 'loaded').length;
  const failedSources = statuses.filter(status => status === 'failed').length;
  const totalSources = statuses.length;

  let availability: SupportedEventAvailability;
  if (availableSources === totalSources) availability = 'available';
  else if (availableSources > 0) availability = 'partial';
  else if (failedSources > 0) availability = 'failed';
  else availability = 'unavailable';

  let label: string;
  if (availability === 'partial' || (availability === 'available' && totalSources > 1)) {
    label = `${availableSources}/${totalSources} sources available`;
  } else if (availability === 'failed') {
    label = totalSources > 1
      ? `${failedSources}/${totalSources} source queries failed`
      : 'source query failed';
  } else {
    label = availability;
  }

  return {
    availability,
    availableSources,
    totalSources,
    label,
    warning: availability === 'partial' ? 'Events may be missing' : undefined,
  };
}

export function supportedEventGroups(): SupportedEventGroup[] {
  const categories: Array<{ category: SupportedEventCategory; label: string }> = [
    { category: 'lifecycle', label: 'Lifecycle' },
    { category: 'queries', label: 'Queries' },
    { category: 'merges', label: 'Merges' },
    { category: 'replication', label: 'Replication' },
    { category: 'coordination', label: 'Coordination' },
    { category: 'storage', label: 'Storage' },
    { category: 'changes', label: 'Changes' },
    { category: 'maintenance', label: 'Maintenance' },
    { category: 'multiple', label: 'Cross-category' },
  ];
  return categories
    .map(group => ({
      ...group,
      events: SUPPORTED_EVENT_TYPES.filter(event => event.category === group.category),
    }))
    .filter(group => group.events.length > 0);
}

export function toUtcEventInstant(date: Date): string {
  // Keep the absolute instant explicit until the core query builder renders it.
  return date.toISOString();
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

export function eventKindLabel(event: OperationalEvent): string {
  return EVENT_KIND_LABELS[event.kind] ?? event.kind.replaceAll('_', ' ');
}

export function eventDetailLabel(event: OperationalEvent): string {
  return EVENT_KIND_DEFINITIONS[event.kind].detailLabel;
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

export function eventDetailSections(event: OperationalEvent): EventDetailSection[] {
  const exception = event.exception_name
    ? `${event.exception_name}${event.exception_code != null ? ` (${event.exception_code})` : ''}`
    : event.exception_code != null ? String(event.exception_code) : undefined;
  const replicaState = event.kind === 'replica_readonly' || event.kind === 'replica_unavailable'
    ? event.ended_at ? 'Recovered' : 'Ongoing at range end'
    : undefined;
  const keeperEndpoint = event.keeper_host
    ? `${event.keeper_host}${event.keeper_port != null ? `:${event.keeper_port}` : ''}`
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
        detailRow('Status', event.status ?? event.connection_state),
        detailRow('Started at', event.started_at
          ? formatEventDateTime(event.started_at)
          : undefined),
        detailRow('Storage', event.storage_name, true),
        detailRow('Format', event.format),
        detailRow('Rows', event.rows),
        detailRow('Data', event.bytes != null ? formatBytes(event.bytes) : undefined),
        detailRow('Files', event.num_files),
        detailRow('Total size', event.total_size != null
          ? formatBytes(event.total_size)
          : undefined),
        detailRow('Keeper cluster', event.keeper_name, true),
        detailRow('Keeper node', keeperEndpoint, true),
        detailRow('Reason', event.reason),
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
      label: 'Identifiers',
      rows: compactRows([
        detailRow('Query ID', event.query_id, true),
        detailRow('Flush query ID', event.flush_query_id, true),
        detailRow('Initial query ID', event.initial_query_id, true),
        detailRow('Normalized hash', event.normalized_query_hash, true),
        detailRow('Operation ID', event.operation_id, true),
        detailRow('Keeper client ID', event.keeper_client_id, true),
      ]),
    },
  ];

  return sections.filter(section => section.rows.length > 0);
}

export function sortAndFilterEvents(
  events: readonly OperationalEvent[],
  filters: EventDashboardFilters,
): OperationalEvent[] {
  const needle = filters.search.trim().toLowerCase();
  return sortEventsDescending(events)
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

export function sortEventsDescending(events: readonly OperationalEvent[]): OperationalEvent[] {
  return [...events].sort(
    (a, b) => Date.parse(b.occurred_at) - Date.parse(a.occurred_at),
  );
}

export const EVENT_LIST_CLUSTER_WINDOW_MS = 5_000;

function eventListClusterSignature(event: OperationalEvent): string {
  return [
    event.kind,
    event.category,
    event.severity,
    event.title.trim().toLowerCase(),
    event.hostname ?? '',
    event.source,
  ].join('\u0000');
}

/**
 * Collapses visually repetitive bursts without changing the underlying event
 * model. A cluster is intentionally bounded by its first event so a steady
 * stream cannot chain into one unmanageably large row.
 */
export function clusterSimilarEvents(
  events: readonly OperationalEvent[],
  windowMs = EVENT_LIST_CLUSTER_WINDOW_MS,
): EventListCluster[] {
  const clusters: EventListCluster[] = [];

  for (const event of sortEventsDescending(events)) {
    const eventMs = Date.parse(event.occurred_at);
    const previous = clusters.at(-1);
    const primary = previous?.events[0];
    const canJoin = previous
      && primary
      && Number.isFinite(eventMs)
      && eventListClusterSignature(primary) === eventListClusterSignature(event)
      && previous.endMs - eventMs <= windowMs;

    if (canJoin) {
      previous.events.push(event);
      previous.startMs = eventMs;
      previous.id = `${previous.events[0].id}:${event.id}`;
      continue;
    }

    clusters.push({
      id: event.id,
      events: [event],
      startMs: Number.isFinite(eventMs) ? eventMs : 0,
      endMs: Number.isFinite(eventMs) ? eventMs : 0,
    });
  }

  return clusters;
}

export function observedEventKinds(events: readonly OperationalEvent[]): EventKind[] {
  return [...new Set(events.map(event => event.kind))]
    .sort((a, b) => (EVENT_KIND_LABELS[a] ?? a).localeCompare(EVENT_KIND_LABELS[b] ?? b));
}

export function selectEvent(
  displayedEvents: readonly OperationalEvent[],
  fallbackEvents: readonly OperationalEvent[],
  selectedEventId?: string,
  selectedEventTime?: string,
): OperationalEvent | undefined {
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
  events: readonly OperationalEvent[],
): EventSeverityCounts {
  return {
    critical: events.filter(event => event.severity === 'critical').length,
    error: events.filter(event => event.severity === 'error').length,
    warning: events.filter(event => event.severity === 'warning').length,
    info: events.filter(event => event.severity === 'info').length,
  };
}

export function buildEventMarkerSelection(
  events: readonly OperationalEvent[],
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
