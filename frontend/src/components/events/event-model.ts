import type {
  EventCategory,
  EventKind,
  EventSeverity,
  OperationalEvent,
} from '@tracehouse/core';
import {
  EVENT_CATEGORY_DEFINITIONS,
  EVENT_KIND_DEFINITIONS,
  EVENT_SEVERITIES,
} from '@tracehouse/core';

export const EVENT_SEVERITY_VALUES: readonly EventSeverity[] = EVENT_SEVERITIES;

export const EVENT_CATEGORIES = Object.keys(
  EVENT_CATEGORY_DEFINITIONS,
) as EventCategory[];

export const EVENT_SEVERITY_COLORS: Record<EventSeverity, string> = {
  critical: '#f85149',
  error: '#ff7a3d',
  warning: '#f2cc60',
  info: '#58a6ff',
};

export const EVENT_CATEGORY_LABELS = Object.fromEntries(
  Object.entries(EVENT_CATEGORY_DEFINITIONS).map(([category, definition]) => [
    category,
    definition.label,
  ]),
) as Record<EventCategory, string>;

export const EVENT_KIND_LABELS = Object.fromEntries(
  Object.entries(EVENT_KIND_DEFINITIONS).map(([kind, definition]) => [
    kind,
    definition.label,
  ]),
) as Record<EventKind, string>;

export const EVENT_KIND_SHORT_LABELS = Object.fromEntries(
  Object.entries(EVENT_KIND_DEFINITIONS).map(([kind, definition]) => [
    kind,
    definition.shortLabel,
  ]),
) as Record<EventKind, string>;

export interface EventFilter {
  hiddenSeverities: ReadonlySet<EventSeverity>;
  hiddenCategories: ReadonlySet<EventCategory>;
  hiddenKinds: ReadonlySet<EventKind>;
}

export function emptyEventFilter(): EventFilter {
  return {
    hiddenSeverities: new Set(),
    hiddenCategories: new Set(),
    hiddenKinds: new Set(),
  };
}

export function filterEvents(
  events: readonly OperationalEvent[],
  filter: EventFilter,
): OperationalEvent[] {
  return events.filter(event =>
    !filter.hiddenSeverities.has(event.severity)
    && !filter.hiddenCategories.has(event.category)
    && !filter.hiddenKinds.has(event.kind),
  );
}

const SEVERITY_RANK: Record<EventSeverity, number> = {
  critical: 4,
  error: 3,
  warning: 2,
  info: 1,
};

export interface EventCluster {
  id: string;
  occurredAtMs: number;
  events: OperationalEvent[];
  primaryEvent: OperationalEvent;
  severity: EventSeverity;
}

function shortEventLabel(event: OperationalEvent): string {
  if (event.kind === 'error_burst' && event.exception_name) {
    if (/KEEPER|ZOOKEEPER|REPLICA/.test(event.exception_name)) return 'Keeper';
    if (/CORRUPT|CHECKSUM|UNEXPECTED_DATA_PARTS/.test(event.exception_name)) return 'Corruption';
    if (/SPACE|FILE|FSYNC/.test(event.exception_name)) return 'Storage';
  }
  return EVENT_KIND_SHORT_LABELS[event.kind] ?? EVENT_KIND_LABELS[event.kind]
    ?? event.kind.replaceAll('_', ' ');
}

/**
 * A marker should explain what happened, not merely how many rows collided.
 * Only genuinely mixed clusters fall back to the generic “N events” label.
 */
export function eventClusterLabel(cluster: EventCluster): string {
  const labels = new Set(cluster.events.map(shortEventLabel));
  if (labels.size !== 1) return `${cluster.events.length} events`;
  const label = [...labels][0];
  return cluster.events.length > 1 ? `${label} ×${cluster.events.length}` : label;
}

/**
 * Groups only markers that would visually overlap. Source occurrences remain
 * untouched, so zooming in reveals the original cadence.
 */
export function clusterEvents(
  events: readonly OperationalEvent[],
  rangeStartMs: number,
  rangeEndMs: number,
  pixelWidth: number,
  minDistancePx = 14,
): EventCluster[] {
  const rangeMs = Math.max(1, rangeEndMs - rangeStartMs);
  const effectiveWidth = Math.max(1, pixelWidth);
  const collisionWindowMs = rangeMs * (minDistancePx / effectiveWidth);
  const visible = events
    .map(event => ({ event, ms: Date.parse(event.occurred_at) }))
    .filter(item =>
      Number.isFinite(item.ms)
      && item.ms >= rangeStartMs
      && item.ms <= rangeEndMs,
    )
    .sort((a, b) => a.ms - b.ms || a.event.id.localeCompare(b.event.id));

  const groups: Array<Array<{ event: OperationalEvent; ms: number }>> = [];
  for (const item of visible) {
    const current = groups[groups.length - 1];
    if (!current || item.ms - current[0].ms > collisionWindowMs) {
      groups.push([item]);
    } else {
      current.push(item);
    }
  }

  return groups.map(group => {
    const sortedBySeverity = [...group].sort((a, b) =>
      SEVERITY_RANK[b.event.severity] - SEVERITY_RANK[a.event.severity]
      || a.ms - b.ms
      || a.event.id.localeCompare(b.event.id),
    );
    const middle = group[Math.floor((group.length - 1) / 2)];
    const primaryEvent = sortedBySeverity[0].event;
    return {
      id: group.map(item => item.event.id).join('|'),
      occurredAtMs: middle.ms,
      events: sortedBySeverity.map(item => item.event),
      primaryEvent,
      severity: primaryEvent.severity,
    };
  });
}

export function eventFilterCount(filter: EventFilter): number {
  return filter.hiddenSeverities.size
    + filter.hiddenCategories.size
    + filter.hiddenKinds.size;
}

export function buildEventsUrl(
  event: OperationalEvent,
  rangeHours = 1,
): string {
  const params = new URLSearchParams({
    event_id: event.id,
    event_time: event.occurred_at,
    range_center: event.occurred_at,
    event_range: String(rangeHours),
    from: 'timetravel',
  });
  return `/events?${params.toString()}`;
}

/** @deprecated Use buildEventsUrl. Retained for external callers during migration. */
export const buildAnalyticsEventUrl = buildEventsUrl;

export function buildTimeTravelEventUrl(event: OperationalEvent): string {
  const params = new URLSearchParams({
    event_id: event.id,
    event_time: event.occurred_at,
    from: 'events',
  });
  return `/timetravel?${params.toString()}`;
}
