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

export function eventKindName(kind: EventKind): string {
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

/** Categories present in the loaded events, in taxonomy order: the filter only offers these. */
export function observedEventFilterCategories(
  events: readonly OperationalEvent[],
): EventCategory[] {
  return EVENT_CATEGORIES.filter(category =>
    events.some(event => event.category === category),
  );
}

/** Kinds present in the loaded events, sorted by label. */
export function observedEventFilterKinds(
  events: readonly OperationalEvent[],
): EventKind[] {
  return [...new Set(events.map(event => event.kind))]
    .sort((a, b) => eventKindName(a).localeCompare(eventKindName(b)));
}

/** A filter from lists of hidden values (URL parameters); values outside the taxonomy are dropped. */
export function eventFilterFromLists(lists: {
  severities?: readonly string[];
  categories?: readonly string[];
  kinds?: readonly string[];
}): EventFilter {
  const known = <T extends string>(values: readonly string[] | undefined, allowed: readonly T[]) =>
    new Set((values ?? []).filter((value): value is T => (allowed as readonly string[]).includes(value)));
  return {
    hiddenSeverities: known(lists.severities, EVENT_SEVERITY_VALUES),
    hiddenCategories: known(lists.categories, EVENT_CATEGORIES),
    hiddenKinds: known(lists.kinds, Object.keys(EVENT_KIND_DEFINITIONS) as EventKind[]),
  };
}

/** What a filter checkbox shows: everything shown, nothing shown, or part of it. */
export type EventFilterCheckState = 'checked' | 'unchecked' | 'partial';

/**
 * Categories each kind appears in among the loaded events. Usually one, but
 * some detectors classify per event (error_burst spans several categories).
 */
function categoriesByKind(events: readonly OperationalEvent[]): Map<EventKind, Set<EventCategory>> {
  const byKind = new Map<EventKind, Set<EventCategory>>();
  for (const event of events) {
    const categories = byKind.get(event.kind) ?? new Set<EventCategory>();
    categories.add(event.category);
    byKind.set(event.kind, categories);
  }
  return byKind;
}

/**
 * A kind is shown only where its category is shown, so its checkbox follows
 * the category: unchecked when hidden itself or when all its categories are
 * hidden, partial when only some are.
 */
export function eventKindCheckState(
  kind: EventKind,
  filter: EventFilter,
  events: readonly OperationalEvent[],
): EventFilterCheckState {
  if (filter.hiddenKinds.has(kind)) return 'unchecked';
  const categories = [...(categoriesByKind(events).get(kind) ?? [])];
  const shown = categories.filter(category => !filter.hiddenCategories.has(category)).length;
  if (categories.length > 0 && shown === 0) return 'unchecked';
  return shown < categories.length ? 'partial' : 'checked';
}

/** A category is partial when some of its kinds are hidden. */
export function eventCategoryCheckState(
  category: EventCategory,
  filter: EventFilter,
  events: readonly OperationalEvent[],
): EventFilterCheckState {
  if (filter.hiddenCategories.has(category)) return 'unchecked';
  const kinds = [...new Set(events.filter(event => event.category === category).map(event => event.kind))];
  const hidden = kinds.filter(kind => filter.hiddenKinds.has(kind)).length;
  if (kinds.length > 0 && hidden === kinds.length) return 'unchecked';
  return hidden > 0 ? 'partial' : 'checked';
}

/** Checked hides the category; partial or unchecked shows it with all its kinds. */
export function toggleEventCategory(
  filter: EventFilter,
  category: EventCategory,
  events: readonly OperationalEvent[],
): EventFilter {
  if (eventCategoryCheckState(category, filter, events) === 'checked') {
    return { ...filter, hiddenCategories: toggleSetValue(filter.hiddenCategories, category) };
  }
  const kinds = new Set(events.filter(event => event.category === category).map(event => event.kind));
  const hiddenCategories = new Set(filter.hiddenCategories);
  hiddenCategories.delete(category);
  return {
    ...filter,
    hiddenCategories,
    hiddenKinds: new Set([...filter.hiddenKinds].filter(kind => !kinds.has(kind))),
  };
}

/**
 * Checked hides the kind. Partial or unchecked shows it everywhere it occurs:
 * hidden categories it belongs to come back, but only for this kind, so the
 * other kinds in those categories stay hidden.
 */
export function toggleEventKind(
  filter: EventFilter,
  kind: EventKind,
  events: readonly OperationalEvent[],
): EventFilter {
  if (eventKindCheckState(kind, filter, events) === 'checked') {
    return { ...filter, hiddenKinds: toggleSetValue(filter.hiddenKinds, kind) };
  }
  const byKind = categoriesByKind(events);
  const reopened = [...(byKind.get(kind) ?? [])].filter(category => filter.hiddenCategories.has(category));
  const hiddenCategories = new Set(filter.hiddenCategories);
  const hiddenKinds = new Set(filter.hiddenKinds);
  hiddenKinds.delete(kind);
  for (const category of reopened) {
    hiddenCategories.delete(category);
    for (const [other, categories] of byKind) {
      if (other === kind || !categories.has(category)) continue;
      // Kinds that were fully hidden by the category stay hidden.
      if (eventKindCheckState(other, filter, events) === 'unchecked') hiddenKinds.add(other);
    }
  }
  return { ...filter, hiddenCategories, hiddenKinds };
}

/** Section-wide "all" / "none" for severities. */
export function setAllEventSeverities(filter: EventFilter, visible: boolean): EventFilter {
  return { ...filter, hiddenSeverities: new Set(visible ? [] : EVENT_SEVERITY_VALUES) };
}

/** Section-wide "all" / "none" for categories; "all" also brings back their kinds. */
export function setAllEventCategories(
  filter: EventFilter,
  visible: boolean,
  events: readonly OperationalEvent[],
): EventFilter {
  return visible
    ? { ...filter, hiddenCategories: new Set(), hiddenKinds: new Set() }
    : { ...filter, hiddenCategories: new Set(observedEventFilterCategories(events)) };
}

/** Section-wide "all" / "none" for kinds; "all" also brings back hidden categories. */
export function setAllEventKinds(
  filter: EventFilter,
  visible: boolean,
  events: readonly OperationalEvent[],
): EventFilter {
  return visible
    ? { ...filter, hiddenCategories: new Set(), hiddenKinds: new Set() }
    : { ...filter, hiddenKinds: new Set(observedEventFilterKinds(events)) };
}

/** "Only" on a severity row: every other severity is hidden. */
export function onlyEventSeverity(filter: EventFilter, severity: EventSeverity): EventFilter {
  return { ...filter, hiddenSeverities: new Set(EVENT_SEVERITY_VALUES.filter(value => value !== severity)) };
}

/** "Only" on a category row: every other category is hidden and this one shows all its kinds. */
export function onlyEventCategory(
  filter: EventFilter,
  category: EventCategory,
  events: readonly OperationalEvent[],
): EventFilter {
  const kinds = new Set(events.filter(event => event.category === category).map(event => event.kind));
  return {
    ...filter,
    hiddenCategories: new Set(observedEventFilterCategories(events).filter(value => value !== category)),
    hiddenKinds: new Set([...filter.hiddenKinds].filter(kind => !kinds.has(kind))),
  };
}

/** "Only" on a kind row: every other kind is hidden and this kind's categories are shown. */
export function onlyEventKind(
  filter: EventFilter,
  kind: EventKind,
  events: readonly OperationalEvent[],
): EventFilter {
  const categories = categoriesByKind(events).get(kind) ?? new Set<EventCategory>();
  return {
    ...filter,
    hiddenCategories: new Set([...filter.hiddenCategories].filter(category => !categories.has(category))),
    hiddenKinds: new Set(observedEventFilterKinds(events).filter(value => value !== kind)),
  };
}

export function severityPresetFilter(
  visible: ReadonlySet<EventSeverity>,
): EventFilter {
  return {
    hiddenSeverities: new Set(
      EVENT_SEVERITY_VALUES.filter(severity => !visible.has(severity)),
    ),
    hiddenCategories: new Set(),
    hiddenKinds: new Set(),
  };
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
/** Events page centered on an event, opened from Analytics; the back link travels in navigation state. */
export function buildAnalyticsEventUrl(
  event: OperationalEvent,
  rangeHours = 1,
): string {
  const params = new URLSearchParams({
    event_id: event.id,
    event_time: event.occurred_at,
    range_center: event.occurred_at,
    event_range: String(rangeHours),
  });
  return `/events?${params.toString()}`;
}

export function buildTimeTravelEventUrl(event: OperationalEvent): string {
  const params = new URLSearchParams({
    event_id: event.id,
    event_time: event.occurred_at,
    from: 'events',
  });
  return `/timetravel?${params.toString()}`;
}
