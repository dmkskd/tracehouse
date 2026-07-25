import type {
  TimelineEvent,
  TimelineEventCategory,
  TimelineEventKind,
  TimelineEventSeverity,
} from '@tracehouse/core';

export const TIMELINE_EVENT_SEVERITIES: readonly TimelineEventSeverity[] = [
  'critical',
  'error',
  'warning',
  'info',
];

export const TIMELINE_EVENT_CATEGORIES: readonly TimelineEventCategory[] = [
  'lifecycle',
  'queries',
  'replication',
  'coordination',
  'storage',
  'changes',
  'maintenance',
];

export const EVENT_SEVERITY_COLORS: Record<TimelineEventSeverity, string> = {
  critical: '#f85149',
  error: '#f0883e',
  warning: '#d29922',
  info: '#58a6ff',
};

export const EVENT_CATEGORY_LABELS: Record<TimelineEventCategory, string> = {
  lifecycle: 'Lifecycle',
  queries: 'Queries',
  replication: 'Replication',
  storage: 'Storage',
  coordination: 'Coordination',
  changes: 'Changes',
  maintenance: 'Maintenance',
};

export const EVENT_KIND_LABELS: Partial<Record<TimelineEventKind, string>> = {
  server_restart: 'Server restart',
  server_crash: 'Server crash',
  query_oom: 'Query OOM',
  query_rejected: 'Query rejected',
  query_timeout: 'Query timeout',
  query_resource_limit: 'Query resource limit',
  query_failure: 'Query failure',
  replica_readonly: 'Replica read-only',
  replica_unavailable: 'Replica unavailable',
  replication_data_loss: 'Replication data loss',
  replication_task_failure: 'Replication task failure',
  part_failure: 'Part failure',
  background_task_failure: 'Background task failure',
  error_burst: 'Operational error burst',
  ddl: 'DDL',
  keeper_connection: 'Keeper connection',
  backup: 'Backup / restore',
  async_insert_failure: 'Async insert failure',
  server_log: 'Server log',
};

export const EVENT_KIND_SHORT_LABELS: Partial<Record<TimelineEventKind, string>> = {
  server_restart: 'Restart',
  server_crash: 'Crash',
  query_oom: 'OOM',
  query_rejected: 'Rejected',
  query_timeout: 'Timeout',
  query_resource_limit: 'Resource limit',
  query_failure: 'Query failure',
  replica_readonly: 'Read-only',
  replica_unavailable: 'Unavailable',
  replication_data_loss: 'Data loss',
  replication_task_failure: 'Task failure',
  part_failure: 'Part failure',
  background_task_failure: 'Background failure',
  error_burst: 'Operational error',
  ddl: 'DDL',
  keeper_connection: 'Keeper',
  backup: 'Backup',
  async_insert_failure: 'Async insert',
  server_log: 'Server log',
};

export interface TimelineEventFilter {
  hiddenSeverities: ReadonlySet<TimelineEventSeverity>;
  hiddenCategories: ReadonlySet<TimelineEventCategory>;
  hiddenKinds: ReadonlySet<TimelineEventKind>;
}

export function emptyTimelineEventFilter(): TimelineEventFilter {
  return {
    hiddenSeverities: new Set(),
    hiddenCategories: new Set(),
    hiddenKinds: new Set(),
  };
}

export function filterTimelineEvents(
  events: readonly TimelineEvent[],
  filter: TimelineEventFilter,
): TimelineEvent[] {
  return events.filter(event =>
    !filter.hiddenSeverities.has(event.severity)
    && !filter.hiddenCategories.has(event.category)
    && !filter.hiddenKinds.has(event.kind),
  );
}

const SEVERITY_RANK: Record<TimelineEventSeverity, number> = {
  critical: 4,
  error: 3,
  warning: 2,
  info: 1,
};

export interface TimelineEventCluster {
  id: string;
  occurredAtMs: number;
  events: TimelineEvent[];
  primaryEvent: TimelineEvent;
  severity: TimelineEventSeverity;
}

function shortEventLabel(event: TimelineEvent): string {
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
export function timelineEventClusterLabel(cluster: TimelineEventCluster): string {
  const labels = new Set(cluster.events.map(shortEventLabel));
  if (labels.size !== 1) return `${cluster.events.length} events`;
  const label = [...labels][0];
  return cluster.events.length > 1 ? `${label} ×${cluster.events.length}` : label;
}

/**
 * Groups only markers that would visually overlap. Source occurrences remain
 * untouched, so zooming in reveals the original cadence.
 */
export function clusterTimelineEvents(
  events: readonly TimelineEvent[],
  rangeStartMs: number,
  rangeEndMs: number,
  pixelWidth: number,
  minDistancePx = 14,
): TimelineEventCluster[] {
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

  const groups: Array<Array<{ event: TimelineEvent; ms: number }>> = [];
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
      || a.ms - b.ms,
    );
    const middle = group[Math.floor((group.length - 1) / 2)];
    const primaryEvent = sortedBySeverity[0].event;
    return {
      id: group.map(item => item.event.id).join('|'),
      occurredAtMs: middle.ms,
      events: group.map(item => item.event),
      primaryEvent,
      severity: primaryEvent.severity,
    };
  });
}

export function timelineEventFilterCount(filter: TimelineEventFilter): number {
  return filter.hiddenSeverities.size
    + filter.hiddenCategories.size
    + filter.hiddenKinds.size;
}

export function buildEventsUrl(
  event: TimelineEvent,
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

export function buildTimeTravelEventUrl(event: TimelineEvent): string {
  const params = new URLSearchParams({
    event_id: event.id,
    event_time: event.occurred_at,
    from: 'events',
  });
  return `/timetravel?${params.toString()}`;
}
