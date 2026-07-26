/**
 * Timeline-specific event projection.
 *
 * Generic event taxonomy, filtering, clustering, and links belong to the
 * Events domain. Re-exports remain here for compatibility with timeline
 * components while dependencies flow from Timeline to Events.
 */
export * from '../events/event-model';
import {
  EVENT_CATEGORIES,
  EVENT_SEVERITY_VALUES,
  clusterEvents,
  emptyEventFilter,
  eventClusterLabel,
  eventFilterCount,
  filterEvents,
  type EventCluster,
  type EventFilter,
} from '../events/event-model';

export const TIMELINE_EVENT_SEVERITIES = EVENT_SEVERITY_VALUES;
export const TIMELINE_EVENT_CATEGORIES = EVENT_CATEGORIES;
export type TimelineEventFilter = EventFilter;
export const emptyTimelineEventFilter = emptyEventFilter;
export const filterTimelineEvents = filterEvents;
export const timelineEventFilterCount = eventFilterCount;
export type TimelineEventCluster = EventCluster;
export const clusterTimelineEvents = clusterEvents;
export const timelineEventClusterLabel = eventClusterLabel;

export function buildTimelineNavigatorRequestScope({
  activeMetric,
  navigatorHours,
  hostname,
  activityLimit,
  eventCapabilities,
}: {
  activeMetric: string;
  navigatorHours: number;
  hostname?: string | null;
  activityLimit: number;
  eventCapabilities?: readonly string[];
}): string {
  return [
    activeMetric,
    navigatorHours,
    hostname ?? 'all-hosts',
    activityLimit,
    [...(eventCapabilities ?? [])].sort().join(','),
  ].join('_');
}
