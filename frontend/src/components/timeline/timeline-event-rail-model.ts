import {
  eventKindName,
  observedEventFilterCategories,
  observedEventFilterKinds,
  severityPresetFilter,
  type TimelineEventCluster,
} from './timeline-event-model';

// Filter helpers live in the Events domain; these names remain for timeline callers.
export {
  toggleSetValue,
} from './timeline-event-model';
export const timelineEventKindLabel = eventKindName;
export const observedTimelineEventCategories = observedEventFilterCategories;
export const observedTimelineEventKinds = observedEventFilterKinds;
export const buildSeverityPresetFilter = severityPresetFilter;

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

export function timelineEventMarkerTitle(cluster: TimelineEventCluster): string {
  return cluster.events.length === 1
    ? `${cluster.primaryEvent.title}\n${
      formatTimelineEventTime(cluster.primaryEvent.occurred_at)
    }`
    : `${cluster.events.length} events\n${
      cluster.events.slice(0, 8).map(event => `• ${event.title}`).join('\n')
    }`;
}
