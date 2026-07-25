import { describe, expect, it } from 'vitest';
import type {
  TimelineEvent,
  TimelineEventCategory,
  TimelineEventKind,
  TimelineEventSeverity,
} from '@tracehouse/core';
import {
  clusterTimelineEvents,
  buildEventsUrl,
  buildTimeTravelEventUrl,
  emptyTimelineEventFilter,
  filterTimelineEvents,
  timelineEventClusterLabel,
} from '../timeline-event-model';

function event(
  id: string,
  occurredAt: string,
  severity: TimelineEventSeverity = 'info',
  category: TimelineEventCategory = 'changes',
  kind: TimelineEventKind = 'ddl',
): TimelineEvent {
  return {
    id,
    occurred_at: occurredAt,
    kind,
    category,
    severity,
    precision: 'exact',
    title: id,
    source: 'system.query_log',
    capability: 'query_log',
  };
}

describe('timeline event filtering', () => {
  it('can independently hide severity, category, and kind dimensions', () => {
    const events = [
      event('ddl', '2026-07-25T12:00:00.000Z'),
      event('oom', '2026-07-25T12:01:00.000Z', 'error', 'queries', 'query_oom'),
      event('restart', '2026-07-25T12:02:00.000Z', 'warning', 'lifecycle', 'server_restart'),
    ];

    expect(filterTimelineEvents(events, {
      hiddenSeverities: new Set(['warning']),
      hiddenCategories: new Set(['changes']),
      hiddenKinds: new Set(['query_timeout']),
    }).map(item => item.id)).toEqual(['oom']);
  });

  it('shows newly introduced event values by default', () => {
    const filter = emptyTimelineEventFilter();
    expect(filterTimelineEvents([
      event('future', '2026-07-25T12:00:00.000Z', 'info', 'maintenance', 'backup'),
    ], filter)).toHaveLength(1);
  });
});

describe('timeline event display clustering', () => {
  it('clusters only events close enough to collide at the current width', () => {
    const events = [
      event('first', '2026-07-25T12:00:00.000Z', 'info'),
      event('nearby', '2026-07-25T12:00:00.500Z', 'critical', 'lifecycle', 'server_crash'),
      event('later', '2026-07-25T12:00:20.000Z', 'warning', 'lifecycle', 'server_restart'),
    ];
    const start = Date.parse('2026-07-25T12:00:00.000Z');
    const end = Date.parse('2026-07-25T12:01:00.000Z');

    const clusters = clusterTimelineEvents(events, start, end, 600, 14);

    expect(clusters).toHaveLength(2);
    expect(clusters[0].events.map(item => item.id)).toEqual(['first', 'nearby']);
    expect(clusters[0].primaryEvent.id).toBe('nearby');
    expect(clusters[0].severity).toBe('critical');
    expect(clusters[1].events.map(item => item.id)).toEqual(['later']);
  });

  it('drops invalid and out-of-range timestamps without changing source data', () => {
    const events = [
      event('before', '2026-07-25T11:59:59.000Z'),
      event('inside', '2026-07-25T12:00:30.000Z'),
      event('invalid', 'not-a-time'),
    ];
    const start = Date.parse('2026-07-25T12:00:00.000Z');
    const end = Date.parse('2026-07-25T12:01:00.000Z');

    expect(clusterTimelineEvents(events, start, end, 600)).toEqual([
      expect.objectContaining({
        events: [expect.objectContaining({ id: 'inside' })],
      }),
    ]);
    expect(events).toHaveLength(3);
  });

  it('labels homogeneous clusters by meaning and mixed clusters generically', () => {
    const start = Date.parse('2026-07-25T12:00:00.000Z');
    const end = Date.parse('2026-07-25T12:01:00.000Z');
    const oomClusters = clusterTimelineEvents([
      event('oom-1', '2026-07-25T12:00:10.000Z', 'error', 'queries', 'query_oom'),
      event('oom-2', '2026-07-25T12:00:10.100Z', 'error', 'queries', 'query_oom'),
    ], start, end, 600);
    const mixedClusters = clusterTimelineEvents([
      event('oom', '2026-07-25T12:00:10.000Z', 'error', 'queries', 'query_oom'),
      event('ddl', '2026-07-25T12:00:10.100Z', 'info', 'changes', 'ddl'),
    ], start, end, 600);

    expect(timelineEventClusterLabel(oomClusters[0])).toBe('OOM ×2');
    expect(timelineEventClusterLabel(mixedClusters[0])).toBe('2 events');
  });

  it('builds encoded links between Time Travel and the Events page', () => {
    const sourceEvent = event(
      'query_log:host:12:00:oom/id',
      '2026-07-25T12:00:10.125Z',
      'error',
      'queries',
      'query_oom',
    );

    const url = buildEventsUrl(sourceEvent, 6);
    const params = new URLSearchParams(url.slice(url.indexOf('?') + 1));

    expect(url.startsWith('/events?')).toBe(true);
    expect(params.get('event_id')).toBe(sourceEvent.id);
    expect(params.get('event_time')).toBe(sourceEvent.occurred_at);
    expect(params.get('range_center')).toBe(sourceEvent.occurred_at);
    expect(params.get('event_range')).toBe('6');
    expect(params.get('from')).toBe('timetravel');

    const timeTravelUrl = buildTimeTravelEventUrl(sourceEvent);
    const timeTravelParams = new URLSearchParams(
      timeTravelUrl.slice(timeTravelUrl.indexOf('?') + 1),
    );
    expect(timeTravelUrl.startsWith('/timetravel?')).toBe(true);
    expect(timeTravelParams.get('event_id')).toBe(sourceEvent.id);
    expect(timeTravelParams.get('event_time')).toBe(sourceEvent.occurred_at);
  });
});
