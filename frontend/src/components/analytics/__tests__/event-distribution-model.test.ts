import { describe, expect, it } from 'vitest';
import type { TimelineEvent } from '@tracehouse/core';
import {
  buildEventDistributionLanes,
  buildEventHoverCardModel,
  eventMarkerShape,
  groupEventsByCategory,
} from '../event-distribution-model';

function event(
  overrides: Partial<TimelineEvent> & Pick<TimelineEvent, 'id' | 'occurred_at' | 'kind' | 'category'>,
): TimelineEvent {
  return {
    severity: 'warning',
    precision: 'exact',
    title: overrides.kind,
    source: 'test',
    capability: 'test',
    ...overrides,
  };
}

describe('event distribution model', () => {
  it('makes active lanes taller while preserving category order', () => {
    const events = [
      event({
        id: 'restart',
        occurred_at: '2026-07-25T18:00:00Z',
        kind: 'server_restart',
        category: 'lifecycle',
      }),
    ];

    const lanes = buildEventDistributionLanes(groupEventsByCategory(events));

    expect(lanes.map(lane => lane.category)).toEqual([
      'lifecycle',
      'queries',
      'replication',
      'coordination',
      'storage',
      'changes',
      'maintenance',
    ]);
    expect(lanes[0]).toMatchObject({ eventCount: 1, laneHeight: 30, yTop: 10 });
    expect(lanes[1]).toMatchObject({ eventCount: 0, laneHeight: 19, yTop: 40 });
  });

  it('assigns meaningful singleton marker shapes', () => {
    expect(eventMarkerShape(event({
      id: 'restart',
      occurred_at: '2026-07-25T18:00:00Z',
      kind: 'server_restart',
      category: 'lifecycle',
    }))).toBe('diamond');
    expect(eventMarkerShape(event({
      id: 'oom',
      occurred_at: '2026-07-25T18:00:00Z',
      kind: 'query_oom',
      category: 'queries',
    }))).toBe('square');
  });

  it('summarizes a cluster without discarding its event types or hosts', () => {
    const events = [
      event({
        id: 'oom-1',
        occurred_at: '2026-07-25T18:00:00Z',
        kind: 'query_oom',
        category: 'queries',
        title: 'Query OOM',
        hostname: 'ch-1',
      }),
      event({
        id: 'timeout-1',
        occurred_at: '2026-07-25T18:01:00Z',
        kind: 'query_timeout',
        category: 'queries',
        title: 'Query timeout',
        hostname: 'ch-2',
      }),
    ];

    const model = buildEventHoverCardModel(events, events[0], '2 events');

    expect(model).toMatchObject({
      categoryLabel: 'Queries',
      title: '2 events',
      hostsLabel: '2 hosts',
      distinctTitles: ['Query OOM', 'Query timeout'],
      actionLabel: 'Click to inspect these events',
    });
  });
});
