import { describe, expect, it } from 'vitest';
import type { TimelineEvent } from '@tracehouse/core';
import {
  buildEventMarkerSelection,
  sortAndFilterEvents,
  toClickHouseEventTime,
} from '../events-dashboard-model';

function event(
  overrides: Partial<TimelineEvent> & Pick<TimelineEvent, 'id' | 'occurred_at' | 'kind' | 'category'>,
): TimelineEvent {
  return {
    severity: 'info',
    precision: 'exact',
    title: overrides.kind,
    source: 'test',
    capability: 'test',
    ...overrides,
  };
}

describe('events dashboard model', () => {
  it('normalizes service bounds to whole-second ClickHouse time', () => {
    expect(toClickHouseEventTime(new Date('2026-07-25T19:39:29.049Z')))
      .toBe('2026-07-25 19:39:29');
  });

  it('sorts and filters events outside the React component', () => {
    const events = [
      event({
        id: 'ddl',
        occurred_at: '2026-07-25T18:00:00Z',
        kind: 'ddl',
        category: 'changes',
        title: 'DDL · Alter',
      }),
      event({
        id: 'oom',
        occurred_at: '2026-07-25T19:00:00Z',
        kind: 'query_oom',
        category: 'queries',
        severity: 'error',
        title: 'Query OOM',
        hostname: 'ch-1',
      }),
    ];

    expect(sortAndFilterEvents(events, {
      search: 'ch-1',
      severity: 'error',
      category: 'queries',
      kind: 'all',
    }).map(item => item.id)).toEqual(['oom']);
  });

  it('builds marker selections for singleton and clustered events', () => {
    const events = [
      event({
        id: 'first',
        occurred_at: '2026-07-25T18:00:00Z',
        kind: 'query_oom',
        category: 'queries',
      }),
      event({
        id: 'second',
        occurred_at: '2026-07-25T18:05:00Z',
        kind: 'query_oom',
        category: 'queries',
      }),
    ];

    const selection = buildEventMarkerSelection(events, 0);

    expect([...selection.eventIds]).toEqual(['first', 'second']);
    expect(selection.startMs).toBe(Date.parse(events[0].occurred_at));
    expect(selection.endMs).toBe(Date.parse(events[1].occurred_at));
  });
});
