import { describe, expect, it } from 'vitest';
import type { TimelineEvent, TimelineEventSourceCoverage } from '@tracehouse/core';
import {
  SUPPORTED_EVENT_TYPES,
  buildEventMarkerSelection,
  eventDetailLabel,
  eventDetailSections,
  sortAndFilterEvents,
  supportedEventAvailability,
  supportedEventGroups,
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

  it('presents inferred restart detector facts without generic evidence copy', () => {
    const restart = event({
      id: 'restart',
      occurred_at: '2026-07-25T18:00:00Z',
      observed_at: '2026-07-25T18:00:00.334Z',
      kind: 'server_restart',
      category: 'lifecycle',
      severity: 'warning',
      precision: 'inferred',
      hostname: 'ch-1',
      metric_name: 'Uptime',
      metric_value: 0.334,
      previous_metric_value: 2119.123,
      metric_unit: 's',
    });
    const detection = eventDetailSections(restart)
      .find(section => section.id === 'detection');
    const rows = Object.fromEntries(
      detection?.rows.map(row => [row.label, row.value]) ?? [],
    );

    expect(eventDetailLabel(restart)).toBe('Detection method');
    expect(rows.Metric).toBe('Uptime');
    expect(rows['Previous sample']).toBe('2,119.123 s');
    expect(rows['Detected sample']).toBe('0.334 s');
  });

  it('groups identifiers after operational event fields', () => {
    const sections = eventDetailSections(event({
      id: 'ddl',
      occurred_at: '2026-07-25T18:00:00Z',
      kind: 'ddl',
      category: 'changes',
      query_id: 'query-1',
      query_kind: 'Create',
      hostname: 'ch-1',
      database: 'tracehouse',
      tables: ['tracehouse.events'],
    }));

    expect(sections.map(section => section.id)).toEqual([
      'metadata',
      'details',
      'identifiers',
    ]);
    expect(sections.at(-1)?.label).toBe('Query identifiers');
  });

  it('does not expose TraceHouse internal event IDs', () => {
    const sections = eventDetailSections(event({
      id: 'internal-event-id',
      occurred_at: '2026-07-25T18:00:00Z',
      kind: 'server_restart',
      category: 'lifecycle',
    }));

    expect(sections.some(section =>
      section.rows.some(row => row.value === 'internal-event-id'),
    )).toBe(false);
    expect(sections.some(section => section.id === 'identifiers')).toBe(false);
  });

  it('catalogs only event kinds currently emitted by the service', () => {
    expect(SUPPORTED_EVENT_TYPES).toHaveLength(13);
    expect(SUPPORTED_EVENT_TYPES.map(item => item.kind)).toContain('server_restart');
    expect(SUPPORTED_EVENT_TYPES.map(item => item.kind)).toContain('replica_readonly');
    expect(SUPPORTED_EVENT_TYPES.map(item => item.kind)).not.toContain('backup');
    expect(supportedEventGroups().flatMap(group => group.events)).toHaveLength(
      SUPPORTED_EVENT_TYPES.length,
    );
  });

  it('reports supported-event availability from source coverage', () => {
    const coverage: TimelineEventSourceCoverage[] = [
      {
        source: 'system.query_log',
        capability: 'query_log',
        status: 'loaded',
        event_count: 2,
      },
      {
        source: 'system.part_log',
        capability: 'part_log',
        status: 'loaded',
        event_count: 0,
      },
      {
        source: 'system.background_schedule_pool_log',
        capability: 'background_schedule_pool_log',
        status: 'unavailable',
        event_count: 0,
      },
      {
        source: 'system.metric_log',
        capability: 'metric_log_replication_failures',
        status: 'unavailable',
        event_count: 0,
      },
    ];
    const queryOom = SUPPORTED_EVENT_TYPES.find(item => item.kind === 'query_oom');
    const replicationFailure = SUPPORTED_EVENT_TYPES.find(
      item => item.kind === 'replication_task_failure',
    );

    expect(queryOom && supportedEventAvailability(queryOom, coverage)).toBe('available');
    expect(
      replicationFailure
      && supportedEventAvailability(replicationFailure, coverage),
    ).toBe('partial');
  });
});
