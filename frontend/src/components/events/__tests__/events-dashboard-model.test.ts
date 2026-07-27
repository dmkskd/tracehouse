import { describe, expect, it } from 'vitest';
import type { OperationalEvent, EventSourceCoverage } from '@tracehouse/core';
import {
  SUPPORTED_EVENT_TYPES,
  buildEventMarkerSelection,
  clusterSimilarEvents,
  eventDetailLabel,
  eventDetailSections,
  eventSourceStatusDetail,
  sortAndFilterEvents,
  supportedEventAvailability,
  supportedEventCoverage,
  supportedEventGroups,
  toUtcEventInstant,
} from '../events-dashboard-model';

function event(
  overrides: Partial<OperationalEvent> & Pick<OperationalEvent, 'id' | 'occurred_at' | 'kind' | 'category'>,
): OperationalEvent {
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
  it('keeps service bounds as explicit UTC instants', () => {
    expect(toUtcEventInstant(new Date('2026-07-25T19:39:29.049Z')))
      .toBe('2026-07-25T19:39:29.049Z');
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

  it('clusters adjacent similar events inside a bounded time window', () => {
    const events = [
      event({
        id: 'ddl-3',
        occurred_at: '2026-07-25T18:00:04.500Z',
        kind: 'ddl',
        category: 'changes',
        title: 'DDL · Create',
        hostname: 'ch-1',
      }),
      event({
        id: 'ddl-2',
        occurred_at: '2026-07-25T18:00:03.000Z',
        kind: 'ddl',
        category: 'changes',
        title: 'DDL · Create',
        hostname: 'ch-1',
      }),
      event({
        id: 'ddl-1',
        occurred_at: '2026-07-25T18:00:00.000Z',
        kind: 'ddl',
        category: 'changes',
        title: 'DDL · Create',
        hostname: 'ch-1',
      }),
      event({
        id: 'ddl-too-old',
        occurred_at: '2026-07-25T17:59:58.000Z',
        kind: 'ddl',
        category: 'changes',
        title: 'DDL · Create',
        hostname: 'ch-1',
      }),
      event({
        id: 'ddl-other-host',
        occurred_at: '2026-07-25T17:59:57.900Z',
        kind: 'ddl',
        category: 'changes',
        title: 'DDL · Create',
        hostname: 'ch-2',
      }),
    ];

    const clusters = clusterSimilarEvents(events);

    expect(clusters.map(cluster => cluster.events.map(item => item.id))).toEqual([
      ['ddl-3', 'ddl-2', 'ddl-1'],
      ['ddl-too-old'],
      ['ddl-other-host'],
    ]);
    expect(clusters[0].endMs - clusters[0].startMs).toBe(4_500);
  });

  it('does not group different event titles or severities', () => {
    const events = [
      event({
        id: 'create',
        occurred_at: '2026-07-25T18:00:00.200Z',
        kind: 'ddl',
        category: 'changes',
        title: 'DDL · Create',
      }),
      event({
        id: 'drop',
        occurred_at: '2026-07-25T18:00:00.100Z',
        kind: 'ddl',
        category: 'changes',
        title: 'DDL · Drop',
      }),
      event({
        id: 'warning-create',
        occurred_at: '2026-07-25T18:00:00.000Z',
        kind: 'ddl',
        category: 'changes',
        title: 'DDL · Create',
        severity: 'warning',
      }),
    ];

    expect(clusterSimilarEvents(events)).toHaveLength(3);
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
    expect(sections.at(-1)?.label).toBe('Identifiers');
  });

  it('shows backup, async-insert, and Keeper-specific event fields', () => {
    const sections = eventDetailSections(event({
      id: 'backup',
      occurred_at: '2026-07-25T18:00:00Z',
      kind: 'backup',
      category: 'maintenance',
      status: 'BACKUP_CREATED',
      operation_id: 'backup-1',
      storage_name: "Disk('backups', 'daily.zip')",
      started_at: '2026-07-25T17:59:00Z',
      num_files: 12,
      total_size: 4096,
      flush_query_id: 'flush-1',
      keeper_name: 'default',
      keeper_host: 'keeper-1',
      keeper_port: 9181,
      keeper_client_id: '9223372036854775000',
    }));
    const rows = Object.fromEntries(
      sections.flatMap(section => section.rows).map(row => [row.label, row.value]),
    );

    expect(rows.Status).toBe('BACKUP_CREATED');
    expect(rows.Storage).toBe("Disk('backups', 'daily.zip')");
    expect(rows.Files).toBe('12');
    expect(rows['Total size']).toBe('4 KB');
    expect(rows['Keeper node']).toBe('keeper-1:9181');
    expect(rows['Operation ID']).toBe('backup-1');
    expect(rows['Flush query ID']).toBe('flush-1');
    expect(rows['Keeper client ID']).toBe('9223372036854775000');
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
    expect(SUPPORTED_EVENT_TYPES).toHaveLength(16);
    expect(SUPPORTED_EVENT_TYPES.map(item => item.kind)).toContain('server_restart');
    expect(SUPPORTED_EVENT_TYPES.map(item => item.kind)).toContain('replica_readonly');
    expect(SUPPORTED_EVENT_TYPES.map(item => item.kind)).toContain('backup');
    expect(SUPPORTED_EVENT_TYPES.map(item => item.kind)).toContain('async_insert_failure');
    expect(SUPPORTED_EVENT_TYPES.map(item => item.kind)).toContain('keeper_connection');
    expect(SUPPORTED_EVENT_TYPES.find(item => item.kind === 'backup')?.severity)
      .toBe('error');
    expect(supportedEventGroups().map(group => group.category)).toContain('coordination');
    expect(supportedEventGroups().flatMap(group => group.events)).toHaveLength(
      SUPPORTED_EVENT_TYPES.length,
    );
  });

  it('reports supported-event availability from source coverage', () => {
    const coverage: EventSourceCoverage[] = [
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
    expect(
      replicationFailure
      && supportedEventCoverage(replicationFailure, coverage),
    ).toMatchObject({
      availability: 'partial',
      availableSources: 1,
      totalSources: 3,
      label: '1/3 sources available',
      warning: 'Events may be missing',
    });
  });

  it('uses the capability probe reason for an unavailable event source', () => {
    expect(eventSourceStatusDetail({
      source: 'system.crash_log',
      capability: 'crash_log',
      status: 'unavailable',
      event_count: 0,
      detail: 'Capability not available',
    }, [{
      id: 'crash_log',
      label: 'Crash Log',
      description: 'Crash records',
      available: false,
      category: 'logging',
      detail: 'Table not found',
    }])).toBe('Table not found');
  });
});
