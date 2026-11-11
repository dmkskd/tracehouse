import { describe, expect, it } from 'vitest';
import type { OperationalEvent } from '@tracehouse/core';
import { buildEventLayerMarks } from '../event-layer-model';
import { eventFilterFromLists } from '../event-model';

function event(
  overrides: Partial<OperationalEvent> & Pick<OperationalEvent, 'id' | 'occurred_at' | 'kind' | 'category'>,
): OperationalEvent {
  return {
    severity: 'warning',
    precision: 'exact',
    title: overrides.kind,
    source: 'test',
    capability: 'test',
    ...overrides,
  };
}

const window = { startMs: Date.parse('2026-11-09T13:00:00Z'), endMs: Date.parse('2026-11-09T14:00:00Z') };

describe('event layer marks', () => {
  it('draws point events between samples at their own time and skips events outside the window', () => {
    const marks = buildEventLayerMarks([
      event({ id: 'restart', occurred_at: '2026-11-09T13:05:23Z', kind: 'server_restart', category: 'lifecycle' }),
      event({ id: 'ddl', occurred_at: '2026-11-09T13:30:00Z', kind: 'ddl', category: 'changes', severity: 'info' }),
      event({ id: 'before', occurred_at: '2026-11-09T12:59:59Z', kind: 'server_restart', category: 'lifecycle' }),
    ], window, 600);

    expect(marks.points.map(point => [point.primaryEvent.id, point.occurredAtMs]))
      .toEqual([
        ['restart', Date.parse('2026-11-09T13:05:23Z')],
        ['ddl', Date.parse('2026-11-09T13:30:00Z')],
      ]);
    expect(marks.bands).toEqual([]);
  });

  it('runs an episode without recovery to the window end and clips one that started earlier', () => {
    const marks = buildEventLayerMarks([
      event({
        id: 'open', kind: 'replica_readonly', category: 'replication', severity: 'error',
        started_at: '2026-11-09T13:40:00Z', occurred_at: '2026-11-09T13:40:00Z',
      }),
      event({
        id: 'earlier', kind: 'replica_readonly', category: 'replication', severity: 'error',
        started_at: '2026-11-09T12:30:00Z', occurred_at: '2026-11-09T12:30:00Z', ended_at: '2026-11-09T13:10:00Z',
      }),
    ], window, 600);

    expect(marks.points).toEqual([]);
    expect(marks.bands.map(({ event: e, ...band }) => ({ id: e.id, ...band }))).toEqual([
      { id: 'open', startMs: Date.parse('2026-11-09T13:40:00Z'), endMs: window.endMs, open: true, startsBefore: false },
      { id: 'earlier', startMs: window.startMs, endMs: Date.parse('2026-11-09T13:10:00Z'), open: false, startsBefore: true },
    ]);
  });
});

describe('eventFilterFromLists', () => {
  it('keeps known hidden values from URL lists and drops the rest', () => {
    const filter = eventFilterFromLists({
      severities: ['info', 'loud'],
      categories: ['changes'],
      kinds: ['ddl', 'not_a_kind'],
    });
    expect([...filter.hiddenSeverities]).toEqual(['info']);
    expect([...filter.hiddenCategories]).toEqual(['changes']);
    expect([...filter.hiddenKinds]).toEqual(['ddl']);
  });
});
