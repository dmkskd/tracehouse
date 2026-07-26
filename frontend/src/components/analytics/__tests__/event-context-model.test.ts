import { describe, expect, it } from 'vitest';
import type { EventContextMetricPoint, TimelineEvent } from '@tracehouse/core';
import {
  buildMetricChartGeometry,
  closestMetricPointIndex,
  formatContextOffset,
  selectNearbyEvents,
  sparklinePoints,
} from '../event-context-model';

function event(overrides: Partial<TimelineEvent>): TimelineEvent {
  return {
    id: 'selected',
    occurred_at: '2026-07-26T10:00:00.000Z',
    hostname: 'host-a',
    kind: 'server_restart',
    category: 'lifecycle',
    severity: 'warning',
    precision: 'inferred',
    title: 'Server restarted',
    source: 'system.asynchronous_metric_log',
    capability: 'asynchronous_metric_log',
    ...overrides,
  };
}

describe('event context model', () => {
  it('selects nearby events without changing the main range', () => {
    const selected = event({});
    const nearby = selectNearbyEvents(selected, [
      selected,
      event({
        id: 'same-host',
        occurred_at: '2026-07-26T10:00:20.000Z',
        kind: 'query_oom',
        category: 'queries',
      }),
      event({
        id: 'outside',
        occurred_at: '2026-07-26T10:10:00.000Z',
      }),
    ], 60);

    expect(nearby).toHaveLength(1);
    expect(nearby[0]).toMatchObject({
      distanceMs: 20_000,
      relation: 'same host',
    });
  });

  it('formats before and after offsets', () => {
    expect(formatContextOffset(-65_000)).toBe('1m 5s before');
    expect(formatContextOffset(20_000)).toBe('20s after');
    expect(formatContextOffset(0)).toBe('at event time');
  });

  it('produces bounded sparkline coordinates', () => {
    expect(sparklinePoints([10, 20, 15], 100, 20)).toBe(
      '0.0,18.0 50.0,2.0 100.0,10.0',
    );
  });

  it('scales memory and CPU independently and places the event by time', () => {
    const points: EventContextMetricPoint[] = [
      {
        hostname: 'host-a',
        time: '2026-07-26T09:59:55.000Z',
        memory_usage: 100,
        cpu_cores: 0.1,
        active_queries: 1,
        active_merges: 0,
      },
      {
        hostname: 'host-a',
        time: '2026-07-26T10:00:00.000Z',
        memory_usage: 200,
        cpu_cores: 0.2,
        active_queries: 2,
        active_merges: 1,
      },
      {
        hostname: 'host-a',
        time: '2026-07-26T10:00:05.000Z',
        memory_usage: 150,
        cpu_cores: 0.05,
        active_queries: 1,
        active_merges: 0,
      },
    ];
    const geometry = buildMetricChartGeometry(
      points,
      '2026-07-26T10:00:00.000Z',
      100,
      40,
    );

    expect(geometry.eventX).toBe(50);
    expect(geometry.memoryPoints).toContain('50.0,');
    expect(geometry.cpuPoints).toContain('50.0,');
    expect(geometry.cpuAxis.minimum).toBe(0);
    expect(geometry.cpuAxis.maximum).toBeCloseTo(0.22);
  });

  it('finds the nearest metric point for chart hover', () => {
    const points = [
      { time: '2026-07-26T10:00:00.000Z' },
      { time: '2026-07-26T10:00:05.000Z' },
      { time: '2026-07-26T10:00:10.000Z' },
    ] as EventContextMetricPoint[];

    expect(closestMetricPointIndex(points, 52, 100)).toBe(1);
    expect(closestMetricPointIndex([], 50, 100)).toBeNull();
  });
});
