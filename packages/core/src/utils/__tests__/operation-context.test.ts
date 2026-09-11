import { describe, it, expect } from 'vitest';
import { buildOperationContext, buildOperationSpan, operationsOverlap } from '../operation-context.js';
import type { OperationRow } from '../timeline-operations.js';

const T0 = Date.UTC(2026, 8, 10, 21, 17, 0);

function row(overrides: Partial<OperationRow> = {}): OperationRow {
  const base: OperationRow = {
    kind: 'query', idx: 0, id: 'q1', kindLabel: 'SELECT', label: 'SELECT 1',
    user: 'u', hostname: 'h1',
    startMs: T0, endMs: T0 + 10_000, durationMs: 10_000,
    metricValue: 100, cpuUs: 100, peakMemory: 0,
    diskBytes: 0, diskReadBytes: 0, diskWriteBytes: 0, netBytes: 0,
    isRunning: false, metricEstimated: false, matchedHash: false, failed: false,
    source: {} as OperationRow['source'],
  };
  return { ...base, ...overrides };
}

describe('operationsOverlap', () => {
  const a = row();
  it('is true for intervals that intersect', () => {
    expect(operationsOverlap(a, row({ startMs: T0 + 5_000, endMs: T0 + 20_000 }))).toBe(true);
  });
  it('is true when one interval contains the other', () => {
    expect(operationsOverlap(a, row({ startMs: T0 + 1_000, endMs: T0 + 2_000 }))).toBe(true);
  });
  it('is true when they only touch at an endpoint', () => {
    expect(operationsOverlap(a, row({ startMs: T0 + 10_000, endMs: T0 + 30_000 }))).toBe(true);
  });
  it('is false for disjoint intervals', () => {
    expect(operationsOverlap(a, row({ startMs: T0 + 11_000, endMs: T0 + 20_000 }))).toBe(false);
  });
});

describe('buildOperationContext', () => {
  const selected = row({ id: 'sel', metricValue: 100 });
  const overlapping = row({ id: 'big', kind: 'merge', metricValue: 300, startMs: T0 + 5_000, endMs: T0 + 15_000 });
  const alsoOverlapping = row({ id: 'small', metricValue: 50, startMs: T0 - 5_000, endMs: T0 + 1_000 });
  const elsewhere = row({ id: 'later', metricValue: 999, startMs: T0 + 60_000, endMs: T0 + 70_000 });

  it('returns an empty context when nothing is selected', () => {
    const c = buildOperationContext(null, [selected, overlapping]);
    expect(c.neighbors).toEqual([]);
    expect(c.neighborCount).toBe(0);
    expect(c.metricShare).toBe(0);
  });

  it('lists only the operations that overlapped, biggest first', () => {
    const c = buildOperationContext(selected, [selected, overlapping, alsoOverlapping, elsewhere]);
    expect(c.neighbors.map(n => n.id)).toEqual(['big', 'small']);
    expect(c.neighborCount).toBe(2);
  });

  it('excludes the selected operation itself, matching on kind, id and host', () => {
    const twin = row({ id: 'sel', kind: 'merge', metricValue: 7 });
    const otherHost = row({ id: 'sel', hostname: 'h2', metricValue: 9 });
    const c = buildOperationContext(selected, [selected, twin, otherHost]);
    expect(c.neighbors.map(n => n.metricValue).sort()).toEqual([7, 9]);
  });

  it('reports the share of everything in view', () => {
    const c = buildOperationContext(selected, [selected, overlapping, elsewhere]);
    expect(c.metricShare).toBeCloseTo(100 / 1399);
  });

  it('reports the share of what ran concurrently', () => {
    const c = buildOperationContext(selected, [selected, overlapping, alsoOverlapping, elsewhere]);
    expect(c.concurrentMetricTotal).toBe(450);
    expect(c.concurrentShare).toBeCloseTo(100 / 450);
  });

  it('ranks the selected operation among the concurrent set', () => {
    expect(buildOperationContext(selected, [selected, overlapping, alsoOverlapping]).rank).toBe(2);
    expect(buildOperationContext(selected, [selected, alsoOverlapping]).rank).toBe(1);
  });

  it('caps the neighbor list but keeps the true count', () => {
    const many = Array.from({ length: 9 }, (_, i) => row({ id: `n${i}`, metricValue: i + 1 }));
    const c = buildOperationContext(selected, [selected, ...many], 3);
    expect(c.neighbors).toHaveLength(3);
    expect(c.neighborCount).toBe(9);
  });

  it('handles a lone operation without dividing by zero', () => {
    const c = buildOperationContext(row({ metricValue: 0 }), [row({ metricValue: 0 })]);
    expect(c.metricShare).toBe(0);
    expect(c.concurrentShare).toBe(0);
    expect(c.rank).toBe(1);
  });
});

describe('buildOperationSpan', () => {
  const windowStart = T0, windowEnd = T0 + 100_000;

  it('places the operation as fractions of the window', () => {
    const span = buildOperationSpan(row({ startMs: T0 + 20_000, endMs: T0 + 60_000 }), windowStart, windowEnd, null);
    expect(span.startFraction).toBeCloseTo(0.2);
    expect(span.endFraction).toBeCloseTo(0.6);
    expect(span.clippedStart).toBe(false);
    expect(span.clippedEnd).toBe(false);
  });

  it('clamps and flags an operation that started before the window', () => {
    const span = buildOperationSpan(row({ startMs: T0 - 50_000, endMs: T0 + 10_000 }), windowStart, windowEnd, null);
    expect(span.startFraction).toBe(0);
    expect(span.clippedStart).toBe(true);
  });

  it('clamps and flags an operation still running at the window edge', () => {
    const span = buildOperationSpan(row({ startMs: T0 + 90_000, endMs: T0 + 200_000 }), windowStart, windowEnd, null);
    expect(span.endFraction).toBe(1);
    expect(span.clippedEnd).toBe(true);
  });

  it('places the pin when it falls inside the window', () => {
    const span = buildOperationSpan(row(), windowStart, windowEnd, T0 + 25_000);
    expect(span.pinFraction).toBeCloseTo(0.25);
  });

  it('reports no pin when there is none or it lies outside', () => {
    expect(buildOperationSpan(row(), windowStart, windowEnd, null).pinFraction).toBeNull();
    expect(buildOperationSpan(row(), windowStart, windowEnd, T0 - 1).pinFraction).toBeNull();
  });

  it('survives a zero-length window', () => {
    const span = buildOperationSpan(row(), T0, T0, null);
    expect(Number.isFinite(span.startFraction)).toBe(true);
  });
});
