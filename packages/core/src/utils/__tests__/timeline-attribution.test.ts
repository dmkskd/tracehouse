import { describe, expect, it } from 'vitest';
import { attributeTimeline } from '../timeline-attribution.js';
import type { MemoryTimeline, QuerySeries } from '../../types/timeline.js';
const base = Date.parse('2026-11-17T16:00:00Z');
const at = (seconds: number) => new Date(base + seconds * 1000).toISOString();
const range = (start: number, end: number) => ({ kind: 'range' as const, startMs: base + start * 1000, endMs: base + end * 1000 });
function query(id = 'q', start = 0, end = 60, cores = 1): QuerySeries {
  return { query_id: id, query_kind: 'SELECT', user: 'default', label: 'SELECT count()', hostname: 'ch-01', tables: ['db.events'],
    start_time: at(start), end_time: at(end), duration_ms: (end - start) * 1000, cpu_us: (end - start) * cores * 1e6, peak_memory: 999999,
    net_send: 0, net_recv: 0, disk_read: (end - start) * 100, disk_write: 0, points: [] };
}
function data(queries: QuerySeries[]): MemoryTimeline {
  return { queries, merges: [], mutations: [], window_start: at(0), window_end: at(3600), target: 'all', server_cpu: [], server_memory: [],
    server_network_send: [], server_network_recv: [], server_disk_read: [], server_disk_write: [],
    host_count: 1, cpu_cores: 8, server_total_ram: 1024, query_count: queries.length, merge_count: 0, merge_peak_total: 0, mutation_count: 0 };
}

describe('shared instant and range attribution', () => {
  it('retains active operations with unavailable memory instead of using their execution peaks', () => {
    const result = attributeTimeline(data([query()]), { selection: { kind: 'instant', atMs: base + 30000 }, mode: 'memory' });
    expect(result.operations[0]).toMatchObject({ value: null, basis: 'unavailable' });
    expect(result.coverage.unavailable).toBe(1);
    expect(result.peakValue).toBeNull();
  });
  it('clips work to range overlap and normalizes the mean over the full range', () => {
    const result = attributeTimeline(data([query('short', -10, 10, 2), query('later', 30, 60)]), { selection: range(0, 60), mode: 'cpu' });
    expect(result.work).toBe(50e6);
    expect(result.total).toBe(50e6);
    expect(result.meanValue).toBeCloseTo(50e6 / 60);
    expect(result.unit).toBe('cpu-us');
    expect(result.operations.find(row => row.source === result.operations[0].source)?.basis).toBe('average');
  });
  it('integrates a full hour without treating each active operation as running for the whole hour', () => {
    const result = attributeTimeline(data([query('full', 0, 3600, 2), query('half', 1800, 3600, 1)]), { selection: range(0, 3600), mode: 'cpu' });
    expect(result.total).toBe(9000e6);
    expect(result.meanValue).toBe(2.5e6);
    expect(result.work).toBe(9000e6);
    expect(result.peakValue).toBe(3e6);
  });
  it('uses interval samples where covered, and labels the remaining CPU time estimated', () => {
    const q = query(); q.zoomSamples = [{ ms: base + 10000, cpu_cores: 3, memory: 100, net_rate: 0, disk_rate: 0 }];
    const input = data([q]);
    const whole = attributeTimeline(input, { selection: range(0, 60), mode: 'cpu' });
    expect(whole.operations[0]).toMatchObject({ basis: 'mixed', sampledMs: 1000, estimatedMs: 59000, missingMs: 0, work: 62e6 });
    const left = attributeTimeline(input, { selection: range(0, 30), mode: 'cpu' });
    const right = attributeTimeline(input, { selection: range(30, 60), mode: 'cpu' });
    expect(whole.work).toBe(left.work! + right.work!);
  });
  it('does not sum memory peaks of operations that ran at different times', () => {
    const a = query('first', 0, 10), b = query('second', 10, 20);
    a.zoomSamples = [{ ms: base + 5000, memory: 100, cpu_cores: 0, net_rate: 0, disk_rate: 0 }];
    b.zoomSamples = [{ ms: base + 15000, memory: 200, cpu_cores: 0, net_rate: 0, disk_rate: 0 }];
    const result = attributeTimeline(data([a, b]), { selection: range(0, 20), mode: 'memory' });
    expect(result.peakValue).toBe(200);
    expect(result.total).toBe(1200); // 4s × 100 + 4s × 200: byte-seconds.
    expect(result.unit).toBe('byte-seconds');
    expect(result.meanValue).toBe(60);
    expect(result.coverage.missingOperationMs).toBe(12000);
    expect(result.work).toBeNull();
  });
  it('computes the peak of simultaneous memory samples and distinguishes missing coverage from zero', () => {
    const a = query('a'), b = query('b');
    a.zoomSamples = [{ ms: base + 5000, memory: 100, cpu_cores: 0, net_rate: 0, disk_rate: 0 }];
    b.zoomSamples = [{ ms: base + 5000, memory: 200, cpu_cores: 0, net_rate: 0, disk_rate: 0 }];
    const result = attributeTimeline(data([a, b]), { selection: range(3, 7), mode: 'memory' });
    expect(result.total).toBe(1200); expect(result.meanValue).toBe(300); expect(result.peakValue).toBe(300);
    expect(result.coverage.missingOperationMs).toBe(0);
    b.zoomSamples[0].memory = 0;
    expect(attributeTimeline(data([b]), { selection: { kind: 'instant', atMs: base + 5000 }, mode: 'memory' }).operations[0]).toMatchObject({ value: 0, basis: 'sampled' });
  });
  it('keeps measured server coverage independent of the operation estimates', () => {
    const input = data([query()]); input.server_cpu = [{ t: at(10), v: 4e6 }];
    const result = attributeTimeline(input, { selection: range(0, 60), mode: 'cpu' });
    expect(result.measured).toMatchObject({ value: 16e6, meanValue: 4e6, coverageMs: 4000 });
    expect(result.total).toBe(60e6);
    expect(result.capacity).toBe(480e6);
  });
  it('uses the selected host set for operations, measured CPU, and capacity', () => {
    const input = data([query('a'), { ...query('b', 0, 60, 2), hostname: 'ch-02' }, { ...query('c', 0, 60, 3), hostname: 'ch-03' }]);
    input.host_count = 3;
    input.per_host_cpu = { 'ch-01': [{ t: at(30), v: 1e6 }], 'ch-02': [{ t: at(30), v: 2e6 }], 'ch-03': [{ t: at(30), v: 3e6 }] };
    input.per_host_cpu_cores = { 'ch-01': 1, 'ch-02': 2, 'ch-03': 4 };
    const result = attributeTimeline(input, { selection: { kind: 'instant', atMs: base + 30000 }, mode: 'cpu', hostname: ['ch-01', 'ch-03'] });
    expect(result.operations).toHaveLength(2); expect(result.total).toBe(4e6);
    expect(result.measured.value).toBe(4e6); expect(result.capacity).toBe(5e6);
  });
  it('keeps estimated CPU for a gauge-only baseline while accepting its sampled memory', () => {
    const q = query(); q.zoomSamples = [{ ms: base + 30000, memory: 100, cpu_cores: 0, net_rate: 0, disk_rate: 0, rate_sampled: false }];
    const input = data([q]);
    expect(attributeTimeline(input, { selection: { kind: 'instant', atMs: base + 30000 }, mode: 'cpu' }).operations[0]).toMatchObject({ value: 1e6, basis: 'average' });
    expect(attributeTimeline(input, { selection: { kind: 'instant', atMs: base + 30000 }, mode: 'memory' }).operations[0]).toMatchObject({ value: 100, basis: 'sampled' });
  });
  it('applies scope, hidden kinds, and lifetime membership before reading nearby samples', () => {
    const q = query(); q.zoomSamples = [{ ms: base, cpu_cores: 3, memory: 100, net_rate: 0, disk_rate: 0 }];
    expect(attributeTimeline(data([q]), { selection: { kind: 'instant', atMs: base - 1 }, mode: 'cpu' }).operations).toEqual([]);
    expect(attributeTimeline(data([q]), { selection: range(0, 60), mode: 'cpu', hostname: 'ch-02' }).operations).toEqual([]);
    expect(attributeTimeline(data([q]), { selection: range(0, 60), mode: 'cpu', hiddenKinds: new Set(['query']) }).operations).toEqual([]);
    expect(attributeTimeline(data([q]), { selection: range(60, 0), mode: 'cpu' }).operations).toEqual([]);
  });
});
