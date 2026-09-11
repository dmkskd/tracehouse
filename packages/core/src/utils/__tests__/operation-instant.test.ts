import { describe, it, expect } from 'vitest';
import { buildInstantBreakdown, measuredValueAt, selectServerSeries } from '../operation-instant.js';
import type { InstantBreakdownInput } from '../operation-instant.js';
import type { MergeSeries, MutationSeries, QuerySeries } from '../../types/timeline.js';

const MB = 1024 * 1024;
const T0 = Date.UTC(2026, 8, 10, 21, 17, 0);
const AT = T0 + 42_000;  // 21:17:42

function q(overrides: Partial<QuerySeries> = {}): QuerySeries {
  return {
    query_id: 'q1', label: 'SELECT 1', user: 'u',
    peak_memory: 100 * MB, duration_ms: 10_000,
    cpu_us: 10_000_000,  // 1 core average
    net_send: 0, net_recv: 0, disk_read: 0, disk_write: 0,
    start_time: '2026-09-10 21:17:40', end_time: '2026-09-10 21:17:50',
    query_kind: 'Select', points: [],
    ...overrides,
  };
}

function merge(overrides: Partial<MergeSeries> = {}): MergeSeries {
  return {
    part_name: 'all_1_2_1', table: 'db.events',
    peak_memory: 20 * MB, duration_ms: 10_000,
    cpu_us: 20_000_000,  // 2 cores average
    net_send: 0, net_recv: 0, disk_read: 0, disk_write: 0,
    start_time: '2026-09-10 21:17:40', end_time: '2026-09-10 21:17:50',
    ...overrides,
  };
}

function mutation(overrides: Partial<MutationSeries> = {}): MutationSeries {
  return {
    part_name: 'all_3_3_0_5', table: 'db.events',
    peak_memory: 5 * MB, duration_ms: 10_000,
    cpu_us: 5_000_000,
    net_send: 0, net_recv: 0, disk_read: 0, disk_write: 0,
    start_time: '2026-09-10 21:17:40', end_time: '2026-09-10 21:17:50',
    ...overrides,
  };
}

function input(overrides: Partial<InstantBreakdownInput> = {}): InstantBreakdownInput {
  return { queries: [], merges: [], mutations: [], serverSeries: [], ...overrides };
}

// 8 cores at full tilt, in the µs/s unit the CPU bands use.
const CPU_CAPACITY = 8 * 1_000_000;

describe('buildInstantBreakdown', () => {
  it('groups queries by kind and keeps merges and mutations apart', () => {
    const b = buildInstantBreakdown(input({
      queries: [q(), q({ query_id: 'q2', query_kind: 'Insert', cpu_us: 5_000_000 })],
      merges: [merge()],
      mutations: [mutation()],
    }), { atMs: AT, mode: 'cpu', capacity: CPU_CAPACITY });
    // Equal values (INSERT and MUTATION) fall back to an alphabetical tiebreak.
    expect(b.segments.map(s => s.key)).toEqual(['MERGE', 'SELECT', 'INSERT', 'MUTATION']);
    expect(b.segments.map(s => s.value)).toEqual([2_000_000, 1_000_000, 500_000, 500_000]);
  });

  it('sums operations of the same kind and counts them', () => {
    const b = buildInstantBreakdown(input({ queries: [q(), q({ query_id: 'q2' })] }),
      { atMs: AT, mode: 'cpu', capacity: CPU_CAPACITY });
    expect(b.segments).toHaveLength(1);
    expect(b.segments[0]).toMatchObject({ key: 'SELECT', value: 2_000_000, count: 2 });
  });

  it('reports each segment as a share of capacity and of the stack', () => {
    const b = buildInstantBreakdown(input({ queries: [q()], merges: [merge()] }),
      { atMs: AT, mode: 'cpu', capacity: CPU_CAPACITY });
    const [mergeSeg, selectSeg] = b.segments;
    expect(mergeSeg.shareOfCapacity).toBeCloseTo(0.25);   // 2 of 8 cores
    expect(selectSeg.shareOfCapacity).toBeCloseTo(0.125); // 1 of 8 cores
    expect(mergeSeg.shareOfShown).toBeCloseTo(2 / 3);
    expect(b.shownShareOfCapacity).toBeCloseTo(0.375);
  });

  it('leaves capacity shares null for metrics with no capacity', () => {
    const b = buildInstantBreakdown(input({ queries: [q({ disk_read: 1000, disk_write: 1000 })] }),
      { atMs: AT, mode: 'disk', capacity: null });
    expect(b.segments[0].shareOfCapacity).toBeNull();
    expect(b.shownShareOfCapacity).toBeNull();
    expect(b.segments[0].shareOfShown).toBe(1);
  });

  it('excludes operations that were not running at the instant', () => {
    const b = buildInstantBreakdown(input({
      queries: [q(), q({ query_id: 'earlier', start_time: '2026-09-10 21:16:00', end_time: '2026-09-10 21:16:30' })],
    }), { atMs: AT, mode: 'cpu', capacity: CPU_CAPACITY });
    expect(b.segments[0].count).toBe(1);
  });

  it('includes operations whose interval touches the instant exactly', () => {
    const b = buildInstantBreakdown(input({
      queries: [q({ start_time: '2026-09-10 21:17:42', end_time: '2026-09-10 21:17:50' })],
    }), { atMs: AT, mode: 'cpu', capacity: CPU_CAPACITY });
    expect(b.segments).toHaveLength(1);
  });

  it('honors the metric mode when valuing a band', () => {
    const b = buildInstantBreakdown(input({ queries: [q()] }),
      { atMs: AT, mode: 'memory', capacity: 1024 * MB });
    expect(b.segments[0].value).toBe(100 * MB);
    expect(b.segments[0].shareOfCapacity).toBeCloseTo(100 / 1024);
  });

  it('drops kinds hidden from the chart so the panel matches the picture', () => {
    const b = buildInstantBreakdown(input({ queries: [q()], merges: [merge()], mutations: [mutation()] }),
      { atMs: AT, mode: 'cpu', capacity: CPU_CAPACITY, hiddenKinds: new Set(['merge', 'mutation'] as const) });
    expect(b.segments.map(s => s.key)).toEqual(['SELECT']);
  });

  it('prefers a nearby zoom sample over the lifetime average and says so', () => {
    const b = buildInstantBreakdown(input({
      queries: [q({ zoomSamples: [{ ms: AT, memory: 0, cpu_cores: 3, net_rate: 0, disk_rate: 0 }] })],
    }), { atMs: AT, mode: 'cpu', capacity: CPU_CAPACITY });
    expect(b.segments[0].value).toBe(3_000_000);
    expect(b.hasSamples).toBe(true);
  });

  it('falls back to the average when the nearest sample is too far away', () => {
    const b = buildInstantBreakdown(input({
      queries: [q({ zoomSamples: [{ ms: AT - 60_000, memory: 0, cpu_cores: 3, net_rate: 0, disk_rate: 0 }] })],
    }), { atMs: AT, mode: 'cpu', capacity: CPU_CAPACITY });
    expect(b.segments[0].value).toBe(1_000_000);
    expect(b.hasSamples).toBe(false);
  });

  it('reads the measured server value at the instant', () => {
    const b = buildInstantBreakdown(input({
      queries: [q()],
      serverSeries: [
        { t: '2026-09-10 21:17:41', v: 4_000_000 },
        { t: '2026-09-10 21:17:42', v: 4_480_000 },
      ],
    }), { atMs: AT, mode: 'cpu', capacity: CPU_CAPACITY });
    expect(b.measured).toBe(4_480_000);
    expect(b.measuredShareOfCapacity).toBeCloseTo(0.56);
  });

  it('reports no measurement rather than zero when nothing is near', () => {
    const b = buildInstantBreakdown(input({
      queries: [q()],
      serverSeries: [{ t: '2026-09-10 21:10:00', v: 4_000_000 }],
    }), { atMs: AT, mode: 'cpu', capacity: CPU_CAPACITY });
    expect(b.measured).toBeNull();
    expect(b.measuredShareOfCapacity).toBeNull();
  });

  it('returns an empty breakdown for a non-finite instant', () => {
    const b = buildInstantBreakdown(input({ queries: [q()] }),
      { atMs: NaN, mode: 'cpu', capacity: CPU_CAPACITY });
    expect(b.segments).toEqual([]);
    expect(b.shownTotal).toBe(0);
  });
});

describe('measuredValueAt', () => {
  const series = [
    { t: '2026-09-10 21:17:40', v: 10 },
    { t: '2026-09-10 21:17:44', v: 20 },
  ];

  it('picks the closest point within tolerance', () => {
    expect(measuredValueAt(series, AT)).toBe(10);          // 21:17:42 is 2s from both, first wins
    expect(measuredValueAt(series, T0 + 43_000)).toBe(20);
  });

  it('returns null beyond tolerance and for an empty series', () => {
    expect(measuredValueAt(series, T0)).toBeNull();
    expect(measuredValueAt([], AT)).toBeNull();
  });
});

describe('selectServerSeries', () => {
  const data = {
    server_memory: [{ t: 'm', v: 1 }],
    server_cpu: [{ t: 'c', v: 2 }],
    server_network_send: [{ t: 'n', v: 10 }],
    server_network_recv: [{ t: 'n', v: 5 }],
    server_disk_read: [{ t: 'd', v: 100 }],
    server_disk_write: [{ t: 'd', v: 20 }],
  };

  it('returns the matching series for memory and cpu', () => {
    expect(selectServerSeries(data, 'memory')).toEqual([{ t: 'm', v: 1 }]);
    expect(selectServerSeries(data, 'cpu')).toEqual([{ t: 'c', v: 2 }]);
  });

  it('sums both directions for network and disk', () => {
    expect(selectServerSeries(data, 'network')).toEqual([{ t: 'n', v: 15 }]);
    expect(selectServerSeries(data, 'disk')).toEqual([{ t: 'd', v: 120 }]);
  });

  it('treats a missing counterpart series as zero', () => {
    expect(selectServerSeries({ ...data, server_network_recv: [] }, 'network')).toEqual([{ t: 'n', v: 10 }]);
    expect(selectServerSeries({ ...data, server_disk_write: undefined }, 'disk')).toEqual([{ t: 'd', v: 100 }]);
    expect(selectServerSeries({ ...data, server_disk_read: undefined }, 'disk')).toEqual([]);
  });
});
