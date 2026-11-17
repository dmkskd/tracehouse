import { describe, expect, it } from 'vitest';
import { resampleQueryIntervals, type QueryZoomInterval } from '../query-interval-resampling.js';

const base = Date.parse('2026-11-17T16:00:00Z');
function row(ts: number, dt: number, overrides: Partial<QueryZoomInterval> = {}): QueryZoomInterval {
  return { query_id: 'root', sample_query_id: 'local', hostname: 'host', ts_ms: base + ts, dt,
    cpu_us: dt * 1e6, memory_usage: 10, net_bytes: dt * 20, disk_bytes: dt * 40, ...overrides };
}

describe('query interval resampling', () => {
  it('preserves a constant rate across irregular sample timing and partial buckets', () => {
    const output = resampleQueryIntervals([row(500, 0.5), row(1500, 1), row(2700, 1.2)]).get('root')!;
    expect(output.map(s => s.ms)).toEqual([base, base + 1000, base + 2000]);
    for (const sample of output) {
      expect(sample.cpu_cores).toBeCloseTo(1);
      expect(sample.net_rate).toBeCloseTo(20);
      expect(sample.disk_rate).toBeCloseTo(40);
    }
  });
  it('isolates local execution durations before aggregating a distributed root', () => {
    const output = resampleQueryIntervals([
      row(500, 0.5),
      row(250, 0.25, { hostname: 'second-host', cpu_us: 500000, memory_usage: 20 }),
    ]).get('root')!;
    expect(output).toHaveLength(1);
    expect(output[0].cpu_cores).toBe(3);
    expect(output[0].memory).toBe(30);
  });
  it('does not create gauges for seconds missing during a long sampler outage', () => {
    const gap = 30 * 24 * 3600_000;
    const output = resampleQueryIntervals([row(500, 0.5), row(gap + 500, gap / 1000, { memory_usage: 20 })]).get('root')!;
    expect(output.map(s => s.ms)).toEqual([base, base + gap]);
    expect(output.map(s => s.memory)).toEqual([10, 20]);
    expect(output.every(s => Number.isFinite(s.cpu_cores))).toBe(true);
  });
  it('retains a gauge-only baseline without claiming that its unknown rates were sampled zero', () => {
    const [sample] = resampleQueryIntervals([row(500, 0.01, { interval_valid: false, cpu_us: 0, net_bytes: 0, disk_bytes: 0 })]).get('root')!;
    expect(sample.memory).toBe(10);
    expect(sample.rate_sampled).toBe(false);
    const [observedZero] = resampleQueryIntervals([row(500, 0.5, { cpu_us: 0, net_bytes: 0, disk_bytes: 0 })]).get('root')!;
    expect(observedZero.cpu_cores).toBe(0);
    expect(observedZero.rate_sampled).not.toBe(false);
  });
  it('ignores invalid timestamps and avoids dividing counters by invalid durations', () => {
    const output = resampleQueryIntervals([row(Number.NaN, 1), row(500, 0)]).get('root')!;
    expect(output).toHaveLength(1);
    expect(output[0].memory).toBe(10);
    expect(Number.isFinite(output[0].cpu_cores)).toBe(true);
  });
});
