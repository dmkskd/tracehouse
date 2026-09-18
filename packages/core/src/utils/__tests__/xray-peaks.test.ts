import { describe, it, expect } from 'vitest';
import { peakSustainedCores } from '../xray-peaks.js';

const s = (d_cpu_cores: number, rate_clamped = false) => ({ d_cpu_cores, rate_clamped });

describe('peakSustainedCores', { tags: ['query-analysis'] }, () => {
  it('reports the plain maximum when nothing was capped', () => {
    expect(peakSustainedCores([s(1), s(4.5), s(2)]))
      .toEqual({ value: 4.5, excludedCapped: false, allCapped: false });
  });

  it('ignores a capped teardown spike, which is a ceiling rather than a measurement', () => {
    expect(peakSustainedCores([s(1), s(2.2), s(9, true)]))
      .toEqual({ value: 2.2, excludedCapped: true, allCapped: false });
  });

  it('makes the two sources agree on a query whose only difference is the final row', () => {
    // processes_history never samples the teardown; query_metric_log does.
    const sampler = [s(1), s(2.2), s(1.8)];
    const metricLog = [s(1), s(2.2), s(1.8), s(9, true)];
    expect(peakSustainedCores(metricLog).value).toBe(peakSustainedCores(sampler).value);
  });

  it('falls back to the ceiling when every interval was capped, and says so', () => {
    expect(peakSustainedCores([s(4, true), s(9, true)]))
      .toEqual({ value: 9, excludedCapped: false, allCapped: true });
  });

  it('returns zero for no samples rather than -Infinity', () => {
    expect(peakSustainedCores([])).toEqual({ value: 0, excludedCapped: false, allCapped: false });
  });
});
