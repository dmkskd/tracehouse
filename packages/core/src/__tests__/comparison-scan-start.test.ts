import { describe, it, expect } from 'vitest';
import {
  commonScanStart,
  buildQueryMetricLogSamplesSQL,
  QUERY_METRIC_LOG_MISSING_FIELDS,
} from '../queries/query-metric-log-queries.js';
import { timelineMetricsExcluding, TIMELINE_METRICS } from '../queries/process-queries.js';

describe('commonScanStart', { tags: ['query-analysis'] }, () => {
  it('uses the earliest start when the set fits inside one scan window', () => {
    expect(commonScanStart(['2026-09-17 10:00:00', '2026-09-17 18:30:00']))
      .toBe('2026-09-17 10:00:00');
  });

  it('accepts ISO-8601, as the UI carries it', () => {
    expect(commonScanStart(['2026-09-17T18:30:00.000Z', '2026-09-17T10:00:00.000Z']))
      .toBe('2026-09-17T10:00:00.000Z');
  });

  it('gives up when the queries span more than a day, rather than dropping the far ones', () => {
    expect(commonScanStart(['2026-09-12 10:00:00', '2026-09-17 10:00:00'])).toBeUndefined();
  });

  it('gives up when any start time is missing or unparsable', () => {
    expect(commonScanStart(['2026-09-17 10:00:00', undefined])).toBeUndefined();
    expect(commonScanStart(['2026-09-17 10:00:00', 'nonsense'])).toBeUndefined();
    expect(commonScanStart([])).toBeUndefined();
  });

  it('falls back to the lookback in the SQL when it gives up', () => {
    const sql = buildQueryMetricLogSamplesSQL(['a', 'b'], { startedAt: commonScanStart(['x', 'y']) });
    expect(sql).toMatch(/event_date >= toDate\(now/);
  });

  it('bounds every arm of a multi-query scan by the shared start', () => {
    const sql = buildQueryMetricLogSamplesSQL(['a', 'b'], {
      startedAt: commonScanStart(['2026-09-17 10:00:00', '2026-09-17 11:00:00']),
    });
    expect(sql.match(/event_date BETWEEN toDate\('2026-09-17 10:00:00'\)/g)).toHaveLength(2);
  });
});

describe('timelineMetricsExcluding', { tags: ['query-analysis'] }, () => {
  it('returns every metric when the source can populate all of them', () => {
    expect(timelineMetricsExcluding([])).toEqual(TIMELINE_METRICS);
  });

  it('drops a metric whose field the source cannot populate', () => {
    const ids = timelineMetricsExcluding(['d_read_mb']).map(m => m.id);
    expect(ids).not.toContain('d_read_mb');
    expect(ids).toContain('d_cpu_cores');
  });

  it('drops a multi-line metric when ANY of its lines is unavailable', () => {
    // Half a two-line chart is more misleading than no chart.
    const ids = timelineMetricsExcluding(['d_net_send_wait_s']).map(m => m.id);
    expect(ids).not.toContain('network_wait');
  });

  it('matches what the query_metric_log source declares missing', () => {
    const ids = timelineMetricsExcluding([...QUERY_METRIC_LOG_MISSING_FIELDS]).map(m => m.id);
    expect(ids).not.toContain('d_read_mb');
    expect(ids).toContain('memory_mb');
    expect(ids).toContain('d_cpu_cores');
  });
});
