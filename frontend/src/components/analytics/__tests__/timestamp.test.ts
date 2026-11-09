import { describe, expect, it } from 'vitest';
import { buildChartData, formatXTick } from '../charts';
import { formatAnalyticsTimestamp, formatAnalyticsCell, parseAnalyticsTimestamp } from '../timestamp';

describe('Analytics timestamp presentation', () => {
  it('retains all fractional digits when localizing exact identities', () => {
    const a = formatAnalyticsTimestamp('2026-11-08T17:00:00.123456Z');
    const b = formatAnalyticsTimestamp('2026-11-08T17:00:00.123457Z');
    expect(a).toMatch(/\.123456$/);
    expect(b).toMatch(/\.123457$/);
    expect(a).not.toBe(b);
  });
  it('localizes proven merge-attempt labels while retaining their raw chart/link keys', () => {
    const event_time = '2026-11-08T17:00:00.123456Z';
    const row = { event_time, host: 'fixture-host', attempt: `${event_time} fixture-host`, cpu: 2 };
    const expected = `${formatAnalyticsTimestamp(event_time)} fixture-host`;
    expect(formatAnalyticsCell(row.attempt, 'attempt', row)).toBe(expected);
    expect(formatAnalyticsCell(row.attempt, 'custom', row)).toBe(row.attempt);
    const chart = buildChartData([row], Object.keys(row), 'attempt', 'cpu');
    expect(chart[0].label).toBe(row.attempt);
    expect(chart[0].displayLabel).toBe(expected);
  });
  it('keeps explicitly zoned timestamps absolute and rejects impossible dates', () => {
    expect(parseAnalyticsTimestamp('2026-11-08T12:00:00.123456-05:00')).toBe(Date.parse('2026-11-08T17:00:00.123Z'));
    expect(formatAnalyticsTimestamp('2026-02-30T12:00:00Z')).toBe('2026-02-30T12:00:00Z');
  });

  it('does not reinterpret calendar dates, unmarked custom strings or labels', () => {
    for (const input of ['2026-11-08', '2026-11-08 18:00:00', 'weekly bucket', '42']) {
      expect(formatAnalyticsTimestamp(input)).toBe(input);
    }
    expect(formatXTick('weekly bucket')).toBe('weekly bucke…');
  });

  it('keeps two repeated-hour instants distinct as chart keys', () => {
    const times = ['2026-10-25T00:30:00Z', '2026-10-25T01:30:00Z'];
    const chart = buildChartData(times.map((clock, i) => ({ clock, value: i + 1 })), ['clock', 'value'], 'clock', 'value');
    expect(chart.map(point => point.label)).toEqual(times);
    expect(parseAnalyticsTimestamp(times[1]) - parseAnalyticsTimestamp(times[0])).toBe(3600000);
  });
});
