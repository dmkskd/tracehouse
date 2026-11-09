import { describe, expect, it } from 'vitest';
import { rowToQuerySeries } from '../query-series';

describe('Analytics linked query clocks', () => {
  it('uses the finish clock when start columns are present but empty', () => {
    const result = rowToQuerySeries({ query_id: 'query', query_start_time: '', query_start_time_microseconds: '',
      event_time: '2026-11-08T17:00:02Z', query_duration_ms: 2000 });
    expect(result.start_time).toBe('2026-11-08T17:00:00.000Z');
    expect(result.end_time).toBe('2026-11-08T17:00:02.000Z');
  });
  it('uses a completed query finish clock as the end of its interval', () => {
    const result = rowToQuerySeries({ query_id: 'query', event_time: '2026-11-08 17:00:02Z', query_duration_ms: 2000 });
    expect(result.start_time).toBe('2026-11-08T17:00:00.000Z');
    expect(result.end_time).toBe('2026-11-08T17:00:02.000Z');
    expect(result.query_id).toBe('query');
  });

  it('prefers the explicit start clock, including microseconds and offsets', () => {
    const result = rowToQuerySeries({ query_id: 'query', query_start_time_microseconds: '2026-11-08 12:00:00.123456-05:00',
      event_time: '2026-11-08T17:00:02Z', query_duration_ms: 2000 });
    expect(result.start_time).toBe('2026-11-08T17:00:00.123Z');
    expect(result.end_time).toBe('2026-11-08T17:00:02.123Z');
  });
});
