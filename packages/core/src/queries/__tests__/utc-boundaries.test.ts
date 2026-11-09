import { describe, expect, it } from 'vitest';
import { buildQuery, eventDateBoundParameter, eventDateBoundForWindow, utcDateTime } from '../builder.js';
import { GET_QUERY_MONITOR_STATS } from '../overview-queries.js';

describe('UTC range and partition boundaries', { tags: ['observability'] }, () => {
  it.each([
    ['2026-03-25', '2026-03-25 00:00:00'],
    ['2026-03-25T09:00', '2026-03-25 09:00:00'],
    ['2026-03-25T09:00+01:00', '2026-03-25 08:00:00'],
    ['2026-03-25 09:00-0500', '2026-03-25 14:00:00'],
  ])('retains the range boundary contract for %s', (input, utc) => {
    expect(buildQuery('SELECT {start}', { start: utcDateTime(input) })).toBe(`SELECT toDateTime('${utc}', 'UTC')`);
  });

  it('uses the common conservative partition bound without quoting its SQL expression', () => {
    expect(buildQuery('event_date >= {start_date}', { start_date: eventDateBoundParameter('2026-11-08T00:30:00Z') }))
      .toBe("event_date >= toDate('2026-11-08') - 1");
  });

  it('uses the actual one-hour rejection window and fifteen-minute QPS window', () => {
    expect(GET_QUERY_MONITOR_STATS).toContain(`event_date >= ${eventDateBoundForWindow(3600)}`);
    expect(GET_QUERY_MONITOR_STATS).toContain(`event_date >= ${eventDateBoundForWindow(900)}`);
    expect(GET_QUERY_MONITOR_STATS).not.toContain('- 1 - 1');
  });
});
