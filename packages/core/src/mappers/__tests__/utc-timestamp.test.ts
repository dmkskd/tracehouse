import { describe, expect, it } from 'vitest';
import { normalizeTimestamp } from '../timestamp.js';
import { mapQueryHistoryItem } from '../query-mappers.js';
import { mapMergeHistoryRecord, mapMutationInfo, mapMutationHistoryRecord } from '../merge-mappers.js';

describe('Explicit UTC mapper migration', { tags: ['query-analysis'] }, () => {
  it('hides absent system failure clocks without invalidating epoch zero generally', () => {
    for (const map of [mapMutationInfo, mapMutationHistoryRecord]) {
      expect(map({ latest_fail_time_utc: '1970-01-01 00:00:00Z' }).latest_fail_time).toBe('');
      expect(map({ latest_fail_time_utc: '2026-11-08 17:00:00Z' }).latest_fail_time).toBe('2026-11-08T17:00:00.000Z');
    }
    expect(normalizeTimestamp('1970-01-01 00:00:00Z')).toBe('1970-01-01T00:00:00.000Z');
  });
  it('prefers guaranteed UTC projections over retained typed server-local columns', () => {
    expect(mapMergeHistoryRecord({ event_time: '2026-11-08 18:00:00', event_time_utc: '2026-11-08 17:00:00' }).event_time)
      .toBe('2026-11-08T17:00:00.000Z');
    for (const map of [mapMutationInfo, mapMutationHistoryRecord]) {
      const row = map({ create_time: '2026-11-08 18:00:00', create_time_utc: '2026-11-08 17:00:00',
        latest_fail_time: '2026-11-08 18:01:00', latest_fail_time_utc: '2026-11-08 17:01:00' });
      expect(row.create_time).toBe('2026-11-08T17:00:00.000Z');
      expect(row.latest_fail_time).toBe('2026-11-08T17:01:00.000Z');
    }
  });
  it('maps UTC strings and signed offsets to the same absolute history instant', () => {
    for (const input of ['2026-11-08 17:00:00', '2026-11-08T17:00:00', '2026-11-08T18:00:00+01:00', '2026-11-08T12:00:00-05:00']) {
      expect(mapQueryHistoryItem({ query_start_time: input }).query_start_time).toBe('2026-11-08T17:00:00.000Z');
    }
  });

  it('preserves epoch units, Date values and invalid-value policy', () => {
    for (const input of [1794157200, 1794157200000, '1794157200', '1794157200000', new Date('2026-11-08T17:00:00Z')]) {
      expect(normalizeTimestamp(input)).toBe('2026-11-08T17:00:00.000Z');
    }
    for (const input of [null, undefined, '', 'bad', '2026-02-30 10:00:00', new Date(NaN)]) {
      expect(normalizeTimestamp(input)).toBe('');
    }
  });

  it('keeps the mapper usable as an Array.map callback', () => {
    const rows = [{ query_start_time: '2026-11-08T17:00:00Z' }];
    expect(rows.map(mapQueryHistoryItem)[0].query_start_time).toBe('2026-11-08T17:00:00.000Z');
  });
});
