import { describe, expect, it } from 'vitest';
import {
  TRACKER_TIME_PRESETS,
  resolveTrackerTimeRange,
  trackerTimeRangeHours,
} from '../trackerTimeRange';

describe('tracker time ranges', () => {
  it('only offers bounded one-click ranges', () => {
    expect(TRACKER_TIME_PRESETS).toEqual([
      { label: '15m', interval: '15 MINUTE' },
      { label: '1h', interval: '1 HOUR' },
      { label: '3h', interval: '3 HOUR' },
    ]);
  });

  it('resolves relative presets against the supplied current time', () => {
    expect(resolveTrackerTimeRange(
      '3 DAY',
      undefined,
      undefined,
      new Date('2026-07-26T20:00:00.000Z'),
    )).toEqual({
      startTime: '2026-07-23T20:00:00.000Z',
      endTime: '2026-07-26T20:00:00.000Z',
    });
  });

  it('resolves custom picker values as an absolute range', () => {
    const range = resolveTrackerTimeRange(
      'CUSTOM:2026-07-25T10:00,2026-07-26T14:30',
    );

    expect(Date.parse(range.endTime) - Date.parse(range.startTime))
      .toBe(28.5 * 60 * 60 * 1000);
    expect(trackerTimeRangeHours(
      'CUSTOM:2026-07-25T10:00,2026-07-26T14:30',
    )).toBe(28.5);
  });

  it('preserves a canonical UTC custom range exactly', () => {
    expect(resolveTrackerTimeRange(
      'CUSTOM:2026-07-25T10:00:00.000Z,2026-07-26T14:30:00.000Z',
    )).toEqual({
      startTime: '2026-07-25T10:00:00.000Z',
      endTime: '2026-07-26T14:30:00.000Z',
    });
  });

  it('retains legacy explicit dates when no relative range is present', () => {
    expect(resolveTrackerTimeRange(
      undefined,
      '2026-07-01T00:00:00.000Z',
      '2026-07-02T00:00:00.000Z',
    )).toEqual({
      startTime: '2026-07-01T00:00:00.000Z',
      endTime: '2026-07-02T00:00:00.000Z',
    });
  });
});
