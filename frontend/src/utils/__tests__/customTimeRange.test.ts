import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import {
  createCustomTimeRange,
  customTimeRangeToLocalInputs,
  resolveCustomTimeRange,
} from '../customTimeRange';

const ORIGINAL_TZ = process.env.TZ;

describe.sequential('custom time range UTC contract', () => {
  beforeEach(() => {
    process.env.TZ = 'Europe/London';
  });

  afterAll(() => {
    process.env.TZ = ORIGINAL_TZ;
  });

  it('normalizes a London summer wall clock using BST', () => {
    expect(createCustomTimeRange('2026-07-27T14:13', '2026-07-27T15:05')).toBe(
      'CUSTOM:2026-07-27T13:13:00.000Z,2026-07-27T14:05:00.000Z',
    );
  });

  it('normalizes a London winter wall clock using GMT', () => {
    expect(createCustomTimeRange('2026-01-27T14:13', '2026-01-27T15:05')).toBe(
      'CUSTOM:2026-01-27T14:13:00.000Z,2026-01-27T15:05:00.000Z',
    );
  });

  it('accepts legacy ClickHouse-shaped wall-clock values as browser-local', () => {
    expect(resolveCustomTimeRange(
      'CUSTOM:2026-07-27 14:13:00,2026-07-27 15:05:00',
    )).toEqual({
      startTime: '2026-07-27T13:13:00.000Z',
      endTime: '2026-07-27T14:05:00.000Z',
    });
  });

  it('uses the browser timezone rather than a hard-coded user or server timezone', () => {
    process.env.TZ = 'America/New_York';
    expect(createCustomTimeRange('2026-07-27T14:13', '2026-07-27T15:05')).toBe(
      'CUSTOM:2026-07-27T18:13:00.000Z,2026-07-27T19:05:00.000Z',
    );
  });

  it('preserves canonical instants when read in another browser timezone', () => {
    process.env.TZ = 'Europe/Berlin';
    expect(resolveCustomTimeRange(
      'CUSTOM:2026-07-27T13:13:00.000Z,2026-07-27T14:05:00.000Z',
    )).toEqual({
      startTime: '2026-07-27T13:13:00.000Z',
      endTime: '2026-07-27T14:05:00.000Z',
    });
    expect(customTimeRangeToLocalInputs(
      'CUSTOM:2026-07-27T13:13:00.000Z,2026-07-27T14:05:00.000Z',
    )).toEqual({
      start: '2026-07-27T15:13',
      end: '2026-07-27T16:05',
    });
  });

  it('handles the UK spring DST transition as absolute elapsed time', () => {
    expect(resolveCustomTimeRange(
      'CUSTOM:2026-03-29T00:30,2026-03-29T02:30',
    )).toEqual({
      startTime: '2026-03-29T00:30:00.000Z',
      endTime: '2026-03-29T01:30:00.000Z',
    });
  });

  it('rejects incomplete, invalid, and reversed ranges', () => {
    expect(resolveCustomTimeRange('CUSTOM:2026-07-27T14:13,')).toBeNull();
    expect(resolveCustomTimeRange('CUSTOM:not-a-date,2026-07-27T15:05')).toBeNull();
    expect(resolveCustomTimeRange('CUSTOM:2026-07-27T15:05,2026-07-27T14:13')).toBeNull();
  });
});
