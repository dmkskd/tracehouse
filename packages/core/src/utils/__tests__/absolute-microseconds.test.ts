import { describe, expect, it } from 'vitest';
import { parseAbsoluteTimestampMicros, parseOptionalTimestampMicros, normalizeAbsoluteTimestamp } from '../absolute-timestamp.js';
import { parseOperationTimestamp } from '../timeline-operations.js';
import { parseTimeValue } from '../time.js';

describe('Shared timestamp precision and wrapper contracts', { tags: ['observability'] }, () => {
  it('retains the optional trace and diagnostic event parsing policies', () => {
    expect(parseOptionalTimestampMicros(undefined)).toBe(0);
    expect(parseOptionalTimestampMicros('')).toBe(0);
    expect(parseOptionalTimestampMicros('bad')).toBeNaN();
    expect(normalizeAbsoluteTimestamp(' bad ')).toBe('bad');
    expect(normalizeAbsoluteTimestamp('2026-11-08 12:00:00-05:00')).toBe('2026-11-08T17:00:00.000Z');
  });
  it('retains microsecond differences for explicitly zoned and UTC string inputs', () => {
    const a = parseAbsoluteTimestampMicros('2026-11-08 12:00:00.123456-05:00');
    expect(a).toBe(1794157200123456);
    expect(parseAbsoluteTimestampMicros('2026-11-08T17:00:00.123457Z') - a).toBe(1);
    expect(parseAbsoluteTimestampMicros('2026-11-08 17:00:00.123456789')).toBe(a);
    expect(parseAbsoluteTimestampMicros('1969-12-31T23:59:59.123456Z')).toBe(-876544);
    expect(parseAbsoluteTimestampMicros('bad')).toBeNaN();
  });

  it('treats unmarked ISO as UTC and respects negative offsets across wrappers', () => {
    for (const value of ['2026-11-08T17:00:00', '2026-11-08 12:00:00-05:00', '2026-11-08T18:00:00+0100']) {
      expect(parseOperationTimestamp(value)).toBe(1794157200000);
      expect(parseTimeValue(value).timeMs).toBe(1794157200000);
    }
    expect(parseTimeValue('1794157200000').timeMs).toBe(1794157200000);
    expect(parseTimeValue(1794157200000).timeMs).toBe(1794157200000);
  });
});
