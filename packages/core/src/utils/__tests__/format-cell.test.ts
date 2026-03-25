import { describe, it, expect } from 'vitest';
import { detectTimestamp, timestampToDate, formatCell } from '../format-cell.js';

describe('detectTimestamp', () => {
  it('detects epoch seconds in valid range', () => {
    // 2026-03-25 ~= 1774450000
    expect(detectTimestamp(1_774_450_000)).toBe('seconds');
  });

  it('detects epoch milliseconds in valid range', () => {
    expect(detectTimestamp(1_774_450_000_000)).toBe('milliseconds');
  });

  it('returns null for small integers (not timestamps)', () => {
    expect(detectTimestamp(0)).toBeNull();
    expect(detectTimestamp(42)).toBeNull();
    expect(detectTimestamp(1_000_000)).toBeNull();
  });

  it('returns null for out-of-range values', () => {
    // Before 2000-01-01
    expect(detectTimestamp(900_000_000)).toBeNull();
    // After 2100-01-01
    expect(detectTimestamp(5_000_000_000)).toBeNull();
  });

  it('returns null for non-integers', () => {
    expect(detectTimestamp(1_774_450_000.5)).toBeNull();
  });

  it('handles boundary values', () => {
    // Just inside seconds range
    expect(detectTimestamp(946_684_801)).toBe('seconds');
    expect(detectTimestamp(4_102_444_799)).toBe('seconds');
    // Just outside seconds range
    expect(detectTimestamp(946_684_800)).toBeNull();
    expect(detectTimestamp(4_102_444_800)).toBeNull();
  });

  it('handles boundary values for milliseconds', () => {
    expect(detectTimestamp(946_684_800_001)).toBe('milliseconds');
    expect(detectTimestamp(4_102_444_799_999)).toBe('milliseconds');
  });

  it('does not misclassify large non-timestamp integers', () => {
    // A count or byte value that happens to be large
    expect(detectTimestamp(10_000_000_000_000)).toBeNull();
  });
});

describe('timestampToDate', () => {
  it('converts epoch seconds to Date', () => {
    const d = timestampToDate(1_774_450_000);
    expect(d).toBeInstanceOf(Date);
    expect(d!.getUTCFullYear()).toBe(2026);
  });

  it('converts epoch milliseconds to Date', () => {
    const d = timestampToDate(1_774_450_000_000);
    expect(d).toBeInstanceOf(Date);
    expect(d!.getUTCFullYear()).toBe(2026);
  });

  it('returns same date for equivalent seconds and milliseconds', () => {
    const fromSec = timestampToDate(1_774_450_000);
    const fromMs = timestampToDate(1_774_450_000_000);
    expect(fromSec!.getTime()).toBe(fromMs!.getTime());
  });

  it('returns null for non-timestamp values', () => {
    expect(timestampToDate(42)).toBeNull();
    expect(timestampToDate(0)).toBeNull();
  });
});

describe('formatCell', () => {
  it('formats null/undefined as dash', () => {
    expect(formatCell(null)).toBe('—');
    expect(formatCell(undefined)).toBe('—');
  });

  it('formats regular integers with locale separators', () => {
    expect(formatCell(42)).toBe('42');
    expect(formatCell(1000)).toMatch(/1.000|1,000/); // locale-dependent
  });

  it('formats floats with max 2 decimal places', () => {
    const result = formatCell(3.14159);
    expect(result).toMatch(/3\.14/);
  });

  it('formats epoch seconds as ISO date string', () => {
    const result = formatCell(1_774_450_000);
    // Should be ISO-ish format: "YYYY-MM-DD HH:MM:SS"
    expect(result).toMatch(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/);
    expect(result).toContain('2026');
  });

  it('formats epoch milliseconds as ISO date string', () => {
    const result = formatCell(1_774_450_000_000);
    expect(result).toMatch(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/);
    expect(result).toContain('2026');
  });

  it('formats arrays as comma-separated', () => {
    expect(formatCell(['a', 'b', 'c'])).toBe('a, b, c');
  });

  it('formats strings as-is', () => {
    expect(formatCell('hello')).toBe('hello');
  });
});
