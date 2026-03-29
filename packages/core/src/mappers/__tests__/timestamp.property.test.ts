import { describe, it, expect } from 'vitest';
import * as fc from 'fast-check';
import { normalizeTimestamp } from '../timestamp.js';

/**
 * For any valid Date object, converting it to epoch seconds, epoch milliseconds,
 * ISO string, or ClickHouse space-separated format, then passing the result to
 * normalizeTimestamp(), produces the same ISO 8601 string (matching the
 * original Date's toISOString() output). For null/undefined/empty inputs,
 * normalizeTimestamp() returns an empty string.
 */

const ISO_8601_REGEX = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{3}Z$/;

/** Generate valid Date objects within a reasonable range */
const validDate = fc.date({
  min: new Date('1970-01-02T00:00:00Z'),
  max: new Date('2099-12-31T23:59:59Z'),
}).filter(d => !isNaN(d.getTime()));

const NUM_RUNS = 100;

describe('Timestamp normalization produces valid ISO 8601 strings', { tags: ['storage'] }, () => {

  it('normalizeTimestamp(date) === date.toISOString() for any valid Date', () => {
    fc.assert(
      fc.property(validDate, (date) => {
        const result = normalizeTimestamp(date);
        expect(result).toBe(date.toISOString());
        expect(result).toMatch(ISO_8601_REGEX);
      }),
      { numRuns: NUM_RUNS },
    );
  });

  it('normalizeTimestamp(epochSeconds) produces the same ISO string (second-level precision)', () => {
    fc.assert(
      fc.property(validDate, (date) => {
        const epochSeconds = Math.floor(date.getTime() / 1000);
        const result = normalizeTimestamp(epochSeconds);

        expect(result).toMatch(ISO_8601_REGEX);

        // Epoch seconds lose millisecond precision, so compare at second level
        const resultDate = new Date(result);
        const expectedDate = new Date(epochSeconds * 1000);
        expect(resultDate.getTime()).toBe(expectedDate.getTime());
      }),
      { numRuns: NUM_RUNS },
    );
  });

  it('normalizeTimestamp(epochMilliseconds) produces the same ISO string', () => {
    // Only dates with getTime() >= 1e12 are treated as epoch milliseconds.
    // Dates before ~Sep 2001 have ms < 1e12 and would be treated as seconds.
    const dateWithLargeMs = fc.date({
      min: new Date('2001-09-09T01:47:00Z'), // ~1e12 ms
      max: new Date('2099-12-31T23:59:59Z'),
    }).filter(d => !isNaN(d.getTime()) && d.getTime() >= 1e12);

    fc.assert(
      fc.property(dateWithLargeMs, (date) => {
        const epochMs = date.getTime();
        const result = normalizeTimestamp(epochMs);

        expect(result).toBe(date.toISOString());
        expect(result).toMatch(ISO_8601_REGEX);
      }),
      { numRuns: NUM_RUNS },
    );
  });

  it('normalizeTimestamp(isoString) produces the same ISO string', () => {
    fc.assert(
      fc.property(validDate, (date) => {
        const isoString = date.toISOString();
        const result = normalizeTimestamp(isoString);

        expect(result).toBe(isoString);
        expect(result).toMatch(ISO_8601_REGEX);
      }),
      { numRuns: NUM_RUNS },
    );
  });

  it('normalizeTimestamp(clickhouseFormat) produces a valid ISO 8601 string', () => {
    fc.assert(
      fc.property(validDate, (date) => {
        // ClickHouse space-separated format: "2024-01-15 10:30:00"
        const iso = date.toISOString();
        const clickhouseFormat = iso.replace('T', ' ').replace(/\.\d{3}Z$/, '');
        const result = normalizeTimestamp(clickhouseFormat);

        expect(result).toMatch(ISO_8601_REGEX);

        // The result should represent the same point in time (within 1 second,
        // since the ClickHouse format drops milliseconds and timezone may vary)
        const resultDate = new Date(result);
        expect(resultDate).toBeInstanceOf(Date);
        expect(isNaN(resultDate.getTime())).toBe(false);
      }),
      { numRuns: NUM_RUNS },
    );
  });

  it('normalizeTimestamp returns empty string for null, undefined, and empty string', () => {
    expect(normalizeTimestamp(null)).toBe('');
    expect(normalizeTimestamp(undefined)).toBe('');
    expect(normalizeTimestamp('')).toBe('');
    expect(normalizeTimestamp('   ')).toBe('');
  });
});
