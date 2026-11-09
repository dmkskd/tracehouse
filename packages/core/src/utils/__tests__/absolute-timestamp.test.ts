import { describe, expect, it } from 'vitest';
import * as fc from 'fast-check';
import { hasExplicitOffset, parseAbsoluteTimestamp, parseUtcDateTimeInput } from '../absolute-timestamp.js';
import { normalizeTimestamp } from '../../mappers/timestamp.js';
import { absoluteTimestampCases, invalidAbsoluteTimestamps } from './fixtures/absolute-timestamps.js';

describe('parseAbsoluteTimestamp', { tags: ['storage'] }, () => {
  it.each(['Z', 'z', '+01:00', '-05:00', '+0100', '-0500'])('recognizes the explicit offset %s', offset => {
    expect(hasExplicitOffset(`2026-11-08T10:00:00${offset} `)).toBe(true);
  });

  it.each([undefined, null, '', '2026-11-08T10:00:00', '2026-11-08T10:00:00+01'])('does not infer an offset in %s', input => {
    expect(hasExplicitOffset(input)).toBe(false);
  });

  it('preserves UTC range inputs with date or minute precision', () => {
    expect(parseUtcDateTimeInput('2026-11-08')).toBe(Date.parse('2026-11-08T00:00:00Z'));
    expect(parseUtcDateTimeInput('2026-11-08 10:00-0500')).toBe(Date.parse('2026-11-08T15:00:00Z'));
  });
  it.each(absoluteTimestampCases)('preserves the instant in %s', (input, expected) => {
    expect(parseAbsoluteTimestamp(input)).toBe(Date.parse(expected));
  });

  it.each(invalidAbsoluteTimestamps)('rejects unsupported or invalid input %s', input => {
    expect(parseAbsoluteTimestamp(input)).toBeNaN();
  });

  it('retains instants for generated offsets and both separators', () => {
    fc.assert(fc.property(
      fc.integer({ min: 0, max: 4_102_444_799 }),
      fc.integer({ min: -840, max: 840 }),
      fc.boolean(), fc.boolean(),
      (epochSeconds, offsetMinutes, compact, space) => {
        const wallClock = new Date((epochSeconds + offsetMinutes * 60) * 1000)
          .toISOString().slice(0, 19).replace('T', space ? ' ' : 'T');
        const absMinutes = Math.abs(offsetMinutes);
        const offset = `${offsetMinutes < 0 ? '-' : '+'}${String(Math.floor(absMinutes / 60)).padStart(2, '0')}${compact ? '' : ':'}${String(absMinutes % 60).padStart(2, '0')}`;
        expect(parseAbsoluteTimestamp(wallClock + offset)).toBe(epochSeconds * 1000);
      },
    ), { numRuns: 200 });
  });

  it('uses the shared UTC contract after the producer migration gate', () => {
    const input = '2026-11-08 10:00:00';
    // Query, merge/mutation, part and lineage producers now guarantee UTC.
    expect(normalizeTimestamp(input)).toBe('2026-11-08T10:00:00.000Z');
    expect(parseAbsoluteTimestamp(input)).toBe(Date.parse('2026-11-08T10:00:00Z'));
    expect(normalizeTimestamp(1794139200)).toBe('2026-11-08T12:00:00.000Z');
    expect(normalizeTimestamp(1794139200000)).toBe('2026-11-08T12:00:00.000Z');
    expect(normalizeTimestamp(null)).toBe('');
  });
});
