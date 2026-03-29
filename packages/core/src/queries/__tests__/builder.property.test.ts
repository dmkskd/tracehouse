import { describe, it, expect } from 'vitest';
import * as fc from 'fast-check';
import { escapeValue } from '../builder.js';

/**
 * For any string value, escapeValue(value) produces a string where all
 * single quote characters are preceded by a backslash and all backslash
 * characters are doubled. Furthermore, wrapping the escaped value in single
 * quotes and embedding it in a SQL template does not allow the value to
 * break out of the string literal context.
 */

/**
 * Unescape a value that was escaped by escapeValue.
 * Reverses: \' → ' then \\\\ → \\
 */
function unescapeValue(escaped: string): string {
  return escaped.replace(/\\'/g, "'").replace(/\\\\/g, '\\');
}

/**
 * Parse a SQL string literal to find where the quoted value ends.
 * Walks character by character from the start, treating \' and \\\\ as
 * escaped sequences. Returns the index of the closing quote, or -1 if
 * the value breaks out of the string literal context.
 */
function findClosingQuote(escaped: string): number {
  let i = 0;
  while (i < escaped.length) {
    if (escaped[i] === '\\') {
      // Skip the escaped character
      i += 2;
    } else if (escaped[i] === "'") {
      // Unescaped single quote — this is the closing quote position
      return i;
    } else {
      i++;
    }
  }
  // Reached end without finding a closing quote — value is contained
  return escaped.length;
}

describe('SQL escapeValue neutralizes injection characters', { tags: ['security'] }, () => {
  const NUM_RUNS = 100;

  it('every single quote in the output is preceded by a backslash', () => {
    fc.assert(
      fc.property(fc.string(), (input) => {
        const escaped = escapeValue(input);
        for (let i = 0; i < escaped.length; i++) {
          if (escaped[i] === "'" ) {
            expect(i).toBeGreaterThan(0);
            expect(escaped[i - 1]).toBe('\\');
          }
        }
      }),
      { numRuns: NUM_RUNS },
    );
  });

  it('every backslash in the original input is doubled in the output', () => {
    fc.assert(
      fc.property(fc.string(), (input) => {
        const escaped = escapeValue(input);
        // Count backslashes in input
        const inputBackslashes = (input.match(/\\/g) || []).length;
        // Count double-backslashes in output (escaped backslashes)
        const outputDoubleBackslashes = (escaped.match(/\\\\/g) || []).length;
        // The number of escaped backslash pairs should match the input count
        // Note: some \\ pairs come from escaping quotes (\'), so we count differently.
        // Instead, verify by round-trip: unescape should recover original.
        // Direct check: every \\ in the output that is NOT followed by ' is a doubled backslash.
        // Simpler: count original backslashes and verify they appear doubled.
        
        // Walk the escaped string and count actual backslash-backslash pairs
        // vs backslash-quote pairs
        let bsCount = 0;
        let i = 0;
        while (i < escaped.length) {
          if (escaped[i] === '\\') {
            if (i + 1 < escaped.length && escaped[i + 1] === '\\') {
              bsCount++;
              i += 2;
            } else if (i + 1 < escaped.length && escaped[i + 1] === "'") {
              i += 2;
            } else {
              i++;
            }
          } else {
            i++;
          }
        }
        expect(bsCount).toBe(inputBackslashes);
      }),
      { numRuns: NUM_RUNS },
    );
  });

  it('the escaped value cannot break out of a single-quoted string literal context', () => {
    fc.assert(
      fc.property(fc.string(), (input) => {
        const escaped = escapeValue(input);
        // Simulate embedding: SELECT * FROM t WHERE col = '<escaped>'
        // The escaped value should not contain an unescaped single quote
        // that would terminate the string literal early.
        const closingPos = findClosingQuote(escaped);
        // If the value is properly escaped, we should reach the end of the
        // string without finding an unescaped quote
        expect(closingPos).toBe(escaped.length);
      }),
      { numRuns: NUM_RUNS },
    );
  });

  it('round-trip: unescaping the output recovers the original value', () => {
    fc.assert(
      fc.property(fc.string(), (input) => {
        const escaped = escapeValue(input);
        const recovered = unescapeValue(escaped);
        expect(recovered).toBe(input);
      }),
      { numRuns: NUM_RUNS },
    );
  });
});

import { buildQuery } from '../builder.js';

/**
 * For any SQL template containing named placeholders {key} and a corresponding
 * parameter map with entries for every placeholder, buildQuery(template, params)
 * returns a string containing no remaining {key} placeholders, where each
 * string parameter value is wrapped in escaped single quotes and each numeric
 * parameter value appears as its numeric string representation.
 */

/**
 * Arbitrary for alphanumeric non-empty key names (valid placeholder identifiers).
 */
const keyArb = fc.stringMatching(/^[a-zA-Z_][a-zA-Z0-9_]{0,19}$/);

/**
 * Arbitrary for parameter values — either a string or a finite number.
 */
const paramValueArb: fc.Arbitrary<string | number> = fc.oneof(
  fc.string(),
  fc.double({ noNaN: true, noDefaultInfinity: true }),
  fc.integer(),
);

/**
 * Generate a params record with 1-5 unique keys and associated values,
 * plus a template string that contains all those placeholders embedded
 * in surrounding text.
 */
const templateAndParamsArb = keyArb
  .chain((firstKey) =>
    fc.tuple(
      fc.uniqueArray(keyArb, { minLength: 0, maxLength: 4 }),
      fc.array(paramValueArb, { minLength: 1, maxLength: 5 }),
    ).map(([extraKeys, values]) => {
      const allKeys = [firstKey, ...extraKeys.filter((k) => k !== firstKey)];
      const params: Record<string, string | number> = {};
      for (let i = 0; i < allKeys.length; i++) {
        params[allKeys[i]] = values[i % values.length];
      }
      // Build a template that uses every key at least once
      const template = allKeys.map((k) => `SELECT {${k}}`).join(' UNION ALL ');
      return { template, params };
    }),
  );

describe('buildQuery substitutes all placeholders with escaped values', { tags: ['security'] }, () => {
  const NUM_RUNS = 100;

  it('no remaining {key} placeholders exist in the output when all keys are provided', () => {
    fc.assert(
      fc.property(templateAndParamsArb, ({ template, params }) => {
        const result = buildQuery(template, params);
        // Verify no placeholder from the params remains
        for (const key of Object.keys(params)) {
          expect(result).not.toContain(`{${key}}`);
        }
      }),
      { numRuns: NUM_RUNS },
    );
  });

  it('string parameter values appear wrapped in escaped single quotes', () => {
    fc.assert(
      fc.property(
        keyArb,
        fc.string({ minLength: 1 }),
        (key, value) => {
          const template = `SELECT {${key}} FROM t`;
          const result = buildQuery(template, { [key]: value });
          const escaped = escapeValue(value);
          // The result should contain the value wrapped in single quotes
          expect(result).toContain(`'${escaped}'`);
          // And no leftover placeholder
          expect(result).not.toContain(`{${key}}`);
        },
      ),
      { numRuns: NUM_RUNS },
    );
  });

  it('numeric parameter values appear as their string representation without quotes', () => {
    fc.assert(
      fc.property(
        keyArb,
        fc.oneof(
          fc.integer(),
          fc.double({ noNaN: true, noDefaultInfinity: true }),
        ),
        (key, value) => {
          const template = `SELECT {${key}} FROM t`;
          const result = buildQuery(template, { [key]: value });
          const numStr = String(value);
          // The result should contain the numeric string representation
          expect(result).toContain(numStr);
          // And should NOT wrap it in quotes
          expect(result).not.toContain(`'${numStr}'`);
          // And no leftover placeholder
          expect(result).not.toContain(`{${key}}`);
        },
      ),
      { numRuns: NUM_RUNS },
    );
  });

  it('all placeholders are substituted even when the same key appears multiple times', () => {
    fc.assert(
      fc.property(
        keyArb,
        paramValueArb,
        fc.integer({ min: 2, max: 5 }),
        (key, value, count) => {
          // Build a template with the same placeholder repeated `count` times
          const template = Array.from({ length: count }, () => `{${key}}`).join(' AND ');
          const result = buildQuery(template, { [key]: value });
          expect(result).not.toContain(`{${key}}`);
        },
      ),
      { numRuns: NUM_RUNS },
    );
  });
});
