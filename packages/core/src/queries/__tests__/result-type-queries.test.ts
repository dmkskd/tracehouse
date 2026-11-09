import { describe, expect, it } from 'vitest';
import { describeResultTypesQuery } from '../result-type-queries.js';

describe('Grafana native result metadata SQL', () => {
  it.each([
    'SELECT now() AS t;\n-- note',
    'SELECT now() AS t FORMAT JSON',
    'SELECT now() AS t FORMAT JSON; -- note\n/* source:TraceHouse:Analytics:test */',
    'SELECT now() AS t /* nested /* comment */ ok */; # note',
  ])('removes output syntax and trailing comments from %s', input => {
    expect(describeResultTypesQuery(input)).toMatch(/^DESCRIBE TABLE \(\nSELECT now\(\) AS t(?: \/\* nested.*\*\/)?\n\)$/);
  });
  it('preserves quoted syntax, nested expressions, heredocs and settings', () => {
    const expression = "SELECT '; FORMAT JSON' AS text, (SELECT 'FORMAT JSON') AS nested, $tag$; FORMAT JSON$tag$ AS raw";
    expect(describeResultTypesQuery(expression + ' FORMAT JSON SETTINGS max_threads=1')).toContain(expression + '  SETTINGS max_threads=1');
  });
  it('rejects multiple statements without splitting literals', () => {
    expect(() => describeResultTypesQuery('SELECT 1; SELECT 2')).toThrow('one SQL statement');
    expect(describeResultTypesQuery("SELECT ';' AS text;")).toContain("SELECT ';' AS text");
  });
});
