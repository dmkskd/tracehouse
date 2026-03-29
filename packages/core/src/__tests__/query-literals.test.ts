import { describe, it, expect } from 'vitest';
import { extractLiterals, diffLiterals, formatLiteral, extractQueryParameters } from '../utils/query-literals.js';

describe('extractLiterals', { tags: ['query-analysis'] }, () => {
  it('extracts string literals', () => {
    const sql = "SELECT * FROM orders WHERE region = 'US' AND status = 'active'";
    const literals = extractLiterals(sql);
    expect(literals.map(l => l.value)).toEqual(["'US'", "'active'"]);
  });

  it('extracts numeric literals', () => {
    const sql = 'SELECT * FROM t WHERE id = 42 AND price > 3.14 LIMIT 100';
    const literals = extractLiterals(sql);
    expect(literals.map(l => l.value)).toEqual(['42', '3.14', '100']);
  });

  it('extracts mixed literals', () => {
    const sql = "SELECT * FROM orders WHERE date >= '2026-01-01' AND amount > 500 LIMIT 50";
    const literals = extractLiterals(sql);
    expect(literals.map(l => l.value)).toEqual(["'2026-01-01'", '500', '50']);
  });

  it('handles escaped quotes in strings', () => {
    const sql = "SELECT * FROM t WHERE name = 'it''s a test'";
    const literals = extractLiterals(sql);
    expect(literals.map(l => l.value)).toEqual(["'it''s a test'"]);
  });

  it('skips double-quoted identifiers', () => {
    const sql = 'SELECT "my column" FROM t WHERE id = 1';
    const literals = extractLiterals(sql);
    expect(literals.map(l => l.value)).toEqual(['1']);
  });

  it('skips backtick-quoted identifiers', () => {
    const sql = 'SELECT `my column` FROM t WHERE id = 1';
    const literals = extractLiterals(sql);
    expect(literals.map(l => l.value)).toEqual(['1']);
  });

  it('skips single-line comments', () => {
    const sql = "SELECT * FROM t -- WHERE id = 42\nWHERE name = 'test'";
    const literals = extractLiterals(sql);
    expect(literals.map(l => l.value)).toEqual(["'test'"]);
  });

  it('skips block comments', () => {
    const sql = "SELECT * FROM t /* WHERE id = 42 */ WHERE name = 'test'";
    const literals = extractLiterals(sql);
    expect(literals.map(l => l.value)).toEqual(["'test'"]);
  });

  it('does not match numbers inside identifiers', () => {
    const sql = "SELECT col1, col2 FROM table3 WHERE id = 5";
    const literals = extractLiterals(sql);
    expect(literals.map(l => l.value)).toEqual(['5']);
  });

  it('handles hex literals', () => {
    const sql = 'SELECT * FROM t WHERE flags = 0xFF';
    const literals = extractLiterals(sql);
    expect(literals.map(l => l.value)).toEqual(['0xFF']);
  });

  it('handles scientific notation', () => {
    const sql = 'SELECT * FROM t WHERE val > 1e6';
    const literals = extractLiterals(sql);
    expect(literals.map(l => l.value)).toEqual(['1e6']);
  });
});

describe('extractLiterals context', { tags: ['query-analysis'] }, () => {
  it('extracts column name context for WHERE clause', () => {
    const sql = "SELECT * FROM orders WHERE country_code = 'US'";
    const literals = extractLiterals(sql);
    expect(literals[0].context).toBe('country_code');
  });

  it('extracts LIMIT keyword context', () => {
    const sql = 'SELECT * FROM t LIMIT 100';
    const literals = extractLiterals(sql);
    expect(literals[0].context).toBe('LIMIT');
  });

  it('extracts context for comparison operators', () => {
    const sql = "SELECT * FROM t WHERE date >= '2026-01-01'";
    const literals = extractLiterals(sql);
    expect(literals[0].context).toBe('date');
  });

  it('extracts INTERVAL keyword context', () => {
    const sql = "SELECT * FROM t WHERE date > now() - INTERVAL 7 DAY";
    const literals = extractLiterals(sql);
    expect(literals[0].context).toBe('INTERVAL');
  });
});

describe('diffLiterals', { tags: ['query-analysis'] }, () => {
  it('finds no diffs for identical queries', () => {
    const sql = "SELECT * FROM orders WHERE region = 'US' LIMIT 100";
    expect(diffLiterals(sql, sql)).toEqual([]);
  });

  it('finds string literal diffs with context', () => {
    const ref = "SELECT * FROM orders WHERE region = 'US' AND date = '2026-01-01'";
    const cur = "SELECT * FROM orders WHERE region = 'EU' AND date = '2026-02-01'";
    const diffs = diffLiterals(ref, cur);
    expect(diffs).toEqual([
      { index: 0, reference: "'US'", current: "'EU'", context: 'region' },
      { index: 1, reference: "'2026-01-01'", current: "'2026-02-01'", context: 'date' },
    ]);
  });

  it('finds numeric literal diffs with context', () => {
    const ref = 'SELECT * FROM t WHERE id > 100 LIMIT 50';
    const cur = 'SELECT * FROM t WHERE id > 999 LIMIT 200';
    const diffs = diffLiterals(ref, cur);
    expect(diffs).toEqual([
      { index: 0, reference: '100', current: '999', context: 'id' },
      { index: 1, reference: '50', current: '200', context: 'LIMIT' },
    ]);
  });

  it('only reports changed positions', () => {
    const ref = "SELECT * FROM t WHERE a = 'x' AND b = 'y' AND c = 'z'";
    const cur = "SELECT * FROM t WHERE a = 'x' AND b = 'CHANGED' AND c = 'z'";
    const diffs = diffLiterals(ref, cur);
    expect(diffs).toEqual([
      { index: 1, reference: "'y'", current: "'CHANGED'", context: 'b' },
    ]);
  });
});

describe('formatLiteral', { tags: ['query-analysis'] }, () => {
  it('strips quotes from strings', () => {
    expect(formatLiteral("'hello'")).toBe('hello');
  });

  it('truncates long values', () => {
    expect(formatLiteral("'a very long string value here'", 10)).toBe('a very lon…');
  });

  it('passes through numbers', () => {
    expect(formatLiteral('42')).toBe('42');
  });

  it('handles escaped quotes', () => {
    expect(formatLiteral("'it''s'")).toBe("it's");
  });
});

describe('extractQueryParameters', { tags: ['query-analysis'] }, () => {
  it('returns literal values as strings', () => {
    const sql = "SELECT * FROM t WHERE id = 42 AND name = 'test'";
    expect(extractQueryParameters(sql)).toEqual(['42', "'test'"]);
  });
});
