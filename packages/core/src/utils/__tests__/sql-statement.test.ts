import { describe, expect, it } from 'vitest';
import { isSelectStatement, leadingSqlKeyword } from '../sql-statement.js';

describe('SQL statement eligibility', () => {
  it('recognizes SELECT after whitespace and leading comments', () => {
    expect(isSelectStatement(' SELECT 1')).toBe(true);
    expect(isSelectStatement('-- generated query\nSELECT 1')).toBe(true);
    expect(isSelectStatement('# generated query\nSELECT 1')).toBe(true);
    expect(isSelectStatement('/* metadata */\nSELECT 1')).toBe(true);
    expect(leadingSqlKeyword('/* one */ -- two\n SELECT 1')).toBe('SELECT');
  });

  it('recognizes CTE-based SELECT statements', () => {
    expect(isSelectStatement('WITH recent AS (SELECT 1) SELECT * FROM recent')).toBe(true);
  });

  it('rejects non-SELECT and malformed input', () => {
    expect(isSelectStatement('INSERT INTO events SELECT * FROM staging')).toBe(false);
    expect(isSelectStatement('ALTER TABLE events DELETE WHERE 1')).toBe(false);
    expect(isSelectStatement('/* unterminated')).toBe(false);
    expect(isSelectStatement('')).toBe(false);
  });
});
