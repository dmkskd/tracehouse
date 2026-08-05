import { describe, expect, it } from 'vitest';
import { buildColumnCommentsSQL } from '../query-queries.js';
import { escapeIdentifier } from '../builder.js';

/**
 * ClickHouse honours backslash escapes in string literals, so doubling quotes
 * is not enough: a value ending in a backslash before a quote closes the
 * literal early. escapeValue() escapes backslashes first, then quotes.
 *
 * escapeValue() itself is covered by builder.property.test.ts — these tests
 * only check that the query builders actually use it.
 */
describe('buildColumnCommentsSQL', { tags: ['security'] }, () => {
  it('escapes a column name whose backslash precedes a quote', () => {
    // Anyone who can create a table controls this name, and it reaches us via
    // query_log:  CREATE TABLE t (`x\' OR 1=1 --` String)
    const sql = buildColumnCommentsSQL([
      { database: 'default', table: 'events', name: String.raw`x\' OR 1=1 --` },
    ]);

    // Doubling the quote alone would emit  c.name = 'x\'' OR 1=1 --'
    // where ClickHouse reads \' as an escaped quote, ends the literal at the
    // next ', and executes the rest. Escaping the backslash first prevents it.
    expect(sql).toContain(String.raw`c.name = 'x\\\' OR 1=1 --'`);
    expect(sql).not.toContain(String.raw`c.name = 'x\'' OR 1=1 --'`);
  });

  it('escapes injected database and table names', () => {
    const sql = buildColumnCommentsSQL([
      { database: String.raw`d\'`, table: String.raw`t\'`, name: 'user_id' },
    ]);

    expect(sql).toContain(String.raw`c.database = 'd\\\''`);
    expect(sql).toContain(String.raw`c.table = 't\\\''`);
  });

  it('leaves ordinary names readable', () => {
    const sql = buildColumnCommentsSQL([
      { database: 'analytics', table: 'events', name: 'user_id' },
    ]);

    expect(sql).toContain("(c.database = 'analytics' AND c.table = 'events' AND c.name = 'user_id')");
  });

  it('never matches when there are no columns to look up', () => {
    expect(buildColumnCommentsSQL([])).toContain('WHERE (0)');
  });
});

describe('escapeIdentifier', { tags: ['security'] }, () => {
  it('escapes backticks and backslashes', () => {
    expect(escapeIdentifier('tbl`x')).toBe(String.raw`tbl\`x`);
    expect(escapeIdentifier('tbl\\')).toBe('tbl\\\\');
  });

  it('leaves ordinary identifiers untouched', () => {
    expect(escapeIdentifier('user_id')).toBe('user_id');
  });
});
