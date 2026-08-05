import { describe, expect, it } from 'vitest';
import { buildColumnCommentsSQL } from '../query-queries.js';
import { escapeIdentifier } from '../builder.js';

/**
 * Split generated SQL into string-literal contents and the code around them,
 * using ClickHouse literal rules: a backslash escapes the next character, so
 * only an unescaped quote closes a literal.
 *
 * A value that stays inside its literal shows up verbatim in `literals` and
 * contributes nothing to `code`.
 */
function tokenize(sql: string): { literals: string[]; code: string } {
  const literals: string[] = [];
  let code = '';
  let i = 0;

  while (i < sql.length) {
    if (sql[i] !== "'") {
      code += sql[i];
      i++;
      continue;
    }

    i++; // opening quote
    let literal = '';
    while (i < sql.length && sql[i] !== "'") {
      if (sql[i] === '\\') {
        literal += sql[i + 1] ?? '';
        i += 2;
      } else {
        literal += sql[i];
        i++;
      }
    }
    i++; // closing quote
    literals.push(literal);
    code += '?';
  }

  return { literals, code };
}

describe('buildColumnCommentsSQL', { tags: ['security'] }, () => {
  it('keeps a backslash immediately before a quote inside the string literal', () => {
    // Recorded in query_log by:
    //   SELECT `x\' OR 1=1 --` FROM default.tracehouse_escape_test;
    // Doubling quotes without escaping backslashes lets ClickHouse read the
    // backslash as escaping the first quote, closing the literal early.
    const injected = "x\\' OR 1=1 --";

    const sql = buildColumnCommentsSQL([
      { database: 'default', table: 'tracehouse_escape_test', name: injected },
    ]);
    const { literals, code } = tokenize(sql);

    expect(literals).toEqual(['default', 'tracehouse_escape_test', injected]);
    expect(code).not.toContain('OR 1=1');
    expect(code).not.toContain('--');
  });

  it('contains injected database and table names in their literals', () => {
    const sql = buildColumnCommentsSQL([
      {
        database: "d\\' OR 1=1 --",
        table: "t\\' UNION ALL SELECT 1 --",
        name: 'user_id',
      },
    ]);
    const { literals, code } = tokenize(sql);

    expect(literals).toEqual(["d\\' OR 1=1 --", "t\\' UNION ALL SELECT 1 --", 'user_id']);
    expect(code).not.toContain('OR 1=1');
    expect(code).not.toContain('UNION ALL');
  });

  it('escapes trailing backslashes so the closing quote is not consumed', () => {
    const sql = buildColumnCommentsSQL([
      { database: 'default', table: 'events', name: 'col\\' },
    ]);
    const { literals } = tokenize(sql);

    expect(literals).toEqual(['default', 'events', 'col\\']);
  });

  it('emits one OR-ed condition per column for ordinary identifiers', () => {
    const sql = buildColumnCommentsSQL([
      { database: 'analytics', table: 'events', name: 'user_id' },
      { database: 'system', table: 'query_log', name: 'query_id' },
    ]);

    expect(sql).toContain("(c.database = 'analytics' AND c.table = 'events' AND c.name = 'user_id')");
    expect(sql).toContain("(c.database = 'system' AND c.table = 'query_log' AND c.name = 'query_id')");
    expect(sql).toContain(' OR ');
    expect(sql).toContain('length(c.comment) > 0');
  });

  it('never matches when there are no columns to look up', () => {
    expect(buildColumnCommentsSQL([])).toContain('WHERE (0)');
  });
});

describe('escapeIdentifier', { tags: ['security'] }, () => {
  /** Walk a backtick-quoted identifier and return where it actually ends. */
  function closingBacktick(escaped: string): number {
    let i = 0;
    while (i < escaped.length) {
      if (escaped[i] === '\\') i += 2;
      else if (escaped[i] === '`') return i;
      else i++;
    }
    return escaped.length;
  }

  it('keeps a backtick inside the quoted identifier', () => {
    const escaped = escapeIdentifier('tbl` UNION ALL SELECT 1 --');
    expect(closingBacktick(escaped)).toBe(escaped.length);
  });

  it('escapes a trailing backslash so it cannot consume the closing backtick', () => {
    const escaped = escapeIdentifier('tbl\\');
    expect(closingBacktick(escaped)).toBe(escaped.length);
    expect(escaped).toBe('tbl\\\\');
  });

  it('leaves ordinary identifiers untouched', () => {
    expect(escapeIdentifier('user_id')).toBe('user_id');
    expect(escapeIdentifier('system.query_log')).toBe('system.query_log');
  });
});
