// Fixture for `semgrep --test`. Each `ruleid:` line must be flagged by that
// rule; each `ok:` line must not be. Run: semgrep --test .semgrep/
//
// The first case is the original buildColumnCommentsSQL bug (CVE-style repro:
// a column name of  x\' OR 1=1 --  recorded in query_log).

declare function escapeValue(v: string): string;

export function vulnerable(columns: Array<{ database: string; name: string }>): string {
  // ruleid: clickhouse-incomplete-quote-escaping
  const quote = (value: string) => value.replace(/'/g, "''");
  return columns
    // ruleid: clickhouse-unescaped-sql-literal-interpolation
    .map(c => `(c.database = '${quote(c.database)}' AND c.name = '${quote(c.name)}')`)
    .join(' OR ');
}

export function alsoVulnerableBackslashOnly(part: string): string {
  // ruleid: clickhouse-incomplete-quote-escaping
  const escaped = part.replace(/'/g, "\\'");
  // ruleid: clickhouse-unescaped-sql-literal-interpolation
  return `WHERE _part = '${escaped}'`;
}

export function unescapedEntirely(queryId: string): string {
  // ruleid: clickhouse-unescaped-sql-literal-interpolation
  return `WHERE query_id = '${queryId}'`;
}

export function unescapedIdentifier(database: string, table: string): string {
  // ruleid: clickhouse-unescaped-identifier-interpolation
  return `SELECT count() FROM \`${database}\`.\`${table}\``;
}

export function fixed(columns: Array<{ database: string; name: string }>): string {
  return columns
    // ok: clickhouse-unescaped-sql-literal-interpolation
    .map(c => `(c.database = '${escapeValue(c.database)}' AND c.name = '${escapeValue(c.name)}')`)
    .join(' OR ');
}

const APP_RE_COMPONENT = 'source:TraceHouse:([A-Za-z]+)';
export function constantsAreFine(): string {
  // ok: clickhouse-unescaped-sql-literal-interpolation
  return `SELECT extractAllGroups(query, '${APP_RE_COMPONENT}')[1][1] AS component`;
}
