/**
 * Return the first SQL keyword after whitespace and ClickHouse-supported
 * leading comments. This is intentionally a lightweight eligibility check,
 * not a general SQL parser.
 */
export function leadingSqlKeyword(sql: string): string | null {
  let rest = sql;

  while (rest.length > 0) {
    rest = rest.trimStart();

    if (rest.startsWith('--') || rest.startsWith('#')) {
      const newline = rest.indexOf('\n');
      if (newline < 0) return null;
      rest = rest.slice(newline + 1);
      continue;
    }

    if (rest.startsWith('/*')) {
      const end = rest.indexOf('*/', 2);
      if (end < 0) return null;
      rest = rest.slice(end + 2);
      continue;
    }

    break;
  }

  return rest.match(/^([A-Za-z_]+)/)?.[1]?.toUpperCase() ?? null;
}

/**
 * Whether SQL is eligible for ClickHouse EXPLAIN ANALYZE.
 *
 * WITH is accepted because ClickHouse SELECT statements may begin with CTEs.
 * ClickHouse remains the final parser and rejects unsupported WITH forms
 * without executing them.
 */
export function isSelectStatement(sql: string): boolean {
  const keyword = leadingSqlKeyword(sql);
  return keyword === 'SELECT' || keyword === 'WITH';
}
