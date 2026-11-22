/** TabSeparated escape sequences other than an escaped character standing for itself. */
const TSV_ESCAPES: Record<string, string> = { b: '\b', f: '\f', r: '\r', n: '\n', t: '\t', '0': '\0' };

/**
 * Lines of a response to a query sent without FORMAT (EXPLAIN, CHECK GRANT), as the
 * text ClickHouse produced. The server answers in its default format, named in the
 * X-ClickHouse-Format header: JSONEachRow gives each row's first value, and
 * TabSeparated escapes backslashes, quotes and control characters, which are decoded.
 * A missing header is the server default, TabSeparated.
 */
export function rawResponseLines(text: string, format: string | null | undefined): string[] {
  const lines = text.split('\n').filter(line => line !== '');
  if (lines.length > 0 && lines[0].trimStart().startsWith('{')) {
    return lines.map(line => {
      try {
        const values = Object.values(JSON.parse(line) as Record<string, unknown>);
        return values.length > 0 ? String(values[0]) : line;
      } catch {
        return line;
      }
    });
  }
  if (format && format !== 'TabSeparated' && format !== 'TSV') return lines;
  return lines.map(line => line.replace(/\\(.)/g, (_, c: string) => TSV_ESCAPES[c] ?? c));
}

/** The response format header, from a header record or a fetch Headers object. */
export function responseFormat(headers: Record<string, unknown> | Headers | undefined): string | null {
  if (!headers) return null;
  if (typeof (headers as Headers).get === 'function') return (headers as Headers).get('x-clickhouse-format');
  const value = (headers as Record<string, unknown>)['x-clickhouse-format'];
  return typeof value === 'string' ? value : Array.isArray(value) ? String(value[0]) : null;
}
