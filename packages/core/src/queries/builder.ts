/**
 * Escape a string value for safe inclusion in SQL.
 * Prevents SQL injection by escaping single quotes and backslashes.
 */
export function escapeValue(value: string): string {
  return value.replace(/\\/g, '\\\\').replace(/'/g, "\\'");
}

/**
 * Build a SQL query from a template and parameters.
 * Named placeholders like {database} or {limit:UInt32} are replaced with escaped values.
 * Supports ClickHouse-style typed placeholders like {name:Type}.
 */
export function buildQuery(
  template: string,
  params: Record<string, string | number>
): string {
  let sql = template;
  for (const [key, value] of Object.entries(params)) {
    const escaped =
      typeof value === 'string' ? `'${escapeValue(value)}'` : String(value);
    // Replace both {key} and {key:Type} formats
    sql = sql.replaceAll(`{${key}}`, () => escaped);
    // Also handle ClickHouse typed placeholders like {key:UInt32}
    const typedPattern = new RegExp(`\\{${key}:[^}]+\\}`, 'g');
    sql = sql.replace(typedPattern, escaped);
  }
  return sql;
}

/**
 * Prepend a SQL comment tag to a query for traceability.
 * The tag appears in system.query_log.query, making it searchable
 * via the existing query_text filter (positionCaseInsensitive).
 *
 * Works through both HttpAdapter and GrafanaAdapter since the comment
 * is part of the SQL string itself.
 *
 * @example
 * tagQuery('SELECT 1', sourceTag(TAB_OVERVIEW, 'serverMetrics'))
 * // returns: 'SELECT 1 \/\* source:Overview:serverMetrics \*\/'
 */
export function tagQuery(sql: string, source: string): string {
  return `${sql.trimEnd()} /* source:${source} */`;
}

/**
 * Compute the event_date lower bound for partition pruning.
 *
 * When the caller knows the query's start time (e.g. from query_start_time in
 * the history list), we use that date minus 1 day as the bound. The 1-day buffer
 * covers timezone differences and queries that span midnight.
 *
 * When no date is known (e.g. manual query ID entry), falls back to
 * `today() - <fallbackDays>` (default 7, matching the default system log TTL).
 *
 * Returns a raw SQL expression suitable for `event_date >= <result>`.
 */
export function eventDateBound(eventDate?: string, fallbackDays = 7): string {
  if (eventDate) {
    // Extract YYYY-MM-DD from ISO string or ClickHouse datetime
    const dateStr = eventDate.slice(0, 10);
    if (/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) {
      return `toDate('${dateStr}') - 1`;
    }
  }
  return `today() - ${fallbackDays}`;
}
