/**
 * QUERY TIMEZONE CONTRACT: use UTC for every timestamp instant going forward.
 * Bind absolute inputs with utcDateTime()/utcDateTime64(); project datetime
 * strings with toString(value, 'UTC'). Localize only for UI display. Preserve
 * microsecond identities as explicitly zoned strings, not JavaScript Dates.
 * This also applies to inline service SQL and Analytics SQL. Grafana and direct
 * clients serialize raw DateTime fields differently: verify both transports.
 * See ./README.md for alias, partition-pruning and legacy-identity rules.
 */
import { parseUtcDateTimeInput } from '../utils/absolute-timestamp.js';
import type { TaggedQuery } from '../adapters/types.js';

export interface UtcDateTimeParameter {
  readonly kind: 'utc_datetime';
  readonly value: string;
}

export interface UtcDateTime64Parameter {
  readonly kind: 'utc_datetime64';
  readonly value: string;
  readonly precision: number;
}

export interface EventDateBoundParameter {
  readonly kind: 'event_date_bound';
  readonly value: string;
}

/** Safely plumb the existing conservative partition-date helper into buildQuery. */
export function eventDateBoundParameter(value: string): EventDateBoundParameter {
  return { kind: 'event_date_bound', value };
}

export type QueryParameter =
  | string
  | number
  | UtcDateTimeParameter
  | UtcDateTime64Parameter
  | EventDateBoundParameter;

/**
 * Escape a string value for safe inclusion in SQL.
 * Prevents SQL injection by escaping single quotes and backslashes.
 */
export function escapeValue(value: string): string {
  return value.replace(/\\/g, '\\\\').replace(/'/g, "\\'");
}

/**
 * Escape an identifier for inclusion in a backtick-quoted name.
 *
 * ClickHouse identifiers may contain backticks and backslashes (both escaped
 * with a backslash), so database/table/column names read back from system
 * tables must be escaped before being interpolated into `db`.`table`.
 *
 * Use this for identifiers only — string values go through escapeValue().
 */
export function escapeIdentifier(name: string): string {
  return name.replace(/\\/g, '\\\\').replace(/`/g, '\\`');
}

function parseUtcDate(value: string | Date): Date {
  if (value instanceof Date) return value;
  return new Date(parseUtcDateTimeInput(value));
}

/**
 * Format an absolute instant as a whole-second UTC datetime.
 *
 * Offset-less ISO and ClickHouse datetime strings are treated as UTC. Browser
 * wall-clock values must be converted to an ISO string with an offset before
 * they reach this core boundary.
 */
export function formatUtcDateTime(value: string | Date): string {
  const date = parseUtcDate(value);

  if (!Number.isFinite(date.getTime())) {
    throw new Error(`Invalid datetime: ${String(value)}`);
  }

  return date.toISOString().slice(0, 19).replace('T', ' ');
}

/**
 * Mark a query parameter as an absolute UTC instant.
 *
 * buildQuery() renders this as an explicit ClickHouse expression instead of a
 * timezone-ambiguous string literal.
 */
export function utcDateTime(value: string | Date): UtcDateTimeParameter {
  return { kind: 'utc_datetime', value: formatUtcDateTime(value) };
}

/** Build a readable, executable ClickHouse UTC datetime expression. */
export function utcDateTimeLiteral(value: string | Date): string {
  return `toDateTime('${escapeValue(formatUtcDateTime(value))}', 'UTC')`;
}

/** Mark a query parameter as a millisecond-precision absolute UTC instant. */
export function utcDateTime64(
  value: string | Date,
  precision = 3,
): UtcDateTime64Parameter {
  if (!Number.isInteger(precision) || precision < 0 || precision > 9) {
    throw new Error(`Invalid DateTime64 precision: ${precision}`);
  }
  const date = parseUtcDate(value);
  if (!Number.isFinite(date.getTime())) {
    throw new Error(`Invalid datetime: ${String(value)}`);
  }
  return { kind: 'utc_datetime64', value: date.toISOString(), precision };
}

/** Build an explicit ClickHouse UTC DateTime64 expression. */
export function utcDateTime64Literal(
  value: string | Date,
  precision = 3,
): string {
  const parameter = utcDateTime64(value, precision);
  const iso = parameter.value;
  const whole = iso.slice(0, 19).replace('T', ' ');
  const milliseconds = iso.slice(20, 23);
  const fraction = precision === 0
    ? ''
    : `.${milliseconds.padEnd(precision, '0').slice(0, precision)}`;
  return `toDateTime64('${whole}${fraction}', ${precision}, 'UTC')`;
}

function formatParameter(value: QueryParameter): string {
  if (typeof value === 'string') return `'${escapeValue(value)}'`;
  if (typeof value === 'number') return String(value);
  if (value.kind === 'event_date_bound') return eventDateBound(value.value);
  return value.kind === 'utc_datetime'
    ? utcDateTimeLiteral(value.value)
    : utcDateTime64Literal(value.value, value.precision);
}

/**
 * Build a SQL query from a template and parameters.
 * Named placeholders like {database} or {limit:UInt32} are replaced with escaped values.
 * Supports ClickHouse-style typed placeholders like {name:Type}.
 * This escapes parameters; it does not validate or restrict the SQL statement.
 */
export function buildQuery(
  template: string,
  params: Record<string, QueryParameter>
): string {
  let sql = template;
  for (const [key, value] of Object.entries(params)) {
    const escaped = formatParameter(value);
    // Replace both {key} and {key:Type} formats
    sql = sql.replaceAll(`{${key}}`, () => escaped);
    // Also handle ClickHouse typed placeholders like {key:UInt32}
    const typedPattern = new RegExp(`\\{${key}:[^}]+\\}`, 'g');
    sql = sql.replace(typedPattern, escaped);
  }
  return sql;
}

/**
 * Append a SQL comment tag to a query for traceability.
 * The tag appears in system.query_log.query, making it searchable
 * via the existing query_text filter (positionCaseInsensitive).
 *
 * Works through both HttpAdapter and GrafanaAdapter since the comment
 * is part of the SQL string itself.
 *
 * @example
 * tagQuery('SELECT 1', sourceTag(TAB_OVERVIEW, 'serverMetrics'))
 * // returns: 'SELECT 1 \/\* source:TraceHouse:Overview:serverMetrics \*\/'
 */
export function tagQuery(sql: string, source: string): TaggedQuery {
  return `${sql.trimEnd()} /* source:${source} */` as TaggedQuery;
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
      return conservativeEventDateBound(`'${dateStr}'`);
    }
  }
  return `today() - ${fallbackDays}`;
}

/** One lower-bound policy; public helpers below admit only validated inputs. */
function conservativeEventDateBound(utcStartExpression: string): string {
  return `toDate(${utcStartExpression}) - 1`;
}

/** A date or explicitly UTC datetime placeholder in a query template. */
export function eventDateBoundForParameter(name: string): string {
  if (!/^[A-Za-z_]\w*$/.test(name)) throw new Error('Invalid date-bound parameter name');
  return conservativeEventDateBound(`{${name}}`);
}

/** Runtime windows use the same UTC-start/previous-date policy as absolute
 * ranges. A parameter reference is constrained to a UInt32 placeholder. */
export function eventDateBoundForWindow(seconds: number | { parameter: string }): string {
  let interval: string;
  if (typeof seconds === 'number') {
    if (!Number.isSafeInteger(seconds) || seconds < 0) throw new Error('Invalid date-bound window');
    interval = String(seconds);
  } else {
    if (!/^[A-Za-z_]\w*$/.test(seconds.parameter)) throw new Error('Invalid date-bound parameter name');
    interval = `{${seconds.parameter}:UInt32}`;
  }
  return conservativeEventDateBound(`toTimeZone(now(), 'UTC') - toIntervalSecond(${interval})`);
}
