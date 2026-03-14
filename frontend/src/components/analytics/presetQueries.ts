/**
 * Query management (preset + custom).
 *
 * - Defines the Query type (composes ParsedDirectives)
 * - Parses raw SQL via metaLanguage and maps to Query
 * - Custom query CRUD (localStorage)
 * - Template resolution ({{time_range}}, {{drill:...}})
 */

import {
  parseDirectives,
  buildDirectiveHeader,
  type ParsedDirectives,
  type QueryGroup,
} from './metaLanguage';
import { RAW_QUERIES } from './queries';
import { type Query, type QueryType } from './types';

export { RAW_QUERIES };
export { type Query, type QueryType } from './types';


/* ─── constants ─── */

export const MAX_SIDEBAR_QUERIES = 12;

/** Predefined time range options for the picker. */
export const TIME_RANGE_OPTIONS: { label: string; interval: string | null }[] = [
  { label: '15m',     interval: '15 MINUTE' },
  { label: '1h',      interval: '1 HOUR' },
  { label: '6h',      interval: '6 HOUR' },
  { label: '1d',      interval: '1 DAY' },
  { label: '2d',      interval: '2 DAY' },
  { label: '7d',      interval: '7 DAY' },
  { label: '30d',     interval: '30 DAY' },
];

/* ─── mapping: ParsedDirectives → Query ─── */

function toQuery(sql: string, directives: ParsedDirectives, type: QueryType): Query {
  return {
    name: directives.meta!.title,
    description: directives.meta!.description ?? '',
    sql,
    group: directives.meta!.group as QueryGroup,
    type,
    directives,
  };
}

/** Parse raw SQL into a Query. Returns null if no valid @meta found. */
export function parseQueryMetadata(sql: string, type: QueryType = 'preset'): Query | null {
  const directives = parseDirectives(sql);
  if (!directives) return null;
  return toQuery(sql, directives, type);
}

/* ─── preset queries ─── */

export const PRESET_QUERIES: Query[] = RAW_QUERIES
  .map(sql => parseQueryMetadata(sql, 'preset'))
  .filter((q): q is Query => q !== null);

/* ─── template resolution ─── */

/**
 * Replace {{time_range}} placeholders with a ClickHouse time expression.
 */
export function resolveTimeRange(sql: string, defaultInterval?: string, userInterval?: string | null): string {
  if (!sql.includes('{{time_range}}')) return sql;
  const interval = userInterval ?? defaultInterval;
  if (!interval) return sql;
  if (interval.startsWith('CUSTOM:')) {
    const [rawStart, rawEnd] = interval.slice(7).split(',');
    const normaliseDT = (v: string) => {
      let s = v.replace('T', ' ');
      if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$/.test(s)) s += ':00';
      return s;
    };
    const start = normaliseDT(rawStart);
    const end = normaliseDT(rawEnd);
    let resolved = sql;
    resolved = resolved.replace(
      /event_date\s*>=\s*toDate\(\{\{time_range\}\}\)/g,
      `event_date >= toDate('${start}') AND event_date <= toDate('${end}')`
    );
    resolved = resolved.replace(
      /event_time\s*>\s*\{\{time_range\}\}/g,
      `event_time > toDateTime('${start}') AND event_time < toDateTime('${end}')`
    );
    resolved = resolved.replaceAll('{{time_range}}', `toDateTime('${start}')`);
    return resolved;
  }
  return sql.replaceAll('{{time_range}}', `now() - INTERVAL ${interval}`);
}

/**
 * Replace {{drill:column | fallback}} and {{drill_value:column | fallback}} placeholders.
 */
export function resolveDrillParams(sql: string, drillParams: Record<string, string>): string {
  let result = sql.replace(
    /\{\{drill:(\w+)\s*\|\s*([^}]+)\}\}/g,
    (_match, column: string, fallback: string) => {
      const value = drillParams[column];
      if (value !== undefined) {
        const escaped = value.replace(/'/g, "''");
        return `${column} = '${escaped}'`;
      }
      return fallback.trim();
    }
  );
  result = result.replace(
    /\{\{drill_value:(\w+)\s*\|\s*([^}]+)\}\}/g,
    (_match, column: string, fallback: string) => {
      const value = drillParams[column];
      if (value !== undefined) {
        const escaped = value.replace(/'/g, "''");
        return `'${escaped}'`;
      }
      return fallback.trim();
    }
  );
  return result;
}

/** Human-readable description of a time range interval (for tooltips). */
export function describeTimeRange(defaultInterval?: string, userInterval?: string | null): string {
  const interval = userInterval ?? defaultInterval;
  if (!interval) return '(no time filter)';
  if (interval.startsWith('CUSTOM:')) {
    const [start, end] = interval.slice(7).split(',');
    return `${start} → ${end || 'now'}`;
  }
  return `now() - INTERVAL ${interval}`;
}

/* ─── custom query CRUD (localStorage) ─── */

const CUSTOM_QUERIES_KEY = 'tracehouse-custom-queries';

/** Build a SQL string with embedded @meta header for a custom query. */
export function buildCustomQuerySql(name: string, description: string, bodySql?: string, group?: string): string {
  const header = buildDirectiveHeader(name, description, group);
  const body = bodySql ?? `SELECT
    database,
    table,
    count() AS count
FROM system.parts
WHERE active
GROUP BY database, table
ORDER BY count DESC
LIMIT 10`;
  return `${header}\n${body}`;
}

/** Load user-created queries from localStorage. */
export function loadCustomQueries(): Query[] {
  try {
    const raw = localStorage.getItem(CUSTOM_QUERIES_KEY);
    if (raw) {
      const parsed = JSON.parse(raw) as string[];
      if (Array.isArray(parsed)) {
        return parsed
          .map(sql => typeof sql === 'string' ? parseQueryMetadata(sql, 'custom') : null)
          .filter((q): q is Query => q !== null);
      }
    }
  } catch { /* ignore corrupt data */ }
  return [];
}

function saveCustomQuerySqls(sqls: string[]): void {
  localStorage.setItem(CUSTOM_QUERIES_KEY, JSON.stringify(sqls));
}

function loadCustomQuerySqls(): string[] {
  try {
    const raw = localStorage.getItem(CUSTOM_QUERIES_KEY);
    if (raw) {
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) return parsed.filter(s => typeof s === 'string');
    }
  } catch { /* ignore */ }
  return [];
}

/** Check if a query name is already taken (across builtins + custom). */
export function isQueryNameTaken(name: string): boolean {
  return PRESET_QUERIES.some(q => q.name === name) || loadCustomQueries().some(q => q.name === name);
}

/** Add or update a custom query by raw SQL (must contain @meta header). Returns updated list. */
export function addCustomQuery(rawSql: string): Query[] {
  const parsed = parseQueryMetadata(rawSql);
  if (!parsed) throw new Error('SQL must contain a valid @meta directive');
  if (PRESET_QUERIES.some(q => q.name === parsed.name)) {
    throw new Error(`Name "${parsed.name}" is already used by a built-in query`);
  }
  const sqls = loadCustomQuerySqls();
  const existing = sqls.map(s => parseQueryMetadata(s)).filter((q): q is Query => q !== null);
  const idx = existing.findIndex(q => q.name === parsed.name);
  if (idx >= 0) {
    sqls[idx] = rawSql;
  } else {
    sqls.push(rawSql);
  }
  saveCustomQuerySqls(sqls);
  return loadCustomQueries();
}

/** Delete a custom query by name. Returns updated list. */
export function deleteCustomQuery(name: string): Query[] {
  const sqls = loadCustomQuerySqls();
  const filtered = sqls.filter(s => {
    const parsed = parseQueryMetadata(s);
    return parsed?.name !== name;
  });
  saveCustomQuerySqls(filtered);
  return loadCustomQueries();
}

/** Wipe all custom queries. */
export function resetCustomQueries(): void {
  localStorage.removeItem(CUSTOM_QUERIES_KEY);
}

/** Get all queries: builtins + custom. */
export function getAllQueries(): Query[] {
  return [...PRESET_QUERIES, ...loadCustomQueries()];
}
