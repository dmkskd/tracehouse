/**
 * Custom query CRUD (localStorage).
 */

import { buildDirectiveHeader, parseQueryMetadata } from './metaLanguage';
import { PRESET_QUERIES } from './presetQueries';
import { type Query } from './types';

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
