/**
 * ClickHouse SQL dialect for CodeMirror 6.
 *
 * Extends the standard SQL dialect with ClickHouse-specific keywords
 * and syntax (hash comments, backslash string escapes).
 *
 * The builtin function list is loaded dynamically from
 * `SELECT name FROM system.functions` via buildClickHouseDialect().
 */

import { SQLDialect } from '@codemirror/lang-sql';

// Note: keywords must be lowercase — the CodeMirror SQL tokenizer lowercases
// input words before looking them up in the keyword dictionary.

const CLICKHOUSE_KEYWORDS = [
  // Standard SQL
  'select', 'from', 'where', 'and', 'or', 'not', 'in', 'is', 'null', 'like', 'between',
  'join', 'left', 'right', 'inner', 'outer', 'on', 'group', 'by', 'having', 'order',
  'asc', 'desc', 'limit', 'offset', 'as', 'distinct', 'union', 'insert', 'into',
  'values', 'update', 'set', 'delete', 'create', 'drop', 'alter', 'table', 'case',
  'when', 'then', 'else', 'end', 'cast', 'with', 'over', 'partition', 'interval',
  'ilike', 'array', 'exists',
  // ClickHouse-specific
  'final', 'prewhere', 'global', 'sample', 'totals', 'settings', 'format',
  'materialized', 'engine', 'ttl', 'populate', 'live', 'attach', 'detach',
  'optimize', 'system', 'flush', 'logs', 'reload', 'dictionaries',
  'mutations', 'replicas', 'kill', 'mutation', 'query',
  'all', 'any', 'anti', 'semi', 'asof', 'cross',
  'using', 'temporary', 'if', 'database', 'databases', 'tables',
  'columns', 'show', 'describe', 'explain', 'pipeline', 'syntax',
  'rename', 'exchange', 'dictionary', 'view', 'cluster',
].join(' ');

const DIALECT_OPTIONS = {
  hashComments: true,
  slashComments: true,
  backslashEscapes: true,
  operatorChars: '+-*/<>=~!@#%^&|`?',
} as const;

/**
 * Build a ClickHouse dialect with a custom function list.
 * Functions should be lowercase names (the tokenizer lowercases input before lookup).
 */
export function buildClickHouseDialect(functions?: string[]): SQLDialect {
  return SQLDialect.define({
    keywords: CLICKHOUSE_KEYWORDS,
    builtin: functions?.join(' ') ?? '',
    ...DIALECT_OPTIONS,
  });
}

/** Default dialect with keywords only (no builtin functions until loaded dynamically). */
export const clickhouseDialect = buildClickHouseDialect();
