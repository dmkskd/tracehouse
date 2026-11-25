import type { Attributes } from '../model/types.js';

export interface TableName { readonly database: string; readonly name: string }
export type ScopeTarget =
  | { readonly kind: 'global' }
  | { readonly kind: 'database' | 'database-prefix'; readonly database: string }
  | { readonly kind: 'table' | 'table-prefix'; readonly database: string; readonly table: string }
  | { readonly kind: 'column'; readonly database: string; readonly table: string; readonly column: string };
export interface AccessScope {
  readonly target: ScopeTarget;
  readonly operations: readonly string[];
  readonly accessObject: string | null;
  readonly effect: 'grant' | 'revoke';
}

/** Normalize SQL metadata once at the boundary; evaluators do not inspect raw attributes. */
export function normalizeScope(attributes: Attributes): AccessScope {
  const database = attributes.database == null ? null : String(attributes.database);
  const table = attributes.table == null ? null : String(attributes.table);
  const wildcard = Number(attributes.is_wildcard) === 1;
  const target: ScopeTarget = database === null ? { kind: 'global' }
    : table === null ? { kind: wildcard ? 'database-prefix' : 'database', database }
    : wildcard ? { kind: 'table-prefix', database, table }
    : attributes.column != null ? { kind: 'column', database, table, column: String(attributes.column) }
    : { kind: 'table', database, table };
  return Object.freeze({ target: Object.freeze(target),
    operations: Object.freeze(Array.isArray(attributes.table_operations) ? attributes.table_operations.map(String)
      : attributes.access_type == null ? [] : [String(attributes.access_type)]),
    accessObject: attributes.access_object ? String(attributes.access_object) : null,
    effect: Number(attributes.partial_revoke ?? attributes.is_partial_revoke) === 1 ? 'revoke' : 'grant' });
}

/** Specificity: global, database prefix (longest first), database, table prefix, table, column. */
export function nameScopeRank(target: ScopeTarget, table: TableName): [number, number] | null {
  switch (target.kind) {
    case 'global': return [0, 0];
    case 'database-prefix': return table.database.startsWith(target.database) ? [1, target.database.length] : null;
    case 'database': return table.database === target.database ? [2, 0] : null;
    case 'table-prefix': return table.database === target.database && table.name.startsWith(target.table) ? [3, target.table.length] : null;
    case 'table': return table.database === target.database && table.name === target.table ? [4, 0] : null;
    case 'column': return table.database === target.database && table.name === target.table ? [5, 0] : null;
  }
}
export function scopeRank(scope: AccessScope, table: TableName, operation: string): [number, number] | null {
  return !scope.accessObject && scope.operations.includes(operation) ? nameScopeRank(scope.target, table) : null;
}
export const compareRank = (a: [number, number], b: [number, number]): number => a[0] - b[0] || a[1] - b[1];
export const exactTableScope = (target: ScopeTarget): boolean => target.kind === 'table' || target.kind === 'column';
