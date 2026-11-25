import type { Capture, Observation } from '../model/types.js';
import { normalizeScope, type AccessScope, type TableName } from './scope.js';

/** Inventory resolution is distinct from a reference preserved when metadata is missing. */
type InventoryObject =
  | { readonly kind: 'user' | 'role'; readonly resolution: 'resolved' | 'unresolved' }
  | { readonly kind: 'table'; readonly resolution: 'resolved'; readonly table: TableName }
  | { readonly kind: 'table'; readonly resolution: 'unresolved' }
  | { readonly kind: 'grant-scope'; readonly scope: AccessScope };
interface RoleAssignment { readonly role: string; readonly isDefault: boolean }
export interface AccessIndex {
  readonly tables: readonly string[];
  readonly gaps: readonly string[];
  nameOf(key: string): string;
  table(key: string, host: string): TableName | undefined;
  tableHosts(key: string): string[];
  principal(key: string, host: string): boolean;
  roles(host: string, from: string): readonly RoleAssignment[];
  scopes(host: string, holder: string): readonly string[];
  scope(key: string, host: string): AccessScope | undefined;
}

function normalizeObject(record: Observation): InventoryObject | undefined {
  const resolution = record.attributes.unresolvedReference ? 'unresolved' : 'resolved';
  switch (record.kind) {
    case 'user': case 'role': return Object.freeze({ kind: record.kind, resolution });
    case 'table': return Object.freeze(resolution === 'resolved' && typeof record.attributes.database === 'string' && typeof record.attributes.name === 'string'
      ? { kind: 'table', resolution, table: Object.freeze({ database: record.attributes.database, name: record.attributes.name }) }
      : { kind: 'table', resolution: 'unresolved' });
    case 'grant-scope': return Object.freeze({ kind: 'grant-scope', scope: normalizeScope(record.attributes) });
    default: return undefined;
  }
}

/** A copied, readonly snapshot. Maps remain private, arrays/records are frozen, and no raw
 * attributes are retained. Rebuilding observes mutations to caller-owned Capture fixtures. */
export function createAccessIndex(capture: Capture, gaps: readonly string[]): AccessIndex {
  const names = new Map<string, string>();
  const records = new Map<string, Map<string, InventoryObject>>();
  const roles = new Map<string, Map<string, RoleAssignment[]>>();
  const scopeSets = new Map<string, Map<string, Set<string>>>();
  for (const record of capture.observations) {
    names.set(record.key, record.name);
    const normalized = normalizeObject(record);
    if (!normalized) continue;
    const hosts = records.get(record.key) ?? new Map<string, InventoryObject>();
    hosts.set(record.host, normalized);records.set(record.key, hosts);
  }
  for (const edge of capture.relationships) {
    if (edge.kind === 'has-role') {
      const holders = roles.get(edge.host) ?? new Map<string, RoleAssignment[]>();
      const assigned = holders.get(edge.from) ?? [];
      assigned.push(Object.freeze({ role: edge.to, isDefault: Number(edge.evidence.granted_role_is_default) !== 0 }));
      holders.set(edge.from, assigned);
      roles.set(edge.host, holders);
    }
    if (edge.kind === 'grant') {
      const holders = scopeSets.get(edge.host) ?? new Map<string, Set<string>>();
      const held = holders.get(edge.from) ?? new Set<string>();
      held.add(edge.to);holders.set(edge.from, held);scopeSets.set(edge.host, holders);
    }
  }
  for (const holders of roles.values()) for (const assigned of holders.values()) Object.freeze(assigned);
  const scopes = new Map([...scopeSets].map(([host, holders]) => [host,
    new Map([...holders].map(([holder, held]) => [holder, Object.freeze([...held])])),
  ]));
  const table = (key: string, host: string) => {
    const record = records.get(key)?.get(host);
    return record?.kind === 'table' && record.resolution === 'resolved' ? record.table : undefined;
  };
  return Object.freeze({
    tables: Object.freeze([...records.keys()].filter(key => [...records.get(key)!.keys()].some(host => table(key, host)))),
    gaps: Object.freeze([...gaps]),
    nameOf: (key: string) => names.get(key) ?? key,
    table,
    tableHosts: (key: string) => [...records.get(key)?.keys() ?? []].filter(host => table(key, host)),
    principal: (key: string, host: string) => { const record = records.get(key)?.get(host); return record?.kind === 'user' || record?.kind === 'role'; },
    roles: (host: string, from: string) => roles.get(host)?.get(from) ?? [],
    scopes: (host: string, holder: string) => scopes.get(host)?.get(holder) ?? [],
    scope: (key: string, host: string) => { const record = records.get(key)?.get(host); return record?.kind === 'grant-scope' ? record.scope : undefined; },
  });
}
