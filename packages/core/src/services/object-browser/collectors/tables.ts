import type { IClickHouseAdapter } from '../../../adapters/types.js';
import { canonical } from '../model/canonical.js';
import { engineSpec } from '../model/engines.js';
import { objectKey, type Observation, type Relationship } from '../model/types.js';
import { collectDefinitionReferences, type ReferenceTarget } from './references.js';
import { locationNodes, type LocationEntry } from './locations.js';

type Row = Record<string, unknown>;
const text = (v: unknown) => String(v ?? '');

/** Links between tables and to their storage: native dependency and target fields
 * first, then definition references for what the native fields do not cover.
 * `fields` are the optional system.tables fields every host has. View queries are analyzed
 * on `analyzedOn`, the connected node. These are candidate links: validateAnalyzedLinks
 * checks cross-host catalog agreement after referenced system tables have been loaded. */
export async function tableLinks(adapter: IClickHouseAdapter, connection: string, tables: Row[], fields: ReadonlySet<string>,
  check: () => void, analyzedOn: string | null = null): Promise<{ observations: Observation[]; relationships: Relationship[]; notes: string[];
    /** Views whose query was not analyzed, by table key and host, with what analysis would do. */
    unanalyzed: { table: string; host: string; effects: string[] }[] }> {
  const k = (...names: string[]) => objectKey(connection, 'table', ...names);
  const relationships: Relationship[] = [], notes: string[] = [];
  const link = (host: string, from: string, to: string, kind: Relationship['kind'], label: string, evidence: Record<string, unknown>) =>
    relationships.push({ id: canonical([host, from, to, kind, evidence]), host, from, to, kind, label, source: 'tables', evidence });
  for (const row of tables) {
    const self = k(text(row.database), text(row.name)), host = text(row.host);
    if (fields.has('dependencies_database') && fields.has('dependencies_table')) {
      const databases = row.dependencies_database, names = row.dependencies_table;
      if (!Array.isArray(databases) || !Array.isArray(names) || databases.length !== names.length) {
        notes.push(`Malformed native dependency arrays for ${row.database}.${row.name}; dependency links omitted.`);
      } else names.forEach((name, index) => {
        // Native dependents are insert triggers only for an insert-triggered engine
        // observed on the same host. Loading order is not used.
        if (tables.some(target => target.host === row.host && target.database === databases[index]
          && target.name === name && engineSpec(target.engine).insertTriggered)) {
          link(host, self, k(text(databases[index]), text(name)), 'insert-trigger', 'triggers on insert', { method: 'native', field: 'dependencies_table' });
        }
      });
    }
    if (engineSpec(row.engine).nativeTarget && fields.has('target_database') && fields.has('target_table') && row.target_table) {
      link(host, self, k(text(row.target_database || row.database), text(row.target_table)), 'writes-to', 'writes to', { method: 'native', field: 'target_table' });
    }
  }
  if (!fields.has('as_select') && !fields.has('engine_full')) return { observations: [], relationships, notes, unanalyzed: [] };

  const found = await collectDefinitionReferences(adapter, tables, check);
  notes.push(...found.notes);
  const locations: LocationEntry[] = [];
  // Each target kind resolves to a table key, or is handed to the location nodes (null).
  const resolve: { [K in ReferenceTarget['kind']]: (target: Extract<ReferenceTarget, { kind: K }>, row: Row, self: string) => string | null } = {
    table: target => k(target.database, target.table),
    qualified: (target, row) => {
      // The analyzer prints db.table; match catalog names, since either part may contain a dot.
      const matches = tables.filter(t => t.host === row.host && `${t.database}.${t.name}` === target.qualified);
      if (matches.length > 1) { notes.push(`${row.database}.${row.name}: ambiguous source ${target.qualified}`); return null; }
      if (matches.length) return k(text(matches[0].database), text(matches[0].name));
      const dot = target.qualified.indexOf('.');
      if (dot < 0) { notes.push(`${row.database}.${row.name}: unqualified source ${target.qualified} not linked`); return null; }
      return k(target.qualified.slice(0, dot), target.qualified.slice(dot + 1));
    },
    location: (target, row, self) => {
      locations.push({ host: text(row.host), table: self, location: target.location, backend: target.backend, definition: text(row.engine_full) });
      return null;
    },
  };
  for (const ref of found.references) {
    const row = tables[ref.row], host = text(row.host), self = k(text(row.database), text(row.name));
    const other = (resolve[ref.target.kind] as (t: ReferenceTarget, r: Row, s: string) => string | null)(ref.target, row, self);
    if (other === null) continue;
    const [from, to] = ref.flow === 'upstream' ? [other, self] : [self, other];
    // A native link between the same objects takes precedence.
    if (relationships.some(r => r.host === host && r.from === from && r.to === to && r.evidence.method === 'native')) continue;
    link(host, from, to, ref.kind, ref.label, { method: ref.method, definition: ref.definition,
      ...(ref.method === 'analyzer' && analyzedOn ? { analyzedOn } : {}) });
  }
  const stored = locationNodes(connection, locations);
  const unanalyzed = found.unanalyzed.map(({ row, effects }) => ({ table: k(text(tables[row].database), text(tables[row].name)), host: text(tables[row].host), effects }));
  return { observations: stored.observations, relationships: [...relationships, ...stored.relationships], notes, unanalyzed };
}

/** Validate candidate analyzer links against enriched host inventories. Discovery must precede
 * this step: otherwise excluded system tables look absent before their by-name lookup runs. */
export function validateAnalyzedLinks(relationships: Relationship[], observations: Observation[]): { relationships: Relationship[]; notes: string[] } {
  const inventory = new Map(observations.filter(o => o.kind === 'table' && !o.attributes.unresolvedReference)
    .map(o => [canonical([o.key, o.host]), o]));
  const notes: string[] = [];
  const accepted = relationships.filter(link => {
    const analyzedOn = link.evidence.analyzedOn;
    if (link.evidence.method !== 'analyzer' || typeof analyzedOn !== 'string' || link.host === analyzedOn) return true;
    // Analyzer links run source → view; compare the resolved source on both hosts.
    const source = inventory.get(canonical([link.from, link.host]));
    const connected = inventory.get(canonical([link.from, analyzedOn]));
    if (source && connected && source.attributes.engine === connected.attributes.engine) return true;
    const sourceName = (JSON.parse(link.from) as string[]).slice(2).join('.');
    const viewName = (JSON.parse(link.to) as string[]).slice(2).join('.');
    const reason = !source || !connected ? 'is not in the collected inventory' : 'has another engine';
    notes.push(`${viewName} on ${link.host}: analyzed on ${analyzedOn}; ${sourceName} ${reason} there, link omitted`);
    return false;
  });
  return { relationships: accepted, notes };
}
