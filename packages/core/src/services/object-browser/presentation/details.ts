import { ICEBERG_SNAPSHOTS, OBJECT_TABLE_DDL, explainViewSourceList, showCreateRole, showCreateRowPolicy, showCreateUser, showGrantsFor } from '../../../queries/access-queries.js';
import { buildQuery, tagQuery } from '../../../queries/builder.js';
import type { IClickHouseAdapter } from '../../../adapters/types.js';
import type { GraphNode, ObjectGraph, Observation } from '../model/types.js';
import { REFERENCE_METHODS, type ReferenceMethod } from '../model/methods.js';
import { RELATIONSHIPS } from '../model/relationships.js';
import { describeCard, nameParts, type CardFact, type ObjectCard, type ObjectType } from './cards.js';

/** `external` describes the outside call a statement makes; such statements load only on request. */
export interface DetailStatement { title: string; sql: string; external?: string }
/** `method` names how a link was obtained, for kinds that record it (RELATIONSHIPS[kind].methods). */
export interface DetailRelationship { relation: string; direction: 'out' | 'in'; key: string; name: string; method?: string }
export type DetailSection =
  | { kind: 'facts'; title: string; facts: CardFact[] }
  | { kind: 'statement'; statement: DetailStatement }
  | { kind: 'relationships'; title: string; relationships: DetailRelationship[] }
  | { kind: 'records'; title: string; records: Observation[] };
export interface ObjectDetail { card: ObjectCard; sections: DetailSection[] }

type Statements = (node: GraphNode) => DetailStatement[];
/** Reading Iceberg snapshots makes ClickHouse read the table's metadata from storage,
 * so the detail view loads it only on request. */
const icebergSnapshots: Statements = node => {
  const [database, table] = nameParts(node);
  return table === undefined ? [] : [{ title: 'Snapshots', sql: buildQuery(ICEBERG_SNAPSHOTS, { database, table }),
    external: 'Reads Iceberg metadata from storage' }];
};
/** A view query the collector did not analyze (attribute analysisEffects): the analyzer's
 * tables load on request, labeled with what analysis does. */
const effectText = (effect: string) => effect === 'scalar subquery' ? 'Runs scalar subqueries'
  : `Asks ${effect.replace(/ table function$/, '')} for its columns; can contact its source`;
const viewSources: Statements = node => {
  const record = node.records.find(r => Array.isArray(r.attributes.analysisEffects) && typeof r.attributes.as_select === 'string');
  if (!record) return [];
  const effects = record.attributes.analysisEffects as string[];
  return [{ title: 'Tables read', sql: explainViewSourceList(String(record.attributes.as_select)), external: effects.map(effectText).join(' · ') }];
};
const tableDdl: Statements = node => {
  const [database, table] = nameParts(node);
  return table === undefined ? [] : [{ title: 'CREATE statement', sql: buildQuery(OBJECT_TABLE_DDL, { database, table }) }];
};
/** Server-produced statements per type. A grant scope has no DDL of its own;
 * its grants appear in each recipient's SHOW GRANTS. */
const statements: Record<ObjectType, Statements> = {
  user: node => [{ title: 'CREATE statement', sql: showCreateUser(node.name) }, { title: 'Grants', sql: showGrantsFor(node.name) }],
  role: node => [{ title: 'CREATE statement', sql: showCreateRole(node.name) }, { title: 'Grants', sql: showGrantsFor(node.name) }],
  'row-policy': node => {
    const [database, table, name] = nameParts(node);
    return name === undefined ? [] : [{ title: 'CREATE statement', sql: showCreateRowPolicy(name, database, table) }];
  },
  'grant-scope': () => [],
  'merge-tree': tableDdl, view: node => [...tableDdl(node), ...viewSources(node)], 'materialized-view': node => [...tableDdl(node), ...viewSources(node)], distributed: tableDdl, dictionary: tableDdl, buffer: tableDdl, table: tableDdl,
  iceberg: node => [...tableDdl(node), ...icebergSnapshots(node)],
  lake: tableDdl,
  'object-storage': tableDdl,
  // A location is TraceHouse's grouping of definition text; ClickHouse has no DDL for it.
  location: () => [],
};

const IDENTITY_FIELDS: [string, string][] = [['id', 'ID'], ['uuid', 'UUID'], ['engine', 'Engine'], ['storage', 'Storage'],
  ['primary_key', 'Primary key'], ['storage_policy', 'Storage policy']];

/** Full detail for one object: type facts, server statements, relationships and host records. */
export function describeDetail(node: GraphNode, graph: ObjectGraph): ObjectDetail {
  const { card, facts } = describeCard(node, graph);
  const shown = new Set(facts.map(f => f.label));
  for (const [field, label] of IDENTITY_FIELDS) {
    const value = node.records.find(r => r.attributes[field] != null && r.attributes[field] !== '')?.attributes[field];
    if (value != null && !shown.has(label)) facts.push({ label, value: String(value) });
  }
  const names = new Map(graph.nodes.map(n => [n.key, n.name]));
  const method = (e: ObjectGraph['edges'][number]) => {
    if (!RELATIONSHIPS[e.kind].methods) return {};
    const recorded = e.records.find(r => typeof r.evidence.method === 'string')?.evidence.method as ReferenceMethod | undefined;
    return recorded && REFERENCE_METHODS[recorded] ? { method: REFERENCE_METHODS[recorded] } : {};
  };
  const relationships: DetailRelationship[] = graph.edges.flatMap(e => [
    ...(e.from === node.key ? [{ relation: e.label, direction: 'out' as const, key: e.to, name: names.get(e.to) ?? e.to, ...method(e) }] : []),
    ...(e.to === node.key ? [{ relation: e.label, direction: 'in' as const, key: e.from, name: names.get(e.from) ?? e.from, ...method(e) }] : []),
  ]);
  return { card, sections: [
    ...(facts.length ? [{ kind: 'facts' as const, title: 'Summary', facts }] : []),
    ...statements[card.type](node).map(statement => ({ kind: 'statement' as const, statement })),
    ...(relationships.length ? [{ kind: 'relationships' as const, title: 'Relationships', relationships }] : []),
    { kind: 'records', title: 'Host records', records: node.records },
  ] };
}

/** Reads one statement from the connected host. Errors such as missing SHOW
 * privileges propagate so the caller can show them in place of the text. */
export async function loadDetailStatement(adapter: IClickHouseAdapter, sql: string): Promise<string> {
  const rows = await adapter.executeQuery<Record<string, unknown>>(tagQuery(sql, 'TraceHouse:Objects:detail'));
  return rows.map(row => String(Object.values(row)[0] ?? '')).join('\n');
}
