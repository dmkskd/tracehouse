import { ICEBERG_SNAPSHOTS, OBJECT_TABLE_DDL, OBJECT_TABLE_DDL_FORMATTED, explainViewSources, showCreateRole, showCreateRowPolicy, showCreateUser, showGrantsFor } from '../../../queries/access-queries.js';
import { buildQuery, tagQuery } from '../../../queries/builder.js';
import type { IClickHouseAdapter } from '../../../adapters/types.js';
import { OWN_ACCESS, type GraphNode, type ObjectGraph, type Observation } from '../model/types.js';
import { canonical } from '../model/canonical.js';
import { REFERENCE_METHODS, type ReferenceMethod } from '../model/methods.js';
import { DETAIL_GROUPS, RELATIONSHIPS, isRevokeEdge, type DetailGroup } from '../model/relationships.js';
import { analyzerTableNames } from '../collectors/references.js';
import { describeCard, nameParts, type CardFact, type ObjectCard, type ObjectType } from './cards.js';
import { DESTINATIONS, type DestinationInput } from '../model/destinations.js';

/** `external` describes the external call a statement makes; such statements load only on request.
 * `sources` marks an EXPLAIN QUERY TREE, shown as the tables and table functions it resolved.
 * `highlight` marks a result that is SQL. `fallbackSql` runs when the server lacks a function `sql` uses. */
export interface DetailStatement { title: string; sql: string; external?: string; sources?: true; highlight?: true; fallbackSql?: string }
/** `method` names how a link was obtained, for kinds that record it (RELATIONSHIPS[kind].methods). */
/** Another TraceHouse page filtered to this object: `label` is the button text ("Show queries"),
 * `description` says what the page shows. `forUser` names the user in focus context when the
 * link is filtered to that user too. */
export interface DetailDestination { label: string; description: string; page: string; url: string; forUser?: string }
/** An object at the other end of a relationship. `name` drops the database the object shares
 * with the detail's object; `method` says how the link was obtained, for the tooltip. */
export interface DetailRelationItem { key: string; name: string; fullName: string; method?: string }
/** One kind of relationship from the object's side ("Read by"), with every object it reaches. */
export interface DetailRelationRow { relation: string; items: DetailRelationItem[] }
export interface DetailRelationGroup { title: DetailGroup; rows: DetailRelationRow[] }
export type DetailSection =
  | { kind: 'facts'; title: string; facts: CardFact[] }
  | { kind: 'statement'; statement: DetailStatement }
  | { kind: 'relationships'; title: string; groups: DetailRelationGroup[] }
  | { kind: 'records'; title: string; records: Observation[] };
/** `links` open other pages filtered to the object; they belong with the title, not the sections. */
export interface ObjectDetail { card: ObjectCard; links: DetailDestination[]; sections: DetailSection[] }

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
  return [{ title: 'Tables read', sql: explainViewSources(String(record.attributes.as_select)), external: effects.map(effectText).join(' · '), sources: true }];
};
const tableDdl: Statements = node => {
  const [database, table] = nameParts(node);
  return table === undefined ? [] : [{ title: 'CREATE statement', sql: buildQuery(OBJECT_TABLE_DDL_FORMATTED, { database, table }),
    fallbackSql: buildQuery(OBJECT_TABLE_DDL, { database, table }), highlight: true }];
};
/** Server-produced statements per type. A grant scope has no DDL of its own;
 * its grants appear in each recipient's SHOW GRANTS. */
const statements: Record<ObjectType, Statements> = {
  user: node => [{ title: 'CREATE statement', sql: showCreateUser(node.name), highlight: true }, { title: 'Grants', sql: showGrantsFor(node.name), highlight: true }],
  role: node => [{ title: 'CREATE statement', sql: showCreateRole(node.name), highlight: true }, { title: 'Grants', sql: showGrantsFor(node.name), highlight: true }],
  'row-policy': node => {
    const [database, table, name] = nameParts(node);
    return name === undefined ? [] : [{ title: 'CREATE statement', sql: showCreateRowPolicy(name, database, table), highlight: true }];
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

const MAX_VALUE = 160;
const shownValue = (value: unknown) => {
  const text = value === undefined ? '(absent)' : typeof value === 'string' ? value : JSON.stringify(value);
  return text.length > MAX_VALUE ? `${text.slice(0, MAX_VALUE)}…` : text;
};
/** One row per distinct value of each differing field, naming the hosts that hold it. */
function hostDifferences(node: GraphNode): CardFact[] {
  const resolved = node.records.filter(r => !r.attributes.unresolvedReference);
  return (node.differences ?? []).flatMap(field => {
    if (field === OWN_ACCESS) return [{ label: field, value: 'differ between hosts; see Relationships' }];
    const groups = new Map<string, { value: unknown; hosts: string[] }>();
    for (const r of resolved) {
      const value = r.attributes[field], id = field in r.attributes ? canonical(value) : '';
      const group = groups.get(id) ?? { value, hosts: [] };
      group.hosts.push(r.host);groups.set(id, group);
    }
    return [...groups.values()].map((group, i) => ({ label: i === 0 ? field : '', value: `${group.hosts.sort().join(', ')}: ${shownValue(group.value)}` }));
  });
}

/** The pages DESTINATIONS lists for the object's type. Links that name the user in focus
 * context appear only with one. A placeholder for an unresolved reference opens nothing. */
function destinationsOf(node: GraphNode, type: ObjectType, contextUser?: string): DetailDestination[] {
  if (node.records.some(r => r.attributes.unresolvedReference)) return [];
  const record = node.records[0]?.attributes ?? {};
  const input: DestinationInput = node.kind === 'table' ? { database: String(record.database), table: String(record.name), contextUser }
    : node.kind === 'user' ? { user: node.name } : {};
  const object = node.kind === 'table' ? `${input.database}.${input.table}` : node.name;
  return DESTINATIONS[type].filter(spec => !spec.contextUser || contextUser).map(spec => ({
    label: spec.label, page: spec.page,
    description: spec.description.replace('{object}', object).replace('{user}', contextUser ?? ''),
    url: `${spec.path}?${new URLSearchParams(spec.params(input)).toString()}`,
    ...(spec.contextUser ? { forUser: contextUser } : {}) }));
}

/** The wording of a link to a table from a privilege, by what the privilege covers. */
function coverage(edge: ObjectGraph['edges'][number], graph: ObjectGraph): string {
  if (isRevokeEdge(edge)) return 'Revoked by';
  const operations = graph.nodes.find(n => n.key === edge.from)?.records[0]?.attributes.table_operations;
  const ops = Array.isArray(operations) ? operations : [];
  return ops.includes('SELECT') && ops.includes('INSERT') ? 'Readable and writable through'
    : ops.includes('SELECT') ? 'Readable through' : ops.includes('INSERT') ? 'Writable through' : RELATIONSHIPS['candidate-scope'].detail.incoming;
}

/** Relationships of `node`, grouped (Data flow, Storage, Access) and merged into one row per
 * wording. Names drop the database the object belongs to, and a row policy drops `ON <object>`. */
function relationGroups(node: GraphNode, graph: ObjectGraph): DetailRelationGroup[] {
  const nodes = new Map(graph.nodes.map(n => [n.key, n]));
  const database = node.kind === 'table' ? String(node.records[0]?.attributes.database ?? '') : '';
  const short = (name: string) => {
    if (database && name.startsWith(`${database}.`) && !name.includes(' ')) return name.slice(database.length + 1);
    return name.endsWith(` ON ${node.name}`) ? name.slice(0, -` ON ${node.name}`.length) : name;
  };
  const method = (e: ObjectGraph['edges'][number]) => {
    if (!RELATIONSHIPS[e.kind].methods) return {};
    const recorded = e.records.find(r => typeof r.evidence.method === 'string')?.evidence.method as ReferenceMethod | undefined;
    return recorded && REFERENCE_METHODS[recorded] ? { method: REFERENCE_METHODS[recorded] } : {};
  };
  const rows = new Map<DetailGroup, Map<string, DetailRelationItem[]>>();
  for (const e of graph.edges) {
    const outgoing = e.from === node.key;
    if (!outgoing && e.to !== node.key) continue;
    const spec = RELATIONSHIPS[e.kind].detail;
    const revokeGrant = e.kind === 'grant' && isRevokeEdge(e);
    const wording = revokeGrant ? (outgoing ? 'Has revoked' : 'Revoked from')
      : outgoing ? spec.outgoing : e.kind === 'candidate-scope' ? coverage(e, graph) : spec.incoming;
    const relation = wording === 'label' ? e.label.charAt(0).toUpperCase() + e.label.slice(1) : wording;
    const other = outgoing ? e.to : e.from, fullName = nodes.get(other)?.name ?? other;
    const group = rows.get(spec.group) ?? new Map<string, DetailRelationItem[]>();
    rows.set(spec.group, group);
    const items = group.get(relation) ?? [];
    group.set(relation, items);
    if (!items.some(item => item.key === other)) items.push({ key: other, name: short(fullName), fullName, ...method(e) });
  }
  return DETAIL_GROUPS.filter(title => rows.has(title)).map(title => ({ title,
    rows: [...rows.get(title)!].map(([relation, items]) => ({ relation, items: items.sort((a, b) => a.name.localeCompare(b.name)) })) }));
}

/** Full detail for one object: type facts, links to other pages, server statements,
 * relationships and host records. `contextUser` is the user in focus context. */
export function describeDetail(node: GraphNode, graph: ObjectGraph, options: { contextUser?: string } = {}): ObjectDetail {
  const { card, facts } = describeCard(node, graph);
  const destinations = destinationsOf(node, card.type, options.contextUser);
  const shown = new Set(facts.map(f => f.label));
  for (const [field, label] of IDENTITY_FIELDS) {
    const value = node.records.find(r => r.attributes[field] != null && r.attributes[field] !== '')?.attributes[field];
    if (value != null && !shown.has(label)) facts.push({ label, value: String(value) });
  }
  const groups = relationGroups(node, graph);
  return { card, links: destinations, sections: [
    ...(node.differences?.length ? [{ kind: 'facts' as const, title: 'Differs across hosts', facts: hostDifferences(node) }] : []),
    ...(facts.length ? [{ kind: 'facts' as const, title: 'Summary', facts }] : []),
    ...statements[card.type](node).map(statement => ({ kind: 'statement' as const, statement })),
    ...(groups.length ? [{ kind: 'relationships' as const, title: 'Relationships', groups }] : []),
    { kind: 'records', title: 'Host records', records: node.records },
  ] };
}

/** Reads one statement from the connected host. Errors such as missing SHOW
 * privileges propagate so the caller can show them in place of the text. An EXPLAIN
 * runs as its own statement (executeRawQuery), as in collection. */
export async function loadDetailStatement(adapter: IClickHouseAdapter, statement: DetailStatement): Promise<string> {
  const sql = tagQuery(statement.sql, 'TraceHouse:Objects:detail');
  if (statement.sources) {
    if (!adapter.executeRawQuery) throw new Error('connection cannot run EXPLAIN statements');
    const { tables, tableFunctions } = analyzerTableNames(await adapter.executeRawQuery(sql));
    return [...tables, ...tableFunctions.map(name => `${name}()`)].join('\n');
  }
  const run = async (text: string) => (await adapter.executeQuery<Record<string, unknown>>(tagQuery(text, 'TraceHouse:Objects:detail')))
    .map(row => String(Object.values(row)[0] ?? '')).join('\n');
  try {
    return await run(statement.sql);
  } catch (error) {
    // A server without the formatter (UNKNOWN_FUNCTION, code 46) gets the stored text. The old
    // planner says "Unknown function", the analyzer "Function with name `…` does not exist".
    const message = error instanceof Error ? error.message : String(error);
    if (!statement.fallbackSql || !/UNKNOWN_FUNCTION|Unknown function|Function with name .* does not exist|Code: 46\b/i.test(message)) throw error;
    return run(statement.fallbackSql);
  }
}
