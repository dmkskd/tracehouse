import { ICEBERG_SNAPSHOTS, OBJECT_TABLE_ACTIVITY, OBJECT_TABLE_PLACEMENT, OBJECT_TABLE_DDL, OBJECT_TABLE_DDL_FORMATTED, explainViewSources, showCreateRole, showCreateRowPolicy, showCreateUser, showGrantsFor } from '../../../queries/access-queries.js';
import { buildQuery, tagQuery } from '../../../queries/builder.js';
import { buildQueryLinkUrl } from '../../../utils/deep-link-resolution.js';
import type { IClickHouseAdapter } from '../../../adapters/types.js';
import { OWN_ACCESS, type GraphNode, type ObjectGraph, type Observation } from '../model/types.js';
import { canonical } from '../model/canonical.js';
import { REFERENCE_METHODS, type ReferenceMethod } from '../model/methods.js';
import { DETAIL_GROUPS, RELATIONSHIPS, isRevokeEdge, type DetailGroup } from '../model/relationships.js';
import { analyzerTableNames } from '../collectors/references.js';
import { describeCard, nameParts, type CardFact, type ObjectCard, type ObjectType } from './cards.js';
import { DESTINATIONS, type DestinationInput } from '../model/destinations.js';
import { provenancePaths } from '../projections/graph.js';
import { ttlDelay, ttlSettings, type TTLRule } from '../../../utils/ttl-parser.js';
import { diskLinkEvidence, movingTTL } from '../model/disk-links.js';

/** `external` describes the external call a statement makes; such statements load only on request.
 * `sources` marks an EXPLAIN QUERY TREE, shown as the tables and table functions it resolved.
 * `highlight` marks a result that is SQL. `fallbackSql` runs when the server lacks a function `sql` uses. */
export interface DetailStatement { title: string; sql: string; external?: string; sources?: true; highlight?: true; fallbackSql?: string }
/** `method` names how a link was obtained, for kinds that record it (RELATIONSHIPS[kind].methods). */
/** Another TraceHouse page filtered to this object: `label` is the button text ("Show queries"),
 * `description` says what the page shows. `forUser` names the user in focus context when the
 * link is filtered to that user too. */
export interface DetailDestination { label: string; description: string; page: string; url: string }
/**
 * Links to other pages filtered to the object. On a table, `users` lists who the pages can also
 * be filtered to: the user in focus context first, then users with a configured path to the
 * table (at most 25). `byUser['']` holds the links for all users; `byUser[name]` for that user.
 */
export interface DetailLinks { users: string[]; defaultUser: string; byUser: Record<string, DetailDestination[]> }
/** An object at the other end of a relationship. `name` drops the database the object shares
 * with the detail's object; `method` says how the link was obtained, for the tooltip. */
export interface DetailRelationItem { key: string; name: string; fullName: string; method?: string }
/** One kind of relationship from the object's side ("Read by"), with every object it reaches. */
export interface DetailRelationRow { relation: string; items: DetailRelationItem[] }
export interface DetailRelationGroup { title: DetailGroup; rows: DetailRelationRow[] }
export type DetailSection =
  | { kind: 'facts'; title: string; facts: CardFact[] }
  | { kind: 'statement'; statement: DetailStatement }
  /** Query activity read from the server when the detail opens (loadTableActivity). */
  | { kind: 'activity'; title: string; statement: DetailStatement; table: string }
  | { kind: 'relationships'; title: string; groups: DetailRelationGroup[] }
  /** TTL rules in order, the TTL settings of the definition, and where the parts are now
   * (`statement`, read with loadTablePlacement when the detail opens). */
  | { kind: 'lifecycle'; title: string; steps: LifecycleStep[]; settings: CardFact[]; statement: DetailStatement }
  | { kind: 'records'; title: string; records: Observation[] };
/** One TTL rule: when it applies ("5 minutes after _inserted_at"), what it does, and for a move,
 * the storage node the parts go to. */
export interface LifecycleStep { when: string; action: string; target?: { key: string; name: string } }
/** Active parts of a table on one disk of the connected host. `storage` is the object storage type
 * (S3) or the disk type (Local). `pastMoveTTL` counts parts whose earliest move TTL has passed;
 * `nextMove` says when the next one is due ("in 3 min"), or is null. */
export interface TablePlacement { disk: string; storage: string; parts: number; rows: number; bytes: number; pastMoveTTL: number; nextMove: string | null }
/** `links` open other pages filtered to the object; they belong with the title, not the sections. */
export interface ObjectDetail { card: ObjectCard; links: DetailLinks; sections: DetailSection[] }

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
  // A disk is server configuration; ClickHouse has no DDL for it.
  disk: () => [],
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

/** The pages DESTINATIONS lists for the object's type, for all users and for each user the
 * filter offers. A placeholder for an unresolved reference opens nothing. */
const MAX_FILTER_USERS = 25;
function destinationsOf(node: GraphNode, graph: ObjectGraph, type: ObjectType, contextUser?: string): DetailLinks {
  if (node.records.some(r => r.attributes.unresolvedReference)) return { users: [], defaultUser: '', byUser: { '': [] } };
  const record = node.records[0]?.attributes ?? {};
  const base: DestinationInput = node.kind === 'table' ? { database: String(record.database), table: String(record.name) }
    : node.kind === 'user' ? { user: node.name } : {};
  const object = node.kind === 'table' ? `${base.database}.${base.table}` : node.name;
  const linksFor = (user?: string): DetailDestination[] => DESTINATIONS[type].map(spec => ({
    label: spec.label, page: spec.page,
    description: (user && spec.descriptionForUser ? spec.descriptionForUser : spec.description).replace('{object}', object).replace('{user}', user ?? ''),
    url: `${spec.path}?${new URLSearchParams(spec.params({ ...base, contextUser: user })).toString()}` }));
  // Only pages about a table filter by user; a user's own pages are already that user's.
  const filters = DESTINATIONS[type].some(spec => spec.descriptionForUser);
  const withPath = filters ? graph.nodes.filter(n => n.kind === 'user' && provenancePaths(graph, n.key, node.key, 1).length > 0).map(n => n.name).sort() : [];
  const users = filters ? [...new Set([...(contextUser ? [contextUser] : []), ...withPath])].slice(0, MAX_FILTER_USERS) : [];
  return { users, defaultUser: contextUser && users.includes(contextUser) ? contextUser : '',
    byUser: Object.fromEntries([['', linksFor()], ...users.map(user => [user, linksFor(user)])]) };
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

const TTL_STEPS: Record<TTLRule['action'], (rule: TTLRule) => string> = {
  delete: rule => rule.where ? `Delete rows where ${rule.where}` : 'Delete rows',
  move: rule => `Move parts to ${rule.destination!.type} ${rule.destination!.name}`,
  recompress: rule => `Recompress with ${rule.codec}`,
  'group-by': rule => `Roll up by ${rule.groupBy}${rule.set ? `, set ${rule.set}` : ''}`,
};
/** Lifecycle of a table with TTL rules or a remote disk: the rules in order, each move with the
 * disk it reaches (DiskLinkEvidence.ttl). */
function lifecycle(node: GraphNode, graph: ObjectGraph): DetailSection[] {
  const [database, table] = nameParts(node);
  const record = node.records.find(r => Array.isArray(r.attributes.ttl_rules))?.attributes;
  const rules = (record?.ttl_rules ?? []) as TTLRule[];
  const stored = graph.edges.filter(e => e.from === node.key && e.records.some(r => diskLinkEvidence(r.evidence)));
  if (table === undefined || (!rules.length && !stored.length)) return [];
  const steps = rules.map((rule): LifecycleStep => {
    const delay = ttlDelay(rule.expression);
    const edge = rule.action === 'move' ? stored.find(e => movingTTL(e.records).includes(rule.expression)) : undefined;
    const target = edge && graph.nodes.find(n => n.key === edge.to);
    return { when: delay ? `${delay.long} after ${delay.column}` : `At ${rule.expression}`, action: TTL_STEPS[rule.action](rule),
      ...(target ? { target: { key: target.key, name: target.name } } : {}) };
  });
  const definition = String(node.records.find(r => typeof r.attributes.engine_full === 'string')?.attributes.engine_full ?? '');
  return [{ kind: 'lifecycle', title: 'Lifecycle', steps, settings: ttlSettings(definition).map(([label, value]) => ({ label, value })),
    statement: { title: 'Placement', sql: buildQuery(OBJECT_TABLE_PLACEMENT, { database, table }) } }];
}

/** Full detail for one object: type facts, links to other pages, server statements,
 * relationships and host records. `contextUser` is the user in focus context. */
export function describeDetail(node: GraphNode, graph: ObjectGraph, options: { contextUser?: string } = {}): ObjectDetail {
  const { card, facts } = describeCard(node, graph);
  const destinations = destinationsOf(node, graph, card.type, options.contextUser);
  const shown = new Set(facts.map(f => f.label));
  for (const [field, label] of IDENTITY_FIELDS) {
    const value = node.records.find(r => r.attributes[field] != null && r.attributes[field] !== '')?.attributes[field];
    if (value != null && !shown.has(label)) facts.push({ label, value: String(value) });
  }
  const groups = relationGroups(node, graph);
  const [database, table] = nameParts(node);
  return { card, links: destinations, sections: [
    ...(node.differences?.length ? [{ kind: 'facts' as const, title: 'Differs across hosts', facts: hostDifferences(node) }] : []),
    ...(facts.length ? [{ kind: 'facts' as const, title: 'Summary', facts }] : []),
    ...(node.kind === 'table' && table !== undefined && !node.records.some(r => r.attributes.unresolvedReference) ? [{ kind: 'activity' as const, title: 'Activity · last 24 hours',
      statement: { title: 'Activity', sql: buildQuery(OBJECT_TABLE_ACTIVITY, { table: `${database}.${table}` }) }, table: `${database}.${table}` }] : []),
    ...(node.kind === 'table' ? lifecycle(node, graph) : []),
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

const inTime = (seconds: number) => seconds < 60 ? 'in under a minute'
  : seconds < 3600 ? `in ${Math.round(seconds / 60)} min` : seconds < 172800 ? `in ${Math.round(seconds / 3600)} h` : `in ${Math.round(seconds / 86400)} days`;
/** Reads where a table's active parts are on the connected host (OBJECT_TABLE_PLACEMENT). */
export async function loadTablePlacement(adapter: IClickHouseAdapter, statement: DetailStatement): Promise<TablePlacement[]> {
  const rows = await adapter.executeQuery<Record<string, unknown>>(tagQuery(statement.sql, 'TraceHouse:Objects:detail'));
  return rows.map(row => {
    const next = Number(row.next_move), now = Number(row.server_now);
    const objectStorage = String(row.object_storage_type ?? '');
    return { disk: String(row.disk), storage: objectStorage && objectStorage !== 'None' ? objectStorage : String(row.type ?? ''),
      parts: Number(row.parts), rows: Number(row.rows), bytes: Number(row.bytes), pastMoveTTL: Number(row.past_move_ttl),
      nextMove: next > now ? inTime(next - now) : null };
  });
}

/** A table's queries in the last 24 hours; null when there were none. `lastQuery` is server
 * time, `lastQueryAgo` the same as a distance ("1 h 40 min ago"). Each count has `url`: the
 * Queries page over the last day, filtered to the table and to what the count counts;
 * `lastQueryUrl` opens the latest query's detail. */
export interface TableActivity {
  counts: { label: 'SELECT' | 'INSERT' | 'Other' | 'Failed'; value: number; description: string; url: string }[];
  users: number; topUsers: { user: string; queries: number; url: string }[];
  lastQuery: string; lastQueryAgo: string; lastQueryUrl: string;
}
const ago = (seconds: number) => {
  if (seconds < 60) return 'just now';
  const minutes = Math.floor(seconds / 60) % 60, hours = Math.floor(seconds / 3600);
  return hours === 0 ? `${minutes} min ago` : minutes === 0 ? `${hours} h ago` : `${hours} h ${minutes} min ago`;
};
const url = (path: string, params: [string, string][]) => `${path}?${new URLSearchParams(params).toString()}`;
/** Reads a table's query activity (OBJECT_TABLE_ACTIVITY); `table` is `database.table`. */
export async function loadTableActivity(adapter: IClickHouseAdapter, statement: DetailStatement, table: string): Promise<TableActivity | null> {
  const [row] = await adapter.executeQuery<Record<string, unknown>>(tagQuery(statement.sql, 'TraceHouse:Objects:detail'));
  if (!row) return null;
  const queries = (...filters: [string, string][]) => url('/queries', [['timeRange', '1 DAY'], ['tableName', table], ...filters]);
  const otherKinds = (row.other_kinds as string[]).map(kind => ['queryKind', kind] as [string, string]);
  return {
    counts: [
      { label: 'SELECT', value: Number(row.selects), description: `SELECT queries on ${table}`, url: queries(['queryKind', 'Select']) },
      { label: 'INSERT', value: Number(row.inserts), description: `INSERT queries on ${table}`, url: queries(['queryKind', 'Insert']) },
      { label: 'Other', value: Number(row.other), description: `Other queries on ${table}: ${(row.other_kinds as string[]).join(', ') || 'none'}`,
        url: queries(...otherKinds) },
      { label: 'Failed', value: Number(row.failed), description: `Queries on ${table} that ended with an exception`, url: queries(['status', 'error']) },
    ],
    users: Number(row.users),
    topUsers: (row.top_users as [string, unknown][]).map(([user, total]) => ({ user, queries: Number(total), url: queries(['user', user]) })),
    lastQuery: String(row.last_query), lastQueryAgo: ago(Math.max(0, Number(row.seconds_ago))),
    lastQueryUrl: buildQueryLinkUrl(String(row.last_query_id), Number(row.last_query_ms)),
  };
}
