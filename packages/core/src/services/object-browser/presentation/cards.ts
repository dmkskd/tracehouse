import { sortedSet } from '../model/canonical.js';
import type { GraphEdge, GraphNode, ObjectGraph } from '../model/types.js';
import { engineSpec, type TableType } from '../model/engines.js';
import { KINDS } from '../model/kinds.js';
import { RELATIONSHIPS } from '../model/relationships.js';
import { engineCall } from '../collectors/references.js';
import { ttlDelay, type TTLRule } from '../../../utils/ttl-parser.js';
import { movingTTL } from '../model/disk-links.js';

/** Access objects keep their kind; tables take their engine family's type. */
export type ObjectType = 'user' | 'role' | 'grant-scope' | 'row-policy' | 'location' | 'disk' | TableType;
export interface CardFact { label: string; value: string }
export interface ObjectCard {
  type: ObjectType;
  title: string;
  /** Database or grant target, shown apart from the title so names do not wrap mid-word. */
  qualifier?: string;
  label: string;
  /** One muted line under the title: the type and the fact that matters most for it. */
  summary: string;
  facts: CardFact[];
  flags: ('differs' | 'partial' | 'unresolved' | 'revoke')[];
  /** Present only when host coverage carries information: several hosts or partial. */
  hosts?: string;
  /** Corner badge for behavior the card text does not show (`TTL`); `detail` lists the rules. */
  badge?: { text: string; detail: string };
  hover: ObjectHover;
}
export interface ValueRange { min: number; max: number }
/** What the hover shows: what the object is and where it lives, its size and comment,
 * and host or flag notes only when something is off. Keys and links stay in the detail. */
export interface ObjectHover {
  /** Type family: MergeTree, Materialized view, Role. */
  type: string;
  /** Where the object lives: its database (label `db`), or the target of a privilege or row policy. */
  context?: { label?: string; value: string };
  title: string;
  /** Exact engine where it names more than the type (ReplicatedReplacingMergeTree). */
  engine?: string;
  /** Lowest and highest value across hosts; absent for engines that store no data. */
  rows?: ValueRange;
  bytes?: ValueRange;
  parts?: ValueRange;
  /** Write time of the newest active part on any host, Unix seconds. */
  newestPart?: number;
  /** The stored definition that says what the object does: a view's query, or the engine call. */
  definition?: { label: string; text: string };
  comment?: string;
  notes: string[];
}
export const FLAG_LABELS: Record<ObjectCard['flags'][number], string> = {
  differs: 'differs across hosts', partial: 'partial / not comparable', unresolved: 'unresolved reference',
  revoke: 'partial revoke',
};

const MAX_FACTS = 3;
const ACCESS_TYPES = new Set<ObjectType>(['user', 'role', 'grant-scope', 'row-policy']);
export const isAccessType = (type: ObjectType) => ACCESS_TYPES.has(type);

/** First host record carrying the attribute. Host differences are flagged separately. */
function attr(node: GraphNode, name: string): unknown {
  return node.records.find(r => r.attributes[name] != null)?.attributes[name];
}
const str = (value: unknown) => value == null ? '' : String(value);
const TTL_ACTIONS: Record<TTLRule['action'], (rule: TTLRule) => string> = {
  delete: rule => rule.where ? `delete where ${rule.where}` : 'delete',
  move: rule => `move to ${rule.destination!.type} ${rule.destination!.name}`,
  recompress: rule => `recompress ${rule.codec}`,
  'group-by': rule => `group by ${rule.groupBy}${rule.set ? ` set ${rule.set}` : ''}`,
};
/** `TTL` on a table with TTL rules; the detail lists them, one per line. */
function ttlBadge(node: GraphNode): ObjectCard['badge'] {
  const rules = ttlFacts(node);
  return rules.length ? { text: 'TTL', detail: rules.map(rule => rule.value).join('\n') } : undefined;
}
/** Caption drawn on a link that a TTL rule moves parts along: the delay (`5 min`) when there is
 * one `<column> + interval` rule, `TTL` otherwise. Other links have none. */
export function edgeCaption(edge: GraphEdge): string | undefined {
  const rules = movingTTL(edge.records);
  if (!rules.length) return undefined;
  return rules.length === 1 ? ttlDelay(rules[0])?.short ?? 'TTL' : 'TTL';
}
/** One fact per table TTL rule (`ttl_rules`, parsed from engine_full at collection). */
const ttlFacts = (node: GraphNode): CardFact[] => {
  const rules = attr(node, 'ttl_rules');
  return Array.isArray(rules) ? (rules as TTLRule[]).map(rule => ({ label: 'TTL', value: `${rule.expression} · ${TTL_ACTIONS[rule.action](rule)}` })) : [];
};
const list = (values: string[]) => values.join(', ');

/** Native names from the observation key; display names can contain dots. */
function nameParts(node: GraphNode): string[] {
  if (!node.key.startsWith('[')) return [node.name];
  const parts = (JSON.parse(node.key) as string[]).slice(2);
  return parts.length ? parts : [node.name];
}

/** Per kind: tables take their engine family's type; other kinds are their own type. */
const KIND_TYPES: Record<GraphNode['kind'], (node: GraphNode) => ObjectType> = {
  user: () => 'user', role: () => 'role', 'grant-scope': () => 'grant-scope', 'row-policy': () => 'row-policy',
  table: node => engineSpec(attr(node, 'engine')).type,
  location: () => 'location', disk: () => 'disk',
};
export function objectTypeOf(node: GraphNode): ObjectType {
  return KIND_TYPES[node.kind](node);
}

const TYPE_LABELS: Record<ObjectType, string> = {
  user: 'User', role: 'Role', 'grant-scope': 'Privilege', 'row-policy': 'Row policy',
  'merge-tree': 'MergeTree', view: 'View', 'materialized-view': 'Materialized view',
  distributed: 'Distributed', dictionary: 'Dictionary', buffer: 'Buffer', table: 'Table',
  iceberg: 'Iceberg', lake: 'Data lake', 'object-storage': 'Object storage', location: 'Storage', disk: 'Disk',
};

/** Table reference relative to the owning database: same-database names stay short. */
function tableRef(graph: ObjectGraph, key: string, database: string): string {
  const node = graph.nodes.find(n => n.key === key);
  if (!node) return key;
  if (node.kind !== 'table') return node.name;
  const [db, name] = nameParts(node);
  return name === undefined ? db : db === database ? name : `${db}.${name}`;
}

/** `summary` overrides the default summary line: the label, then the first fact. */
type Describe = (node: GraphNode, graph: ObjectGraph) => Pick<ObjectCard, 'title' | 'qualifier' | 'facts'> & { label?: string; summary?: string };

function principals(prefix: string, all: unknown, names: unknown, except: unknown): CardFact {
  const included = sortedSet(names), excluded = sortedSet(except);
  const value = Number(all) === 1 ? excluded.length ? `all except ${list(excluded)}` : 'all'
    : included.length ? list(included) : 'none';
  return { label: prefix, value };
}

/** Neighbor facts declared per relationship kind (RELATIONSHIPS[kind].fact), grouped by fact label. */
function flowFacts(node: GraphNode, graph: ObjectGraph): CardFact[] {
  const database = nameParts(node)[0];
  const facts = new Map<string, Set<string>>();
  const add = (label: string, key: string) => facts.set(label, (facts.get(label) ?? new Set()).add(tableRef(graph, key, database)));
  // Upstream facts first, then downstream.
  for (const e of graph.edges) {
    const incoming = RELATIONSHIPS[e.kind].fact?.incoming;
    if (incoming && e.to === node.key) add(incoming, e.from);
  }
  for (const e of graph.edges) {
    const outgoing = RELATIONSHIPS[e.kind].fact?.outgoing;
    if (outgoing && e.from === node.key) add(outgoing === 'label' ? e.label.charAt(0).toUpperCase() + e.label.slice(1) : outgoing, e.to);
  }
  return [...facts].map(([label, names]) => ({ label, value: list([...names].sort()) }));
}

function tableCard(node: GraphNode): ReturnType<Describe> {
  const [database, name] = nameParts(node);
  return name === undefined ? { title: database, facts: [] } : { title: name, qualifier: database, facts: [] };
}
/** A table whose facts are its neighbors (reads from, writes to, stored on). */
const flowCard: Describe = (node, graph) => ({ ...tableCard(node), facts: flowFacts(node, graph) });
/** As `flowCard`, labeled with the engine name (IcebergS3, DeltaLake, S3). */
const engineFlowCard: Describe = (node, graph) => ({ ...flowCard(node, graph), label: str(attr(node, 'engine')) });
/** A table that holds data: its summary is the engine and database, which tells apart
 * tables of the same name. Its links are drawn, so they stay out of the summary. */
const withDatabase = (describe: Describe): Describe => (node, graph) => {
  const described = describe(node, graph);
  return { ...described, summary: [described.label || TYPE_LABELS[objectTypeOf(node)], described.qualifier].filter(Boolean).join(' · ') };
};

const describers: Record<ObjectType, Describe> = {
  user: node => {
    const hosts = ['host_ip', 'host_names', 'host_names_regexp', 'host_names_like'].flatMap(f => sortedSet(attr(node, f)));
    const unrestricted = hosts.length === 1 && hosts[0] === '::/0';
    return { title: node.name, facts: [
      principals('Default roles', attr(node, 'default_roles_all'), attr(node, 'default_roles_list'), attr(node, 'default_roles_except')),
      ...(attr(node, 'storage') ? [{ label: 'Storage', value: str(attr(node, 'storage')) }] : []),
      ...(!unrestricted && hosts.length ? [{ label: 'Hosts', value: list(hosts) }] : []),
    ] };
  },
  role: (node, graph) => {
    const members = new Set(graph.edges.filter(e => e.kind === 'has-role' && e.to === node.key).map(e => e.from));
    const grants = new Set(graph.edges.filter(e => e.kind === 'grant' && e.from === node.key).map(e => e.to));
    return { title: node.name, facts: [
      { label: 'Members', value: String(members.size) }, { label: 'Grants', value: String(grants.size) },
      ...(attr(node, 'storage') ? [{ label: 'Storage', value: str(attr(node, 'storage')) }] : []),
    ] };
  },
  'grant-scope': (node, graph) => {
    const access = str(attr(node, 'access_type')), column = attr(node, 'column'), object = str(attr(node, 'access_object'));
    const database = attr(node, 'database'), table = attr(node, 'table');
    const scope = object ? 'object' : database == null ? 'global' : table == null ? 'database' : column == null ? 'table' : 'column';
    const grants = graph.edges.filter(e => e.kind === 'grant' && e.to === node.key);
    const matches = new Set(graph.edges.filter(e => e.kind === 'candidate-scope' && e.from === node.key).map(e => e.to));
    if (!access) return { title: node.name, facts: [] };
    const total = graph.scopeMatches?.[node.key] ?? matches.size;
    const matched = matches.size ? ` · ${total} matched` : '';
    // What the grant covers, in words; the exact target stays in the qualifier.
    const covers: Record<typeof scope, string> = {
      global: matched ? `All tables${matched}` : 'Global', database: `All tables in ${str(database)}${matched}`,
      table: `ON ${str(database)}.${str(table)}`, column: `ON ${str(database)}.${str(table)}`, object: `ON ${object}`,
    };
    const revoke = Number(attr(node, 'partial_revoke')) === 1;
    return {
      title: `${revoke ? 'REVOKE ' : ''}${column == null ? access : `${access}(${str(column)})`}`,
      summary: revoke ? `Removed ${covers[scope].replace(/^ON /, 'on ').replace(/^All tables/, 'from all tables')}` : covers[scope],
      qualifier: `ON ${object || `${str(database ?? '*')}.${str(table ?? '*')}`}`,
      // Group and level come from system.privileges; the scope is the grant target.
      facts: [
        ...(attr(node, 'privilege_group') ? [{ label: 'Group', value: str(attr(node, 'privilege_group')) }] : []),
        { label: 'Scope', value: scope },
        ...(matches.size ? [{ label: 'Matched tables', value: graph.scopeMatches?.[node.key]
          ? `${matches.size} of ${graph.scopeMatches[node.key]}` : String(matches.size) }] : []),
        { label: 'Granted to', value: String(new Set(grants.map(e => e.from)).size) },
        ...(attr(node, 'privilege_level') ? [{ label: 'Level', value: str(attr(node, 'privilege_level')) }] : []),
        ...(grants.some(e => e.records.some(r => Number(r.evidence.is_partial_revoke) === 1)) ? [{ label: 'Revoke', value: 'partial revoke present' }] : []),
      ],
    };
  },
  'row-policy': node => {
    const database = str(attr(node, 'database')), table = str(attr(node, 'table'));
    return {
      title: str(attr(node, 'short_name')) || node.name,
      ...(database ? { qualifier: `ON ${database}.${table || '*'}` } : {}),
      facts: [
        ...(attr(node, 'select_filter') ? [{ label: 'Filter', value: str(attr(node, 'select_filter')) }] : []),
        ...(attr(node, 'is_restrictive') != null ? [{ label: 'Mode', value: Number(attr(node, 'is_restrictive')) === 1 ? 'restrictive' : 'permissive' }] : []),
        ...(attr(node, 'apply_to_all') != null ? [principals('Applies to', attr(node, 'apply_to_all'), attr(node, 'apply_to_list'), attr(node, 'apply_to_except'))] : []),
      ],
    };
  },
  'merge-tree': withDatabase(node => ({ ...tableCard(node), label: str(attr(node, 'engine')), facts: [
    ...(attr(node, 'sorting_key') ? [{ label: 'ORDER BY', value: str(attr(node, 'sorting_key')) }] : []),
    ...(attr(node, 'partition_key') ? [{ label: 'PARTITION BY', value: str(attr(node, 'partition_key')) }] : []),
    ...(attr(node, 'storage_policy') && attr(node, 'storage_policy') !== 'default' ? [{ label: 'Storage policy', value: str(attr(node, 'storage_policy')) }] : []),
    ...ttlFacts(node),
  ] })),
  'materialized-view': flowCard, view: flowCard, distributed: flowCard, buffer: flowCard,
  dictionary: withDatabase(tableCard),
  table: withDatabase(engineFlowCard),
  iceberg: withDatabase(engineFlowCard), lake: withDatabase(engineFlowCard), 'object-storage': withDatabase(engineFlowCard),
  // A storage server table definitions point to; the label is its backend (S3, HDFS, Local, URL).
  location: (node, graph) => {
    const tables = new Set(graph.edges.filter(e => e.kind === 'points-to' && e.to === node.key).map(e => e.from)).size;
    const label = str(attr(node, 'backend')) || TYPE_LABELS.location;
    return { title: node.name, label, summary: `${label} · ${tables} table${tables === 1 ? '' : 's'}`, facts: [{ label: 'Tables', value: String(tables) }] };
  },
  // A remote disk of tables' storage policies (collectors/storage.ts); system.disks holds no endpoint.
  disk: (node, graph) => {
    const tables = new Set(graph.edges.filter(e => e.kind === 'points-to' && e.to === node.key).map(e => e.from)).size;
    const label = str(attr(node, 'backend')) || TYPE_LABELS.disk;
    const cache = attr(node, 'cache_disks'), buckets = attr(node, 'buckets');
    return { title: node.name, label, summary: `${label} · ${tables} table${tables === 1 ? '' : 's'}`, facts: [{ label: 'Tables', value: String(tables) },
      { label: 'Disk', value: str(attr(node, 'disk')) },
      ...(Array.isArray(cache) ? [{ label: 'Cached by', value: cache.join(', ') }] : []),
      { label: 'Bucket', value: Array.isArray(buckets) ? buckets.join(', ') : 'not in system.blob_storage_log' },
      { label: 'Endpoint', value: 'not in system tables' },
    ] };
  },
};

/** Range of a numeric attribute over the host records that report it. */
function valueRange(node: GraphNode, name: string): ValueRange | undefined {
  const values = node.records.map(r => r.attributes[name]).filter(v => v != null && v !== '').map(Number).filter(Number.isFinite);
  return values.length ? { min: Math.min(...values), max: Math.max(...values) } : undefined;
}

const viewQuery = (node: GraphNode) => { const text = str(attr(node, 'as_select')).trim(); return text ? { label: 'Query', text } : undefined; };
const engineDefinition = (node: GraphNode) => { const text = engineCall(str(attr(node, 'engine_full'))); return text ? { label: 'Engine', text } : undefined; };
/** Types whose definition the hover shows (for MergeTree tables, their TTL rules); the others are
 * described by their name and engine. */
const ttlDefinition = (node: GraphNode) => { const text = ttlFacts(node).map(fact => fact.value).join('\n'); return text ? { label: 'TTL', text } : undefined; };
const DEFINITIONS: Partial<Record<ObjectType, (node: GraphNode) => ObjectHover['definition']>> = {
  'merge-tree': ttlDefinition, view: viewQuery, 'materialized-view': viewQuery, distributed: engineDefinition, buffer: engineDefinition,
};

/** Native names: [database, table] for tables, [database, table, policy] for row policies. */
export { nameParts };

/** Type-specific card summary, plus every fact the type describes (the card shows the
 * first few). Facts restate collected records; none is an access verdict. */
export function describeCard(node: GraphNode, graph: ObjectGraph): { card: ObjectCard; facts: CardFact[] } {
  const type = objectTypeOf(node);
  const { label, facts, summary, ...naming } = describers[type](node, graph);
  const typeLabel = label || TYPE_LABELS[type];
  const flags: ObjectCard['flags'] = [];
  if (node.differs) flags.push('differs');
  if (node.partial) flags.push('partial');
  if (node.records.some(r => r.attributes.unresolvedReference)) flags.push('unresolved');
  if (node.kind === 'grant-scope' && node.records.some(r => Number(r.attributes.partial_revoke) === 1)) flags.push('revoke');
  const showHosts = KINDS[node.kind].everyHost && (node.expectedHosts.length > 1 || node.partial);
  const first = facts[0];
  const hosts = `${node.observedHosts.length}/${node.expectedHosts.length} hosts`;
  const rows = valueRange(node, 'total_rows'), bytes = valueRange(node, 'total_bytes'), parts = valueRange(node, 'active_parts');
  const newestPart = valueRange(node, 'newest_part')?.max, definition = DEFINITIONS[type]?.(node), comment = str(attr(node, 'comment')).trim();
  const hover: ObjectHover = { type: TYPE_LABELS[type], title: naming.title,
    ...(naming.qualifier ? { context: node.kind === 'table' ? { label: 'db', value: naming.qualifier } : { value: naming.qualifier } } : {}),
    ...(typeLabel !== TYPE_LABELS[type] ? { engine: typeLabel } : {}),
    ...(rows ? { rows } : {}), ...(bytes ? { bytes } : {}), ...(parts ? { parts } : {}), ...(newestPart ? { newestPart } : {}),
    ...(definition ? { definition } : {}), ...(comment ? { comment } : {}),
    notes: [...(showHosts && node.observedHosts.length < node.expectedHosts.length ? [hosts] : []), ...flags.map(flag => FLAG_LABELS[flag])] };
  const badge = node.kind === 'table' ? ttlBadge(node) : undefined;
  return { facts, card: { type, ...naming, label: typeLabel, facts: facts.slice(0, MAX_FACTS), flags, ...(badge ? { badge } : {}),
    summary: summary ?? [typeLabel, first && `${first.label} ${first.value}`].filter(Boolean).join(' · '),
    ...(showHosts ? { hosts } : {}), hover } };
}
export const describeObject = (node: GraphNode, graph: ObjectGraph): ObjectCard => describeCard(node, graph).card;

export function describeObjects(graph: ObjectGraph): Map<string, ObjectCard> {
  return new Map(graph.nodes.map(node => [node.key, describeObject(node, graph)]));
}
