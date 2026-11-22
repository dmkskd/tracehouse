import type { GraphNode, Investigation, ObjectGraph, ObjectKind } from '../model/types.js';
import { RELATIONSHIPS, type RelationshipFamily } from '../model/relationships.js';
import { nameParts } from './cards.js';

/** One object the user picks to answer a guide's question. */
export interface GuidePick { label: string; candidates: (graph: ObjectGraph) => GraphNode[] }
export interface Guide {
  id: string;
  title: string;
  question: string;
  description: string;
  /** Picked in order. `focus` opens focus on the last pick; `investigate` collects
   * provenance for the picked user and table. */
  picks: GuidePick[];
  action: 'focus' | 'investigate';
  /** Footer counts from the current catalog. */
  signal: (graph: ObjectGraph) => string;
}

const resolved = (graph: ObjectGraph, ...kinds: ObjectKind[]) =>
  graph.nodes.filter(n => kinds.includes(n.kind) && !n.records.every(r => r.attributes.unresolvedReference));
/** Objects at either end of a link in `family`. */
const linked = (graph: ObjectGraph, family: RelationshipFamily) =>
  new Set(graph.edges.filter(e => RELATIONSHIPS[e.kind].family === family).flatMap(e => [e.from, e.to]));
const countOf = (count: number, one: string, many = `${one}s`) => `${count} ${count === 1 ? one : many}`;

const TABLE: GuidePick = { label: 'Table', candidates: graph => resolved(graph, 'table') };

/** Guided questions on the object browser page, each answered by focus or provenance. */
export const GUIDES: Guide[] = [
  { id: 'table-access', title: 'Access', question: 'Who can read this table?', description: 'Privileges, roles and users',
    picks: [TABLE], action: 'focus',
    signal: graph => `${countOf(resolved(graph, 'table').length, 'table')} · ${countOf(resolved(graph, 'grant-scope').length, 'privilege')}` },
  { id: 'principal-reach', title: 'Principals', question: 'What can this user reach?', description: 'Roles, privileges and tables',
    picks: [{ label: 'User or role', candidates: graph => resolved(graph, 'user', 'role') }], action: 'focus',
    signal: graph => `${countOf(resolved(graph, 'user').length, 'user')} · ${countOf(resolved(graph, 'role').length, 'role')}` },
  { id: 'data-flow', title: 'Data flow', question: 'Where does this data go?', description: 'Materialized views, views, Buffer, Distributed',
    picks: [{ label: 'Table', candidates: graph => { const flow = linked(graph, 'data-flow'); return resolved(graph, 'table').filter(n => flow.has(n.key)); } }],
    action: 'focus',
    signal: graph => `${countOf(graph.edges.filter(e => RELATIONSHIPS[e.kind].family === 'data-flow').length, 'link')} between tables` },
  { id: 'storage', title: 'Storage', question: 'Which tables are stored externally?', description: 'Iceberg, Delta Lake, S3 and URL tables',
    picks: [{ label: 'Storage server', candidates: graph => resolved(graph, 'location') }], action: 'focus',
    signal: graph => `${countOf(resolved(graph, 'location').length, 'storage server')} · ${countOf(new Set(graph.edges.filter(e => RELATIONSHIPS[e.kind].family === 'storage').map(e => e.from)).size, 'table')}` },
  { id: 'row-policies', title: 'Row policies', question: 'Which policies filter this table?', description: 'Filters and who they apply to',
    picks: [{ label: 'Table', candidates: graph => { const targets = new Set(graph.edges.filter(e => e.kind === 'policy-target').map(e => e.to)); return resolved(graph, 'table').filter(n => targets.has(n.key)); } }],
    action: 'focus',
    signal: graph => countOf(resolved(graph, 'row-policy').length, 'row policy', 'row policies') },
  { id: 'provenance', title: 'Provenance', question: 'How does a user reach a table?', description: 'Configured paths per host, no verdict',
    picks: [{ label: 'User', candidates: graph => resolved(graph, 'user') }, TABLE], action: 'investigate',
    signal: () => 'SELECT · paths per host' },
];

/** The provenance capture for a picked user and table. */
export function investigationFor(user: GraphNode, table: GraphNode): Omit<Investigation, 'connection'> {
  const [database, name = ''] = nameParts(table);
  return { principal: user.name, database, table: name, operation: 'SELECT' };
}
