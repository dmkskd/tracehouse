import { RELATIONSHIPS } from '../model/relationships.js';
import { objectKey, type Coverage, type GraphNode, type ObjectGraph, type Source } from '../model/types.js';
import { edgesBetween } from '../access/traversal.js';

/** An object another page opens the object browser on. `user` on a table is the
 * principal in context: focus then shows only that user's paths to the table. */
export type ObjectLink =
  | { kind: 'table'; database: string; table: string; user?: string }
  | { kind: 'user'; user: string }
  | { kind: 'role'; role: string };

/** URL parameters of an object link, read by the object browser page. */
export const OBJECT_LINK_PARAMS = ['ob_kind', 'ob_db', 'ob_tbl', 'ob_user', 'ob_role'] as const;

/** `/objects` URL that opens focus on `link`. */
export function objectLinkUrl(link: ObjectLink): string {
  const params = new URLSearchParams({ ob_kind: link.kind });
  if (link.kind === 'table') {
    params.set('ob_db', link.database);params.set('ob_tbl', link.table);
    if (link.user) params.set('ob_user', link.user);
  } else if (link.kind === 'user') params.set('ob_user', link.user);
  else params.set('ob_role', link.role);
  return `/objects?${params.toString()}`;
}

/** The link in URL parameters, or null when a required parameter is missing. */
export function parseObjectLink(get: (name: string) => string | null | undefined): ObjectLink | null {
  const value = (name: string) => get(name) || undefined;
  const kind = value('ob_kind'), database = value('ob_db'), table = value('ob_tbl'), user = value('ob_user'), role = value('ob_role');
  if (kind === 'table' && database && table) return { kind, database, table, ...(user ? { user } : {}) };
  if (kind === 'user' && user) return { kind, user };
  if (kind === 'role' && role) return { kind, role };
  return null;
}

export const objectLinkName = (link: ObjectLink): string =>
  link.kind === 'table' ? `${link.database}.${link.table}` : link.kind === 'user' ? link.user : link.role;

export interface MissingObject { kind: 'table' | 'user' | 'role'; name: string }
export type ResolvedObjectLink = { focus: string; context: string | null } | { missing: MissingObject[] };

/** Graph keys for a link's object and its principal in context. `missing` names what the
 * collected catalog does not hold. */
export function resolveObjectLink(graph: ObjectGraph, link: ObjectLink): ResolvedObjectLink {
  const find = (kind: string, name: string) => graph.nodes.find(node => node.kind === kind && (kind === 'table'
    ? node.records.some(r => r.attributes.database === (link as { database: string }).database && r.attributes.name === name)
    : node.name === name))?.key ?? null;
  const focus = link.kind === 'table' ? find('table', link.table) : link.kind === 'user' ? find('user', link.user) : find('role', link.role);
  const context = link.kind === 'table' && link.user ? find('user', link.user) : null;
  const missing: MissingObject[] = [...(focus ? [] : [{ kind: link.kind, name: objectLinkName(link) }]),
    ...(link.kind === 'table' && link.user && !context ? [{ kind: 'user' as const, name: link.user }] : [])];
  return missing.length ? { missing } : { focus: focus!, context };
}

const SOURCE_OF: Record<MissingObject['kind'], Source> = { table: 'tables', user: 'users', role: 'roles' };

/** Edit distance, bounded: names further apart than `max` return `max + 1`. */
function distance(a: string, b: string, max: number): number {
  if (Math.abs(a.length - b.length) > max) return max + 1;
  let previous = Array.from({ length: b.length + 1 }, (_, i) => i);
  for (let i = 1; i <= a.length; i++) {
    const row = [i];
    for (let j = 1; j <= b.length; j++) row[j] = Math.min(previous[j] + 1, row[j - 1] + 1, previous[j - 1] + (a[i - 1] === b[j - 1] ? 0 : 1));
    previous = row;
  }
  return previous[b.length];
}

export interface MissingLinkSuggestion { focus: string; context: string | null; name: string }
export interface MissingLinkNotice {
  /** What the link asked for, e.g. `demo.trips for alice`. */
  requested: string;
  missing: MissingObject[];
  /** Sources of the missing kinds the catalog did not read in full: `system.users: truncated`. */
  incomplete: string[];
  /** Objects of the same kind with a similar name, opened the way the link would have been. */
  suggestions: MissingLinkSuggestion[];
}

/** Why a link opened nothing and what it may have meant. Similar names contain the missing
 * name, are contained in it, hold each of its `_`, `.` or `-` separated parts (`th_alice` finds
 * `th_ob_alice`), or are at most two edits away; five at most. */
export function describeMissingLink(graph: ObjectGraph, link: ObjectLink, missing: MissingObject[], coverage: Coverage[]): MissingLinkNotice {
  const similar = (kind: MissingObject['kind'], name: string) => {
    const wanted = name.toLowerCase();
    return graph.nodes.filter(node => node.kind === kind).map(node => {
      const have = node.name.toLowerCase();
      const parts = wanted.split(/[_.-]+/).filter(Boolean);
      const score = have === wanted ? 0 : have.includes(wanted) || wanted.includes(have) ? 1
        : parts.length > 1 && parts.every(part => have.includes(part)) ? 2 : distance(have, wanted, 2) + 1;
      return { node, score };
    }).filter(m => m.score <= 3).sort((a, b) => a.score - b.score || a.node.name.localeCompare(b.node.name)).map(m => m.node);
  };
  const resolved = resolveObjectLink(graph, link.kind === 'table' ? { ...link, user: undefined } : link);
  const focusKey = 'focus' in resolved ? resolved.focus : null;
  const suggestions: MissingLinkSuggestion[] = [];
  for (const item of missing) {
    for (const node of similar(item.kind, link.kind === 'table' && item.kind === 'table' ? link.table : item.name)) {
      // A missing user in context keeps the table and swaps the user; anything else opens the match.
      if (item.kind === 'user' && link.kind === 'table') {
        if (focusKey) suggestions.push({ focus: focusKey, context: node.key, name: `${objectLinkName(link)} for ${node.name}` });
      } else suggestions.push({ focus: node.key, context: null, name: node.name });
    }
  }
  const incomplete = [...new Set(missing.map(m => SOURCE_OF[m.kind]))].flatMap(source => {
    const entry = coverage.find(c => c.source === source);
    return entry && entry.state !== 'readable' ? [`system.${source}: ${entry.state}`] : [];
  });
  return { requested: link.kind === 'table' && link.user ? `${objectLinkName(link)} for ${link.user}` : objectLinkName(link),
    missing, incomplete, suggestions: suggestions.slice(0, 5) };
}

/** The link that reopens focus on `node`, carrying the chain's user to a table. A node
 * without a link of its own reopens on the user, or returns null. */
export function objectLinkFor(node: GraphNode | undefined, chainUser?: string): ObjectLink | null {
  const record = node?.records[0]?.attributes;
  if (node?.kind === 'table' && record && !record.unresolvedReference) {
    return { kind: 'table', database: String(record.database), table: String(record.name), ...(chainUser ? { user: chainUser } : {}) };
  }
  if (node?.kind === 'user') return { kind: 'user', user: node.name };
  if (node?.kind === 'role') return { kind: 'role', role: node.name };
  return chainUser ? { kind: 'user', user: chainUser } : null;
}

/** Graph key of the table `database.table` names, split at its first dot; null without one. */
export function tableKeyOf(connection: string, qualified: string): string | null {
  const dot = qualified.indexOf('.');
  return dot > 0 && dot < qualified.length - 1 ? objectKey(connection, 'table', qualified.slice(0, dot), qualified.slice(dot + 1)) : null;
}

/** A table's focus graph narrowed to one principal: access objects on its configured paths
 * to the table (edgesBetween), row policies that name a principal on those paths, and every
 * data-flow and storage object. */
export function withPrincipalContext(graph: ObjectGraph, principal: string, target: string): { graph: ObjectGraph } {
  const onPath = edgesBetween(graph, principal, target);
  const keep = new Set([principal, target]);
  for (const edge of graph.edges) if (onPath.has(edge.key)) { keep.add(edge.from);keep.add(edge.to); }
  for (const edge of graph.edges) if (edge.kind === 'policy-recipient' && keep.has(edge.to)) keep.add(edge.from);
  const access = new Set(graph.nodes.filter(n => ['user', 'role', 'grant-scope', 'row-policy'].includes(n.kind)).map(n => n.key));
  const visible = (key: string) => !access.has(key) || keep.has(key);
  // Assignments, grants and name matches stay only on a path; other principals may share a role or privilege.
  const pathOnly = (edge: ObjectGraph['edges'][number]) => ['access', 'candidate'].includes(RELATIONSHIPS[edge.kind].family);
  const edges = graph.edges.filter(edge => visible(edge.from) && visible(edge.to) && (!pathOnly(edge) || onPath.has(edge.key)));
  return { graph: { ...graph, nodes: graph.nodes.filter(n => visible(n.key)), edges } };
}
