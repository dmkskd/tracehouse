import { projectCatalogGraph, provenancePaths } from '../projections/graph.js';
import { RELATIONSHIPS, isRevokeEdge } from '../model/relationships.js';
import { objectKey, type Capture, type Coverage, type GraphNode, type ObjectGraph, type Source } from '../model/types.js';

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
      const parts = wanted.split(/[_.\-]+/).filter(Boolean);
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

/** What the configured paths between a principal and a table hold: `paths` grant paths,
 * `revokes` paths that end in a partial revoke of the privilege, and `inactiveRoles` roles on a
 * path that the principal holds without them being default roles (inactive until SET ROLE). */
export interface PrincipalContextSummary {
  /** Grant paths that no revoke on the same holder cancels. */
  paths: number;
  /** Of `paths`, those whose first role the principal holds as a default role, or with no role. */
  activePaths: number;
  /** Columns the active paths' grants name, when every one of them names columns (SELECT(id)); else empty. */
  columnsOnly: string[];
  /** Paths that end in a partial revoke. */
  revokes: number;
  /** Roles on a path the principal holds without them being default roles (inactive until SET ROLE). */
  inactiveRoles: string[];
  /** Users and roles holding the grants of `paths`. */
  grantedThrough: string[];
  /** Holders of a revoke for the table that cancels none of their own grants. */
  revokedOn: string[];
  /** Grants a partial revoke on the same holder cancels for the table: ClickHouse applies a
   * principal's revoke to its own grants. Revokes are not combined across holders. */
  cancelled: { holder: string; grant: string }[];
  /** Keys of the cancelled grants' links to the table, for the diagram to mark. */
  cancelledEdges: string[];
}

/** A line about a principal and a table: the text, and the same text in parts for styling. */
export interface ProvenanceLine {
  text: string;
  parts: { text: string; kind: 'lead' | 'name' | 'plain' }[];
}

/** What the configured paths from a principal to a table hold (PrincipalContextSummary). */
export function summarizePrincipalPaths(graph: ObjectGraph, principal: string, target: string): PrincipalContextSummary & { all: ReturnType<typeof provenancePaths> } {
  const all = provenancePaths(graph, principal, target);
  const edgeOf = new Map(graph.edges.map(e => [e.key, e]));
  const nameOf = (key: string) => graph.nodes.find(n => n.key === key)?.name ?? key;
  const accessOf = (key: string) => graph.nodes.find(n => n.key === key)?.records[0]?.attributes.access_type;
  const columnOf = (key: string) => graph.nodes.find(n => n.key === key)?.records[0]?.attributes.column;
  // Each path's grant link (holder → privilege), its link to the table, and whether it is a revoke.
  const steps = all.map(p => {
    const edges = p.edges.map(key => edgeOf.get(key)).filter((e): e is ObjectGraph['edges'][number] => !!e);
    const grant = [...edges].reverse().find(e => e.kind === 'grant');
    const toTable = edges.find(e => e.kind === 'candidate-scope' && e.to === target);
    // A role the principal holds without it being a default role is inactive until SET ROLE; roles it grants follow it.
    const inactive = edges.some(e => e.kind === 'has-role' && e.from === principal && e.records.every(r => Number(r.evidence.granted_role_is_default) === 0));
    return { grant, toTable, revoke: edges.some(isRevokeEdge), inactive };
  }).filter((s): s is { grant: ObjectGraph['edges'][number]; toTable: ObjectGraph['edges'][number] | undefined; revoke: boolean; inactive: boolean } => !!s.grant);
  const revokedByHolder = new Map<string, Set<unknown>>();
  for (const s of steps.filter(s => s.revoke)) {
    const revoked = revokedByHolder.get(s.grant.from) ?? new Set();
    revoked.add(accessOf(s.grant.to));revokedByHolder.set(s.grant.from, revoked);
  }
  const grants = steps.filter(s => !s.revoke);
  const cancelledSteps = grants.filter(s => revokedByHolder.get(s.grant.from)?.has(accessOf(s.grant.to)));
  const live = grants.filter(s => !cancelledSteps.includes(s));
  const cancellingHolders = new Set(cancelledSteps.map(s => s.grant.from));
  const active = live.filter(s => !s.inactive);
  const columns = active.map(s => columnOf(s.grant.to));
  const inactiveRoles = [...new Set(all.flatMap(p => p.edges.map(key => edgeOf.get(key)).filter((e): e is ObjectGraph['edges'][number] => !!e)
    .filter(e => e.kind === 'has-role' && e.from === principal && e.records.every(r => Number(r.evidence.granted_role_is_default) === 0))
    .map(e => nameOf(e.to))))].sort();
  return {
    all, paths: live.length, activePaths: active.length,
    columnsOnly: columns.length && columns.every(c => c != null && c !== '') ? [...new Set(columns.map(String))].sort() : [],
    revokes: steps.filter(s => s.revoke).length, inactiveRoles,
    grantedThrough: [...new Set(live.map(s => nameOf(s.grant.from)))].sort(),
    revokedOn: [...revokedByHolder.keys()].filter(holder => !cancellingHolders.has(holder)).map(nameOf).sort(),
    cancelled: [...new Map(cancelledSteps.map(s => [`${s.grant.from}|${s.grant.to}`, { holder: nameOf(s.grant.from), grant: nameOf(s.grant.to) }])).values()],
    cancelledEdges: cancelledSteps.flatMap(s => s.toTable ? [s.toTable.key] : []),
  };
}

/** How the configured paths answer one operation: `granted` with a path active at login,
 * `columns` when the active paths' grants name only some columns, `set-role` with paths only
 * through roles that need SET ROLE, `revoked` when a revoke on the same holder cancels every
 * grant, `no-path` with no grant at all. Configuration only. */
export type ProvenanceState = 'granted' | 'columns' | 'set-role' | 'revoked' | 'no-path';
export function provenanceState(summary: Pick<PrincipalContextSummary, 'paths' | 'activePaths' | 'columnsOnly' | 'cancelled'>): ProvenanceState {
  if (summary.paths > 0) return summary.activePaths === 0 ? 'set-role' : summary.columnsOnly.length ? 'columns' : 'granted';
  return summary.cancelled.length ? 'revoked' : 'no-path';
}

/** Graph key of the table `database.table` names, split at its first dot; null without one. */
export function tableKeyOf(connection: string, qualified: string): string | null {
  const dot = qualified.indexOf('.');
  return dot > 0 && dot < qualified.length - 1 ? objectKey(connection, 'table', qualified.slice(0, dot), qualified.slice(dot + 1)) : null;
}

/** How the configured paths from a user to a table answer one access type: the state, the line
 * explaining it, and the summary with the graph keys of the user and table. */
export interface ProvenanceAnswer { operation: string; state: ProvenanceState; line: ProvenanceLine; summary: PrincipalContextSummary; user: string; target: string }

/** The answer for `user`, `table` and `operation` from any capture holding both: a catalog, or a
 * read of the user and table by name. Privileges link to the table as the catalog links them for a
 * focused table (projectCatalogGraph), whatever links the capture was collected with. */
export function answerFor(capture: Capture, user: string, table: string, operation: string): ProvenanceAnswer {
  const unlinked = { ...capture, relationships: capture.relationships.filter(r => r.kind !== 'candidate-scope') };
  const graph = projectCatalogGraph(unlinked, table, Number.MAX_SAFE_INTEGER, [operation]);
  const { all: _all, ...summary } = summarizePrincipalPaths(graph, user, table);
  const nameOf = (key: string) => graph.nodes.find(n => n.key === key)?.name ?? key;
  return { operation, state: provenanceState(summary), line: describeProvenance(summary, nameOf(user), nameOf(table), operation), summary, user, target: table };
}

/**
 * The line under a provenance view and in the focus header: which holders grant the
 * privilege, which grants a revoke on the same holder cancels, and roles that need SET ROLE.
 * Configuration only: ClickHouse decides access when a query runs.
 */
export function describeProvenance(summary: PrincipalContextSummary, principal: string, table: string, operation?: string | readonly string[]): ProvenanceLine {
  type Part = ProvenanceLine['parts'][number];
  const plain = (text: string): Part => ({ text, kind: 'plain' }), name = (text: string): Part => ({ text, kind: 'name' });
  const list = (names: string[]): Part[] => names.flatMap((n, i) => [...(i ? [plain(', ')] : []), name(n)]);
  // The access types asked: one for a provenance, any of several for a catalog focus.
  const operations = operation === undefined ? [] : typeof operation === 'string' ? [operation] : [...operation];
  const asked = operations.flatMap((op, i): Part[] => [...(i ? [plain(' or ')] : []), name(op)]);
  const cancelled = summary.cancelled.flatMap((c, i): Part[] => [...(i ? [plain('; ')] : []), name(c.holder), plain("'s "), name(c.grant), plain(' is revoked for '), name(table)]);
  const parts: Part[] = summary.paths > 0
    ? [{ text: 'Granted through', kind: 'lead' }, plain(' '), ...list(summary.grantedThrough), ...(asked.length ? [plain(' for '), ...asked] : []),
      ...(summary.revokedOn.length ? [plain(' · revoked on '), ...list(summary.revokedOn)] : []),
      ...(cancelled.length ? [plain(' · '), ...cancelled] : [])]
    : cancelled.length
      ? [{ text: 'No grant path left:', kind: 'lead' }, plain(' '), ...cancelled]
      : [{ text: 'No grant path:', kind: 'lead' }, plain(' no role or privilege of '), name(principal), plain(' covers '),
        ...(asked.length ? [...asked, plain(' on ')] : []), name(table),
        ...(summary.revokes ? [plain(` (${summary.revokes} revoke${summary.revokes === 1 ? '' : 's'})`)] : [])];
  if (summary.columnsOnly.length) parts.push(plain(' · only columns '), ...list(summary.columnsOnly));
  for (const role of summary.inactiveRoles) {
    parts.push(plain('. '), name(role), plain(' is not active when '), name(principal), plain(' logs in; it needs '), name(`SET ROLE ${role}`));
  }
  return { text: parts.map(p => p.text).join(''), parts };
}

/** A table's focus graph narrowed to one principal: access objects on its configured paths
 * to the table, row policies that name a principal on those paths, and every data-flow and
 * storage object, with a summary of those paths. */
export function withPrincipalContext(graph: ObjectGraph, principal: string, target: string): { graph: ObjectGraph } & PrincipalContextSummary {
  const { all, ...summary } = summarizePrincipalPaths(graph, principal, target);
  const paths = all;
  const onPath = new Set(paths.flatMap(p => p.edges));
  const keep = new Set([principal, target]);
  for (const edge of graph.edges) if (onPath.has(edge.key)) { keep.add(edge.from);keep.add(edge.to); }
  for (const edge of graph.edges) if (edge.kind === 'policy-recipient' && keep.has(edge.to)) keep.add(edge.from);
  const access = new Set(graph.nodes.filter(n => ['user', 'role', 'grant-scope', 'row-policy'].includes(n.kind)).map(n => n.key));
  const visible = (key: string) => !access.has(key) || keep.has(key);
  // Assignments, grants and name matches stay only on a path; other principals may share a role or privilege.
  const pathOnly = (edge: ObjectGraph['edges'][number]) => ['access', 'candidate'].includes(RELATIONSHIPS[edge.kind].family);
  const edges = graph.edges.filter(edge => visible(edge.from) && visible(edge.to) && (!pathOnly(edge) || onPath.has(edge.key)));
  return { graph: { ...graph, nodes: graph.nodes.filter(n => visible(n.key)), edges }, ...summary };
}
