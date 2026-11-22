import { canonical } from '../model/canonical.js';
import { KINDS } from '../model/kinds.js';
import { RELATIONSHIPS } from '../model/relationships.js';
import type { Capture, GraphNode, ObjectGraph, ObjectKind, Source } from '../model/types.js';

/** Per-host measurements: replicas and shards hold different amounts of data, so these
 * are left out when host records are compared. */
const MEASURED = new Set(['total_rows', 'total_bytes', 'active_parts', 'newest_part']);
const definition = (attributes: Record<string, unknown>) =>
  canonical(Object.fromEntries(Object.entries(attributes).filter(([name]) => !MEASURED.has(name))));

/** Display coalescing preserves native records. Never union grants into a principal. */
export function projectGraph(capture: Capture): ObjectGraph {
  const expectedHosts = [...new Set(capture.hosts.map(h => h.host))].sort();
  const grouped = new Map<string, GraphNode>();
  for (const record of capture.observations) {
    let node = grouped.get(record.key);
    if (!node) {
      node = { key: record.key, kind: record.kind, name: record.name, records: [],
        observedHosts: [], expectedHosts, differs: false, partial: false };
      grouped.set(record.key, node);
    }
    node.records.push(record);
  }
  const readable = (source: Source, host: string) => capture.coverage.some(c =>
    c.source === source && c.state === 'readable' && c.hosts.includes(host));
  for (const node of grouped.values()) {
    const resolved = node.records.filter(r => !r.attributes.unresolvedReference);
    node.observedHosts = [...new Set(resolved.map(r => r.host))].sort();
    const attributes = new Set(resolved.map(r => definition(r.attributes)));
    const ownSets = new Set<string>();
    for (const record of node.records) {
      const ownReadable = readable('grants', record.host) && readable('role_grants', record.host);
      if (record.attributes.unresolvedReference || !readable(record.source, record.host)
        || ((node.kind === 'user' || node.kind === 'role') && !ownReadable)) node.partial = true;
      if (!record.attributes.unresolvedReference && ownReadable) {
        const own = capture.relationships.filter(e => e.host === record.host && e.from === node.key
          && (e.kind === 'grant' || e.kind === 'has-role'))
          .map(e => canonical([e.kind, e.to, e.evidence])).sort();
        ownSets.add(canonical([...new Set(own)]));
      }
    }
    // Host comparison applies only to kinds expected on every host (KINDS[kind].everyHost).
    const everyHost = KINDS[node.kind].everyHost;
    node.differs = everyHost && (attributes.size > 1 || ownSets.size > 1);
    if (everyHost) node.partial ||= node.observedHosts.length !== expectedHosts.length;
  }
  const edges = new Map<string, ObjectGraph['edges'][number]>();
  for (const r of capture.relationships) {
    if (!grouped.has(r.from) || !grouped.has(r.to)) continue;
    const key = canonical([r.from, r.to, r.kind, r.label, r.evidence]);
    let edge = edges.get(key);
    if (!edge) { edge = { key, from: r.from, to: r.to, kind: r.kind, label: r.label, hosts: [], records: [] }; edges.set(key, edge); }
    edge.records.push(r);
    if (!edge.hosts.includes(r.host)) edge.hosts.push(r.host);
  }
  return { nodes: [...grouped.values()], edges: [...edges.values()] };
}

/** Enumerate bounded directed paths; an edge must exist on a common host throughout. */
export function provenancePaths(graph: ObjectGraph, start: string, target: string, maxPaths = 50):
  { edges: string[]; hosts: string[] }[] {
  const paths: { edges: string[]; hosts: string[] }[] = [];
  const walk = (at: string, hosts: string[] | null, path: string[], seen: Set<string>) => {
    if (paths.length >= maxPaths || path.length >= 16) return;
    if (at === target) { paths.push({ edges: path, hosts: hosts ?? [] }); return; }
    for (const edge of graph.edges) {
      if (edge.from !== at || seen.has(edge.to) || !RELATIONSHIPS[edge.kind].provenance) continue;
      const common = hosts === null ? edge.hosts : hosts.filter(h => edge.hosts.includes(h));
      if (common.length) walk(edge.to, common, [...path, edge.key], new Set([...seen, edge.to]));
    }
  };
  walk(start, null, [], new Set([start]));
  return paths;
}

/** Tables a broad scope may link to, per focus kind; `null` is every table. A table
 * and a storage server scope it to their own tables, so the cap never drops them. */
const FOCUS_TABLES: Record<ObjectKind, (capture: Capture, focus: string) => ReadonlySet<string> | null> = {
  table: (_, focus) => new Set([focus]),
  location: (capture, focus) => new Set(capture.relationships.filter(r => r.kind === 'points-to' && r.to === focus).map(r => r.from)),
  'row-policy': (capture, focus) => new Set(capture.relationships.filter(r => r.kind === 'policy-target' && r.from === focus).map(r => r.to)),
  user: () => null, role: () => null, 'grant-scope': () => null,
};

/** Catalog projection expands broad name scopes for one focus object, over the tables
 * FOCUS_TABLES gives for its kind, at most `maxMatches` per scope, with the full count
 * in `scopeMatches`. Without a focus object only exact scopes link. Edges remain
 * host-specific metadata candidates, including revokes as evidence. */
export function projectCatalogGraph(capture: Capture, focus?: string, maxMatches = 100): ObjectGraph {
  const relationships = [...capture.relationships];
  const focusKind = capture.observations.find(o => o.key === focus)?.kind;
  const scoped = focus && focusKind ? FOCUS_TABLES[focusKind](capture, focus) : null;
  const tables = capture.observations.filter(o => o.kind === 'table' && !o.attributes.unresolvedReference
    && (!scoped || scoped.has(o.key)));
  const matched = new Map<string, Set<string>>(), linked = new Map<string, Set<string>>();
  for (const grant of capture.relationships.filter(e => e.kind === 'grant')) {
    const r = grant.evidence;
    if (Number(r.is_wildcard) === 1 || r.access_object || !['SELECT', 'INSERT'].includes(String(r.access_type))) continue;
    // Without a focus object, only exact scopes are expanded.
    if (!focusKind && (r.database == null || r.table == null)) continue;
    const all = matched.get(grant.to) ?? new Set<string>(), shown = linked.get(grant.to) ?? new Set<string>();
    matched.set(grant.to, all);linked.set(grant.to, shown);
    for (const table of tables) {
      if (table.host !== grant.host || (r.database != null && r.database !== table.attributes.database)
        || (r.table != null && r.table !== table.attributes.name)) continue;
      all.add(table.key);
      if (!shown.has(table.key) && shown.size >= maxMatches) continue;
      shown.add(table.key);
      const label = Number(r.is_partial_revoke) === 1 ? 'excluded scope · evidence' : 'candidate name match';
      relationships.push({ ...grant, id: canonical([grant.id, table.key]), from: grant.to, to: table.key,
        kind: 'candidate-scope', label });
    }
  }
  const graph = projectGraph({ ...capture, relationships });
  const scopeMatches = Object.fromEntries([...matched].filter(([, keys]) => keys.size > maxMatches).map(([key, keys]) => [key, keys.size]));
  return Object.keys(scopeMatches).length ? { ...graph, scopeMatches } : graph;
}
