import type { GraphNode, ObjectGraph } from '../model/types.js';

/** Ancestors and descendants of `origin`, each followed in one direction only, so
 * shared objects do not turn traversal sideways into other principals' grants. */
export function lineage(graph: ObjectGraph, origin: string): Set<string> {
  const keys = new Set([origin]);
  for (const downstream of [false, true]) {
    const adjacency = new Map<string, string[]>();
    for (const edge of graph.edges) {
      const [from, to] = downstream ? [edge.from, edge.to] : [edge.to, edge.from];
      adjacency.set(from, [...adjacency.get(from) ?? [], to]);
    }
    const reached = new Set([origin]), pending = [origin];
    while (pending.length) {
      for (const next of adjacency.get(pending.pop()!) ?? []) {
        if (!reached.has(next)) { reached.add(next); keys.add(next); pending.push(next); }
      }
    }
  }
  return keys;
}

const restrict = (graph: ObjectGraph, keys: ReadonlySet<string>): ObjectGraph => ({ ...graph,
  nodes: graph.nodes.filter(node => keys.has(node.key)), edges: graph.edges.filter(edge => keys.has(edge.from) && keys.has(edge.to)) });

/** The focus graph of one object: its lineage, plus row policies that name a reached
 * principal and those policies' targets. Policies that only share a reached table stay out. */
export function focusGraphFrom(graph: ObjectGraph, origin: string): ObjectGraph {
  const keys = lineage(graph, origin);
  for (const edge of graph.edges) if (edge.kind === 'policy-recipient' && keys.has(edge.to)) keys.add(edge.from);
  for (const edge of graph.edges) if (edge.kind === 'policy-target' && keys.has(edge.from)) keys.add(edge.to);
  return restrict(graph, keys);
}

/** Objects whose name contains `query` (case-insensitive), and the graph of those
 * matches plus the selected object and its direct neighbors. An empty query keeps the graph. */
export function searchGraph(graph: ObjectGraph, query: string, selected: string | null): { matches: GraphNode[]; graph: ObjectGraph } {
  const needle = query.trim().toLowerCase();
  if (!needle) return { matches: [], graph };
  const matches = graph.nodes.filter(node => node.name.toLowerCase().includes(needle));
  const visible = new Set(matches.map(node => node.key));
  if (selected) {
    visible.add(selected);
    for (const edge of graph.edges) if (edge.from === selected || edge.to === selected) { visible.add(edge.from); visible.add(edge.to); }
  }
  return { matches, graph: restrict(graph, visible) };
}

/** The essential lines of a graph: database-wide and global privileges keep no line per
 * matched table (their card states the match count). `stubs` lists those privileges, so a
 * view can end each in a short line at the data band. Table privileges keep their lines;
 * every object stays. */
export function essentialEdges(graph: ObjectGraph): { graph: ObjectGraph; stubs: string[] } {
  const broad = new Set(graph.nodes.filter(n => n.kind === 'grant-scope'
    && n.records.some(r => r.attributes.access_type != null && (r.attributes.database == null || r.attributes.table == null))).map(n => n.key));
  const dropped = new Set(graph.edges.filter(e => e.kind === 'candidate-scope' && broad.has(e.from)));
  return { graph: { ...graph, edges: graph.edges.filter(e => !dropped.has(e)) }, stubs: [...new Set([...dropped].map(e => e.from))] };
}
