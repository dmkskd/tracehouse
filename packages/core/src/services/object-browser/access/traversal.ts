import type { GraphEdge, ObjectGraph } from '../model/types.js';

interface HostAdjacency {
  edges: GraphEdge[];
  forward: Map<string, string[]>;
  reverse: Map<string, string[]>;
}
/** Local indexes have the lifetime of one traversal batch; mutable graphs are never cached. */
function adjacencyByHost(graph: ObjectGraph): Map<string, HostAdjacency> {
  const hosts = new Map<string, HostAdjacency>();
  for (const edge of graph.edges) {
    if (!['has-role', 'grant', 'candidate-scope'].includes(edge.kind)) continue;
    for (const host of edge.hosts) {
      const adjacency: HostAdjacency = hosts.get(host) ?? { edges: [], forward: new Map(), reverse: new Map() };
      adjacency.edges.push(edge);
      const out = adjacency.forward.get(edge.from) ?? [];out.push(edge.to);adjacency.forward.set(edge.from, out);
      const incoming = adjacency.reverse.get(edge.to) ?? [];incoming.push(edge.from);adjacency.reverse.set(edge.to, incoming);
      hosts.set(host, adjacency);
    }
  }
  return hosts;
}
function reach(start: string, adjacency: ReadonlyMap<string, readonly string[]>): Set<string> {
  const seen = new Set([start]), queue = [start];
  for (let cursor = 0; cursor < queue.length; cursor++) {
    for (const next of adjacency.get(queue[cursor]) ?? []) if (!seen.has(next)) { seen.add(next);queue.push(next); }
  }
  return seen;
}

/** Edges on a configured path, preserving a common host throughout and without path bounds. */
export function edgesBetween(graph: ObjectGraph, user: string, table: string): Set<string> {
  const on = new Set<string>();
  for (const host of adjacencyByHost(graph).values()) {
    const fromUser = reach(user, host.forward), toTable = reach(table, host.reverse);
    for (const edge of host.edges) if (fromUser.has(edge.from) && toTable.has(edge.to)) on.add(edge.key);
  }
  return on;
}

/** All principals with a configured path to a table. One reverse search per host, including
 * shared roles, cycles and partial-revoke evidence; no search is repeated per user. */
export function principalsReaching(graph: ObjectGraph, table: string): ReadonlySet<string> {
  const keys = new Set<string>();
  for (const host of adjacencyByHost(graph).values()) for (const key of reach(table, host.reverse)) keys.add(key);
  return keys;
}
