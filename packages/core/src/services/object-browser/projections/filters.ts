import type { ObjectGraph } from '../model/types.js';
import { nameParts } from '../presentation/cards.js';

/** Database ownership uses canonical name parts, including names containing dots. */
export function databaseOf(node: ObjectGraph['nodes'][number]): string | undefined {
  return node.kind === 'table' || node.kind === 'row-policy' ? nameParts(node)[0] : undefined;
}

/** Restricts catalog data to selected databases. Access configuration stays visible;
 * external storage stays only when a remaining table points to it. Focus uses the full graph. */
export function inDatabases(graph: ObjectGraph, databases: readonly string[] | null): ObjectGraph {
  if (databases === null) return graph;
  const selected = new Set(databases);
  const kept = new Set(graph.nodes.filter(n => {
    const database = databaseOf(n);
    return database !== undefined ? selected.has(database) : n.kind !== 'location' && n.kind !== 'disk';
  }).map(n => n.key));
  for (const edge of graph.edges) if (edge.kind === 'points-to' && kept.has(edge.from)) kept.add(edge.to);
  return { ...graph, nodes: graph.nodes.filter(n => kept.has(n.key)), edges: graph.edges.filter(e => kept.has(e.from) && kept.has(e.to)) };
}

/** Removes the default user, its links, and grant scopes granted only to it.
 * Scopes also granted to other principals stay. */
export function withoutDefaultUser(graph: ObjectGraph): ObjectGraph {
  const hidden = new Set(graph.nodes.filter(n => n.kind === 'user' && n.name === 'default').map(n => n.key));
  for (const scope of graph.nodes.filter(n => n.kind === 'grant-scope')) {
    const owners = graph.edges.filter(e => e.kind === 'grant' && e.to === scope.key);
    if (owners.length && owners.every(e => hidden.has(e.from))) hidden.add(scope.key);
  }
  return { ...graph, nodes: graph.nodes.filter(n => !hidden.has(n.key)), edges: graph.edges.filter(e => !hidden.has(e.from) && !hidden.has(e.to)) };
}
