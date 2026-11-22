import type { ObjectGraph } from '../model/types.js';

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
