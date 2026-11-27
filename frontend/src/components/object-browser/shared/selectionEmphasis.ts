import { databaseOf, lineage, type ObjectGraph } from '@tracehouse/core/services/object-browser';

/** Relationship emphasis, not authorization. Upstream and downstream traversal use
 * core lineage independently, so sharing a role does not pull in sibling users. */
export function selectionEmphasis({ graph, lineageGraph = graph, hoverGraph = lineageGraph, selected, hovered,
  highlight = false, selectedDatabase, showAll = false, muted }: {
  graph: ObjectGraph; lineageGraph?: ObjectGraph; hoverGraph?: ObjectGraph;
  selected: string | null; hovered?: string | null; highlight?: boolean;
  selectedDatabase?: string | null; showAll?: boolean; muted?: ReadonlySet<string>;
}) {
  const connected = selected ? lineage(lineageGraph, selected) : new Set<string>();
  const hoverConnected = hovered ? lineage(hoverGraph, hovered) : new Set<string>();
  const databaseMembers = new Set<string>(), otherDatabaseMembers = new Set<string>();
  for (const node of graph.nodes) {
    const database = databaseOf(node);
    if (selectedDatabase && database === selectedDatabase) databaseMembers.add(node.key);
    else if (selectedDatabase && database !== undefined) otherDatabaseMembers.add(node.key);
  }
  const dimmed = (key: string) => highlight && hovered ? !hoverConnected.has(key)
    : otherDatabaseMembers.has(key) || (!showAll && !!selected && !connected.has(key));
  const linked = (from: string, to: string) => highlight && hovered
    ? hoverConnected.has(from) && hoverConnected.has(to)
    : selectedDatabase ? !otherDatabaseMembers.has(from) && !otherDatabaseMembers.has(to)
      : showAll || !selected || (connected.has(from) && connected.has(to));
  const activeDatabases = new Set(graph.nodes.filter(n => n.kind === 'table' && !dimmed(n.key) && !muted?.has(n.key)).map(databaseOf));
  return { dimmed, linked, databaseMembers, activeDatabases };
}
