import { databaseOf, type ObjectGraph } from '@tracehouse/core/services/object-browser';
import type { DatabaseGroup } from './ObjectViewModel';

/** Membership uses canonical name parts, never a split of display names on dots. */
export function databaseGroups(graph: ObjectGraph | null): DatabaseGroup[] {
  const groups = new Map<string, string[]>();
  for (const node of graph?.nodes ?? []) {
    const database = databaseOf(node);
    if (node.kind !== 'table' || database === undefined) continue;
    const keys = groups.get(database) ?? [];
    keys.push(node.key);groups.set(database, keys);
  }
  return [...groups].sort(([a], [b]) => a.localeCompare(b))
    .map(([name, keys]) => ({ name, keys, count: keys.length }));
}
