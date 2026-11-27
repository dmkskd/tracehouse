import { canonical, databaseOf, type GraphNode, type ObjectGraph } from '@tracehouse/core/services/object-browser';

export interface SpatialGroup {
  id: string;
  kind: 'users' | 'user-set' | 'roles' | 'scope' | 'database' | 'external';
  label: string;
  note: string;
  database?: string;
  keys: string[];
  defaultExpanded: boolean;
}

const order = (a: GraphNode, b: GraphNode) => a.name.localeCompare(b.name) || a.key.localeCompare(b.key);

/** Identical assignment profiles, not equivalent access. Keep host coverage, default
 * role selectors and assignment options in the signature. Partial/differing users
 * remain individual; their missing evidence cannot establish equivalence. */
function roleProfile(graph: ObjectGraph, node: GraphNode): string {
  if (node.partial || node.differs) return canonical(['individual', node.key]);
  const defaults = node.records.map(r => [r.host, r.attributes.default_roles_all,
    r.attributes.default_roles_list, r.attributes.default_roles_except]);
  const assignments = graph.edges.filter(e => e.kind === 'has-role' && e.from === node.key)
    .flatMap(e => e.records.map(r => [r.host, r.to, r.evidence]));
  return canonical([node.observedHosts, defaults.map(canonical).sort(), assignments.map(canonical).sort()]);
}

/** Display-only groups. Every original node belongs to exactly one group; no grant,
 * relationship or host observation is unioned into a synthetic principal. */
export function groupObjects(graph: ObjectGraph): SpatialGroup[] {
  const groups: SpatialGroup[] = [];
  const add = (kind: SpatialGroup['kind'], label: string, nodes: GraphNode[], note: string,
    id = canonical([kind, label]), defaultExpanded = true, database?: string) => {
    if (nodes.length) groups.push({ id, kind, label, note, keys: [...nodes].sort(order).map(n => n.key), defaultExpanded, database });
  };
  const profiles = new Map<string, GraphNode[]>();
  for (const user of graph.nodes.filter(n => n.kind === 'user').sort(order)) {
    const profile = roleProfile(graph, user);
    profiles.set(profile, [...profiles.get(profile) ?? [], user]);
  }
  add('users', 'Users', [...profiles.values()].filter(p => p.length === 1).flat(), 'Individual assignment profiles');
  for (const [profile, users] of profiles) {
    if (users.length < 2) continue;
    const assigned = graph.edges.some(e => e.kind === 'has-role' && e.from === users[0].key);
    add('user-set', assigned ? 'Same role assignments' : 'No assigned roles', users,
      'Direct grants can differ · expand to inspect holders', canonical(['user-set', profile]), false);
  }
  add('roles', 'Roles', graph.nodes.filter(n => n.kind === 'role'), 'Recipient → assigned role; shared roles appear once');
  const scopes = new Map<string, GraphNode[]>();
  for (const node of graph.nodes.filter(n => n.kind === 'grant-scope')) {
    const variants = [...new Set(node.records.map(r => canonical([
      r.attributes.access_object ?? '', r.attributes.database ?? null, r.attributes.table ?? null, r.attributes.is_wildcard ?? 0,
    ])))].sort();
    const signature = canonical(variants);
    scopes.set(signature, [...scopes.get(signature) ?? [], node]);
  }
  for (const [signature, nodes] of [...scopes].sort(([a], [b]) => a.localeCompare(b))) {
    const attrs = nodes[0].records[0]?.attributes ?? {};
    const label = new Set(nodes.flatMap(n => n.records.map(r => canonical([
      r.attributes.access_object ?? '', r.attributes.database ?? null, r.attributes.table ?? null, r.attributes.is_wildcard ?? 0,
    ])))).size > 1 ? 'Scopes differ by host' : attrs.access_object ? String(attrs.access_object)
      : `${attrs.database ?? '*'}${Number(attrs.is_wildcard) === 1 && attrs.table == null && attrs.database != null ? '*' : ''}.${attrs.table ?? '*'}${Number(attrs.is_wildcard) === 1 && attrs.table != null ? '*' : ''}`;
    add('scope', label, nodes, 'Scope group · holders and grant options stay on individual objects', canonical(['scope', signature]));
  }
  const databases = new Map<string, GraphNode[]>();
  for (const node of graph.nodes) {
    const database = databaseOf(node);
    if (database !== undefined) databases.set(database, [...databases.get(database) ?? [], node]);
  }
  // Largest database first gives the overview a useful starting point. Selection
  // never changes group order. Native keys, not label splitting, determine ownership.
  const ordered = [...databases].sort(([a, an], [b, bn]) => bn.length - an.length || a.localeCompare(b));
  ordered.forEach(([name, nodes], i) => add('database', name, nodes, 'Tables, views and row policies', canonical(['database', name]), i < 2, name));
  add('external', 'Shared storage', graph.nodes.filter(n => n.kind === 'location' || n.kind === 'disk'), 'Endpoints and disks referenced across databases');
  return groups;
}
