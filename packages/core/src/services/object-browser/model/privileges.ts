export interface PrivilegeNode { level: string | null; parent: string | null }
export type PrivilegeTree = Map<string, PrivilegeNode>;

export function privilegeTree(rows: Record<string, unknown>[]): PrivilegeTree {
  const text = (value: unknown) => value == null || value === '' ? null : String(value);
  return new Map(rows.map(row => [String(row.privilege), { level: text(row.level), parent: text(row.parent_group) }]));
}

/** The top-level group ClickHouse files a privilege under: the ancestor just below ALL.
 * SHOW TABLES → SHOW, SHOW USERS → ACCESS MANAGEMENT, SELECT → SELECT. */
export function privilegeGroup(tree: PrivilegeTree, privilege: string): string | null {
  if (!tree.has(privilege)) return null;
  let at = privilege;
  for (let depth = 0; depth < tree.size; depth++) {
    const parent = tree.get(at)?.parent;
    if (!parent || parent === 'ALL') return at;
    at = parent;
  }
  return null;
}
