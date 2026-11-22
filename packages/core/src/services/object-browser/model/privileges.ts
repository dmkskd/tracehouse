export interface PrivilegeNode { level: string | null; parent: string | null }
export type PrivilegeTree = Map<string, PrivilegeNode>;

export function privilegeTree(rows: Record<string, unknown>[]): PrivilegeTree {
  const text = (value: unknown) => value == null || value === '' ? null : String(value);
  return new Map(rows.map(row => [String(row.privilege), { level: text(row.level), parent: text(row.parent_group) }]));
}

/** Whether `granted` is `required` or one of its ancestors: ALL includes SELECT. */
export function privilegeIncludes(tree: PrivilegeTree, granted: string, required: string): boolean {
  let at: string | null = required;
  for (let depth = 0; at && depth <= tree.size; depth++) {
    if (at === granted) return true;
    at = tree.get(at)?.parent ?? null;
  }
  return false;
}

/** The table operations a privilege includes. Without the tree only the exact name counts. */
export function tableOperations(tree: PrivilegeTree | null, privilege: string): ('SELECT' | 'INSERT')[] {
  return (['SELECT', 'INSERT'] as const).filter(op => tree ? privilegeIncludes(tree, privilege, op) : privilege === op);
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
