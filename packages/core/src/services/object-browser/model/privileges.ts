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

/** The table-level operations a privilege includes (tableLevelOperations): ALL includes SELECT and
 * DROP TABLE, ALTER includes ALTER DELETE. Without the tree only SELECT and INSERT, by exact name. */
export function tableOperations(tree: PrivilegeTree | null, privilege: string): string[] {
  if (!tree) return ['SELECT', 'INSERT'].filter(op => privilege === op);
  let names = operationNames.get(tree);
  if (!names) { names = tableLevelOperations(tree).map(op => op.name);operationNames.set(tree, names); }
  return names.filter(op => privilegeIncludes(tree, privilege, op));
}
// One list per tree: the collector asks for every grant row.
const operationNames = new WeakMap<PrivilegeTree, string[]>();

/** Levels whose privileges act on one table, view or its columns. */
const TABLE_LEVELS = new Set(['TABLE', 'COLUMN', 'VIEW']);
/** A privilege a provenance can ask about, with its group (privilegeGroup). */
export interface TableOperation { name: string; group: string | null }

/** Privileges that act on one table, view or its columns, in the server's order. */
export function tableLevelOperations(tree: PrivilegeTree): TableOperation[] {
  return [...tree].filter(([, node]) => node.level !== null && TABLE_LEVELS.has(node.level))
    .map(([name]) => ({ name, group: privilegeGroup(tree, name) }));
}

/** A selection of operations with `operation` added or removed; the last one stays selected. */
export function toggleOperation(selected: readonly string[], operation: string): string[] {
  if (!selected.includes(operation)) return [...selected, operation];
  return selected.length > 1 ? selected.filter(op => op !== operation) : [...selected];
}

/** Operations by group, groups and operations in first-seen order. */
export function groupOperations(operations: TableOperation[]): { group: string; names: string[] }[] {
  const groups = new Map<string, string[]>();
  for (const op of operations) {
    const group = op.group ?? op.name;
    groups.set(group, [...(groups.get(group) ?? []), op.name]);
  }
  return [...groups].map(([group, names]) => ({ group, names }));
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
