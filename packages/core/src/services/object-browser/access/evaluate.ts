import type { Capture } from '../model/types.js';
import { createAccessIndex, type AccessIndex } from './index.js';
import { scopeRank, compareRank, type TableName } from './scope.js';
import type { AccessEvaluation, ReachEvaluation, HostAccess, Columns } from './types.js';
import { evidenceGaps } from './evidence.js';

/** Roles a user holds on a host: those active at login (default roles and the roles they hold),
 * and every role it holds. */
function roleClosure(index: AccessIndex, host: string, user: string): { active: Set<string>; all: Set<string> } {
  const walk = (activeOnly: boolean) => {
    const seen = new Set<string>(), queue = [user];
    for (let cursor = 0; cursor < queue.length; cursor++) {
      const from = queue[cursor];
      for (const { role, isDefault } of index.roles(host, from)) {
        if (seen.has(role) || (activeOnly && from === user && !isDefault)) continue;
        seen.add(role);queue.push(role);
      }
    }
    return seen;
  };
  return { active: walk(true), all: walk(false) };
}

interface HolderAccess { columns: Columns; grants: string[]; cancelled: string[] }

/** One holder's access to the table on one host: the most specific rule decides the table, and
 * column rules then decide their columns. A revoke wins a tie. */
function holderAccess(index: AccessIndex, host: string, holder: string, table: TableName, operation: string): HolderAccess {
  const rules = index.scopes(host, holder).flatMap(scope => {
    const rule = index.scope(scope, host);
    const rank = rule ? scopeRank(rule, table, operation) : null;
    return rule && rank ? [{ scope, rank, revoke: rule.effect === 'revoke', column: rule.target.kind === 'column' ? rule.target.column : null }] : [];
  });
  const tableRules = rules.filter(r => r.rank[0] < 5).sort((a, b) => compareRank(b.rank, a.rank) || Number(b.revoke) - Number(a.revoke));
  const top = tableRules[0];
  const whole = !!top && !top.revoke;
  const columnRules = rules.filter(r => r.rank[0] === 5);
  const revokedColumns = [...new Set(columnRules.filter(r => r.revoke).map(r => r.column!))].sort();
  const grantedColumns = [...new Set(columnRules.filter(r => !r.revoke && !revokedColumns.includes(r.column!)).map(r => r.column!))].sort();
  const columns: Columns = whole ? (revokedColumns.length ? { kind: 'except', columns: revokedColumns } : { kind: 'all' })
    : grantedColumns.length ? { kind: 'only', columns: grantedColumns } : { kind: 'none' };
  const grants = rules.filter(r => !r.revoke).map(r => r.scope);
  // Grants this holder holds for the table that its own more specific revoke takes back.
  const cancelled = top?.revoke ? tableRules.filter(r => !r.revoke && compareRank(r.rank, top.rank) < 0).map(r => r.scope) : [];
  return { columns, grants, cancelled };
}

/** Union across holders, as ClickHouse combines a user's own rights with its enabled roles'. */
function union(each: Columns[]): Columns {
  if (each.some(c => c.kind === 'all')) return { kind: 'all' };
  const only = [...new Set(each.flatMap(c => c.kind === 'only' ? c.columns : []))];
  const excepts = each.flatMap(c => c.kind === 'except' ? [c.columns] : []);
  if (excepts.length) {
    const except = excepts.reduce((a, b) => a.filter(c => b.includes(c))).filter(c => !only.includes(c)).sort();
    return except.length ? { kind: 'except', columns: except } : { kind: 'all' };
  }
  return only.length ? { kind: 'only', columns: only.sort() } : { kind: 'none' };
}

/** One host's evaluation: per holder (the user, and the roles it holds there), then the union. */
function hostAccess(index: AccessIndex, host: string, user: string, tableKey: string, operation: string): HostAccess {
  const table = index.table(tableKey, host)!;
  const { active, all } = roleClosure(index, host, user);
  const evaluate = (holders: string[]) => holders.map(holder => ({ holder, ...holderAccess(index, host, holder, table, operation) }));
  const atLogin = evaluate(index.principal(user, host) ? [user, ...active] : []);
  const loginColumns = union(atLogin.map(h => h.columns));
  const held = loginColumns.kind !== 'none' ? atLogin : evaluate(index.principal(user, host) ? [user, ...all] : []);
  const heldColumns = loginColumns.kind !== 'none' ? loginColumns : union(held.map(h => h.columns));
  // Holders that grant the whole table on their own, else every holder whose columns make up the union.
  const whole = held.filter(h => h.columns.kind === 'all');
  const contributing = whole.length ? whole : held.filter(h => h.columns.kind !== 'none');
  const names = (keys: string[]) => keys.map(index.nameOf).sort();
  const base = { host, columns: heldColumns, grantedThrough: names(contributing.map(h => h.holder)),
    holders: held.map(h => ({ holder: h.holder, grants: h.grants, cancelled: h.cancelled })),
    cancelled: held.flatMap(h => h.cancelled.map(scope => ({ holder: index.nameOf(h.holder), scope }))) };
  if (loginColumns.kind !== 'none') return { ...base, state: loginColumns.kind === 'all' ? 'granted' : 'columns', inactiveRoles: [] };
  if (heldColumns.kind !== 'none') {
    const inactive = [...all].filter(role => !active.has(role) && index.roles(host, user).some(r => r.role === role && !r.isDefault));
    return { ...base, state: 'set-role', inactiveRoles: names(inactive) };
  }
  return { ...base, state: base.cancelled.length ? 'revoked' : 'no-path', inactiveRoles: [] };
}

/** Compare semantics, never formatted text (names may contain commas or parentheses). */
export const outcomeKey = (access: HostAccess): string => JSON.stringify([access.state, access.columns, access.inactiveRoles]);

export function evaluateAccess(index: AccessIndex, user: string, table: string, operation: string): AccessEvaluation {
  const hosts = index.tableHosts(table).sort().map(host => hostAccess(index, host, user, table, operation));
  const holding = hosts.flatMap(h => h.holders.flatMap(x => x.grants.map(scope => ({ scope, cancelled: x.cancelled.includes(scope) }))));
  const cancelledScopes = [...new Set(holding.map(x => x.scope))].filter(scope => holding.filter(x => x.scope === scope).every(x => x.cancelled));
  const outcomes = new Set(hosts.map(outcomeKey));
  const state = outcomes.size > 1 ? 'differs' : hosts[0]?.state ?? 'no-path';
  const gaps = index.gaps;
  return { operation, user, target: table, userName: index.nameOf(user), tableName: index.nameOf(table), hosts, cancelledScopes,
    gaps: [...gaps], state: gaps.length && (state === 'no-path' || state === 'revoked') ? 'incomplete' : state };
}

export function evaluateReach(index: AccessIndex, user: string, operation: string): ReachEvaluation {
  const answers = index.tables.map(table => evaluateAccess(index, user, table, operation));
  const atLogin = (h: HostAccess) => h.state === 'granted' || h.state === 'columns';
  const reached = (a: AccessEvaluation, test: (h: HostAccess) => boolean) => a.state !== 'incomplete' && a.hosts.length > 0 && a.hosts.some(test);
  const everywhere = (a: AccessEvaluation, test: (h: HostAccess) => boolean) => reached(a, test) && a.hosts.every(test);
  const login = answers.filter(a => everywhere(a, atLogin)).length;
  const loginSome = answers.filter(a => reached(a, atLogin) && !everywhere(a, atLogin)).length;
  const later = answers.filter(a => !reached(a, atLogin) && reached(a, h => h.state === 'set-role'));
  const laterSome = later.filter(a => !a.hosts.every(h => h.state === 'set-role')).length;
  const needs = later.flatMap(a => a.hosts.filter(h => h.state === 'set-role').map(h => ({ host: h.host, roles: h.inactiveRoles })));
  return { operation, userName: index.nameOf(user), login, loginSome, later: later.length, laterSome, needs, gaps: [...index.gaps],
    tables: login || loginSome || later.length,
    state: login ? 'granted' : loginSome ? 'differs' : later.length ? 'set-role' : index.gaps.length ? 'incomplete' : 'no-path' };
}

/** Build a private immutable index once for a batch of evaluations. Public captures may be
 * mutable; no cache is keyed by their identity, so subsequent evaluations observe changes. */
export function accessEvaluator(capture: Capture) {
  const index = createAccessIndex(capture, evidenceGaps(capture));
  return { answer: (user: string, table: string, operation: string) => evaluateAccess(index, user, table, operation),
    reach: (user: string, operation: string) => evaluateReach(index, user, operation) };
}
