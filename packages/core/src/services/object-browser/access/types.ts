/** `columns`: granted on some columns only, or on all but some. `differs`: hosts disagree.
 * `incomplete`: no grant found, but the collection that would hold one was not fully read. */
export type ProvenanceState = 'granted' | 'columns' | 'set-role' | 'revoked' | 'no-path' | 'incomplete' | 'differs';

/** One host's evaluation: the state, the columns granted, the holders that grant them, roles that
 * need SET ROLE, and per holder the grants it holds for the table and those its own revoke takes back. */
export interface HostAccess {
  host: string;
  state: Exclude<ProvenanceState, 'incomplete' | 'differs'>;
  columns: Columns;
  grantedThrough: string[];
  inactiveRoles: string[];
  holders: { holder: string; grants: string[]; cancelled: string[] }[];
  cancelled: { holder: string; scope: string }[];
}

/** Columns of a table a grant covers: all, all but some, only some, or none. */
export type Columns = { kind: 'all' } | { kind: 'except'; columns: string[] } | { kind: 'only'; columns: string[] } | { kind: 'none' };

/** A semantic answer, before any text or diagram formatting. */
export interface AccessEvaluation {
  operation: string;
  state: ProvenanceState;
  hosts: HostAccess[];
  gaps: string[];
  cancelledScopes: string[];
  user: string;
  target: string;
  userName: string;
  tableName: string;
}
export interface ReachEvaluation {
  operation: string;
  state: ProvenanceState;
  tables: number;
  userName: string;
  login: number;
  loginSome: number;
  later: number;
  laterSome: number;
  needs: { host: string; roles: readonly string[] }[];
  gaps: string[];
}
