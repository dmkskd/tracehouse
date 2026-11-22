/** `location` is the storage server a table definition points to (an S3 endpoint, an HDFS namenode, a local disk). */
export type ObjectKind = 'user' | 'role' | 'table' | 'grant-scope' | 'row-policy' | 'location';
export type Source = 'users' | 'roles' | 'role_grants' | 'grants' | 'tables' | 'row_policies';
export type Attributes = Record<string, unknown>;
export interface Observation {
  key: string;
  kind: ObjectKind;
  name: string;
  host: string;
  source: Source;
  attributes: Attributes;
}
export interface Relationship {
  id: string;
  host: string;
  from: string;
  to: string;
  kind: 'has-role' | 'grant' | 'candidate-scope' | 'policy-target' | 'policy-recipient' | 'insert-trigger' | 'writes-to' | 'reads-from' | 'points-to';
  label: string;
  source: Source;
  evidence: Attributes;
}
export interface Coverage {
  source: Source;
  state: 'readable' | 'schema-unavailable' | 'failed' | 'truncated';
  hosts: string[];
  reason?: string;
  /** Reading rows does not prove the observer sees every catalog entry. */
  visibility: 'unknown';
}
export interface Investigation {
  connection: string;
  principal: string;
  database: string;
  table: string;
  operation: 'SELECT' | 'INSERT';
}
export interface Capture {
  schemaVersion: 1;
  mode?: 'catalog' | 'investigation';
  investigation: Investigation;
  startedAt: string;
  completedAt: string;
  hosts: { host: string; version: string }[];
  observations: Observation[];
  relationships: Relationship[];
  coverage: Coverage[];
  verdict: { kind: 'not-checked'; reason: string };
}
export interface GraphNode {
  key: string;
  kind: ObjectKind;
  name: string;
  records: Observation[];
  observedHosts: string[];
  expectedHosts: string[];
  differs: boolean;
  partial: boolean;
}
export interface GraphEdge {
  key: string;
  from: string;
  to: string;
  kind: Relationship['kind'];
  label: string;
  hosts: string[];
  records: Relationship[];
}
export interface ObjectGraph {
  nodes: GraphNode[];
  edges: GraphEdge[];
  /** Total matching tables for scopes whose links were capped. */
  scopeMatches?: Record<string, number>;
}
/** Tuple serialization avoids delimiter collisions in quoted ClickHouse names. */
export const objectKey = (connection: string, kind: ObjectKind, ...name: string[]): string =>
  JSON.stringify([connection, kind, ...name]);
