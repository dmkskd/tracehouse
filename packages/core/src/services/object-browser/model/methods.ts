/** How a relationship was obtained, from most to least server-backed. Recorded in
 * `evidence.method` and shown with the link. */
export type ReferenceMethod = 'native' | 'analyzer' | 'engine-arguments' | 'engine-location' | 'storage-policy';
export const REFERENCE_METHODS: Record<ReferenceMethod, string> = {
  native: 'system.tables: dependencies_table, target_table',
  analyzer: 'EXPLAIN QUERY TREE of system.tables.as_select',
  'engine-arguments': 'EXPLAIN AST of the engine call in system.tables.engine_full',
  'engine-location': 'EXPLAIN AST of the engine call in system.tables.engine_full; server from the location argument; no storage request',
  'storage-policy': 'system.tables.storage_policy, system.storage_policies and system.disks; TTL moves from the TTL clause of system.tables.engine_full; no storage request',
};
