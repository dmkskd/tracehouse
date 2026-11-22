import { TimeTravelLoading } from '../timeline/TimeTravelLoading';

/** What a capture reads, in collection order (collectObjects). */
const reads = [
  { label: 'Hosts and schema', detail: 'one, columns' },
  { label: 'Users and roles', detail: 'users, roles, role_grants' },
  { label: 'Grants and row policies', detail: 'grants, privileges, row_policies' },
  { label: 'Tables', detail: 'tables' },
  { label: 'Table links', detail: 'EXPLAIN of view queries and engine calls' },
];

/** Shared Time Travel loading presentation; shown when a capture takes longer than a second. */
export function ObjectsLoading() {
  return <TimeTravelLoading title="Loading objects"
    description="Reading access and table metadata from ClickHouse system tables." reads={reads} />;
}
