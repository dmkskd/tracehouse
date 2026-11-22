import { ACCESS_CATALOGS } from '../../../queries/access-queries.js';
import type { Source } from '../model/types.js';
/** Four-host catalog fixture. No server verdict is encoded. */
export function accessFixture() {
  const hosts = ['ch-1','ch-2','ch-3','ch-4'];
  const rows: Record<Source, Record<string, unknown>[]> = {
    users: [], roles: [], role_grants: [], grants: [], tables: [], row_policies: [],
  };
  for (const host of hosts) {
    rows.users.push({ host, name: 'alice', id: 'user-id', storage: 'replicated', default_roles_all: 0,
      default_roles_list: ['reader'], default_roles_except: [], host_ip: ['::/0'], host_names: [], host_names_regexp: [], host_names_like: [] });
    rows.roles.push({ host, name: 'reader', id: 'role-id', storage: 'replicated' });
    rows.role_grants.push({ host, user_name: 'alice', role_name: null, granted_role_name: 'reader',
      granted_role_id: 'role-id', granted_role_is_default: 1, with_admin_option: 0 });
    rows.grants.push({ host, user_name: null, role_name: 'reader', access_type: 'SELECT', access_object: '',
      database: 'demo', table: null, column: null, is_partial_revoke: 0, grant_option: 0, is_wildcard: 0 });
    rows.tables.push({ host, database: 'demo', name: 'trips', uuid: 'table-id', engine: 'MergeTree',
      partition_key: '', sorting_key: 'id', primary_key: 'id', storage_policy: 'default' });
    rows.row_policies.push({ host, name: 'tenant ON demo.trips', short_name: 'tenant', id: 'policy-id', storage: 'replicated',
      database: 'demo', table: 'trips', select_filter: 'tenant_id = 42', is_restrictive: 0, apply_to_all: 0,
      apply_to_list: ['reader'], apply_to_except: [] });
  }
  const schema: { host: string; table: string; name: string }[] = hosts.flatMap(host => Object.entries(ACCESS_CATALOGS).flatMap(([table, spec]) => spec.columns.map(name => ({ host, table, name }))));
  return { hosts, rows, schema };
}
