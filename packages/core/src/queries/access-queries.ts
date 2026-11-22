/** Object-browser metadata only. All variants/probes remain in the core SQL layer. */
import { escapeIdentifier } from './builder.js';
export const ACCESS_HOSTS = `
SELECT hostName() AS host, version() AS version
FROM {{cluster_aware:system.one}}
SETTINGS skip_unavailable_shards = 0`;

export const ACCESS_SCHEMA = `
SELECT hostName() AS host, table, name
FROM {{cluster_aware:system.columns}}
WHERE database = 'system'
  AND table IN ('users','roles','role_grants','grants','tables','row_policies')
SETTINGS skip_unavailable_shards = 0`;

export const ACCESS_CATALOGS = {
  users: {
    columns: ['name','id','storage','default_roles_all','default_roles_list','default_roles_except','host_ip','host_names','host_names_regexp','host_names_like'],
    sql: `SELECT hostName() AS host, name, toString(id) AS id, storage,
      default_roles_all, default_roles_list, default_roles_except,
      host_ip, host_names, host_names_regexp, host_names_like
      FROM {{cluster_aware:system.users}} WHERE ({browse} = 1 OR name = {principal})
      ORDER BY host, name LIMIT {limit} SETTINGS skip_unavailable_shards = 0`,
  },
  roles: {
    columns: ['name','id','storage'],
    sql: `SELECT hostName() AS host, name, toString(id) AS id, storage
      FROM {{cluster_aware:system.roles}} ORDER BY host, name
      LIMIT {limit} SETTINGS skip_unavailable_shards = 0`,
  },
  role_grants: {
    columns: ['user_name','role_name','granted_role_name','granted_role_id','granted_role_is_default','with_admin_option'],
    sql: `SELECT hostName() AS host, user_name, role_name, granted_role_name,
      toString(granted_role_id) AS granted_role_id, granted_role_is_default, with_admin_option
      FROM {{cluster_aware:system.role_grants}}
      WHERE {browse} = 1 OR user_name = {principal} OR role_name IS NOT NULL
      ORDER BY host, user_name, role_name, granted_role_name
      LIMIT {limit} SETTINGS skip_unavailable_shards = 0`,
  },
  grants: {
    columns: ['user_name','role_name','access_type','access_object','database','table','column','is_partial_revoke','grant_option','is_wildcard'],
    sql: `SELECT hostName() AS host, user_name, role_name, toString(access_type) AS access_type,
      access_object, database, table, column, is_partial_revoke, grant_option, is_wildcard
      FROM {{cluster_aware:system.grants}}
      WHERE {browse} = 1 OR user_name = {principal} OR role_name IS NOT NULL
      ORDER BY host, user_name, role_name, access_type, database, table, column
      LIMIT {limit} SETTINGS skip_unavailable_shards = 0`,
  },
  tables: {
    columns: ['database','name','uuid','engine','partition_key','sorting_key','primary_key','storage_policy'],
    sql: `SELECT hostName() AS host, database, name, toString(uuid) AS uuid, engine,
      partition_key, sorting_key, primary_key, storage_policy /* dependency_fields */
      FROM {{cluster_aware:system.tables}} WHERE ({browse} = 0 AND database = {database} AND name = {table})
        OR ({browse} = 1 AND database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA'))
      ORDER BY host LIMIT {limit} SETTINGS skip_unavailable_shards = 0`,
  },
  row_policies: {
    columns: ['name','short_name','id','storage','database','table','select_filter','is_restrictive','apply_to_all','apply_to_list','apply_to_except'],
    sql: `SELECT hostName() AS host, name, short_name, toString(id) AS id, storage,
      database, table, select_filter, is_restrictive, apply_to_all, apply_to_list, apply_to_except
      FROM {{cluster_aware:system.row_policies}}
      WHERE {browse} = 1 OR (database = {database} AND (table = {table} OR table = ''))
      ORDER BY host, name LIMIT {limit} SETTINGS skip_unavailable_shards = 0`,
  },
} as const;

/** Fixed optional projections, selected only after host-labelled schema probing. */
export const ACCESS_TABLE_DEPENDENCY_FIELDS = ['dependencies_database','dependencies_table','target_database','target_table','as_select','engine_full'] as const;
export function accessTableCatalogQuery(available: ReadonlySet<string>): string {
  const fields = ACCESS_TABLE_DEPENDENCY_FIELDS.filter(field => available.has(field));
  return ACCESS_CATALOGS.tables.sql.replace('/* dependency_fields */', fields.length ? `, ${fields.join(', ')}` : '');
}

/** Object detail statements, read from the connected server when a detail opens.
 * Names are native identifiers, backtick-quoted after escaping. */
const ident = (name: string) => `\`${escapeIdentifier(name)}\``;
export const OBJECT_TABLE_DDL = `
SELECT create_table_query AS statement
FROM system.tables WHERE database = {database} AND name = {table}`;
/** Secrets stay hidden even where the server allows displaying them. */
export const showCreateUser = (name: string) =>
  `SHOW CREATE USER ${ident(name)} SETTINGS format_display_secrets_in_show_and_select = 0`;
export const showCreateRole = (name: string) => `SHOW CREATE ROLE ${ident(name)}`;
export const showGrantsFor = (name: string) => `SHOW GRANTS FOR ${ident(name)}`;
/** An empty table name is a database-wide policy (`ON db.*`). */
export const showCreateRowPolicy = (name: string, database: string, table: string) =>
  `SHOW CREATE ROW POLICY ${ident(name)} ON ${ident(database)}.${table ? ident(table) : '*'}`;

/** References missing from the native fields. The input is the stored definition
 * from system.tables. EXPLAIN only parses or analyzes it, with a time limit.
 * Wrapping EXPLAIN in a SELECT returns ordinary rows on every adapter. */
const EXPLAIN_LIMITS = 'SETTINGS max_execution_time = 10';
/** Server parse of a stored view query (system.tables.as_select). Parsing reads no
 * table, source or dictionary. */
export const explainViewAst = (asSelect: string) =>
  `SELECT explain FROM (EXPLAIN AST ${asSelect}) ${EXPLAIN_LIMITS}`;
/** Analyzer-resolved tables of a stored view query (system.tables.as_select). Resolution
 * runs scalar subqueries and asks table functions for their columns, which can contact
 * their source; explainViewAst shows beforehand whether the query has either. */
export const explainViewSources = (asSelect: string) =>
  `SELECT explain FROM (EXPLAIN QUERY TREE ${asSelect}) ${EXPLAIN_LIMITS}`;
/** Tables and table functions the analyzer resolved, one per row, for the detail view. */
export const explainViewSourceList = (asSelect: string) => `
SELECT DISTINCT if(match(explain, '^\\\\s*TABLE id:'), extract(explain, 'table_name: (.+)$'),
  concat(extract(explain, 'table_function_name: (.+)$'), '()')) AS source
FROM (EXPLAIN QUERY TREE ${asSelect})
WHERE match(explain, '^\\\\s*TABLE(_FUNCTION)? id:') ${EXPLAIN_LIMITS}`;
/** Server parse of an engine call isolated from system.tables.engine_full. */
export const explainEngineCall = (engineCall: string) =>
  `SELECT explain FROM (EXPLAIN AST SELECT ${engineCall}) ${EXPLAIN_LIMITS}`;

/** The server's privilege tree: each privilege's parent group and the level it applies to.
 * Read from the connected host; it depends on the server version. */
export const ACCESS_PRIVILEGES = `
SELECT toString(privilege) AS privilege, toString(level) AS level, toString(parent_group) AS parent_group
FROM system.privileges`;

/** Iceberg snapshots of one table. ClickHouse reads the table's Iceberg metadata from
 * storage to answer, so this runs only when requested in the detail view. */
export const ICEBERG_SNAPSHOTS = `
SELECT concat(toString(made_current_at), '  ', toString(operation), '  snapshot ', toString(snapshot_id),
  '  records ', summary['total-records'], '  files ', summary['total-data-files'], '  bytes ', summary['total-files-size']) AS snapshot
FROM system.iceberg_history WHERE database = {database} AND table = {table}
ORDER BY made_current_at DESC LIMIT 20`;
