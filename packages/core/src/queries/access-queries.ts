/** Object-browser metadata only. All variants/probes remain in the core SQL layer. */
import { escapeIdentifier } from './builder.js';
/** The node this connection's statements run on, such as EXPLAIN, which cannot go through clusterAllReplicas. */
export const ACCESS_CONNECTED_HOST = `SELECT hostName() AS host`;

export const ACCESS_HOSTS = `
SELECT hostName() AS host, version() AS version
FROM {{cluster_aware:system.one}}
SETTINGS skip_unavailable_shards = 0`;

export const ACCESS_SCHEMA = `
SELECT hostName() AS host, table, name
FROM {{cluster_aware:system.columns}}
WHERE database = 'system'
  AND table IN ('users','roles','role_grants','grants','tables','row_policies','blob_storage_log')
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
      WHERE ({browse} = 0 AND (user_name = {principal} OR role_name IS NOT NULL))
        -- A catalog lists global grants, and grants on the databases its tables come from:
        -- the system database with {system} = 1, every other database without.
        OR ({browse} = 1 AND (database IS NULL
          OR ({system} = 1) = (database IN ('system', 'information_schema', 'INFORMATION_SCHEMA'))))
      ORDER BY host, user_name, role_name, access_type, database, table, column
      LIMIT {limit} SETTINGS skip_unavailable_shards = 0`,
  },
  tables: {
    columns: ['database','name','uuid','engine','partition_key','sorting_key','primary_key','storage_policy'],
    sql: `SELECT hostName() AS host, database, name, toString(uuid) AS uuid, engine,
      partition_key, sorting_key, primary_key, storage_policy /* optional_fields */
      FROM {{cluster_aware:system.tables}} WHERE ({browse} = 0 AND database = {database} AND name = {table})
        OR ({browse} = 1 AND {system} = 1 AND database = 'system')
        OR ({browse} = 1 AND {system} = 0 AND database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA'))
      ORDER BY host LIMIT {limit} SETTINGS skip_unavailable_shards = 0`,
  },
  row_policies: {
    columns: ['name','short_name','id','storage','database','table','select_filter','is_restrictive','apply_to_all','apply_to_list','apply_to_except'],
    sql: `SELECT hostName() AS host, name, short_name, toString(id) AS id, storage,
      database, table, select_filter, is_restrictive, apply_to_all, apply_to_list, apply_to_except
      FROM {{cluster_aware:system.row_policies}}
      WHERE ({browse} = 1 AND ({system} = 0 OR database = 'system')) OR ({browse} = 0 AND database = {database} AND (table = {table} OR table = ''))
      ORDER BY host, name LIMIT {limit} SETTINGS skip_unavailable_shards = 0`,
  },
} as const;

/** Fixed optional projections, selected only after host-labelled schema probing. */
export const ACCESS_TABLE_DEPENDENCY_FIELDS = ['dependencies_database','dependencies_table','target_database','target_table','as_select','engine_full'] as const;
/** Size and comment for the object hover. Size is null for engines that store no data. */
export const ACCESS_TABLE_SUMMARY_FIELDS = ['total_rows','total_bytes','comment'] as const;
export function accessTableCatalogQuery(available: ReadonlySet<string>): string {
  const fields = [...ACCESS_TABLE_DEPENDENCY_FIELDS, ...ACCESS_TABLE_SUMMARY_FIELDS].filter(field => available.has(field));
  return ACCESS_CATALOGS.tables.sql.replace('/* optional_fields */', fields.length ? `, ${fields.join(', ')}` : '');
}

/** Active parts per table and host, and when the newest one was written (Unix seconds). */
export const ACTIVE_PARTS_BY_TABLE = `
SELECT hostName() AS host, database, table, count() AS active_parts,
  toUnixTimestamp(max(modification_time)) AS newest_part
FROM {{cluster_aware:system.parts}}
WHERE active AND (({browse} = 0 AND database = {database} AND table = {table})
  OR ({browse} = 1 AND {system} = 1 AND database = 'system')
  OR ({browse} = 1 AND {system} = 0 AND database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')))
GROUP BY host, database, table
SETTINGS skip_unavailable_shards = 0`;

/** Volumes of each storage policy, in the order parts fill them. */
export const ACCESS_STORAGE_POLICIES = `
SELECT hostName() AS host, policy_name, volume_name, volume_priority, disks
FROM {{cluster_aware:system.storage_policies}}
ORDER BY host, policy_name, volume_priority
SETTINGS skip_unavailable_shards = 0`;

/** Disks as the server configured them. system.disks holds no endpoint or bucket. A cache disk
 * has a cache_path and shares `path` with the disk it caches. */
export const ACCESS_DISKS = `
SELECT hostName() AS host, name, type, object_storage_type, is_remote, path, cache_path
FROM {{cluster_aware:system.disks}}
ORDER BY host, name
SETTINGS skip_unavailable_shards = 0`;

/** The bucket each disk wrote to or read from since yesterday, from system.blob_storage_log.
 * The server has that log only when its config enables it, so the collector runs this when
 * ACCESS_SCHEMA finds the table on every host. No storage request. */
export const ACCESS_DISK_BUCKETS = `
SELECT hostName() AS host, disk_name, groupUniqArray(3)(bucket) AS buckets
FROM {{cluster_aware:system.blob_storage_log}}
WHERE event_date >= yesterday() AND disk_name != '' AND bucket != ''
GROUP BY host, disk_name
SETTINGS skip_unavailable_shards = 0`;

/** System tables the collected views read from. The catalog skips the system databases,
 * so these rows are read by name: {tables} is a JSON array of [database, name] pairs. */
const SYSTEM_TABLES_BY_NAME = `
SELECT hostName() AS host, database, name, toString(uuid) AS uuid, engine,
  partition_key, sorting_key, primary_key, storage_policy /* optional_fields */
FROM {{cluster_aware:system.tables}}
WHERE database IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
  AND has(JSONExtract({tables}, 'Array(Tuple(String, String))'), (database, name))
ORDER BY host SETTINGS skip_unavailable_shards = 0`;
export function systemTablesByNameQuery(available: ReadonlySet<string>): string {
  const fields = ACCESS_TABLE_SUMMARY_FIELDS.filter(field => available.has(field));
  return SYSTEM_TABLES_BY_NAME.replace('/* optional_fields */', fields.length ? `, ${fields.join(', ')}` : '');
}

/** Where a table's active parts are on the connected host, per disk: count, rows and bytes, how
 * many have passed their earliest move TTL, and the next move TTL still ahead (Unix seconds, 0 when
 * none). Reads part metadata only. */
export const OBJECT_TABLE_PLACEMENT = `
SELECT p.disk_name AS disk, any(d.type) AS type, any(d.object_storage_type) AS object_storage_type,
  count() AS parts, sum(p.rows) AS rows, sum(p.bytes_on_disk) AS bytes,
  countIf(notEmpty(p.move_ttl_info.min) AND arrayMin(p.move_ttl_info.min) <= now()) AS past_move_ttl,
  toUnixTimestamp(minIf(arrayMin(p.move_ttl_info.min), notEmpty(p.move_ttl_info.min) AND arrayMin(p.move_ttl_info.min) > now())) AS next_move,
  toUnixTimestamp(now()) AS server_now
FROM system.parts AS p LEFT JOIN system.disks AS d ON d.name = p.disk_name
WHERE p.active AND p.database = {database} AND p.table = {table}
GROUP BY p.disk_name ORDER BY p.disk_name`;

/** Object detail statements, read from the connected server when a detail opens.
 * Names are native identifiers, backtick-quoted after escaping. */
const ident = (name: string) => `\`${escapeIdentifier(name)}\``;
export const OBJECT_TABLE_DDL = `
SELECT create_table_query AS statement
FROM system.tables WHERE database = {database} AND name = {table}`;
/** The definition laid out by ClickHouse's own formatter (formatQueryOrNull, 23.11+); the stored
 * single-line text when the formatter cannot parse it. Older servers use OBJECT_TABLE_DDL. */
export const OBJECT_TABLE_DDL_FORMATTED = `
SELECT coalesce(formatQueryOrNull(create_table_query), create_table_query) AS statement
FROM system.tables WHERE database = {database} AND name = {table}`;
/** Queries that touched a table (`{table}` = `database.table`, as query_log.tables holds it) in the
 * last 24 hours, per kind. A query counts once by initial_query_id, so a query through a
 * Distributed table that fans out to the shards' local tables counts once, under its initial user.
 * `top_users` holds up to 5 (user, queries) pairs, most queries first; `other_kinds` the query
 * kinds counted in `other`; `last_query_id` the initial_query_id of the latest query. */
export const OBJECT_TABLE_ACTIVITY = `
SELECT
  sum(selects) AS selects, sum(inserts) AS inserts, sum(other) AS other, sum(failed) AS failed,
  count() AS users,
  arraySlice(arrayReverseSort(pair -> pair.2, groupArray((user, total))), 1, 5) AS top_users,
  arraySort(arrayDistinct(arrayFlatten(groupArray(kinds)))) AS other_kinds,
  toString(toDateTime(max(latest))) AS last_query,
  argMax(latest_id, latest) AS last_query_id,
  toUnixTimestamp64Milli(max(latest)) AS last_query_ms,
  dateDiff('second', toDateTime(max(latest)), now()) AS seconds_ago
FROM (
  SELECT initial_user AS user,
    uniqExactIf(initial_query_id, query_kind = 'Select') AS selects,
    uniqExactIf(initial_query_id, query_kind = 'Insert') AS inserts,
    uniqExactIf(initial_query_id, query_kind NOT IN ('Select', 'Insert')) AS other,
    uniqExactIf(initial_query_id, type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing')) AS failed,
    groupUniqArrayIf(toString(query_kind), query_kind NOT IN ('Select', 'Insert')) AS kinds,
    uniqExact(initial_query_id) AS total,
    max(event_time_microseconds) AS latest,
    argMax(initial_query_id, event_time_microseconds) AS latest_id
  FROM {{cluster_aware:system.query_log}}
  WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 1 DAY
    AND type != 'QueryStart' AND has(tables, {table})
  GROUP BY initial_user
)
HAVING users > 0`;
/** Secrets stay hidden even where the server allows displaying them. */
export const showCreateUser = (name: string) =>
  `SHOW CREATE USER ${ident(name)} SETTINGS format_display_secrets_in_show_and_select = 0`;
export const showCreateRole = (name: string) => `SHOW CREATE ROLE ${ident(name)}`;
export const showGrantsFor = (name: string) => `SHOW GRANTS FOR ${ident(name)}`;
/** An empty table name is a database-wide policy (`ON db.*`). */
export const showCreateRowPolicy = (name: string, database: string, table: string) =>
  `SHOW CREATE ROW POLICY ${ident(name)} ON ${ident(database)}.${table ? ident(table) : '*'}`;

/** References missing from the native fields. The input is the stored definition
 * from system.tables. Each EXPLAIN is a statement of its own, run with the adapter's
 * executeRawQuery: inside `SELECT … FROM (EXPLAIN …)` ClickHouse reads it through the
 * viewExplain table function, which requires CREATE TEMPORARY TABLE. */
/** Server parse of a stored view query (system.tables.as_select). Parsing reads no
 * table, source or dictionary. */
export const explainViewAst = (asSelect: string) => `EXPLAIN AST ${asSelect}`;
/** Analyzer-resolved tables of a stored view query (system.tables.as_select). Resolution
 * runs scalar subqueries and asks table functions for their columns, which can contact
 * their source; explainViewAst shows beforehand whether the query has either. The query
 * is parenthesized so the time limit applies to EXPLAIN. */
export const explainViewSources = (asSelect: string) => `EXPLAIN QUERY TREE (${asSelect}) SETTINGS max_execution_time = 10`;
/** Server parse of an engine call isolated from system.tables.engine_full. */
export const explainEngineCall = (engineCall: string) => `EXPLAIN AST SELECT ${engineCall}`;

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
