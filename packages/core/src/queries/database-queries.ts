/**
 * SQL query templates for database exploration.
 *
 * All metadata queries use GROUP BY on the natural key with any() for
 * non-key columns. This deduplicates rows when clusterAllReplicas returns
 * identical data from multiple replicas within a shard.
 */

/** List all databases with table counts. Natural key: (database name). */
export const LIST_DATABASES = `
  SELECT
    db.name,
    db.engine,
    db.table_count,
    COALESCE(p.total_bytes, 0) AS total_bytes
  FROM (
    SELECT
      d.name,
      any(d.engine) AS engine,
      COUNT(DISTINCT t.name) AS table_count
    FROM {{cluster_metadata:system.databases}} AS d
    LEFT JOIN {{cluster_metadata:system.tables}} AS t ON d.name = t.database
    GROUP BY d.name
  ) AS db
  LEFT JOIN (
    SELECT database, sum(part_bytes) AS total_bytes
    FROM (
      SELECT database, table, name, any(bytes_on_disk) AS part_bytes
      FROM {{cluster_metadata:system.parts}}
      WHERE active = 1
      GROUP BY database, table, name
    )
    GROUP BY database
  ) AS p ON db.name = p.database
  ORDER BY db.name
`;

/** List all tables in a database. Natural key: (database, name). */
export const LIST_TABLES = `
  SELECT
    database,
    name,
    any(engine) AS engine,
    any(total_rows) AS total_rows,
    any(total_bytes) AS total_bytes,
    any(partition_key) AS partition_key,
    any(sorting_key) AS sorting_key
  FROM {{cluster_metadata:system.tables}}
  WHERE database = {database}
  GROUP BY database, name
  ORDER BY name
`;

/** Get column schema for a table. Natural key: (database, table, name). */
export const GET_TABLE_SCHEMA = `
  SELECT
    name,
    any(type) AS type,
    any(default_kind) AS default_kind,
    any(default_expression) AS default_expression,
    any(comment) AS comment,
    any(is_in_partition_key) AS is_in_partition_key,
    any(is_in_sorting_key) AS is_in_sorting_key,
    any(is_in_primary_key) AS is_in_primary_key,
    any(is_in_sampling_key) AS is_in_sampling_key,
    any(position) AS position
  FROM {{cluster_metadata:system.columns}}
  WHERE database = {database}
    AND table = {table}
  GROUP BY name
  ORDER BY position
`;

/** Get active parts for a table. Natural key: (name) within (database, table). */
export const GET_TABLE_PARTS = `
  SELECT
    any(partition_id) AS partition_id,
    name,
    any(rows) AS rows,
    any(bytes_on_disk) AS bytes_on_disk,
    any(modification_time) AS modification_time,
    any(level) AS level,
    any(primary_key_bytes_in_memory) AS primary_key_bytes_in_memory,
    any(marks_bytes) AS marks_bytes,
    any(data_compressed_bytes) AS data_compressed_bytes,
    any(data_uncompressed_bytes) AS data_uncompressed_bytes
  FROM {{cluster_metadata:system.parts}}
  WHERE database = {database}
    AND table = {table}
    AND active = 1
  GROUP BY name
  ORDER BY modification_time DESC
`;

/** Get detailed info for a specific part. Natural key: (name). */
export const GET_PART_DETAIL = `
  SELECT
    any(partition_id) AS partition_id,
    name,
    any(rows) AS rows,
    any(bytes_on_disk) AS bytes_on_disk,
    any(modification_time) AS modification_time,
    any(level) AS level,
    any(min_block_number) AS min_block_number,
    any(max_block_number) AS max_block_number,
    any(marks) AS marks,
    any(marks_bytes) AS marks_bytes,
    any(data_compressed_bytes) AS data_compressed_bytes,
    any(data_uncompressed_bytes) AS data_uncompressed_bytes,
    any(primary_key_bytes_in_memory) AS primary_key_bytes_in_memory,
    any(min_date) AS min_date,
    any(max_date) AS max_date,
    any(disk_name) AS disk_name,
    any(path) AS path,
    any(part_type) AS part_type
  FROM {{cluster_metadata:system.parts}}
  WHERE database = {database}
    AND table = {table}
    AND name = {part_name}
    AND active = 1
  GROUP BY name
`;

/** Get column sizes for a part with compression codec. Natural key: (column, type). */
export const GET_PART_COLUMNS = `
  SELECT
    pc.column,
    pc.type,
    any(pc.column_data_compressed_bytes) AS compressed,
    any(pc.column_data_uncompressed_bytes) AS uncompressed,
    any(c.compression_codec) AS codec
  FROM {{cluster_metadata:system.parts_columns}} AS pc
  LEFT JOIN {{cluster_metadata:system.columns}} AS c
    ON c.database = pc.database AND c.table = pc.table AND c.name = pc.column
  WHERE pc.database = {database}
    AND pc.table = {table}
    AND pc.name = {part_name}
    AND pc.active = 1
  GROUP BY pc.column, pc.type
  ORDER BY compressed DESC
`;

/** Get table key columns. Natural key: (database, name). */
export const GET_TABLE_KEYS = `
  SELECT
    any(partition_key) AS partition_key,
    any(sorting_key) AS sorting_key,
    any(primary_key) AS primary_key
  FROM {{cluster_metadata:system.tables}}
  WHERE database = {database}
    AND name = {table}
  GROUP BY database, name
`;

/** Get row count for a specific part. */
export const GET_PART_ROW_COUNT = `
  SELECT count() AS cnt
  FROM {database_table}
  WHERE _part = {part_name}
`;

/** Get column names for a table. Natural key: (name) within (database, table). */
export const GET_TABLE_COLUMN_NAMES = `
  SELECT name
  FROM {{cluster_metadata:system.columns}}
  WHERE database = {database}
    AND table = {table}
  GROUP BY name, position
  ORDER BY position
`;

/** Get sample data from a specific part. */
export const GET_PART_DATA = `
  SELECT *
  FROM {database_table}
  WHERE _part = {part_name}
  LIMIT {limit}
`;
