/**
 * SQL query templates for merge tracking.
 *
 * Extracted from backend/services/database_explorer.py (merge-related
 * queries) and grafana-app-plugin/src/datasource.ts. All placeholders use
 * {param} syntax compatible with buildQuery().
 */

/** Get currently active merges from system.merges. */
export const GET_ACTIVE_MERGES = `
  SELECT
    database,
    table,
    elapsed,
    progress,
    num_parts,
    source_part_names,
    result_part_name,
    total_size_bytes_compressed,
    rows_read,
    rows_written,
    memory_usage,
    merge_type,
    merge_algorithm,
    is_mutation,
    bytes_read_uncompressed,
    bytes_written_uncompressed,
    columns_written,
    thread_id,
    hostName() AS hostname
  FROM {{cluster_aware:system.merges}}
  ORDER BY elapsed DESC
`;

/** Get merge history for a specific database and table from system.part_log. */
export const GET_MERGE_HISTORY = `
  SELECT
    event_time,
    event_type,
    database,
    table,
    part_name,
    partition_id,
    rows,
    size_in_bytes,
    duration_ms,
    merge_reason,
    merged_from,
    bytes_uncompressed,
    read_bytes,
    read_rows,
    peak_memory_usage,
    merge_algorithm,
    disk_name,
    path_on_disk,
    query_id,
    ProfileEvents,
    error,
    exception,
    hostName() AS hostname
  FROM {{cluster_aware:system.part_log}}
  WHERE database = {database}
    AND table = {table}
    AND event_type IN ('MergeParts', 'MutatePart', 'MovePart')
  ORDER BY event_time DESC
  LIMIT {limit}
`;

/** Get all merge history across all databases from system.part_log. */
export const GET_ALL_MERGE_HISTORY = `
  SELECT
    event_time,
    event_type,
    database,
    table,
    part_name,
    partition_id,
    rows,
    size_in_bytes,
    duration_ms,
    merge_reason,
    merged_from,
    bytes_uncompressed,
    read_bytes,
    read_rows,
    peak_memory_usage,
    merge_algorithm,
    disk_name,
    path_on_disk,
    query_id,
    ProfileEvents,
    error,
    exception,
    hostName() AS hostname
  FROM {{cluster_aware:system.part_log}}
  WHERE event_type IN ('MergeParts', 'MutatePart', 'MovePart')
  ORDER BY event_time DESC
  LIMIT {limit}
`;

/** Get merge history for a database (all tables) from system.part_log. */
export const GET_DATABASE_MERGE_HISTORY = `
  SELECT
    event_time,
    event_type,
    database,
    table,
    part_name,
    partition_id,
    rows,
    size_in_bytes,
    duration_ms,
    merge_reason,
    merged_from,
    bytes_uncompressed,
    read_bytes,
    read_rows,
    peak_memory_usage,
    merge_algorithm,
    disk_name,
    path_on_disk,
    query_id,
    ProfileEvents,
    error,
    exception,
    hostName() AS hostname
  FROM {{cluster_aware:system.part_log}}
  WHERE database = {database}
    AND event_type IN ('MergeParts', 'MutatePart', 'MovePart')
  ORDER BY event_time DESC
  LIMIT {limit}
`;

/** Get mutations from system.mutations with full details.
 * Natural key: (database, table, mutation_id). Dedup across replicas/shards.
 * Uses HAVING instead of WHERE so that a mutation only appears as active
 * when ALL shards still have work to do (min(is_done) = 0).
 * Note: parts_to_do_names is an Array column that doesn't resolve through
 * clusterAllReplicas subqueries, so we flatten via toString(). */
export const GET_MUTATIONS = `
  SELECT
    database,
    table,
    mutation_id,
    any(command) AS command,
    any(create_time) AS create_time,
    any(partition_ids) AS partition_ids,
    any(block_numbers) AS block_numbers,
    sum(sub_parts_to_do) AS parts_to_do,
    0 AS parts_in_progress,
    anyIf(parts_to_do_names_str, raw_is_done = 0) AS parts_to_do_names,
    '[]' AS parts_in_progress_names,
    min(raw_is_done) AS is_done,
    any(latest_failed_part) AS latest_failed_part,
    any(latest_fail_time) AS latest_fail_time,
    any(latest_fail_reason) AS latest_fail_reason,
    max(raw_is_killed) AS is_killed
  FROM (
    SELECT
      database,
      table,
      mutation_id,
      command,
      create_time,
      block_numbers.partition_id AS partition_ids,
      block_numbers.number AS block_numbers,
      parts_to_do AS sub_parts_to_do,
      toString(parts_to_do_names) AS parts_to_do_names_str,
      is_done AS raw_is_done,
      latest_failed_part,
      latest_fail_time,
      latest_fail_reason,
      is_killed AS raw_is_killed
    FROM {{cluster_metadata:system.mutations}}
  )
  GROUP BY database, table, mutation_id
  HAVING min(raw_is_done) = 0 AND max(raw_is_killed) = 0
  ORDER BY
    parts_to_do DESC,
    create_time DESC
`;

/** Get background thread pool metrics from system.metrics (cluster-aggregated). */
export const GET_BACKGROUND_POOL_METRICS = `
  SELECT
    sumIf(value, metric = 'BackgroundMergesAndMutationsPoolSize') AS merge_pool_size,
    sumIf(value, metric = 'BackgroundMergesAndMutationsPoolTask') AS merge_pool_active,
    sumIf(value, metric = 'BackgroundMovePoolSize') AS move_pool_size,
    sumIf(value, metric = 'BackgroundMovePoolTask') AS move_pool_active,
    sumIf(value, metric = 'BackgroundFetchesPoolSize') AS fetch_pool_size,
    sumIf(value, metric = 'BackgroundFetchesPoolTask') AS fetch_pool_active,
    sumIf(value, metric = 'BackgroundSchedulePoolSize') AS schedule_pool_size,
    sumIf(value, metric = 'BackgroundSchedulePoolTask') AS schedule_pool_active,
    sumIf(value, metric = 'BackgroundCommonPoolSize') AS common_pool_size,
    sumIf(value, metric = 'BackgroundCommonPoolTask') AS common_pool_active,
    sumIf(value, metric = 'BackgroundDistributedSchedulePoolSize') AS distributed_pool_size,
    sumIf(value, metric = 'BackgroundDistributedSchedulePoolTask') AS distributed_pool_active,
    sumIf(value, metric = 'Merge') AS active_merges,
    sumIf(value, metric = 'PartMutation') AS active_mutations,
    sumIf(value, metric = 'PartsActive') AS active_parts,
    sumIf(value, metric = 'PartsOutdated') AS outdated_parts
  FROM {{cluster_aware:system.metrics}}
  WHERE metric IN (
    'BackgroundMergesAndMutationsPoolSize', 'BackgroundMergesAndMutationsPoolTask',
    'BackgroundMovePoolSize', 'BackgroundMovePoolTask',
    'BackgroundFetchesPoolSize', 'BackgroundFetchesPoolTask',
    'BackgroundSchedulePoolSize', 'BackgroundSchedulePoolTask',
    'BackgroundCommonPoolSize', 'BackgroundCommonPoolTask',
    'BackgroundDistributedSchedulePoolSize', 'BackgroundDistributedSchedulePoolTask',
    'Merge', 'PartMutation', 'PartsActive', 'PartsOutdated'
  )
`;


/** Get total size of outdated parts that can be reclaimed.
 * Natural key: (database, table, name). Dedup via subquery, then aggregate. */
export const GET_OUTDATED_PARTS_SIZE = `
  SELECT
    count() AS outdated_parts_count,
    sum(part_bytes) AS outdated_parts_bytes
  FROM (
    SELECT name, any(bytes_on_disk) AS part_bytes
    FROM {{cluster_metadata:system.parts}}
    WHERE active = 0
    GROUP BY database, table, name
  )
`;


/** Get mutation history from system.mutations (completed mutations).
 * Natural key: (database, table, mutation_id).
 * Uses HAVING min(is_done) = 1 so mutations only appear as completed
 * when ALL shards have finished (not just one shard). */
export const GET_MUTATION_HISTORY = `
  SELECT
    database,
    table,
    mutation_id,
    any(command) AS command,
    any(create_time) AS create_time,
    min(raw_is_done) AS is_done,
    max(raw_is_killed) AS is_killed,
    any(latest_failed_part) AS latest_failed_part,
    any(latest_fail_time) AS latest_fail_time,
    any(latest_fail_reason) AS latest_fail_reason
  FROM (
    SELECT *, is_done AS raw_is_done, is_killed AS raw_is_killed
    FROM {{cluster_metadata:system.mutations}}
  )
  GROUP BY database, table, mutation_id
  HAVING min(raw_is_done) = 1 OR max(raw_is_killed) = 1
  ORDER BY create_time DESC
  LIMIT {limit}
`;

/** Get mutation history for a specific database.
 * Natural key: (database, table, mutation_id). */
export const GET_DATABASE_MUTATION_HISTORY = `
  SELECT
    database,
    table,
    mutation_id,
    any(command) AS command,
    any(create_time) AS create_time,
    min(raw_is_done) AS is_done,
    max(raw_is_killed) AS is_killed,
    any(latest_failed_part) AS latest_failed_part,
    any(latest_fail_time) AS latest_fail_time,
    any(latest_fail_reason) AS latest_fail_reason
  FROM (
    SELECT *, is_done AS raw_is_done, is_killed AS raw_is_killed
    FROM {{cluster_metadata:system.mutations}}
    WHERE database = {database}
  )
  GROUP BY database, table, mutation_id
  HAVING min(raw_is_done) = 1 OR max(raw_is_killed) = 1
  ORDER BY create_time DESC
  LIMIT {limit}
`;

/** Get mutation history for a specific table.
 * Natural key: (database, table, mutation_id). */
export const GET_TABLE_MUTATION_HISTORY = `
  SELECT
    database,
    table,
    mutation_id,
    any(command) AS command,
    any(create_time) AS create_time,
    min(raw_is_done) AS is_done,
    max(raw_is_killed) AS is_killed,
    any(latest_failed_part) AS latest_failed_part,
    any(latest_fail_time) AS latest_fail_time,
    any(latest_fail_reason) AS latest_fail_reason
  FROM (
    SELECT *, is_done AS raw_is_done, is_killed AS raw_is_killed
    FROM {{cluster_metadata:system.mutations}}
    WHERE database = {database}
      AND table = {table}
  )
  GROUP BY database, table, mutation_id
  HAVING min(raw_is_done) = 1 OR max(raw_is_killed) = 1
  ORDER BY create_time DESC
  LIMIT {limit}
`;


/** Get storage policy volume mapping: disk → volume → policy.
 * Used to resolve logical volume names for TTL move events. */
export const GET_STORAGE_POLICY_VOLUMES = `
  SELECT
    policy_name,
    volume_name,
    disks
  FROM system.storage_policies
`;


/**
 * Fetch text_log messages for a merge/mutation event by query_id.
 *
 * ClickHouse logs background merges to system.text_log with query_id
 * formatted as `{table_uuid}::{result_part_name}`. The caller constructs
 * this ID from the table UUID + part_name from part_log.
 */
export const GET_MERGE_TEXT_LOGS_BY_QUERY_ID = `
  SELECT
    toString(event_time) AS event_time,
    toString(event_time_microseconds) AS event_time_microseconds,
    query_id,
    level,
    message,
    logger_name AS source,
    thread_id,
    thread_name
  FROM {{cluster_aware:system.text_log}}
  WHERE query_id = {query_id}
    AND event_date >= {event_date_bound}
  ORDER BY event_time_microseconds ASC
  LIMIT 500
`;

/**
 * Get the table UUID from system.tables.
 * Background merges use `{table_uuid}::{result_part_name}` as query_id in
 * system.text_log, even though part_log.query_id is empty.
 * We fetch the UUID so we can construct the exact query_id for text_log lookup.
 */
export const GET_TABLE_UUID = `
  SELECT any(uuid) AS uuid
  FROM {{cluster_metadata:system.tables}}
  WHERE database = {database}
    AND name = {table}
  GROUP BY database, name
`;

/**
 * Look up a single MergeHistoryRecord by database + table + part_name.
 * Returns the most recent part_log entry for that result part.
 */
export const GET_MERGE_HISTORY_BY_PART_NAME = `
  SELECT
    event_time,
    event_type,
    database,
    table,
    part_name,
    partition_id,
    rows,
    size_in_bytes,
    duration_ms,
    merge_reason,
    merged_from,
    bytes_uncompressed,
    read_bytes,
    read_rows,
    peak_memory_usage,
    merge_algorithm,
    disk_name,
    path_on_disk,
    query_id,
    ProfileEvents,
    error,
    exception,
    hostName() AS hostname
  FROM {{cluster_aware:system.part_log}}
  WHERE database = {database}
    AND table = {table}
    AND part_name = {part_name}
    AND event_type IN ('MergeParts', 'MutatePart', 'MovePart')
  ORDER BY event_time DESC
  LIMIT 1
`;

