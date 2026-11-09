/**
 * SQL query templates for merge tracking.
 *
 * Extracted from backend/services/database_explorer.py (merge-related
 * queries) and grafana-app-plugin/src/datasource.ts. All placeholders use
 * {param} syntax compatible with buildQuery().
 */

// Keep the exported query constants valid for capable/current ClickHouse
// servers. The resolver recognizes these comments and substitutes only the
// marked expressions for older schemas.
const MUTATION_IS_KILLED_EXPRESSION =
  'is_killed /* tracehouse:capability=mutation_is_killed */';
const MUTATION_IS_KILLED_SUPPORTED =
  'toUInt8(1) /* tracehouse:capability=mutation_is_killed_supported */';

/**
 * Resolve the optional system.mutations.is_killed column before buildQuery().
 * Older servers still expose useful mutation state, but cannot distinguish a
 * killed mutation from other unfinished/completed states.
 */
export function withMutationKilledCapability(sql: string, supported: boolean): string {
  return sql
    .replaceAll(
      MUTATION_IS_KILLED_EXPRESSION,
      supported ? 'is_killed' : 'toUInt8(0)',
    )
    .replaceAll(
      MUTATION_IS_KILLED_SUPPORTED,
      supported ? 'toUInt8(1)' : 'toUInt8(0)',
    );
}

/** Detect system.mutations.is_killed on every queried host. */
export const GET_MUTATION_CAPABILITIES = `
  SELECT min(has_is_killed) AS has_is_killed
  FROM (
    SELECT
      hostName() AS hostname,
      countIf(name = 'is_killed') > 0 AS has_is_killed
    FROM {{cluster_aware:system.columns}}
    WHERE database = 'system'
      AND table = 'mutations'
    GROUP BY hostname
  )
`;

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

/** Columns for the merge history listing table.
 * This includes enough part_log fields to classify and summarize rows in the
 * list. Additional detail-only fields such as ProfileEvents and path_on_disk
 * are fetched on-demand via GET_MERGE_HISTORY_BY_PART_NAME. */
const MERGE_HISTORY_LITE_COLUMNS = `
    event_time,
    toString(event_time, 'UTC') AS event_time_utc,
    concat(toString(event_time_microseconds, 'UTC'), 'Z') AS event_time_us,
    event_type,
    database,
    table,
    part_name,
    rows,
    size_in_bytes,
    duration_ms,
    merge_reason,
    merged_from,
    bytes_uncompressed,
    read_bytes,
    read_rows,
    error,
    substring(exception, 1, 512) AS exception,
    hostName() AS hostname`;

/** Get merge history for a specific database and table from system.part_log. */
export const GET_MERGE_HISTORY = `
  SELECT ${MERGE_HISTORY_LITE_COLUMNS}
  FROM {{cluster_aware:system.part_log}}
  WHERE database = {database}
    AND table = {table}
    AND event_type IN ('MergeParts', 'MutatePart', 'MovePart', 'DownloadPart')
  ORDER BY event_time DESC
  LIMIT {limit}
`;

/** Get all merge history across all databases from system.part_log. */
export const GET_ALL_MERGE_HISTORY = `
  SELECT ${MERGE_HISTORY_LITE_COLUMNS}
  FROM {{cluster_aware:system.part_log}}
  WHERE event_type IN ('MergeParts', 'MutatePart', 'MovePart', 'DownloadPart')
  ORDER BY event_time DESC
  LIMIT {limit}
`;

/** Get merge history for a database (all tables) from system.part_log. */
export const GET_DATABASE_MERGE_HISTORY = `
  SELECT ${MERGE_HISTORY_LITE_COLUMNS}
  FROM {{cluster_aware:system.part_log}}
  WHERE database = {database}
    AND event_type IN ('MergeParts', 'MutatePart', 'MovePart', 'DownloadPart')
  ORDER BY event_time DESC
  LIMIT {limit}
`;

/** Get mutations from system.mutations with full details.
 * Natural key: (database, table, mutation_id). Dedup across replicas/shards.
 * Uses HAVING instead of WHERE so that a mutation only appears as active
 * when ALL shards still have work to do (min(is_done) = 0).
 * Qualify aggregated clocks and repeat the aggregate in ORDER BY: bare aliases
 * can resolve to source columns with prefer_column_name_to_alias=1.
 * Note: parts_to_do_names is an Array column that doesn't resolve through
 * clusterAllReplicas subqueries, so we flatten via toString(). */
export const GET_MUTATIONS = `
  SELECT
    database,
    table,
    mutation_id,
    any(command) AS command,
    any(mutation_rows.create_time) AS create_time,
    toString(any(mutation_rows.create_time), 'UTC') AS create_time_utc,
    any(partition_ids) AS partition_ids,
    any(block_numbers) AS block_numbers,
    sum(sub_parts_to_do) AS parts_to_do,
    0 AS parts_in_progress,
    anyIf(parts_to_do_names_str, raw_is_done = 0) AS parts_to_do_names,
    '[]' AS parts_in_progress_names,
    min(raw_is_done) AS is_done,
    any(latest_failed_part) AS latest_failed_part,
    any(mutation_rows.latest_fail_time) AS latest_fail_time,
    toString(any(mutation_rows.latest_fail_time), 'UTC') AS latest_fail_time_utc,
    any(latest_fail_reason) AS latest_fail_reason,
    max(raw_is_killed) AS is_killed,
    min(raw_is_killed_supported) AS is_killed_supported
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
      ${MUTATION_IS_KILLED_EXPRESSION} AS raw_is_killed,
      ${MUTATION_IS_KILLED_SUPPORTED} AS raw_is_killed_supported
    FROM {{cluster_aware:system.mutations}}
  ) AS mutation_rows
  GROUP BY database, table, mutation_id
  HAVING min(raw_is_done) = 0 AND max(raw_is_killed) = 0
  ORDER BY
    parts_to_do DESC,
    any(mutation_rows.create_time) DESC
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
    FROM {{cluster_aware:system.parts}}
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
    any(mutation_rows.create_time) AS create_time,
    toString(any(mutation_rows.create_time), 'UTC') AS create_time_utc,
    min(raw_is_done) AS is_done,
    max(raw_is_killed) AS is_killed,
    min(raw_is_killed_supported) AS is_killed_supported,
    any(latest_failed_part) AS latest_failed_part,
    any(mutation_rows.latest_fail_time) AS latest_fail_time,
    toString(any(mutation_rows.latest_fail_time), 'UTC') AS latest_fail_time_utc,
    any(latest_fail_reason) AS latest_fail_reason
  FROM (
    SELECT
      *,
      is_done AS raw_is_done,
      ${MUTATION_IS_KILLED_EXPRESSION} AS raw_is_killed,
      ${MUTATION_IS_KILLED_SUPPORTED} AS raw_is_killed_supported
    FROM {{cluster_aware:system.mutations}}
  ) AS mutation_rows
  GROUP BY database, table, mutation_id
  HAVING min(raw_is_done) = 1 OR max(raw_is_killed) = 1
  ORDER BY any(mutation_rows.create_time) DESC
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
    any(mutation_rows.create_time) AS create_time,
    toString(any(mutation_rows.create_time), 'UTC') AS create_time_utc,
    min(raw_is_done) AS is_done,
    max(raw_is_killed) AS is_killed,
    min(raw_is_killed_supported) AS is_killed_supported,
    any(latest_failed_part) AS latest_failed_part,
    any(mutation_rows.latest_fail_time) AS latest_fail_time,
    toString(any(mutation_rows.latest_fail_time), 'UTC') AS latest_fail_time_utc,
    any(latest_fail_reason) AS latest_fail_reason
  FROM (
    SELECT
      *,
      is_done AS raw_is_done,
      ${MUTATION_IS_KILLED_EXPRESSION} AS raw_is_killed,
      ${MUTATION_IS_KILLED_SUPPORTED} AS raw_is_killed_supported
    FROM {{cluster_aware:system.mutations}}
    WHERE database = {database}
  ) AS mutation_rows
  GROUP BY database, table, mutation_id
  HAVING min(raw_is_done) = 1 OR max(raw_is_killed) = 1
  ORDER BY any(mutation_rows.create_time) DESC
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
    any(mutation_rows.create_time) AS create_time,
    toString(any(mutation_rows.create_time), 'UTC') AS create_time_utc,
    min(raw_is_done) AS is_done,
    max(raw_is_killed) AS is_killed,
    min(raw_is_killed_supported) AS is_killed_supported,
    any(latest_failed_part) AS latest_failed_part,
    any(mutation_rows.latest_fail_time) AS latest_fail_time,
    toString(any(mutation_rows.latest_fail_time), 'UTC') AS latest_fail_time_utc,
    any(latest_fail_reason) AS latest_fail_reason
  FROM (
    SELECT
      *,
      is_done AS raw_is_done,
      ${MUTATION_IS_KILLED_EXPRESSION} AS raw_is_killed,
      ${MUTATION_IS_KILLED_SUPPORTED} AS raw_is_killed_supported
    FROM {{cluster_aware:system.mutations}}
    WHERE database = {database}
      AND table = {table}
  ) AS mutation_rows
  GROUP BY database, table, mutation_id
  HAVING min(raw_is_done) = 1 OR max(raw_is_killed) = 1
  ORDER BY any(mutation_rows.create_time) DESC
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
 * Fetch text_log messages for a merge/mutation event by exact query_id.
 *
 * ClickHouse logs background merges to system.text_log with query_id
 * formatted as `{table_uuid}::{result_part_name}`. The caller constructs
 * this ID from the table UUID + part_name from part_log.
 */
export const GET_MERGE_TEXT_LOGS_BY_QUERY_ID = `
  SELECT
    toString(event_time, 'UTC') AS event_time,
    toString(event_time_microseconds, 'UTC') AS event_time_microseconds,
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

/** Same as GET_MERGE_TEXT_LOGS_BY_QUERY_ID but filtered to a specific hostname. */
export const GET_MERGE_TEXT_LOGS_BY_QUERY_ID_HOST = `
  SELECT
    toString(event_time, 'UTC') AS event_time,
    toString(event_time_microseconds, 'UTC') AS event_time_microseconds,
    query_id,
    level,
    message,
    logger_name AS source,
    thread_id,
    thread_name
  FROM {{cluster_aware:system.text_log}}
  WHERE query_id = {query_id}
    AND event_date >= {event_date_bound}
    AND hostName() = {hostname}
  ORDER BY event_time_microseconds ASC
  LIMIT 500
`;

/**
 * Fetch text_log messages by part name suffix match.
 *
 * The internal UUID used in text_log query_ids may differ from system.tables.uuid
 * on Replicated database clusters. This query uses a suffix LIKE pattern
 * (`%::{part_name}`) to match regardless of which UUID prefix is used.
 * Combined with event_date pruning this is fast (~150ms).
 */
export const GET_MERGE_TEXT_LOGS_BY_PART_SUFFIX = `
  SELECT
    toString(event_time, 'UTC') AS event_time,
    toString(event_time_microseconds, 'UTC') AS event_time_microseconds,
    query_id,
    level,
    message,
    logger_name AS source,
    thread_id,
    thread_name
  FROM {{cluster_aware:system.text_log}}
  WHERE query_id LIKE {query_id_suffix}
    AND event_date >= {event_date_bound}
  ORDER BY event_time_microseconds ASC
  LIMIT 500
`;

/** Same as above but filtered to a specific hostname. */
export const GET_MERGE_TEXT_LOGS_BY_PART_SUFFIX_HOST = `
  SELECT
    toString(event_time, 'UTC') AS event_time,
    toString(event_time_microseconds, 'UTC') AS event_time_microseconds,
    query_id,
    level,
    message,
    logger_name AS source,
    thread_id,
    thread_name
  FROM {{cluster_aware:system.text_log}}
  WHERE query_id LIKE {query_id_suffix}
    AND event_date >= {event_date_bound}
    AND hostName() = {hostname}
  ORDER BY event_time_microseconds ASC
  LIMIT 500
`;

/** Get column names for a table (used for vertical merge progress).
 * Uses GROUP BY to deduplicate across replicas in cluster-aware queries. */
export const GET_TABLE_COLUMNS = `
  SELECT name, min(position) AS pos
  FROM {{cluster_aware:system.columns}}
  WHERE database = {database}
    AND table = {table}
  GROUP BY name
  ORDER BY pos
`;

/**
 * Get the table UUID from system.tables.
 * Background merges use `{table_uuid}::{result_part_name}` as query_id in
 * system.text_log, even though part_log.query_id is empty.
 * We fetch the UUID so we can construct the exact query_id for text_log lookup.
 */
export const GET_TABLE_UUID = `
  SELECT any(uuid) AS uuid
  FROM {{cluster_aware:system.tables}}
  WHERE database = {database}
    AND name = {table}
  GROUP BY database, name
`;

/**
 * Get average merge throughput for a table, grouped by merge algorithm and size bucket.
 * Used to estimate remaining time for active merges.
 * Only considers successful merges (duration > 0, size > 0).
 *
 * Size buckets: <10 MB, 10–100 MB, 100 MB–1 GB, 1–10 GB, ≥10 GB.
 */
export const GET_MERGE_THROUGHPUT_ESTIMATE = `
  SELECT
    merge_algorithm,
    multiIf(
      size_in_bytes < 10485760, toUInt64(0),
      size_in_bytes < 104857600, toUInt64(10485760),
      size_in_bytes < 1073741824, toUInt64(104857600),
      size_in_bytes < 10737418240, toUInt64(1073741824),
      toUInt64(10737418240)
    ) AS size_bucket_lower,
    count() AS merge_count,
    avg(size_in_bytes / (duration_ms / 1000)) AS avg_bytes_per_sec,
    median(size_in_bytes / (duration_ms / 1000)) AS median_bytes_per_sec,
    avg(duration_ms) AS avg_duration_ms,
    avg(size_in_bytes) AS avg_size_bytes
  FROM {{cluster_aware:system.part_log}}
  WHERE database = {database}
    AND table = {table}
    AND event_type = 'MergeParts'
    AND duration_ms > 100
    AND size_in_bytes > 0
  GROUP BY merge_algorithm, size_bucket_lower
`;

/**
 * Look up a single MergeHistoryRecord by database + table + part_name.
 * Returns the most recent part_log entry for that result part.
 */
export const GET_MERGE_HISTORY_BY_PART_NAME = `
  SELECT
    event_time,
    toString(event_time, 'UTC') AS event_time_utc,
    -- Not aliased as event_time_microseconds: the alias would shadow the column in WHERE / ORDER BY.
    concat(toString(event_time_microseconds, 'UTC'), 'Z') AS event_time_us,
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
    AND event_type IN ('MergeParts', 'MutatePart', 'MovePart', 'DownloadPart')
  ORDER BY event_time DESC
  LIMIT 1
`;

/**
 * Most recent part_log record for a result part on one host. Replicas each
 * record the same part name, so a link from a specific replica's merge must
 * not fall back to another replica's row. Same host filter as
 * GET_MERGE_HISTORY_BY_IDENTITY.
 */
export const GET_MERGE_HISTORY_BY_PART_ON_HOST = `
  SELECT
    event_time,
    toString(event_time, 'UTC') AS event_time_utc,
    -- Not aliased as event_time_microseconds: the alias would shadow the column in WHERE / ORDER BY.
    concat(toString(event_time_microseconds, 'UTC'), 'Z') AS event_time_us,
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
    AND hostName() = {hostname}
    AND event_type IN ('MergeParts', 'MutatePart', 'MovePart', 'DownloadPart')
  ORDER BY event_time DESC
  LIMIT 1
`;

/**
 * Look up the exact selected part_log operation. Replicated tables can record
 * the same result part on multiple hosts, and retries can reuse a part name on
 * one host, so part_name alone is not a stable event identity.
 */
export const GET_MERGE_HISTORY_BY_IDENTITY = `
  SELECT
    event_time,
    toString(event_time, 'UTC') AS event_time_utc,
    -- Not aliased as event_time_microseconds: the alias would shadow the column in WHERE / ORDER BY.
    concat(toString(event_time_microseconds, 'UTC'), 'Z') AS event_time_us,
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
    AND hostName() = {hostname}
    AND event_type = {event_type}
  ORDER BY
    abs(dateDiff(
      'millisecond',
      event_time_microseconds,
      parseDateTime64BestEffort({event_time}, 6, 'UTC')
    )) ASC,
    event_time_microseconds DESC
  LIMIT 1
`;

/**
 * The part_log record with exactly this event_time_microseconds (as a UTC string with an explicit Z suffix). Used by links that name one attempt: no
 * nearest-time fallback, so a retry in the same second or millisecond is never
 * substituted.
 */
export const GET_MERGE_HISTORY_BY_EXACT_TIME = `
  SELECT
    event_time,
    toString(event_time, 'UTC') AS event_time_utc,
    -- Not aliased as event_time_microseconds: the alias would shadow the column in WHERE / ORDER BY.
    concat(toString(event_time_microseconds, 'UTC'), 'Z') AS event_time_us,
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
    AND hostName() = {hostname}
    AND event_type = {event_type}
    AND event_time_microseconds = parseDateTime64BestEffort({event_time_us}, 6, 'UTC')
  LIMIT 1
`;

/** Old unzoned identities are opaque source-rendered keys. Return up to two
 * candidates so the service can reject ambiguity without choosing a retry. */
export const GET_MERGE_HISTORY_BY_LEGACY_TIME = `
  SELECT
    event_time,
    toString(event_time, 'UTC') AS event_time_utc,
    -- Not aliased as event_time_microseconds: the alias would shadow the column in WHERE / ORDER BY.
    concat(toString(event_time_microseconds, 'UTC'), 'Z') AS event_time_us,
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
    AND hostName() = {hostname}
    AND event_type = {event_type}
    AND toString(event_time_microseconds) = {event_time_us}
  LIMIT 2
`;

/**
 * The merge (or mutation) record on one host whose attempt started at
 * `started_ms` (epoch ms), within `tolerance_ms`. For links to a merge that
 * was running when the link was made and may have finished since: a retry of
 * the same part starts later and a fetch / move is not a merge, so neither is
 * returned.
 */
export const GET_MERGE_HISTORY_BY_START = `
  SELECT
    event_time,
    toString(event_time, 'UTC') AS event_time_utc,
    -- Not aliased as event_time_microseconds: the alias would shadow the column in WHERE / ORDER BY.
    concat(toString(event_time_microseconds, 'UTC'), 'Z') AS event_time_us,
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
    AND hostName() = {hostname}
    AND event_type IN ('MergeParts', 'MutatePart')
    -- Start of the attempt = finish - duration. Within the tolerance of the
    -- link's start (both server clock), so a later retry is never taken.
    AND abs(toInt64(toUnixTimestamp64Milli(event_time_microseconds)) - toInt64(duration_ms) - {started_ms}) <= {tolerance_ms}
  ORDER BY abs(toInt64(toUnixTimestamp64Milli(event_time_microseconds)) - toInt64(duration_ms) - {started_ms}) ASC
  LIMIT 1
`;

/**
 * Get table engines for all user tables.
 * Used to enrich merge history with engine info (e.g. to distinguish
 * lightweight delete cleanup from ReplacingMergeTree dedup).
 * Deduped across replicas via GROUP BY.
 */
export const GET_TABLE_ENGINES = `
  SELECT database, name AS table, any(engine) AS engine
  FROM {{cluster_aware:system.tables}}
  WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
  GROUP BY database, name
`;
