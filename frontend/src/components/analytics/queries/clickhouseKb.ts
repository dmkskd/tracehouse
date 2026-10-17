/**
 * ClickHouse Knowledge Base - read-only diagnostic queries adapted from
 * https://clickhouse.com/docs/knowledgebase.
 *
 * The official KB is article-based, so this module turns operationally useful
 * KB guidance into dashboard panels. Sources are kept per query via @source.
 *
 * NOTE: @meta description='...' is single-quoted by the parser, so descriptions
 * must not contain apostrophes / single quotes.
 */

const queries: string[] = [
  // ═══════════════════ Understanding part types and storage formats ═══════════════════
  // https://clickhouse.com/docs/knowledgebase/understanding-part-types-and-storage-formats

  `-- @meta: title='Part Type Summary' group='Knowledge Base' description='Understanding part types and storage formats - inspect Wide vs Compact part types for active non-system parts. The KB also covers part_storage_type for ClickHouse Cloud builds where that system.parts column exists.'
-- @chart: type=bar group_by=part_type value=parts style=2d
-- @source: https://clickhouse.com/docs/knowledgebase/understanding-part-types-and-storage-formats
SELECT
    part_type,
    max(level) AS max_level,
    count() AS parts,
    formatReadableSize(max(data_uncompressed_bytes)) AS max_uncompressed,
    formatReadableSize(min(data_uncompressed_bytes)) AS min_uncompressed,
    sum(data_uncompressed_bytes) AS total_uncompressed_bytes,
    formatReadableSize(total_uncompressed_bytes) AS total_uncompressed
FROM {{cluster_aware:system.parts}}
WHERE database != 'system' AND active
GROUP BY part_type
ORDER BY part_type ASC`,

  `-- @meta: title='Part Type by Table' group='Knowledge Base' description='Understanding part types and storage formats - tables with the most active parts, split by Wide and Compact part type.'
-- @chart: type=stacked_bar group_by=table_name value=parts series=part_type style=2d
-- @cell: column=parts type=gauge max=max_parts
-- @source: https://clickhouse.com/docs/knowledgebase/understanding-part-types-and-storage-formats
SELECT
    concat(database, '.', table) AS table_name,
    part_type,
    count() AS parts,
    max(count()) OVER () AS max_parts,
    formatReadableSize(sum(data_compressed_bytes)) AS compressed,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed
FROM {{cluster_aware:system.parts}}
WHERE database != 'system' AND active
GROUP BY database, table, part_type
ORDER BY parts DESC
LIMIT 30`,

  `-- @meta: title='Part Levels by Table' group='Knowledge Base' description='Understanding part types and storage formats - part levels by table and part type. In ClickHouse Cloud, storage-format thresholds can use level to decide when parts move from Packed to Full storage.'
-- @cell: column=parts type=gauge max=max_parts
-- @source: https://clickhouse.com/docs/knowledgebase/understanding-part-types-and-storage-formats
SELECT
    concat(database, '.', table) AS table_name,
    part_type,
    min(level) AS min_level,
    max(level) AS max_level,
    count() AS parts,
    max(count()) OVER () AS max_parts,
    formatReadableSize(sum(data_compressed_bytes)) AS compressed,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed
FROM {{cluster_aware:system.parts}}
WHERE database != 'system' AND active
GROUP BY database, table, part_type
ORDER BY parts DESC
LIMIT 30`,

  `-- @meta: title='Part Format Settings' group='Knowledge Base' description='Understanding part types and storage formats - MergeTree thresholds that decide when parts become Wide or Full storage.'
-- @source: https://clickhouse.com/docs/knowledgebase/understanding-part-types-and-storage-formats
SELECT
    database,
    name AS table,
    engine,
    nullIf(extract(create_table_query, '(?i)min_bytes_for_wide_part\\\\s*=\\\\s*([^,\\\\s]+)'), '') AS min_bytes_for_wide_part,
    nullIf(extract(create_table_query, '(?i)min_rows_for_wide_part\\\\s*=\\\\s*([^,\\\\s]+)'), '') AS min_rows_for_wide_part,
    nullIf(extract(create_table_query, '(?i)min_bytes_for_full_part_storage\\\\s*=\\\\s*([^,\\\\s]+)'), '') AS min_bytes_for_full_part_storage,
    nullIf(extract(create_table_query, '(?i)min_rows_for_full_part_storage\\\\s*=\\\\s*([^,\\\\s]+)'), '') AS min_rows_for_full_part_storage,
    nullIf(extract(create_table_query, '(?i)min_level_for_full_part_storage\\\\s*=\\\\s*([^,\\\\s]+)'), '') AS min_level_for_full_part_storage
FROM {{cluster_aware:system.tables}}
WHERE database != 'system'
  AND engine LIKE '%MergeTree%'
ORDER BY database, table
LIMIT 100`,

  // ═══════════════════ Find counts and sizes of wide or compact parts ═══════════════════
  // https://clickhouse.com/docs/knowledgebase/count-parts-by-type

  `-- @meta: title='Part Counts by Type' group='Knowledge Base' description='Find counts and sizes of wide or compact parts - active part counts by table and part type, matching the KB count query.'
-- @source: https://clickhouse.com/docs/knowledgebase/count-parts-by-type
SELECT
    table,
    part_type,
    count() AS \`count()\`
FROM {{cluster_aware:system.parts}}
WHERE active
GROUP BY
    table,
    part_type
ORDER BY
    table ASC,
    part_type ASC`,

  `-- @meta: title='Column Sizes by Part Type' group='Knowledge Base' description='Find counts and sizes of wide or compact parts - column rows and bytes by table, column and part type. Compact column sizes show as zero because column sizes are not calculated for compact parts.'
-- @source: https://clickhouse.com/docs/knowledgebase/count-parts-by-type
SELECT
    table,
    column,
    part_type,
    sum(rows) AS \`sum(rows)\`,
    sum(column_data_compressed_bytes) AS \`sum(column_data_compressed_bytes)\`,
    sum(column_data_uncompressed_bytes) AS \`sum(column_data_uncompressed_bytes)\`
FROM {{cluster_aware:system.parts_columns}}
WHERE active
GROUP BY
    table,
    column,
    part_type
ORDER BY
    table ASC,
    column ASC,
    part_type ASC`,

  // ═══════════════════ Memory limit exceeded for query ═══════════════════
  // https://clickhouse.com/docs/knowledgebase/memory-limit-exceeded-for-query

  `-- @meta: title='Top Queries by Memory' group='Knowledge Base' interval='1 DAY' description='Memory limit exceeded for query - completed query shapes with the highest peak memory over the selected window.'
-- @cell: column=max_memory_mb type=gauge max=max_seen_memory_mb unit=MB
-- @cell: column=max_memory_mb type=rag green<512 amber<4096
-- @drill: on=query_hash into='Memory Query Executions'
-- @source: https://clickhouse.com/docs/knowledgebase/memory-limit-exceeded-for-query
SELECT
    query_hash,
    query_kind,
    executions,
    max_memory_mb,
    avg_memory_mb,
    max_seen_memory_mb,
    example_query
FROM (
    SELECT
        lower(hex(normalized_query_hash)) AS query_hash,
        query_kind,
        count() AS executions,
        round(max(memory_usage) / 1048576, 1) AS max_memory_mb,
        round(avg(memory_usage) / 1048576, 1) AS avg_memory_mb,
        max(round(max(memory_usage) / 1048576, 1)) OVER () AS max_seen_memory_mb,
        substring(argMax(query, memory_usage), 1, 160) AS example_query
    FROM {{cluster_aware:system.query_log}}
    WHERE type = 'QueryFinish'
      AND event_time > {{time_range}}
    GROUP BY query_hash, query_kind
    ORDER BY max_memory_mb DESC
    LIMIT 30
)
ORDER BY max_memory_mb DESC`,

  `-- @meta: title='Memory Query Executions' group='Knowledge Base' interval='1 DAY' description='Individual executions for a selected memory-heavy query shape. Drill target from Top Queries by Memory.'
-- @link: on=query_id into='Query Detail by ID'
-- @source: https://clickhouse.com/docs/knowledgebase/memory-limit-exceeded-for-query
SELECT
    query_id,
    event_time,
    user,
    query_duration_ms,
    round(memory_usage / 1048576, 1) AS memory_mb,
    formatReadableSize(read_bytes) AS read_bytes,
    read_rows,
    substring(query, 1, 220) AS query_text
FROM {{cluster_aware:system.query_log}}
WHERE type = 'QueryFinish'
  AND lower(hex(normalized_query_hash)) = {{drill_value:query_hash | ''}}
  AND event_time > {{time_range}}
ORDER BY memory_usage DESC
LIMIT 50`,

  `-- @meta: title='Memory Spill & Join Settings' group='Knowledge Base' description='Memory limit exceeded for query - current settings related to external aggregation, external sort, join algorithms and query memory limits.'
-- @source: https://clickhouse.com/docs/knowledgebase/memory-limit-exceeded-for-query
SELECT
    name,
    value,
    changed,
    description
FROM system.settings
WHERE name IN (
    'max_memory_usage',
    'max_bytes_before_external_group_by',
    'max_bytes_ratio_before_external_group_by',
    'max_bytes_before_external_sort',
    'max_bytes_ratio_before_external_sort',
    'join_algorithm',
    'max_bytes_in_join',
    'join_overflow_mode',
    'distributed_aggregation_memory_efficient'
)
ORDER BY name`,

  // ═══════════════════ Runbook: JSON schema ═══════════════════
  // https://clickhouse.com/docs/knowledgebase

  `-- @meta: title='JSON Column Inventory' group='Knowledge Base' description='Runbook JSON schema - inventory columns that use JSON or String-backed JSON-like storage across non-system tables.'
-- @chart: type=bar group_by=column_family value=columns style=2d
-- @source: https://clickhouse.com/docs/knowledgebase
SELECT
    multiIf(
        type LIKE 'JSON%', 'JSON',
        type LIKE 'Object(%', 'Object',
        type = 'String' AND match(lower(name), 'json|payload|body|properties|attrs|attributes|metadata'), 'String candidate',
        'Other'
    ) AS column_family,
    count() AS columns,
    groupUniqArray(concat(database, '.', table)) AS tables
FROM {{cluster_aware:system.columns}}
WHERE database != 'system'
  AND (
      type LIKE 'JSON%'
      OR type LIKE 'Object(%'
      OR (type = 'String' AND match(lower(name), 'json|payload|body|properties|attrs|attributes|metadata'))
  )
GROUP BY column_family
ORDER BY columns DESC`,

  `-- @meta: title='JSON Columns by Table' group='Knowledge Base' description='Runbook JSON schema - per-table listing of JSON, Object and likely String-backed JSON columns.'
-- @source: https://clickhouse.com/docs/knowledgebase
SELECT
    database,
    table,
    groupArray(concat(name, ' ', type)) AS json_columns,
    count() AS columns
FROM {{cluster_aware:system.columns}}
WHERE database != 'system'
  AND (
      type LIKE 'JSON%'
      OR type LIKE 'Object(%'
      OR (type = 'String' AND match(lower(name), 'json|payload|body|properties|attrs|attributes|metadata'))
  )
GROUP BY database, table
ORDER BY columns DESC, database, table
LIMIT 100`,

  // ═══════════════════ Resolving the Too many parts exception ═══════════════════
  // https://clickhouse.com/docs/knowledgebase/exception-too-many-parts
  // The KB queries filter one table; the panels use the dashboard Table filter
  // and add the server-default limits each count is checked against.

  `-- @meta: title='Partitions Closest to Too Many Parts' group='Knowledge Base' description='Resolving the Too many parts exception - partitions with the most active parts, against the server default parts_to_delay_insert (inserts slowed) and parts_to_throw_insert (inserts rejected). Table-level overrides in SETTINGS are not reflected. Uses the Table filter; empty means all tables.'
-- @cell: column=pct_of_throw_limit type=rag green<33 amber<80
-- @source: https://clickhouse.com/docs/knowledgebase/exception-too-many-parts
WITH
    {{drill_value:tbl | ''}} AS target_table,
    (SELECT toUInt64(value) FROM system.merge_tree_settings WHERE name = 'parts_to_delay_insert') AS delay_limit,
    (SELECT toUInt64(value) FROM system.merge_tree_settings WHERE name = 'parts_to_throw_insert') AS throw_limit
SELECT
    hostName() AS host,
    database,
    table,
    partition_id,
    count() AS active_parts,
    sum(rows) AS rows,
    formatReadableSize(sum(bytes_on_disk)) AS size_on_disk,
    delay_limit,
    throw_limit,
    round(100 * active_parts / throw_limit, 1) AS pct_of_throw_limit
FROM {{cluster_aware:system.parts}}
WHERE active
  AND (target_table = '' OR concat(database, '.', table) = target_table)
GROUP BY host, database, table, partition_id
ORDER BY active_parts DESC
LIMIT 50`,

  `-- @meta: title='Tables Closest to max_parts_in_total' group='Knowledge Base' description='Resolving the Too many parts exception - active parts per table against the server default max_parts_in_total, the table-wide limit. Table-level overrides in SETTINGS are not reflected. Uses the Table filter; empty means all tables.'
-- @cell: column=pct_of_total_limit type=rag green<33 amber<80
-- @source: https://clickhouse.com/docs/knowledgebase/exception-too-many-parts
WITH
    {{drill_value:tbl | ''}} AS target_table,
    (SELECT toUInt64(value) FROM system.merge_tree_settings WHERE name = 'max_parts_in_total') AS total_limit
SELECT
    hostName() AS host,
    database,
    table,
    count() AS active_parts,
    sum(rows) AS rows,
    formatReadableSize(sum(bytes_on_disk)) AS size_on_disk,
    total_limit,
    round(100 * active_parts / total_limit, 1) AS pct_of_total_limit
FROM {{cluster_aware:system.parts}}
WHERE active
  AND (target_table = '' OR concat(database, '.', table) = target_table)
GROUP BY host, database, table
ORDER BY active_parts DESC
LIMIT 50`,

  // ═══════════════════ How do I view the number of active or queued mutations? ═══════════════════
  // https://clickhouse.com/docs/knowledgebase/view_number_of_active_mutations

  `-- @meta: title='Active or Queued Mutations' group='Knowledge Base' description='How do I view the number of active or queued mutations - unfinished mutations per host. Any non-zero count is work still pending.'
-- @cell: column=active_or_queued type=rag green<1 amber<10
-- @source: https://clickhouse.com/docs/knowledgebase/view_number_of_active_mutations
SELECT
    hostName() AS host,
    count() AS active_or_queued
FROM {{cluster_aware:system.mutations}}
WHERE NOT is_done
GROUP BY host
ORDER BY active_or_queued DESC`,

  // ═══════════════════ Useful queries for troubleshooting ═══════════════════
  // https://clickhouse.com/docs/knowledgebase/useful-queries-for-troubleshooting

  // Fan-out side effect, verified on 26.9: a clusterAllReplicas query reports
  // queue_max_wait_ms, run_query_in_background, skip_unavailable_shards(_mode)
  // as changed on the initiator and interactive_delay on the other nodes. They
  // are excluded so the panel only shows settings the profile actually changes.
  `-- @meta: title='Settings Changed from Default' group='Knowledge Base' description='Useful queries for troubleshooting - session settings that differ from the default, per host, as seen by the TraceHouse user profile. Settings set by the cluster fan-out itself are excluded.'
-- @source: https://clickhouse.com/docs/knowledgebase/useful-queries-for-troubleshooting
SELECT
    hostName() AS host,
    name,
    value
FROM {{cluster_aware:system.settings}}
WHERE changed
  AND name NOT IN ('queue_max_wait_ms', 'run_query_in_background', 'skip_unavailable_shards', 'skip_unavailable_shards_mode', 'interactive_delay')
ORDER BY host, name`,

  `-- @meta: title='Projection Disk Usage' group='Knowledge Base' description='Useful queries for troubleshooting - how much disk space projections use, per projection part, and the ratio to the parent part.'
-- @source: https://clickhouse.com/docs/knowledgebase/useful-queries-for-troubleshooting
SELECT
    hostName() AS host,
    database,
    table,
    name,
    parent_name,
    formatReadableSize(bytes_on_disk) AS bytes,
    formatReadableSize(parent_bytes_on_disk) AS parent_bytes,
    round(bytes_on_disk / parent_bytes_on_disk, 3) AS ratio
FROM {{cluster_aware:system.projection_parts}}
WHERE active
ORDER BY bytes_on_disk DESC
LIMIT 100`,
];

export default queries;
