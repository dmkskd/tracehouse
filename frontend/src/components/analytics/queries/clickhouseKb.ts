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
];

export default queries;
