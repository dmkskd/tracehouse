/**
 * Memory monitoring queries — deep visibility into ClickHouse memory consumers.
 *
 * Sources:
 * - ClickHouse docs: https://clickhouse.com/docs/guides/developer/debugging-memory-issues
 * - Altinity KB: https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-who-ate-my-memory/
 */

const queries: string[] = [
  `-- @meta: title='Memory Breakdown' group='Memory' description='Comprehensive snapshot of all memory consumers — OS, caches, queries, merges, primary keys, dictionaries, and more'
-- Source: https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-who-ate-my-memory/
SELECT category, name, formatReadableSize(val) AS readable_size
FROM (
    SELECT 'OS' AS category, metric AS name, toInt64(value) AS val
    FROM system.asynchronous_metrics WHERE metric LIKE 'OSMemory%'
      UNION ALL
    SELECT 'Caches' AS category, metric AS name, toInt64(value) AS val
    FROM system.asynchronous_metrics WHERE metric LIKE '%CacheBytes'
      UNION ALL
    SELECT 'Caches' AS category, metric AS name, toInt64(value) AS val
    FROM system.metrics WHERE metric LIKE '%CacheBytes'
      UNION ALL
    SELECT 'MMaps' AS category, metric AS name, toInt64(value) AS val
    FROM system.metrics WHERE metric LIKE 'MMappedFileBytes'
      UNION ALL
    SELECT 'Process' AS category, metric AS name, toInt64(value) AS val
    FROM system.asynchronous_metrics WHERE metric LIKE 'Memory%'
      UNION ALL
    SELECT 'Tables (Memory engines)' AS category, engine AS name, toInt64(sum(total_bytes)) AS val
    FROM system.tables WHERE engine IN ('Join','Memory','Buffer','Set') GROUP BY engine
      UNION ALL
    SELECT 'Storage Buffers' AS category, metric AS name, toInt64(value) AS val
    FROM system.metrics WHERE metric = 'StorageBufferBytes'
      UNION ALL
    SELECT 'Running Queries' AS category, 'queries' AS name, toInt64(sum(memory_usage)) AS val
    FROM system.processes
      UNION ALL
    SELECT 'Dictionaries' AS category, type AS name, toInt64(sum(bytes_allocated)) AS val
    FROM system.dictionaries GROUP BY type
      UNION ALL
    SELECT 'Primary Keys' AS category, 'db:' || database AS name, toInt64(sum(primary_key_bytes_in_memory_allocated)) AS val
    FROM system.parts GROUP BY database
      UNION ALL
    SELECT 'Merges' AS category, 'db:' || database AS name, toInt64(sum(memory_usage)) AS val
    FROM system.merges GROUP BY database
      UNION ALL
    SELECT 'In-Memory Parts' AS category, 'db:' || database AS name, toInt64(sum(data_uncompressed_bytes)) AS val
    FROM system.parts WHERE part_type = 'InMemory' GROUP BY database
      UNION ALL
    SELECT 'MemoryTracking' AS category, 'total' AS name, toInt64(value) AS val
    FROM system.metrics WHERE metric = 'MemoryTracking'
)
WHERE val > 0
ORDER BY category, val DESC`,

  `-- @meta: title='Memory Breakdown (WIP summary)' group='Memory' description='[TEST] Aggregated memory by category with descriptions — iterating on this before replacing the main query'
-- Source: https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-who-ate-my-memory/
-- @drill: on=category into='Memory Breakdown Detail (WIP)'
-- @chart: type=bar group_by=category description=description value=total_mb unit=MB style=2d
SELECT category, description, round(total_bytes / 1048576, 1) AS total_mb, formatReadableSize(total_bytes) AS readable_size, items
FROM (
    SELECT category, description, sum(val) AS total_bytes, count() AS items
    FROM (
        SELECT 'OS' AS category, 'Total, available, free, cached, and swap memory reported by the OS' AS description, metric AS name, toInt64(value) AS val
        FROM system.asynchronous_metrics WHERE metric LIKE 'OSMemory%'
          UNION ALL
        SELECT 'Caches' AS category, 'ClickHouse internal caches — mark, uncompressed, compiled expressions, DNS, page cache, query cache, etc.' AS description, metric AS name, toInt64(value) AS val
        FROM system.asynchronous_metrics WHERE metric LIKE '%CacheBytes'
          UNION ALL
        SELECT 'Caches' AS category, 'ClickHouse internal caches — mark, uncompressed, compiled expressions, DNS, page cache, query cache, etc.' AS description, metric AS name, toInt64(value) AS val
        FROM system.metrics WHERE metric LIKE '%CacheBytes'
          UNION ALL
        SELECT 'MMaps' AS category, 'Memory-mapped files used by ClickHouse for reading data without copying into userspace' AS description, metric AS name, toInt64(value) AS val
        FROM system.metrics WHERE metric LIKE 'MMappedFileBytes'
          UNION ALL
        SELECT 'Process' AS category, 'OS-level process memory — virtual, resident, shared, code segments (from /proc)' AS description, metric AS name, toInt64(value) AS val
        FROM system.asynchronous_metrics WHERE metric LIKE 'Memory%'
          UNION ALL
        SELECT 'Tables (Memory engines)' AS category, 'Tables using Memory, Set, Join, or Buffer engines — data lives entirely in RAM' AS description, engine AS name, toInt64(sum(total_bytes)) AS val
        FROM system.tables WHERE engine IN ('Join','Memory','Buffer','Set') GROUP BY engine
          UNION ALL
        SELECT 'Storage Buffers' AS category, 'Buffer() engine pending data not yet flushed to destination tables' AS description, metric AS name, toInt64(value) AS val
        FROM system.metrics WHERE metric = 'StorageBufferBytes'
          UNION ALL
        SELECT 'Running Queries' AS category, 'Memory held by currently executing queries (SELECT, INSERT, etc.)' AS description, 'queries' AS name, toInt64(sum(memory_usage)) AS val
        FROM system.processes
          UNION ALL
        SELECT 'Dictionaries' AS category, 'Loaded external dictionaries kept in memory for fast lookups' AS description, type AS name, toInt64(sum(bytes_allocated)) AS val
        FROM system.dictionaries GROUP BY type
          UNION ALL
        SELECT 'Primary Keys' AS category, 'Primary index loaded in RAM for each MergeTree part — grows with table count and key width' AS description, 'db:' || database AS name, toInt64(sum(primary_key_bytes_in_memory_allocated)) AS val
        FROM system.parts GROUP BY database
          UNION ALL
        SELECT 'Merges' AS category, 'Temporary memory used by background merge operations' AS description, 'db:' || database AS name, toInt64(sum(memory_usage)) AS val
        FROM system.merges GROUP BY database
          UNION ALL
        SELECT 'In-Memory Parts' AS category, 'Small parts stored entirely in RAM (part_type = InMemory) before being merged to disk' AS description, 'db:' || database AS name, toInt64(sum(data_uncompressed_bytes)) AS val
        FROM system.parts WHERE part_type = 'InMemory' GROUP BY database
          UNION ALL
        SELECT 'MemoryTracking' AS category, 'Total memory tracked by ClickHouse allocator — the main memory accounting metric' AS description, 'total' AS name, toInt64(value) AS val
        FROM system.metrics WHERE metric = 'MemoryTracking'
    )
    WHERE val > 0
    GROUP BY category, description
)
ORDER BY total_bytes DESC`,

  `-- @meta: title='Memory Breakdown Detail (WIP)' group='Memory' description='[TEST] Individual items within a category — drill target for the summary'
-- @chart: type=bar group_by=name description=description value=size unit=bytes style=2d
SELECT name, val AS size, formatReadableSize(val) AS readable_size, description
FROM (
    SELECT 'OS' AS category, metric AS name, toInt64(value) AS val,
        am.description AS description
    FROM system.asynchronous_metrics am WHERE metric LIKE 'OSMemory%'
      UNION ALL
    SELECT 'Caches' AS category, metric AS name, toInt64(value) AS val,
        am.description AS description
    FROM system.asynchronous_metrics am WHERE metric LIKE '%CacheBytes'
      UNION ALL
    SELECT 'Caches' AS category, metric AS name, toInt64(value) AS val,
        m.description AS description
    FROM system.metrics m WHERE metric LIKE '%CacheBytes'
      UNION ALL
    SELECT 'MMaps' AS category, metric AS name, toInt64(value) AS val,
        m.description AS description
    FROM system.metrics m WHERE metric LIKE 'MMappedFileBytes'
      UNION ALL
    SELECT 'Process' AS category, metric AS name, toInt64(value) AS val,
        am.description AS description
    FROM system.asynchronous_metrics am WHERE metric LIKE 'Memory%'
      UNION ALL
    SELECT 'Tables (Memory engines)' AS category, database || '.' || name AS name, toInt64(total_bytes) AS val,
        engine || ' engine table' AS description
    FROM system.tables WHERE engine IN ('Join','Memory','Buffer','Set')
      UNION ALL
    SELECT 'Storage Buffers' AS category, metric AS name, toInt64(value) AS val,
        m.description AS description
    FROM system.metrics m WHERE metric = 'StorageBufferBytes'
      UNION ALL
    SELECT 'Running Queries' AS category, substring(query, 1, 80) AS name, toInt64(memory_usage) AS val,
        'query_id: ' || initial_query_id AS description
    FROM system.processes
      UNION ALL
    SELECT 'Dictionaries' AS category, database || '.' || name AS name, toInt64(bytes_allocated) AS val,
        type || ', ' || toString(element_count) || ' elements' AS description
    FROM system.dictionaries
      UNION ALL
    SELECT 'Primary Keys' AS category, database || '.' || table AS name, toInt64(sum(primary_key_bytes_in_memory_allocated)) AS val,
        toString(count()) || ' parts' AS description
    FROM system.parts GROUP BY database, table
      UNION ALL
    SELECT 'Merges' AS category, database || '.' || table AS name, toInt64(sum(memory_usage)) AS val,
        toString(count()) || ' active merges' AS description
    FROM system.merges GROUP BY database, table
      UNION ALL
    SELECT 'In-Memory Parts' AS category, database || '.' || table AS name, toInt64(sum(data_uncompressed_bytes)) AS val,
        toString(count()) || ' in-memory parts' AS description
    FROM system.parts WHERE part_type = 'InMemory' GROUP BY database, table
      UNION ALL
    SELECT 'MemoryTracking' AS category, metric AS name, toInt64(value) AS val,
        m.description AS description
    FROM system.metrics m WHERE metric = 'MemoryTracking'
)
WHERE category = {{drill_value:category | 'Caches'}}
  AND val > 0
ORDER BY val DESC`,

  `-- @meta: title='Cache Sizes (current)' group='Memory' description='Current values of all cache and memory metrics from system.metrics and system.asynchronous_metrics'
-- Source: https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-who-ate-my-memory/
-- Source: https://clickhouse.com/docs/guides/developer/debugging-memory-issues
SELECT source, metric, toInt64(value) AS bytes, formatReadableSize(value) AS size
FROM (
    SELECT 'metrics' AS source, metric, value
    FROM system.metrics
    WHERE (metric ILIKE '%Cach%' OR metric ILIKE '%Mem%') AND value != 0
      UNION ALL
    SELECT 'async_metrics' AS source, metric, value
    FROM system.asynchronous_metrics
    WHERE (metric LIKE '%Cach%' OR metric LIKE '%Mem%') AND value > 0
)
ORDER BY value DESC`,

  `-- @meta: title='Primary Key Memory by Database' group='Memory' description='Primary key memory allocated per database — often a silent memory hog on wide tables'
-- @chart: type=bar group_by=database value=pk_allocated_mb unit=MB style=2d
-- @drill: on=database into='Primary Key Memory by Table'
-- Source: https://clickhouse.com/docs/guides/developer/debugging-memory-issues
-- Source: https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-who-ate-my-memory/
SELECT
    database,
    count() AS tables,
    formatReadableSize(sum(primary_key_bytes_in_memory)) AS pk_used,
    formatReadableSize(sum(primary_key_bytes_in_memory_allocated)) AS pk_allocated,
    round(sum(primary_key_bytes_in_memory_allocated) / 1048576, 2) AS pk_allocated_mb,
    formatReadableSize(sumIf(data_uncompressed_bytes, part_type = 'InMemory')) AS in_memory_parts
FROM system.parts
GROUP BY database
ORDER BY sum(primary_key_bytes_in_memory_allocated) DESC`,

  `-- @meta: title='Primary Key Memory by Table' group='Memory' description='Primary key memory per table within a database — find which tables dominate PK memory'
-- Source: https://clickhouse.com/docs/guides/developer/debugging-memory-issues
SELECT
    database,
    table,
    count() AS parts,
    formatReadableSize(sum(primary_key_bytes_in_memory)) AS pk_used,
    formatReadableSize(sum(primary_key_bytes_in_memory_allocated)) AS pk_allocated,
    round(sum(primary_key_bytes_in_memory_allocated) / 1048576, 2) AS pk_allocated_mb,
    formatReadableSize(sumIf(data_uncompressed_bytes, part_type = 'InMemory')) AS in_memory_parts
FROM system.parts
WHERE database = {{drill_value:database | 'default'}}
GROUP BY database, table
ORDER BY sum(primary_key_bytes_in_memory_allocated) DESC`,

  `-- @meta: title='Memory-Engine Tables' group='Memory' description='Tables using Memory, Set, Join, or Buffer engines — these live entirely in RAM'
-- Source: https://clickhouse.com/docs/guides/developer/debugging-memory-issues
-- Source: https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-who-ate-my-memory/
SELECT
    database,
    name AS table_name,
    engine,
    formatReadableSize(total_bytes) AS readable_size,
    total_bytes AS bytes,
    formatReadableSize(total_rows) AS rows
FROM system.tables
WHERE engine IN ('Memory', 'Set', 'Join', 'Buffer')
  AND total_bytes > 0
ORDER BY total_bytes DESC`,

  `-- @meta: title='Dictionary Memory' group='Memory' description='Memory allocated by each loaded dictionary'
-- Source: https://clickhouse.com/docs/guides/developer/debugging-memory-issues
-- Source: https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-who-ate-my-memory/
SELECT
    database,
    name,
    type,
    status,
    element_count,
    formatReadableSize(bytes_allocated) AS memory,
    bytes_allocated AS bytes,
    loading_duration
FROM system.dictionaries
ORDER BY bytes_allocated DESC`,

  `-- @meta: title='Top Running Queries by Memory' group='Memory' description='Currently running queries sorted by peak memory — spot memory-hungry queries in real time'
-- @rag: column=peak_memory_mb green<100 amber<500
-- Source: https://clickhouse.com/docs/guides/developer/debugging-memory-issues
-- Source: https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-who-ate-my-memory/
SELECT
    initial_query_id,
    user,
    round(elapsed, 1) AS elapsed_sec,
    formatReadableSize(memory_usage) AS current_memory,
    formatReadableSize(peak_memory_usage) AS peak_memory,
    round(peak_memory_usage / 1048576, 2) AS peak_memory_mb,
    read_rows,
    formatReadableSize(read_bytes) AS read_bytes,
    substring(query, 1, 150) AS query_preview
FROM system.processes
ORDER BY peak_memory_usage DESC
LIMIT 30`,

  `-- @meta: title='Historical Top Memory Queries' group='Memory' interval='1 DAY' description='Most memory-hungry completed queries from query_log — find past offenders'
-- @rag: column=memory_mb green<100 amber<500
-- @drill: on=query_hash into='Memory Query Executions'
-- Source: https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-who-ate-my-memory/
SELECT
    lower(hex(normalized_query_hash)) AS query_hash,
    count() AS executions,
    round(max(memory_usage) / 1048576, 2) AS max_memory_mb,
    round(avg(memory_usage) / 1048576, 2) AS avg_memory_mb,
    round(avg(memory_usage) / 1048576, 2) AS memory_mb,
    formatReadableSize(max(memory_usage)) AS max_memory,
    round(avg(query_duration_ms), 1) AS avg_duration_ms,
    any(user) AS user,
    substring(any(query), 1, 150) AS query_preview
FROM {{cluster_aware:system.query_log}}
WHERE type = 'QueryFinish'
  AND event_time > {{time_range}}
GROUP BY query_hash
ORDER BY max(memory_usage) DESC
LIMIT 30`,

  `-- @meta: title='Memory Query Executions' group='Memory' interval='1 DAY' description='Individual executions of a specific high-memory query shape'
-- @rag: column=memory_mb green<100 amber<500
SELECT
    query_id,
    event_time,
    user,
    query_duration_ms,
    round(memory_usage / 1048576, 2) AS memory_mb,
    formatReadableSize(memory_usage) AS memory,
    read_rows,
    formatReadableSize(read_bytes) AS read_bytes,
    substring(query, 1, 200) AS query_text
FROM {{cluster_aware:system.query_log}}
WHERE type = 'QueryFinish'
  AND lower(hex(normalized_query_hash)) = {{drill_value:query_hash | ''}}
  AND event_time > {{time_range}}
ORDER BY memory_usage DESC
LIMIT 50`,

  `-- @meta: title='Memory Peak Analysis (5min)' group='Memory' interval='1 DAY' description='Memory peak events from trace_log in 5-minute windows — identifies the biggest query per window'
-- @chart: type=bar group_by=t value=sum_of_peaks_mb unit=MB style=2d
-- Source: https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-who-ate-my-memory/
SELECT
    t,
    queries,
    round(sum_of_peaks / 1048576, 1) AS sum_of_peaks_mb,
    formatReadableSize(sum_of_peaks) AS sum_of_peaks,
    formatReadableSize(biggest_peak) AS biggest_query_peak,
    biggest_query_id
FROM (
    SELECT
        toStartOfInterval(event_time, INTERVAL 5 MINUTE) AS t,
        count() AS queries,
        sum(peak_size) AS sum_of_peaks,
        max(peak_size) AS biggest_peak,
        argMax(query_id, peak_size) AS biggest_query_id
    FROM (
        SELECT
            toStartOfInterval(event_time, INTERVAL 5 MINUTE) AS t,
            query_id,
            event_time,
            max(size) AS peak_size
        FROM {{cluster_aware:system.trace_log}}
        WHERE trace_type = 'MemoryPeak'
          AND event_time > {{time_range}}
        GROUP BY t, query_id, event_time
    )
    GROUP BY t
)
ORDER BY t ASC`,

  `-- @meta: title='Memory Trend (MemoryTracking)' group='Memory' interval='1 HOUR' description='Total tracked memory over time — the headline memory metric'
-- @chart: type=area group_by=t value=memory_mb unit=MB style=2d
-- Source: https://clickhouse.com/docs/guides/developer/debugging-memory-issues
-- Source: https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-who-ate-my-memory/
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    round(avg(value) / 1048576, 1) AS memory_mb
FROM {{cluster_aware:system.asynchronous_metric_log}}
WHERE metric = 'MemoryTracking'
  AND event_time > {{time_range}}
GROUP BY t
ORDER BY t ASC`,

  `-- @meta: title='Cache Trend' group='Memory' interval='1 HOUR' description='Individual cache sizes over time — mark cache, uncompressed cache, page cache, etc.'
-- @chart: type=stacked_area group_by=t value=value_mb series=metric unit=MB style=2d
-- Source: https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-who-ate-my-memory/
SELECT
    toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS t,
    metric,
    round(avg(value) / 1048576, 1) AS value_mb
FROM {{cluster_aware:system.asynchronous_metric_log}}
WHERE metric IN (
    'MarkCacheBytes', 'UncompressedCacheBytes',
    'MMapCacheCells', 'CompiledExpressionCacheBytes',
    'DNSCacheEntries'
)
  AND event_time > {{time_range}}
GROUP BY t, metric
ORDER BY t ASC, metric`,

  `-- @meta: title='Memory by Query Kind (hourly)' group='Memory' interval='1 DAY' description='Hourly peak memory broken down by query type — SELECTs vs INSERTs vs merges'
-- @chart: type=stacked_bar group_by=hour value=memory_mb series=query_kind unit=MB style=2d
-- Source: https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-who-ate-my-memory/
SELECT
    toStartOfHour(event_time) AS hour,
    query_kind,
    round(sum(memory_usage) / 1048576, 1) AS memory_mb
FROM {{cluster_aware:system.query_log}}
WHERE type = 'QueryFinish'
  AND event_time > {{time_range}}
  AND query_kind != ''
GROUP BY hour, query_kind
ORDER BY hour ASC, memory_mb DESC`,

  `-- @meta: title='Merge Memory' group='Memory' description='Memory consumed by currently active merge operations, grouped by database and table'
-- @chart: type=bar group_by=table value=memory_mb unit=MB style=2d
-- Source: https://clickhouse.com/docs/guides/developer/debugging-memory-issues
-- Source: https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-who-ate-my-memory/
SELECT
    database,
    table,
    count() AS active_merges,
    formatReadableSize(sum(memory_usage)) AS memory,
    round(sum(memory_usage) / 1048576, 2) AS memory_mb,
    round(sum(progress) / count() * 100, 1) AS avg_progress_pct
FROM system.merges
GROUP BY database, table
ORDER BY sum(memory_usage) DESC`,
];

export default queries;
