/**
 * SQL queries for the Overview page
 * Organized by polling tier for efficient data fetching
 */

// =============================================================================
// TIER 1: Virtual Tables - Poll every 5 seconds (zero cost)
// =============================================================================

/**
 * Running queries with ProfileEvents for CPU/IO attribution
 */
export const GET_RUNNING_QUERIES = `
SELECT
    hostName() AS hostname,
    query_id,
    user,
    elapsed,
    memory_usage,
    read_rows,
    read_bytes,
    total_rows_approx,
    query_kind,
    query,
    ProfileEvents['UserTimeMicroseconds'] AS user_time_us,
    ProfileEvents['SystemTimeMicroseconds'] AS system_time_us,
    ProfileEvents['OSReadBytes'] AS os_read_bytes,
    ProfileEvents['OSWriteBytes'] AS os_write_bytes,
    ProfileEvents['SelectedParts'] AS selected_parts,
    ProfileEvents['SelectedMarks'] AS selected_marks,
    ProfileEvents['MarkCacheHits'] AS mark_cache_hits,
    ProfileEvents['MarkCacheMisses'] AS mark_cache_misses
FROM {{cluster_aware:system.processes}}
WHERE is_initial_query = 1
ORDER BY (user_time_us + system_time_us) DESC
`;

/**
 * Active merges with progress and resource usage
 */
export const GET_ACTIVE_MERGES_LIVE = `
SELECT
    hostName() AS hostname,
    database,
    table,
    result_part_name AS part_name,
    elapsed,
    progress,
    memory_usage,
    bytes_read_uncompressed,
    bytes_written_uncompressed,
    rows_read,
    num_parts,
    is_mutation,
    merge_type
FROM {{cluster_aware:system.merges}}
ORDER BY elapsed DESC
`;

/**
 * Queries per second over the last 15 minutes (15-second buckets).
 * Uses metric_log which records ProfileEvent_Query each second.
 * event_date added for partition pruning on metric_log.
 */
export const GET_QPS_HISTORY = `
SELECT
    toStartOfInterval(event_time, INTERVAL 15 SECOND) AS t,
    avg(ProfileEvent_Query) AS qps
FROM {{cluster_aware:system.metric_log}}
WHERE event_date >= today()
  AND event_time > now() - INTERVAL 15 MINUTE
GROUP BY t
ORDER BY t ASC
`;

/**
 * Instant metrics from system.metrics
 */
export const GET_INSTANT_METRICS = `
SELECT metric, value
FROM system.metrics
WHERE metric IN (
    'Query',
    'Merge',
    'PartMutation',
    'ReplicatedFetch',
    'ReplicatedSend',
    'QueryPipelineExecutorThreadsActive',
    'MergeTreeBackgroundExecutorThreadsActive',
    'BackgroundFetchesPoolTask',
    'TCPConnection',
    'HTTPConnection',
    'QueryPreempted'
)
`;

/**
 * Server concurrency limit from system.server_settings.
 * Falls back to system.settings if server_settings is unavailable.
 */
export const GET_MAX_CONCURRENT_QUERIES = `
SELECT toUInt64(value) AS max_concurrent
FROM system.server_settings
WHERE name = 'max_concurrent_queries'
LIMIT 1
`;

/**
 * Count of queries rejected with TOO_MANY_SIMULTANEOUS_QUERIES in the last hour.
 * Uses ExceptionBeforeStart — these queries never executed.
 *
 * event_date added for partition pruning — query_log is partitioned by event_date.
 * ClickHouse may infer it from event_time, but explicit is safer and costs nothing.
 */
export const GET_REJECTED_QUERIES_COUNT = `
SELECT count() AS cnt
FROM {{cluster_aware:system.query_log}}
WHERE type = 'ExceptionBeforeStart'
  AND exception LIKE '%TOO_MANY_SIMULTANEOUS_QUERIES%'
  AND event_date >= today() - 1
  AND event_time > now() - INTERVAL 1 HOUR
`;

/**
 * Async metrics for CPU/Memory/IO totals
 * Uses OSUserTimeNormalized etc. which are already normalized to [0..1] per core
 * This avoids needing to know the core count separately
 */
export const GET_ASYNC_METRICS = `
SELECT metric, value
FROM system.asynchronous_metrics
WHERE metric IN (
    'OSUserTime',
    'OSSystemTime',
    'OSIdleTime',
    'OSIOWaitTime',
    'OSUserTimeNormalized',
    'OSSystemTimeNormalized',
    'OSIdleTimeNormalized',
    'OSIOWaitTimeNormalized',
    'LoadAverage1',
    'LoadAverage5',
    'LoadAverage15',
    'OSMemoryTotal',
    'OSMemoryAvailable',
    'MemoryResident',
    'MarkCacheBytes',
    'MarkCacheFiles',
    'UncompressedCacheBytes',
    'UncompressedCacheCells',
    'jemalloc.allocated',
    'jemalloc.resident',
    'jemalloc.mapped',
    'jemalloc.retained',
    'jemalloc.metadata',
    'Uptime'
)
`;

/**
 * Cluster-aware async metrics — returns per-host rows so we can aggregate
 * CPU ratios (averaged) and memory/IO bytes (summed) across all nodes.
 */
export const GET_CLUSTER_ASYNC_METRICS = `
SELECT
    hostName() AS hostname,
    metric,
    value
FROM {{cluster_aware:system.asynchronous_metrics}}
WHERE metric IN (
    'OSUserTime',
    'OSSystemTime',
    'OSUserTimeNormalized',
    'OSSystemTimeNormalized',
    'OSMemoryTotal',
    'OSMemoryAvailable',
    'MemoryResident',
    'MarkCacheBytes',
    'UncompressedCacheBytes',
    'jemalloc.allocated',
    'jemalloc.resident',
    'Uptime',
    'LoadAverage1'
)
`;

/**
 * Recent I/O rates from metric_log (last 10 seconds).
 * ProfileEvent_OSReadBytes / ProfileEvent_OSWriteBytes are per-interval deltas,
 * so we average them to get bytes/sec.
 * OSReadBytes/OSWriteBytes do NOT exist in system.asynchronous_metrics.
 */
export const GET_RECENT_IO_RATES = `
SELECT
    avg(ProfileEvent_OSReadBytes) AS read_bytes_per_sec,
    avg(ProfileEvent_OSWriteBytes) AS write_bytes_per_sec
FROM {{cluster_aware:system.metric_log}}
WHERE event_time >= now() - INTERVAL 10 SECOND
`;

// =============================================================================
// TIER 2: Log Tables - Poll every 30 seconds (light reads)
// =============================================================================

/**
 * CPU from recently completed merges (for attribution).
 * event_date added for partition pruning on part_log.
 */
export const GET_RECENT_MERGE_CPU = `
SELECT
    sum(ProfileEvents['UserTimeMicroseconds']) AS user_time_us,
    sum(ProfileEvents['SystemTimeMicroseconds']) AS system_time_us
FROM {{cluster_aware:system.part_log}}
WHERE event_type = 'MergeParts'
  AND event_date >= today()
  AND event_time > now() - INTERVAL {window_seconds:UInt32} SECOND
`;

/**
 * CPU from recently completed mutations (for attribution).
 * event_date added for partition pruning on part_log.
 */
export const GET_RECENT_MUTATION_CPU = `
SELECT
    sum(ProfileEvents['UserTimeMicroseconds']) AS user_time_us,
    sum(ProfileEvents['SystemTimeMicroseconds']) AS system_time_us
FROM {{cluster_aware:system.part_log}}
WHERE event_type = 'MutatePart'
  AND event_date >= today()
  AND event_time > now() - INTERVAL {window_seconds:UInt32} SECOND
`;

// =============================================================================
// TIER 3: Structural Tables - Poll every 60 seconds (aggregations)
// =============================================================================

/**
 * Parts count per partition for "too many parts" alert.
 * Natural key: (database, table, partition_id, name). Use uniq(name) to dedup.
 */
export const GET_PARTS_ALERTS = `
SELECT 
    database, 
    table, 
    partition_id, 
    uniq(name) AS part_count
FROM {{cluster_metadata:system.parts}} 
WHERE active
GROUP BY database, table, partition_id
HAVING part_count > {parts_threshold:UInt32}
ORDER BY part_count DESC
LIMIT 10
`;

/**
 * Disk space for low disk alerts. Natural key: (name).
 * Disks can differ across nodes — use min(free_space) for worst-case alerting.
 * Uses subquery to filter total_space > 0 and to isolate aggregation from
 * the free_ratio calculation (avoids nested aggregate alias collision).
 */
export const GET_DISK_ALERTS = `
SELECT
    name,
    path,
    free_space,
    total_space,
    free_space / total_space AS free_ratio
FROM (
    SELECT
        name,
        any(path) AS path,
        min(free_space) AS free_space,
        any(total_space) AS total_space
    FROM (
        SELECT * FROM {{cluster_metadata:system.disks}}
        WHERE total_space > 0
    )
    GROUP BY name
)
`;

/**
 * Replication health summary.
 * Natural key for replicas: (database, table). Dedup via subquery GROUP BY,
 * then aggregate the deduped rows.
 */
export const GET_REPLICATION_SUMMARY = `
SELECT
    count() AS total_tables,
    countIf(is_readonly = 0 AND absolute_delay < 300) AS healthy_tables,
    countIf(is_readonly = 1) AS readonly_replicas,
    max(absolute_delay) AS max_delay,
    sum(queue_size) AS queue_size,
    sum(active_replicas) AS active_replicas
FROM (
    SELECT
        database,
        table,
        any(is_readonly) AS is_readonly,
        any(absolute_delay) AS absolute_delay,
        any(queue_size) AS queue_size,
        any(active_replicas) AS active_replicas
    FROM {{cluster_metadata:system.replicas}}
    GROUP BY database, table
)
`;

/**
 * Primary key memory by table (for memory attribution).
 * Natural key: (database, table, name) in system.parts. Dedup via subquery,
 * then sum the deduped pk bytes.
 */
export const GET_PK_MEMORY = `
SELECT sum(pk_bytes) AS pk_bytes
FROM (
    SELECT name, any(primary_key_bytes_in_memory) AS pk_bytes
    FROM {{cluster_metadata:system.parts}}
    WHERE active
    GROUP BY database, table, name
)
`;

/**
 * Dictionary memory (for memory attribution).
 * Natural key: (name). Dedup via subquery, then sum.
 */
export const GET_DICT_MEMORY = `
SELECT sum(dict_bytes) AS dict_bytes
FROM (
    SELECT name, any(bytes_allocated) AS dict_bytes
    FROM {{cluster_metadata:system.dictionaries}}
    GROUP BY name
)
`;

/**
 * Server info
 */
export const GET_SERVER_INFO = `
SELECT
    hostName() AS hostname,
    version() AS version
`;

/**
 * Per-host resource capacity across the cluster.
 * Returns effective cores (cgroup-limited) and memory for each node.
 * Uses {{cluster_aware:...}} so it fans out to all replicas.
 */
export const GET_CLUSTER_RESOURCE_CAPACITY = `
SELECT
    hostName() AS hostname,
    anyIf(value, metric = 'NumberOfCPUCores') AS host_cores,
    anyIf(value, metric = 'CGroupMaxCPU') AS cgroup_cpu,
    anyIf(value, metric = 'OSMemoryTotal') AS host_mem,
    greatest(anyIf(value, metric = 'CGroupMemoryTotal'), anyIf(value, metric = 'CGroupMemoryLimit')) AS cgroup_mem
FROM {{cluster_aware:system.asynchronous_metrics}}
WHERE metric IN ('NumberOfCPUCores', 'CGroupMaxCPU', 'OSMemoryTotal', 'CGroupMemoryLimit', 'CGroupMemoryTotal')
GROUP BY hostname
`;

// =============================================================================
// DEEP-DIVE WIDGET QUERIES (for Overview landing page teasers)
// =============================================================================

/**
 * Top 5 tables by total bytes on disk (compressed).
 * Uses system.parts to sum active parts per table.
 */
export const GET_TOP_TABLES = `
SELECT
    database,
    table,
    sum(bytes_on_disk) AS total_bytes,
    sum(rows) AS total_rows,
    uniq(name) AS part_count
FROM {{cluster_metadata:system.parts}}
WHERE active AND database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
GROUP BY database, table
ORDER BY total_bytes DESC
LIMIT 5
`;

/**
 * Engine health: jemalloc fragmentation, thread pool saturation, background tasks.
 * Combines asynchronous_metrics and system.metrics in one pass.
 */
export const GET_ENGINE_HEALTH = `
SELECT
    'async' AS src,
    metric,
    value
FROM system.asynchronous_metrics
WHERE metric IN (
    'jemalloc.allocated',
    'jemalloc.resident',
    'MemoryResident',
    'OSMemoryTotal'
)
UNION ALL
SELECT
    'instant' AS src,
    metric,
    toFloat64(value) AS value
FROM system.metrics
WHERE metric IN (
    'BackgroundMergesAndMutationsPoolTask',
    'BackgroundMergesAndMutationsPoolSize',
    'BackgroundFetchesPoolTask',
    'BackgroundFetchesPoolSize',
    'BackgroundCommonPoolTask',
    'BackgroundCommonPoolSize',
    'BackgroundSchedulePoolTask',
    'BackgroundSchedulePoolSize'
)
`;

/**
 * Slow queries completed in the last hour (> 10s duration).
 * event_date added for partition pruning (see GET_REJECTED_QUERIES_COUNT).
 */
export const GET_SLOW_QUERIES_SUMMARY = `
SELECT
    count() AS slow_count,
    max(query_duration_ms) AS max_duration_ms,
    avg(query_duration_ms) AS avg_duration_ms
FROM {{cluster_aware:system.query_log}}
WHERE type = 'QueryFinish'
  AND query_duration_ms > 10000
  AND event_date >= today() - 1
  AND event_time > now() - INTERVAL 1 HOUR
  AND query_kind = 'Select'
`;

/**
 * Ordering key efficiency: worst-performing table by granule selectivity
 * in the last 24 hours. Lower ratio = worse efficiency.
 * event_date bounds added for partition pruning — today() - 1 covers the
 * 24-hour window across midnight boundaries.
 */
export const GET_WORST_ORDERING_KEY = `
SELECT
    databases[1] AS database,
    tables[1] AS table,
    sum(ProfileEvents['SelectedMarks']) AS selected_marks,
    sum(ProfileEvents['SelectedParts']) AS selected_parts,
    greatest(1, sum(read_rows)) AS total_read_rows,
    count() AS query_count
FROM {{cluster_aware:system.query_log}}
WHERE type = 'QueryFinish'
  AND query_kind = 'Select'
  AND event_date >= today() - 1
  AND event_time > now() - INTERVAL 24 HOUR
  AND length(databases) = 1
  AND length(tables) = 1
  AND databases[1] NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
GROUP BY database, table
HAVING query_count >= 3 AND selected_marks > 0
ORDER BY selected_marks / greatest(1, selected_parts) DESC
LIMIT 1
`;

/**
 * Recent CPU spikes in the last 15 minutes (count of 30s buckets where CPU > 70%).
 * Uses metric_log which records once per second.
 *
 * CPU percentage is calculated relative to the number of CPU cores, not a single core.
 * ProfileEvent_OSCPUVirtualTimeMicroseconds is the delta µs of CPU time per sample.
 * 100% = all cores fully utilized = cpu_cores × 1,000,000 µs/s.
 * Without dividing by core count, a 16-core machine would alert at ~4.4% actual usage.
 *
 * Requires: cpu_cores param (from EnvironmentDetector or async metrics).
 * event_date added for partition pruning (metric_log is partitioned by event_date).
 */
export const GET_RECENT_CPU_SPIKES = `
SELECT
    count() AS spike_count,
    max(cpu_pct) AS max_cpu
FROM (
    SELECT
        toStartOfInterval(event_time, INTERVAL 30 SECOND) AS t,
        100 * avg(ProfileEvent_OSCPUVirtualTimeMicroseconds)
            / ({cpu_cores:UInt32} * 1000000) AS cpu_pct
    FROM {{cluster_aware:system.metric_log}}
    WHERE event_date >= today()
      AND event_time > now() - INTERVAL 15 MINUTE
    GROUP BY t
    HAVING cpu_pct > 70
)
`;
