/**
 * SQL queries for the Engine Internals page
 * Provides deep-dive engine diagnostics
 */

// =============================================================================
// Memory X-Ray Queries
// =============================================================================

/**
 * Jemalloc memory allocator stats
 */
export const GET_JEMALLOC_STATS = `
SELECT metric, value
FROM system.asynchronous_metrics
WHERE metric IN (
    'jemalloc.allocated',
    'jemalloc.resident',
    'jemalloc.mapped',
    'jemalloc.retained',
    'jemalloc.metadata',
    'MemoryResident',
    'OSMemoryTotal',
    'OSMemoryAvailable',
    'CGroupMemoryLimit',
    'CGroupMemoryTotal',
    'TotalPrimaryKeyBytesInMemory',
    'TotalPrimaryKeyBytesInMemoryAllocated'
)
`;

/**
 * Mark cache statistics
 */
export const GET_MARK_CACHE_STATS = `
SELECT
    value AS bytes,
    (SELECT value FROM system.asynchronous_metrics WHERE metric = 'MarkCacheFiles') AS files,
    (SELECT value FROM system.events WHERE event = 'MarkCacheHits') AS hits,
    (SELECT value FROM system.events WHERE event = 'MarkCacheMisses') AS misses
FROM system.asynchronous_metrics
WHERE metric = 'MarkCacheBytes'
`;

/**
 * Uncompressed cache statistics
 */
export const GET_UNCOMPRESSED_CACHE_STATS = `
SELECT
    value AS bytes,
    (SELECT value FROM system.asynchronous_metrics WHERE metric = 'UncompressedCacheCells') AS cells,
    (SELECT value FROM system.events WHERE event = 'UncompressedCacheHits') AS hits,
    (SELECT value FROM system.events WHERE event = 'UncompressedCacheMisses') AS misses
FROM system.asynchronous_metrics
WHERE metric = 'UncompressedCacheBytes'
`;

/**
 * Query memory from running processes
 */
export const GET_QUERY_MEMORY = `
SELECT sum(memory_usage) AS total_memory
FROM system.processes
WHERE is_initial_query = 1
`;

/**
 * Merge memory from active merges
 */
export const GET_MERGE_MEMORY = `
SELECT sum(memory_usage) AS total_memory
FROM {{cluster_aware:system.merges}}
`;

// =============================================================================
// CPU Core Map Queries
// =============================================================================

/**
 * Per-core CPU metrics (if available)
 * Note: ClickHouse exposes OSUserTimeCPU0, OSUserTimeCPU1, etc.
 */
export const GET_CPU_CORE_METRICS = `
SELECT metric, value
FROM system.asynchronous_metrics
WHERE metric LIKE 'OSUserTimeCPU%'
   OR metric LIKE 'OSSystemTimeCPU%'
   OR metric LIKE 'OSIOWaitTimeCPU%'
   OR metric LIKE 'OSIdleTimeCPU%'
   OR metric = 'NumberOfCPUCores'
   OR metric = 'NumberOfPhysicalCores'
ORDER BY metric
`;

/**
 * Cgroup-aware CPU count.
 * In Kubernetes, NumberOfCPUCores / NumberOfPhysicalCores report the host node's
 * cores, not the pod's cgroup limit. ClickHouse detects cgroup limits at startup
 * and reflects them in the CGroupMaxCPU async metric (available since ~23.8).
 * We also query max_threads from system.settings as a fallback — ClickHouse sets
 * this to the detected CPU count on startup.
 */
export const GET_CGROUP_CPU = `
SELECT metric, value
FROM system.asynchronous_metrics
WHERE metric = 'CGroupMaxCPU'
`;

export const GET_MAX_THREADS = `
SELECT value
FROM system.settings
WHERE name = 'max_threads'
LIMIT 1
`;

// =============================================================================
// Thread Pool Queries
// =============================================================================

/**
 * Thread pool metrics
 */
export const GET_THREAD_POOL_METRICS = `
SELECT metric, value
FROM system.metrics
WHERE metric IN (
    'QueryThread',
    'BackgroundMergesAndMutationsPoolTask',
    'BackgroundMergesAndMutationsPoolSize',
    'BackgroundFetchesPoolTask',
    'BackgroundFetchesPoolSize',
    'BackgroundSchedulePoolTask',
    'BackgroundSchedulePoolSize',
    'BackgroundMovePoolTask',
    'BackgroundMovePoolSize',
    'BackgroundCommonPoolTask',
    'BackgroundCommonPoolSize',
    'GlobalThread',
    'GlobalThreadActive',
    'IOThreads',
    'IOThreadsActive'
)
`;

// =============================================================================
// Primary Key Index Queries
// =============================================================================

/**
 * Primary key memory by table.
 * Natural key: (database, table, name) in system.parts. Dedup parts first,
 * then aggregate per table.
 */
export const GET_PK_INDEX_BY_TABLE = `
SELECT
    database,
    table,
    sum(pk_memory) AS pk_memory,
    sum(pk_allocated) AS pk_allocated,
    count() AS parts,
    sum(part_rows) AS total_rows,
    sum(part_marks) AS granules
FROM (
    SELECT
        database,
        table,
        name,
        any(primary_key_bytes_in_memory) AS pk_memory,
        any(primary_key_bytes_in_memory_allocated) AS pk_allocated,
        any(rows) AS part_rows,
        any(marks) AS part_marks
    FROM {{cluster_metadata:system.parts}}
    WHERE active
    GROUP BY database, table, name
)
GROUP BY database, table
ORDER BY pk_memory DESC
LIMIT {limit:UInt32}
`;

// =============================================================================
// Dictionary Queries
// =============================================================================

/**
 * Dictionaries in memory. Natural key: (name). Dedup across replicas.
 */
export const GET_DICTIONARIES = `
SELECT
    name,
    any(type) AS type,
    any(bytes_allocated) AS bytes_allocated,
    any(element_count) AS element_count,
    any(load_factor) AS load_factor,
    any(source) AS source,
    any(status) AS loading_status,
    any(last_successful_update_time) AS last_successful_update_time
FROM {{cluster_metadata:system.dictionaries}}
GROUP BY name
ORDER BY bytes_allocated DESC
`;

// =============================================================================
// Query Internals Queries
// =============================================================================

/**
 * Detailed query information for a specific query
 */
export const GET_QUERY_INTERNALS = `
SELECT
    query_id,
    user,
    elapsed,
    memory_usage,
    query_kind,
    query,
    read_rows,
    read_bytes,
    total_rows_approx,
    ProfileEvents['UserTimeMicroseconds'] AS user_time_us,
    ProfileEvents['SystemTimeMicroseconds'] AS system_time_us,
    ProfileEvents['RealTimeMicroseconds'] AS real_time_us,
    ProfileEvents['OSIOWaitMicroseconds'] AS io_wait_us,
    ProfileEvents['ReadCompressedBytes'] AS read_compressed_bytes,
    ProfileEvents['SelectedParts'] AS selected_parts,
    ProfileEvents['SelectedMarks'] AS selected_marks,
    ProfileEvents['MarkCacheHits'] AS mark_cache_hits,
    ProfileEvents['MarkCacheMisses'] AS mark_cache_misses,
    thread_ids
FROM system.processes
WHERE query_id = {query_id:String}
`;

/**
 * Get all running queries for internals view
 */
export const GET_ALL_QUERY_INTERNALS = `
SELECT
    query_id,
    user,
    elapsed,
    memory_usage,
    query_kind,
    query,
    read_rows,
    read_bytes,
    total_rows_approx,
    ProfileEvents['UserTimeMicroseconds'] AS user_time_us,
    ProfileEvents['SystemTimeMicroseconds'] AS system_time_us,
    ProfileEvents['RealTimeMicroseconds'] AS real_time_us,
    ProfileEvents['OSIOWaitMicroseconds'] AS io_wait_us,
    ProfileEvents['ReadCompressedBytes'] AS read_compressed_bytes,
    ProfileEvents['SelectedParts'] AS selected_parts,
    ProfileEvents['SelectedMarks'] AS selected_marks,
    ProfileEvents['MarkCacheHits'] AS mark_cache_hits,
    ProfileEvents['MarkCacheMisses'] AS mark_cache_misses,
    length(thread_ids) AS thread_count
FROM system.processes
WHERE is_initial_query = 1
ORDER BY memory_usage DESC
`;

/**
 * Server info for engine internals header
 */
export const GET_ENGINE_SERVER_INFO = `
SELECT
    hostName() AS hostname,
    version() AS version
`;


// =============================================================================
// CPU Sampling Attribution (from system.trace_log)
// =============================================================================

/**
 * Aggregate CPU + Real-time samples by thread_name from trace_log.
 * Uses both 'CPU' and 'Real' trace types because under heavy CPU saturation,
 * the CPU profiler (SIGPROF/CLOCK_THREAD_CPUTIME_ID) signal delivery gets
 * delayed/coalesced by the kernel, producing very few samples. The Real-time
 * profiler (CLOCK_MONOTONIC) doesn't suffer from this — and for active threads,
 * a real-time sample IS effectively a CPU sample.
 * 
 * thread_name examples:
 *   - QueryPipelineEx  (query execution threads)
 *   - BgSchPool        (background schedule pool)
 *   - Merge            (merge threads)  
 *   - MutateThread     (mutation threads)
 *   - BgMovePool       (background move/TTL pool)
 *   - HTTPHandler      (HTTP connection handlers)
 *   - TCPHandler       (TCP/native protocol handlers)
 *   - BackgrProcPool   (background processing pool)
 *   - IOThreadPool     (IO thread pool)
 */
export const GET_CPU_SAMPLES_BY_THREAD = `
SELECT
    thread_name,
    count() AS cpu_samples,
    countIf(query_id != '') AS query_samples,
    countIf(query_id = '') AS background_samples
FROM {{cluster_aware:system.trace_log}}
WHERE event_time >= now() - toIntervalSecond({window_seconds:UInt32} + {offset_seconds:UInt32})
  AND event_time <= now() - toIntervalSecond({offset_seconds:UInt32})
  AND trace_type = 'CPU'
GROUP BY thread_name
ORDER BY cpu_samples DESC
LIMIT 30
`;

/** Fallback for servers where trace_log lacks thread_name column */
export const GET_CPU_SAMPLES_BY_THREAD_FALLBACK = `
SELECT
    CAST(thread_id, 'String') AS thread_name,
    count() AS cpu_samples,
    countIf(query_id != '') AS query_samples,
    countIf(query_id = '') AS background_samples
FROM {{cluster_aware:system.trace_log}}
WHERE event_time >= now() - toIntervalSecond({window_seconds:UInt32} + {offset_seconds:UInt32})
  AND event_time <= now() - toIntervalSecond({offset_seconds:UInt32})
  AND trace_type = 'CPU'
GROUP BY thread_name
ORDER BY cpu_samples DESC
LIMIT 30
`;

/**
 * Top stack traces by sample count — shows what functions are burning CPU.
 * Uses demangle + addressToSymbol for human-readable function names.
 * Limited to top frames to keep it lightweight.
 */
export const GET_TOP_CPU_STACKS = `
SELECT
    thread_name,
    demangle(addressToSymbol(trace[1])) AS top_function,
    count() AS samples
FROM {{cluster_aware:system.trace_log}}
WHERE event_time >= now() - toIntervalSecond({window_seconds:UInt32} + {offset_seconds:UInt32})
  AND event_time <= now() - toIntervalSecond({offset_seconds:UInt32})
  AND trace_type = 'CPU'
GROUP BY thread_name, top_function
ORDER BY samples DESC
LIMIT 20
`;

/** Fallback for servers where trace_log lacks thread_name column */
export const GET_TOP_CPU_STACKS_FALLBACK = `
SELECT
    CAST(thread_id, 'String') AS thread_name,
    demangle(addressToSymbol(trace[1])) AS top_function,
    count() AS samples
FROM {{cluster_aware:system.trace_log}}
WHERE event_time >= now() - toIntervalSecond({window_seconds:UInt32} + {offset_seconds:UInt32})
  AND event_time <= now() - toIntervalSecond({offset_seconds:UInt32})
  AND trace_type = 'CPU'
GROUP BY thread_name, top_function
ORDER BY samples DESC
LIMIT 20
`;

// =============================================================================
// Per-Core Timeline (from system.trace_log)
// =============================================================================

/**
 * Per-core CPU timeline from trace_log.
 * Each row is a sampling event pinned to a physical CPU core with microsecond
 * precision. This powers the "physical view" swimlane visualization.
 *
 * We bucket samples into small time slots (100ms) per core to build a dense
 * timeline. For each slot we report the dominant thread pool and whether the
 * work was query-driven or background.
 */
export const GET_CORE_TIMELINE = `
SELECT
    toUInt32(cpu_id) AS core,
    toStartOfInterval(event_time_microseconds, INTERVAL 100000 microsecond) AS slot,
    thread_name,
    query_id,
    count() AS samples,
    countIf(trace_type = 'CPU') AS cpu_samples,
    countIf(trace_type = 'Real') AS real_samples
FROM {{cluster_aware:system.trace_log}}
WHERE event_time >= now() - INTERVAL {window_seconds:UInt32} SECOND
  AND trace_type IN ('CPU', 'Real')
GROUP BY core, slot, thread_name, query_id
ORDER BY core, slot, samples DESC
`;

/** Fallback for servers without thread_name column */
export const GET_CORE_TIMELINE_FALLBACK = `
SELECT
    toUInt32(cpu_id) AS core,
    toStartOfInterval(event_time_microseconds, INTERVAL 100000 microsecond) AS slot,
    CAST(thread_id, 'String') AS thread_name,
    query_id,
    count() AS samples,
    countIf(trace_type = 'CPU') AS cpu_samples,
    countIf(trace_type = 'Real') AS real_samples
FROM {{cluster_aware:system.trace_log}}
WHERE event_time >= now() - INTERVAL {window_seconds:UInt32} SECOND
  AND trace_type IN ('CPU', 'Real')
GROUP BY core, slot, thread_name, query_id
ORDER BY core, slot, samples DESC
`;

/**
 * Fallback for ClickHouse Cloud where cpu_id is not available.
 * Uses thread_id modulo a synthetic core count to distribute samples
 * across virtual "lanes" so the swimlane visualization still works.
 * The core count is estimated from the number of distinct thread_ids
 * capped at 64 to keep the visualization readable.
 */
export const GET_CORE_TIMELINE_NO_CPU_ID = `
SELECT
    toUInt32(thread_id % {core_count:UInt32}) AS core,
    toStartOfInterval(event_time_microseconds, INTERVAL 100000 microsecond) AS slot,
    thread_name,
    query_id,
    count() AS samples,
    countIf(trace_type = 'CPU') AS cpu_samples,
    countIf(trace_type = 'Real') AS real_samples
FROM {{cluster_aware:system.trace_log}}
WHERE event_time >= now() - INTERVAL {window_seconds:UInt32} SECOND
  AND trace_type IN ('CPU', 'Real')
GROUP BY core, slot, thread_name, query_id
ORDER BY core, slot, samples DESC
`;

/** Same as above but also without thread_name (very old CH Cloud) */
export const GET_CORE_TIMELINE_NO_CPU_ID_NO_THREAD_NAME = `
SELECT
    toUInt32(thread_id % {core_count:UInt32}) AS core,
    toStartOfInterval(event_time_microseconds, INTERVAL 100000 microsecond) AS slot,
    CAST(thread_id, 'String') AS thread_name,
    query_id,
    count() AS samples,
    countIf(trace_type = 'CPU') AS cpu_samples,
    countIf(trace_type = 'Real') AS real_samples
FROM {{cluster_aware:system.trace_log}}
WHERE event_time >= now() - INTERVAL {window_seconds:UInt32} SECOND
  AND trace_type IN ('CPU', 'Real')
GROUP BY core, slot, thread_name, query_id
ORDER BY core, slot, samples DESC
`;
