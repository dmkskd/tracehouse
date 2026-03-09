/**
 * SQL queries for timeline/snapshot analysis.
 * These queries fetch server metrics, query activity, merges, and mutations
 * within a time window for the Time Travel visualization.
 */

/** Max rows returned per activity type (queries, merges, mutations). */
export const TIMELINE_ACTIVITY_LIMIT = 100;

/** Server memory timeseries from metric_log */
export const SERVER_MEMORY_TIMESERIES = `
  SELECT
    toString(event_time) AS t,
    avg(CurrentMetric_MemoryTracking) AS v
  FROM {{cluster_aware:system.metric_log}}
  WHERE event_time >= {start_time}
    AND event_time <= {end_time}
  GROUP BY event_time
  ORDER BY event_time ASC
`;

/** Server CPU timeseries from metric_log with interval calculation.
 *  On clusters, each host computes its own lag-based interval (PARTITION BY hostname())
 *  and the outer query averages across hosts per timestamp so the result represents
 *  average per-host CPU usage. The Y-axis ceiling is single-host capacity.
 */
export const SERVER_CPU_TIMESERIES = `
  SELECT
    t,
    avg(v) AS v,
    avg(interval_ms) AS interval_ms
  FROM (
    SELECT
      toString(event_time) AS t,
      hostname() AS host,
      ProfileEvent_OSCPUVirtualTimeMicroseconds AS v,
      if(
        dateDiff('millisecond', lagInFrame(event_time) OVER (PARTITION BY hostname() ORDER BY event_time), event_time) > 0
        AND dateDiff('millisecond', lagInFrame(event_time) OVER (PARTITION BY hostname() ORDER BY event_time), event_time) < 10000,
        dateDiff('millisecond', lagInFrame(event_time) OVER (PARTITION BY hostname() ORDER BY event_time), event_time),
        1000
      ) AS interval_ms
    FROM {{cluster_aware:system.metric_log}}
    WHERE event_time >= {start_time}
      AND event_time <= {end_time}
  )
  GROUP BY t
  ORDER BY t ASC
`;

/** Server network timeseries from metric_log */
export const SERVER_NETWORK_TIMESERIES = `
  SELECT
    toString(event_time) AS t,
    avg(ProfileEvent_NetworkSendBytes) AS send_v,
    avg(ProfileEvent_NetworkReceiveBytes) AS recv_v
  FROM {{cluster_aware:system.metric_log}}
  WHERE event_time >= {start_time}
    AND event_time <= {end_time}
  GROUP BY event_time
  ORDER BY event_time ASC
`;

/** Server disk IO timeseries from metric_log */
export const SERVER_DISK_IO_TIMESERIES = `
  SELECT
    toString(event_time) AS t,
    avg(ProfileEvent_OSReadBytes) AS read_v,
    avg(ProfileEvent_OSWriteBytes) AS write_v
  FROM {{cluster_aware:system.metric_log}}
  WHERE event_time >= {start_time}
    AND event_time <= {end_time}
  GROUP BY event_time
  ORDER BY event_time ASC
`;

/** Get total RAM from asynchronous_metric_log (per-host aware) */
export const SERVER_TOTAL_RAM = `
  SELECT
    hostname() AS host,
    argMax(value, event_time) AS value
  FROM {{cluster_aware:system.asynchronous_metric_log}}
  WHERE metric = 'OSMemoryTotal'
    AND event_time >= {start_time}
    AND event_time <= {end_time}
  GROUP BY host
`;

/** Get CPU cores from asynchronous_metric_log - prefer logical cores (per-host aware) */
export const SERVER_CPU_CORES = `
  SELECT
    hostname() AS host,
    argMax(value, event_time) AS value
  FROM {{cluster_aware:system.asynchronous_metric_log}}
  WHERE metric = 'NumberOfCPUCores'
    AND event_time >= {start_time}
    AND event_time <= {end_time}
  GROUP BY host
`;

/** Fallback: Get CPU cores from asynchronous_metrics - prefer logical cores */
export const SERVER_CPU_CORES_FALLBACK = `
  SELECT value
  FROM system.asynchronous_metrics
  WHERE metric = 'NumberOfCPUCores'
  LIMIT 1
`;

/** Fallback: Count CPU cores from OSUserTimeCPU metrics */
export const SERVER_CPU_CORES_FALLBACK2 = `
  SELECT count(DISTINCT metric)
  FROM system.asynchronous_metrics
  WHERE metric LIKE 'OSUserTimeCPU%'
`;

/**
 * Cgroup-aware CPU count for Kubernetes pods.
 * CGroupMaxCPU (available since ~23.8) reflects the pod's cgroup CPU limit.
 * Returns 0 when no cgroup limit is set.
 */
export const SERVER_CGROUP_CPU = `
  SELECT value
  FROM system.asynchronous_metrics
  WHERE metric = 'CGroupMaxCPU'
  LIMIT 1
`;

/** Fallback: max_threads reflects cgroup-detected CPU count at startup */
export const SERVER_MAX_THREADS = `
  SELECT value
  FROM system.settings
  WHERE name = 'max_threads'
  LIMIT 1
`;

/** Active queries during the window with resource usage */
export const ACTIVE_QUERIES = `
  SELECT
    query_id,
    user,
    hostname() AS host,
    substring(query, 1, 500) AS query_short, substring(query, 1, 500) AS query_short,
    memory_usage,
    query_duration_ms,
    query_kind,
    toString(query_start_time) AS qst,
    toString(query_start_time + toIntervalMillisecond(query_duration_ms)) AS qet,
    ProfileEvents['OSCPUVirtualTimeMicroseconds'] AS cpu_us,
    ProfileEvents['NetworkSendBytes'] AS net_send,
    ProfileEvents['NetworkReceiveBytes'] AS net_recv,
    read_bytes AS disk_read,
    written_bytes AS disk_write,
    type AS status,
    exception_code,
    exception
  FROM {{cluster_aware:system.query_log}}
  WHERE type IN ('QueryFinish', 'ExceptionWhileProcessing')
    AND event_date >= {start_date}
    AND query_start_time <= {end_time}
    AND (query_start_time + toIntervalMillisecond(query_duration_ms)) >= {start_time}
    AND query NOT LIKE '%source:Monitor:%'
    AND memory_usage > 1048576
  ORDER BY {query_order_by} DESC
  LIMIT {activity_limit}
`;
/** Count of queries active during the window (before LIMIT/memory filter of detail query) */
export const ACTIVE_QUERIES_COUNT = `
  SELECT count()
  FROM {{cluster_aware:system.query_log}}
  WHERE type IN ('QueryFinish', 'ExceptionWhileProcessing')
    AND event_date >= {start_date}
    AND query_start_time <= {end_time}
    AND (query_start_time + toIntervalMillisecond(query_duration_ms)) >= {start_time}
    AND query NOT LIKE '%source:Monitor:%'
    AND memory_usage > 1048576
`;

/**
 * Count and sum of active merges (includes TTL moves).
 * The 1MB memory filter keeps the timeline from flooding with tiny merges.
 * MovePart events bypass it since they're file relocations with ~0 memory.
 * TODO: consider making the memory threshold configurable via UI filter.
 */
export const ACTIVE_MERGES_COUNT = `
  SELECT count(), sum(peak_memory_usage)
  FROM {{cluster_aware:system.part_log}}
  WHERE event_type IN ('MergeParts', 'MovePart')
    AND event_date >= {start_date}
    AND (event_time - toIntervalMillisecond(duration_ms)) <= {end_time}
    AND event_time >= {start_time}
    AND (peak_memory_usage > 1048576 OR event_type = 'MovePart')
`;

/** Individual merges active during the window (includes TTL moves) */
export const ACTIVE_MERGES_DETAIL = `
  SELECT
    part_name,
    hostname() AS host,
    database || '.' || table AS tbl,
    peak_memory_usage,
    duration_ms,
    merge_reason,
    event_type,
    toString(event_time - toIntervalMillisecond(duration_ms)) AS merge_start,
    toString(event_time) AS merge_end
  FROM {{cluster_aware:system.part_log}}
  WHERE event_type IN ('MergeParts', 'MovePart')
    AND event_date >= {start_date}
    AND (event_time - toIntervalMillisecond(duration_ms)) <= {end_time}
    AND event_time >= {start_time}
    AND (peak_memory_usage > 1048576 OR event_type = 'MovePart')
  ORDER BY {merge_order_by} DESC
  LIMIT {activity_limit}
`;

/** ProfileEvents for merges (may fail on older CH versions) */
export const ACTIVE_MERGES_PROFILE = `
  SELECT
    part_name,
    ProfileEvents['OSCPUVirtualTimeMicroseconds'] AS cpu_us,
    ProfileEvents['NetworkSendBytes'] AS net_send,
    ProfileEvents['NetworkReceiveBytes'] AS net_recv,
    ProfileEvents['OSReadBytes'] AS disk_read,
    ProfileEvents['OSWriteBytes'] AS disk_write
  FROM {{cluster_aware:system.part_log}}
  WHERE event_type IN ('MergeParts', 'MovePart')
    AND event_date >= {start_date}
    AND (event_time - toIntervalMillisecond(duration_ms)) <= {end_time}
    AND event_time >= {start_time}
    AND (peak_memory_usage > 1048576 OR event_type = 'MovePart')
  ORDER BY {merge_order_by} DESC
  LIMIT {activity_limit}
`;

/** Count of active mutations */
export const ACTIVE_MUTATIONS_COUNT = `
  SELECT count()
  FROM {{cluster_aware:system.part_log}}
  WHERE event_type = 'MutatePart'
    AND event_date >= {start_date}
    AND (event_time - toIntervalMillisecond(duration_ms)) <= {end_time}
    AND event_time >= {start_time}
`;

/** Individual mutations active during the window */
export const ACTIVE_MUTATIONS_DETAIL = `
  SELECT
    part_name,
    hostname() AS host,
    database || '.' || table AS tbl,
    peak_memory_usage,
    duration_ms,
    toString(event_time - toIntervalMillisecond(duration_ms)) AS mut_start,
    toString(event_time) AS mut_end
  FROM {{cluster_aware:system.part_log}}
  WHERE event_type = 'MutatePart'
    AND event_date >= {start_date}
    AND (event_time - toIntervalMillisecond(duration_ms)) <= {end_time}
    AND event_time >= {start_time}
  ORDER BY {merge_order_by} DESC
  LIMIT {activity_limit}
`;

/** ProfileEvents for mutations */
export const ACTIVE_MUTATIONS_PROFILE = `
  SELECT
    part_name,
    ProfileEvents['OSCPUVirtualTimeMicroseconds'] AS cpu_us,
    ProfileEvents['NetworkSendBytes'] AS net_send,
    ProfileEvents['NetworkReceiveBytes'] AS net_recv,
    ProfileEvents['OSReadBytes'] AS disk_read,
    ProfileEvents['OSWriteBytes'] AS disk_write
  FROM {{cluster_aware:system.part_log}}
  WHERE event_type = 'MutatePart'
    AND event_date >= {start_date}
    AND (event_time - toIntervalMillisecond(duration_ms)) <= {end_time}
    AND event_time >= {start_time}
  ORDER BY {merge_order_by} DESC
  LIMIT {activity_limit}
`;

// =============================================================================
// IN-FLIGHT QUERIES (from virtual tables - system.processes, system.merges)
// =============================================================================

/** Currently running queries from system.processes with ProfileEvents */
export const RUNNING_QUERIES_TIMELINE = `
  SELECT
    query_id,
    user,
    hostname() AS host,
    substring(query, 1, 500) AS query_short,
    memory_usage,
    toUInt64(elapsed * 1000) AS query_duration_ms,
    query_kind,
    toString(now() - toIntervalSecond(toUInt32(elapsed))) AS qst,
    ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds'] AS cpu_us,
    ProfileEvents['NetworkSendBytes'] AS net_send,
    ProfileEvents['NetworkReceiveBytes'] AS net_recv,
    ProfileEvents['OSReadBytes'] AS disk_read,
    ProfileEvents['OSWriteBytes'] AS disk_write
  FROM system.processes
  WHERE is_initial_query = 1
    AND query NOT LIKE '%source:Monitor:%'
    AND memory_usage > 1048576
  ORDER BY {query_order_by} DESC
  LIMIT {activity_limit}
`;

/** Currently running merges from system.merges (no ProfileEvents, always sort by memory) */
export const RUNNING_MERGES_TIMELINE = `
  SELECT
    result_part_name AS part_name,
    hostname() AS host,
    database || '.' || table AS tbl,
    memory_usage AS peak_memory_usage,
    toUInt64(elapsed * 1000) AS duration_ms,
    merge_type,
    toString(now() - toIntervalSecond(toUInt32(elapsed))) AS merge_start,
    progress,
    bytes_read_uncompressed AS disk_read,
    bytes_written_uncompressed AS disk_write,
    is_mutation
  FROM {{cluster_aware:system.merges}}
  WHERE memory_usage > 1048576
  ORDER BY memory_usage DESC
  LIMIT {activity_limit}
`;

/**
 * CPU spike analysis: fetch per-second CPU data with percentage calculation.
 * Returns each metric_log row with its CPU percentage relative to all cores.
 * The spike grouping logic is done in TypeScript for flexibility.
 * On clusters, partitions lag by hostname and averages across hosts per timestamp.
 */
export const CPU_SPIKE_TIMESERIES = `
  SELECT
    t,
    avg(cpu_us) AS cpu_us,
    avg(interval_ms) AS interval_ms
  FROM (
    SELECT
      toString(event_time) AS t,
      hostname() AS host,
      ProfileEvent_OSCPUVirtualTimeMicroseconds AS cpu_us,
      if(
        dateDiff('millisecond', lagInFrame(event_time) OVER (PARTITION BY hostname() ORDER BY event_time), event_time) > 0
        AND dateDiff('millisecond', lagInFrame(event_time) OVER (PARTITION BY hostname() ORDER BY event_time), event_time) < 10000,
        dateDiff('millisecond', lagInFrame(event_time) OVER (PARTITION BY hostname() ORDER BY event_time), event_time),
        1000
      ) AS interval_ms
    FROM {{cluster_aware:system.metric_log}}
    WHERE event_time >= {start_time}
      AND event_time <= {end_time}
  )
  GROUP BY t
  ORDER BY t ASC
`;



// =============================================================================
// PER-HOST TIMESERIES (for cluster views)
// =============================================================================

/** Per-host memory timeseries */
export const CLUSTER_MEMORY_TIMESERIES = `
  SELECT
    toString(event_time) AS t,
    hostname() AS host,
    CurrentMetric_MemoryTracking AS v
  FROM {{cluster_aware:system.metric_log}}
  WHERE event_time >= {start_time}
    AND event_time <= {end_time}
  ORDER BY host, event_time ASC
`;

/** Per-host CPU timeseries */
export const CLUSTER_CPU_TIMESERIES = `
  SELECT
    t, host, v,
    if(interval_ms > 0 AND interval_ms < 10000, interval_ms, 1000) AS interval_ms
  FROM (
    SELECT
      toString(event_time) AS t,
      hostname() AS host,
      ProfileEvent_OSCPUVirtualTimeMicroseconds AS v,
      dateDiff('millisecond', lagInFrame(event_time) OVER (PARTITION BY hostname() ORDER BY event_time), event_time) AS interval_ms
    FROM {{cluster_aware:system.metric_log}}
    WHERE event_time >= {start_time}
      AND event_time <= {end_time}
    ORDER BY host, event_time ASC
  )
`;

/** Per-host network timeseries */
export const CLUSTER_NETWORK_TIMESERIES = `
  SELECT
    toString(event_time) AS t,
    hostname() AS host,
    ProfileEvent_NetworkSendBytes AS send_v,
    ProfileEvent_NetworkReceiveBytes AS recv_v
  FROM {{cluster_aware:system.metric_log}}
  WHERE event_time >= {start_time}
    AND event_time <= {end_time}
  ORDER BY host, event_time ASC
`;

/** Per-host disk IO timeseries */
export const CLUSTER_DISK_IO_TIMESERIES = `
  SELECT
    toString(event_time) AS t,
    hostname() AS host,
    ProfileEvent_OSReadBytes AS read_v,
    ProfileEvent_OSWriteBytes AS write_v
  FROM {{cluster_aware:system.metric_log}}
  WHERE event_time >= {start_time}
    AND event_time <= {end_time}
  ORDER BY host, event_time ASC
`;

/** Per-host total RAM */
export const CLUSTER_TOTAL_RAM = `
  SELECT
    hostname() AS host,
    argMax(value, event_time) AS value
  FROM {{cluster_aware:system.asynchronous_metric_log}}
  WHERE metric = 'OSMemoryTotal'
    AND event_time >= {start_time}
    AND event_time <= {end_time}
  GROUP BY host
`;

/** Per-host cgroup memory limit (0 or very large if no limit) */
export const CLUSTER_CGROUP_MEMORY = `
  SELECT
    hostname() AS host,
    argMax(value, event_time) AS value
  FROM {{cluster_aware:system.asynchronous_metric_log}}
  WHERE metric IN ('CGroupMemoryTotal', 'CGroupMemoryLimit')
    AND event_time >= {start_time}
    AND event_time <= {end_time}
  GROUP BY host
`;

/** Per-host CPU cores */
export const CLUSTER_CPU_CORES = `
  SELECT
    hostname() AS host,
    argMax(value, event_time) AS value
  FROM {{cluster_aware:system.asynchronous_metric_log}}
  WHERE metric = 'NumberOfCPUCores'
    AND event_time >= {start_time}
    AND event_time <= {end_time}
  GROUP BY host
`;
