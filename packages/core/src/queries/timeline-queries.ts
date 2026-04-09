/**
 * SQL queries for timeline/snapshot analysis.
 * These queries fetch server metrics, query activity, merges, and mutations
 * within a time window for the Time Travel visualization.
 */

/** Max rows returned per activity type (queries, merges, mutations). */
export const TIMELINE_ACTIVITY_LIMIT = 100;

/** Minimum memory (bytes) to include an activity row — filters out noise from tiny operations. */
const MIN_MEMORY_BYTES = 1048576; // 1 MB

/** Seconds → milliseconds multiplier. */
const SEC_TO_MS = 1000;

import { APP_SOURCE_LIKE } from './source-tags.js';

/** Default metric_log sampling interval (ms) — used when the actual gap is missing or out of range. */
const DEFAULT_INTERVAL_MS = 1000;

/**
 * Assumed CPU core utilization for in-flight merges (system.merges has no ProfileEvents).
 * Multiplied by elapsed seconds to estimate cpu_us. 1.0 = full core, 0.5 = half core.
 */
export const RUNNING_MERGE_CPU_CORES = 0.5;

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

/** Server CPU timeseries from asynchronous_metric_log.
 *  Uses CGroup CPU metrics (captures ALL process CPU including background merges/mutations)
 *  instead of metric_log's ProfileEvent_OSCPUVirtualTimeMicroseconds (query threads only).
 *  In k8s/containers, prefers CGroupUserTime+CGroupSystemTime (container-scoped).
 *  Falls back to OSUserTime+OSSystemTime on bare metal.
 *  Output is in microseconds (×1e6) with interval_ms=1000 for pipeline compatibility.
 */
export const SERVER_CPU_TIMESERIES = `
  SELECT
    t,
    avg(v) AS v,
    toUInt32(${DEFAULT_INTERVAL_MS}) AS interval_ms
  FROM (
    SELECT
      toString(event_time) AS t,
      hostname() AS host,
      if(
        countIf(metric IN ('CGroupUserTime', 'CGroupSystemTime')) > 0,
        sumIf(value, metric IN ('CGroupUserTime', 'CGroupSystemTime')),
        sumIf(value, metric IN ('OSUserTime', 'OSSystemTime'))
      ) * 1000000 AS v
    FROM {{cluster_aware:system.asynchronous_metric_log}}
    WHERE metric IN ('CGroupUserTime', 'CGroupSystemTime', 'OSUserTime', 'OSSystemTime')
      AND event_time >= {start_time}
      AND event_time <= {end_time}
    GROUP BY event_time, hostname()
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
    substring(query, 1, 500) AS query_short,
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
    AND query NOT LIKE ${APP_SOURCE_LIKE}
    AND memory_usage > ${MIN_MEMORY_BYTES}
  ORDER BY {query_order_by} DESC
  LIMIT {activity_limit}
`;
/** Active queries filtered by normalized_query_hash — shows ALL executions of a query hash (no memory filter, no limit) */
export const ACTIVE_QUERIES_BY_HASH = `
  SELECT
    query_id,
    user,
    hostname() AS host,
    substring(query, 1, 500) AS query_short,
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
    AND query NOT LIKE ${APP_SOURCE_LIKE}
    AND normalized_query_hash = {normalized_query_hash}
  ORDER BY query_start_time DESC
`;

/** Count of queries active during the window (before LIMIT/memory filter of detail query) */
export const ACTIVE_QUERIES_COUNT = `
  SELECT count()
  FROM {{cluster_aware:system.query_log}}
  WHERE type IN ('QueryFinish', 'ExceptionWhileProcessing')
    AND event_date >= {start_date}
    AND query_start_time <= {end_time}
    AND (query_start_time + toIntervalMillisecond(query_duration_ms)) >= {start_time}
    AND query NOT LIKE ${APP_SOURCE_LIKE}
    AND memory_usage > ${MIN_MEMORY_BYTES}
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
    AND (peak_memory_usage > ${MIN_MEMORY_BYTES} OR event_type = 'MovePart')
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
    AND (peak_memory_usage > ${MIN_MEMORY_BYTES} OR event_type = 'MovePart')
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
    AND (peak_memory_usage > ${MIN_MEMORY_BYTES} OR event_type = 'MovePart')
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
    toUInt64(elapsed * ${SEC_TO_MS}) AS query_duration_ms,
    query_kind,
    toString(now() - toIntervalSecond(toUInt32(elapsed))) AS qst,
    ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds'] AS cpu_us,
    ProfileEvents['NetworkSendBytes'] AS net_send,
    ProfileEvents['NetworkReceiveBytes'] AS net_recv,
    ProfileEvents['OSReadBytes'] AS disk_read,
    ProfileEvents['OSWriteBytes'] AS disk_write
  FROM system.processes
  WHERE is_initial_query = 1
    AND query NOT LIKE ${APP_SOURCE_LIKE}
    AND memory_usage > ${MIN_MEMORY_BYTES}
  ORDER BY {query_order_by} DESC
  LIMIT {activity_limit}
`;

/**
 * Currently running merges from system.merges.
 * system.merges has no ProfileEvents, so cpu_us is estimated as
 * elapsed × RUNNING_MERGE_CPU_CORES. Once completed, part_log has real ProfileEvents.
 */
export const RUNNING_MERGES_TIMELINE = `
  SELECT
    result_part_name AS part_name,
    hostname() AS host,
    database || '.' || table AS tbl,
    memory_usage AS peak_memory_usage,
    toUInt64(elapsed * ${SEC_TO_MS}) AS duration_ms,
    merge_type,
    toString(now() - toIntervalSecond(toUInt32(elapsed))) AS merge_start,
    progress,
    toUInt64(elapsed * ${RUNNING_MERGE_CPU_CORES * 1_000_000}) AS cpu_us,
    bytes_read_uncompressed AS disk_read,
    bytes_written_uncompressed AS disk_write,
    is_mutation
  FROM {{cluster_aware:system.merges}}
  WHERE (memory_usage > ${MIN_MEMORY_BYTES} OR is_mutation = 1)
  ORDER BY memory_usage DESC
  LIMIT {activity_limit}
`;

/**
 * CPU spike analysis: fetch per-second CPU data with percentage calculation.
 * Uses asynchronous_metric_log for OS-level CPU (captures all process threads).
 * The spike grouping logic is done in TypeScript for flexibility.
 * On clusters, averages across hosts per timestamp.
 */
export const CPU_SPIKE_TIMESERIES = `
  SELECT
    t,
    avg(cpu_us) AS cpu_us,
    toUInt32(${DEFAULT_INTERVAL_MS}) AS interval_ms
  FROM (
    SELECT
      toString(event_time) AS t,
      hostname() AS host,
      if(
        countIf(metric IN ('CGroupUserTime', 'CGroupSystemTime')) > 0,
        sumIf(value, metric IN ('CGroupUserTime', 'CGroupSystemTime')),
        sumIf(value, metric IN ('OSUserTime', 'OSSystemTime'))
      ) * 1000000 AS cpu_us
    FROM {{cluster_aware:system.asynchronous_metric_log}}
    WHERE metric IN ('CGroupUserTime', 'CGroupSystemTime', 'OSUserTime', 'OSSystemTime')
      AND event_time >= {start_time}
      AND event_time <= {end_time}
    GROUP BY event_time, hostname()
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

/** Per-host CPU timeseries from asynchronous_metric_log (captures all process CPU) */
export const CLUSTER_CPU_TIMESERIES = `
  SELECT
    toString(event_time) AS t,
    hostname() AS host,
    if(
      countIf(metric IN ('CGroupUserTime', 'CGroupSystemTime')) > 0,
      sumIf(value, metric IN ('CGroupUserTime', 'CGroupSystemTime')),
      sumIf(value, metric IN ('OSUserTime', 'OSSystemTime'))
    ) * 1000000 AS v,
    toUInt32(${DEFAULT_INTERVAL_MS}) AS interval_ms
  FROM {{cluster_aware:system.asynchronous_metric_log}}
  WHERE metric IN ('CGroupUserTime', 'CGroupSystemTime', 'OSUserTime', 'OSSystemTime')
    AND event_time >= {start_time}
    AND event_time <= {end_time}
  GROUP BY event_time, hostname()
  ORDER BY host, event_time ASC
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

/**
 * Per-host cgroup CPU limit for Kubernetes/containerized environments.
 * CGroupMaxCPU (available since ~23.8) reflects the pod's cgroup CPU limit.
 * Returns 0 or no rows when no cgroup limit is set.
 */
export const CLUSTER_CGROUP_CPU = `
  SELECT
    hostname() AS host,
    argMax(value, event_time) AS value
  FROM {{cluster_aware:system.asynchronous_metric_log}}
  WHERE metric = 'CGroupMaxCPU'
    AND event_time >= {start_time}
    AND event_time <= {end_time}
  GROUP BY host
`;
