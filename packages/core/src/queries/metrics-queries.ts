/**
 * SQL query templates for server metrics collection.
 *
 * Extracted from backend/services/metrics_collector.py. These queries
 * have no parameters so they can be used directly without buildQuery().
 */

/**
 * Collect core metrics from system.metrics and system.events.
 * Combines MemoryTracking, active task counts, disk I/O counters,
 * and server uptime into a single result set.
 */
export const METRICS_QUERY = `
  SELECT
    metric,
    toInt64(value) AS value
  FROM system.metrics
  WHERE metric IN (
    'MemoryTracking',
    'Query',
    'Merge',
    'BackgroundPoolTask'
  )

  UNION ALL

  SELECT
    event AS metric,
    toInt64(value) AS value
  FROM system.events
  WHERE event IN (
    'ReadBufferFromFileDescriptorReadBytes',
    'WriteBufferFromFileDescriptorWriteBytes'
  )

  UNION ALL

  SELECT
    'uptime' AS metric,
    toInt64(uptime()) AS value
`;

/** Get OS-level memory information from system.asynchronous_metrics. */
export const MEMORY_INFO_QUERY = `
  SELECT
    metric,
    value
  FROM system.asynchronous_metrics
  WHERE metric IN (
    'OSMemoryTotal',
    'OSMemoryAvailable',
    'OSMemoryFreePlusCached',
    'CGroupMemoryLimit',
    'CGroupMemoryTotal'
  )
`;

/** Get CPU-related metrics from system.asynchronous_metrics. */
export const CPU_METRICS_QUERY = `
  SELECT
    metric,
    value
  FROM system.asynchronous_metrics
  WHERE metric IN (
    'OSUserTime',
    'OSSystemTime',
    'OSIdleTime',
    'OSIOWaitTime',
    'LoadAverage1',
    'LoadAverage5',
    'LoadAverage15',
    'NumberOfPhysicalCPUCores'
  )
`;

// Note: Historical metrics queries are centralized in timeline-queries.ts
// Use SERVER_CPU_TIMESERIES, SERVER_MEMORY_TIMESERIES, etc. from there
