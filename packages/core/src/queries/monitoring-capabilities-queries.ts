/**
 * SQL queries for probing ClickHouse monitoring capabilities.
 * 
 * These detect which system log tables exist, which settings are enabled,
 * and what observability features the server supports.
 */

/**
 * Probe which system log tables exist and have data.
 * Natural key: (name). Dedup across replicas.
 */
export const PROBE_SYSTEM_LOG_TABLES = `
SELECT
    name,
    any(engine) AS engine,
    any(total_rows) AS total_rows,
    any(total_bytes) AS total_bytes,
    any(create_table_query) AS create_table_query
FROM {{cluster_metadata:system.tables}}
WHERE database = 'system'
  AND name IN (
    'text_log',
    'query_log',
    'query_thread_log',
    'query_views_log',
    'part_log',
    'trace_log',
    'opentelemetry_span_log',
    'metric_log',
    'asynchronous_metric_log',
    'crash_log',
    'processors_profile_log',
    'backup_log',
    's3queue_log',
    'blob_storage_log',
    'session_log',
    'zookeeper_log',
    'transactions_info_log',
    'filesystem_cache_log',
    'filesystem_read_prefetches_log',
    'asynchronous_insert_log'
  )
GROUP BY name
ORDER BY name
`;

/**
 * Probe relevant server settings that affect monitoring.
 * These settings control whether certain log tables are populated.
 */
export const PROBE_MONITORING_SETTINGS = `
SELECT
    name,
    value,
    changed,
    description
FROM system.settings
WHERE name IN (
    'log_queries',
    'log_queries_min_type',
    'log_query_threads',
    'log_profile_events',
    'log_processors_profiles',
    'opentelemetry_start_trace_probability',
    'opentelemetry_trace_processors',
    'log_comment',
    'send_logs_level',
    'query_profiler_cpu_time_period_ns',
    'query_profiler_real_time_period_ns',
    'allow_introspection_functions'
)
ORDER BY name
`;

/**
 * Check if ZooKeeper/Keeper is configured by checking system.tables.
 * Dedup via GROUP BY to handle clusterAllReplicas duplicates.
 */
export const PROBE_ZOOKEEPER = `
SELECT count() AS cnt
FROM (
    SELECT name
    FROM {{cluster_metadata:system.tables}}
    WHERE database = 'system' AND name = 'zookeeper'
    GROUP BY name
)
`;

/**
 * Get server version for capability gating.
 */
export const PROBE_SERVER_VERSION = `
SELECT version() AS version
`;

/**
 * Check if the CPU profiler is actually producing samples in trace_log.
 * The profiler settings can be enabled but still produce 0 samples when
 * the SYS_PTRACE capability is missing (common in Kubernetes).
 * We check for any CPU trace_type rows in the last 5 minutes as a signal.
 */
export const PROBE_CPU_PROFILER_SAMPLES = `
SELECT count() AS cnt
FROM system.trace_log
WHERE trace_type = 'CPU'
  AND event_date >= today() - 1
  AND event_time >= now() - INTERVAL 5 MINUTE
`;

/**
 * Check if tracehouse sampling tables exist (processes_history, merges_history).
 * Created by infra/scripts/setup_sampling.sh.
 */
export const PROBE_TRACEHOUSE_SAMPLING_TABLES = `
SELECT
    name,
    any(engine) AS engine,
    max(total_rows) AS total_rows,
    max(total_bytes) AS total_bytes
FROM {{cluster_metadata:system.tables}}
WHERE database = 'tracehouse'
  AND name IN ('processes_history', 'merges_history')
GROUP BY name
`;

/**
 * Probe access to operational system tables (not log tables) that pages
 * depend on. A simple `SELECT 1 FROM system.X LIMIT 0` per table.
 * Returns the table name only if the SELECT succeeds.
 *
 * We probe these individually via Promise.allSettled in the service
 * because a single failing table shouldn't block probing of the others.
 */
export const PROBE_SYSTEM_TABLE_ACCESS_TABLES = [
  'merges',     // Merge Tracker
  'mutations',  // Merge Tracker mutations tab
  'clusters',   // Cluster page
  'replicas',   // Replication page
  'parts',      // Database Explorer parts, Analytics
  'databases',  // Database Explorer
  'processes',  // Query Monitor running queries
] as const;

/**
 * Detect ClickHouse Cloud by checking for cloud-specific build options
 * or settings. Returns 1 if any cloud indicator is found.
 * 
 * Detection signals (any one is sufficient):
 * - cloud_mode_engine setting exists (Cloud-managed engine routing)
 * - display_name contains 'clickhouse-cloud' or 'clickhouse cloud'
 * - build_options contains CLICKHOUSE_CLOUD
 */
export const PROBE_CLOUD_SERVICE = `
SELECT
    (
        (SELECT count() FROM system.settings WHERE name = 'cloud_mode_engine') +
        (SELECT count() FROM system.build_options WHERE name = 'SYSTEM_SCOPE' AND value = 'cloud')
    ) > 0 AS is_cloud
`;
