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
    any(total_bytes) AS total_bytes
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
