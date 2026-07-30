/**
 * SQL queries for probing ClickHouse monitoring capabilities.
 * 
 * These detect which system log tables exist, which settings are enabled,
 * and what observability features the server supports.
 */

/**
 * Probe which system log tables exist and have data.
 *
 * A cluster-wide query can only read a log table when it exists on every
 * targeted host. Keep both host counts so a partial rollout is not reported as
 * available merely because one replica returned the table from system.tables.
 */
export const PROBE_SYSTEM_LOG_TABLES = `
WITH (
  SELECT uniqExact(hostname())
  FROM {{cluster_aware:system.one}}
) AS expected_hosts
SELECT
    name,
    any(engine) AS engine,
    any(total_rows) AS total_rows,
    any(total_bytes) AS total_bytes,
    any(create_table_query) AS create_table_query,
    uniqExact(hostname()) AS available_hosts,
    expected_hosts
FROM {{cluster_aware:system.tables}}
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
    'error_log',
    'background_schedule_pool_log',
    'processors_profile_log',
    'backup_log',
    's3queue_log',
    'blob_storage_log',
    'session_log',
    'zookeeper_log',
    'zookeeper_connection_log',
    'transactions_info_log',
    'filesystem_cache_log',
    'filesystem_read_prefetches_log',
    'asynchronous_insert_log'
  )
GROUP BY name
ORDER BY name
`;

/**
 * Feature-level metric_log columns used to reconstruct replication history.
 * metric_log itself may exist on older versions without every flattened
 * CurrentMetric/ProfileEvent column, so table-level capability is insufficient.
 */
export const PROBE_METRIC_LOG_REPLICATION_COLUMNS = `
SELECT name
FROM system.columns
WHERE database = 'system'
  AND table = 'metric_log'
  AND name IN (
    'CurrentMetric_ReadonlyReplica',
    'ProfileEvent_ReplicatedDataLoss',
    'ProfileEvent_ReplicatedPartFailedFetches',
    'ProfileEvent_ReplicatedPartChecksFailed'
  )
ORDER BY name
`;

/**
 * Detect the processors_profile_log query shape on every queried host.
 *
 * The base schema covers both Pipeline timing correlation and Distributed
 * topology processor-name enrichment. Plan-step columns are optional and are
 * used only by the richer Distributed topology projection.
 */
export const PROBE_PROCESSORS_PROFILE_LOG_SCHEMA = `
SELECT
  count() AS host_count,
  countIf(has_base_schema) AS base_host_count,
  countIf(has_plan_step_schema) AS plan_step_host_count
FROM (
  SELECT
    hostName() AS hostname,
    countIf(
      database = 'system'
      AND table = 'processors_profile_log'
      AND name IN (
        'query_id',
        'initial_query_id',
        'event_date',
        'event_time_microseconds',
        'name',
        'elapsed_us',
        'input_wait_elapsed_us',
        'output_wait_elapsed_us',
        'input_rows',
        'input_bytes',
        'output_rows',
        'output_bytes'
      )
    ) = 12 AS has_base_schema,
    countIf(
      database = 'system'
      AND table = 'processors_profile_log'
      AND name IN (
        'query_id',
        'initial_query_id',
        'event_date',
        'event_time_microseconds',
        'name',
        'elapsed_us',
        'input_wait_elapsed_us',
        'output_wait_elapsed_us',
        'input_rows',
        'input_bytes',
        'output_rows',
        'output_bytes',
        'plan_step_name',
        'plan_step_description'
      )
    ) = 14 AS has_plan_step_schema
  FROM {{cluster_aware:system.columns}}
  GROUP BY hostname
)
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
    FROM {{cluster_aware:system.tables}}
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
FROM {{cluster_aware:system.tables}}
WHERE database = 'tracehouse'
  AND name IN ('processes_history', 'merges_history')
GROUP BY name
`;

/** Operational system tables whose presence is captured in one snapshot. */
export const PROBE_SYSTEM_TABLE_ACCESS_TABLES = [
  'merges',     // Merge Tracker
  'mutations',  // Merge Tracker mutations tab
  'clusters',   // Cluster page
  'replicas',   // Replication page
  'parts',      // Database Explorer parts, Analytics
  'databases',  // Database Explorer
  'processes',  // Query Monitor running queries
  // Metric / settings / operational tables consumed across analytics and dashboards.
  'metrics',                // instantaneous metric gauges
  'asynchronous_metrics',   // OS/server async gauges
  'events',                 // cumulative event counters
  'settings',               // session settings
  'server_settings',        // server config values
  'parts_columns',          // per-column part storage
  'columns',                // column metadata + codecs
  'detached_parts',         // detached parts + reasons
  'replication_queue',      // pending replication tasks
  'distributed_ddl_queue',  // ON CLUSTER DDL queue (needs Keeper)
  'stack_trace',            // live thread stacks (needs introspection)
  'errors',                 // per-error counters
  'dictionaries',           // loaded dictionaries
  'asynchronous_inserts',   // pending async insert buffers
  'user_processes',         // per-user resource usage
  'query_cache',            // cached query results
] as const;

/**
 * One successful metadata snapshot for connection-wide capability detection.
 *
 * This intentionally avoids executing the features being detected. Calling
 * demangle() or selecting from system.distributed_ddl_queue can throw expected
 * errors and pollute system.query_log. Metadata is sufficient to establish:
 * - server version;
 * - whether introspection functions exist and are enabled for this session;
 * - which operational system tables are present/visible, including the
 *   system.zookeeper signal used for Keeper configuration.
 *
 * SELECT privileges for an individual feature are verified lazily when that
 * feature is actually used.
 */
export const PROBE_CAPABILITY_SNAPSHOT = `
SELECT
  version() AS version,
  (
    SELECT count() = 2
    FROM system.functions
    WHERE name IN ('demangle', 'addressToSymbol')
  ) AS introspection_functions_present,
  (
    SELECT max(toUInt8OrZero(value))
    FROM system.settings
    WHERE name = 'allow_introspection_functions'
  ) AS introspection_enabled,
  (
    SELECT arraySort(groupUniqArray(name))
    FROM {{cluster_aware:system.tables}}
    WHERE database = 'system'
      AND name IN (${PROBE_SYSTEM_TABLE_ACCESS_TABLES.map(table => `'${table}'`).join(', ')}, 'zookeeper')
  ) AS system_tables
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
