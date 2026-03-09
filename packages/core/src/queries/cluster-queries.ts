/**
 * SQL queries for the Cluster overview page.
 * Shows cluster topology, node details, and replication health.
 */

/** All clusters with shard/replica counts.
 *  Shows all clusters from system.clusters. Virtual clusters created by
 *  Replicated databases are included because they represent real cluster
 *  topology that should be visible in the overview. */
export const GET_CLUSTERS = `
SELECT
    cluster,
    count() AS node_count,
    uniq(shard_num) AS shard_count,
    max(replica_num) AS max_replicas_per_shard
FROM system.clusters
GROUP BY cluster
ORDER BY node_count DESC
`;

/** Detailed node list for a specific cluster */
export const GET_CLUSTER_NODES = `
SELECT
    cluster,
    shard_num,
    shard_weight,
    replica_num,
    host_name,
    host_address,
    port,
    is_local,
    errors_count,
    slowdowns_count,
    estimated_recovery_time
FROM system.clusters
WHERE cluster = {cluster_name}
ORDER BY shard_num, replica_num
`;

/** Replication health per table. Natural key: (database, table). */
export const GET_REPLICATION_DETAIL = `
SELECT
    database,
    table,
    any(engine) AS engine,
    max(is_leader) AS is_leader,
    max(is_readonly) AS is_readonly,
    max(is_session_expired) AS is_session_expired,
    max(absolute_delay) AS absolute_delay,
    sum(queue_size) AS queue_size,
    sum(inserts_in_queue) AS inserts_in_queue,
    sum(merges_in_queue) AS merges_in_queue,
    any(log_pointer) AS log_pointer,
    any(total_replicas) AS total_replicas,
    any(active_replicas) AS active_replicas,
    any(replica_name) AS replica_name,
    uniq(zookeeper_path) AS shard_count,
    any(zookeeper_path) AS zk_path
FROM {{cluster_metadata:system.replicas}}
GROUP BY database, table
ORDER BY absolute_delay DESC, queue_size DESC
`;

/** Keeper/ZooKeeper connection status */
export const GET_KEEPER_STATUS = `
SELECT
    name,
    value
FROM system.zookeeper
WHERE path = '/'
LIMIT 1
`;

/** Keeper/ZooKeeper node connections — shows all configured keeper hosts */
export const GET_KEEPER_CONNECTIONS = `
SELECT
    host,
    port,
    index,
    connected_time,
    is_expired,
    keeper_api_version
FROM system.zookeeper_connection
ORDER BY index
`;

/** Per-host resource snapshot for cluster topology hover cards.
 *  Returns both host-level and cgroup-limited core counts so the UI
 *  can display the effective (container) cores with a cgroup badge. */
export const GET_CLUSTER_HOST_METRICS = `
SELECT
    hostName() AS hostname,
    uptime() AS uptime,
    version() AS version,
    anyIf(value, metric = 'OSMemoryTotal') AS mem_total,
    anyIf(value, metric = 'OSMemoryFreeWithoutCached') AS mem_free,
    anyIf(value, metric = 'LoadAverage1') AS load_1m,
    countIf(metric LIKE 'OSUserTimeCPU%') AS cpu_cores_host,
    anyIf(value, metric = 'CGroupMaxCPU') AS cgroup_cpu,
    anyIf(value, metric = 'NumberOfCPUCores') AS os_cpu_cores,
    greatest(anyIf(value, metric = 'CGroupMemoryTotal'), anyIf(value, metric = 'CGroupMemoryLimit')) AS cgroup_mem_limit,
    anyIf(value, metric = 'CGroupMemoryUsed') AS cgroup_mem_used
FROM {{cluster_aware:system.asynchronous_metrics}}
WHERE metric IN ('OSMemoryTotal', 'OSMemoryFreeWithoutCached', 'LoadAverage1', 'CGroupMaxCPU', 'NumberOfCPUCores', 'CGroupMemoryLimit', 'CGroupMemoryTotal', 'CGroupMemoryUsed')
   OR metric LIKE 'OSUserTimeCPU%'
GROUP BY hostname, uptime, version
`;

/** Database engine info. Natural key: (name). Dedup across replicas. */
export const GET_DATABASE_ENGINES = `
SELECT
    name,
    any(engine) AS engine,
    any(data_path) AS data_path,
    any(uuid) AS uuid
FROM {{cluster_metadata:system.databases}}
GROUP BY name
ORDER BY name
`;

/** Replication queue entries for a specific database.
 *  Shows what's actually pending in the replication queue. */
export const GET_REPLICATION_QUEUE = `
SELECT
    database,
    table,
    replica_name,
    type,
    create_time,
    source_replica,
    new_part_name,
    parts_to_merge,
    is_currently_executing,
    num_tries,
    last_attempt_time,
    last_exception,
    num_postponed,
    postpone_reason
FROM {{cluster_metadata:system.replication_queue}}
WHERE database = {database}
ORDER BY create_time ASC
`;
