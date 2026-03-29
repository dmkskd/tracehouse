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
FROM {{cluster_aware:system.replicas}}
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
FROM {{cluster_aware:system.databases}}
GROUP BY name
ORDER BY name
`;

// ── Replication Topology queries ──

/** Table engine metadata — engine_full, partition key, order key.
 *  Also checks if a Distributed table points to this local table. */
export const GET_TABLE_ENGINE_INFO = `
SELECT
    any(engine) AS engine,
    any(engine_full) AS engine_full,
    any(partition_key) AS partition_key,
    any(sorting_key) AS sorting_key
FROM {{cluster_aware:system.tables}}
WHERE database = {database} AND table = {table}
`;

/** Find Distributed tables that route to a given local table.
 *  Returns the sharding key expression if one exists. */
export const GET_DISTRIBUTED_FOR_TABLE = `
SELECT
    database,
    table AS dist_table,
    engine_full
FROM {{cluster_aware:system.tables}}
WHERE engine = 'Distributed'
  AND engine_full LIKE concat('%', {database}, '%', {table}, '%')
LIMIT 1
`;

/** Per-replica detail for a specific table — shows each replica's state individually
 *  (not aggregated like GET_REPLICATION_DETAIL). Used by the topology map. */
export const GET_REPLICA_TOPOLOGY = `
SELECT
    hostName() AS hostname,
    database,
    table,
    replica_name,
    zookeeper_path,
    replica_path,
    is_leader,
    is_readonly,
    is_session_expired,
    absolute_delay,
    queue_size,
    inserts_in_queue,
    merges_in_queue,
    log_pointer,
    log_max_index,
    total_replicas,
    active_replicas
FROM {{cluster_aware:system.replicas}}
WHERE database = {database} AND table = {table}
`;

/** Per-replica part stats — active parts count and total bytes grouped by replica.
 *  Used by the topology map to show data distribution across shards/replicas. */
export const GET_REPLICA_PARTS = `
SELECT
    hostName() AS hostname,
    count() AS part_count,
    sum(bytes_on_disk) AS bytes_on_disk,
    sum(rows) AS total_rows,
    uniq(partition_id) AS partition_count
FROM {{cluster_aware:system.parts}}
WHERE database = {database} AND table = {table} AND active = 1
GROUP BY hostname
`;

/** Per-shard partition distribution — shows data skew across shards.
 *  Groups by hostname+partition to show which partitions live where. */
export const GET_SHARD_DISTRIBUTION = `
SELECT
    hostName() AS hostname,
    partition_id,
    count() AS part_count,
    sum(bytes_on_disk) AS bytes_on_disk,
    sum(rows) AS total_rows
FROM {{cluster_aware:system.parts}}
WHERE database = {database} AND table = {table} AND active = 1
GROUP BY hostname, partition_id
ORDER BY hostname, partition_id
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
FROM {{cluster_aware:system.replication_queue}}
WHERE database = {database}
ORDER BY create_time ASC
`;
