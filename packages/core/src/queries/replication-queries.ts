/**
 * SQL queries for the Replication Topology Map.
 * Per-table replica/shard detail, partition distribution, and engine metadata.
 */

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

/** Table engine metadata — engine_full, partition key, order key. */
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
