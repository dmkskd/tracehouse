/**
 * Altinity Knowledge Base - diagnostic queries reproduced natively from the
 * Altinity ClickHouse KB (https://kb.altinity.com/).
 *
 * Each query maps to a specific KB article: the @source link is the article,
 * and the panel title/description deliberately echo the article's own wording
 * so a reader can map the KB page to the dashboard at a glance. The dashboard
 * groups these into one collapsible section per KB article (see dashboards.ts).
 *
 * Fidelity: the SQL mirrors the KB's queries. The only intentional differences
 * are MECHANICAL adaptations - `type = 2` -> `type = 'QueryFinish'`/`IN (2,4)`,
 * date filters -> {{time_range}}, `clusterAllReplicas('{cluster}', ...)` ->
 * {{cluster_aware:...}}, `normalizedQueryHash(query)` ->
 * lower(hex(normalized_query_hash)), LIMIT, and round()/formatReadableSize()
 * for display. Read-only only: the KB's maintenance/command-generator recipes
 * (ALTER / DROP / DETACH / FORGET / delete DDL) are intentionally excluded.
 *
 * NOTE: @meta description='...' is single-quoted by the parser, so descriptions
 * must not contain apostrophes / single quotes.
 */

import { APP_SOURCE_LIKE } from '@tracehouse/core';

const queries: string[] = [
  // ═══════════════════ Who ate my CPU ═══════════════════
  // https://kb.altinity.com/altinity-kb-setup-and-maintenance/who-ate-my-cpu/
  // The article gives four queries: merges, mutations, running queries, history.

  `-- @meta: title='Merges' group='Knowledge Base' description='Who ate my CPU - currently running merges, with completion estimate, progress, compressed size and memory usage.'
-- @source: https://kb.altinity.com/altinity-kb-setup-and-maintenance/who-ate-my-cpu/
SELECT
    hostName() AS host,
    database,
    table,
    round((elapsed * (1 / progress)) - elapsed, 2) AS estimate_sec,
    round(elapsed, 1) AS elapsed_sec,
    round(progress, 3) AS progress,
    is_mutation,
    formatReadableSize(total_size_bytes_compressed) AS size,
    formatReadableSize(memory_usage) AS memory
FROM {{cluster_aware:system.merges}}
ORDER BY elapsed DESC`,

  `-- @meta: title='Mutations' group='Knowledge Base' description='Who ate my CPU - mutations still in progress, with parts remaining to do and any latest failure reason.'
-- @source: https://kb.altinity.com/altinity-kb-setup-and-maintenance/who-ate-my-cpu/
SELECT
    hostName() AS host,
    database,
    table,
    substr(command, 1, 50) AS command,
    sum(parts_to_do) AS parts_to_do,
    anyIf(latest_fail_reason, latest_fail_reason != '') AS latest_fail_reason
FROM {{cluster_aware:system.mutations}}
WHERE NOT is_done
GROUP BY host, database, table, command
ORDER BY parts_to_do DESC`,

  `-- @meta: title='Current Processes' group='Knowledge Base' description='Who ate my CPU - initial queries currently running for more than 2 seconds.'
-- @source: https://kb.altinity.com/altinity-kb-setup-and-maintenance/who-ate-my-cpu/
SELECT
    hostName() AS host,
    round(elapsed, 1) AS elapsed_sec,
    user,
    query_id,
    substring(query, 1, 150) AS query
FROM {{cluster_aware:system.processes}}
WHERE is_initial_query AND elapsed > 2
ORDER BY elapsed DESC`,

  `-- @meta: title='Processes retrospectively' group='Knowledge Base' interval='1 DAY' description='Who ate my CPU - query shapes ranked by user CPU time (UserTimeMicroseconds) from query_log. userCPUms is summed across all runs of the same shape; cpu_per_sec is userCPUms / query_duration_ms. Click a row to see individual executions.'
-- @cell: column=userCPUms type=gauge max=max_cpu unit=ms
-- @cell: column=userCPUms type=rag green<10000 amber<100000
-- @cell: column=cpu_per_sec type=rag green<0.5 amber<1.5
-- @cell: column=executions type=gauge max=max_exec
-- @drill: on=query_hash into='CPU Query Executions'
-- @source: https://kb.altinity.com/altinity-kb-setup-and-maintenance/who-ate-my-cpu/
SELECT
    query_hash,
    database,
    executions,
    userCPUms,
    duration_ms,
    cpu_per_sec,
    heaviest_query,
    max(userCPUms) OVER () AS max_cpu,
    max(executions) OVER () AS max_exec
FROM (
    SELECT
        lower(hex(normalized_query_hash)) AS query_hash,
        current_database AS database,
        count() AS executions,
        round(sum(ProfileEvents['UserTimeMicroseconds']) / 1000, 1) AS userCPUms,
        round(sum(query_duration_ms), 1) AS duration_ms,
        round(sum(ProfileEvents['UserTimeMicroseconds']) / 1000 / greatest(sum(query_duration_ms), 1), 2) AS cpu_per_sec,
        substring(argMax(query, ProfileEvents['UserTimeMicroseconds']), 1, 120) AS heaviest_query
    FROM {{cluster_aware:system.query_log}}
    WHERE type = 'QueryFinish' AND event_time > {{time_range}}
    GROUP BY database, query_hash
    ORDER BY userCPUms DESC
    LIMIT 20
)
ORDER BY userCPUms DESC`,

  `-- @meta: title='CPU Query Executions' group='Knowledge Base' interval='1 DAY' description='Individual executions of a selected query shape - drill target from Processes retrospectively. Click a query_id for the full query.'
-- @link: on=query_id into='Query Detail by ID'
-- @source: https://kb.altinity.com/altinity-kb-setup-and-maintenance/who-ate-my-cpu/
SELECT
    query_id,
    event_time,
    user,
    query_duration_ms,
    round(ProfileEvents['UserTimeMicroseconds'] / 1000, 1) AS userCPUms,
    read_rows,
    formatReadableSize(read_bytes) AS read_bytes,
    substring(query, 1, 200) AS query_text
FROM {{cluster_aware:system.query_log}}
WHERE type = 'QueryFinish'
  AND lower(hex(normalized_query_hash)) = {{drill_value:query_hash | ''}}
  AND event_time > {{time_range}}
ORDER BY userCPUms DESC
LIMIT 50`,

  // ═══════════════════ Who ate my ClickHouse memory? ═══════════════════
  // https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-who-ate-my-memory/

  `-- @meta: title='Memory breakdown' group='Knowledge Base' description='Who ate my ClickHouse memory? - the main breakdown of memory across every consumer (OS, caches, queries, merges, primary keys, dictionaries, etc.). The inner query is the KB query (group / name / val bytes); the outer wrapper adds a readable size and an inline bar relative to the largest consumer. The KB SYSTEM JEMALLOC PURGE preamble is omitted (maintenance command; panels run a single read-only SELECT).'
-- @cell: column=val type=gauge max=max_val
-- @source: https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-who-ate-my-memory/
SELECT \`group\`, name, formatReadableSize(val) AS readable, val, max(val) OVER () AS max_val
FROM (
SELECT 'OS' AS \`group\`, metric AS name, toInt64(value) AS val FROM system.asynchronous_metrics WHERE metric LIKE 'OSMemory%'
    UNION ALL
SELECT 'Caches' AS \`group\`, metric AS name, toInt64(value) FROM system.asynchronous_metrics WHERE metric LIKE '%CacheBytes'
    UNION ALL
SELECT 'Caches' AS \`group\`, metric AS name, toInt64(value) FROM system.metrics WHERE metric LIKE '%CacheBytes'
    UNION ALL
SELECT 'MMaps' AS \`group\`, metric AS name, toInt64(value) FROM system.metrics WHERE metric LIKE 'MMappedFileBytes'
    UNION ALL
SELECT 'Process' AS \`group\`, metric AS name, toInt64(value) FROM system.asynchronous_metrics WHERE metric LIKE 'Memory%'
    UNION ALL
SELECT 'MemoryTable', engine AS name, toInt64(sum(total_bytes)) FROM system.tables WHERE engine IN ('Join','Memory','Buffer','Set') GROUP BY engine
    UNION ALL
SELECT 'StorageBuffer' AS \`group\`, metric AS name, toInt64(value) FROM system.metrics WHERE metric='StorageBufferBytes'
    UNION ALL
SELECT 'Queries' AS \`group\`, left(query,7) AS name, toInt64(sum(memory_usage)) FROM system.processes GROUP BY name
    UNION ALL
SELECT 'Dictionaries' AS \`group\`, type AS name, toInt64(sum(bytes_allocated)) FROM system.dictionaries GROUP BY name
    UNION ALL
SELECT 'PrimaryKeys' AS \`group\`, 'db:'||database AS name, toInt64(sum(primary_key_bytes_in_memory_allocated)) FROM system.parts GROUP BY name
    UNION ALL
SELECT 'Merges' AS \`group\`, 'db:'||database AS name, toInt64(sum(memory_usage)) FROM system.merges GROUP BY name
    UNION ALL
SELECT 'InMemoryParts' AS \`group\`, 'db:'||database AS name, toInt64(sum(data_uncompressed_bytes)) FROM system.parts WHERE part_type = 'InMemory' GROUP BY name
    UNION ALL
SELECT 'AsyncInserts' AS \`group\`, 'db:'||database AS name, toInt64(sum(total_bytes)) FROM system.asynchronous_inserts GROUP BY name
    UNION ALL
SELECT 'FileBuffersVirtual' AS \`group\`, metric AS name, toInt64(value * 2*1024*1024) FROM system.metrics WHERE metric LIKE 'OpenFileFor%'
    UNION ALL
SELECT 'ThreadStacksVirual' AS \`group\`, metric AS name, toInt64(value * 8*1024*1024) FROM system.metrics WHERE metric = 'GlobalThread'
    UNION ALL
SELECT 'UserMemoryTracking' AS \`group\`, user AS name, toInt64(memory_usage) FROM system.user_processes
    UNION ALL
SELECT 'QueryCacheBytes' AS \`group\`, '', toInt64(sum(result_size)) FROM system.query_cache
    UNION ALL
SELECT 'MemoryTracking' AS \`group\`, 'total' AS name, toInt64(value) FROM system.metrics WHERE metric = 'MemoryTracking'
)
ORDER BY val DESC`,

  `-- @meta: title='Top queries by peak memory' group='Knowledge Base' description='Who ate my ClickHouse memory? - currently running queries sorted by peak memory usage. peak_mb (numeric MB) carries an inline bar relative to the biggest and is coloured by size; the KB columns are kept alongside.'
-- @link: on=query_id into='Query Detail by ID'
-- @cell: column=peak_mb type=gauge max=max_peak_mb
-- @cell: column=peak_mb type=rag green<100 amber<1000
-- @source: https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-who-ate-my-memory/
SELECT
    hostName() AS host,
    initial_query_id AS query_id,
    elapsed,
    formatReadableSize(memory_usage) AS memory,
    formatReadableSize(peak_memory_usage) AS peak_memory,
    round(peak_memory_usage / 1048576, 1) AS peak_mb,
    max(round(peak_memory_usage / 1048576, 1)) OVER () AS max_peak_mb,
    query
FROM {{cluster_aware:system.processes}}
ORDER BY peak_memory_usage DESC
LIMIT 10`,

  `-- @meta: title='Top memory queries (history)' group='Knowledge Base' interval='1 DAY' description='Who ate my ClickHouse memory? - completed queries from query_log sorted by memory usage.'
-- @link: on=query_id into='Query Detail by ID'
-- @source: https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-who-ate-my-memory/
SELECT
    type,
    event_time,
    initial_query_id AS query_id,
    formatReadableSize(memory_usage) AS memory,
    memory_usage,
    query
FROM {{cluster_aware:system.query_log}}
WHERE event_time > {{time_range}}
ORDER BY memory_usage DESC
LIMIT 10`,

  `-- @meta: title='RAM peaks by hour (retrospection)' group='Knowledge Base' interval='1 DAY' description='Who ate my ClickHouse memory? - retrospection analysis of RAM usage from part_log, query_log and query_views_log: peak RAM per hour, broken down by operation type (inserts, selects, merges, mutations, etc.).'
-- @source: https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-who-ate-my-memory/
WITH
    {{time_range}} AS min_time,
    now() AS max_time,
    INTERVAL 1 HOUR AS time_frame_size
SELECT
    toStartOfInterval(event_timestamp, time_frame_size) AS timeframe,
    formatReadableSize(max(mem_overall)) AS peak_ram,
    formatReadableSize(maxIf(mem_by_type, event_type = 'Insert')) AS inserts_ram,
    formatReadableSize(maxIf(mem_by_type, event_type = 'Select')) AS selects_ram,
    formatReadableSize(maxIf(mem_by_type, event_type = 'MergeParts')) AS merge_ram,
    formatReadableSize(maxIf(mem_by_type, event_type = 'MutatePart')) AS mutate_ram,
    formatReadableSize(maxIf(mem_by_type, event_type = 'Alter')) AS alter_ram,
    formatReadableSize(maxIf(mem_by_type, event_type = 'Create')) AS create_ram,
    formatReadableSize(maxIf(mem_by_type, event_type NOT IN ('Insert', 'Select', 'MergeParts', 'MutatePart', 'Alter', 'Create'))) AS other_types_ram,
    groupUniqArrayIf(event_type, event_type NOT IN ('Insert', 'Select', 'MergeParts', 'MutatePart', 'Alter', 'Create')) AS other_types
FROM (
    SELECT
        toDateTime(toUInt32(ts)) AS event_timestamp,
        t AS event_type,
        sum(mem) OVER (PARTITION BY t ORDER BY ts) AS mem_by_type,
        sum(mem) OVER (ORDER BY ts) AS mem_overall
    FROM (
        WITH arrayJoin([(toFloat64(event_time_microseconds) - (duration_ms / 1000), toInt64(peak_memory_usage)), (toFloat64(event_time_microseconds), -peak_memory_usage)]) AS data
        SELECT CAST(event_type, 'LowCardinality(String)') AS t, data.1 AS ts, data.2 AS mem
        FROM {{cluster_aware:system.part_log}}
        WHERE event_time BETWEEN min_time AND max_time AND peak_memory_usage != 0
        UNION ALL
        WITH arrayJoin([(toFloat64(query_start_time_microseconds), toInt64(memory_usage)), (toFloat64(event_time_microseconds), -memory_usage)]) AS data
        SELECT query_kind, data.1 AS ts, data.2 AS mem
        FROM {{cluster_aware:system.query_log}}
        WHERE event_time BETWEEN min_time AND max_time AND memory_usage != 0
        UNION ALL
        WITH arrayJoin([(toFloat64(event_time_microseconds) - (view_duration_ms / 1000), toInt64(peak_memory_usage)), (toFloat64(event_time_microseconds), -peak_memory_usage)]) AS data
        SELECT CAST(toString(view_type) || 'View', 'LowCardinality(String)') AS t, data.1 AS ts, data.2 AS mem
        FROM {{cluster_aware:system.query_views_log}}
        WHERE event_time BETWEEN min_time AND max_time AND peak_memory_usage != 0
    )
)
GROUP BY timeframe
ORDER BY timeframe`,

  // ═══════════════════ Threads ═══════════════════
  // https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-threads/

  `-- @meta: title='Thread pool limits (settings)' group='Knowledge Base' description='Threads - the static, configured pool sizes from system.settings (the limits). Compare these against the current usage panel.'
-- @source: https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-threads/
SELECT name, value, description
FROM system.settings
WHERE name LIKE '%pool%'
ORDER BY name ASC`,

  `-- @meta: title='Thread pool usage (current)' group='Knowledge Base' description='Threads - the live, in-use values aggregated from system.metrics and system.asynchronous_metrics: background pool tasks and current / async thread metrics, tagged by source.'
-- @source: https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-threads/
SELECT 'Background pool tasks' AS category, metric AS name, toString(value) AS value, description
FROM system.metrics WHERE metric LIKE 'Background%'
UNION ALL
SELECT 'Thread metrics (current)' AS category, metric AS name, toString(value) AS value, description
FROM system.metrics WHERE lower(metric) LIKE '%thread%'
UNION ALL
SELECT 'Thread metrics (async)' AS category, metric AS name, toString(value) AS value, description
FROM system.asynchronous_metrics WHERE lower(metric) LIKE '%thread%'
ORDER BY category ASC, name ASC`,

  `-- @meta: title='Threads used by running queries' group='Knowledge Base' description='Threads - thread count of each in-flight query (length of thread_ids). Find queries fanning out across many threads.'
-- @source: https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-threads/
SELECT
    hostName() AS host,
    query_id,
    user,
    length(thread_ids) AS threads,
    round(elapsed, 1) AS elapsed_sec,
    formatReadableSize(memory_usage) AS memory,
    substring(query, 1, 100) AS query
FROM {{cluster_aware:system.processes}}
WHERE is_initial_query
ORDER BY threads DESC
LIMIT 30`,

  // ═══════════════════ cgroups and k8s ═══════════════════
  // https://kb.altinity.com/altinity-kb-setup-and-maintenance/cgroups_k8s/

  `-- @meta: title='max_threads detection' group='Knowledge Base' description='cgroups and k8s - the KB checks max_threads from system.settings; we also surface NumberOfPhysicalCPUCores for context. Under cgroup/k8s limits cores can be mis-detected, deflating CPU parallelism.'
-- @source: https://kb.altinity.com/altinity-kb-setup-and-maintenance/cgroups_k8s/
SELECT 'max_threads' AS setting, toString(value) AS value FROM system.settings WHERE name = 'max_threads'
UNION ALL SELECT 'NumberOfPhysicalCPUCores', toString(toInt64(value)) FROM system.asynchronous_metrics WHERE metric = 'NumberOfPhysicalCPUCores'`,

  // ═══════════════════ ClickHouse Monitoring ═══════════════════
  // https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-monitoring/
  // Reproduces the SQL checks from "Minimum necessary set of checks" that read
  // system.metrics / system.asynchronous_metrics / count(). The system.events
  // error counters from the same list live in the Error Events panel below. The
  // curl / shell / cross-node checks and the ZooKeeper-availability probe (which
  // throws without Keeper configured) are omitted.

  `-- @meta: title='Minimum necessary set of checks' group='Knowledge Base' description='ClickHouse Monitoring - the non-event single-value health checks the KB recommends (metrics and counts). The system.events error counters are in the Error Events panel.'
-- @source: https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-monitoring/
SELECT 'Running queries' AS check, toString(coalesce((SELECT value FROM system.metrics WHERE metric = 'Query'), 0)) AS value
UNION ALL SELECT 'Read-only replicas', toString(coalesce((SELECT value FROM system.metrics WHERE metric = 'ReadonlyReplica'), 0))
UNION ALL SELECT 'Delayed inserts', toString(coalesce((SELECT value FROM system.metrics WHERE metric = 'DelayedInserts'), 0))
UNION ALL SELECT 'Stuck replication tasks', toString((SELECT count() FROM system.replication_queue WHERE num_tries > 100 OR num_postponed > 1000))
UNION ALL SELECT 'Detached parts', toString((SELECT count() FROM system.detached_parts))
UNION ALL SELECT 'Max parts per partition', toString(coalesce((SELECT value FROM system.asynchronous_metrics WHERE metric = 'MaxPartCountForPartition'), 0))
UNION ALL SELECT 'Distributed files to insert', toString(coalesce((SELECT value FROM system.metrics WHERE metric = 'DistributedFilesToInsert'), 0))
UNION ALL SELECT 'Dictionary exceptions', toString((SELECT count() FROM system.dictionaries WHERE last_exception != ''))
UNION ALL SELECT 'Uptime (sec)', toString(toInt64(coalesce((SELECT value FROM system.asynchronous_metrics WHERE metric = 'Uptime'), 0)))`,

  `-- @meta: title='Error Events' group='Knowledge Base' description='ClickHouse Monitoring - all the system.events error counters from the KB checks (data loss, data-differs, rejected inserts, ZooKeeper / distributed connection failures, failed fetches). Shown only when non-zero; an empty panel means none have fired.'
-- @chart: type=bar group_by=event value=value style=2d
-- @source: https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-monitoring/
SELECT event, value, description
FROM system.events
WHERE event IN ('ReplicatedDataLoss', 'DataAfterMergeDiffersFromReplica', 'DataAfterMutationDiffersFromReplica', 'RejectedInserts', 'ZooKeeperHardwareExceptions', 'DistributedConnectionFailTry', 'DistributedConnectionFailAtAll', 'ReplicatedPartFailedFetches')
  AND value > 0
ORDER BY value DESC`,

  // ═══════════════════ memory configuration settings ═══════════════════
  // https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-memory-configuration-settings/

  `-- @meta: title='RAM vs max_server_memory_usage' group='Knowledge Base' description='memory configuration settings - physical / cgroup RAM reported by the OS vs the configured server memory limit. ILIKE MemoryTotal surfaces the cgroup limit on k8s. Confirm the limit matches the host.'
-- @source: https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-memory-configuration-settings/
SELECT metric AS name, formatReadableSize(toUInt64(value)) AS size, toUInt64(value) AS bytes
FROM system.asynchronous_metrics WHERE metric ILIKE '%MemoryTotal%'
UNION ALL
SELECT name, formatReadableSize(toUInt64(value)) AS size, toUInt64(value) AS bytes
FROM system.server_settings WHERE name = 'max_server_memory_usage'`,

  // ═══════════════════ Handy queries for system.query_log ═══════════════════
  // https://kb.altinity.com/altinity-kb-useful-queries/query_log/

  `-- @meta: title='Most resource-intensive queries' group='Knowledge Base' interval='1 DAY' description='Handy queries for system.query_log - query shapes ranked by CPU virtual time, with the full KB resource breakdown (CPU, memory, disk, network, selected/read/written/result). GROUPING SETS adds per-host and grand-total rollups. Click a row for executions.'
-- @drill: on=query_hash into='CPU Query Executions'
-- @cell: column=OSCPUVirtualTime type=rag green<10 amber<60
-- @source: https://kb.altinity.com/altinity-kb-useful-queries/query_log/
SELECT
    hostName() AS host,
    lower(hex(normalized_query_hash)) AS query_hash,
    min(event_time) AS first_seen,
    max(event_time) AS last_seen,
    replace(substr(argMax(query, utime), 1, 80), '\\n', ' ') AS query,
    argMax(query_id, utime) AS sample_query_id,
    count() AS cnt,
    sum(query_duration_ms) / 1000 AS QueriesDurationSec,
    sum(ProfileEvents['RealTimeMicroseconds']) / 1000000 AS RealTime,
    sum(ProfileEvents['UserTimeMicroseconds'] AS utime) / 1000000 AS UserTime,
    sum(ProfileEvents['SystemTimeMicroseconds']) / 1000000 AS SystemTime,
    sum(ProfileEvents['DiskReadElapsedMicroseconds']) / 1000000 AS DiskReadTime,
    sum(ProfileEvents['DiskWriteElapsedMicroseconds']) / 1000000 AS DiskWriteTime,
    sum(ProfileEvents['NetworkSendElapsedMicroseconds']) / 1000000 AS NetworkSendTime,
    sum(ProfileEvents['NetworkReceiveElapsedMicroseconds']) / 1000000 AS NetworkReceiveTime,
    sum(ProfileEvents['ZooKeeperWaitMicroseconds']) / 1000000 AS ZooKeeperWaitTime,
    sum(ProfileEvents['OSIOWaitMicroseconds']) / 1000000 AS OSIOWaitTime,
    sum(ProfileEvents['OSCPUWaitMicroseconds']) / 1000000 AS OSCPUWaitTime,
    sum(ProfileEvents['OSCPUVirtualTimeMicroseconds']) / 1000000 AS OSCPUVirtualTime,
    formatReadableSize(sum(ProfileEvents['NetworkReceiveBytes'])) AS NetworkReceiveBytes,
    formatReadableSize(sum(ProfileEvents['NetworkSendBytes'])) AS NetworkSendBytes,
    sum(ProfileEvents['SelectedParts']) AS SelectedParts,
    sum(ProfileEvents['SelectedMarks']) AS SelectedMarks,
    sum(read_rows) AS ReadRows,
    formatReadableSize(sum(read_bytes)) AS ReadBytes,
    sum(written_rows) AS WrittenRows,
    formatReadableSize(sum(written_bytes)) AS WrittenBytes,
    sum(result_rows) AS ResultRows,
    formatReadableSize(quantile(0.97)(memory_usage)) AS MemoryUsageQ97
FROM {{cluster_aware:system.query_log}}
WHERE event_time > {{time_range}} AND type IN (2, 4)
GROUP BY
    GROUPING SETS (
        (normalized_query_hash, host),
        (host),
        ()
    )
ORDER BY OSCPUVirtualTime DESC
LIMIT 30`,

  `-- @meta: title='Most Used Functions' group='Knowledge Base' interval='1 DAY' description='Handy queries for system.query_log - functions most frequently used by completed queries in the selected window.'
-- @chart: type=bar group_by=function_name value=queries style=2d
-- @source: https://kb.altinity.com/altinity-kb-useful-queries/query_log/
SELECT
    arrayJoin(used_functions) AS function_name,
    count() AS queries,
    uniqExact(normalized_query_hash) AS query_shapes,
    round(sum(query_duration_ms) / 1000, 2) AS total_duration_sec,
    formatReadableSize(sum(read_bytes)) AS read_bytes
FROM {{cluster_aware:system.query_log}}
WHERE event_time > {{time_range}}
  AND type = 'QueryFinish'
  AND notEmpty(used_functions)
GROUP BY function_name
ORDER BY queries DESC
LIMIT 50`,

  // The KB query counts every query_log columns entry. Over a TraceHouse
  // connection most entries are TraceHouse polling system tables, so the panel
  // excludes its own queries, system and _table_function columns, and reports
  // each column against the number of selects that read its table.
  `-- @meta: title='Most Selected Columns' group='Knowledge Base' interval='1 DAY' description='Handy queries for system.query_log - for each user table, the columns its SELECT queries read and the percent of those queries that read each column. Low percentages mark rarely read columns; high ones are candidates for ORDER BY or skip indexes. TraceHouse queries are excluded. Uses the Table filter; empty means all tables.'
-- @cell: column=pct_of_table_queries type=gauge max=100
-- @source: https://kb.altinity.com/altinity-kb-useful-queries/query_log/
WITH {{drill_value:tbl | ''}} AS target_table
SELECT
    c.table_name AS table_name,
    c.column_name AS column_name,
    c.queries AS queries,
    round(100 * c.queries / t.table_queries, 1) AS pct_of_table_queries,
    c.query_shapes AS query_shapes,
    t.table_queries AS table_queries
FROM (
    SELECT
        concat(parts[1], '.', parts[2]) AS table_name,
        arrayStringConcat(arraySlice(parts, 3), '.') AS column_name,
        uniqExact(query_id) AS queries,
        uniqExact(normalized_query_hash) AS query_shapes
    FROM (
        SELECT query_id, normalized_query_hash, splitByChar('.', col) AS parts
        FROM {{cluster_aware:system.query_log}}
        ARRAY JOIN columns AS col
        WHERE event_time > {{time_range}}
          AND type = 'QueryFinish'
          AND query_kind = 'Select'
          AND query NOT LIKE ${APP_SOURCE_LIKE}
    )
    WHERE parts[1] NOT IN ('system', '_table_function', 'INFORMATION_SCHEMA', 'information_schema')
      AND (target_table = '' OR concat(parts[1], '.', parts[2]) = target_table)
    GROUP BY table_name, column_name
) AS c
INNER JOIN (
    SELECT
        concat(parts[1], '.', parts[2]) AS table_name,
        uniqExact(query_id) AS table_queries
    FROM (
        SELECT query_id, splitByChar('.', col) AS parts
        FROM {{cluster_aware:system.query_log}}
        ARRAY JOIN columns AS col
        WHERE event_time > {{time_range}}
          AND type = 'QueryFinish'
          AND query_kind = 'Select'
          AND query NOT LIKE ${APP_SOURCE_LIKE}
    )
    GROUP BY table_name
) AS t ON c.table_name = t.table_name
ORDER BY t.table_queries DESC, table_name, queries DESC
LIMIT 100`,

  // Same treatment as Most Selected Columns. The WHERE/PREWHERE regex alone
  // cannot tell which table a name belongs to and also matches aliases, so its
  // names are intersected with the query_log columns array (db.table.column
  // actually read); only real columns, attributed to their own table, remain.
  `-- @meta: title='Columns Used in WHERE' group='Knowledge Base' interval='1 DAY' description='Handy queries for system.query_log - for each user table, the columns its SELECT queries filter on in WHERE or PREWHERE, and the percent of those queries that filter on each. Frequently filtered columns are candidates for the ORDER BY prefix or skip indexes. TraceHouse queries are excluded. Uses the Table filter; empty means all tables.'
-- @cell: column=pct_of_table_queries type=gauge max=100
-- @source: https://kb.altinity.com/altinity-kb-useful-queries/query_log/
WITH {{drill_value:tbl | ''}} AS target_table
SELECT
    c.table_name AS table_name,
    c.column_name AS column_name,
    c.queries AS queries,
    round(100 * c.queries / t.table_queries, 1) AS pct_of_table_queries,
    c.query_shapes AS query_shapes,
    t.table_queries AS table_queries
FROM (
    SELECT
        concat(parts[1], '.', parts[2]) AS table_name,
        replaceAll(arrayStringConcat(arraySlice(parts, 3), '.'), '\`', '') AS column_name,
        uniqExact(query_id) AS queries,
        uniqExact(normalized_query_hash) AS query_shapes
    FROM (
        SELECT
            query_id,
            normalized_query_hash,
            splitByChar('.', col) AS parts,
            arrayDistinct(arrayMap(x -> lower(x), extractAll(
                extract(query, '(?is)\\\\b(?:WHERE|PREWHERE)\\\\b(.+?)(?:\\\\bGROUP\\\\s+BY\\\\b|\\\\bORDER\\\\s+BY\\\\b|\\\\bLIMIT\\\\b|\\\\bSETTINGS\\\\b|$)'),
                '\`?([A-Za-z_][A-Za-z0-9_]*)\`?\\\\s*(?:=|!=|<>|<=|>=|<|>|IN\\\\s*\\\\(|LIKE|ILIKE|BETWEEN)'
            ))) AS predicate_columns
        FROM {{cluster_aware:system.query_log}}
        ARRAY JOIN columns AS col
        WHERE event_time > {{time_range}}
          AND type = 'QueryFinish'
          AND query_kind = 'Select'
          AND query NOT LIKE ${APP_SOURCE_LIKE}
          AND match(query, '(?i)\\\\bWHERE\\\\b|\\\\bPREWHERE\\\\b')
    )
    WHERE parts[1] NOT IN ('system', '_table_function', 'INFORMATION_SCHEMA', 'information_schema')
      AND has(predicate_columns, lower(replaceAll(arrayStringConcat(arraySlice(parts, 3), '.'), '\`', '')))
      AND (target_table = '' OR concat(parts[1], '.', parts[2]) = target_table)
    GROUP BY table_name, column_name
) AS c
INNER JOIN (
    SELECT
        concat(parts[1], '.', parts[2]) AS table_name,
        uniqExact(query_id) AS table_queries
    FROM (
        SELECT query_id, splitByChar('.', col) AS parts
        FROM {{cluster_aware:system.query_log}}
        ARRAY JOIN columns AS col
        WHERE event_time > {{time_range}}
          AND type = 'QueryFinish'
          AND query_kind = 'Select'
          AND query NOT LIKE ${APP_SOURCE_LIKE}
    )
    GROUP BY table_name
) AS t ON c.table_name = t.table_name
ORDER BY t.table_queries DESC, table_name, queries DESC
LIMIT 100`,

  `-- @meta: title='Unfinished or Failed Queries' group='Knowledge Base' interval='1 DAY' description='Handy queries for system.query_log - currently running queries plus recent query exceptions from query_log.'
-- @link: on=query_id into='Query Detail by ID'
-- @source: https://kb.altinity.com/altinity-kb-useful-queries/query_log/
SELECT *
FROM (
    SELECT
        toString('running') AS status,
        toString(query_id) AS query_id,
        toDateTime(now()) AS event_time,
        toString(user) AS user,
        toFloat64(round(elapsed * 1000, 0)) AS query_duration_ms,
        toString('') AS exception,
        toString(substring(query, 1, 180)) AS query
    FROM {{cluster_aware:system.processes}}
    WHERE is_initial_query
    UNION ALL
    SELECT
        toString(type) AS status,
        toString(query_id) AS query_id,
        toDateTime(event_time) AS event_time,
        toString(user) AS user,
        toFloat64(query_duration_ms) AS query_duration_ms,
        toString(substring(exception, 1, 160)) AS exception,
        toString(substring(query, 1, 180)) AS query
    FROM {{cluster_aware:system.query_log}}
    WHERE event_time > {{time_range}}
      AND type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing')
)
ORDER BY event_time DESC, query_duration_ms DESC
LIMIT 100`,

  `-- @meta: title='Worst Offender Query Ranks' group='Knowledge Base' interval='1 DAY' description='Handy queries for system.query_log - query shapes ranked across duration, CPU, reads, memory and failures. Lower rank values mean worse offenders.'
-- @cell: column=worst_rank type=gauge max=max_worst_rank
-- @cell: column=failures type=rag green<1 amber<5
-- @drill: on=query_hash into='CPU Query Executions'
-- @source: https://kb.altinity.com/altinity-kb-useful-queries/query_log/
SELECT
    query_hash,
    sample_query,
    executions,
    failures,
    round(total_duration_ms / 1000, 2) AS total_duration_sec,
    round(cpu_us / 1000000, 2) AS cpu_sec,
    formatReadableSize(read_bytes) AS read_bytes,
    formatReadableSize(max_memory_usage) AS max_memory,
    duration_rank,
    cpu_rank,
    read_rank,
    memory_rank,
    failure_rank,
    least(duration_rank, cpu_rank, read_rank, memory_rank, failure_rank) AS worst_rank,
    max(least(duration_rank, cpu_rank, read_rank, memory_rank, failure_rank)) OVER () AS max_worst_rank
FROM (
    SELECT
        *,
        rank() OVER (ORDER BY total_duration_ms DESC) AS duration_rank,
        rank() OVER (ORDER BY cpu_us DESC) AS cpu_rank,
        rank() OVER (ORDER BY read_bytes DESC) AS read_rank,
        rank() OVER (ORDER BY max_memory_usage DESC) AS memory_rank,
        rank() OVER (ORDER BY failures DESC) AS failure_rank
    FROM (
        SELECT
            lower(hex(normalized_query_hash)) AS query_hash,
            substring(argMax(query, query_duration_ms), 1, 180) AS sample_query,
            count() AS executions,
            countIf(type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing')) AS failures,
            sum(query_duration_ms) AS total_duration_ms,
            sum(ProfileEvents['OSCPUVirtualTimeMicroseconds']) AS cpu_us,
            sum(read_bytes) AS read_bytes,
            max(memory_usage) AS max_memory_usage
        FROM {{cluster_aware:system.query_log}}
        WHERE event_time > {{time_range}}
          AND type IN ('QueryFinish', 'ExceptionBeforeStart', 'ExceptionWhileProcessing')
          AND normalized_query_hash != 0
        GROUP BY query_hash
    )
)
ORDER BY worst_rank ASC, total_duration_ms DESC
LIMIT 50`,

  // ═══════════════════ Number of active parts in a partition ═══════════════════
  // https://kb.altinity.com/altinity-kb-useful-queries/altinity-kb-number-of-active-parts-in-a-partition/

  `-- @meta: title='Active Parts per Partition' group='Knowledge Base' description='Number of active parts in a partition - partitions with the most active parts. High counts signal merge pressure / too-many-parts risk. Click a row to list its parts.'
-- @cell: column=part_count type=rag green<20 amber<100
-- @cell: column=part_count type=gauge max=max_parts
-- @drill: on=table_key into='Active Parts by Table'
-- @source: https://kb.altinity.com/altinity-kb-useful-queries/altinity-kb-number-of-active-parts-in-a-partition/
SELECT
    database,
    table,
    concat(database, '.', table) AS table_key,
    partition,
    sum(rows) AS rows,
    count() AS part_count,
    max(count()) OVER () AS max_parts
FROM system.parts
WHERE active
GROUP BY database, table, partition
ORDER BY part_count DESC
LIMIT 20`,

  `-- @meta: title='Active Parts by Table' group='Knowledge Base' description='Individual active parts for a selected table - drill target from Active Parts per Partition.'
-- @part_link: on=name database=database table=table
-- @source: https://kb.altinity.com/altinity-kb-useful-queries/altinity-kb-number-of-active-parts-in-a-partition/
SELECT
    database,
    table,
    name,
    partition,
    rows,
    formatReadableSize(bytes_on_disk) AS size,
    level,
    part_type,
    modification_time
FROM system.parts
WHERE active
  AND concat(database, '.', table) = {{drill_value:table_key | concat(database, '.', table)}}
ORDER BY modification_time DESC
LIMIT 100`,

  // ═══════════════════ MultiDisk (JBOD) Balancing ═══════════════════
  // https://kb.altinity.com/altinity-kb-setup-and-maintenance/multidisk-jbod-balancing/
  // The KB article's only query is a MOVE PART command-generator (excluded). This
  // is a derived read-only view of the per-disk distribution that article acts on.

  `-- @meta: title='Per-Disk Data Distribution' group='Knowledge Base' description='MultiDisk (JBOD) Balancing - derived read-only view of how active data is spread across disks. Spot JBOD imbalance or a filling volume. (The KB article itself only provides a MOVE PART generator, which is excluded.)'
-- @chart: type=bar group_by=disk_name value=size_gb unit=GB style=2d
-- @source: https://kb.altinity.com/altinity-kb-setup-and-maintenance/multidisk-jbod-balancing/
SELECT
    disk_name,
    count() AS parts,
    uniqExact(partition) AS partitions,
    sum(rows) AS rows,
    round(sum(bytes_on_disk) / 1073741824, 2) AS size_gb,
    formatReadableSize(sum(bytes_on_disk)) AS size
FROM system.parts
WHERE active
GROUP BY disk_name
ORDER BY sum(bytes_on_disk) DESC`,

  // ═══════════════════ Database Size - Table - Column size ═══════════════════
  // https://kb.altinity.com/altinity-kb-useful-queries/altinity-kb-database-size-table-column-size/
  // The "part_log" section: per-second part-lifecycle rates over 30-minute buckets.

  `-- @meta: title='Part Lifecycle (30m)' group='Knowledge Base' interval='1 DAY' description='Database Size - Table - Column size - per-second part-event rates (NewPart / MergeParts / DownloadPart / RemovePart / MutatePart / MovePart) per table over 30-minute buckets. The ingestion and merge rhythm.'
-- @source: https://kb.altinity.com/altinity-kb-useful-queries/altinity-kb-database-size-table-column-size/
WITH 30 * 60 AS frame_size
SELECT
    toStartOfInterval(event_time, toIntervalSecond(frame_size)) AS t,
    database,
    table,
    round(countIf(event_type = 'NewPart') / frame_size, 4) AS new,
    round(countIf(event_type = 'MergeParts') / frame_size, 4) AS merge,
    round(countIf(event_type = 'DownloadPart') / frame_size, 4) AS dl,
    round(countIf(event_type = 'RemovePart') / frame_size, 4) AS rm,
    round(countIf(event_type = 'MutatePart') / frame_size, 4) AS mut,
    round(countIf(event_type = 'MovePart') / frame_size, 4) AS mv
FROM {{cluster_aware:system.part_log}}
WHERE event_time > {{time_range}}
GROUP BY t, database, table
ORDER BY database ASC, table ASC, t ASC`,

  // ═══════════════════ Ingestion metrics from system.part_log ═══════════════════
  // https://kb.altinity.com/altinity-kb-useful-queries/ingestion-rate-part_log/

  `-- @meta: title='Parts per Insert' group='Knowledge Base' interval='1 DAY' description='Ingestion metrics from system.part_log - per-table insert aggregation showing how many new parts each insert creates.'
-- @cell: column=max_parts_per_insert type=rag green<2 amber<10
-- @cell: column=median_parts_per_insert type=rag green<2 amber<5
-- @source: https://kb.altinity.com/altinity-kb-useful-queries/ingestion-rate-part_log/
SELECT
    database,
    table,
    time_bucket,
    max(parts_per_insert) AS max_parts_per_insert,
    median(parts_per_insert) AS median_parts_per_insert,
    count() AS inserts,
    sum(parts_per_insert) AS new_parts,
    sum(rows_per_insert) AS rows_inserted,
    round(sum(parts_per_insert) / greatest(count(), 1), 2) AS avg_parts_per_insert
FROM (
    SELECT
        database,
        table,
        toStartOfHour(event_time) AS time_bucket,
        query_id,
        count() AS parts_per_insert,
        sum(rows) AS rows_per_insert
    FROM {{cluster_aware:system.part_log}}
    WHERE event_time > {{time_range}}
      AND event_type = 'NewPart'
      AND query_id != ''
    GROUP BY database, table, time_bucket, query_id
)
GROUP BY database, table, time_bucket
ORDER BY time_bucket DESC, max_parts_per_insert DESC
LIMIT 100`,

  `-- @meta: title='Rows per Insert' group='Knowledge Base' interval='1 DAY' description='Ingestion metrics from system.part_log - row and byte density per insert and per part. Small medians point to tiny inserts.'
-- @cell: column=median_rows_per_insert type=rag green>10000 amber>1000
-- @source: https://kb.altinity.com/altinity-kb-useful-queries/ingestion-rate-part_log/
SELECT
    database,
    table,
    time_bucket,
    count() AS inserts,
    median(rows_per_insert) AS median_rows_per_insert,
    min(rows_per_insert) AS min_rows_per_insert,
    max(rows_per_insert) AS max_rows_per_insert,
    median(rows_per_part) AS median_rows_per_part,
    formatReadableSize(median(bytes_per_insert)) AS median_bytes_per_insert,
    formatReadableSize(median(bytes_per_part)) AS median_bytes_per_part
FROM (
    SELECT
        database,
        table,
        toStartOfHour(event_time) AS time_bucket,
        query_id,
        sum(rows) AS rows_per_insert,
        median(rows) AS rows_per_part,
        sum(size_in_bytes) AS bytes_per_insert,
        median(size_in_bytes) AS bytes_per_part
    FROM {{cluster_aware:system.part_log}}
    WHERE event_time > {{time_range}}
      AND event_type = 'NewPart'
      AND query_id != ''
    GROUP BY database, table, time_bucket, query_id
)
GROUP BY database, table, time_bucket
ORDER BY time_bucket DESC, median_rows_per_insert ASC
LIMIT 100`,

  `-- @meta: title='New Parts by Partition' group='Knowledge Base' interval='1 DAY' description='Ingestion metrics from system.part_log - partitions receiving the most new parts in the selected window.'
-- @chart: type=bar group_by=partition_key value=new_parts style=2d
-- @cell: column=new_parts type=rag green<60 amber<600
-- @source: https://kb.altinity.com/altinity-kb-useful-queries/ingestion-rate-part_log/
SELECT
    database,
    table,
    partition_id,
    concat(database, '.', table, '/', partition_id) AS partition_key,
    count() AS new_parts,
    round(avg(rows), 0) AS avg_rows_per_part,
    formatReadableSize(avg(size_in_bytes)) AS avg_part_size
FROM {{cluster_aware:system.part_log}}
WHERE event_time > {{time_range}}
  AND event_type = 'NewPart'
GROUP BY database, table, partition_id
ORDER BY new_parts DESC
LIMIT 50`,

  `-- @meta: title='Too Fast Inserts' group='Knowledge Base' interval='1 DAY' description='Ingestion metrics from system.part_log - new-part creation by minute. More than 60 new parts per table per minute usually indicates too many tiny inserts.'
-- @chart: type=grouped_line group_by=t value=new_parts series=table_name style=2d
-- @cell: column=new_parts type=rag green<60 amber<300
-- @source: https://kb.altinity.com/altinity-kb-useful-queries/ingestion-rate-part_log/
SELECT
    toStartOfMinute(event_time) AS t,
    concat(database, '.', table) AS table_name,
    count() AS new_parts,
    round(avg(rows), 0) AS avg_rows_per_part,
    formatReadableSize(avg(size_in_bytes)) AS avg_part_size
FROM {{cluster_aware:system.part_log}}
WHERE event_time > {{time_range}}
  AND event_type = 'NewPart'
GROUP BY t, table_name
ORDER BY t ASC, new_parts DESC
LIMIT 500`,

  // ═══════════════════ Can detached parts be dropped? ═══════════════════
  // https://kb.altinity.com/altinity-kb-useful-queries/detached-parts/

  `-- @meta: title='Detached Parts by Reason' group='Knowledge Base' description='Can detached parts be dropped? - detached parts per database, table and reason (broken, unexpected, manual detach, etc.).'
-- @source: https://kb.altinity.com/altinity-kb-useful-queries/detached-parts/
SELECT
    hostName() AS host,
    database,
    table,
    reason,
    count() AS parts
FROM {{cluster_aware:system.detached_parts}}
GROUP BY host, database, table, reason
ORDER BY parts DESC`,

  `-- @meta: title='Detached Parts Trend' group='Knowledge Base' interval='1 DAY' description='Can detached parts be dropped? - detached part counts over time (total and user-detached). A rising line points to recurring corruption or failed attaches.'
-- @chart: type=grouped_line group_by=t value=detached_parts series=metric style=2d
-- @source: https://kb.altinity.com/altinity-kb-useful-queries/detached-parts/
SELECT
    toStartOfInterval(event_time, INTERVAL 10 MINUTE) AS t,
    metric,
    round(avg(value), 1) AS detached_parts
FROM {{cluster_aware:system.asynchronous_metric_log}}
WHERE metric IN ('NumberOfDetachedParts', 'NumberOfDetachedByUserParts') AND event_time > {{time_range}}
GROUP BY t, metric
ORDER BY t ASC`,

  // ═══════════════════ How to test different compression codecs ═══════════════════
  // https://kb.altinity.com/altinity-kb-schema-design/codecs/altinity-kb-how-to-test-different-compression-codecs/

  `-- @meta: title='Compression Ratio by Table' group='Knowledge Base' description='How to test different compression codecs - uncompressed / compressed ratio per table. Low ratios may indicate poorly-ordered data or wrong codecs. Click a row for per-column detail.'
-- @chart: type=bar group_by=table value=ratio style=2d
-- @drill: on=table into='Compression by Column'
-- @source: https://kb.altinity.com/altinity-kb-schema-design/codecs/altinity-kb-how-to-test-different-compression-codecs/
SELECT
    database,
    table,
    count() AS parts,
    uniqExact(partition_id) AS partition_cnt,
    sum(rows) AS rows,
    formatReadableSize(sum(data_compressed_bytes)) AS compressed,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed,
    round(sum(data_uncompressed_bytes) / greatest(sum(data_compressed_bytes), 1), 2) AS ratio
FROM system.parts
WHERE active
GROUP BY database, table
ORDER BY sum(data_compressed_bytes) DESC
LIMIT 20`,

  `-- @meta: title='Compression by Column' group='Knowledge Base' description='How to test different compression codecs - per-column size, ratio and codec for a selected table. Drill target from Compression Ratio by Table.'
-- @source: https://kb.altinity.com/altinity-kb-schema-design/codecs/altinity-kb-how-to-test-different-compression-codecs/
SELECT
    pc.column AS column,
    any(pc.type) AS type,
    sum(pc.rows) AS rows,
    formatReadableSize(sum(pc.column_data_compressed_bytes)) AS compressed,
    formatReadableSize(sum(pc.column_data_uncompressed_bytes)) AS uncompressed,
    round(sum(pc.column_data_uncompressed_bytes) / greatest(sum(pc.column_data_compressed_bytes), 1), 2) AS ratio,
    any(c.compression_codec) AS codec
FROM system.parts_columns AS pc
LEFT JOIN system.columns AS c
    ON pc.database = c.database AND pc.table = c.table AND pc.column = c.name
WHERE pc.active AND pc.table = {{drill_value:table | ''}}
GROUP BY column
ORDER BY sum(pc.column_data_compressed_bytes) DESC
LIMIT 50`,

  // ═══════════════════ ClickHouse Replication problems ═══════════════════
  // https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-check-replication-ddl-queue/

  `-- @meta: title='Read-Only Replicas' group='Knowledge Base' description='ClickHouse Replication problems - replicas stuck in read-only mode (the is_readonly predicate the KB recovery procedure targets). Usually a lost ZooKeeper session or metadata issue.'
-- @source: https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-check-replication-ddl-queue/
SELECT
    hostName() AS host,
    database,
    table,
    replica_name,
    is_session_expired,
    absolute_delay,
    queue_size,
    zookeeper_path
FROM {{cluster_aware:system.replicas}}
WHERE is_readonly
ORDER BY database, table`,

  // ═══════════════════ Replication queue ═══════════════════
  // https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-replication-queue/

  `-- @meta: title='Replication Queue by Type' group='Knowledge Base' description='Replication queue - the queue aggregated by table and task type, with executing / postponed / error counters and max retries.'
-- @chart: type=stacked_bar group_by=table value=count_all series=type style=2d
-- @source: https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-replication-queue/
SELECT
    hostName() AS host,
    database,
    table,
    type,
    count() AS count_all,
    countIf(is_currently_executing) AS executing,
    countIf(num_postponed > 0) AS postponed,
    countIf(last_exception != '') AS errors,
    max(num_tries) AS max_tries
FROM {{cluster_aware:system.replication_queue}}
GROUP BY host, database, table, type
ORDER BY count_all DESC
LIMIT 50`,

  // ═══════════════════ DDLWorker and DDL queue problems ═══════════════════
  // https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-ddlworker/
  // The KB only shows a `SELECT * FROM system.distributed_ddl_queue` snapshot;
  // this is a bounded read-only listing of the not-yet-finished tasks.

  `-- @meta: title='Distributed DDL Queue' group='Knowledge Base' description='DDLWorker and DDL queue problems - read-only listing of ON CLUSTER DDL tasks that have not finished. A node lagging here means DDL is not fully applied across the cluster.'
-- @source: https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-ddlworker/
SELECT
    entry,
    cluster,
    host,
    status,
    query_create_time,
    exception_text,
    substring(query, 1, 120) AS query
FROM system.distributed_ddl_queue
WHERE status != 'Finished'
ORDER BY query_create_time DESC
LIMIT 50`,

  // ═══════════════════ System tables ate my disk ═══════════════════
  // https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-system-tables-eat-my-disk/

  `-- @meta: title='System Log Table Sizes' group='Knowledge Base' description='System tables ate my disk - persistent system *_log MergeTree tables ranked by active bytes on disk.'
-- @chart: type=bar group_by=log_table value=bytes_on_disk style=2d
-- @cell: column=bytes_on_disk type=gauge max=max_bytes
-- @source: https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-system-tables-eat-my-disk/
SELECT
    concat(t.database, '.', t.name) AS log_table,
    t.engine,
    coalesce(p.active_parts, 0) AS active_parts,
    coalesce(p.rows, 0) AS rows,
    coalesce(p.bytes_on_disk, 0) AS bytes_on_disk,
    max(coalesce(p.bytes_on_disk, 0)) OVER () AS max_bytes,
    formatReadableSize(coalesce(p.bytes_on_disk, 0)) AS size,
    p.min_time,
    p.max_time
FROM {{cluster_aware:system.tables}} AS t
LEFT JOIN (
    SELECT
        database,
        table AS name,
        count() AS active_parts,
        sum(rows) AS rows,
        sum(bytes_on_disk) AS bytes_on_disk,
        min(min_time) AS min_time,
        max(max_time) AS max_time
    FROM {{cluster_aware:system.parts}}
    WHERE database = 'system' AND active
    GROUP BY database, name
) AS p ON t.database = p.database AND t.name = p.name
WHERE t.database = 'system'
  AND t.engine LIKE '%MergeTree%'
  AND (endsWith(t.name, '_log') OR match(t.name, '.*_log_[0-9]+$'))
ORDER BY bytes_on_disk DESC
LIMIT 50`,

  `-- @meta: title='System Log TTL Coverage' group='Knowledge Base' description='System tables ate my disk - system log tables with detected TTL clauses and partition keys.'
-- @cell: column=has_ttl type=rag mode=text green=yes red=no
-- @source: https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-system-tables-eat-my-disk/
SELECT
    name AS log_table,
    engine,
    partition_key,
    if(positionCaseInsensitive(create_table_query, ' TTL ') > 0, 'yes', 'no') AS has_ttl,
    nullIf(extract(create_table_query, '(?is)\\\\bTTL\\\\s+(.+?)(?:\\\\s+SETTINGS\\\\b|$)'), '') AS ttl_clause
FROM {{cluster_aware:system.tables}}
WHERE database = 'system'
  AND engine LIKE '%MergeTree%'
  AND (endsWith(name, '_log') OR match(name, '.*_log_[0-9]+$'))
ORDER BY has_ttl ASC, log_table ASC`,

  `-- @meta: title='Renamed System Log Tables' group='Knowledge Base' description='System tables ate my disk - old numeric-suffixed system log tables left behind after schema changes or upgrades.'
-- @cell: column=bytes_on_disk type=gauge max=max_bytes
-- @source: https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-system-tables-eat-my-disk/
SELECT
    concat(t.database, '.', t.name) AS log_table,
    t.engine,
    coalesce(p.active_parts, 0) AS active_parts,
    coalesce(p.rows, 0) AS rows,
    coalesce(p.bytes_on_disk, 0) AS bytes_on_disk,
    max(coalesce(p.bytes_on_disk, 0)) OVER () AS max_bytes,
    formatReadableSize(coalesce(p.bytes_on_disk, 0)) AS size
FROM {{cluster_aware:system.tables}} AS t
LEFT JOIN (
    SELECT database, table AS name, count() AS active_parts, sum(rows) AS rows, sum(bytes_on_disk) AS bytes_on_disk
    FROM {{cluster_aware:system.parts}}
    WHERE database = 'system' AND active
    GROUP BY database, name
) AS p ON t.database = p.database AND t.name = p.name
WHERE t.database = 'system'
  AND t.engine LIKE '%MergeTree%'
  AND match(t.name, '.*_log_[0-9]+$')
ORDER BY bytes_on_disk DESC, log_table ASC
LIMIT 50`,

  // ═══════════════════ Debug hanging thing ═══════════════════
  // https://kb.altinity.com/altinity-kb-useful-queries/debug-hang/

  `-- @meta: title='Aggregated Stack Traces' group='Knowledge Base' description='Debug hanging thing - thread stack traces grouped by frequency (address + symbol per frame, as the KB does). The fastest way to see what a busy or hung server is doing. Requires allow_introspection_functions.'
-- @cell: column=threads type=rag green<5 amber<20
-- @source: https://kb.altinity.com/altinity-kb-useful-queries/debug-hang/
SELECT
    arrayStringConcat(arrayMap(x -> concat('0x', lower(hex(x)), '\\t', demangle(addressToSymbol(x))), trace), '\\n') AS trace_functions,
    count() AS threads
FROM system.stack_trace
GROUP BY trace_functions
ORDER BY count() DESC
LIMIT 20
SETTINGS allow_introspection_functions = 1`,

  // ═══════════════════ Notes on errors (replication & distributed connections) ═══════════════════
  // https://kb.altinity.com/altinity-kb-useful-queries/connection-issues-distributed-parts/

  `-- @meta: title='Cluster connectivity probe' group='Knowledge Base' description='Notes on errors (replication and distributed connections) - the KB Check Cluster Connectivity probe: actively fans out to every replica and counts how many respond.'
-- @source: https://kb.altinity.com/altinity-kb-useful-queries/connection-issues-distributed-parts/
SELECT count() AS responding_replicas
FROM {{cluster_aware:system.one}}`,

  `-- @meta: title='Errors by Type' group='Knowledge Base' interval='1 HOUR' description='Notes on errors (replication and distributed connections) - Check for Errors: recent error counters (last hour) with the last message seen for each, per host.'
-- @chart: type=stacked_bar group_by=name value=count series=host style=2d
-- @source: https://kb.altinity.com/altinity-kb-useful-queries/connection-issues-distributed-parts/
SELECT
    hostName() AS host,
    name,
    value AS count,
    last_error_time,
    substring(last_error_message, 1, 120) AS last_message
FROM {{cluster_aware:system.errors}}
WHERE last_error_time > {{time_range}}
ORDER BY value DESC
LIMIT 30`,

  `-- @meta: title='Cluster Connectivity' group='Knowledge Base' description='Notes on errors (replication and distributed connections) - Check for Errors: cluster nodes reporting connection errors or slowdowns, per host. Points at DNS / network / down replicas.'
-- @source: https://kb.altinity.com/altinity-kb-useful-queries/connection-issues-distributed-parts/
SELECT
    hostName() AS host,
    cluster,
    host_name,
    port,
    errors_count,
    slowdowns_count,
    estimated_recovery_time
FROM {{cluster_aware:system.clusters}}
WHERE errors_count > 0 OR slowdowns_count > 0
ORDER BY errors_count DESC`,

  // ═══════════════════ Mutations ═══════════════════
  // https://kb.altinity.com/altinity-kb-queries-and-syntax/mutations/
  // The KB selects hostname(), * - the panel keeps the columns needed to tell
  // whether a mutation finished on every node and why it is stuck.

  `-- @meta: title='Mutations on All Nodes' group='Knowledge Base' description='Mutations - was an ALTER TABLE DELETE/UPDATE ON CLUSTER mutation finished on all the nodes? Unfinished mutations first, with parts left to do and the latest failure reason per host.'
-- @cell: column=parts_to_do type=rag green<1 amber<100
-- @source: https://kb.altinity.com/altinity-kb-queries-and-syntax/mutations/
SELECT
    hostName() AS host,
    database,
    table,
    mutation_id,
    substring(command, 1, 120) AS command,
    create_time,
    is_done,
    parts_to_do,
    is_killed,
    latest_fail_time,
    latest_fail_reason
FROM {{cluster_aware:system.mutations}}
ORDER BY is_done ASC, create_time DESC
LIMIT 200`,

  `-- @meta: title='Mutation Settings' group='Knowledge Base' description='Mutations - MergeTree settings that control the parallelism of mutations.'
-- @source: https://kb.altinity.com/altinity-kb-queries-and-syntax/mutations/
SELECT name, value, changed, description
FROM system.merge_tree_settings
WHERE name LIKE '%mutation%'
ORDER BY name`,

  // ═══════════════════ What are my TTL settings? ═══════════════════
  // https://kb.altinity.com/altinity-kb-queries-and-syntax/ttl/what-are-my-ttls/
  // The KB query uses columns('ttl_info.min') APPLY (min); the panel spells the
  // same aggregates out with readable aliases, restricts to active parts (the
  // KB also counts outdated parts waiting for removal) and adds delete_ttl_expired.

  `-- @meta: title='Parts with TTL' group='Knowledge Base' description='What are my TTL settings - partitions whose parts carry TTL info (delete, move, recompress, group by, rows where). delete_ttl_expired = 1 means the delete TTL is already in the past but the rows are still on disk, waiting for a TTL merge.'
-- @cell: column=delete_ttl_expired type=rag green=0 red=1
-- @source: https://kb.altinity.com/altinity-kb-queries-and-syntax/ttl/what-are-my-ttls/
SELECT
    hostName() AS host,
    database,
    table,
    partition_id,
    count() AS parts,
    min(delete_ttl_info_min) AS delete_ttl_min,
    max(delete_ttl_info_max) AS delete_ttl_max,
    delete_ttl_min > toDateTime(0) AND delete_ttl_min < now() AS delete_ttl_expired,
    min(move_ttl_info.min) AS move_ttl_min,
    min(recompression_ttl_info.min) AS recompression_ttl_min,
    min(group_by_ttl_info.min) AS group_by_ttl_min,
    min(rows_where_ttl_info.min) AS rows_where_ttl_min
FROM {{cluster_aware:system.parts}}
WHERE active
GROUP BY host, database, table, partition_id
HAVING min(delete_ttl_info_min) > toDateTime(0)
    OR NOT empty(min(move_ttl_info.min))
    OR NOT empty(min(recompression_ttl_info.min))
    OR NOT empty(min(group_by_ttl_info.min))
    OR NOT empty(min(rows_where_ttl_info.min))
ORDER BY min(delete_ttl_info_min)
LIMIT 100`,

  // ═══════════════════ Compare query_log for 2 intervals ═══════════════════
  // https://kb.altinity.com/altinity-kb-useful-queries/compare_query_log_for_2_intervals/
  // The KB second query, with timestamp_of_issue = the start of the selected
  // window: "after" is the selected window, "before" is the same length
  // immediately preceding it.

  `-- @meta: title='Query Shapes Changed Between Intervals' group='Knowledge Base' interval='1 DAY' description='Compare query_log for 2 intervals - query shapes whose count grew more than 10 percent, or whose read bytes or user CPU grew more than 20 percent, in the selected window (after) vs the window of the same length before it. Sorted by the read bytes increase. Click a row for executions.'
-- @drill: on=query_hash into='CPU Query Executions'
-- @source: https://kb.altinity.com/altinity-kb-useful-queries/compare_query_log_for_2_intervals/
WITH
    {{time_range}} AS timestamp_of_issue,
    event_time < timestamp_of_issue AS before,
    event_time >= timestamp_of_issue AS after
SELECT
    lower(hex(normalized_query_hash)) AS query_hash,
    substring(any(query), 1, 120) AS query_sample,
    round(quantileIf(0.9)(query_duration_ms, before)) AS duration_q90_before,
    round(quantileIf(0.9)(query_duration_ms, after)) AS duration_q90_after,
    countIf(before) AS cnt_before,
    countIf(after) AS cnt_after,
    sumIf(query_duration_ms, before) AS duration_sum_before,
    sumIf(query_duration_ms, after) AS duration_sum_after,
    sumIf(ProfileEvents['UserTimeMicroseconds'], before) AS usertime_sum_before,
    sumIf(ProfileEvents['UserTimeMicroseconds'], after) AS usertime_sum_after,
    sumIf(read_bytes, before) AS sum_read_bytes_before,
    sumIf(read_bytes, after) AS sum_read_bytes_after,
    formatReadableSize(toInt64(sum_read_bytes_after) - toInt64(sum_read_bytes_before)) AS read_bytes_delta
FROM {{cluster_aware:system.query_log}}
WHERE event_time >= timestamp_of_issue - (now() - timestamp_of_issue)
  AND type IN ('QueryFinish', 'ExceptionWhileProcessing')
GROUP BY query_hash
HAVING cnt_after > 1.1 * cnt_before
    OR sum_read_bytes_after > 1.2 * sum_read_bytes_before
    OR usertime_sum_after > 1.2 * usertime_sum_before
ORDER BY toInt64(sum_read_bytes_after) - toInt64(sum_read_bytes_before) DESC
LIMIT 50`,

  // ═══════════════════ Check table metadata in zookeeper ═══════════════════
  // https://kb.altinity.com/altinity-kb-useful-queries/table-meta-in-zookeeper/
  // The KB filters one table by name; the panel uses the dashboard Table filter
  // and covers every replicated table on the connected node when it is empty.
  // With several tables the window is partitioned per table Keeper path, so a
  // table node is never compared with the previous table's last replica.

  `-- @meta: title='Replica Metadata in ZooKeeper' group='Knowledge Base' description='Check table metadata in zookeeper - compares the metadata and columns nodes of every replica with the table node in Keeper. looks_good = 0 means a replica has diverging metadata. Uses the Table filter; empty means every replicated table on the connected node.'
-- @cell: column=looks_good type=rag green=1 red=0
-- @source: https://kb.altinity.com/altinity-kb-useful-queries/table-meta-in-zookeeper/
WITH {{drill_value:tbl | ''}} AS target_table
SELECT
    path,
    name,
    if(prev_name = name AND name != 'is_active', prev_value = value, 1) AS looks_good,
    ctime,
    mtime,
    value
FROM (
    SELECT
        name,
        path,
        ctime,
        mtime,
        value,
        lagInFrame(name) OVER w AS prev_name,
        lagInFrame(value) OVER w AS prev_value
    FROM system.zookeeper
    WHERE (path IN (
        SELECT arrayJoin(groupUniqArray(if(path LIKE '%/replicas', concat(path, '/', name), path)))
        FROM system.zookeeper
        WHERE path IN (
            SELECT arrayJoin([zookeeper_path, concat(zookeeper_path, '/replicas')])
            FROM system.replicas
            WHERE target_table = '' OR concat(database, '.', table) = target_table
        )
    )) AND (name IN ('metadata', 'columns', 'is_active'))
    WINDOW w AS (PARTITION BY splitByString('/replicas/', path)[1] ORDER BY name = 'is_active', name ASC, path ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
ORDER BY looks_good ASC, path, name`,

  // ═══════════════════ Parts consistency ═══════════════════
  // https://kb.altinity.com/altinity-kb-useful-queries/parts-consistency/
  // The KB filters one table and partition; the panel uses the Table filter,
  // scans every partition, and groups per host so replicas are not mixed.
  // The KB orphan-cleanup generator (delete commands) is excluded.

  `-- @meta: title='Missing Block Numbers' group='Knowledge Base' description='Parts consistency - gaps in block numbers between adjacent active parts of a partition, per host. A gap means blocks are missing: lost data, or an intentional DROP PART / partition operation. Uses the Table filter; empty means all non-system tables.'
-- @cell: column=missing_blocks type=rag green<1 amber<10
-- @source: https://kb.altinity.com/altinity-kb-useful-queries/parts-consistency/
WITH {{drill_value:tbl | ''}} AS target_table
SELECT
    host,
    database,
    table,
    partition_id,
    ranges.1 AS previous_part,
    ranges.2 AS next_part,
    ranges.3 AS previous_block_number,
    ranges.4 AS next_block_number,
    length(range(toUInt64(previous_block_number + 1), toUInt64(next_block_number))) AS missing_blocks
FROM
(
    WITH
        arrayPopFront(groupArray(min_block_number) AS min) AS min_adj,
        arrayPopBack(groupArray(max_block_number) AS max) AS max_adj,
        arrayFilter((x, y, z) -> (y != (z + 1)), arrayZip(arrayPopBack(groupArray(name) AS name_arr), arrayPopFront(name_arr), max_adj, min_adj), min_adj, max_adj) AS missing_ranges
    SELECT host, database, table, partition_id, missing_ranges
    FROM
    (
        SELECT *, hostName() AS host
        FROM {{cluster_aware:system.parts}}
        WHERE active AND database != 'system'
          AND (target_table = '' OR concat(database, '.', table) = target_table)
        ORDER BY min_block_number ASC
    )
    GROUP BY host, database, table, partition_id
)
ARRAY JOIN missing_ranges AS ranges
ORDER BY missing_blocks DESC
LIMIT 100`,

  `-- @meta: title='Parts in ZooKeeper Not on Disk' group='Knowledge Base' description='Parts consistency - parts registered in Keeper for a replica of the connected node but missing from its disk, older than 1 hour. These are orphan Keeper records. Runs on the connected node only (replica paths are per node).'
-- @source: https://kb.altinity.com/altinity-kb-useful-queries/parts-consistency/
SELECT zoo.p_path AS part_zoo, zoo.ctime, zoo.mtime, disk.p_path AS part_disk
FROM
(
    SELECT concat(path, '/', name) AS p_path, ctime, mtime
    FROM system.zookeeper
    WHERE path IN (SELECT concat(replica_path, '/parts') FROM system.replicas)
) zoo
LEFT JOIN
(
    SELECT concat(replica_path, '/parts/', name) AS p_path
    FROM system.parts INNER JOIN system.replicas USING (database, table)
) disk ON zoo.p_path = disk.p_path
WHERE part_disk = '' AND zoo.mtime <= now() - INTERVAL 1 HOUR
ORDER BY part_zoo
LIMIT 200`,

  // ═══════════════════ Setting the background message broker schedule pool size ═══════════════════
  // https://kb.altinity.com/altinity-kb-integrations/altinity-kb-kafka/04-operations-troubleshooting/background_message_broker_schedule_pool_size/
  // Kafka, RabbitMQ and NATS consumers share this pool; when it runs out of
  // free threads, streaming tables stop consuming.

  `-- @meta: title='Message Broker Pool Usage' group='Knowledge Base' description='Background message broker schedule pool size - current tasks vs pool size per host. free_threads = 0 means Kafka / RabbitMQ / NATS tables are starved and stop consuming.'
-- @cell: column=free_threads type=rag red=0
-- @source: https://kb.altinity.com/altinity-kb-integrations/altinity-kb-kafka/04-operations-troubleshooting/background_message_broker_schedule_pool_size/
SELECT
    hostName() AS host,
    sumIf(value, metric = 'BackgroundMessageBrokerSchedulePoolTask') AS tasks,
    sumIf(value, metric = 'BackgroundMessageBrokerSchedulePoolSize') AS pool_size,
    pool_size - tasks AS free_threads
FROM {{cluster_aware:system.metrics}}
WHERE metric IN ('BackgroundMessageBrokerSchedulePoolTask', 'BackgroundMessageBrokerSchedulePoolSize')
GROUP BY host
ORDER BY free_threads ASC`,

  `-- @meta: title='Message Broker Pool Min Free Threads' group='Knowledge Base' interval='1 DAY' description='Background message broker schedule pool size - the lowest number of free pool threads per host over the selected window, from metric_log. 0 means the pool was exhausted at some point.'
-- @cell: column=min_free_threads type=rag red=0
-- @source: https://kb.altinity.com/altinity-kb-integrations/altinity-kb-kafka/04-operations-troubleshooting/background_message_broker_schedule_pool_size/
SELECT
    hostName() AS host,
    min(CurrentMetric_BackgroundMessageBrokerSchedulePoolSize - CurrentMetric_BackgroundMessageBrokerSchedulePoolTask) AS min_free_threads
FROM {{cluster_aware:system.metric_log}}
WHERE event_time > {{time_range}}
GROUP BY host
ORDER BY min_free_threads ASC`,

  `-- @meta: title='Message Broker Pool Sizing' group='Knowledge Base' description='Background message broker schedule pool size - threads needed by the Kafka, RabbitMQ and NATS tables of the connected node (kafka_num_consumers with kafka_thread_per_consumer, else 1; 3 for RabbitMQ and NATS) and the KB recommended pool size (needed x 1.25) vs the current pool size.'
-- @source: https://kb.altinity.com/altinity-kb-integrations/altinity-kb-kafka/04-operations-troubleshooting/background_message_broker_schedule_pool_size/
WITH
    toUInt32OrDefault(extract(engine_full, 'kafka_num_consumers\\\\s*=\\\\s*(\\\\d+)')) AS kafka_num_consumers,
    extract(engine_full, 'kafka_thread_per_consumer\\\\s*=\\\\s*(\\\\d+|\\'true\\')') NOT IN ('', '0') AS kafka_thread_per_consumer,
    multiIf(
        engine = 'Kafka', if(kafka_thread_per_consumer AND kafka_num_consumers > 0, kafka_num_consumers, 1),
        engine = 'RabbitMQ', 3,
        engine = 'NATS', 3,
        0
    ) AS threads_needed
SELECT
    count() AS streaming_tables,
    sum(threads_needed) AS total_threads_needed,
    ceil(sum(threads_needed) * 1.25) AS recommended_pool_size,
    (SELECT value FROM system.metrics WHERE metric = 'BackgroundMessageBrokerSchedulePoolSize') AS current_pool_size
FROM system.tables
WHERE engine IN ('Kafka', 'RabbitMQ', 'NATS')`,
];

export default queries;
