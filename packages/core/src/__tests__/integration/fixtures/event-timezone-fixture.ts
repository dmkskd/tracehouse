/** Read-only, epoch-seeded typed log sources shared by SQL and real transport tests. */
export function eventTimezoneFixture(table: string, epoch: number, zone: string): string {
  const metric = table === 'metric_log';
  return `(SELECT
    toDateTime(${epoch}${metric ? ' + (toInt64(number) - 1) * 5' : ''}, '${zone}') AS event_time,
    toDate(event_time) AS event_date,
    fromUnixTimestamp64Micro(toInt64(toUnixTimestamp(event_time)) * 1000000 + 123456, '${zone}') AS event_time_microseconds,
    event_time_microseconds - INTERVAL 10 SECOND AS query_start_time_microseconds,
    'fixture-query' AS query_id, 'fixture-query' AS initial_query_id,
    toUInt64(42) AS normalized_query_hash, 'default' AS user, 'Create' AS query_kind,
    'CREATE TABLE fixture' AS query, ['default'] AS databases, ['default.fixture'] AS tables,
    '${table === 'zookeeper_connection_log' ? 'Connected' : 'QueryFinish'}' AS type,
    toUInt32(0) AS exception_code, 'fixture exception' AS exception,
    toUInt64(10000) AS query_duration_ms, toUInt64(2097152) AS memory_usage,
    toUInt64(3) AS read_rows, toUInt64(4) AS read_bytes,
    toUInt64(5) AS written_rows, toUInt64(6) AS written_bytes,
    map('OSCPUVirtualTimeMicroseconds', toUInt64(123)) AS ProfileEvents,
    'default' AS database, 'fixture' AS table, 'part-fixture' AS part_name,
    'partition' AS partition_id, 'default' AS disk_name, toUInt64(10000) AS duration_ms,
    'MergeParts' AS event_type, 'fixture-task' AS log_name,
    ${table === 'backup_log' ? "'fixture backup error'" : table === 'error_log' ? "'NOT_ENOUGH_SPACE'" : 'toUInt32(241)'} AS error,
    toUInt32(243) AS code, toUInt8(0) AS remote, toFloat64(1) AS value,
    'Uptime' AS metric,
    '${table === 'backup_log' ? 'BACKUP_CREATED' : 'ParsingError'}' AS status,
    'backup-fixture' AS id, 'NOT_ENOUGH_SPACE' AS name, 'JSONEachRow' AS format,
    'fixture-flush' AS flush_query_id, toUInt64(1) AS rows, toUInt64(200) AS bytes,
    event_time - INTERVAL 5 SECOND AS start_time, event_time AS end_time,
    toUInt64(2) AS num_files, toUInt64(300) AS total_size,
    'keeper.example' AS host, toUInt16(9181) AS port, toInt64(123) AS client_id, '' AS reason,
    toInt32(11) AS signal, 'fixture-version' AS version,
    toUInt64(100) AS CurrentMetric_MemoryTracking,
    toUInt64(2) AS CurrentMetric_Query, toUInt64(3) AS CurrentMetric_Merge,
    toUInt64(100000) AS ProfileEvent_OSCPUVirtualTimeMicroseconds,
    toUInt64(number = 1) AS CurrentMetric_ReadonlyReplica,
    toUInt64(number = 1) AS ProfileEvent_ReplicatedDataLoss,
    toUInt64(0) AS ProfileEvent_ReplicatedPartFailedFetches,
    toUInt64(0) AS ProfileEvent_ReplicatedPartChecksFailed,
    'Error' AS level, 'fixture logger' AS logger_name,
    'fixture message' AS message, 'fixture thread' AS thread_name,
    event_time AS last_error_time, 'fixture message' AS last_error_message
  FROM numbers(${metric ? 3 : 1}))`;
}

