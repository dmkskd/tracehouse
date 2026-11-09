import { eventTimezoneFixture } from './event-timezone-fixture.js';

/** Typed, read-only log rows with the same absolute clocks in any source zone. */
export function queryTimezoneFixture(table: string, epoch: number, zone: string): string {
  if (table === 'asynchronous_metrics') return `(SELECT 'NumberOfCPUCores' AS metric, toUInt64(2) AS value UNION ALL SELECT 'OSMemoryTotal', toUInt64(1000))`;
  const base = eventTimezoneFixture(table, epoch, zone);
  return `(SELECT *,
    toDateTime(query_start_time_microseconds, '${zone}') AS query_start_time,
    query_start_time AS initial_query_start_time,
    query_start_time_microseconds AS initial_query_start_time_microseconds,
    toUInt64(7) AS result_rows, toUInt64(8) AS result_bytes,
    'fixture-client' AS client_hostname, map('fixture', 'value') AS Settings,
    toUInt8(1) AS is_initial_query, '127.0.0.1' AS initial_address,
    'default' AS current_database, 'default' AS initial_user,
    'SELECT fixture' AS formatted_query, [toUInt64(1)] AS thread_ids,
    ['fixture_column'] AS columns, ['fixture_partition'] AS partitions,
    [] AS projections, [] AS views, [] AS used_functions,
    [] AS used_aggregate_functions, [] AS used_aggregate_function_combinators,
    [] AS used_table_functions, [] AS used_storages, [] AS used_formats,
    [] AS used_dictionaries, '' AS stack_trace, 'fixture' AS client_name,
    toUInt32(1) AS client_version_major, toUInt32(2) AS client_version_minor,
    toUInt32(3) AS client_version_patch, toUInt8(1) AS interface,
    toUInt8(0) AS http_method, '' AS http_user_agent, 'None' AS query_cache_usage,
    '' AS log_comment, toUInt64(1) AS thread_id, toUInt64(3000000) AS peak_memory_usage,
    'fixture-host' AS hostname, 'String' AS data_kind,
    event_time_microseconds + INTERVAL 1 SECOND AS flush_time_microseconds, toUInt64(1000) AS timeout_milliseconds
  FROM ${base})`;
}
