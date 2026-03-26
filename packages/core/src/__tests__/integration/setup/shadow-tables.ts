/**
 * Shadow table creation and seeding for integration tests.
 *
 * ClickHouse system tables (system.metric_log, system.asynchronous_metric_log, etc.)
 * are read-only. We create equivalent "shadow" tables in a test database with the
 * same column names our queries reference, then INSERT controlled test data.
 *
 * The approach:
 *   1. CREATE DATABASE IF NOT EXISTS test_shadow
 *   2. CREATE TABLE test_shadow.metric_log (... only the columns our queries use ...)
 *   3. INSERT known rows
 *   4. Queries are rewritten to read from test_shadow.X instead of system.X
 */

import type { ClickHouseClient } from '@clickhouse/client';

const SHADOW_DB = 'test_shadow';

// ---------------------------------------------------------------------------
// DDL for shadow tables — only the columns our queries actually reference
// ---------------------------------------------------------------------------

const METRIC_LOG_DDL = `
  CREATE TABLE IF NOT EXISTS ${SHADOW_DB}.metric_log (
    event_date Date DEFAULT toDate(event_time),
    event_time DateTime DEFAULT now(),
    -- CPU
    ProfileEvent_OSCPUVirtualTimeMicroseconds UInt64 DEFAULT 0,
    -- Memory
    CurrentMetric_MemoryTracking Int64 DEFAULT 0,
    -- Disk I/O
    ProfileEvent_OSReadBytes UInt64 DEFAULT 0,
    ProfileEvent_OSWriteBytes UInt64 DEFAULT 0,
    -- Network I/O
    ProfileEvent_NetworkSendBytes UInt64 DEFAULT 0,
    ProfileEvent_NetworkReceiveBytes UInt64 DEFAULT 0
  ) ENGINE = MergeTree()
  ORDER BY event_time
`;

const ASYNC_METRIC_LOG_DDL = `
  CREATE TABLE IF NOT EXISTS ${SHADOW_DB}.asynchronous_metric_log (
    event_date Date DEFAULT toDate(event_time),
    event_time DateTime DEFAULT now(),
    metric LowCardinality(String),
    value Float64
  ) ENGINE = MergeTree()
  ORDER BY (event_time, metric)
`;

const ASYNC_METRICS_DDL = `
  CREATE TABLE IF NOT EXISTS ${SHADOW_DB}.asynchronous_metrics (
    metric LowCardinality(String),
    value Float64
  ) ENGINE = MergeTree()
  ORDER BY metric
`;

const QUERY_LOG_DDL = `
  CREATE TABLE IF NOT EXISTS ${SHADOW_DB}.query_log (
    event_date Date DEFAULT toDate(event_time),
    event_time DateTime DEFAULT now(),
    query_id String,
    type Enum8(
      'QueryStart' = 1,
      'QueryFinish' = 2,
      'ExceptionBeforeStart' = 3,
      'ExceptionWhileProcessing' = 4
    ),
    query_kind LowCardinality(String) DEFAULT '',
    is_initial_query UInt8 DEFAULT 1,
    query_start_time DateTime DEFAULT now(),
    query_duration_ms UInt64 DEFAULT 0,
    read_rows UInt64 DEFAULT 0,
    read_bytes UInt64 DEFAULT 0,
    written_rows UInt64 DEFAULT 0,
    written_bytes UInt64 DEFAULT 0,
    result_rows UInt64 DEFAULT 0,
    result_bytes UInt64 DEFAULT 0,
    memory_usage Int64 DEFAULT 0,
    query String DEFAULT '',
    normalized_query_hash UInt64 DEFAULT 0,
    tables Array(String) DEFAULT [],
    databases Array(String) DEFAULT [],
    exception String DEFAULT '',
    exception_code Int32 DEFAULT 0,
    user String DEFAULT 'default',
    client_hostname String DEFAULT '',
    ProfileEvents Map(String, UInt64) DEFAULT map()
  ) ENGINE = MergeTree()
  ORDER BY event_time
`;

const PART_LOG_DDL = `
  CREATE TABLE IF NOT EXISTS ${SHADOW_DB}.part_log (
    event_date Date DEFAULT toDate(event_time),
    event_time DateTime DEFAULT now(),
    event_type Enum8(
      'NewPart' = 1,
      'MergeParts' = 2,
      'DownloadPart' = 3,
      'RemovePart' = 4,
      'MutatePart' = 5,
      'MovePart' = 6
    ),
    database String DEFAULT '',
    \`table\` String DEFAULT '',
    part_name String DEFAULT '',
    partition_id String DEFAULT '',
    rows UInt64 DEFAULT 0,
    size_in_bytes UInt64 DEFAULT 0,
    duration_ms UInt64 DEFAULT 0,
    merge_reason String DEFAULT '',
    merged_from Array(String) DEFAULT [],
    bytes_uncompressed UInt64 DEFAULT 0,
    read_bytes UInt64 DEFAULT 0,
    read_rows UInt64 DEFAULT 0,
    peak_memory_usage UInt64 DEFAULT 0,
    ProfileEvents Map(String, UInt64) DEFAULT map()
  ) ENGINE = MergeTree()
  ORDER BY event_time
`;

// ---------------------------------------------------------------------------
// Setup / teardown
// ---------------------------------------------------------------------------

export async function createShadowDatabase(client: ClickHouseClient): Promise<void> {
  await client.command({ query: `CREATE DATABASE IF NOT EXISTS ${SHADOW_DB}` });
  await client.command({ query: METRIC_LOG_DDL });
  await client.command({ query: ASYNC_METRIC_LOG_DDL });
  await client.command({ query: ASYNC_METRICS_DDL });
  await client.command({ query: QUERY_LOG_DDL });
  await client.command({ query: PART_LOG_DDL });
}

export async function dropShadowDatabase(client: ClickHouseClient): Promise<void> {
  await client.command({ query: `DROP DATABASE IF EXISTS ${SHADOW_DB}` });
}

export async function truncateShadowTables(client: ClickHouseClient): Promise<void> {
  for (const table of ['metric_log', 'asynchronous_metric_log', 'asynchronous_metrics', 'query_log', 'part_log']) {
    await client.command({ query: `TRUNCATE TABLE IF EXISTS ${SHADOW_DB}.${table}` });
  }
}

// ---------------------------------------------------------------------------
// Seed helpers
// ---------------------------------------------------------------------------

export interface MetricLogRow {
  event_time: string;
  cpu_us?: number;
  memory_tracking?: number;
  disk_read?: number;
  disk_write?: number;
  net_send?: number;
  net_recv?: number;
}

export async function seedMetricLog(
  client: ClickHouseClient,
  rows: MetricLogRow[],
): Promise<void> {
  for (const row of rows) {
    await client.command({
      query: `
        INSERT INTO ${SHADOW_DB}.metric_log (
          event_time,
          ProfileEvent_OSCPUVirtualTimeMicroseconds,
          CurrentMetric_MemoryTracking,
          ProfileEvent_OSReadBytes,
          ProfileEvent_OSWriteBytes,
          ProfileEvent_NetworkSendBytes,
          ProfileEvent_NetworkReceiveBytes
        ) VALUES (
          '${row.event_time}',
          ${row.cpu_us ?? 0},
          ${row.memory_tracking ?? 0},
          ${row.disk_read ?? 0},
          ${row.disk_write ?? 0},
          ${row.net_send ?? 0},
          ${row.net_recv ?? 0}
        )
      `,
    });
  }
}

export interface AsyncMetricRow {
  event_time: string;
  metric: string;
  value: number;
}

export async function seedAsyncMetricLog(
  client: ClickHouseClient,
  rows: AsyncMetricRow[],
): Promise<void> {
  for (const row of rows) {
    await client.command({
      query: `
        INSERT INTO ${SHADOW_DB}.asynchronous_metric_log (event_time, metric, value)
        VALUES ('${row.event_time}', '${row.metric}', ${row.value})
      `,
    });
  }
}

export interface AsyncMetricStaticRow {
  metric: string;
  value: number;
}

export async function seedAsyncMetrics(
  client: ClickHouseClient,
  rows: AsyncMetricStaticRow[],
): Promise<void> {
  for (const row of rows) {
    await client.command({
      query: `
        INSERT INTO ${SHADOW_DB}.asynchronous_metrics (metric, value)
        VALUES ('${row.metric}', ${row.value})
      `,
    });
  }
}
