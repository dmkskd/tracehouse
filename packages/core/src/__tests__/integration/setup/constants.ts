/** ClickHouse Docker image used by all integration test containers. */
export const CH_IMAGE =
  process.env.CLICKHOUSE_IMAGE ?? 'clickhouse/clickhouse-server:latest';
