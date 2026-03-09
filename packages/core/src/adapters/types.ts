/**
 * Shared compression config for all adapters using @clickhouse/client.
 * Enables gzip on responses — typically 10-20x reduction for JSONEachRow payloads.
 */
export const CLIENT_COMPRESSION = { response: true } as const;

export type AdapterErrorCategory = 'network' | 'authentication' | 'query' | 'timeout' | 'unknown';

export class AdapterError extends Error {
  constructor(
    message: string,
    public readonly category: AdapterErrorCategory,
    public readonly cause?: Error
  ) {
    super(message);
    this.name = 'AdapterError';
  }
}

export interface IClickHouseAdapter {
  /**
   * Execute a SQL query and return typed rows.
   * The adapter handles transport (HTTP or Grafana proxy) and
   * converts columnar response frames into row objects.
   */
  executeQuery<T extends Record<string, unknown>>(
    sql: string,
    params?: Record<string, unknown>
  ): Promise<T[]>;

  /**
   * Execute a command (DDL, SYSTEM statements) that returns no result set.
   * Optional — adapters that don't support it can omit this.
   */
  executeCommand?(sql: string): Promise<void>;

  /**
   * Execute a query that returns raw text lines (e.g. EXPLAIN).
   * Does not append FORMAT — returns each line as a string.
   * Optional — adapters that don't support it can omit this.
   * @param database - Optional database context to run the query in.
   */
  executeRawQuery?(sql: string, database?: string): Promise<string[]>;
}
