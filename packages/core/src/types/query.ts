export interface QueryMetrics {
  query_id: string;
  user: string;
  query: string;
  query_kind: string;
  elapsed_seconds: number;
  memory_usage: number;
  read_rows: number;
  read_bytes: number;
  total_rows_approx: number;
  progress: number;
  /** 1 if this is the original query submitted by the client, 0 if it's a sub-query dispatched to a shard */
  is_initial_query?: number;
  /** For sub-queries on shards, the query_id of the original (coordinator) query */
  initial_query_id?: string;
  /** The ClickHouse server hostname that executed this query */
  hostname?: string;
}
export interface QueryHistoryItem {
  query_id: string;
  query_type: string;
  query_kind: string;
  query_start_time: string;
  query_duration_ms: number;
  read_rows: number;
  read_bytes: number;
  result_rows: number;
  result_bytes: number;
  memory_usage: number;
  query: string;
  exception: string | null;
  user: string;
  client_hostname: string;
  type: string;
  efficiency_score: number | null;
  // ProfileEvents metrics
  cpu_time_us?: number;
  network_send_bytes?: number;
  network_receive_bytes?: number;
  disk_read_bytes?: number;
  disk_write_bytes?: number;
  // Additional performance metrics
  selected_parts?: number;
  selected_parts_total?: number;
  selected_marks?: number;
  selected_marks_total?: number;
  selected_ranges?: number;
  mark_cache_hits?: number;
  mark_cache_misses?: number;
  io_wait_us?: number;
  real_time_us?: number;
  user_time_us?: number;
  system_time_us?: number;
  // Query-level settings overrides
  Settings?: Record<string, string>;
  /** 1 if this is the original query submitted by the client, 0 if it's a sub-query dispatched to a shard */
  is_initial_query?: number;
  /** For sub-queries on shards, the query_id of the original (coordinator) query */
  initial_query_id?: string;
  /** Network address of the initial query's client (for distributed sub-queries) */
  initial_address?: string;
  /** The ClickHouse server hostname that executed this query */
  hostname?: string;
  /** Databases touched by this query (from system.query_log.databases Array(String)) */
  databases?: string[];
  /** Tables touched by this query (from system.query_log.tables Array(String)) */
  tables?: string[];
}
