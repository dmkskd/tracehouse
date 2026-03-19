/**
 * Types for the Analytics feature — table-level ordering key efficiency analysis.
 */

/** Per-table ordering key efficiency summary from query_log + system.parts */
export interface TableOrderingKeyEfficiency {
  /** Database name */
  database: string;
  /** Table name (without database prefix) */
  table_name: string;
  /** Number of SELECT queries that touched this table */
  query_count: number;
  /** Average pruning effectiveness (0-100, higher = better ordering key usage) */
  avg_pruning_pct: number | null;
  /** Number of queries with poor pruning (<50%) */
  poor_pruning_queries: number;
  /** Average percentage of parts scanned (lower = better) */
  avg_parts_scanned_pct: number | null;
  /** Total rows read across all queries */
  total_rows_read: number;
  /** Total marks scanned */
  total_marks_scanned: number;
  /** Total marks available (denominator for pruning) */
  total_marks_available: number;
  /** Total microseconds spent on index filtering */
  total_pk_filter_us: number;
  /** Average query duration in ms for queries touching this table */
  avg_duration_ms: number;
  /** Total CPU time in microseconds */
  total_cpu_us: number;
  /** Average peak memory usage in bytes */
  avg_memory_bytes: number;
  /** ORDER BY / sorting key from system.tables */
  sorting_key: string | null;
  /** Primary key from system.tables */
  primary_key: string | null;
  /** Total rows in the table (active parts) */
  table_rows: number | null;
  /** Total marks in the table (active parts) */
  table_marks: number | null;
  /** Number of active parts */
  active_parts: number | null;
}

/** Options for the ordering key efficiency query */
export interface OrderingKeyEfficiencyOptions {
  /** Number of days to look back in query_log (default: 7) */
  lookback_days?: number;
  /** Minimum number of queries to include a table (default: 1) */
  min_query_count?: number;
  /** Filter to a specific database (optional) */
  database?: string;
}

/** Per-query-pattern breakdown for a specific table */
export interface TableQueryPattern {
  /** normalized_query_hash as string */
  query_hash: string;
  /** A sample query text for this pattern */
  sample_query: string;
  /** How many times this pattern was executed */
  execution_count: number;
  /** Average pruning effectiveness (0-100, higher = better) */
  avg_pruning_pct: number | null;
  /** Number of executions with poor pruning (<50%) */
  poor_pruning_count: number;
  /** Average query duration in ms */
  avg_duration_ms: number;
  /** p50 query duration in ms */
  p50_duration_ms: number;
  /** p95 query duration in ms */
  p95_duration_ms: number;
  /** p99 query duration in ms */
  p99_duration_ms: number;
  /** Average rows read per execution */
  avg_rows_read: number;
  /** Total marks scanned */
  total_marks_scanned: number;
  /** Total marks available */
  total_marks_available: number;
  /** Total CPU time in microseconds */
  total_cpu_us: number;
  /** Average peak memory usage in bytes */
  avg_memory_bytes: number;
  /** p50 memory usage in bytes */
  p50_memory_bytes: number;
  /** p95 memory usage in bytes */
  p95_memory_bytes: number;
  /** p99 memory usage in bytes */
  p99_memory_bytes: number;
  /** First time this pattern was seen */
  first_seen: string;
  /** Last time this pattern was seen */
  last_seen: string;
  /** Per-user execution breakdown: { username: count } */
  user_breakdown: Record<string, number>;
}

/** A single index entry from EXPLAIN indexes = 1, json = 1 output */
export interface ExplainIndexEntry {
  /** Index type: PrimaryKey, MinMax, Partition, Skip */
  type: string;
  /** Column names used by this index */
  keys: string[];
  /** The condition evaluated against the index */
  condition: string;
  /** Index name (only for Skip indexes) */
  name?: string;
  /** Description (only for Skip indexes) */
  description?: string;
  /** Parts selected / total (e.g. "2/5") */
  parts: { selected: number; total: number } | null;
  /** Granules selected / total (e.g. "6/1083") */
  granules: { selected: number; total: number } | null;
}

// ─── Surface visualization types ────────────────────────────────────────

/** A single time-bucketed row of aggregated query stress for a table */
export interface StressSurfaceRow {
  ts: string;
  query_count: number;
  total_duration_ms: number;
  avg_duration_ms: number;
  p95_duration_ms: number;
  total_read_rows: number;
  total_read_bytes: number;
  total_memory: number;
  total_cpu_us: number;
  total_io_wait_us: number;
  total_selected_marks: number;
}

/** Insert activity per time bucket for a table */
export interface StressSurfaceInsertRow {
  ts: string;
  insert_count: number;
  inserted_rows: number;
  inserted_bytes: number;
}

/** Merge activity per time bucket for a table */
export interface StressSurfaceMergeRow {
  ts: string;
  merges: number;
  new_parts: number;
  merge_ms: number;
}

/** Full stress surface dataset for a single table */
export interface StressSurfaceData {
  table: string;
  queries: StressSurfaceRow[];
  inserts: StressSurfaceInsertRow[];
  merges: StressSurfaceMergeRow[];
}

/** A single row for the pattern surface: per (time, pattern) avg duration */
export interface PatternSurfaceRow {
  ts: string;
  normalized_query_hash: string;
  avg_duration_ms: number;
  query_count: number;
  avg_memory: number;
  sample_query: string;
}

/** Options for surface queries */
export interface SurfaceQueryOptions {
  database: string;
  table: string;
  /** Lookback window in hours (ignored when startTime/endTime are set) */
  hours?: number;
  /** Absolute start time (ISO string, e.g. '2026-03-18T09:00') */
  startTime?: string;
  /** Absolute end time (ISO string, e.g. '2026-03-19T09:00') */
  endTime?: string;
}

/** Parsed result of EXPLAIN indexes = 1 for a query */
export interface ExplainIndexesResult {
  /** All index entries found in the EXPLAIN output */
  indexes: ExplainIndexEntry[];
  /** The PrimaryKey index entry, if present */
  primaryKey: ExplainIndexEntry | null;
  /** Skip indexes used, if any */
  skipIndexes: ExplainIndexEntry[];
  /** Whether the query could be explained (false if EXPLAIN failed) */
  success: boolean;
  /** Error message if EXPLAIN failed */
  error?: string;
}
