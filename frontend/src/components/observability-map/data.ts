/**
 * ClickHouse Observability Map — static data model
 *
 * System tables organized by category with key columns and diagnostic queries.
 * Based on Brendan Gregg-style observability tool maps adapted for ClickHouse.
 */

// ─── Domain types ────────────────────────────────────────────

export interface DiagnosticQuery {
  label: string;
  sql: string;
}

export interface ObservabilityColumn {
  name: string;
  desc: string;
  size: number;
}

export interface SystemTable {
  name: string;
  desc: string;
  cols: string[];
  queries: DiagnosticQuery[];
  children: ObservabilityColumn[];
  since?: string;
  cloudOnly?: boolean;
  available?: boolean;
  sortingKey?: string;
  primaryKey?: string;
}

export interface ObservabilityCategory {
  name: string;
  color: string;
  children: SystemTable[];
}

export interface ObservabilityData {
  name: string;
  children: ObservabilityCategory[];
}

// ─── D3 hierarchy types ──────────────────────────────────────

export interface SunburstNodeData {
  name: string;
  value?: number;
  children?: SunburstNodeData[];
  meta?: {
    type: 'root' | 'category' | 'table' | 'column';
    category?: string;
    color?: string;
    desc?: string;
    cols?: string[];
    queries?: DiagnosticQuery[];
    size?: number;
    since?: string;
    cloudOnly?: boolean;
    available?: boolean;
  };
}

// ─── Runtime enrichment ─────────────────────────────────────

/**
 * Probe ClickHouse to discover which system tables exist,
 * then merge availability into the static catalog.
 *
 * Returns a new ObservabilityData with `available` flags ready
 * to be passed into `buildHierarchy`.
 */
export interface ServerTableInfo {
  name: string;
  sorting_key: string;
  primary_key: string;
}

export async function enrichWithAvailability(
  executeQuery: <T extends Record<string, unknown>>(sql: string) => Promise<T[]>,
): Promise<Map<string, ServerTableInfo>> {
  try {
    const rows = await executeQuery<{ name: string; sorting_key: string; primary_key: string }>(
      `SELECT name, sorting_key, primary_key FROM system.tables WHERE database = 'system'`
    );
    const map = new Map<string, ServerTableInfo>();
    for (const r of rows) {
      map.set(`system.${r.name}`, { name: `system.${r.name}`, sorting_key: r.sorting_key || '', primary_key: r.primary_key || '' });
    }
    return map;
  } catch {
    // If the probe fails, treat everything as available
    return new Map<string, ServerTableInfo>();
  }
}

/**
 * Merge a set of available table names into the static data,
 * producing a new ObservabilityData where each SystemTable
 * carries an `available` flag.
 *
 * If `serverTables` is empty (probe failed), all tables are
 * treated as available so the map degrades gracefully.
 */
export function mergeAvailability(
  data: ObservabilityData,
  serverTables: Map<string, ServerTableInfo>,
): ObservabilityData {
  const probeSucceeded = serverTables.size > 0;
  return {
    ...data,
    children: data.children.map(cat => ({
      ...cat,
      children: cat.children.map(table => {
        const info = serverTables.get(table.name);
        return {
          ...table,
          available: !probeSucceeded || !table.name.startsWith('system.') || serverTables.has(table.name),
          sortingKey: info?.sorting_key || undefined,
          primaryKey: info?.primary_key || undefined,
        };
      }),
    })),
  };
}

/**
 * Column comments keyed by "system.table_name.column_name".
 */
export type ColumnCommentMap = Map<string, string>;

/**
 * Fetch column comments from system.columns for all system tables.
 * Returns a map of "system.table.column" → comment string.
 */
export async function fetchColumnComments(
  executeQuery: <T extends Record<string, unknown>>(sql: string) => Promise<T[]>,
): Promise<ColumnCommentMap> {
  try {
    const rows = await executeQuery<{ table: string; name: string; comment: string }>(
      `SELECT table, name, comment FROM system.columns WHERE database = 'system' AND length(comment) > 0`
    );
    const map: ColumnCommentMap = new Map();
    for (const r of rows) {
      if (r.comment) map.set(`system.${r.table}.${r.name}`, r.comment);
    }
    return map;
  } catch {
    return new Map();
  }
}

// ─── Convert domain model → D3 hierarchy ─────────────────────

export function buildHierarchy(data: ObservabilityData): SunburstNodeData {
  return {
    name: data.name,
    meta: { type: 'root' },
    children: data.children.map(cat => ({
      name: cat.name,
      meta: { type: 'category' as const, color: cat.color, category: cat.name },
      children: cat.children.map(table => ({
        name: table.name,
        meta: {
          type: 'table' as const,
          category: cat.name,
          color: cat.color,
          desc: table.desc,
          cols: table.cols,
          queries: table.queries,
          since: table.since,
          cloudOnly: table.cloudOnly,
          available: table.available,
        },
        children: table.children.map(col => ({
          name: col.name,
          value: col.size,
          meta: {
            type: 'column' as const,
            category: cat.name,
            color: cat.color,
            desc: col.desc,
            size: col.size,
          },
        })),
      })),
    })),
  };
}

// ─── Static data ─────────────────────────────────────────────

export const OBSERVABILITY_DATA: ObservabilityData = {
  name: "ClickHouse",
  children: [
    {
      name: "Query Execution",
      color: "#3b82f6",
      children: [
        {
          name: "system.query_log",
          desc: "Complete history of all executed queries with detailed execution statistics. The single most important table for performance analysis.",
          cols: ["event_time", "query_duration_ms", "read_rows", "read_bytes", "written_rows", "written_bytes", "result_rows", "memory_usage", "query", "query_kind", "type", "exception_code", "ProfileEvents", "Settings", "thread_ids", "user", "client_hostname", "query_cache_usage", "used_aggregate_functions", "initial_query_start_time"],
          queries: [
            {
              label: "Slow queries today", sql: `SELECT query_start_time,
  query_duration_ms, read_rows,
  memory_usage, query
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query_duration_ms > 1000
  AND event_date = today()
ORDER BY query_duration_ms DESC
LIMIT 20` },
            {
              label: "Error rate last 5 min", sql: `SELECT round(
  countIf(exception_code != 0)
  / count() * 100, 2
) AS error_pct
FROM system.query_log
WHERE type IN (
  'QueryFinish',
  'ExceptionWhileProcessing')
AND query_start_time
  > now() - INTERVAL 5 MINUTE` },
            {
              label: "Top tables by read volume", sql: `SELECT tables,
  sum(read_rows) AS total_rows,
  formatReadableSize(
    sum(read_bytes)) AS total_read
FROM system.query_log
WHERE event_date = today()
GROUP BY tables
ORDER BY sum(read_bytes) DESC
LIMIT 10` },
            {
              label: "Queries bound by disk I/O", sql: `SELECT query_id,
  ProfileEvents['ReadBufferFromFileDescriptorReadBytes'] as disk_read,
  query
FROM system.query_log
WHERE type = 'QueryFinish'
ORDER BY disk_read DESC
LIMIT 10` },
            {
              label: "Cache hit rate", sql: `SELECT query_kind,
  countIf(query_cache_usage = 'Used')
    AS cached,
  count() AS total,
  round(cached / total * 100, 2)
    AS cache_hit_pct
FROM system.query_log
WHERE event_date = today()
GROUP BY query_kind` },
            {
              label: "Distributed query delays", sql: `SELECT initial_query_id,
  query_duration_ms,
  query_start_time
    - initial_query_start_time
    AS coordinator_delay_ms
FROM system.query_log
WHERE type = 'QueryFinish'
  AND initial_query_id != query_id
ORDER BY coordinator_delay_ms DESC
LIMIT 10` }
          ],
          children: [
            { name: "query_duration_ms", desc: "Total wall-clock time", size: 3 },
            { name: "read_rows / read_bytes", desc: "Data scanned", size: 3 },
            { name: "memory_usage", desc: "Peak memory (bytes)", size: 2 },
            { name: "written_rows", desc: "Rows written (inserts)", size: 2 },
            { name: "ProfileEvents{}", desc: "Map of 500+ counters per query", size: 4 },
            { name: "exception_code", desc: "Non-zero = failed query", size: 2 },
            { name: "query_cache_usage", desc: "None, Used, or Stored", size: 1 }
          ]
        },
        {
          name: "system.processes",
          desc: "Currently running queries — the 'top' for ClickHouse. Shows live resource consumption.",
          cols: ["query", "elapsed", "read_rows", "read_bytes", "total_rows_approx", "memory_usage", "query_id", "user", "is_cancelled"],
          queries: [
            {
              label: "Running queries", sql: `SELECT query_id, user,
  elapsed, read_rows,
  memory_usage, query
FROM system.processes
ORDER BY elapsed DESC` },
            {
              label: "Kill a slow query", sql: `KILL QUERY
  WHERE query_id = '...'` }
          ],
          children: [
            { name: "elapsed", desc: "Seconds running", size: 2 },
            { name: "memory_usage", desc: "Current memory", size: 2 },
            { name: "query_id", desc: "Unique query identifier", size: 2 }
          ]
        },
        {
          name: "system.query_thread_log",
          desc: "Per-thread execution stats for each query — useful for diagnosing thread-level imbalances.",
          cols: ["thread_name", "thread_id", "query_id", "read_rows", "read_bytes", "written_rows", "memory_usage", "ProfileEvents"],
          queries: [
            {
              label: "Thread-level breakdown", sql: `SELECT thread_name,
  read_rows, memory_usage,
  ProfileEvents['RealTimeMicroseconds']
FROM system.query_thread_log
WHERE query_id = '...'` }
          ],
          children: [
            { name: "thread_name", desc: "QueryPipeline, MergeMutate, etc.", size: 2 },
            { name: "ProfileEvents", desc: "Counters per thread", size: 3 }
          ]
        },
        {
          name: "system.query_views_log",
          desc: "Materialized view execution triggered by INSERTs — track which views are slow.",
          cols: ["view_name", "view_type", "view_query", "view_duration_ms", "read_rows", "written_rows", "status", "exception_code"],
          queries: [
            {
              label: "Slow materialized views", sql: `SELECT view_name,
  view_duration_ms, read_rows,
  written_rows, status
FROM system.query_views_log
WHERE view_duration_ms > 500
ORDER BY view_duration_ms DESC` }
          ],
          children: [
            { name: "view_duration_ms", desc: "MV processing time", size: 2 },
            { name: "status", desc: "QueryFinish or exception", size: 1 }
          ]
        },
        {
          name: "system.query_metric_log",
          desc: "Per-query time-series metrics sampled during execution — memory, CPU events over time.",
          since: "24.10",
          cols: ["event_time", "query_id", "memory_usage", "ProfileEvents"],
          queries: [
            {
              label: "Memory timeline for query", sql: `SELECT event_time,
  memory_usage / 1e6 AS mem_mb
FROM system.query_metric_log
WHERE query_id = '...'
ORDER BY event_time` },
            {
              label: "CPU usage over time", sql: `SELECT event_time,
  ProfileEvents['RealTimeMicroseconds']
    AS real_us,
  ProfileEvents['UserTimeMicroseconds']
    AS user_us
FROM system.query_metric_log
WHERE query_id = '...'
ORDER BY event_time` }
          ],
          children: [
            { name: "memory_usage", desc: "Memory at each sample point", size: 2 },
            { name: "ProfileEvents", desc: "Cumulative counters at each sample", size: 3 }
          ]
        },
        {
          name: "EXPLAIN variants",
          desc: "Query plan introspection: AST, PLAN, PIPELINE, ESTIMATE, indexes=1 for index pruning analysis.",
          cols: [],
          queries: [
            {
              label: "Index usage analysis", sql: `EXPLAIN indexes = 1
SELECT * FROM my_table
WHERE date > '2025-01-01'` },
            {
              label: "Pipeline parallelism", sql: `EXPLAIN PIPELINE
  graph = 1
SELECT ...` },
            {
              label: "Read estimate", sql: `EXPLAIN ESTIMATE
SELECT * FROM my_table
WHERE id = 42` }
          ],
          children: [
            { name: "EXPLAIN PLAN", desc: "Logical plan with filters", size: 2 },
            { name: "EXPLAIN PIPELINE", desc: "Execution graph & threads", size: 2 },
            { name: "EXPLAIN indexes=1", desc: "Parts/marks selected vs total", size: 3 },
            { name: "EXPLAIN ESTIMATE", desc: "Estimated rows/bytes", size: 2 }
          ]
        }
      ]
    },
    {
      name: "Profiling & Tracing",
      color: "#8b5cf6",
      children: [
        {
          name: "system.trace_log",
          desc: "Stack traces sampled by the built-in profiler. Essential for CPU flame graphs and memory allocation analysis.",
          cols: ["trace_type", "thread_id", "query_id", "trace", "timer_type", "size"],
          queries: [
            {
              label: "CPU stack traces", sql: `SELECT
  arrayStringConcat(
    arrayMap(
      x -> demangle(
        addressToSymbol(x)),
      trace), '\\n'
  ) AS stack,
  count() AS samples
FROM system.trace_log
WHERE trace_type = 'CPU'
  AND query_id = '...'
GROUP BY trace
ORDER BY samples DESC
LIMIT 20` },
            {
              label: "Memory alloc traces", sql: `SELECT ... trace_type = 'Memory'
  AND size > 1048576  -- >1MB allocs` }
          ],
          children: [
            { name: "CPU traces", desc: "trace_type='CPU'", size: 3 },
            { name: "Real traces", desc: "trace_type='Real' (wall clock)", size: 2 },
            { name: "Memory traces", desc: "trace_type='Memory'", size: 2 },
            { name: "MemorySample", desc: "Sampled allocations", size: 2 }
          ]
        },
        {
          name: "system.opentelemetry_span_log",
          desc: "OpenTelemetry-compatible distributed tracing spans generated by ClickHouse itself.",
          cols: ["trace_id", "span_id", "parent_span_id", "operation_name", "start_time_us", "finish_time_us", "attribute"],
          queries: [
            {
              label: "Query trace spans", sql: `SELECT operation_name,
  (finish_time_us -
   start_time_us) AS dur_us
FROM system.opentelemetry_span_log
WHERE trace_id = '...'
ORDER BY start_time_us` }
          ],
          children: [
            { name: "operation_name", desc: "Span label", size: 2 },
            { name: "duration_us", desc: "finish - start", size: 2 }
          ]
        },
        {
          name: "system.processors_profile_log",
          desc: "Per-processor (pipeline node) execution profile — input/output waits, rows processed per step.",
          cols: ["name", "id", "parent_ids", "elapsed_us", "input_wait_elapsed_us", "output_wait_elapsed_us", "input_rows", "output_rows"],
          queries: [
            {
              label: "Pipeline bottleneck", sql: `SELECT name,
  elapsed_us,
  input_wait_elapsed_us,
  output_wait_elapsed_us,
  output_rows
FROM system.processors_profile_log
WHERE query_id = '...'
ORDER BY elapsed_us DESC` }
          ],
          children: [
            { name: "elapsed_us", desc: "Total processor time", size: 2 },
            { name: "input_wait_us", desc: "Stalled waiting for data", size: 2 },
            { name: "output_wait_us", desc: "Stalled on downstream", size: 2 }
          ]
        }
      ]
    },
    {
      name: "MergeTree Storage",
      color: "#22c55e",
      children: [
        {
          name: "system.parts",
          desc: "Every data part in every MergeTree table. The key to understanding storage layout, part counts, and compression.",
          cols: ["database", "table", "name", "partition", "active", "rows", "bytes_on_disk", "data_compressed_bytes", "data_uncompressed_bytes", "marks_count", "modification_time", "min_block_number", "max_block_number", "level", "primary_key_bytes_in_memory", "data_version"],
          queries: [
            {
              label: "Storage per table", sql: `SELECT database, table,
  count() AS parts,
  sum(rows) AS total_rows,
  formatReadableSize(
    sum(bytes_on_disk)) AS disk,
  round(sum(data_uncompressed_bytes)
   / sum(data_compressed_bytes),
   2) AS compress_ratio
FROM system.parts
WHERE active
GROUP BY database, table
ORDER BY sum(bytes_on_disk) DESC` },
            {
              label: "Too many parts alert", sql: `SELECT database, table,
  count() AS parts
FROM system.parts
WHERE active
GROUP BY database, table
HAVING parts > 100
ORDER BY parts DESC` }
          ],
          children: [
            { name: "rows", desc: "Row count per part", size: 2 },
            { name: "bytes_on_disk", desc: "Compressed on-disk size", size: 2 },
            { name: "data_uncompressed_bytes", desc: "Uncompressed logical size", size: 2 },
            { name: "marks_count", desc: "Number of index granules", size: 1 },
            { name: "level", desc: "Merge generation (0=new)", size: 1 },
            { name: "active", desc: "1=live, 0=being merged away", size: 1 },
            { name: "data_version", desc: "Mutation version (24.3+)", size: 1 }
          ]
        },
        {
          name: "system.parts_columns",
          desc: "Per-column size and compression stats within each part. Find which columns dominate storage.",
          cols: ["database", "table", "column", "type", "rows", "data_compressed_bytes", "data_uncompressed_bytes", "marks_bytes"],
          queries: [
            {
              label: "Column-level compression", sql: `SELECT column, type,
  formatReadableSize(
    sum(data_compressed_bytes)
  ) AS compressed,
  round(sum(data_uncompressed_bytes)
   / sum(data_compressed_bytes),
   2) AS ratio
FROM system.parts_columns
WHERE database='mydb'
  AND table='mytable'
  AND active
GROUP BY column, type
ORDER BY sum(data_compressed_bytes) DESC` }
          ],
          children: [
            { name: "per-column bytes", desc: "Compressed vs uncompressed per col", size: 3 },
            { name: "marks_bytes", desc: "Mark file size per column", size: 1 }
          ]
        },
        {
          name: "system.merges",
          desc: "Currently running merge operations. Monitor progress, size, and whether they are mutations.",
          cols: ["database", "table", "elapsed", "progress", "num_parts", "result_part_name", "total_size_bytes_compressed", "bytes_read_uncompressed", "bytes_written_uncompressed", "is_mutation", "memory_usage"],
          queries: [
            {
              label: "Active merges", sql: `SELECT database, table,
  elapsed, progress,
  num_parts, is_mutation,
  formatReadableSize(
    total_size_bytes_compressed
  ) AS size
FROM system.merges` }
          ],
          children: [
            { name: "progress", desc: "0.0 to 1.0", size: 2 },
            { name: "num_parts", desc: "Source parts being merged", size: 1 },
            { name: "is_mutation", desc: "ALTER UPDATE/DELETE merge", size: 2 }
          ]
        },
        {
          name: "system.mutations",
          desc: "Track ALTER UPDATE / DELETE operations. Mutations rewrite entire parts and can be very expensive.",
          cols: ["database", "table", "mutation_id", "command", "create_time", "is_done", "parts_to_do", "parts_to_do_names", "latest_failed_part", "latest_fail_reason"],
          queries: [
            {
              label: "Stuck mutations", sql: `SELECT database, table,
  mutation_id, command,
  create_time, parts_to_do,
  latest_fail_reason
FROM system.mutations
WHERE NOT is_done
ORDER BY create_time` },
            {
              label: "Kill stuck mutation", sql: `KILL MUTATION
  WHERE mutation_id = '...'` }
          ],
          children: [
            { name: "is_done", desc: "0=still running", size: 2 },
            { name: "parts_to_do", desc: "Remaining parts to rewrite", size: 2 },
            { name: "latest_fail_reason", desc: "Why it's stuck", size: 2 }
          ]
        },
        {
          name: "system.part_log",
          desc: "Historical log of all part lifecycle events: creation, merge, mutation, download, removal.",
          cols: ["event_type", "event_time", "database", "table", "part_name", "rows", "size_in_bytes", "merge_reason", "error", "duration_ms"],
          queries: [
            {
              label: "Failed merge/mutate events", sql: `SELECT event_date, event_type,
  table,
  errorCodeToName(error)
    AS err_name,
  count() AS c
FROM system.part_log
WHERE error != 0
GROUP BY event_date,
  event_type, table, error
ORDER BY event_date DESC` }
          ],
          children: [
            { name: "NewPart", desc: "INSERT created a part", size: 1 },
            { name: "MergeParts", desc: "Background merge completed", size: 2 },
            { name: "MutatePart", desc: "Mutation completed", size: 1 },
            { name: "RemovePart", desc: "Part garbage collected", size: 1 }
          ]
        },
        {
          name: "system.detached_parts",
          desc: "Parts removed from active dataset — broken, orphaned, or manually detached. Monitor for data issues.",
          cols: ["database", "table", "partition_id", "name", "reason", "disk", "min_block_number", "max_block_number"],
          queries: [
            {
              label: "Detached parts check", sql: `SELECT database, table,
  name, reason
FROM system.detached_parts
ORDER BY database, table` }
          ],
          children: [
            { name: "reason", desc: "broken, noquorum, clone, covered", size: 3 }
          ]
        },
        {
          name: "system.dropped_tables_parts",
          desc: "Parts belonging to dropped tables not yet cleaned up. Monitor for storage reclamation.",
          since: "24.1",
          cols: ["database", "table", "name", "rows", "bytes_on_disk", "modification_time"],
          queries: [
            {
              label: "Pending cleanup", sql: `SELECT database, table,
  count() AS parts,
  formatReadableSize(
    sum(bytes_on_disk)) AS pending
FROM system.dropped_tables_parts
GROUP BY database, table
ORDER BY sum(bytes_on_disk) DESC` }
          ],
          children: [
            { name: "bytes_on_disk", desc: "Storage awaiting cleanup", size: 2 },
            { name: "rows", desc: "Rows not yet removed", size: 1 }
          ]
        },
        {
          name: "system.remote_data_paths",
          desc: "Maps local parts to their remote objects (S3, GCS). Used to debug remote storage performance.",
          cols: ["disk_name", "path", "local_path", "remote_path", "size"],
          queries: [
            {
              label: "Remote path layout", sql: `SELECT disk_name, local_path, remote_path,
  formatReadableSize(size) AS s
FROM system.remote_data_paths
LIMIT 20` }
          ],
          children: [
            { name: "remote_path", desc: "Blob path in object storage", size: 3 },
            { name: "size", desc: "Size of the file (compressed)", size: 2 }
          ]
        }
      ]
    },
    {
      name: "Indexes & Schema",
      color: "#06b6d4",
      children: [
        {
          name: "system.tables",
          desc: "Metadata for every table: engine, sorting key, partition key, total size, row count.",
          cols: ["database", "name", "engine", "engine_full", "create_table_query", "sorting_key", "partition_key", "primary_key", "total_rows", "total_bytes", "lifetime_rows", "lifetime_bytes"],
          queries: [
            {
              label: "Table overview", sql: `SELECT database, name,
  engine,
  formatReadableSize(
    total_bytes) AS size,
  total_rows,
  sorting_key, partition_key
FROM system.tables
WHERE database NOT IN (
  'system','INFORMATION_SCHEMA')
ORDER BY total_bytes DESC` }
          ],
          children: [
            { name: "engine_full", desc: "Full CREATE TABLE engine clause", size: 2 },
            { name: "sorting_key", desc: "ORDER BY expression", size: 2 },
            { name: "partition_key", desc: "PARTITION BY expression", size: 2 }
          ]
        },
        {
          name: "system.columns",
          desc: "Every column in every table — types, codecs, default expressions, compression stats.",
          cols: ["database", "table", "name", "type", "compression_codec", "default_kind", "default_expression", "data_compressed_bytes", "data_uncompressed_bytes"],
          queries: [
            {
              label: "Column types & codecs", sql: `SELECT name, type,
  compression_codec,
  formatReadableSize(
    data_compressed_bytes) AS comp,
  formatReadableSize(
    data_uncompressed_bytes) AS raw
FROM system.columns
WHERE database='mydb'
  AND table='mytable'` }
          ],
          children: [
            { name: "type", desc: "Column data type", size: 2 },
            { name: "compression_codec", desc: "LZ4, ZSTD, Delta, etc.", size: 2 }
          ]
        },
        {
          name: "system.data_skipping_indices",
          desc: "Skip indexes defined on tables — minmax, set, bloom_filter, tokenbf, ngrambf.",
          cols: ["database", "table", "name", "type", "expr", "granularity", "data_compressed_bytes", "data_uncompressed_bytes"],
          queries: [
            {
              label: "All skip indexes", sql: `SELECT database, table,
  name, type, expr,
  granularity
FROM system.data_skipping_indices
WHERE database NOT IN
  ('system')` }
          ],
          children: [
            { name: "minmax", desc: "Range-based pruning", size: 1 },
            { name: "set", desc: "Unique value set per granule", size: 1 },
            { name: "bloom_filter", desc: "Probabilistic membership", size: 2 },
            { name: "ngrambf_v1", desc: "N-gram bloom for LIKE", size: 1 },
            { name: "tokenbf_v1", desc: "Token bloom for hasToken()", size: 1 }
          ]
        }
      ]
    },
    {
      name: "Replication",
      color: "#f59e0b",
      children: [
        {
          name: "system.replicas",
          desc: "Health status of all ReplicatedMergeTree tables. Check leader election, lag, queue depth.",
          cols: ["database", "table", "engine", "is_leader", "is_readonly", "is_session_expired", "future_parts", "parts_to_check", "absolute_delay", "queue_size", "inserts_in_queue", "merges_in_queue", "total_replicas", "active_replicas"],
          queries: [
            {
              label: "Replication health", sql: `SELECT database, table,
  is_leader, is_readonly,
  absolute_delay,
  queue_size,
  inserts_in_queue,
  merges_in_queue
FROM system.replicas` },
            {
              label: "Lag alert", sql: `SELECT * FROM system.replicas
WHERE absolute_delay > 60
   OR is_readonly = 1
   OR queue_size > 100` }
          ],
          children: [
            { name: "absolute_delay", desc: "Seconds behind leader", size: 3 },
            { name: "is_readonly", desc: "1 = ZK session lost", size: 2 },
            { name: "queue_size", desc: "Pending fetch/merge tasks", size: 2 }
          ]
        },
        {
          name: "system.replication_queue",
          desc: "Individual replication tasks: fetches, merges, mutations queued per replica.",
          cols: ["database", "table", "replica_name", "type", "create_time", "source_replica", "new_part_name", "num_tries", "last_exception", "is_currently_executing", "num_postponed", "postpone_reason"],
          queries: [
            {
              label: "Stuck replication tasks", sql: `SELECT database, table,
  type, create_time,
  num_tries,
  last_exception
FROM system.replication_queue
WHERE num_tries > 5
ORDER BY create_time` }
          ],
          children: [
            { name: "num_tries", desc: "Retry count (high=stuck)", size: 2 },
            { name: "last_exception", desc: "Error message", size: 2 },
            { name: "type", desc: "GET_PART, MERGE_PARTS, MUTATE_PART", size: 2 }
          ]
        },
        {
          name: "system.replicated_fetches",
          desc: "Currently in-progress part fetches from other replicas.",
          cols: ["database", "table", "elapsed", "progress", "result_part_name", "source_replica_hostname", "bytes_read_compressed", "total_size_bytes_compressed"],
          queries: [
            {
              label: "Active fetches", sql: `SELECT database, table,
  elapsed, progress,
  source_replica_hostname,
  formatReadableSize(
    total_size_bytes_compressed)
FROM system.replicated_fetches` }
          ],
          children: [
            { name: "progress", desc: "0.0 to 1.0", size: 1 },
            { name: "elapsed", desc: "Seconds since start", size: 1 }
          ]
        },
        {
          name: "system.distribution_queue",
          desc: "Pending data in Distributed table send queues to remote shards.",
          cols: ["database", "table", "is_blocked", "error_count", "data_files", "data_compressed_bytes"],
          queries: [
            {
              label: "Queue status", sql: `SELECT database, table,
  is_blocked, error_count,
  data_files,
  formatReadableSize(
    data_compressed_bytes)
FROM system.distribution_queue` }
          ],
          children: [
            { name: "is_blocked", desc: "1 = sending stalled", size: 2 },
            { name: "error_count", desc: "Send failures", size: 1 }
          ]
        },
        {
          name: "system.clusters",
          desc: "Cluster topology: shards, replicas, hosts, ports.",
          cols: ["cluster", "shard_num", "shard_weight", "replica_num", "host_name", "host_address", "port", "is_local"],
          queries: [
            {
              label: "Cluster topology", sql: `SELECT cluster, shard_num,
  replica_num, host_name,
  port, is_local
FROM system.clusters` }
          ],
          children: [
            { name: "shard_num", desc: "Shard index", size: 1 },
            { name: "replica_num", desc: "Replica index in shard", size: 1 }
          ]
        },
        {
          name: "system.zookeeper",
          desc: "Browse ZooKeeper/Keeper znodes directly via SQL. Read-only access to coordination data.",
          cols: ["name", "value", "ctime", "mtime", "version", "numChildren", "path"],
          queries: [
            {
              label: "Browse ZK path", sql: `SELECT name, value,
  numChildren
FROM system.zookeeper
WHERE path =
  '/clickhouse/tables/01/mytable'` }
          ],
          children: [
            { name: "path", desc: "ZK znode path", size: 2 },
            { name: "numChildren", desc: "Child znodes", size: 1 }
          ]
        }
      ]
    },
    {
      name: "System Resources",
      color: "#ef4444",
      children: [
        {
          name: "system.metrics",
          desc: "Real-time gauge metrics — current values of 150+ counters covering queries, merges, memory, threads, connections.",
          cols: ["metric", "value", "description"],
          queries: [
            {
              label: "Key metrics snapshot", sql: `SELECT metric, value,
  description
FROM system.metrics
WHERE metric IN (
  'Query','Merge',
  'MemoryTracking',
  'OpenFileForRead',
  'OpenFileForWrite',
  'BackgroundPoolTask',
  'TCPConnection',
  'HTTPConnection',
  'ReplicatedFetch'
)` },
            {
              label: "Memory pressure", sql: `SELECT
  value / 1e9 AS memory_gb
FROM system.metrics
WHERE metric =
  'MemoryTracking'` }
          ],
          children: [
            { name: "MemoryTracking", desc: "Total RSS tracked", size: 3 },
            { name: "Query", desc: "Active query count", size: 2 },
            { name: "Merge", desc: "Active merge count", size: 2 },
            { name: "BackgroundPoolTask", desc: "Background threads busy", size: 2 },
            { name: "TCPConnection", desc: "Open TCP sessions", size: 1 },
            { name: "HTTPConnection", desc: "Open HTTP sessions", size: 1 }
          ]
        },
        {
          name: "system.events",
          desc: "Cumulative counter events since server start — queries, reads, writes, network bytes, etc.",
          cols: ["event", "value", "description"],
          queries: [
            {
              label: "Key event counters", sql: `SELECT event, value
FROM system.events
WHERE event IN (
  'SelectQuery',
  'InsertQuery',
  'FailedQuery',
  'FileOpen',
  'ReadBufferFromFileDescriptorRead',
  'NetworkSendBytes',
  'NetworkReceiveBytes'
)` },
            {
              label: "Cache hit rates", sql: `SELECT event, value
FROM system.events
WHERE event LIKE '%Cache%'
ORDER BY value DESC` }
          ],
          children: [
            { name: "SelectQuery", desc: "Total SELECTs executed", size: 2 },
            { name: "InsertQuery", desc: "Total INSERTs executed", size: 2 },
            { name: "FailedQuery", desc: "Total failed queries", size: 2 },
            { name: "NetworkSendBytes", desc: "Total bytes sent", size: 1 },
            { name: "FileOpen", desc: "Total file opens", size: 1 }
          ]
        },
        {
          name: "system.asynchronous_metrics",
          desc: "OS-level and internal metrics updated every ~1 second. Memory, CPU, jemalloc, part counts.",
          cols: ["metric", "value", "description"],
          queries: [
            {
              label: "OS-level health", sql: `SELECT metric, value
FROM system.asynchronous_metrics
WHERE metric IN (
  'OSMemoryTotal',
  'OSMemoryAvailable',
  'LoadAverage1',
  'LoadAverage5',
  'MaxPartCountForPartition',
  'jemalloc.allocated',
  'UncompressedCacheBytes',
  'MarkCacheBytes'
)` }
          ],
          children: [
            { name: "LoadAverage1/5/15", desc: "OS load averages", size: 2 },
            { name: "MaxPartCountForPartition", desc: "Alert if >300", size: 3 },
            { name: "jemalloc.*", desc: "Allocator internals", size: 2 },
            { name: "OSMemory*", desc: "Total/Available RAM", size: 2 }
          ]
        },
        {
          name: "system.disks",
          desc: "Mounted disks/volumes with free space, total space, and type (local, s3, hdfs).",
          cols: ["name", "path", "free_space", "total_space", "keep_free_space", "type"],
          queries: [
            {
              label: "Disk usage", sql: `SELECT name, path,
  formatReadableSize(
    free_space) AS free,
  formatReadableSize(
    total_space) AS total,
  type
FROM system.disks` }
          ],
          children: [
            { name: "free_space", desc: "Bytes available", size: 2 },
            { name: "type", desc: "local, s3, hdfs", size: 1 }
          ]
        },
        {
          name: "system.dns_cache",
          desc: "Internal DNS resolution cache. Debug hostname resolution failures and latencies.",
          since: "24.2",
          cols: ["hostname", "ip_address", "ip_family", "cached_at"],
          queries: [
            {
              label: "DNS cache entries", sql: `SELECT hostname,
  ip_address, ip_family,
  cached_at
FROM system.dns_cache
ORDER BY cached_at DESC` }
          ],
          children: [
            { name: "hostname", desc: "Resolved hostname", size: 2 },
            { name: "ip_address", desc: "Cached IP result", size: 1 }
          ]
        },
        {
          name: "system.metric_log",
          desc: "Time-series of system.metrics and system.events sampled every N seconds. Historical resource trends.",
          cols: ["event_time", "CurrentMetric_*", "ProfileEvent_*"],
          queries: [
            {
              label: "Memory over time", sql: `SELECT event_time,
  CurrentMetric_MemoryTracking
    / 1e9 AS mem_gb
FROM system.metric_log
WHERE event_date = today()
ORDER BY event_time` }
          ],
          children: [
            { name: "CurrentMetric_*", desc: "Gauge snapshots", size: 2 },
            { name: "ProfileEvent_*", desc: "Counter deltas", size: 2 }
          ]
        }
      ]
    },
    {
      name: "Logging & Errors",
      color: "#ec4899",
      children: [
        {
          name: "system.text_log",
          desc: "ClickHouse server log as a SQL table. Filter errors, warnings, traces without touching log files.",
          cols: ["event_time", "thread_id", "level", "query_id", "logger_name", "message", "source_file"],
          queries: [
            {
              label: "Recent errors", sql: `SELECT event_time,
  logger_name, message
FROM system.text_log
WHERE level = 'Error'
ORDER BY event_time DESC
LIMIT 50` }
          ],
          children: [
            { name: "level", desc: "Fatal, Error, Warning, Info, Debug, Trace", size: 3 },
            { name: "logger_name", desc: "Component name", size: 2 }
          ]
        },
        {
          name: "system.errors",
          desc: "Aggregated error counters by error code since server start. Quick health check.",
          cols: ["name", "code", "value", "last_error_time", "last_error_message", "last_error_trace", "remote"],
          queries: [
            {
              label: "Error summary", sql: `SELECT name, code,
  value AS count,
  last_error_time,
  last_error_message
FROM system.errors
ORDER BY value DESC
LIMIT 20` }
          ],
          children: [
            { name: "value", desc: "Total occurrences", size: 2 },
            { name: "last_error_message", desc: "Most recent error text", size: 2 }
          ]
        },
        {
          name: "system.crash_log",
          desc: "Records server crashes with signal info, stack traces, and build metadata.",
          cols: ["event_time", "signal", "thread_id", "query_id", "trace", "trace_full", "build_id", "revision"],
          queries: [
            {
              label: "Crash history", sql: `SELECT event_time,
  signal, query_id,
  arrayStringConcat(
    trace_full, '\\n')
FROM system.crash_log
ORDER BY event_time DESC` }
          ],
          children: [
            { name: "signal", desc: "SIGSEGV, SIGABRT, etc.", size: 2 },
            { name: "trace_full", desc: "Full symbolized stack", size: 2 }
          ]
        },
        {
          name: "system.session_log",
          desc: "Login/logout events with auth details. Security audit trail.",
          cols: ["event_time", "auth_type", "user", "client_hostname", "client_port", "event_type"],
          queries: [
            {
              label: "Failed logins", sql: `SELECT event_time, user,
  client_hostname,
  auth_type
FROM system.session_log
WHERE event_type =
  'LoginFailure'
ORDER BY event_time DESC` }
          ],
          children: [
            { name: "LoginSuccess", desc: "Successful auth", size: 1 },
            { name: "LoginFailure", desc: "Failed auth attempt", size: 2 },
            { name: "Logout", desc: "Session ended", size: 1 }
          ]
        }
      ]
    },
    {
      name: "Config & Access",
      color: "#64748b",
      children: [
        {
          name: "system.settings",
          desc: "All session/query-level settings with current values and whether they differ from defaults.",
          cols: ["name", "value", "changed", "description", "min", "max", "readonly", "type"],
          queries: [
            {
              label: "Non-default settings", sql: `SELECT name, value,
  description
FROM system.settings
WHERE changed` }
          ],
          children: [
            { name: "changed", desc: "1 = differs from default", size: 2 },
            { name: "readonly", desc: "0/1/2 restriction level", size: 1 }
          ]
        },
        {
          name: "system.merge_tree_settings",
          desc: "MergeTree engine-level settings: merge behavior, part limits, index granularity.",
          cols: ["name", "value", "changed", "description"],
          queries: [
            {
              label: "Key MergeTree settings", sql: `SELECT name, value
FROM system.merge_tree_settings
WHERE name IN (
  'index_granularity',
  'parts_to_throw_insert',
  'max_bytes_to_merge_at_max_space_in_pool',
  'min_rows_for_wide_part'
)` }
          ],
          children: [
            { name: "index_granularity", desc: "Default 8192", size: 2 },
            { name: "parts_to_throw_insert", desc: "Max parts before reject", size: 2 }
          ]
        },
        {
          name: "system.server_settings",
          desc: "Server-level configuration: max threads, memory limits, ports, paths.",
          cols: ["name", "value", "default", "changed", "description", "type"],
          queries: [
            {
              label: "Server config", sql: `SELECT name, value,
  default, changed
FROM system.server_settings
WHERE changed
ORDER BY name` }
          ],
          children: [
            { name: "max_thread_pool_size", desc: "Global thread limit", size: 1 },
            { name: "max_server_memory_usage", desc: "Server memory cap", size: 2 }
          ]
        },
        {
          name: "system.quota_usage",
          desc: "Current resource usage against configured quotas per user/IP.",
          cols: ["quota_name", "quota_key", "duration", "queries", "errors", "result_rows", "read_rows", "execution_time"],
          queries: [
            {
              label: "Quota consumption", sql: `SELECT quota_name,
  quota_key, queries,
  errors, result_rows
FROM system.quota_usage` }
          ],
          children: [
            { name: "queries", desc: "Queries consumed in window", size: 2 },
            { name: "execution_time", desc: "Total CPU seconds used", size: 1 }
          ]
        },
        {
          name: "system.storage_policies",
          desc: "Tiered storage configuration: volumes, disks, movement policies.",
          cols: ["policy_name", "volume_name", "volume_priority", "disks", "max_data_part_size", "move_factor", "prefer_not_to_merge"],
          queries: [
            {
              label: "Storage tiers", sql: `SELECT policy_name,
  volume_name, disks,
  max_data_part_size,
  move_factor
FROM system.storage_policies` }
          ],
          children: [
            { name: "volume_name", desc: "hot, warm, cold, etc.", size: 2 },
            { name: "move_factor", desc: "Auto-move threshold (0-1)", size: 1 }
          ]
        },
        {
          name: "system.database_engines",
          desc: "Available database engine types and their features. Useful for introspecting supported engines.",
          since: "24.1",
          cols: ["name"],
          queries: [
            {
              label: "Available engines", sql: `SELECT name
FROM system.database_engines
ORDER BY name` }
          ],
          children: [
            { name: "name", desc: "Engine name (Atomic, Lazy, etc.)", size: 2 }
          ]
        },
        {
          name: "system.backups",
          desc: "Status of BACKUP/RESTORE operations.",
          cols: ["id", "name", "status", "error", "start_time", "end_time", "num_files", "total_size", "uncompressed_size", "compressed_size"],
          queries: [
            {
              label: "Backup status", sql: `SELECT name, status,
  start_time, end_time,
  formatReadableSize(
    compressed_size) AS size,
  error
FROM system.backups
ORDER BY start_time DESC` }
          ],
          children: [
            { name: "status", desc: "CREATING, BACKUP_CREATED, ERROR", size: 2 },
            { name: "compressed_size", desc: "Final backup size", size: 1 }
          ]
        }
      ]
    },
    {
      name: "Async & Ingestion",
      color: "#14b8a6",
      children: [
        {
          name: "system.asynchronous_insert_log",
          desc: "Log of async insert buffer flushes — status, timing, exceptions.",
          cols: ["event_time", "database", "table", "format", "query_id", "bytes", "rows", "status", "exception", "flush_time_microseconds", "flush_query_id"],
          queries: [
            {
              label: "Async insert health", sql: `SELECT event_time, table,
  status, rows, bytes,
  flush_time_microseconds / 1e6
    AS flush_sec
FROM system.asynchronous_insert_log
ORDER BY event_time DESC
LIMIT 50` }
          ],
          children: [
            { name: "status", desc: "Ok, FlushError", size: 2 },
            { name: "flush_time_us", desc: "Buffer flush latency", size: 2 }
          ]
        },
        {
          name: "system.moves",
          desc: "Currently executing part moves between disks/volumes (TTL or manual).",
          cols: ["database", "table", "elapsed", "target_disk_name", "target_disk_path", "part_name", "part_size", "thread_id"],
          queries: [
            {
              label: "Active moves", sql: `SELECT database, table,
  elapsed,
  target_disk_name,
  part_name,
  formatReadableSize(
    part_size)
FROM system.moves` }
          ],
          children: [
            { name: "target_disk_name", desc: "s3, cold, etc.", size: 2 },
            { name: "elapsed", desc: "Seconds in progress", size: 1 }
          ]
        },
        {
          name: "system.zookeeper_log",
          desc: "Log of all ZooKeeper/Keeper operations performed by this server.",
          cols: ["event_time", "type", "address", "path", "duration_ms", "error"],
          queries: [
            {
              label: "Slow ZK ops", sql: `SELECT type, path,
  duration_ms, error
FROM system.zookeeper_log
WHERE duration_ms > 100
ORDER BY event_time DESC
LIMIT 30` }
          ],
          children: [
            { name: "type", desc: "Create, Get, Set, Multi", size: 2 },
            { name: "duration_ms", desc: "ZK operation latency", size: 2 }
          ]
        }
      ]
    },
    {
      name: "Caches & Dictionaries",
      color: "#eab308",
      children: [
        {
          name: "system.query_cache",
          desc: "Recently executed queries whose results are cached to serve identical requests instantly.",
          cols: ["query", "result_size", "stale", "expires_at", "key_hash"],
          queries: [
            {
              label: "Query cache usage", sql: `SELECT query,
  formatReadableSize(result_size) AS size,
  expires_at
FROM system.query_cache
ORDER BY result_size DESC LIMIT 20` }
          ],
          children: [
            { name: "result_size", desc: "Size of cached result", size: 2 },
            { name: "stale", desc: "Is cache entry stale", size: 1 }
          ]
        },
        {
          name: "system.filesystem_cache",
          desc: "Object storage cache (S3/Azure/GCS) — cached segments on local disk. Critical for data lake setups.",
          cols: ["cache_name", "file_segment_range_begin", "file_segment_range_end", "size", "state", "cache_hits", "file_path", "downloaded_size"],
          queries: [
            {
              label: "Cache usage summary", sql: `SELECT cache_name,
  count() AS segments,
  formatReadableSize(
    sum(size)) AS total_cached,
  sum(cache_hits) AS hits
FROM system.filesystem_cache
GROUP BY cache_name` }
          ],
          children: [
            { name: "size", desc: "Cached segment size", size: 2 },
            { name: "cache_hits", desc: "Hits on this segment", size: 2 },
            { name: "state", desc: "DOWNLOADED, PARTIALLY_DOWNLOADED", size: 1 }
          ]
        },
        {
          name: "system.filesystem_cache_log",
          desc: "Per-query cache hit/miss log for object storage. Diagnose read amplification from S3/GCS.",
          cols: ["event_time", "query_id", "source_file_path", "file_segment_range", "read_type", "cache_type", "read_from_cache_bytes", "read_from_source_bytes"],
          queries: [
            {
              label: "Cache miss ratio per query", sql: `SELECT query_id,
  sum(read_from_cache_bytes) AS cached,
  sum(read_from_source_bytes) AS remote,
  round(remote / (cached + remote)
    * 100, 2) AS miss_pct
FROM system.filesystem_cache_log
WHERE event_date = today()
GROUP BY query_id
ORDER BY miss_pct DESC
LIMIT 20` }
          ],
          children: [
            { name: "read_from_cache_bytes", desc: "Bytes served from local cache", size: 2 },
            { name: "read_from_source_bytes", desc: "Bytes fetched from remote", size: 2 }
          ]
        },
        {
          name: "system.dictionaries",
          desc: "In-memory key-value stores used for fast lookups. Essential for joins but can consume massive RAM.",
          cols: ["database", "name", "status", "origin", "type", "key", "attribute.names", "bytes_allocated", "hierarchical_index_bytes_allocated", "element_count", "load_factor", "source"],
          queries: [
            {
              label: "Memory used by dicts", sql: `SELECT name, status,
  formatReadableSize(bytes_allocated) AS memory,
  element_count
FROM system.dictionaries
ORDER BY bytes_allocated DESC` }
          ],
          children: [
            { name: "bytes_allocated", desc: "RAM used by dict", size: 3 },
            { name: "element_count", desc: "Number of rows", size: 2 },
            { name: "status", desc: "LOADED, NOT_LOADED, FAILED", size: 2 }
          ]
        }
      ]
    }
  ]
};
