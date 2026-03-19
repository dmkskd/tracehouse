import type { IClickHouseAdapter } from '../adapters/types.js';
import type { QueryMetrics, QueryHistoryItem } from '../types/query.js';
import { RUNNING_QUERIES, QUERY_DETAIL, QUERY_THREAD_BREAKDOWN, PROFILE_EVENT_DESCRIPTIONS, SUB_QUERIES, COORDINATOR_IDS, RUNNING_COORDINATOR_IDS, QUERY_LOG_FLUSH_INTERVAL } from '../queries/query-queries.js';
/**
 * ProfileEvent comparison row between two queries.
 * Inspired by https://clickhouse.com/docs/knowledgebase/comparing-metrics-between-queries
 */
export interface ProfileEventComparison {
  /** ProfileEvent metric name */
  metric: string;
  /** Value from the first (baseline) query */
  v1: number;
  /** Value from the second query */
  v2: number;
  /** Change in decibels: 10 * log10(v2/v1) */
  dB: number;
  /** Percentage change relative to the larger value */
  perc: number;
}

/** ProfileEvent values for N-query comparison */
export interface MultiProfileEventRow {
  metric: string;
  /** Values per query, indexed by position (same order as input queryIds) */
  values: number[];
}
import { buildQuery, tagQuery, eventDateBound, escapeValue } from '../queries/builder.js';
import { TAB_QUERIES, TAB_INTERNAL, APP_SOURCE_PREFIX, sourceTag } from '../queries/source-tags.js';
import { mapQueryMetrics, mapQueryHistoryItem } from '../mappers/query-mappers.js';
import { shortenHostname } from '../mappers/helpers.js';

export class QueryAnalysisError extends Error {
  constructor(message: string, public readonly cause?: Error) {
    super(message);
    this.name = 'QueryAnalysisError';
  }
}

/**
 * Detailed query information from query_log
 */
export interface QueryDetail {
  // Basic info
  query_id: string;
  type: string;
  query_start_time: string;
  query_start_time_microseconds: string;
  query_duration_ms: number;
  query: string;
  formatted_query: string;
  query_kind: string;
  normalized_query_hash: string;
  query_hash: string;
  user: string;
  current_database: string;
  
  // Resource usage
  read_rows: number;
  read_bytes: number;
  written_rows: number;
  written_bytes: number;
  result_rows: number;
  result_bytes: number;
  memory_usage: number;
  
  // Threading
  thread_ids: number[];
  peak_threads_usage: number;
  
  // Objects touched
  databases: string[];
  tables: string[];
  columns: string[];
  partitions: string[];
  projections: string;
  views: string[];
  
  // Functions and features used
  used_functions: string[];
  used_aggregate_functions: string[];
  used_aggregate_function_combinators: string[];
  used_table_functions: string[];
  used_storages: string[];
  used_formats: string[];
  used_dictionaries: string[];
  
  // Error info
  exception_code: number;
  exception: string;
  stack_trace: string;
  
  // Client info
  client_hostname: string;
  client_name: string;
  client_version_major: number;
  client_version_minor: number;
  client_version_patch: number;
  interface: number;
  http_method: number;
  http_user_agent: string;
  
  // Distributed query info
  is_initial_query: number;
  initial_user: string;
  initial_query_id: string;
  initial_address: string;
  initial_query_start_time: string;
  
  // Settings and profile events (full maps)
  Settings: Record<string, string>;
  ProfileEvents: Record<string, number>;
  
  // Cache usage
  query_cache_usage: string;
  
  // Privileges
  used_privileges: string[];
  missing_privileges: string[];
  
  // Log comment
  log_comment: string;

  // Server that executed the query
  hostname: string;
}

/**
 * Similar query summary
 */
export interface SimilarQuery {
  query_id: string;
  query_start_time: string;
  query_duration_ms: number;
  read_rows: number;
  read_bytes: number;
  result_rows: number;
  memory_usage: number;
  cpu_time_us: number;
  user: string;
  client_hostname: string;
  exception_code: number;
  exception: string;
  Settings: Record<string, string>;
  /** Full query text — same structure as other executions but with different literal values */
  query: string;
  /** Query kind — SELECT, INSERT, ALTER, etc. */
  query_kind: string;
}

/**
 * Shard sub-query summary for distributed queries
 */
export interface SubQueryInfo {
  query_id: string;
  hostname: string;
  query_duration_ms: number;
  memory_usage: number;
  read_rows: number;
  read_bytes: number;
  query_preview: string;
  exception_code: number;
  exception: string;
}

/**
 * Setting default value info from system.settings
 */
export interface SettingDefault {
  name: string;
  default: string;
  description: string;
  type: string;
}

/**
 * Per-thread breakdown of a query from system.query_thread_log
 */
export interface QueryThreadBreakdown {
  thread_name: string;
  thread_id: number;
  query_duration_ms: number;
  read_rows: number;
  read_bytes: number;
  written_rows: number;
  written_bytes: number;
  memory_usage: number;
  peak_memory_usage: number;
  /** Microsecond timestamp when this thread finished (event_time_microseconds) */
  event_time_us: string;
  /** Microsecond timestamp when this thread's query started (query_start_time_microseconds) */
  query_start_time_us: string;
  /** Microsecond timestamp when the original query was submitted (initial_query_start_time_microseconds) */
  initial_query_start_time_us: string;
  cpu_time_us: number;
  user_time_us: number;
  system_time_us: number;
  io_wait_us: number;
  real_time_us: number;
  disk_read_bytes: number;
  disk_write_bytes: number;
  network_send_bytes: number;
  network_receive_bytes: number;
}

export interface QueryHistoryOptions {
  start_date: string;
  start_time: string;
  end_time: string;
  limit?: number;
  user?: string;
  query_id?: string;
  query_text?: string;
  min_duration_ms?: number;
  min_memory_bytes?: number;
  exclude_app_queries?: boolean;
  /** Filter by query kind (SELECT, INSERT, etc.) */
  query_kind?: string;
  /** Filter by status: 'success' or 'error' */
  status?: string;
  /** Filter by database name (case-insensitive contains on databases array) */
  database?: string;
  /** Filter by table name (case-insensitive contains on tables array) */
  table?: string;
}

/**
 * Convert ISO 8601 datetime string to ClickHouse DateTime format.
 * Input: '2026-02-11T16:01:59.113Z' or '2026-02-11T16:01:59Z'
 * Output: '2026-02-11 16:01:59'
 */
function toClickHouseDateTime(isoString: string): string {
  const date = new Date(isoString);
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, '0');
  const day = String(date.getUTCDate()).padStart(2, '0');
  const hours = String(date.getUTCHours()).padStart(2, '0');
  const minutes = String(date.getUTCMinutes()).padStart(2, '0');
  const seconds = String(date.getUTCSeconds()).padStart(2, '0');
  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
}

export class QueryAnalyzer {
  private envDetector: import('./environment-detector.js').EnvironmentDetector | null;

  constructor(
    private adapter: IClickHouseAdapter,
    envDetector?: import('./environment-detector.js').EnvironmentDetector,
  ) {
    this.envDetector = envDetector ?? null;
  }
  async getRunningQueries(): Promise<QueryMetrics[]> {
    try {
      const rows = await this.adapter.executeQuery(tagQuery(RUNNING_QUERIES, sourceTag(TAB_QUERIES, 'runningQueries')));
      // RUNNING_QUERIES returns `elapsed` but QueryMetrics expects `elapsed_seconds`
      return rows.map(r => mapQueryMetrics({ ...r, elapsed_seconds: (r as Record<string, unknown>).elapsed }));
    } catch (error) {
      throw new QueryAnalysisError('Failed to get running queries', error as Error);
    }
  }

  /**
   * Get distinct values for a low-cardinality column from query_log.
   * Useful for autocomplete on user/hostname filters.
   */
  async getDistinctFilterValues(column: 'user' | 'hostname' | 'query_kind', limit = 50): Promise<string[]> {
    let sql: string;
    if (column === 'hostname') {
      // Use system.clusters instead of scanning query_log — it's a tiny virtual
      // table that returns all cluster hostnames instantly, vs scanning 7 days
      // of query_log on a busy cluster (potentially billions of rows).
      sql = `SELECT DISTINCT host_name AS hostname FROM {{cluster_aware:system.clusters}} ORDER BY hostname LIMIT ${limit}`;
    } else if (column === 'query_kind') {
      // query_kind has very low cardinality (~6 values), so today() is enough
      sql = `SELECT DISTINCT query_kind FROM {{cluster_aware:system.query_log}} WHERE event_date >= today() AND query_kind != '' ORDER BY query_kind LIMIT ${limit}`;
    } else {
      // user is also low cardinality; today() avoids scanning a full week
      sql = `SELECT DISTINCT user FROM {{cluster_aware:system.query_log}} WHERE event_date >= today() AND user != '' ORDER BY user LIMIT ${limit}`;
    }
    try {
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_QUERIES, 'filterValues')));
      return rows.map(r => {
        const val = String((r as Record<string, unknown>)[column] ?? '');
        return column === 'hostname' ? shortenHostname(val) : val;
      }).filter(Boolean);
    } catch {
      return [];
    }
  }

  async getQueryHistory(options: QueryHistoryOptions): Promise<QueryHistoryItem[]> {
    const limit = options.limit ?? 100;
    
    // Convert ISO datetime strings to ClickHouse format
    const startTime = toClickHouseDateTime(options.start_time);
    const endTime = toClickHouseDateTime(options.end_time);
    
    const params: Record<string, string | number> = {
      start_date: options.start_date,
      start_time: startTime,
      end_time: endTime,
      limit,
    };

    // Build WHERE conditions dynamically
    let whereConditions = [
      "event_date >= {start_date}",
      "event_time >= {start_time}",
      "event_time <= {end_time}",
      "type IN ('QueryFinish', 'ExceptionWhileProcessing')"
    ];

    if (options.user) {
      whereConditions.push("user = {user}");
      params.user = options.user;
    }

    if (options.min_duration_ms != null) {
      whereConditions.push("query_duration_ms >= {min_duration_ms}");
      params.min_duration_ms = options.min_duration_ms;
    }

    if (options.min_memory_bytes != null) {
      whereConditions.push("memory_usage >= {min_memory_bytes}");
      params.min_memory_bytes = options.min_memory_bytes;
    }

    if (options.query_text) {
      whereConditions.push("positionCaseInsensitive(query, {query_text}) > 0");
      params.query_text = options.query_text;
    }

    if (options.query_id) {
      whereConditions.push("positionCaseInsensitive(query_id, {query_id}) > 0");
      params.query_id = options.query_id;
    }

    if (options.exclude_app_queries) {
      whereConditions.push(`positionCaseInsensitive(query, {exclude_app_tag}) = 0`);
      params.exclude_app_tag = APP_SOURCE_PREFIX;
    }

    if (options.query_kind) {
      whereConditions.push("query_kind = {query_kind}");
      params.query_kind = options.query_kind;
    }

    if (options.status) {
      if (options.status === 'error') {
        whereConditions.push("type = 'ExceptionWhileProcessing'");
      } else if (options.status === 'success') {
        whereConditions.push("type = 'QueryFinish'");
      }
    }

    if (options.database) {
      whereConditions.push("arrayExists(x -> positionCaseInsensitive(x, {filter_database}) > 0, databases)");
      params.filter_database = options.database;
    }

    if (options.table) {
      whereConditions.push("arrayExists(x -> positionCaseInsensitive(x, {filter_table}) > 0, tables)");
      params.filter_table = options.table;
    }

    const sql = `
      SELECT
        query_id,
        type,
        query_kind,
        query_start_time,
        query_duration_ms,
        read_rows,
        read_bytes,
        result_rows,
        result_bytes,
        memory_usage,
        query,
        exception,
        user,
        client_hostname,
        ProfileEvents['OSCPUVirtualTimeMicroseconds'] AS cpu_time_us,
        ProfileEvents['NetworkSendBytes'] AS network_send_bytes,
        ProfileEvents['NetworkReceiveBytes'] AS network_receive_bytes,
        ProfileEvents['ReadBufferFromFileDescriptorReadBytes'] AS disk_read_bytes,
        ProfileEvents['WriteBufferFromFileDescriptorWriteBytes'] AS disk_write_bytes,
        ProfileEvents['SelectedParts'] AS selected_parts,
        ProfileEvents['SelectedPartsTotal'] AS selected_parts_total,
        ProfileEvents['SelectedMarks'] AS selected_marks,
        ProfileEvents['SelectedMarksTotal'] AS selected_marks_total,
        ProfileEvents['SelectedRanges'] AS selected_ranges,
        ProfileEvents['MarkCacheHits'] AS mark_cache_hits,
        ProfileEvents['MarkCacheMisses'] AS mark_cache_misses,
        ProfileEvents['OSIOWaitMicroseconds'] AS io_wait_us,
        ProfileEvents['RealTimeMicroseconds'] AS real_time_us,
        ProfileEvents['UserTimeMicroseconds'] AS user_time_us,
        ProfileEvents['SystemTimeMicroseconds'] AS system_time_us,
        Settings,
        is_initial_query,
        initial_query_id,
        initial_address,
        hostName() AS hostname,
        databases,
        tables
      FROM {{cluster_aware:system.query_log}}
      WHERE ${whereConditions.join('\n    AND ')}
      ORDER BY event_time DESC
      LIMIT {limit}
    `;

    const finalSql = buildQuery(sql, params);
    try {
      const rows = await this.adapter.executeQuery(tagQuery(finalSql, sourceTag(TAB_QUERIES, 'queryHistory')));
      // QUERY_HISTORY returns `type` but QueryHistoryItem expects `query_type`
      return rows.map(r => mapQueryHistoryItem({ ...r, query_type: (r as Record<string, unknown>).type }));
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      console.error('[QueryAnalyzer] Failed to get query history:', errorMessage);
      console.error('[QueryAnalyzer] SQL:', finalSql);
      throw new QueryAnalysisError(`Failed to get query history: ${errorMessage}`, error as Error);
    }
  }

  async killQuery(queryId: string): Promise<void> {
    const sql = buildQuery('KILL QUERY WHERE query_id = {query_id}', { query_id: queryId });
    try {
      await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_QUERIES, 'killQuery')));
    } catch (error) {
      throw new QueryAnalysisError('Failed to kill query', error as Error);
    }
  }

  /**
   * Get detailed information for a specific query.
   * Returns all available metadata from query_log.
   */
  async getQueryDetail(queryId: string, eventDate?: string): Promise<QueryDetail | null> {
    const sql = buildQuery(QUERY_DETAIL.replace('{event_date_bound}', eventDateBound(eventDate)), { query_id: queryId });
    try {
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_QUERIES, 'queryDetail')));
      if (rows.length === 0) return null;
      return rows[0] as unknown as QueryDetail;
    } catch (error) {
      throw new QueryAnalysisError('Failed to get query detail', error as Error);
    }
  }

  /**
   * Get all column names for a list of database.table pairs.
   * Returns a map of "database.table" → column names[].
   * Used by Query Anatomy to show selected-vs-total columns.
   */
  async getTableColumns(tables: string[]): Promise<Record<string, string[]>> {
    if (tables.length === 0) return {};
    const result: Record<string, string[]> = {};
    try {
      // Parse "database.table" pairs and query system.columns
      const parsed = tables.map(t => {
        const dot = t.indexOf('.');
        return dot > 0
          ? { db: t.substring(0, dot), tbl: t.substring(dot + 1) }
          : { db: 'default', tbl: t };
      });
      // Build a single query with OR conditions for all tables
      const conditions = parsed.map((p, i) =>
        `(database = '${p.db.replace(/'/g, "''")}' AND table = '${p.tbl.replace(/'/g, "''")}')`
      ).join(' OR ');
      const sql = `
        SELECT database, table, groupArray(name) AS columns
        FROM {{cluster_aware:system.columns}}
        WHERE ${conditions}
        GROUP BY database, table
      `;
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_QUERIES, 'tableColumns')));
      for (const r of rows) {
        const row = r as Record<string, unknown>;
        const key = `${row.database}.${row.table}`;
        const cols = row.columns;
        result[key] = Array.isArray(cols) ? cols.map(String) : [];
      }
    } catch (err) {
      console.warn('[QueryAnalyzer] Failed to fetch table columns:', err);
    }
    return result;
  }

  /**
   * Get shard sub-queries for a distributed (coordinator) query.
   */
  async getSubQueries(initialQueryId: string, eventDate?: string): Promise<SubQueryInfo[]> {
    const sql = buildQuery(SUB_QUERIES.replace('{event_date_bound}', eventDateBound(eventDate)), { initial_query_id: initialQueryId });
    try {
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_QUERIES, 'subQueries')));
      return rows.map((r: any) => ({
        query_id: String(r.query_id),
        hostname: String(r.hostname),
        query_duration_ms: Number(r.query_duration_ms) || 0,
        memory_usage: Number(r.memory_usage) || 0,
        read_rows: Number(r.read_rows) || 0,
        read_bytes: Number(r.read_bytes) || 0,
        query_preview: String(r.query_preview || ''),
        exception_code: Number(r.exception_code) || 0,
        exception: String(r.exception || ''),
      }));
    } catch (error) {
      throw new QueryAnalysisError('Failed to get sub-queries', error as Error);
    }
  }

  /**
   * Get the set of initial_query_id values that have shard sub-queries,
   * scoped to the given candidate query IDs.
   * Single lightweight query — used to tag coordinator queries in the history table.
   */
  async getCoordinatorIds(queryIds: string[], startDate: string): Promise<Set<string>> {
    if (queryIds.length === 0) return new Set();
    const idList = queryIds.map(id => `'${escapeValue(id)}'`).join(',');
    let sql = COORDINATOR_IDS.replace('{{query_id_list}}', idList);
    sql = buildQuery(sql, { start_date: startDate });
    try {
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_QUERIES, 'coordinatorIds')));
      return new Set(rows.map((r: any) => String(r.initial_query_id)));
    } catch {
      return new Set();
    }
  }

  /**
   * Get the set of initial_query_id values from currently running shard sub-queries.
   * Single lightweight query — used to tag coordinator queries in the running queries list.
   */
  async getRunningCoordinatorIds(): Promise<Set<string>> {
    try {
      const rows = await this.adapter.executeQuery(tagQuery(RUNNING_COORDINATOR_IDS, sourceTag(TAB_QUERIES, 'runningCoordinatorIds')));
      return new Set(rows.map((r: any) => String(r.initial_query_id)));
    } catch {
      return new Set();
    }
  }

  /**
   * Find similar queries by hash.
   * When mode is 'normalized' (default), matches by normalized_query_hash (same structure, different literals).
   * When mode is 'exact', matches by sipHash64(query) (byte-identical SQL).
   * Searches within the last 30 days for a comprehensive history view.
   */
  async getSimilarQueries(hash: string, limit = 500, hashMode: 'normalized' | 'exact' = 'normalized'): Promise<SimilarQuery[]> {
    // Both hash types are UInt64 in ClickHouse — validate as numeric to prevent
    // SQL injection. The hash comes from query_log data shown in the UI, but
    // it flows through user-controlled URL/message params so we must sanitize.
    if (!/^\d+$/.test(hash)) {
      throw new QueryAnalysisError(`Invalid hash value: expected numeric UInt64, got '${hash}'`);
    }
    const whereClause = hashMode === 'exact'
      ? `sipHash64(query) = ${hash}`
      : `normalized_query_hash = ${hash}`;
    // 30-day window is intentional — similar-query analysis looks at recent history
    const sql = `
      SELECT * FROM (
        SELECT
          query_id,
          query_start_time,
          query_duration_ms,
          read_rows,
          read_bytes,
          result_rows,
          memory_usage,
          toUInt64(ProfileEvents['UserTimeMicroseconds']) + toUInt64(ProfileEvents['SystemTimeMicroseconds']) AS cpu_time_us,
          user,
          hostName() AS client_hostname,
          exception_code,
          exception,
          Settings,
          query,
          query_kind
        FROM {{cluster_aware:system.query_log}}
        WHERE ${whereClause}
          AND type IN ('QueryFinish', 'ExceptionWhileProcessing')
          AND event_date >= today() - 30
        ORDER BY query_start_time DESC
        LIMIT ${limit}
      )
      ORDER BY query_start_time ASC
    `;
    try {
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_QUERIES, 'similarQueries')));
      return rows as unknown as SimilarQuery[];
    } catch (error) {
      throw new QueryAnalysisError('Failed to get similar queries', error as Error);
    }
  }

  /**
   * Get default values for specific settings from system.settings.
   * Useful for showing what the default was before it was overridden.
   */
  async getSettingsDefaults(settingNames: string[]): Promise<SettingDefault[]> {
    if (settingNames.length === 0) return [];
    
    // Build the IN clause with quoted setting names
    const quotedNames = settingNames.map(n => `'${n.replace(/'/g, "''")}'`).join(', ');
    const sql = `
      SELECT
        name,
        default,
        description,
        type
      FROM system.settings
      WHERE name IN (${quotedNames})
      ORDER BY name
    `;
    try {
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_QUERIES, 'settingsDefaults')));
      return rows as unknown as SettingDefault[];
    } catch (error) {
      throw new QueryAnalysisError('Failed to get settings defaults', error as Error);
    }
  }

  /**
   * Get the query_log flush interval configured on the server, in milliseconds.
   * Falls back to 7500 ms if the setting cannot be read.
   */
  async getQueryLogFlushIntervalMs(): Promise<number> {
    const rows = await this.adapter.executeQuery<{ value: string }>(
      tagQuery(QUERY_LOG_FLUSH_INTERVAL, sourceTag(TAB_INTERNAL, 'queryLogFlushInterval'))
    ).catch((error: Error) => {
      console.warn('Could not read query_log flush interval from server_settings, using default 7500ms:', error.message);
      return [] as { value: string }[];
    });
    if (rows.length > 0) {
      const parsed = Number(rows[0].value);
      if (parsed > 0) return parsed;
    }
    return 7500;
  }

  /**
   * Get server CPU usage timeseries for a time range.
   * Returns ~100 aggregated buckets with avg CPU percentage (0-100).
   * Used to overlay server load on query history charts.
   */
  async getServerCpuForRange(startTime: string, endTime: string): Promise<{ t: string; cpu_pct: number }[]> {
    const start = toClickHouseDateTime(startTime);
    const end = toClickHouseDateTime(endTime);

    // Use EnvironmentDetector for cgroup-aware core count, with fallback
    let cpuCores = 1;
    if (this.envDetector) {
      const env = await this.envDetector.detect();
      if (env.effectiveCores > 0) cpuCores = env.effectiveCores;
    }
    if (cpuCores <= 1 && !this.envDetector) {
      // Legacy fallback when no envDetector is provided
      try {
        const coreRows = await this.adapter.executeQuery(
          tagQuery(`SELECT hostname() AS host, value FROM {{cluster_aware:system.asynchronous_metrics}} WHERE metric = 'NumberOfCPUCores' GROUP BY host, value`, sourceTag(TAB_QUERIES, 'cpuCores'))
        );
        if (coreRows.length > 0) {
          const values = coreRows.map(r => Number((r as Record<string, unknown>).value || 0)).filter(v => v > 0);
          cpuCores = values.length > 0 ? Math.min(...values) : 1;
        }
      } catch {
        try {
          const coreRows = await this.adapter.executeQuery(
            tagQuery(`SELECT value FROM system.asynchronous_metrics WHERE metric = 'NumberOfCPUCores' LIMIT 1`, sourceTag(TAB_QUERIES, 'cpuCores'))
          );
          if (coreRows.length > 0) {
            cpuCores = Math.max(1, Number((coreRows[0] as Record<string, unknown>).value || 1));
          }
        } catch { /* fallback to 1 core */ }
      }
    }

    // Aggregate into ~100 time buckets server-side to avoid transferring 86k+ rows
    const sql = buildQuery(`
      SELECT
        toString(min(event_time)) AS t,
        avg(ProfileEvent_OSCPUVirtualTimeMicroseconds) AS avg_cpu_us
      FROM {{cluster_aware:system.metric_log}}
      WHERE event_time >= {start_time}
        AND event_time <= {end_time}
      GROUP BY intDiv(toUnixTimestamp(event_time) - toUnixTimestamp({start_time}),
               greatest(1, intDiv(toUnixTimestamp({end_time}) - toUnixTimestamp({start_time}), 100)))
      ORDER BY t ASC
    `, { start_time: start, end_time: end });

    try {
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_QUERIES, 'serverCpu')));
      // Each bucket's avg_cpu_us is the average µs of CPU used per metric_log sample in that bucket.
      // metric_log samples every ~1s, so cpu_us per sample ≈ cpu_us per second.
      // 100% = cpuCores * 1_000_000 µs/s
      const fullUtil = cpuCores * 1_000_000;
      return rows.map(r => {
        const row = r as Record<string, unknown>;
        const avgCpuUs = Number(row.avg_cpu_us || 0);
        const pct = fullUtil > 0 ? Math.min(100, (avgCpuUs / fullUtil) * 100) : 0;
        return { t: String(row.t), cpu_pct: pct };
      });
    } catch (error) {
      throw new QueryAnalysisError('Failed to get server CPU data', error as Error);
    }
  }

  /**
   * Get server memory usage timeseries for a time range.
   * Returns ~100 aggregated buckets with avg memory percentage (0-100).
   */
  async getServerMemoryForRange(startTime: string, endTime: string): Promise<{ t: string; mem_pct: number }[]> {
    const start = toClickHouseDateTime(startTime);
    const end = toClickHouseDateTime(endTime);

    // Get total RAM — use min across hosts for conservative percentage
    // In containers, OSMemoryTotal reports host RAM — check cgroup limit
    let totalRam = 0;
    try {
      const ramRows = await this.adapter.executeQuery(
        tagQuery(`SELECT hostname() AS host, metric, value FROM {{cluster_aware:system.asynchronous_metrics}} WHERE metric IN ('OSMemoryTotal', 'CGroupMemoryLimit', 'CGroupMemoryTotal') GROUP BY host, metric, value`, sourceTag(TAB_QUERIES, 'totalRam'))
      );
      if (ramRows.length > 0) {
        // Group by host, pick effective memory per host
        const hostMem = new Map<string, number>();
        const hostCgroupMem = new Map<string, number>();
        for (const r of ramRows) {
          const row = r as Record<string, unknown>;
          const host = String(row.host || '');
          const metric = String(row.metric || '');
          const val = Number(row.value || 0);
          if (metric === 'OSMemoryTotal') hostMem.set(host, val);
          if ((metric === 'CGroupMemoryTotal' || metric === 'CGroupMemoryLimit') && val > 0 && val < 1e18) {
            const existing = hostCgroupMem.get(host) ?? 0;
            if (val > existing) hostCgroupMem.set(host, val);
          }
        }
        const effectiveValues: number[] = [];
        for (const [host, mem] of hostMem) {
          const cgroup = hostCgroupMem.get(host) ?? 0;
          effectiveValues.push((cgroup > 0 && cgroup < mem) ? cgroup : mem);
        }
        totalRam = effectiveValues.length > 0 ? Math.min(...effectiveValues) : 0;
      }
    } catch {
      // Fallback: local-only query
      try {
        const ramRows = await this.adapter.executeQuery(
          tagQuery(`SELECT metric, value FROM system.asynchronous_metrics WHERE metric IN ('OSMemoryTotal', 'CGroupMemoryLimit', 'CGroupMemoryTotal')`, sourceTag(TAB_QUERIES, 'totalRam'))
        );
        let hostRam = 0;
        let cgroupMem = 0;
        for (const r of ramRows) {
          const row = r as Record<string, unknown>;
          const metric = String(row.metric || '');
          const val = Number(row.value || 0);
          if (metric === 'OSMemoryTotal') hostRam = val;
          if ((metric === 'CGroupMemoryTotal' || metric === 'CGroupMemoryLimit') && val > 0 && val < 1e18) cgroupMem = Math.max(cgroupMem, val);
        }
        totalRam = (cgroupMem > 0 && cgroupMem < hostRam) ? cgroupMem : hostRam;
      } catch { /* no total RAM available */ }
    }

    if (totalRam <= 0) return [];

    const sql = buildQuery(`
      SELECT
        toString(min(event_time)) AS t,
        avg(CurrentMetric_MemoryTracking) AS avg_mem
      FROM {{cluster_aware:system.metric_log}}
      WHERE event_time >= {start_time}
        AND event_time <= {end_time}
      GROUP BY intDiv(toUnixTimestamp(event_time) - toUnixTimestamp({start_time}),
               greatest(1, intDiv(toUnixTimestamp({end_time}) - toUnixTimestamp({start_time}), 100)))
      ORDER BY t ASC
    `, { start_time: start, end_time: end });

    try {
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_QUERIES, 'serverMemory')));
      return rows.map(r => {
        const row = r as Record<string, unknown>;
        const avgMem = Number(row.avg_mem || 0);
        const pct = Math.min(100, (avgMem / totalRam) * 100);
        return { t: String(row.t), mem_pct: pct };
      });
    } catch (error) {
      throw new QueryAnalysisError('Failed to get server memory data', error as Error);
    }
  }

  /**
   * Get per-thread breakdown for a query from system.query_thread_log.
   * Returns null if the table is not available or no data found.
   */
  async getQueryThreadBreakdown(queryId: string, eventDate?: string): Promise<QueryThreadBreakdown[]> {
    const sql = buildQuery(QUERY_THREAD_BREAKDOWN.replace('{event_date_bound}', eventDateBound(eventDate)), { query_id: queryId });
    try {
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_QUERIES, 'threadBreakdown')));
      return rows.map(r => {
        const row = r as Record<string, unknown>;
        return {
          thread_name: String(row.thread_name ?? ''),
          thread_id: Number(row.thread_id ?? 0),
          query_duration_ms: Number(row.query_duration_ms ?? 0),
          read_rows: Number(row.read_rows ?? 0),
          read_bytes: Number(row.read_bytes ?? 0),
          written_rows: Number(row.written_rows ?? 0),
          written_bytes: Number(row.written_bytes ?? 0),
          memory_usage: Number(row.memory_usage ?? 0),
          peak_memory_usage: Number(row.peak_memory_usage ?? 0),
          event_time_us: String(row.event_time_microseconds ?? ''),
          query_start_time_us: String(row.query_start_time_microseconds ?? ''),
          initial_query_start_time_us: String(row.initial_query_start_time_microseconds ?? ''),
          cpu_time_us: Number(row.cpu_time_us ?? 0),
          user_time_us: Number(row.user_time_us ?? 0),
          system_time_us: Number(row.system_time_us ?? 0),
          io_wait_us: Number(row.io_wait_us ?? 0),
          real_time_us: Number(row.real_time_us ?? 0),
          disk_read_bytes: Number(row.disk_read_bytes ?? 0),
          disk_write_bytes: Number(row.disk_write_bytes ?? 0),
          network_send_bytes: Number(row.network_send_bytes ?? 0),
          network_receive_bytes: Number(row.network_receive_bytes ?? 0),
        };
      });
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error);
      // Return empty array for known non-fatal errors instead of throwing
      if (msg.includes('UNKNOWN_TABLE') || msg.includes("doesn't exist") || msg.includes('does not exist')) {
        console.log('[QueryAnalyzer] query_thread_log table not available');
        return [];
      }
      throw new QueryAnalysisError(`Failed to get query thread breakdown: ${msg}`, error as Error);
    }
  }

  /**
   * Compare all ProfileEvents between two queries, returning per-metric
   * deltas in both percentage and decibels.
   *
   * Based on the approach from:
   * https://clickhouse.com/docs/knowledgebase/comparing-metrics-between-queries
   *
   * Uses ARRAY JOIN on ProfileEvents to get every metric that differs.
   * The first query_id is the baseline; positive dB/perc means the second query used more.
   */
  async compareQueryProfileEvents(
    queryId1: string,
    queryId2: string,
    eventDates?: string[],
  ): Promise<ProfileEventComparison[]> {
    // We inject query IDs directly since buildQuery quotes them and we need string comparison
    const escapedId1 = queryId1.replace(/'/g, "''");
    const escapedId2 = queryId2.replace(/'/g, "''");

    // Compute the earliest date bound across both queries (with 30-day fallback)
    const dateBound = this.earliestDateBound(eventDates, 30);
    const sql = `
      WITH
        query_id = '${escapedId1}' AS first,
        query_id = '${escapedId2}' AS second
      SELECT
        PE.1 AS metric,
        sumIf(PE.2, first) AS v1,
        sumIf(PE.2, second) AS v2,
        if(v1 > 0 AND v2 > 0, 10 * log10(v2 / v1), 0) AS dB,
        if(v1 != v2, round(((v2 - v1) / if(v2 > v1, v2, v1)) * 100, 2), 0) AS perc
      FROM {{cluster_aware:system.query_log}}
      ARRAY JOIN ProfileEvents AS PE
      WHERE (first OR second)
        AND event_date >= ${dateBound}
        AND type IN ('QueryFinish', 'ExceptionWhileProcessing')
      GROUP BY metric
      HAVING v1 != v2
      ORDER BY dB DESC, v2 DESC, metric ASC
    `;

    try {
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_QUERIES, 'profileCompare')));
      return rows.map(r => {
        const row = r as Record<string, unknown>;
        return {
          metric: String(row.metric ?? ''),
          v1: Number(row.v1 ?? 0),
          v2: Number(row.v2 ?? 0),
          dB: Number(row.dB ?? 0),
          perc: Number(row.perc ?? 0),
        };
      });
    } catch (error) {
      throw new QueryAnalysisError('Failed to compare query profile events', error as Error);
    }
  }

  /**
   * Compare ProfileEvents across N queries (2+).
   * Returns per-metric values for each query, ordered by max spread.
   */
  async compareMultipleQueryProfileEvents(
    queryIds: string[],
    eventDates?: string[],
  ): Promise<MultiProfileEventRow[]> {
    if (queryIds.length < 2) {
      throw new QueryAnalysisError('Need at least 2 query IDs for comparison');
    }

    const escaped = queryIds.map(id => `'${id.replace(/'/g, "''")}'`);
    const caseLines = escaped.map((eid, i) =>
      `sumIf(PE.2, query_id = ${eid}) AS v${i}`
    ).join(',\n        ');

    const dateBound = this.earliestDateBound(eventDates, 30);
    const sql = `
      SELECT
        PE.1 AS metric,
        ${caseLines}
      FROM {{cluster_aware:system.query_log}}
      ARRAY JOIN ProfileEvents AS PE
      WHERE query_id IN (${escaped.join(', ')})
        AND event_date >= ${dateBound}
        AND type IN ('QueryFinish', 'ExceptionWhileProcessing')
      GROUP BY metric
      HAVING ${queryIds.map((_, i) => `v${i}`).join(' + ')} > 0
      ORDER BY metric ASC
    `;

    try {
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_QUERIES, 'profileCompareMulti')));
      return rows.map(r => {
        const row = r as Record<string, unknown>;
        const values = queryIds.map((_, i) => Number(row[`v${i}`] ?? 0));
        return {
          metric: String(row.metric ?? ''),
          values,
        };
      });
    } catch (error) {
      throw new QueryAnalysisError('Failed to compare multiple query profile events', error as Error);
    }
  }

  /**
   * Compute the earliest event_date bound across multiple query dates.
   * Used by comparison methods that need to cover all compared queries.
   */
  private earliestDateBound(eventDates?: string[], fallbackDays = 30): string {
    if (!eventDates || eventDates.length === 0) return `today() - ${fallbackDays}`;
    const validDates = eventDates.filter(Boolean);
    if (validDates.length === 0) return `today() - ${fallbackDays}`;
    // Find the earliest date and use eventDateBound on it
    const sorted = validDates.map(d => d.slice(0, 10)).sort();
    return eventDateBound(sorted[0], fallbackDays);
  }

  /**
   * Fetch human-readable descriptions for all profile events from system.events.
   * Intended to be called once at connection time and cached.
   */
  async fetchProfileEventDescriptions(): Promise<Record<string, string>> {
    const rows = await this.adapter.executeQuery(tagQuery(PROFILE_EVENT_DESCRIPTIONS, sourceTag(TAB_INTERNAL, 'profileEventDescriptions')));
    const map: Record<string, string> = {};
    for (const r of rows) {
      const row = r as Record<string, unknown>;
      const name = String(row.event ?? '');
      const desc = String(row.description ?? '');
      if (name) map[name] = desc;
    }
    return map;
  }

}
