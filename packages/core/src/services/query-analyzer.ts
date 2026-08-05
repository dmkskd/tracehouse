import type { IClickHouseAdapter } from '../adapters/types.js';
import type { QueryMetrics, QueryHistoryItem } from '../types/query.js';
import { RUNNING_QUERIES, QUERY_DETAIL, QUERY_THREAD_BREAKDOWN, PROFILE_EVENT_DESCRIPTIONS, SUB_QUERIES, BATCH_SUB_QUERIES, COORDINATOR_IDS, RUNNING_COORDINATOR_IDS, QUERY_LOG_FLUSH_INTERVAL, DISTRIBUTED_TOPOLOGY_EXECUTIONS, DISTRIBUTED_TOPOLOGY_EXECUTIONS_BY_QUERY_IDS, DISTRIBUTED_TOPOLOGY_CLUSTER_HOSTS, DISTRIBUTED_TOPOLOGY_PROCESSORS, withProcessorPlanStepCapability, DISTRIBUTED_TOPOLOGY_TEXT_LOGS, DISTRIBUTED_TOPOLOGY_ASYNC_INSERT_LOGS, buildColumnCommentsSQL } from '../queries/query-queries.js';
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
import {
  buildQuery,
  tagQuery,
  eventDateBound,
  escapeValue,
  utcDateTime,
} from '../queries/builder.js';
import type { QueryParameter } from '../queries/builder.js';
import { TAB_QUERIES, TAB_INTERNAL, APP_SOURCE_PREFIX, sourceTag } from '../queries/source-tags.js';
import { mapQueryMetrics, mapQueryHistoryItem } from '../mappers/query-mappers.js';
import { shortenHostname } from '../mappers/helpers.js';
import {
  inferDistributedTopology,
  type AsyncInsertLogInput,
  type ClusterHostInput,
  type DistributedQueryExecutionInput,
  type DistributedTextLogInput,
  type DistributedTopology,
  type ProcessorProfileCompatibility,
  type ProcessorProfileInput,
  type ProfileEventsMap,
} from './distributed-query-topology.js';

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
 * Node child-query summary for distributed queries
 */
export interface SubQueryInfo {
  query_id: string;
  normalized_query_hash: string;
  hostname: string;
  query_duration_ms: number;
  memory_usage: number;
  read_rows: number;
  read_bytes: number;
  selected_parts: number;
  selected_parts_total: number;
  selected_marks: number;
  selected_marks_total: number;
  selected_ranges: number;
  query_preview: string;
  exception_code: number;
  exception: string;
  query_start_time_microseconds: string;
}

export type SubQueriesByInitialQueryId = Map<string, SubQueryInfo[]>;

function parseStringArray(raw: unknown): string[] {
  if (Array.isArray(raw)) return raw.map(String);
  if (typeof raw === 'string') {
    try {
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) return parsed.map(String);
    } catch {
      return raw ? [raw] : [];
    }
  }
  return [];
}

function parseProfileEvents(raw: unknown): ProfileEventsMap {
  if (!raw || typeof raw !== 'object') return {};
  const result: ProfileEventsMap = {};
  for (const [key, value] of Object.entries(raw as Record<string, unknown>)) {
    const parsed = Number(value ?? 0);
    result[key] = Number.isFinite(parsed) ? parsed : String(value ?? '');
  }
  return result;
}

function parseSettings(raw: unknown): Record<string, string | number | boolean | undefined> {
  if (!raw) return {};
  if (typeof raw === 'object' && !Array.isArray(raw)) {
    return Object.fromEntries(Object.entries(raw as Record<string, unknown>).map(([key, value]) => [key, String(value ?? '')]));
  }
  if (typeof raw === 'string') {
    try {
      const parsed = JSON.parse(raw);
      if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
        return Object.fromEntries(Object.entries(parsed as Record<string, unknown>).map(([key, value]) => [key, String(value ?? '')]));
      }
    } catch {
      return {};
    }
  }
  return {};
}

function mapDistributedExecutionRow(row: Record<string, unknown>): DistributedQueryExecutionInput {
  return {
    queryId: String(row.query_id ?? ''),
    initialQueryId: String(row.initial_query_id ?? ''),
    isInitialQuery: Number(row.is_initial_query ?? 0) === 1,
    normalizedQueryHash: String(row.normalized_query_hash ?? ''),
    hostname: String(row.hostname ?? ''),
    queryKind: String(row.query_kind ?? ''),
    queryStartTimeMicroseconds: String(row.query_start_time_microseconds ?? ''),
    queryDurationMs: Number(row.query_duration_ms ?? 0),
    memoryUsage: Number(row.memory_usage ?? 0),
    readRows: Number(row.read_rows ?? 0),
    readBytes: Number(row.read_bytes ?? 0),
    writtenRows: Number(row.written_rows ?? 0),
    writtenBytes: Number(row.written_bytes ?? 0),
    resultRows: Number(row.result_rows ?? 0),
    resultBytes: Number(row.result_bytes ?? 0),
    tables: parseStringArray(row.tables),
    settings: parseSettings(row.Settings),
    queryPreview: String(row.query_preview ?? ''),
    profileEvents: parseProfileEvents(row.ProfileEvents),
  };
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
  user?: string | string[];
  query_id?: string | string[];
  query_text?: string;
  min_duration_ms?: number;
  min_memory_bytes?: number;
  exclude_app_queries?: boolean;
  /** Filter by query kind (SELECT, INSERT, etc.) */
  query_kind?: string | string[];
  /** Filter by activity status: 'running', 'success', and/or 'error'. */
  status?: string | string[];
  /** Filter terminal failures by ClickHouse exception code. */
  exception_code?: number | number[];
  /** Filter by database name (case-insensitive contains on databases array) */
  database?: string | string[];
  /** Filter by table name (case-insensitive contains on tables array) */
  table?: string | string[];
  /** Filter by ClickHouse server hostname (case-insensitive contains). */
  hostname?: string | string[];
}

function filterValues(value?: string | string[]): string[] {
  if (Array.isArray(value)) return value.map(item => item.trim()).filter(Boolean);
  return value?.trim() ? [value.trim()] : [];
}

function errorDetail(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export class QueryAnalyzer {
  private envDetector: import('./environment-detector.js').EnvironmentDetector | null;

  constructor(
    private adapter: IClickHouseAdapter,
    envDetector?: import('./environment-detector.js').EnvironmentDetector,
  ) {
    this.envDetector = envDetector ?? null;
  }
  async getRunningQueries(limit?: number): Promise<QueryMetrics[]> {
    try {
      const normalizedLimit = limit == null
        ? null
        : Math.max(1, Math.floor(limit));
      const sql = normalizedLimit == null
        ? RUNNING_QUERIES
        : `${RUNNING_QUERIES}\n  LIMIT ${normalizedLimit}`;
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_QUERIES, 'runningQueries')));
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
      // Resolve hostName() on the same cluster-aware execution path used by
      // query rows. system.clusters.host_name may be a connection alias such
      // as "localhost", which does not match the server identity recorded by
      // system.processes and system.query_log.
      sql = `SELECT DISTINCT hostName() AS hostname FROM {{cluster_aware:system.one}} ORDER BY hostname LIMIT ${limit}`;
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
    
    const params: Record<string, QueryParameter> = {
      start_date: options.start_date,
      start_time: utcDateTime(options.start_time),
      end_time: utcDateTime(options.end_time),
      limit,
    };

    // Build WHERE conditions dynamically
    let whereConditions = [
      "event_date >= {start_date}",
      "event_time >= {start_time}",
      "event_time <= {end_time}",
      "type IN ('QueryFinish', 'ExceptionBeforeStart', 'ExceptionWhileProcessing')"
    ];

    const users = filterValues(options.user);
    if (users.length) {
      const userList = users.map(value => `'${escapeValue(value)}'`).join(', ');
      whereConditions.push(`user IN (${userList})`);
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

    const queryIds = filterValues(options.query_id);
    if (queryIds.length) {
      const ids = queryIds.flatMap(value =>
        value.split(/[\s,]+/).map(item => item.trim()).filter(Boolean)
      );
      if (ids.length > 1) {
        // Multiple IDs: exact match via IN list
        const inList = ids.map(id => `'${escapeValue(id)}'`).join(', ');
        whereConditions.push(`query_id IN (${inList})`);
      } else {
        // Single value: substring match (existing behaviour)
        whereConditions.push("positionCaseInsensitive(query_id, {query_id}) > 0");
        params.query_id = ids[0]!;
      }
    }

    if (options.exclude_app_queries) {
      whereConditions.push(`positionCaseInsensitive(query, {exclude_app_tag}) = 0`);
      params.exclude_app_tag = APP_SOURCE_PREFIX;
    }

    const queryKinds = filterValues(options.query_kind);
    if (queryKinds.length) {
      const queryKindList = queryKinds.map(value => `'${escapeValue(value)}'`).join(', ');
      whereConditions.push(`query_kind IN (${queryKindList})`);
    }

    const statusValues = filterValues(options.status);
    if (statusValues.length) {
      const statuses = new Set(statusValues.map(value => value.toLowerCase()));
      const terminalTypes: string[] = [];
      if (statuses.has('success')) terminalTypes.push('QueryFinish');
      if (statuses.has('error')) {
        terminalTypes.push('ExceptionBeforeStart', 'ExceptionWhileProcessing');
      }
      if (terminalTypes.length === 0) {
        // Running queries come from system.processes, never query_log.
        whereConditions.push('0');
      } else if (terminalTypes.length === 1) {
        whereConditions.push(`type = '${terminalTypes[0]}'`);
      } else if (terminalTypes.length < 3) {
        whereConditions.push(`type IN (${terminalTypes.map(type => `'${type}'`).join(', ')})`);
      }
    }

    const exceptionCodes = (Array.isArray(options.exception_code)
      ? options.exception_code
      : options.exception_code == null ? [] : [options.exception_code])
      .filter(code => Number.isInteger(code) && code >= 0);
    if (exceptionCodes.length === 1) {
      whereConditions.push('exception_code = {exception_code}');
      params.exception_code = exceptionCodes[0]!;
    } else if (exceptionCodes.length > 1) {
      whereConditions.push(`exception_code IN (${exceptionCodes.join(', ')})`);
    }

    const databases = filterValues(options.database);
    if (databases.length) {
      const matches = databases.map((value, index) => {
        const key = `filter_database_${index}`;
        params[key] = value;
        return `positionCaseInsensitive(x, {${key}}) > 0`;
      });
      whereConditions.push(`arrayExists(x -> ${matches.length === 1 ? matches[0] : `(${matches.join(' OR ')})`}, databases)`);
    }

    const tables = filterValues(options.table);
    if (tables.length) {
      const matches = tables.map((value, index) => {
        const key = `filter_table_${index}`;
        params[key] = value;
        return `positionCaseInsensitive(x, {${key}}) > 0`;
      });
      whereConditions.push(`arrayExists(x -> ${matches.length === 1 ? matches[0] : `(${matches.join(' OR ')})`}, tables)`);
    }

    const hostnames = filterValues(options.hostname);
    if (hostnames.length) {
      const matches = hostnames.map((value, index) => {
        const key = `filter_hostname_${index}`;
        params[key] = value;
        return `positionCaseInsensitive(hostName(), {${key}}) > 0`;
      });
      whereConditions.push(matches.length === 1 ? matches[0]! : `(${matches.join(' OR ')})`);
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
        exception_code,
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
      const conditions = parsed.map(p =>
        `(database = '${escapeValue(p.db)}' AND table = '${escapeValue(p.tbl)}')`
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

  /** Load non-empty comments for qualified columns recorded in query_log. */
  async getColumnComments(qualifiedColumns: string[]): Promise<Record<string, string>> {
    const parsed = [...new Set(qualifiedColumns)].flatMap(qualifiedName => {
      const parts = qualifiedName.split('.');
      if (parts.length < 3) return [];
      return [{
        database: parts[0],
        table: parts[1],
        name: parts.slice(2).join('.'),
      }];
    });
    if (parsed.length === 0) return {};

    try {
      const rows = await this.adapter.executeQuery<Record<string, unknown>>(tagQuery(
        buildColumnCommentsSQL(parsed),
        sourceTag(TAB_QUERIES, 'columnComments'),
      ));
      const comments: Record<string, string> = {};
      for (const row of rows) {
        const qualifiedName = `${String(row.database ?? '')}.${String(row.table ?? '')}.${String(row.name ?? '')}`;
        const comment = String(row.comment ?? '');
        if (comment) comments[qualifiedName] = comment;
      }
      return comments;
    } catch (err) {
      console.warn('[QueryAnalyzer] Failed to fetch column comments:', err);
      return {};
    }
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
        normalized_query_hash: String(r.normalized_query_hash || ''),
        hostname: String(r.hostname),
        query_duration_ms: Number(r.query_duration_ms) || 0,
        memory_usage: Number(r.memory_usage) || 0,
        read_rows: Number(r.read_rows) || 0,
        read_bytes: Number(r.read_bytes) || 0,
        selected_parts: Number(r.selected_parts) || 0,
        selected_parts_total: Number(r.selected_parts_total) || 0,
        selected_marks: Number(r.selected_marks) || 0,
        selected_marks_total: Number(r.selected_marks_total) || 0,
        selected_ranges: Number(r.selected_ranges) || 0,
        query_preview: String(r.query_preview || ''),
        exception_code: Number(r.exception_code) || 0,
        exception: String(r.exception || ''),
        query_start_time_microseconds: String(r.query_start_time_microseconds || ''),
      }));
    } catch (error) {
      throw new QueryAnalysisError('Failed to get sub-queries', error as Error);
    }
  }

  /**
   * Get inferred distributed topology for a query using richer, raw evidence.
   *
   * Optional sources degrade independently:
   * - system.clusters supplies host -> shard/replica mapping.
   * - processors_profile_log supplies structured execution phase hints.
   * - system.text_log supplies last-resort human-readable execution phase breadcrumbs.
   * - query_log/ProfileEvents remains the baseline.
   */
  async getDistributedTopology(
    initialQueryId: string,
    eventDate?: string,
    connectionProcessorCompatibility: ProcessorProfileCompatibility = {
      mode: 'unavailable',
      reason: 'capability_not_probed',
      message: 'Processor-profile compatibility has not been probed for this connection; processor enrichment was not run.',
    },
  ): Promise<DistributedTopology> {
    const boundedEventDate = eventDateBound(eventDate);
    const executionsSql = buildQuery(
      DISTRIBUTED_TOPOLOGY_EXECUTIONS.replace('{event_date_bound}', boundedEventDate),
      { initial_query_id: initialQueryId },
    );
    const clusterHostsSql = DISTRIBUTED_TOPOLOGY_CLUSTER_HOSTS;

    try {
      const executionRows = await this.adapter.executeQuery<Record<string, unknown>>(
        tagQuery(executionsSql, sourceTag(TAB_QUERIES, 'distributedTopologyExecutions')),
      );

      const clusterHostRows = await this.adapter.executeQuery<Record<string, unknown>>(
        tagQuery(clusterHostsSql, sourceTag(TAB_QUERIES, 'distributedTopologyClusterHosts')),
      ).catch(() => null);

      let processorProfileCompatibility = connectionProcessorCompatibility;
      let processorRows: Record<string, unknown>[] = [];
      if (processorProfileCompatibility.mode !== 'unavailable') {
        const processorsSql = buildQuery(
          withProcessorPlanStepCapability(
            DISTRIBUTED_TOPOLOGY_PROCESSORS,
            processorProfileCompatibility.mode === 'full',
          ).replace('{event_date_bound}', boundedEventDate),
          { initial_query_id: initialQueryId },
        );
        try {
          processorRows = await this.adapter.executeQuery<Record<string, unknown>>(
            tagQuery(processorsSql, sourceTag(TAB_QUERIES, 'distributedTopologyProcessors')),
          );
        } catch (error) {
          processorProfileCompatibility = {
            mode: 'unavailable',
            reason: 'query_failed',
            message: 'Processor-profile enrichment could not be loaded; topology uses query_log/ProfileEvents only.',
            detail: errorDetail(error),
          };
        }
      }

      let executions: DistributedQueryExecutionInput[] = executionRows.map(mapDistributedExecutionRow);

      const hasWriteExecutions = executions.some(execution => ['Insert', 'AsyncInsertFlush'].includes(execution.queryKind ?? ''));
      const asyncSeedQueryIds = hasWriteExecutions
        ? [...new Set([
            initialQueryId,
            ...executions.map(execution => execution.queryId),
          ].filter(Boolean))]
        : [];
      let asyncInsertLogRows: Record<string, unknown>[] | null = null;
      if (asyncSeedQueryIds.length > 0) {
        const queryIdList = asyncSeedQueryIds.map(id => `'${escapeValue(id)}'`).join(',');
        const asyncInsertLogsSql = DISTRIBUTED_TOPOLOGY_ASYNC_INSERT_LOGS
          .replaceAll('{{query_id_list}}', queryIdList)
          .replace('{event_date_bound}', boundedEventDate);
        asyncInsertLogRows = await this.adapter.executeQuery<Record<string, unknown>>(
          tagQuery(asyncInsertLogsSql, sourceTag(TAB_QUERIES, 'distributedTopologyAsyncInsertLogs')),
        ).catch(() => null);
      }

      const asyncInsertLogs: AsyncInsertLogInput[] = (asyncInsertLogRows ?? []).map((row) => ({
        queryId: String(row.query_id ?? ''),
        flushQueryId: String(row.flush_query_id ?? ''),
        hostname: String(row.hostname ?? ''),
        database: String(row.database ?? ''),
        table: String(row.table ?? ''),
        format: String(row.format ?? ''),
        dataKind: String(row.data_kind ?? ''),
        status: String(row.status ?? ''),
        exception: String(row.exception ?? ''),
        rows: Number(row.rows ?? 0),
        bytes: Number(row.bytes ?? 0),
        eventTimeMicroseconds: String(row.event_time_microseconds ?? ''),
        flushTimeMicroseconds: String(row.flush_time_microseconds ?? ''),
        timeoutMilliseconds: Number(row.timeout_milliseconds ?? 0),
      })).filter(row => row.queryId && row.flushQueryId);

      const knownExecutionIds = new Set(executions.map(execution => execution.queryId));
      const missingLinkedQueryIds = [...new Set(asyncInsertLogs
        .flatMap(row => [row.queryId, row.flushQueryId])
        .filter(id => id && !knownExecutionIds.has(id)))];
      if (missingLinkedQueryIds.length > 0) {
        const queryIdList = missingLinkedQueryIds.map(id => `'${escapeValue(id)}'`).join(',');
        const linkedExecutionsSql = DISTRIBUTED_TOPOLOGY_EXECUTIONS_BY_QUERY_IDS
          .replace('{{query_id_list}}', queryIdList)
          .replace('{event_date_bound}', boundedEventDate);
        const linkedExecutionRows = await this.adapter.executeQuery<Record<string, unknown>>(
          tagQuery(linkedExecutionsSql, sourceTag(TAB_QUERIES, 'distributedTopologyLinkedExecutions')),
        ).catch(() => []);
        executions = [...executions, ...linkedExecutionRows.map(mapDistributedExecutionRow)];
      }

      const clusterHosts: ClusterHostInput[] = (clusterHostRows ?? []).map((row) => ({
        hostName: String(row.host_name ?? ''),
        shardNum: Number(row.shard_num ?? 0),
        replicaNum: Number(row.replica_num ?? 0),
        cluster: String(row.cluster ?? ''),
      })).filter(row => row.hostName && row.shardNum > 0 && row.replicaNum > 0);

      const processorProfiles: ProcessorProfileInput[] = processorRows.map((row) => ({
        queryId: String(row.query_id ?? ''),
        initialQueryId: String(row.initial_query_id ?? ''),
        hostname: String(row.hostname ?? ''),
        planStepName: String(row.plan_step_name ?? ''),
        planStepDescription: String(row.plan_step_description ?? ''),
        processorName: String(row.processor_name ?? ''),
      })).filter(row => row.queryId && row.hostname);

      const textLogQueryIds = [
        initialQueryId,
        ...executions.map(execution => execution.queryId),
      ].filter(Boolean);
      const uniqueTextLogQueryIds = [...new Set(textLogQueryIds)];
      let textLogRows: Record<string, unknown>[] | null = null;
      if (uniqueTextLogQueryIds.length > 0) {
        const queryIdList = uniqueTextLogQueryIds.map(id => `'${escapeValue(id)}'`).join(',');
        const textLogsSql = DISTRIBUTED_TOPOLOGY_TEXT_LOGS
          .replace('{{query_id_list}}', queryIdList)
          .replace('{event_date_bound}', boundedEventDate);
        textLogRows = await this.adapter.executeQuery<Record<string, unknown>>(
          tagQuery(textLogsSql, sourceTag(TAB_QUERIES, 'distributedTopologyTextLogs')),
        ).catch(() => null);
      }

      const textLogs: DistributedTextLogInput[] = (textLogRows ?? []).map((row) => ({
        queryId: String(row.query_id ?? ''),
        eventTimeMicroseconds: String(row.event_time_microseconds ?? ''),
        level: String(row.level ?? ''),
        source: String(row.source ?? ''),
        message: String(row.message ?? ''),
        threadName: String(row.thread_name ?? ''),
      })).filter(row => row.queryId && row.message);

      return inferDistributedTopology({
        rootQueryId: initialQueryId,
        executions,
        clusterHosts,
        processorProfiles,
        processorProfileCompatibility,
        textLogs,
        asyncInsertLogs,
        capabilities: {
          queryLog: true,
          profileEvents: true,
          systemClusters: clusterHostRows !== null,
          processorsProfileLog: processorProfileCompatibility.mode !== 'unavailable',
          textLog: textLogRows !== null,
          asynchronousInsertLog: asyncInsertLogRows !== null,
        },
      });
    } catch (error) {
      throw new QueryAnalysisError('Failed to get distributed topology', error as Error);
    }
  }

  /**
   * Get child query rows for multiple coordinator query IDs in one request.
   * Used by lightweight hover previews so they render real child executions
   * without issuing a ClickHouse query on every hover event.
   */
  async getSubQueriesForInitialQueries(
    initialQueryIds: string[],
    eventDate?: string,
    limitPerInitialQuery = 50,
  ): Promise<SubQueriesByInitialQueryId> {
    const uniqueIds = [...new Set(initialQueryIds.filter(Boolean))];
    if (uniqueIds.length === 0) return new Map();

    const idList = uniqueIds.map(id => `'${escapeValue(id)}'`).join(',');
    const boundedLimitPerInitialQuery = Math.min(Math.max(limitPerInitialQuery, 1), 200);
    let sql = BATCH_SUB_QUERIES.replace('{{initial_query_id_list}}', idList);
    sql = sql.replace('{event_date_bound}', eventDateBound(eventDate));
    sql = buildQuery(sql, { limit_per_initial_query: boundedLimitPerInitialQuery });

    const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_QUERIES, 'batchSubQueries')));
    const byInitialQueryId: SubQueriesByInitialQueryId = new Map();

    for (const r of rows as any[]) {
      const initialQueryId = String(r.initial_query_id || '');
      if (!initialQueryId) continue;

      const child: SubQueryInfo = {
        query_id: String(r.query_id),
        normalized_query_hash: String(r.normalized_query_hash || ''),
        hostname: String(r.hostname),
        query_duration_ms: Number(r.query_duration_ms) || 0,
        memory_usage: Number(r.memory_usage) || 0,
        read_rows: Number(r.read_rows) || 0,
        read_bytes: Number(r.read_bytes) || 0,
        selected_parts: Number(r.selected_parts) || 0,
        selected_parts_total: Number(r.selected_parts_total) || 0,
        selected_marks: Number(r.selected_marks) || 0,
        selected_marks_total: Number(r.selected_marks_total) || 0,
        selected_ranges: Number(r.selected_ranges) || 0,
        query_preview: String(r.query_preview || ''),
        exception_code: Number(r.exception_code) || 0,
        exception: String(r.exception || ''),
        query_start_time_microseconds: String(r.query_start_time_microseconds || ''),
      };

      const existing = byInitialQueryId.get(initialQueryId);
      if (existing) existing.push(child);
      else byInitialQueryId.set(initialQueryId, [child]);
    }

    return byInitialQueryId;
  }

  /**
   * Get the set of initial_query_id values that have child queries,
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
   * Get the set of initial_query_id values from currently running child queries.
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
    // 30-day window is intentional — similar-query analysis looks at recent history.
    // GROUP BY query_id deduplicates rows when clusterAllReplicas returns
    // identical data from multiple replicas within a shard.
    const sql = `
      SELECT * FROM (
        SELECT
          query_id,
          any(query_start_time) AS query_start_time,
          any(query_duration_ms) AS query_duration_ms,
          any(read_rows) AS read_rows,
          any(read_bytes) AS read_bytes,
          any(result_rows) AS result_rows,
          any(memory_usage) AS memory_usage,
          any(toUInt64(ProfileEvents['UserTimeMicroseconds']) + toUInt64(ProfileEvents['SystemTimeMicroseconds'])) AS cpu_time_us,
          any(user) AS user,
          any(hostName()) AS client_hostname,
          any(exception_code) AS exception_code,
          any(exception) AS exception,
          any(Settings) AS Settings,
          any(query) AS query,
          any(query_kind) AS query_kind
        FROM {{cluster_aware:system.query_log}}
        WHERE ${whereClause}
          AND type IN ('QueryFinish', 'ExceptionWhileProcessing', 'ExceptionBeforeStart')
          AND event_date >= today() - 30
        GROUP BY query_id
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
    const quotedNames = settingNames.map(n => `'${escapeValue(n)}'`).join(', ');
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
    const start = utcDateTime(startTime);
    const end = utcDateTime(endTime);

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
    const start = utcDateTime(startTime);
    const end = utcDateTime(endTime);

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
        console.warn('[QueryAnalyzer] query_thread_log unavailable; returning empty thread breakdown');
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
    const escapedId1 = escapeValue(queryId1);
    const escapedId2 = escapeValue(queryId2);

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

    const escaped = queryIds.map(id => `'${escapeValue(id)}'`);
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
