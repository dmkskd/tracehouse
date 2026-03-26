/**
 * AnalyticsService — table-level ordering key efficiency analysis.
 *
 * Queries system.query_log + system.parts to determine how effectively
 * each table's ORDER BY / sorting key is serving the actual query workload.
 */

import type { IClickHouseAdapter } from '../adapters/types.js';
import type { TableOrderingKeyEfficiency, OrderingKeyEfficiencyOptions, TableQueryPattern, ExplainIndexesResult, StressSurfaceData, StressSurfaceRow, StressSurfaceInsertRow, StressSurfaceMergeRow, PatternSurfaceRow, SurfaceQueryOptions, ResourceLanesData, ResourceLaneRow, ResourceTotalsRow, ResourceLanesOptions, ResourceLanesTableOptions } from '../types/analytics.js';
import { TABLE_ORDERING_KEY_EFFICIENCY, TABLE_QUERY_PATTERNS } from '../queries/analytics-queries.js';
import { stressSurfaceQueries, stressSurfaceInserts, stressSurfaceMerges, patternSurface, buildSurfaceTimeFilter, resourceLanesSystem, resourceLanesSystemTotals, resourceLanesTable } from '../queries/surface-queries.js';
import { buildQuery, tagQuery } from '../queries/builder.js';
import { TAB_ANALYTICS, sourceTag } from '../queries/source-tags.js';
import { parseExplainIndexesJson } from './explain-parser.js';

export class AnalyticsServiceError extends Error {
  constructor(message: string, public readonly cause?: Error) {
    super(message);
    this.name = 'AnalyticsServiceError';
  }
}

/** Parse user_breakdown from ClickHouse Map(String, UInt64) → Record<string, number> */
function parseUserBreakdown(raw: unknown): Record<string, number> {
  if (!raw || typeof raw !== 'object') return {};
  // ClickHouse JSONEachRow serializes Map as a plain object
  const result: Record<string, number> = {};
  for (const [key, val] of Object.entries(raw as Record<string, unknown>)) {
    result[key] = Number(val ?? 0);
  }
  return result;
}

export class AnalyticsService {
  constructor(private adapter: IClickHouseAdapter) {}

  /**
   * Get per-table ordering key efficiency analysis.
   *
   * For each table that appears in query_log SELECT queries, computes:
   * - Average mark pruning effectiveness (how well the ordering key skips data)
   * - Count of queries with poor pruning
   * - Parts selectivity
   * - Enriched with the actual ORDER BY / sorting key from system.tables
   */
  async getTableOrderingKeyEfficiency(
    options: OrderingKeyEfficiencyOptions = {},
  ): Promise<TableOrderingKeyEfficiency[]> {
    const lookbackDays = options.lookback_days ?? 7;
    const minQueryCount = options.min_query_count ?? 1;

    const sql = buildQuery(TABLE_ORDERING_KEY_EFFICIENCY, {
      lookback_days: lookbackDays,
      min_query_count: minQueryCount,
    });

    try {
      const rows = await this.adapter.executeQuery<Record<string, unknown>>(
        tagQuery(sql, sourceTag(TAB_ANALYTICS, 'orderingKeys')),
      );

      return rows.map((row) => ({
        database: String(row.tbl_database ?? ''),
        table_name: String(row.tbl_name ?? ''),
        query_count: Number(row.query_count ?? 0),
        avg_pruning_pct: row.avg_pruning_pct != null ? Number(row.avg_pruning_pct) : null,
        poor_pruning_queries: Number(row.poor_pruning_queries ?? 0),
        avg_parts_scanned_pct: row.avg_parts_scanned_pct != null ? Number(row.avg_parts_scanned_pct) : null,
        total_rows_read: Number(row.total_rows_read ?? 0),
        total_marks_scanned: Number(row.total_marks_scanned ?? 0),
        total_marks_available: Number(row.total_marks_available ?? 0),
        total_pk_filter_us: Number(row.total_pk_filter_us ?? 0),
        avg_duration_ms: Number(row.avg_duration_ms ?? 0),
        total_cpu_us: Number(row.total_cpu_us ?? 0),
        avg_memory_bytes: Number(row.avg_memory_bytes ?? 0),
        sorting_key: row.sorting_key != null ? String(row.sorting_key) : null,
        primary_key: row.primary_key != null ? String(row.primary_key) : null,
        table_rows: row.table_rows != null ? Number(row.table_rows) : null,
        table_marks: row.table_marks != null ? Number(row.table_marks) : null,
        active_parts: row.active_parts != null ? Number(row.active_parts) : null,
      }));
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error);
      throw new AnalyticsServiceError(
        `Failed to get table ordering key efficiency: ${msg}`,
        error as Error,
      );
    }
  }

  /**
   * Get per-query-pattern breakdown for a specific table.
   *
   * Returns distinct query shapes (by normalized_query_hash) that hit the table,
   * each with their own pruning stats, execution count, and a sample query.
   */
  async getTableQueryPatterns(
    database: string,
    tableName: string,
    lookbackDays = 7,
  ): Promise<TableQueryPattern[]> {
    const sql = buildQuery(TABLE_QUERY_PATTERNS, {
      tbl_database: database,
      tbl_name: tableName,
      lookback_days: lookbackDays,
    });

    try {
      const rows = await this.adapter.executeQuery<Record<string, unknown>>(
        tagQuery(sql, sourceTag(TAB_ANALYTICS, 'queryPatterns')),
      );

      return rows.map((row) => ({
        query_hash: String(row.query_hash ?? ''),
        sample_query: String(row.sample_query ?? ''),
        execution_count: Number(row.execution_count ?? 0),
        avg_pruning_pct: row.avg_pruning_pct != null ? Number(row.avg_pruning_pct) : null,
        poor_pruning_count: Number(row.poor_pruning_count ?? 0),
        avg_duration_ms: Number(row.avg_duration_ms ?? 0),
        p50_duration_ms: Number(row.p50_duration_ms ?? 0),
        p95_duration_ms: Number(row.p95_duration_ms ?? 0),
        p99_duration_ms: Number(row.p99_duration_ms ?? 0),
        avg_rows_read: Number(row.avg_rows_read ?? 0),
        total_marks_scanned: Number(row.total_marks_scanned ?? 0),
        total_marks_available: Number(row.total_marks_available ?? 0),
        total_cpu_us: Number(row.total_cpu_us ?? 0),
        avg_memory_bytes: Number(row.avg_memory_bytes ?? 0),
        p50_memory_bytes: Number(row.p50_memory_bytes ?? 0),
        p95_memory_bytes: Number(row.p95_memory_bytes ?? 0),
        p99_memory_bytes: Number(row.p99_memory_bytes ?? 0),
        first_seen: String(row.first_seen ?? ''),
        last_seen: String(row.last_seen ?? ''),
        user_breakdown: parseUserBreakdown(row.user_breakdown),
      }));
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error);
      throw new AnalyticsServiceError(
        `Failed to get query patterns for ${database}.${tableName}: ${msg}`,
        error as Error,
      );
    }
  }

  /**
   * Run EXPLAIN indexes = 1, json = 1 on a query to get the actual index
   * usage from the ClickHouse query optimizer.
   *
   * Returns structured data about which indexes (PrimaryKey, MinMax, Skip, etc.)
   * were used, which key columns, the condition, and parts/granules selected.
   *
   * This runs the query planner only (no execution), so it's cheap.
   * Automatically runs when a query pattern is expanded in the UI.
   */
  async explainIndexes(query: string): Promise<ExplainIndexesResult> {
    try {
      // Strip trailing semicolons, FORMAT clause, and whitespace.
      // sample_query comes from system.query_log which stores the full query
      // text as received — the @clickhouse/client-web library appends
      // "FORMAT JSONEachRow" to every query, and EXPLAIN doesn't support it.
      const cleanQuery = query
        .replace(/;\s*$/, '')
        .replace(/\bFORMAT\s+\w+\s*$/i, '')
        .trim();
      const explainSql = `EXPLAIN json = 1, indexes = 1 ${cleanQuery}`;

      // executeQuery returns JSONEachRow where each row has an "explain" field
      // containing one line of the pretty-printed JSON output.
      const rows = await this.adapter.executeQuery<{ explain: string }>(explainSql);
      const jsonText = rows.map(r => String(r.explain ?? '')).join('\n');
      return parseExplainIndexesJson(jsonText);
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error);
      return { indexes: [], primaryKey: null, skipIndexes: [], success: false, error: msg };
    }
  }

  // ─── Surface visualizations ────────────────────────────────────────────

  /**
   * Fetch stress surface data for a table: per-minute query stress,
   * insert activity, and merge activity.
   */
  async getStressSurfaceData(options: SurfaceQueryOptions): Promise<StressSurfaceData> {
    const { database, table } = options;
    const tf = buildSurfaceTimeFilter('event_time', options);
    const params = { database, table_name: table, ...tf.params };
    const fullTable = `${database}.${table}`;

    try {
      const [queries, inserts, merges] = await Promise.all([
        this.adapter.executeQuery<Record<string, unknown>>(
          tagQuery(buildQuery(stressSurfaceQueries(tf.clause), params), sourceTag(TAB_ANALYTICS, 'stressSurface')),
        ),
        this.adapter.executeQuery<Record<string, unknown>>(
          tagQuery(buildQuery(stressSurfaceInserts(tf.clause), params), sourceTag(TAB_ANALYTICS, 'stressSurfaceInserts')),
        ),
        this.adapter.executeQuery<Record<string, unknown>>(
          tagQuery(buildQuery(stressSurfaceMerges(tf.clause), params), sourceTag(TAB_ANALYTICS, 'stressSurfaceMerges')),
        ).catch(() => [] as Record<string, unknown>[]), // part_log may not exist
      ]);

      return {
        table: fullTable,
        queries: queries.map((r): StressSurfaceRow => ({
          ts: String(r.ts ?? ''),
          query_count: Number(r.query_count ?? 0),
          total_duration_ms: Number(r.total_duration_ms ?? 0),
          avg_duration_ms: Number(r.avg_duration_ms ?? 0),
          p95_duration_ms: Number(r.p95_duration_ms ?? 0),
          total_read_rows: Number(r.total_read_rows ?? 0),
          total_read_bytes: Number(r.total_read_bytes ?? 0),
          total_memory: Number(r.total_memory ?? 0),
          total_cpu_us: Number(r.total_cpu_us ?? 0),
          total_io_wait_us: Number(r.total_io_wait_us ?? 0),
          total_selected_marks: Number(r.total_selected_marks ?? 0),
        })),
        inserts: inserts.map((r): StressSurfaceInsertRow => ({
          ts: String(r.ts ?? ''),
          insert_count: Number(r.insert_count ?? 0),
          inserted_rows: Number(r.inserted_rows ?? 0),
          inserted_bytes: Number(r.inserted_bytes ?? 0),
        })),
        merges: merges.map((r): StressSurfaceMergeRow => ({
          ts: String(r.ts ?? ''),
          merges: Number(r.merges ?? 0),
          new_parts: Number(r.new_parts ?? 0),
          merge_ms: Number(r.merge_ms ?? 0),
        })),
      };
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error);
      throw new AnalyticsServiceError(`Failed to get stress surface data for ${fullTable}: ${msg}`, error as Error);
    }
  }

  /**
   * Fetch pattern surface data: per (time, query pattern) avg duration
   * for the top 12 most frequent patterns hitting a table.
   */
  async getPatternSurfaceData(options: SurfaceQueryOptions): Promise<PatternSurfaceRow[]> {
    const { database, table } = options;
    const tf = buildSurfaceTimeFilter('event_time', options);
    const params = { database, table_name: table, ...tf.params };

    try {
      const rows = await this.adapter.executeQuery<Record<string, unknown>>(
        tagQuery(buildQuery(patternSurface(tf.clause), params), sourceTag(TAB_ANALYTICS, 'patternSurface')),
      );

      return rows.map((r): PatternSurfaceRow => ({
        ts: String(r.ts ?? ''),
        normalized_query_hash: String(r.normalized_query_hash ?? ''),
        avg_duration_ms: Number(r.avg_duration_ms ?? 0),
        query_count: Number(r.query_count ?? 0),
        avg_memory: Number(r.avg_memory ?? 0),
        sample_query: String(r.sample_query ?? ''),
      }));
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error);
      throw new AnalyticsServiceError(`Failed to get pattern surface data for ${database}.${table}: ${msg}`, error as Error);
    }
  }

  // ─── Resource lanes ──────────────────────────────────────────────────

  /** Map raw rows to typed ResourceLaneRow */
  private mapLaneRow(r: Record<string, unknown>): ResourceLaneRow {
    return {
      ts: String(r.ts ?? ''),
      lane_id: String(r.lane_id ?? ''),
      lane_label: String(r.lane_label ?? ''),
      query_count: Number(r.query_count ?? 0),
      total_duration_ms: Number(r.total_duration_ms ?? 0),
      total_read_rows: Number(r.total_read_rows ?? 0),
      total_read_bytes: Number(r.total_read_bytes ?? 0),
      total_memory: Number(r.total_memory ?? 0),
      total_cpu_us: Number(r.total_cpu_us ?? 0),
      total_io_wait_us: Number(r.total_io_wait_us ?? 0),
      total_selected_marks: Number(r.total_selected_marks ?? 0),
    };
  }

  /** Map raw rows to typed ResourceTotalsRow */
  private mapTotalsRow(r: Record<string, unknown>): ResourceTotalsRow {
    return {
      ts: String(r.ts ?? ''),
      query_count: Number(r.query_count ?? 0),
      total_duration_ms: Number(r.total_duration_ms ?? 0),
      total_read_rows: Number(r.total_read_rows ?? 0),
      total_read_bytes: Number(r.total_read_bytes ?? 0),
      total_memory: Number(r.total_memory ?? 0),
      total_cpu_us: Number(r.total_cpu_us ?? 0),
      total_io_wait_us: Number(r.total_io_wait_us ?? 0),
      total_selected_marks: Number(r.total_selected_marks ?? 0),
    };
  }

  /**
   * System-level resource lanes: per-minute usage grouped by table.
   * Returns lanes (top N tables) + system totals for normalization.
   */
  async getSystemResourceLanes(options: ResourceLanesOptions = {}): Promise<ResourceLanesData> {
    const maxLanes = options.maxLanes ?? 10;
    const excludeSystem = options.excludeSystemTables ?? true;
    const tf = buildSurfaceTimeFilter('event_time', options);
    const params = { max_lanes: maxLanes, ...tf.params };

    try {
      const [laneRows, totalRows] = await Promise.all([
        this.adapter.executeQuery<Record<string, unknown>>(
          tagQuery(buildQuery(resourceLanesSystem(tf.clause, excludeSystem), params), sourceTag(TAB_ANALYTICS, 'resourceLanesSystem')),
        ),
        this.adapter.executeQuery<Record<string, unknown>>(
          tagQuery(buildQuery(resourceLanesSystemTotals(tf.clause), params), sourceTag(TAB_ANALYTICS, 'resourceLanesTotals')),
        ),
      ]);

      return {
        level: 'system',
        lanes: laneRows.map(r => this.mapLaneRow(r)),
        totals: totalRows.map(r => this.mapTotalsRow(r)),
      };
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error);
      throw new AnalyticsServiceError(`Failed to get system resource lanes: ${msg}`, error as Error);
    }
  }

  /**
   * Table-level resource lanes: per-minute usage grouped by query pattern.
   * Drill-down from system view.
   */
  async getTableResourceLanes(options: ResourceLanesTableOptions): Promise<ResourceLanesData> {
    const { database, table } = options;
    const maxLanes = options.maxLanes ?? 10;
    const tf = buildSurfaceTimeFilter('event_time', options);
    const params = { database, table_name: table, max_lanes: maxLanes, ...tf.params };
    const fullTable = `${database}.${table}`;

    try {
      // Reuse system totals for normalization (same baseline at both levels)
      const [laneRows, totalRows] = await Promise.all([
        this.adapter.executeQuery<Record<string, unknown>>(
          tagQuery(buildQuery(resourceLanesTable(tf.clause), params), sourceTag(TAB_ANALYTICS, 'resourceLanesTable')),
        ),
        this.adapter.executeQuery<Record<string, unknown>>(
          tagQuery(buildQuery(resourceLanesSystemTotals(tf.clause), params), sourceTag(TAB_ANALYTICS, 'resourceLanesTotals')),
        ),
      ]);

      return {
        level: 'table',
        drillTable: fullTable,
        lanes: laneRows.map(r => this.mapLaneRow(r)),
        totals: totalRows.map(r => this.mapTotalsRow(r)),
      };
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error);
      throw new AnalyticsServiceError(`Failed to get table resource lanes for ${fullTable}: ${msg}`, error as Error);
    }
  }
}
