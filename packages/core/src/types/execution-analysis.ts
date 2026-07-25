/** Minimum ClickHouse version that provides EXPLAIN ANALYZE. */
export const EXPLAIN_ANALYZE_MIN_CLICKHOUSE_VERSION = {
  major: 26,
  minor: 7,
} as const;

export const EXPLAIN_ANALYZE_MIN_CLICKHOUSE_VERSION_LABEL =
  `${EXPLAIN_ANALYZE_MIN_CLICKHOUSE_VERSION.major}.${EXPLAIN_ANALYZE_MIN_CLICKHOUSE_VERSION.minor}`;

/**
 * Options controlling the detail included in ClickHouse's runtime query plan.
 */
export interface QueryExecutionAnalysisOptions {
  /** Include min/median/max/sum elapsed-time distribution per processor. */
  processors?: boolean;
  /** Optional database context for resolving unqualified table names. */
  database?: string;
  /** Optional caller-provided query ID for correlation with query logs. */
  queryId?: string;
}

/**
 * Raw execution-aware plan returned by EXPLAIN ANALYZE.
 *
 * The text is intentionally preserved because ClickHouse owns the evolving
 * plan format and can add metrics without requiring a TraceHouse release.
 */
export interface QueryExecutionAnalysisResult {
  kind: 'explain_analyze';
  query: string;
  output: string;
  processors: boolean;
  queryId?: string;
}
