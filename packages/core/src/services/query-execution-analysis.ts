/**
 * QueryExecutionAnalysisService
 *
 * Owns execution-aware query planning. Unlike ordinary EXPLAIN variants,
 * EXPLAIN ANALYZE runs the wrapped SELECT and is therefore deliberately kept
 * separate from TraceService's non-executing plan inspection methods.
 */
import type { IClickHouseAdapter } from '../adapters/types.js';
import type {
  QueryExecutionAnalysisOptions,
  QueryExecutionAnalysisResult,
} from '../types/execution-analysis.js';
import { tagQuery } from '../queries/builder.js';
import { isSelectStatement } from '../utils/sql-statement.js';

export class QueryExecutionAnalysisError extends Error {
  constructor(message: string, public readonly cause?: Error) {
    super(message);
    this.name = 'QueryExecutionAnalysisError';
  }
}

export class QueryExecutionAnalysisService {
  constructor(private adapter: IClickHouseAdapter) {}

  supportsExplicitQueryId(): boolean {
    return this.adapter.supportsExplicitQueryId === true;
  }

  /**
   * Execute a SELECT through ClickHouse EXPLAIN ANALYZE and return its runtime
   * plan. Callers must obtain explicit user intent before invoking this method.
   */
  async analyze(
    query: string,
    source: string,
    options: QueryExecutionAnalysisOptions = {},
  ): Promise<QueryExecutionAnalysisResult> {
    const normalizedQuery = query.trim().replace(/;+\s*$/, '');
    if (!normalizedQuery) {
      throw new QueryExecutionAnalysisError('A query is required for execution analysis.');
    }
    if (!isSelectStatement(normalizedQuery)) {
      throw new QueryExecutionAnalysisError(
        'EXPLAIN ANALYZE is only available for SELECT queries.',
      );
    }

    if (!this.adapter.executeRawQuery) {
      throw new QueryExecutionAnalysisError('Execution analysis is not supported by this connection adapter.');
    }

    const processors = options.processors === true;
    const explainSettings = processors ? ' processors = 1' : '';
    const sql = tagQuery(
      `EXPLAIN ANALYZE${explainSettings} ${normalizedQuery}`,
      source,
    );

    try {
      const lines = await this.adapter.executeRawQuery(
        sql,
        options.database,
        options.queryId ? { queryId: options.queryId } : undefined,
      );

      return {
        kind: 'explain_analyze',
        query: normalizedQuery,
        output: lines.join('\n'),
        processors,
        queryId: options.queryId,
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      throw new QueryExecutionAnalysisError(
        `Failed to analyze query execution: ${message}`,
        error instanceof Error ? error : undefined,
      );
    }
  }
}
