import {
  useCallback,
  useMemo,
  useSyncExternalStore,
} from 'react';
import {
  QueryExecutionAnalysisError,
  randomUUID,
  type QueryExecutionAnalysisOptions,
  type QueryExecutionAnalysisResult,
} from '@tracehouse/core';
import { useClickHouseServices } from '../providers/ClickHouseProvider';
import {
  failQueryExecutionAnalysis,
  getQueryExecutionAnalysisSnapshot,
  resetQueryExecutionAnalysis,
  runQueryExecutionAnalysis,
  subscribeQueryExecutionAnalysis,
  type QueryExecutionAnalysisFailure,
} from '../stores/queryExecutionAnalysisStore';

export interface QueryExecutionAnalysisRequest
  extends Omit<QueryExecutionAnalysisOptions, 'queryId'> {
  query: string;
  source: string;
}

function executionAnalysisFailure(error: unknown): QueryExecutionAnalysisFailure {
  if (error instanceof QueryExecutionAnalysisError) {
    return {
      message: error.message,
      category: error.category,
    };
  }
  return {
    message: error instanceof Error ? error.message : String(error),
    category: 'unknown',
  };
}

/**
 * Shared frontend orchestration for EXPLAIN ANALYZE.
 *
 * The core service owns SQL validation and execution. The external session
 * store owns request state, in-flight deduplication, timing, and bounded
 * retention across React unmounts.
 */
export function useQueryExecutionAnalysis(sessionKey?: string) {
  const services = useClickHouseServices();
  const localScope = useMemo(() => ({ services }), [services]);
  const scope = sessionKey && services ? services : localScope;
  const key = sessionKey ?? 'component';

  const subscribe = useCallback(
    (listener: () => void) =>
      subscribeQueryExecutionAnalysis(scope, key, listener),
    [key, scope],
  );
  const getSnapshot = useCallback(
    () => getQueryExecutionAnalysisSnapshot(scope, key),
    [key, scope],
  );
  const snapshot = useSyncExternalStore(subscribe, getSnapshot, getSnapshot);

  const reset = useCallback(() => {
    resetQueryExecutionAnalysis(scope, key);
  }, [key, scope]);

  const analyze = useCallback(async (
    request: QueryExecutionAnalysisRequest,
  ): Promise<QueryExecutionAnalysisResult | null> => {
    if (!services) {
      failQueryExecutionAnalysis(scope, key, {
        message: 'No active ClickHouse connection.',
        category: 'connection',
      });
      return null;
    }

    return runQueryExecutionAnalysis(
      scope,
      key,
      () => {
        const queryId = services.queryExecutionAnalysisService.supportsExplicitQueryId()
          ? randomUUID()
          : undefined;
        return services.queryExecutionAnalysisService.analyze(
          request.query,
          request.source,
          {
            database: request.database,
            processors: request.processors,
            queryId,
          },
        );
      },
      executionAnalysisFailure,
    );
  }, [key, scope, services]);

  return {
    analyze,
    reset,
    result: snapshot.result,
    requestDurationMs: snapshot.requestDurationMs,
    isAnalyzing: snapshot.status === 'running',
    error: snapshot.failure?.message ?? null,
    errorCategory: snapshot.failure?.category,
    isConnected: Boolean(services),
  };
}
