import { useCallback, useRef, useState } from 'react';
import {
  randomUUID,
  type QueryExecutionAnalysisOptions,
  type QueryExecutionAnalysisResult,
} from '@tracehouse/core';
import { useClickHouseServices } from '../providers/ClickHouseProvider';

export interface QueryExecutionAnalysisRequest
  extends Omit<QueryExecutionAnalysisOptions, 'queryId'> {
  query: string;
  source: string;
}

/**
 * Shared frontend orchestration for EXPLAIN ANALYZE.
 *
 * The core service owns SQL validation and execution. This hook owns React
 * request state, request timing, and adapter-specific query ID correlation.
 */
export function useQueryExecutionAnalysis() {
  const services = useClickHouseServices();
  const [result, setResult] = useState<QueryExecutionAnalysisResult | null>(null);
  const [requestDurationMs, setRequestDurationMs] = useState(0);
  const [isAnalyzing, setIsAnalyzing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const requestGeneration = useRef(0);

  const reset = useCallback(() => {
    requestGeneration.current += 1;
    setResult(null);
    setRequestDurationMs(0);
    setIsAnalyzing(false);
    setError(null);
  }, []);

  const analyze = useCallback(async (
    request: QueryExecutionAnalysisRequest,
  ): Promise<QueryExecutionAnalysisResult | null> => {
    if (!services) {
      requestGeneration.current += 1;
      setResult(null);
      setRequestDurationMs(0);
      setIsAnalyzing(false);
      setError('No active ClickHouse connection.');
      return null;
    }

    const generation = requestGeneration.current + 1;
    requestGeneration.current = generation;
    setIsAnalyzing(true);
    setResult(null);
    setError(null);

    const started = performance.now();
    const queryId = services.queryExecutionAnalysisService.supportsExplicitQueryId()
      ? randomUUID()
      : undefined;

    try {
      const analysis = await services.queryExecutionAnalysisService.analyze(
        request.query,
        request.source,
        {
          database: request.database,
          processors: request.processors,
          queryId,
        },
      );
      if (requestGeneration.current === generation) {
        setResult(analysis);
      }
      return analysis;
    } catch (analysisError) {
      if (requestGeneration.current === generation) {
        setError(analysisError instanceof Error ? analysisError.message : String(analysisError));
      }
      return null;
    } finally {
      if (requestGeneration.current === generation) {
        setRequestDurationMs(performance.now() - started);
        setIsAnalyzing(false);
      }
    }
  }, [services]);

  return {
    analyze,
    reset,
    result,
    requestDurationMs,
    isAnalyzing,
    error,
    isConnected: Boolean(services),
  };
}
