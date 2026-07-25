import { describe, expect, it, vi } from 'vitest';
import type { QueryExecutionAnalysisResult } from '@tracehouse/core';
import {
  QUERY_EXECUTION_ANALYSIS_CACHE_LIMIT,
  getQueryExecutionAnalysisSnapshot,
  runQueryExecutionAnalysis,
} from '../queryExecutionAnalysisStore';

function result(query: string): QueryExecutionAnalysisResult {
  return {
    kind: 'explain_analyze',
    query,
    output: `${query} plan`,
    processors: false,
  };
}

describe('queryExecutionAnalysisStore', () => {
  it('retains only the most recently used completed sessions', async () => {
    const scope = {};

    for (let index = 0; index <= QUERY_EXECUTION_ANALYSIS_CACHE_LIMIT; index += 1) {
      const query = `SELECT ${index}`;
      await runQueryExecutionAnalysis(
        scope,
        `query-${index}`,
        vi.fn().mockResolvedValue(result(query)),
        error => ({
          message: String(error),
          category: 'unknown',
        }),
      );
    }

    expect(
      getQueryExecutionAnalysisSnapshot(
        scope,
        `query-${QUERY_EXECUTION_ANALYSIS_CACHE_LIMIT}`,
      ),
    ).toMatchObject({
      status: 'success',
      result: {
        query: `SELECT ${QUERY_EXECUTION_ANALYSIS_CACHE_LIMIT}`,
      },
    });
    expect(
      getQueryExecutionAnalysisSnapshot(scope, 'query-0'),
    ).toMatchObject({
      status: 'idle',
      result: null,
    });
  });
});
