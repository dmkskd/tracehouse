import { describe, expect, it } from 'vitest';
import type { QueryDetail } from '@tracehouse/core';
import {
  executionAnalysisSessionKey,
  executionAnalysisSql,
  getPreviousExecutionMetrics,
} from '../executionAnalysisModel';

describe('execution analysis model', () => {
  it('derives elapsed, aggregate CPU, and peak memory from query-log detail', () => {
    expect(getPreviousExecutionMetrics({
      query_duration_ms: 56,
      memory_usage: 15 * 1024 * 1024,
      ProfileEvents: {
        UserTimeMicroseconds: 60_000,
        SystemTimeMicroseconds: 4_400,
      },
    } as QueryDetail)).toEqual({
      elapsedMs: 56,
      cpuTimeUs: 64_400,
      peakMemoryBytes: 15 * 1024 * 1024,
    });
  });

  it('does not manufacture CPU time when profile events were not recorded', () => {
    expect(getPreviousExecutionMetrics({
      query_duration_ms: 0,
      memory_usage: 0,
      ProfileEvents: {},
    } as QueryDetail)).toEqual({
      elapsedMs: 0,
      cpuTimeUs: undefined,
      peakMemoryBytes: 0,
    });
  });

  it('returns no summary without query detail', () => {
    expect(getPreviousExecutionMetrics(null)).toBeUndefined();
  });

  it('uses the exact replay SQL and a collision-safe session identity', () => {
    const detail = {
      query_id: 'query:id',
      query: 'SELECT concat(\':\', value) FROM events',
      formatted_query: 'SELECT 0',
    } as QueryDetail;

    expect(executionAnalysisSql(detail)).toBe(detail.query);
    expect(executionAnalysisSessionKey(detail)).toBe(JSON.stringify([
      detail.query_id,
      detail.query,
    ]));
    expect(executionAnalysisSessionKey(null)).toBeUndefined();
  });
});
