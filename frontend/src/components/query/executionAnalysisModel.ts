import type { QueryDetail } from '@tracehouse/core';

export const EXPLAIN_ANALYZE_DOCS_URL =
  'https://clickhouse.com/docs/reference/statements/explain#explain-analyze';

export interface PreviousExecutionMetrics {
  elapsedMs?: number;
  cpuTimeUs?: number;
  peakMemoryBytes?: number;
}

/** Resolve the exact historical SQL text used for execution analysis. */
export function executionAnalysisSql(queryDetail: QueryDetail | null): string {
  return queryDetail?.query || queryDetail?.formatted_query || '';
}

/**
 * Stable identity for one historical-query analysis.
 *
 * JSON tuple encoding avoids delimiter collisions in user-provided query IDs
 * and SQL while allowing the same value to drive both React remounting and
 * shared analysis-session state.
 */
export function executionAnalysisSessionKey(
  queryDetail: QueryDetail | null,
): string | undefined {
  if (!queryDetail) return undefined;
  return JSON.stringify([
    queryDetail.query_id,
    executionAnalysisSql(queryDetail),
  ]);
}

function recordedNumber(value: unknown): number | undefined {
  const numeric = Number(value);
  return value !== null && value !== undefined && Number.isFinite(numeric) && numeric >= 0
    ? numeric
    : undefined;
}

/** Convert query-log fields into the metrics shown before replaying a query. */
export function getPreviousExecutionMetrics(
  queryDetail: QueryDetail | null,
): PreviousExecutionMetrics | undefined {
  if (!queryDetail) return undefined;

  const profileEvents = queryDetail.ProfileEvents;
  const hasCpuTime = profileEvents
    && (
      Object.prototype.hasOwnProperty.call(profileEvents, 'UserTimeMicroseconds')
      || Object.prototype.hasOwnProperty.call(profileEvents, 'SystemTimeMicroseconds')
    );

  return {
    elapsedMs: recordedNumber(queryDetail.query_duration_ms),
    cpuTimeUs: hasCpuTime
      ? recordedNumber(
          Number(profileEvents?.UserTimeMicroseconds ?? 0)
          + Number(profileEvents?.SystemTimeMicroseconds ?? 0),
        )
      : undefined,
    peakMemoryBytes: recordedNumber(queryDetail.memory_usage),
  };
}
