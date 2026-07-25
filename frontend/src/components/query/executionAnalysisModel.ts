import type { QueryDetail } from '@tracehouse/core';

export const EXPLAIN_ANALYZE_DOCS_URL =
  'https://clickhouse.com/docs/reference/statements/explain#explain-analyze';

export interface PreviousExecutionMetrics {
  elapsedMs?: number;
  cpuTimeUs?: number;
  peakMemoryBytes?: number;
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
