export const TRACKER_TIME_PRESETS = [
  { label: '15m', interval: '15 MINUTE' },
  { label: '1h', interval: '1 HOUR' },
  { label: '3h', interval: '3 HOUR' },
] as const;

const INTERVAL_MS: Record<string, number> = {
  '15 MINUTE': 15 * 60 * 1000,
  '1 HOUR': 60 * 60 * 1000,
  '3 HOUR': 3 * 60 * 60 * 1000,
  '6 HOUR': 6 * 60 * 60 * 1000,
  '1 DAY': 24 * 60 * 60 * 1000,
  '2 DAY': 2 * 24 * 60 * 60 * 1000,
  '3 DAY': 3 * 24 * 60 * 60 * 1000,
  '7 DAY': 7 * 24 * 60 * 60 * 1000,
  '30 DAY': 30 * 24 * 60 * 60 * 1000,
};

export interface ResolvedTrackerTimeRange {
  startTime: string;
  endTime: string;
}

export function resolveTrackerTimeRange(
  timeRange?: string | null,
  explicitStartTime?: string,
  explicitEndTime?: string,
  now = new Date(),
): ResolvedTrackerTimeRange {
  if (timeRange?.startsWith('CUSTOM:')) {
    const [customStart, customEnd] = timeRange.slice('CUSTOM:'.length).split(',');
    const start = customStart ? new Date(customStart) : new Date(Number.NaN);
    const end = customEnd ? new Date(customEnd) : new Date(Number.NaN);
    if (Number.isFinite(start.getTime()) && Number.isFinite(end.getTime())) {
      return { startTime: start.toISOString(), endTime: end.toISOString() };
    }
  }

  if (timeRange && INTERVAL_MS[timeRange]) {
    return {
      startTime: new Date(now.getTime() - INTERVAL_MS[timeRange]).toISOString(),
      endTime: now.toISOString(),
    };
  }

  const explicitStart = explicitStartTime ? new Date(explicitStartTime) : null;
  const explicitEnd = explicitEndTime ? new Date(explicitEndTime) : null;
  const end = explicitEnd && Number.isFinite(explicitEnd.getTime()) ? explicitEnd : now;
  const start = explicitStart && Number.isFinite(explicitStart.getTime())
    ? explicitStart
    : new Date(end.getTime() - INTERVAL_MS['1 HOUR']);

  return { startTime: start.toISOString(), endTime: end.toISOString() };
}

export function trackerTimeRangeHours(
  timeRange?: string | null,
  explicitStartTime?: string,
  explicitEndTime?: string,
): number {
  const resolved = resolveTrackerTimeRange(timeRange, explicitStartTime, explicitEndTime);
  return (Date.parse(resolved.endTime) - Date.parse(resolved.startTime)) / (60 * 60 * 1000);
}
