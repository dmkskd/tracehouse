import type { QuerySeries, OperationalEvent } from '@tracehouse/core';

export const EVENT_INTERVAL_HOURS: Record<string, number> = {
  '15 MINUTE': 0.25,
  '1 HOUR': 1,
  '6 HOUR': 6,
  '1 DAY': 24,
  '2 DAY': 48,
  '7 DAY': 168,
  '30 DAY': 720,
};

export const EVENT_HOURS_INTERVAL = new Map(
  Object.entries(EVENT_INTERVAL_HOURS).map(([interval, hours]) => [hours, interval]),
);

/**
 * Older Time Travel links used the selected event time as the Events range
 * anchor. Return a one-time migration value so later event selections cannot
 * move the investigation window.
 */
export function legacyEventsRangeCenter(
  from: string | undefined,
  rangeCenter: string | undefined,
  eventTime: string | undefined,
): string | undefined {
  if (from !== 'timetravel' || rangeCenter || !eventTime) return undefined;
  return eventTime;
}

export function toLocalEventDateTime(ms: number): string {
  const date = new Date(ms);
  const pad = (value: number) => String(value).padStart(2, '0');
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}`
    + `T${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

export function eventToQuerySeries(event: OperationalEvent): QuerySeries {
  const endMs = Date.parse(event.occurred_at);
  const durationMs = event.duration_ms ?? 0;
  const safeEndMs = Number.isFinite(endMs) ? endMs : Date.now();
  return {
    query_id: event.query_id!,
    label: event.query ?? event.title,
    user: event.user ?? 'default',
    hostname: event.hostname,
    peak_memory: event.memory_usage ?? 0,
    duration_ms: durationMs,
    cpu_us: 0,
    net_send: 0,
    net_recv: 0,
    disk_read: 0,
    disk_write: 0,
    start_time: new Date(safeEndMs - durationMs).toISOString(),
    end_time: new Date(safeEndMs).toISOString(),
    status: event.exception_code != null ? 'QueryFailed' : 'QueryFinish',
    exception_code: event.exception_code,
    query_kind: event.query_kind,
    exception: event.detail,
    points: [],
  };
}
