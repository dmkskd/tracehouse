import { type QuerySeries, parseTimeValue } from '@tracehouse/core';

export interface QueryRow {
  query_id?: string;
  event_time?: string;
  query_start_time?: string;
  query_start_time_microseconds?: string;
  query_duration_ms?: number;
  memory_usage?: number;
  read_rows?: number;
  read_bytes?: number;
  query?: string;
  query_text?: string;
  user?: string;
  type?: string;
  [key: string]: unknown;
}

/** Build a QuerySeries from a query_log row so QueryDetailModal can take over. */
export function rowToQuerySeries(row: QueryRow): QuerySeries {
  const durationMs = Number(row.query_duration_ms ?? 0);
  const explicitStart = row.query_start_time_microseconds || row.query_start_time;
  const { timeMs } = parseTimeValue(explicitStart || row.event_time || new Date().toISOString());
  // A completed query_log event_time is its finish clock. Preserve the existing
  // missing/invalid-time fallback, but never add duration twice to a known finish.
  const startMs = !explicitStart && row.event_time && timeMs > 0 ? timeMs - durationMs : timeMs;
  const startTime = new Date(startMs).toISOString();
  const endTime = new Date(startMs + durationMs).toISOString();

  return {
    query_id: String(row.query_id ?? ''),
    label: String(row.query ?? row.query_text ?? ''),
    user: String(row.user ?? 'default'),
    start_time: startTime,
    end_time: endTime,
    duration_ms: durationMs,
    peak_memory: Number(row.memory_usage ?? 0),
    cpu_us: 0,
    net_send: 0,
    net_recv: 0,
    disk_read: Number(row.read_bytes ?? 0),
    disk_write: 0,
    status: String(row.type ?? 'QueryFinish'),
    points: [],
  };
}

