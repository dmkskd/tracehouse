export type EventContextSourceStatus = 'loaded' | 'unavailable' | 'failed';

export interface EventContextSource<T> {
  source: string;
  capability: string;
  status: EventContextSourceStatus;
  data: T[];
  detail?: string;
}

export interface EventContextQuery {
  hostname?: string;
  query_id: string;
  initial_query_id?: string;
  user?: string;
  query_kind?: string;
  start_time: string;
  end_time: string;
  duration_ms: number;
  memory_usage: number;
  cpu_us: number;
  read_rows: number;
  read_bytes: number;
  written_rows: number;
  written_bytes: number;
  status: string;
  exception_code?: number;
  exception?: string;
  query: string;
  is_event_query: boolean;
}

export interface EventContextMetricPoint {
  hostname?: string;
  time: string;
  memory_usage: number;
  active_queries: number;
  active_merges: number;
  cpu_cores: number;
}

export interface EventContextMetricSnapshot extends EventContextMetricPoint {
  /** Milliseconds between the selected event and the preceding metric sample. */
  sample_age_ms: number;
}

export interface EventContextLogEntry {
  hostname?: string;
  time: string;
  level: string;
  logger: string;
  message: string;
  query_id?: string;
  thread_name?: string;
  is_event_query: boolean;
}

export interface EventContextResult {
  event_time: string;
  window_start: string;
  window_end: string;
  workload: EventContextSource<EventContextQuery>;
  metrics: EventContextSource<EventContextMetricPoint> & {
    snapshots: EventContextMetricSnapshot[];
  };
  logs: EventContextSource<EventContextLogEntry>;
}

export interface EventContextOptions {
  eventTime: string;
  hostname?: string | null;
  queryId?: string;
  initialQueryId?: string;
  availableCapabilities: readonly string[];
  /** Context on either side of the event. Defaults to five minutes. */
  windowSeconds?: number;
  limit?: number;
}
