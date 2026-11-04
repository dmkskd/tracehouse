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
  /** Message carries the event's error code name, e.g. "(KEEPER_EXCEPTION)". */
  is_event_error: boolean;
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
  /** Error code name (e.g. KEEPER_EXCEPTION) used to surface the matching log line. */
  exceptionName?: string;
  availableCapabilities: readonly string[];
  /** Context on either side of the event. Defaults to five minutes. */
  windowSeconds?: number;
  limit?: number;
}

/** Current system.errors counter for an error event's error on one host. */
export interface EventErrorCounter {
  hostname: string;
  /** Start of the host's current server process (now() - uptime()). */
  process_started_at: string;
  /** False when the host has no counter row for this error and remote flag. */
  has_counter: boolean;
  value: number;
  last_error_time?: string;
  last_error_message?: string;
}

/**
 * What the live counter says relative to the event. Only facts the counter
 * can support: it covers the current process only.
 */
export type EventErrorStateKind =
  | 'no_later_occurrence'
  | 'later_occurrence'
  | 'restarted_since'
  | 'no_counter'
  | 'host_not_returned';

export interface EventErrorState {
  kind: EventErrorStateKind;
  /** One-sentence factual statement for display. */
  summary: string;
  /**
   * Label for counter.last_error_message, which belongs to the last
   * occurrence; that occurrence may be after the event or in a later process.
   * Absent when there is no message to show.
   */
  message_label?: string;
  counter?: EventErrorCounter;
}

export interface EventErrorStateOptions {
  eventTime: string;
  hostname: string;
  exceptionName: string;
  remote: boolean;
  availableCapabilities: readonly string[];
}

export interface EventErrorStateResult {
  source: EventContextSource<EventErrorCounter>;
}
