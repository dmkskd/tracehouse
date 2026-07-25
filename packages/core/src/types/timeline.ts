/**
 * Types for timeline/snapshot analysis.
 */

export interface TimeseriesPoint {
  t: string;  // ISO timestamp
  v: number;  // Value
}

/**
 * Per-second sampled values for zoom mode.
 * All metrics are included so metric-mode switches don't require re-fetching.
 */
export interface ZoomSample {
  /** Timestamp in milliseconds (epoch) */
  ms: number;
  /** Memory usage in bytes */
  memory: number;
  /** CPU cores used (delta µs / 1e6 / dt) */
  cpu_cores: number;
  /** Network bytes/s (send + recv) */
  net_rate: number;
  /** Disk bytes/s (read + write) */
  disk_rate: number;
}

export interface QuerySeries {
  query_id: string;
  label: string;
  user: string;
  hostname?: string;
  peak_memory: number;
  duration_ms: number;
  cpu_us: number;
  net_send: number;
  net_recv: number;
  disk_read: number;
  disk_write: number;
  start_time: string;
  end_time: string;
  status?: string;  // QueryFinish, QueryFailed, Running, etc.
  exception_code?: number;
  query_kind?: string;  // SELECT, INSERT, ALTER, etc.
  exception?: string;  // Error message if query failed
  is_running?: boolean;  // True if query is currently in-flight
  /** True when this query matches the normalizedQueryHash filter (pattern mode overlay) */
  matched_hash?: boolean;
  points: TimeseriesPoint[];
  /** Per-second sampled metrics from processes_history (zoom mode only) */
  zoomSamples?: ZoomSample[];
}

export interface MergeSeries {
  part_name: string;
  table: string;
  hostname?: string;
  peak_memory: number;
  duration_ms: number;
  cpu_us: number;
  net_send: number;
  net_recv: number;
  disk_read: number;
  disk_write: number;
  start_time: string;
  end_time: string;
  merge_reason?: string;  // RegularMerge, TTLDeleteMerge, TTLRecompressMerge, etc.
  is_running?: boolean;  // True if merge is currently in-flight
  progress?: number;  // Progress 0-1 for running merges
  /** Per-second sampled metrics from merges_history (zoom mode only) */
  zoomSamples?: ZoomSample[];
}

export interface MutationSeries {
  part_name: string;
  table: string;
  hostname?: string;
  peak_memory: number;
  duration_ms: number;
  cpu_us: number;
  net_send: number;
  net_recv: number;
  disk_read: number;
  disk_write: number;
  start_time: string;
  end_time: string;
  is_running?: boolean;  // True if mutation is currently in-flight
  progress?: number;  // Progress 0-1 for running mutations
  /** Per-second sampled metrics from merges_history (zoom mode only) */
  zoomSamples?: ZoomSample[];
}

/**
 * Stable event dimensions used by Time Travel filters.
 *
 * Keep these independent from presentation labels: the UI can change wording,
 * colours, or grouping without having to infer semantics from event text.
 */
export type TimelineEventSeverity = 'critical' | 'error' | 'warning' | 'info';

export type TimelineEventCategory =
  | 'lifecycle'
  | 'queries'
  | 'replication'
  | 'storage'
  | 'coordination'
  | 'changes'
  | 'maintenance';

export type TimelineEventKind =
  | 'server_restart'
  | 'server_crash'
  | 'query_oom'
  | 'query_rejected'
  | 'query_timeout'
  | 'query_resource_limit'
  | 'query_failure'
  | 'replica_readonly'
  | 'replica_unavailable'
  | 'replication_data_loss'
  | 'replication_task_failure'
  | 'part_failure'
  | 'background_task_failure'
  | 'error_burst'
  | 'ddl'
  | 'keeper_connection'
  | 'backup'
  | 'async_insert_failure'
  | 'server_log';

export type TimelineEventPrecision = 'exact' | 'sampled' | 'inferred';

/**
 * An individual operational event. Events remain unaggregated so periodic
 * behaviour (for example, one query OOM every 15 minutes) is not lost.
 * Visualizations may cluster overlapping markers without changing this data.
 */
export interface TimelineEvent {
  /** Deterministic identifier, stable across timeline refreshes. */
  id: string;
  /** Best estimate of when the event itself happened (ISO timestamp). */
  occurred_at: string;
  /**
   * End of a state episode, when known. Point events omit this field; an
   * ongoing episode also omits it until a recovery transition is observed.
   */
  ended_at?: string;
  /** When ClickHouse recorded/observed it, when different from occurred_at. */
  observed_at?: string;
  hostname?: string;
  kind: TimelineEventKind;
  category: TimelineEventCategory;
  severity: TimelineEventSeverity;
  precision: TimelineEventPrecision;
  title: string;
  detail?: string;
  /** ClickHouse table that produced the event. */
  source: string;
  /** Capability ID required to read the source. */
  capability: string;

  // Query correlation fields. normalized_query_hash is a string because it is
  // a ClickHouse UInt64 and may exceed JavaScript's safe integer range.
  query_id?: string;
  initial_query_id?: string;
  normalized_query_hash?: string;
  user?: string;
  query_kind?: string;
  query?: string;
  databases?: string[];
  tables?: string[];
  database?: string;
  table?: string;
  part_name?: string;
  partition_id?: string;
  operation?: string;
  task_name?: string;
  disk_name?: string;
  exception_code?: number;
  exception_name?: string;
  /** Number of occurrences represented by a sampled/aggregated event. */
  count?: number;
  /** Whether ClickHouse recorded the error as originating from a remote server. */
  remote?: boolean;
  duration_ms?: number;
  memory_usage?: number;

  // Crash/lifecycle correlation fields.
  signal?: number;
  version?: string;
}

export type TimelineEventSourceStatus =
  | 'loaded'
  | 'unavailable'
  | 'failed'
  | 'not_requested';

/**
 * Reports whether each event source was actually read. This prevents an empty
 * events array from ambiguously meaning either "nothing happened" or
 * "ClickHouse could not provide the source".
 */
export interface TimelineEventSourceCoverage {
  source: string;
  capability: string;
  status: TimelineEventSourceStatus;
  event_count: number;
  truncated?: boolean;
  detail?: string;
}

export interface MemoryTimeline {
  window_start: string;
  window_end: string;
  target: string;
  server_memory: TimeseriesPoint[];
  server_cpu: TimeseriesPoint[];
  server_network_send: TimeseriesPoint[];
  server_network_recv: TimeseriesPoint[];
  server_disk_read: TimeseriesPoint[];
  server_disk_write: TimeseriesPoint[];
  server_total_ram: number;
  cpu_cores: number;
  /** Number of ClickHouse hosts contributing to this data (1 for single-host, N for cluster "All" mode) */
  host_count: number;
  /** Per-host CPU timeseries for cluster tooltip breakdown (only populated in "All" mode with multiple hosts) */
  per_host_cpu?: Record<string, TimeseriesPoint[]>;
  queries: QuerySeries[];
  merges: MergeSeries[];
  mutations: MutationSeries[];
  query_count: number;
  merge_count: number;
  merge_peak_total: number;
  mutation_count: number;
  /** Operational events occurring inside this timeline window. */
  events: TimelineEvent[];
  /** Per-source availability and collection result for `events`. */
  event_coverage: TimelineEventSourceCoverage[];
}

export interface TimelineOptions {
  timestamp: Date;
  windowSeconds: number;
  /** Include in-flight queries/merges/mutations from system.processes and system.merges. Default: true */
  includeRunning?: boolean;
  /** Filter to a specific host (by hostname()). When null/undefined, shows all hosts (cluster-wide). */
  hostname?: string | null;
  /** Max rows per activity type (queries, merges, mutations). Default: 100 */
  activityLimit?: number;
  /** Active metric tab. Controls which server metrics are fetched and which "top N" sort is used. Default: 'memory' */
  activeMetric?: 'memory' | 'cpu' | 'network' | 'disk';
  /** When set, only fetch queries matching this normalized_query_hash (pattern mode). */
  normalizedQueryHash?: string;
  /**
   * Capability IDs confirmed available by MonitoringCapabilitiesService.
   * When omitted, event collection is not requested. Passing an empty array is
   * distinct: collection was requested, but no event sources are available.
   */
  eventCapabilities?: readonly string[];
  /** Maximum rows read from each event source. Default: 1000. */
  eventLimit?: number;
}

/**
 * A single contiguous period where CPU exceeded 100% of all cores.
 */
export interface CpuSpike {
  /** Start of the spike (ISO string) */
  start_time: string;
  /** End of the spike (ISO string) */
  end_time: string;
  /** Duration in seconds */
  duration_seconds: number;
  /** Peak CPU percentage during this spike */
  peak_cpu_pct: number;
  /** Average CPU percentage during this spike */
  avg_cpu_pct: number;
  /** Number of metric_log data points in this spike */
  data_points: number;
  /** Classification: 'transient' (single point or < threshold) vs 'sustained' */
  classification: 'transient' | 'sustained';
}

/**
 * Summary of CPU spike analysis over a time window.
 */
export interface CpuSpikeAnalysis {
  /** Time window analyzed */
  window_start: string;
  window_end: string;
  /** Number of CPU cores on the server */
  cpu_cores: number;
  /** Total data points in the window */
  total_data_points: number;
  /** Number of data points above 100% */
  points_above_100: number;
  /** Percentage of time spent above 100% */
  pct_time_above_100: number;
  /** Individual spike periods */
  spikes: CpuSpike[];
  /** Count by classification */
  transient_count: number;
  sustained_count: number;
  /** Overall peak CPU % in the window */
  overall_peak_pct: number;
}
