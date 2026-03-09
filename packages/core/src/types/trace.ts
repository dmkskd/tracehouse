/**
 * Types for query tracing and EXPLAIN functionality.
 */

export interface TraceLog {
  event_time: string;
  event_time_microseconds: string;
  query_id: string;
  level: string;
  message: string;
  source: string;
  thread_id: number;
  thread_name: string;
}

export type ExplainType = 'AST' | 'SYNTAX' | 'PLAN' | 'PIPELINE' | 'QUERY TREE';

export interface ExplainResult {
  explain_type: string;
  output: string;
  parsed_tree: Record<string, unknown> | null;
}

export interface OpenTelemetrySpan {
  trace_id: string;
  span_id: string;
  parent_span_id: string;
  operation_name: string;
  start_time_us: number;
  finish_time_us: number;
  duration_us: number;
  duration_ms: number;
  level: number;
  is_root_span: boolean;
  attributes: Record<string, unknown>;
}

/** Raw stack trace sample from system.trace_log */
export interface FlamegraphSample {
  stack: string;  // semicolon-separated stack trace
  value: number;  // sample count
}

/** Hierarchical node for d3-flame-graph */
export interface FlamegraphNode {
  name: string;
  value: number;
  children: FlamegraphNode[];
  /** When set, indicates the data could not be fetched for a known reason (e.g. introspection disabled) */
  unavailableReason?: string;
}

/** Per-processor execution stats from system.processors_profile_log */
export interface ProcessorProfile {
  /** Processor name (e.g. AggregatingTransform, MergeTreeSelect) */
  name: string;
  /** Total wall-clock time in microseconds */
  elapsed_us: number;
  /** Time waiting for input data (upstream bottleneck) */
  input_wait_us: number;
  /** Time waiting to push output (downstream bottleneck) */
  output_wait_us: number;
  /** Total input rows processed */
  input_rows: number;
  /** Total input bytes processed */
  input_bytes: number;
  /** Total output rows produced */
  output_rows: number;
  /** Total output bytes produced */
  output_bytes: number;
  /** Number of processor instances (parallelism) */
  instances: number;
}

