/**
 * Monitoring Capabilities Types
 * 
 * Represents what observability features are available on the connected
 * ClickHouse server. Probed once per connection and used globally to
 * conditionally show/hide UI features that depend on specific system
 * tables, settings, or server-side configuration.
 */

/** Individual capability with its detection result */
export interface MonitoringCapability {
  /** Machine-readable key */
  id: string;
  /** Human-readable label */
  label: string;
  /** Short description of what this enables */
  description: string;
  /** Whether this capability is available */
  available: boolean;
  /** Category for grouping in UI */
  category: 'logging' | 'profiling' | 'tracing' | 'introspection' | 'metrics';
  /** Optional detail (e.g. table engine, row count, setting value) */
  detail?: string;
  /** TTL retention period if detected (e.g. "30 days"), null for non-table capabilities */
  ttl?: string | null;
  /** Source resource (e.g. "system.query_log", "config.xml: query_log", "setting: allow_introspection_functions") */
  source?: string;
}

/** Full set of probed capabilities for a connection */
export interface MonitoringCapabilities {
  /** When the probe was run */
  probedAt: Date;
  /** Server version string */
  serverVersion: string;
  /** All detected capabilities */
  capabilities: MonitoringCapability[];
}

/**
 * Quick-access boolean flags derived from capabilities.
 * Use these in conditional rendering instead of searching the array.
 */
export interface MonitoringFlags {
  hasTextLog: boolean;
  hasQueryLog: boolean;
  hasQueryThreadLog: boolean;
  hasPartLog: boolean;
  hasTraceLog: boolean;
  hasOpenTelemetry: boolean;
  hasQueryProfileEvents: boolean;
  hasProcessorProfileLog: boolean;
  hasMetricLog: boolean;
  hasAsyncMetricLog: boolean;
  hasZookeeper: boolean;
  hasCrashLog: boolean;
  hasBackupLog: boolean;
  hasS3QueueLog: boolean;
  hasBlobStorageLog: boolean;
  hasIntrospectionFunctions: boolean;
  /** CPU profiler is actually producing samples (not just configured) */
  hasCPUProfilerActive: boolean;
  /** ClickStack (HyperDX) embedded UI available at /clickstack/ (CH 26.2+) */
  hasClickStack: boolean;
  /** Connected to ClickHouse Cloud (some OS-level metrics unavailable) */
  isCloudService: boolean;
  /** tracehouse.processes_history exists (live process sampling via refreshable MV) */
  hasProcessesHistory: boolean;
}

/**
 * Parse a ClickHouse version string (e.g. "26.2.1.123") into [major, minor].
 * Returns [0, 0] if parsing fails.
 */
function parseVersionMajorMinor(version: string): [number, number] {
  const match = version.match(/^(\d+)\.(\d+)/);
  if (!match) return [0, 0];
  return [parseInt(match[1], 10), parseInt(match[2], 10)];
}

/** Derive flags from capabilities array */
export function deriveMonitoringFlags(capabilities: MonitoringCapability[], serverVersion?: string): MonitoringFlags {
  const has = (id: string) => capabilities.find(c => c.id === id)?.available ?? false;
  const [major, minor] = parseVersionMajorMinor(serverVersion ?? '');
  return {
    hasTextLog: has('text_log'),
    hasQueryLog: has('query_log'),
    hasQueryThreadLog: has('query_thread_log'),
    hasPartLog: has('part_log'),
    hasTraceLog: has('trace_log'),
    hasOpenTelemetry: has('opentelemetry_span_log'),
    hasQueryProfileEvents: has('query_log_profile_events'),
    hasProcessorProfileLog: has('processors_profile_log'),
    hasMetricLog: has('metric_log'),
    hasAsyncMetricLog: has('asynchronous_metric_log'),
    hasZookeeper: has('zookeeper'),
    hasCrashLog: has('crash_log'),
    hasBackupLog: has('backup_log'),
    hasS3QueueLog: has('s3queue_log'),
    hasBlobStorageLog: has('blob_storage_log'),
    hasIntrospectionFunctions: has('introspection_functions'),
    hasCPUProfilerActive: has('cpu_profiler_active'),
    hasClickStack: has('clickstack') || (major > 26 || (major === 26 && minor >= 2)),
    isCloudService: has('cloud_service'),
    hasProcessesHistory: has('tracehouse_processes_history'),
  };
}
