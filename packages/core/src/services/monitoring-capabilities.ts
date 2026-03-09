/**
 * MonitoringCapabilitiesService
 * 
 * Probes a ClickHouse server to detect which observability features are
 * available. Designed to run once per connection and cache the result
 * in a global store so all pages can conditionally render features.
 */

import type { IClickHouseAdapter } from '../adapters/types.js';
import type {
  MonitoringCapabilities,
  MonitoringCapability,
} from '../types/monitoring-capabilities.js';
import {
  PROBE_SYSTEM_LOG_TABLES,
  PROBE_MONITORING_SETTINGS,
  PROBE_ZOOKEEPER,
  PROBE_SERVER_VERSION,
  PROBE_CLOUD_SERVICE,
} from '../queries/monitoring-capabilities-queries.js';
import { tagQuery } from '../queries/builder.js';
import { TAB_INTERNAL, sourceTag } from '../queries/source-tags.js';

export class MonitoringCapabilitiesServiceError extends Error {
  constructor(message: string, public readonly cause?: Error) {
    super(message);
    this.name = 'MonitoringCapabilitiesServiceError';
  }
}

/** Metadata for each system log table we probe */
const LOG_TABLE_META: Record<string, { label: string; description: string; category: MonitoringCapability['category'] }> = {
  text_log: {
    label: 'Text Log',
    description: 'Server text log messages (errors, warnings, info). Enables log-level filtering in UI.',
    category: 'logging',
  },
  query_log: {
    label: 'Query Log',
    description: 'Completed query history with timing, memory, and profile events.',
    category: 'profiling',
  },
  query_thread_log: {
    label: 'Query Thread Log',
    description: 'Per-thread breakdown of query execution. Enables thread-level CPU attribution.',
    category: 'profiling',
  },
  query_views_log: {
    label: 'Query Views Log',
    description: 'Materialized view execution during queries.',
    category: 'profiling',
  },
  part_log: {
    label: 'Part Log',
    description: 'Merge, mutation, and part lifecycle events. Enables merge history and CPU attribution.',
    category: 'introspection',
  },
  trace_log: {
    label: 'Trace Log',
    description: 'Stack trace sampling for CPU and memory profiling. Enables flamegraphs.',
    category: 'tracing',
  },
  opentelemetry_span_log: {
    label: 'OpenTelemetry Spans',
    description: 'Distributed tracing spans. Enables trace correlation with external systems.',
    category: 'tracing',
  },
  metric_log: {
    label: 'Metric Log',
    description: 'Historical system.metrics snapshots. Enables metric time-series charts.',
    category: 'metrics',
  },
  asynchronous_metric_log: {
    label: 'Async Metric Log',
    description: 'Historical asynchronous_metrics snapshots (CPU, memory, jemalloc).',
    category: 'metrics',
  },
  crash_log: {
    label: 'Crash Log',
    description: 'Server crash/fatal error records.',
    category: 'logging',
  },
  processors_profile_log: {
    label: 'Processors Profile Log',
    description: 'Per-processor pipeline profiling. Enables detailed query pipeline analysis.',
    category: 'profiling',
  },
  backup_log: {
    label: 'Backup Log',
    description: 'Backup and restore operation history.',
    category: 'logging',
  },
  s3queue_log: {
    label: 'S3Queue Log',
    description: 'S3Queue table engine processing log.',
    category: 'logging',
  },
  blob_storage_log: {
    label: 'Blob Storage Log',
    description: 'Blob storage (S3/GCS/Azure) operation log.',
    category: 'logging',
  },
  session_log: {
    label: 'Session Log',
    description: 'Login/logout and session lifecycle events.',
    category: 'logging',
  },
  zookeeper_log: {
    label: 'ZooKeeper Log',
    description: 'ZooKeeper/Keeper request log for replication debugging.',
    category: 'introspection',
  },
  transactions_info_log: {
    label: 'Transactions Log',
    description: 'Transaction lifecycle events.',
    category: 'introspection',
  },
  filesystem_cache_log: {
    label: 'Filesystem Cache Log',
    description: 'Filesystem cache operations for remote storage.',
    category: 'introspection',
  },
  filesystem_read_prefetches_log: {
    label: 'FS Read Prefetch Log',
    description: 'Filesystem read prefetch operations.',
    category: 'introspection',
  },
  asynchronous_insert_log: {
    label: 'Async Insert Log',
    description: 'Asynchronous insert operation log.',
    category: 'logging',
  },
};

export class MonitoringCapabilitiesService {
  constructor(private adapter: IClickHouseAdapter) {}

  /**
   * Probe all monitoring capabilities. Safe to call — catches errors
   * per-probe so partial results are still returned.
   */
  async probe(): Promise<MonitoringCapabilities> {
    const [version, logTables, settings, hasZk, hasIntrospection, isCloud] = await Promise.all([
      this.probeVersion(),
      this.probeLogTables(),
      this.probeSettings(),
      this.probeZookeeper(),
      this.probeIntrospectionFunctions(),
      this.probeCloudService(),
    ]);

    const capabilities: MonitoringCapability[] = [];

    // Add log table capabilities
    for (const [tableId, meta] of Object.entries(LOG_TABLE_META)) {
      const tableInfo = logTables.get(tableId);
      const available = !!tableInfo;
      const detail = tableInfo
        ? `${tableInfo.engine} · ${formatRowCount(tableInfo.totalRows)} rows`
        : 'Table not found';

      capabilities.push({
        id: tableId,
        label: meta.label,
        description: meta.description,
        available,
        category: meta.category,
        detail,
      });
    }

    // Add profile events capability (derived from settings)
    // This requires both query_log AND the log_profile_events setting enabled.
    // Without the setting, query_log exists but ProfileEvents columns are empty.
    const logProfileEvents = settings.get('log_profile_events');
    const profileEventsEnabled = logProfileEvents?.value === '1';
    const hasQueryLogTable = logTables.has('query_log');
    capabilities.push({
      id: 'query_log_profile_events',
      label: 'Query Profile Events',
      description: 'Per-query ProfileEvents counters (CPU time, IO, cache hits). Enables resource attribution.',
      available: hasQueryLogTable && profileEventsEnabled,
      category: 'profiling',
      detail: !hasQueryLogTable
        ? 'query_log not available'
        : profileEventsEnabled
          ? 'Enabled (log_profile_events=1)'
          : 'Disabled — set log_profile_events=1 to collect per-query counters',
    });

    // Add OpenTelemetry tracing probability
    const otelProb = settings.get('opentelemetry_start_trace_probability');
    const otelAvailable = logTables.has('opentelemetry_span_log');
    if (otelProb) {
      const prob = parseFloat(otelProb.value);
      const otelCap = capabilities.find(c => c.id === 'opentelemetry_span_log');
      if (otelCap) {
        otelCap.detail = `${otelCap.detail} · trace probability: ${prob}`;
        if (prob === 0 && otelAvailable) {
          otelCap.detail += ' (table exists but tracing disabled)';
        }
      }
    }

    // Enrich trace_log capability with CPU profiler status
    const cpuProfilerPeriod = settings.get('query_profiler_cpu_time_period_ns');
    const realProfilerPeriod = settings.get('query_profiler_real_time_period_ns');
    const traceLogCap = capabilities.find(c => c.id === 'trace_log');
    if (traceLogCap && traceLogCap.available) {
      const cpuNs = cpuProfilerPeriod ? parseInt(cpuProfilerPeriod.value, 10) : 0;
      const realNs = realProfilerPeriod ? parseInt(realProfilerPeriod.value, 10) : 0;
      const parts: string[] = [];
      if (cpuNs > 0) parts.push(`CPU: ${(cpuNs / 1_000_000).toFixed(0)}ms`);
      else parts.push('CPU profiler: off');
      if (realNs > 0) parts.push(`Real: ${(realNs / 1_000_000).toFixed(0)}ms`);
      else parts.push('Real profiler: off');
      traceLogCap.detail = `${traceLogCap.detail} · ${parts.join(', ')}`;
    }

    // Add ZooKeeper capability
    capabilities.push({
      id: 'zookeeper',
      label: 'ZooKeeper / Keeper',
      description: 'Coordination service for replication. Enables replica health monitoring.',
      available: hasZk,
      category: 'introspection',
      detail: hasZk ? 'Connected' : 'Not configured or not accessible',
    });

    // Add introspection functions capability (needed for flamegraphs and CPU sampling)
    // We probe this by actually calling demangle('') — the setting value in
    // system.settings can be stale or reflect the wrong profile, so a functional
    // test is more reliable.
    const introspectionSetting = settings.get('allow_introspection_functions');
    const introspectionEnabled = hasIntrospection;
    const settingValue = introspectionSetting?.value;
    capabilities.push({
      id: 'introspection_functions',
      label: 'Introspection Functions',
      description: 'Functions like demangle() and addressToSymbol() for stack trace symbolization. Required for flamegraphs and CPU sampling.',
      available: introspectionEnabled,
      category: 'profiling',
      detail: introspectionEnabled
        ? `Enabled${settingValue === '1' ? '' : ' (functional test passed)'}`
        : `Disabled (set allow_introspection_functions=1)`,
    });

    // ClickStack (HyperDX) embedded log viewer — available in CH 26.2+
    const [major, minor] = this.parseVersion(version);
    const hasClickStack = major > 26 || (major === 26 && minor >= 2);
    capabilities.push({
      id: 'clickstack',
      label: 'ClickStack',
      description: 'Embedded HyperDX log viewer UI at /clickstack/. Enables deep-link from trace logs.',
      available: hasClickStack,
      category: 'logging',
      detail: hasClickStack
        ? `Available (v${version})`
        : `Requires ClickHouse 26.2+ (current: v${version})`,
    });

    // ClickHouse Cloud detection — affects feature availability
    // (e.g. no cpu_id in trace_log, no direct OS metrics)
    capabilities.push({
      id: 'cloud_service',
      label: 'ClickHouse Cloud',
      description: 'Managed ClickHouse Cloud service. Some OS-level metrics (cpu_id, OS counters) are not available.',
      available: isCloud,
      category: 'introspection',
      detail: isCloud ? 'ClickHouse Cloud detected' : 'Self-hosted',
    });

    return {
      probedAt: new Date(),
      serverVersion: version,
      capabilities,
    };
  }

  private async probeVersion(): Promise<string> {
    try {
      const rows = await this.adapter.executeQuery<{ version: string }>(tagQuery(PROBE_SERVER_VERSION, sourceTag(TAB_INTERNAL, 'serverVersion')));
      return rows[0]?.version ?? 'unknown';
    } catch {
      return 'unknown';
    }
  }

  private async probeLogTables(): Promise<Map<string, { engine: string; totalRows: number; totalBytes: number }>> {
    const result = new Map<string, { engine: string; totalRows: number; totalBytes: number }>();
    try {
      const rows = await this.adapter.executeQuery<{
        name: string;
        engine: string;
        total_rows: number;
        total_bytes: number;
      }>(tagQuery(PROBE_SYSTEM_LOG_TABLES, sourceTag(TAB_INTERNAL, 'logTables')));

      for (const row of rows) {
        result.set(String(row.name), {
          engine: String(row.engine),
          totalRows: Number(row.total_rows) || 0,
          totalBytes: Number(row.total_bytes) || 0,
        });
      }
    } catch (error) {
      console.error('[MonitoringCapabilitiesService] probeLogTables error:', error);
    }
    return result;
  }

  private async probeSettings(): Promise<Map<string, { value: string; changed: boolean; description: string }>> {
    const result = new Map<string, { value: string; changed: boolean; description: string }>();
    try {
      const rows = await this.adapter.executeQuery<{
        name: string;
        value: string;
        changed: number;
        description: string;
      }>(tagQuery(PROBE_MONITORING_SETTINGS, sourceTag(TAB_INTERNAL, 'settings')));

      for (const row of rows) {
        result.set(String(row.name), {
          value: String(row.value),
          changed: Boolean(row.changed),
          description: String(row.description),
        });
      }
    } catch (error) {
      console.error('[MonitoringCapabilitiesService] probeSettings error:', error);
    }
    return result;
  }

  private async probeZookeeper(): Promise<boolean> {
    try {
      // First check if the system.zookeeper virtual table exists
      const rows = await this.adapter.executeQuery<{ cnt: number }>(tagQuery(PROBE_ZOOKEEPER, sourceTag(TAB_INTERNAL, 'zookeeper')));
      if ((rows[0]?.cnt ?? 0) === 0) return false;

      // Table exists — check if there are any replicated tables as a stronger signal
      try {
        const replicaRows = await this.adapter.executeQuery<{ cnt: number }>(
          tagQuery(`SELECT count() AS cnt FROM (SELECT database, table FROM {{cluster_metadata:system.replicas}} GROUP BY database, table) LIMIT 1`, sourceTag(TAB_INTERNAL, 'replicas'))
        );
        return (replicaRows[0]?.cnt ?? 0) > 0;
      } catch {
        // system.replicas might not be accessible, but zookeeper table exists
        return true;
      }
    } catch {
      return false;
    }
  }
  /**
   * Probe introspection functions by actually calling demangle('').
   * This is more reliable than checking system.settings because the
   * setting value can reflect the wrong profile or be stale when
   * set via users.d/ XML config.
   */
  private async probeIntrospectionFunctions(): Promise<boolean> {
    try {
      await this.adapter.executeQuery<{ ok: number }>(
        tagQuery(`SELECT demangle('') AS ok`, sourceTag(TAB_INTERNAL, 'introspection'))
      );
      return true;
    } catch {
      return false;
    }
  }

  private parseVersion(version: string): [number, number] {
    const match = version.match(/^(\d+)\.(\d+)/);
    if (!match) return [0, 0];
    return [parseInt(match[1], 10), parseInt(match[2], 10)];
  }

  /**
   * Detect ClickHouse Cloud by probing for cloud-specific settings
   * or build options. Safe — returns false on any error.
   */
  private async probeCloudService(): Promise<boolean> {
    try {
      const rows = await this.adapter.executeQuery<{ is_cloud: number }>(
        tagQuery(PROBE_CLOUD_SERVICE, sourceTag(TAB_INTERNAL, 'cloudDetect'))
      );
      return rows.length > 0 && Number(rows[0].is_cloud) === 1;
    } catch {
      return false;
    }
  }
}

function formatRowCount(rows: number): string {
  if (rows < 1000) return rows.toString();
  if (rows < 1_000_000) return `${(rows / 1000).toFixed(1)}K`;
  if (rows < 1_000_000_000) return `${(rows / 1_000_000).toFixed(1)}M`;
  return `${(rows / 1_000_000_000).toFixed(1)}B`;
}
