/**
 * OverviewService
 * Service for collecting real-time metrics for the Overview monitoring page
 */

import type { IClickHouseAdapter } from '../adapters/types.js';
import type {
  ResourceAttribution,
  RunningQueryInfo,
  ActiveMergeInfo,
  ReplicationSummary,
  AlertInfo,
  OverviewData,
  AlertThresholds,
  QueryConcurrency,
  QpsPoint,
  DeepDiveWidgets,
} from '../types/overview.js';
import { DEFAULT_ALERT_THRESHOLDS } from '../types/overview.js';
import {
  GET_RUNNING_QUERIES,
  GET_ACTIVE_MERGES_LIVE,
  GET_INSTANT_METRICS,
  GET_ASYNC_METRICS,
  GET_RECENT_MERGE_CPU,
  GET_RECENT_MUTATION_CPU,
  GET_PARTS_ALERTS,
  GET_DISK_ALERTS,
  GET_REPLICATION_SUMMARY,
  GET_PK_MEMORY,
  GET_DICT_MEMORY,
  GET_SERVER_INFO,
  GET_CLUSTER_RESOURCE_CAPACITY,
  GET_CLUSTER_ASYNC_METRICS,
  GET_REJECTED_QUERIES_COUNT,
  GET_QPS_HISTORY,
  GET_MAX_CONCURRENT_QUERIES,
  GET_RECENT_IO_RATES,
  GET_QUERY_MONITOR_STATS,
} from '../queries/overview-queries.js';
import { buildQuery, tagQuery } from '../queries/builder.js';
import { TAB_OVERVIEW, TAB_QUERIES, sourceTag } from '../queries/source-tags.js';

export class OverviewServiceError extends Error {
  constructor(message: string, public readonly cause?: Error) {
    super(message);
    this.name = 'OverviewServiceError';
  }
}

/**
 * Calculate CPU cores used from ProfileEvents
 * Formula: (UserTimeMicroseconds + SystemTimeMicroseconds) / (elapsed * 1_000_000)
 */
export function calculateCpuCores(
  userTimeMicroseconds: number,
  systemTimeMicroseconds: number,
  elapsedSeconds: number
): number {
  if (elapsedSeconds <= 0) return 0;
  return (userTimeMicroseconds + systemTimeMicroseconds) / (elapsedSeconds * 1_000_000);
}

/**
 * Calculate query progress percentage
 * Formula: (read_rows / total_rows_approx) * 100, clamped to [0, 100]
 */
export function calculateProgress(readRows: number, totalRowsApprox: number): number {
  if (totalRowsApprox <= 0) return 0;
  const progress = (readRows / totalRowsApprox) * 100;
  return Math.max(0, Math.min(100, progress));
}

/**
 * Calculate rate from bytes and elapsed time
 */
export function calculateRate(bytes: number, elapsedSeconds: number): number {
  if (elapsedSeconds <= 0) return 0;
  return bytes / elapsedSeconds;
}

/**
 * Detect if a mutation is stuck (running > 1 hour with parts remaining)
 */
export function isStuckMutation(elapsedSeconds: number, partsToDo: number): boolean {
  return elapsedSeconds > 3600 && partsToDo > 0;
}

/**
 * Check if a parts alert should be generated
 * Alert is generated iff part_count > 150
 */
export function shouldGeneratePartsAlert(partCount: number): boolean {
  return partCount > 150;
}

/**
 * Check if a disk alert should be generated
 * Alert is generated iff free_space / total_space < 0.15
 */
export function shouldGenerateDiskAlert(freeSpace: number, totalSpace: number): boolean {
  if (totalSpace <= 0) return false;
  return freeSpace / totalSpace < 0.15;
}

/**
 * Check if a readonly replica alert should be generated
 * Alert is generated iff is_readonly = true
 */
export function shouldGenerateReadonlyAlert(isReadonly: boolean): boolean {
  return isReadonly === true;
}

/**
 * Calculate attribution remainder ("other" category)
 * Formula: total - sum(all_attributed_categories), clamped to >= 0
 * 
 * This handles the case where attributed values may exceed total due to timing
 * differences in data collection.
 */
export function calculateAttributionRemainder(total: number, attributedValues: number[]): number {
  const sumAttributed = attributedValues.reduce((sum, val) => sum + val, 0);
  return Math.max(0, total - sumAttributed);
}

/**
 * Format uptime seconds to human-readable string
 */
function formatUptime(seconds: number): string {
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  const mins = Math.floor((seconds % 3600) / 60);
  
  if (days > 0) return `${days}d ${hours}h`;
  if (hours > 0) return `${hours}h ${mins}m`;
  return `${mins}m`;
}

import type { EnvironmentDetector } from './environment-detector.js';

export class OverviewService {
  private thresholds: AlertThresholds;
  private envDetector: EnvironmentDetector | null;

  constructor(
    private adapter: IClickHouseAdapter,
    thresholds: Partial<AlertThresholds> = {},
    envDetector?: EnvironmentDetector,
  ) {
    this.thresholds = { ...DEFAULT_ALERT_THRESHOLDS, ...thresholds };
    this.envDetector = envDetector ?? null;
  }

  /**
   * Update alert thresholds at runtime
   */
  setThresholds(thresholds: Partial<AlertThresholds>): void {
    this.thresholds = { ...this.thresholds, ...thresholds };
  }

  /**
   * Get current alert thresholds
   */
  getThresholds(): AlertThresholds {
    return { ...this.thresholds };
  }

  /**
   * Get all overview data in a single call
   * Combines data from all polling tiers
   */
  async getOverviewData(): Promise<OverviewData> {
    try {
      // Fetch all data in parallel
      const [
        serverInfo,
        runningQueries,
        activeMerges,
        instantMetrics,
        asyncMetrics,
        recentMergeCPU,
        recentMutationCPU,
        partsAlerts,
        diskAlerts,
        replication,
        pkMemory,
        dictMemory,
        rejectedCount,
        qpsHistory,
        maxConcurrent,
        ioRates,
      ] = await Promise.all([
        this.getServerInfo(),
        this.getRunningQueries(),
        this.getActiveMerges(),
        this.getInstantMetrics(),
        this.getAsyncMetrics(),
        this.getRecentCompletedMergeCPU(35),
        this.getRecentCompletedMutationCPU(35),
        this.getPartsAlerts(),
        this.getDiskAlerts(),
        this.getReplicationSummary(),
        this.getPKMemory(),
        this.getDictMemory(),
        this.getRejectedQueriesCount(),
        this.getQpsHistory(),
        this.getMaxConcurrentQueries(),
        this.getRecentIORates(),
      ]);

      // Calculate resource attribution — pass cluster-wide totals from serverInfo
      const resourceAttribution = this.calculateResourceAttribution(
        asyncMetrics,
        instantMetrics,
        runningQueries,
        activeMerges,
        recentMergeCPU,
        recentMutationCPU,
        pkMemory,
        dictMemory,
        serverInfo.cores,
        serverInfo.totalRAM,
        ioRates,
      );

      // Combine alerts
      const alerts: AlertInfo[] = [
        ...partsAlerts,
        ...diskAlerts,
        ...this.getReadonlyReplicaAlerts(replication),
      ];

      // Extract query concurrency metrics
      const queryConcurrency: QueryConcurrency = {
        running: instantMetrics.get('Query') || 0,
        queued: instantMetrics.get('QueryPreempted') || 0,
        maxConcurrent,
        rejectedRecent: rejectedCount,
        qpsHistory,
      };

      return {
        serverInfo,
        resourceAttribution,
        runningQueries,
        activeMerges,
        replication,
        queryConcurrency,
        alerts,
        deepDiveWidgets: undefined,
        lastPollTime: new Date(),
      };
    } catch (error) {
      throw new OverviewServiceError('Failed to get overview data', error as Error);
    }
  }

  /**
   * Lightweight query for the Query Monitor stat cards only.
   * Returns QueryConcurrency in a single round-trip instead of the 16-query
   * getOverviewData() call.
   */
  async getQueryMonitorStats(): Promise<QueryConcurrency> {
    const rows = await this.adapter.executeQuery<{
      src: string;
      key: string;
      val: string;
    }>(tagQuery(GET_QUERY_MONITOR_STATS, sourceTag(TAB_QUERIES, 'monitorStats')));

    let running = 0;
    let queued = 0;
    let maxConcurrent = 0;
    let rejectedRecent = 0;
    const qpsHistory: QpsPoint[] = [];

    for (const row of rows) {
      switch (row.src) {
        case 'metric':
          if (row.key === 'Query') running = Number(row.val) || 0;
          if (row.key === 'QueryPreempted') queued = Number(row.val) || 0;
          break;
        case 'setting':
          maxConcurrent = Number(row.val) || 0;
          break;
        case 'rejected':
          rejectedRecent = Number(row.val) || 0;
          break;
        case 'qps':
          qpsHistory.push({ time: row.key, qps: Number(row.val) || 0 });
          break;
      }
    }

    return { running, queued, maxConcurrent, rejectedRecent, qpsHistory };
  }

  // ===========================================================================
  // Tier 1: Virtual Tables (5s polling)
  // ===========================================================================

  async getRunningQueries(): Promise<RunningQueryInfo[]> {
    try {
      const rows = await this.adapter.executeQuery<{
        hostname: string;
        query_id: string;
        user: string;
        elapsed: number;
        memory_usage: number;
        read_rows: number;
        read_bytes: number;
        total_rows_approx: number;
        query_kind: string;
        query: string;
        user_time_us: number;
        system_time_us: number;
        os_read_bytes: number;
        os_write_bytes: number;
        selected_parts: number;
        selected_marks: number;
        mark_cache_hits: number;
        mark_cache_misses: number;
      }>(tagQuery(GET_RUNNING_QUERIES, sourceTag(TAB_OVERVIEW, 'runningQueries')));

      return rows.map(row => {
        const elapsed = Number(row.elapsed) || 0;
        const userTimeUs = Number(row.user_time_us) || 0;
        const systemTimeUs = Number(row.system_time_us) || 0;
        const readRows = Number(row.read_rows) || 0;
        const totalRowsApprox = Number(row.total_rows_approx) || 0;
        const osReadBytes = Number(row.os_read_bytes) || 0;

        return {
          queryId: String(row.query_id),
          user: String(row.user),
          elapsed,
          cpuCores: calculateCpuCores(userTimeUs, systemTimeUs, elapsed),
          memoryUsage: Number(row.memory_usage) || 0,
          ioReadRate: calculateRate(osReadBytes, elapsed),
          rowsRead: readRows,
          bytesRead: Number(row.read_bytes) || 0,
          progress: calculateProgress(readRows, totalRowsApprox),
          queryKind: String(row.query_kind) || 'Unknown',
          query: String(row.query),
          hostname: String(row.hostname || ''),
          profileEvents: {
            userTimeMicroseconds: userTimeUs,
            systemTimeMicroseconds: systemTimeUs,
            osReadBytes,
            osWriteBytes: Number(row.os_write_bytes) || 0,
            selectedParts: Number(row.selected_parts) || 0,
            selectedMarks: Number(row.selected_marks) || 0,
            markCacheHits: Number(row.mark_cache_hits) || 0,
            markCacheMisses: Number(row.mark_cache_misses) || 0,
          },
        };
      });
    } catch (error) {
      console.error('[OverviewService] getRunningQueries error:', error);
      return [];
    }
  }

  async getActiveMerges(): Promise<ActiveMergeInfo[]> {
    try {
      const rows = await this.adapter.executeQuery<{
        hostname: string;
        database: string;
        table: string;
        part_name: string;
        elapsed: number;
        progress: number;
        memory_usage: number;
        bytes_read_uncompressed: number;
        bytes_written_uncompressed: number;
        rows_read: number;
        num_parts: number;
        is_mutation: number;
        merge_type: string;
      }>(tagQuery(GET_ACTIVE_MERGES_LIVE, sourceTag(TAB_OVERVIEW, 'activeMerges')));

      return rows.map(row => {
        const elapsed = Number(row.elapsed) || 0;
        const bytesRead = Number(row.bytes_read_uncompressed) || 0;
        const bytesWritten = Number(row.bytes_written_uncompressed) || 0;

        return {
          database: String(row.database),
          table: String(row.table),
          partName: String(row.part_name),
          elapsed,
          progress: Number(row.progress) || 0,
          memoryUsage: Number(row.memory_usage) || 0,
          readBytesPerSec: calculateRate(bytesRead, elapsed),
          writeBytesPerSec: calculateRate(bytesWritten, elapsed),
          rowsRead: Number(row.rows_read) || 0,
          numParts: Number(row.num_parts) || 0,
          isMutation: Boolean(row.is_mutation),
          cpuEstimate: 0, // Will be estimated from historical data
          mergeType: String(row.merge_type || ''),
          hostname: String(row.hostname || ''),
        };
      });
    } catch (error) {
      console.error('[OverviewService] getActiveMerges error:', error);
      return [];
    }
  }


  async getInstantMetrics(): Promise<Map<string, number>> {
    try {
      const rows = await this.adapter.executeQuery<{ metric: string; value: number }>(
        tagQuery(GET_INSTANT_METRICS, sourceTag(TAB_OVERVIEW, 'instantMetrics'))
      );
      return new Map(rows.map(r => [String(r.metric), Number(r.value)]));
    } catch (error) {
      console.error('[OverviewService] getInstantMetrics error:', error);
      return new Map();
    }
  }

  async getAsyncMetrics(): Promise<Map<string, number>> {
    try {
      // Try cluster-aware query first — returns per-host rows
      const rows = await this.adapter.executeQuery<{ hostname?: string; metric: string; value: number }>(
        tagQuery(GET_CLUSTER_ASYNC_METRICS, sourceTag(TAB_OVERVIEW, 'asyncMetrics'))
      );

      // Detect if we got multi-host data
      const hosts = new Set(rows.map(r => String(r.hostname ?? '')).filter(Boolean));
      const hostCount = hosts.size;

      if (hostCount <= 1) {
        // Single-node: return as-is (same as before)
        return new Map(rows.map(r => [String(r.metric), Number(r.value)]));
      }

      // Multi-host: aggregate per metric across hosts.
      // CPU ratios (Normalized) → average across hosts (each is already [0..1] per core)
      // Absolute counters (bytes, memory) → sum across hosts
      // Load averages, Uptime → average across hosts
      const AVERAGE_METRICS = new Set([
        'OSUserTimeNormalized', 'OSSystemTimeNormalized',
        'LoadAverage1',
        'Uptime',
      ]);

      const sums = new Map<string, number>();
      const counts = new Map<string, number>();

      for (const row of rows) {
        const metric = String(row.metric);
        const value = Number(row.value);
        sums.set(metric, (sums.get(metric) || 0) + value);
        counts.set(metric, (counts.get(metric) || 0) + 1);
      }

      const result = new Map<string, number>();
      for (const [metric, sum] of sums) {
        if (AVERAGE_METRICS.has(metric)) {
          result.set(metric, sum / (counts.get(metric) || 1));
        } else {
          result.set(metric, sum);
        }
      }

      // For CPU: the non-normalized OSUserTime/OSSystemTime are absolute seconds
      // and summing them is correct (total CPU time across cluster).
      // The normalized values are averaged (each host's per-core ratio).

      return result;
    } catch (error) {
      // Fallback to local-only query
      try {
        const rows = await this.adapter.executeQuery<{ metric: string; value: number }>(
          tagQuery(GET_ASYNC_METRICS, sourceTag(TAB_OVERVIEW, 'asyncMetrics'))
        );
        return new Map(rows.map(r => [String(r.metric), Number(r.value)]));
      } catch (fallbackError) {
        console.error('[OverviewService] getAsyncMetrics error:', fallbackError);
        return new Map();
      }
    }
  }
  async getRejectedQueriesCount(): Promise<number> {
      try {
        const rows = await this.adapter.executeQuery<{ cnt: number }>(
          tagQuery(GET_REJECTED_QUERIES_COUNT, sourceTag(TAB_OVERVIEW, 'rejectedQueries'))
        );
        return Number(rows[0]?.cnt) || 0;
      } catch (error) {
        // query_log may not be available; gracefully return 0
        console.error('[OverviewService] getRejectedQueriesCount error:', error);
        return 0;
      }
    }
  async getQpsHistory(): Promise<QpsPoint[]> {
    try {
      const rows = await this.adapter.executeQuery<{ t: string; qps: number }>(
        tagQuery(GET_QPS_HISTORY, sourceTag(TAB_OVERVIEW, 'qpsHistory'))
      );
      return rows.map(r => ({ time: String(r.t), qps: Number(r.qps) || 0 }));
    } catch (error) {
      // metric_log may not be available
      console.error('[OverviewService] getQpsHistory error:', error);
      return [];
    }
  }
  async getMaxConcurrentQueries(): Promise<number> {
    try {
      const rows = await this.adapter.executeQuery<{ max_concurrent: number }>(
        tagQuery(GET_MAX_CONCURRENT_QUERIES, sourceTag(TAB_OVERVIEW, 'maxConcurrent'))
      );
      return Number(rows[0]?.max_concurrent) || 100;
    } catch (error) {
      // server_settings may not be accessible; default to 100
      console.error('[OverviewService] getMaxConcurrentQueries error:', error);
      return 100;
    }
  }

  // ===========================================================================
  // Tier 2: Log Tables (30s polling)
  // ===========================================================================

  async getRecentCompletedMergeCPU(windowSeconds: number): Promise<number> {
    try {
      const sql = buildQuery(GET_RECENT_MERGE_CPU, { window_seconds: windowSeconds });
      const rows = await this.adapter.executeQuery<{
        user_time_us: number;
        system_time_us: number;
      }>(tagQuery(sql, sourceTag(TAB_OVERVIEW, 'mergeCpu')));

      if (rows.length === 0) return 0;
      const row = rows[0];
      return (Number(row.user_time_us) || 0) + (Number(row.system_time_us) || 0);
    } catch (error) {
      console.error('[OverviewService] getRecentCompletedMergeCPU error:', error);
      return 0;
    }
  }

  async getRecentCompletedMutationCPU(windowSeconds: number): Promise<number> {
    try {
      const sql = buildQuery(GET_RECENT_MUTATION_CPU, { window_seconds: windowSeconds });
      const rows = await this.adapter.executeQuery<{
        user_time_us: number;
        system_time_us: number;
      }>(tagQuery(sql, sourceTag(TAB_OVERVIEW, 'mutationCpu')));

      if (rows.length === 0) return 0;
      const row = rows[0];
      return (Number(row.user_time_us) || 0) + (Number(row.system_time_us) || 0);
    } catch (error) {
      console.error('[OverviewService] getRecentCompletedMutationCPU error:', error);
      return 0;
    }
  }

  // ===========================================================================
  // Tier 3: Structural Tables (60s polling)
  // ===========================================================================

  async getPartsAlerts(): Promise<AlertInfo[]> {
    try {
      const sql = buildQuery(GET_PARTS_ALERTS, { parts_threshold: this.thresholds.partsPerPartition });
      const rows = await this.adapter.executeQuery<{
        database: string;
        table: string;
        partition_id: string;
        part_count: number;
      }>(tagQuery(sql, sourceTag(TAB_OVERVIEW, 'partsAlerts')));

      return rows.map(row => ({
        severity: 'warn' as const,
        message: `Too many parts: ${row.database}.${row.table} partition ${row.partition_id} has ${row.part_count} parts`,
        source: 'parts' as const,
        details: {
          database: String(row.database),
          table: String(row.table),
          partition: String(row.partition_id),
          value: Number(row.part_count),
        },
      }));
    } catch (error) {
      console.error('[OverviewService] getPartsAlerts error:', error);
      return [];
    }
  }

  async getDiskAlerts(): Promise<AlertInfo[]> {
    try {
      const rows = await this.adapter.executeQuery<{
        name: string;
        path: string;
        free_space: number;
        total_space: number;
        free_ratio: number;
      }>(tagQuery(GET_DISK_ALERTS, sourceTag(TAB_OVERVIEW, 'diskAlerts')));

      return rows
        .filter(row => Number(row.free_ratio) < this.thresholds.diskFreeRatio)
        .map(row => {
          const usedPct = Math.round((1 - Number(row.free_ratio)) * 100);
          return {
            severity: 'warn' as const,
            message: `Low disk space: ${row.name} is ${usedPct}% full`,
            source: 'disk' as const,
            details: {
              value: usedPct,
            },
          };
        });
    } catch (error) {
      console.error('[OverviewService] getDiskAlerts error:', error);
      return [];
    }
  }

  async getReplicationSummary(): Promise<ReplicationSummary> {
    try {
      const rows = await this.adapter.executeQuery<{
        total_tables: number;
        healthy_tables: number;
        readonly_replicas: number;
        max_delay: number;
        queue_size: number;
        active_replicas: number;
      }>(tagQuery(GET_REPLICATION_SUMMARY, sourceTag(TAB_OVERVIEW, 'replication')));

      if (rows.length === 0) {
        return {
          totalTables: 0,
          healthyTables: 0,
          readonlyReplicas: 0,
          maxDelay: 0,
          queueSize: 0,
          fetchesActive: 0,
        };
      }

      const row = rows[0];
      return {
        totalTables: Number(row.total_tables) || 0,
        healthyTables: Number(row.healthy_tables) || 0,
        readonlyReplicas: Number(row.readonly_replicas) || 0,
        maxDelay: Number(row.max_delay) || 0,
        queueSize: Number(row.queue_size) || 0,
        fetchesActive: 0, // Will be fetched from instant metrics
      };
    } catch (error) {
      console.error('[OverviewService] getReplicationSummary error:', error);
      return {
        totalTables: 0,
        healthyTables: 0,
        readonlyReplicas: 0,
        maxDelay: 0,
        queueSize: 0,
        fetchesActive: 0,
      };
    }
  }

  async getPKMemory(): Promise<number> {
    try {
      const rows = await this.adapter.executeQuery<{ pk_bytes: number }>(tagQuery(GET_PK_MEMORY, sourceTag(TAB_OVERVIEW, 'pkMemory')));
      return rows.length > 0 ? Number(rows[0].pk_bytes) || 0 : 0;
    } catch (error) {
      console.error('[OverviewService] getPKMemory error:', error);
      return 0;
    }
  }

  async getDictMemory(): Promise<number> {
    try {
      const rows = await this.adapter.executeQuery<{ dict_bytes: number }>(tagQuery(GET_DICT_MEMORY, sourceTag(TAB_OVERVIEW, 'dictMemory')));
      return rows.length > 0 ? Number(rows[0].dict_bytes) || 0 : 0;
    } catch (error) {
      console.error('[OverviewService] getDictMemory error:', error);
      return 0;
    }
  }

  async getRecentIORates(): Promise<{ readBytesPerSec: number; writeBytesPerSec: number }> {
    try {
      const rows = await this.adapter.executeQuery<{
        read_bytes_per_sec: number;
        write_bytes_per_sec: number;
      }>(tagQuery(GET_RECENT_IO_RATES, sourceTag(TAB_OVERVIEW, 'ioRates')));
      if (rows.length > 0) {
        return {
          readBytesPerSec: Number(rows[0].read_bytes_per_sec) || 0,
          writeBytesPerSec: Number(rows[0].write_bytes_per_sec) || 0,
        };
      }
      return { readBytesPerSec: 0, writeBytesPerSec: 0 };
    } catch (error) {
      console.error('[OverviewService] getRecentIORates error:', error);
      return { readBytesPerSec: 0, writeBytesPerSec: 0 };
    }
  }

  async getServerInfo(): Promise<OverviewData['serverInfo']> {
    try {
      const [infoRows, asyncMetrics, clusterCapacity] = await Promise.all([
        this.adapter.executeQuery<{ hostname: string; version: string }>(tagQuery(GET_SERVER_INFO, sourceTag(TAB_OVERVIEW, 'serverInfo'))),
        this.getAsyncMetrics(),
        this.getClusterResourceCapacity(),
      ]);

      const info = infoRows.length > 0 ? infoRows[0] : { hostname: 'unknown', version: 'unknown' };
      const uptime = asyncMetrics.get('Uptime') || 0;

      // Use cluster-wide totals (summed across all nodes, cgroup-aware)
      let cores = clusterCapacity.totalCores;
      let totalRAM = clusterCapacity.totalMemory;

      // Fallback to local-only detection independently for cores and memory
      let environment: OverviewData['serverInfo']['environment'];
      const needCoresFallback = cores === 0;
      const needMemFallback = totalRAM === 0;

      if (needCoresFallback || needMemFallback) {
        const hostCores = asyncMetrics.get('OSProcessorsCount') || 0;
        const hostRAM = asyncMetrics.get('OSMemoryTotal') || 0;
        if (needCoresFallback) cores = hostCores;
        if (needMemFallback) totalRAM = hostRAM;

        if (this.envDetector) {
          const env = await this.envDetector.detect();
          if (needCoresFallback && env.effectiveCores > 0) cores = env.effectiveCores;
          if (needMemFallback && env.effectiveMemoryBytes > 0) totalRAM = env.effectiveMemoryBytes;
          environment = {
            isContainerized: env.isContainerized,
            isKubernetes: env.isKubernetes,
            effectiveCores: env.effectiveCores,
            hostCores: env.hostCores,
            isCgroupLimited: env.isCgroupLimited,
          };
        }
      } else if (this.envDetector) {
        const env = await this.envDetector.detect();
        environment = {
          isContainerized: env.isContainerized,
          isKubernetes: env.isKubernetes,
          effectiveCores: env.effectiveCores,
          hostCores: env.hostCores,
          isCgroupLimited: env.isCgroupLimited,
        };
      }

      return {
        hostname: String(info.hostname),
        version: String(info.version),
        uptime: formatUptime(uptime),
        cores,
        totalRAM,
        environment,
        clusterHosts: clusterCapacity.hostnames,
      };
    } catch (error) {
      console.error('[OverviewService] getServerInfo error:', error);
      return {
        hostname: 'unknown',
        version: 'unknown',
        uptime: '0m',
        cores: 0,
        totalRAM: 0,
        clusterHosts: [],
      };
    }
  }

  /**
   * Fetch per-host resource capacity across the cluster and sum totals.
   * Each node's effective cores/memory is cgroup-aware.
   */
  private async getClusterResourceCapacity(): Promise<{ totalCores: number; totalMemory: number; hostnames: string[] }> {
    try {
      const rows = await this.adapter.executeQuery<{
        hostname: string;
        host_cores: number;
        cgroup_cpu: number;
        host_mem: number;
        cgroup_mem: number;
      }>(tagQuery(GET_CLUSTER_RESOURCE_CAPACITY, sourceTag(TAB_OVERVIEW, 'clusterCapacity')));

      let totalCores = 0;
      let totalMemory = 0;
      const hostnames: string[] = [];

      for (const row of rows) {
        const hostCores = Number(row.host_cores) || 0;
        const cgroupCpu = Number(row.cgroup_cpu) || 0;
        const hostMem = Number(row.host_mem) || 0;
        const cgroupMem = Number(row.cgroup_mem) || 0;

        if (row.hostname) hostnames.push(String(row.hostname));

        // Effective cores: cgroup limit if set and less than host
        const effectiveCores = (cgroupCpu > 0 && cgroupCpu < hostCores)
          ? Math.round(cgroupCpu)
          : hostCores;

        // Effective memory: cgroup limit if set and ≤ host (cgroup v2 may report OSMemoryTotal = cgroup limit)
        const effectiveMem = (cgroupMem > 0 && cgroupMem < 1e18 && cgroupMem <= hostMem)
          ? cgroupMem
          : hostMem;

        totalCores += effectiveCores;
        totalMemory += effectiveMem;
      }

      return { totalCores, totalMemory, hostnames: hostnames.sort() };
    } catch (error) {
      console.error('[OverviewService] getClusterResourceCapacity error:', error);
      return { totalCores: 0, totalMemory: 0, hostnames: [] };
    }
  }

  // ===========================================================================
  // Attribution Calculations
  // ===========================================================================

  calculateResourceAttribution(
    asyncMetrics: Map<string, number>,
    instantMetrics: Map<string, number>,
    queries: RunningQueryInfo[],
    merges: ActiveMergeInfo[],
    recentMergeCPU: number,
    recentMutationCPU: number,
    pkMemory: number,
    dictMemory: number,
    clusterTotalCores?: number,
    clusterTotalRAM?: number,
    ioRates?: { readBytesPerSec: number; writeBytesPerSec: number },
  ): ResourceAttribution {
    // Use normalized CPU metrics (already in [0..1] range per core)
    // OSUserTimeNormalized + OSSystemTimeNormalized = total CPU usage as ratio
    const userTimeNorm = asyncMetrics.get('OSUserTimeNormalized') || 0;
    const systemTimeNorm = asyncMetrics.get('OSSystemTimeNormalized') || 0;
    
    // Total CPU usage as percentage (0-100)
    const totalCpuPct = Math.min(100, (userTimeNorm + systemTimeNorm) * 100);
    
    // Use cluster-wide core count if provided (summed across all nodes, cgroup-aware).
    // Fall back to local EnvironmentDetector or metric-derived count.
    let cores = 1;
    if (clusterTotalCores && clusterTotalCores > 0) {
      cores = clusterTotalCores;
    } else {
      const envInfo = this.envDetector?.getCached();
      if (envInfo && envInfo.effectiveCores > 0) {
        cores = envInfo.effectiveCores;
      } else {
        const userTimeTotal = asyncMetrics.get('OSUserTime') || 0;
        if (userTimeNorm > 0.001 && userTimeTotal > 0) {
          cores = Math.round(userTimeTotal / userTimeNorm);
        }
      }
    }
    
    // Calculate CPU attribution based on ACTUAL running operations, not thread pools
    // Thread pool metrics (MergeTreeBackgroundExecutorThreadsActive) are misleading
    // because they handle more than just merges
    
    // For merges, we don't have direct CPU metrics, but we can estimate based on
    // whether merges are actually running (from system.merges)
    const actualMergeCount = merges.filter(m => !m.isMutation).length;
    const actualMutationCount = merges.filter(m => m.isMutation).length;
    
    // Calculate breakdown
    let queryCpuPct = 0;
    let mergeCpuPct = 0;
    let mutationCpuPct = 0;
    let otherCpuPct = totalCpuPct;
    
    if (totalCpuPct > 0) {
      // Calculate per-query CPU cores and sum them.
      // Each query's CPU cores = its CPU time / its own elapsed time.
      // Summing across queries gives total cores consumed by all queries.
      let queryCoresUsed = 0;
      for (const q of queries) {
        const userTime = q.profileEvents?.userTimeMicroseconds || 0;
        const systemTime = q.profileEvents?.systemTimeMicroseconds || 0;
        const cpuTimeUs = userTime + systemTime;
        if (q.elapsed > 0 && cpuTimeUs > 0) {
          queryCoresUsed += cpuTimeUs / (q.elapsed * 1_000_000);
        }
      }
      queryCpuPct = Math.min(totalCpuPct, (queryCoresUsed / cores) * 100);
      
      // Only attribute CPU to merges/mutations if they're actually running
      // Use a simple heuristic: if merges are running, they likely use some CPU
      if (actualMergeCount > 0) {
        // Estimate merge CPU based on remaining CPU after queries
        // Merges typically use 1-2 cores each when active
        const estimatedMergeCores = Math.min(actualMergeCount * 1.5, cores * 0.5);
        mergeCpuPct = Math.min(totalCpuPct - queryCpuPct, (estimatedMergeCores / cores) * 100);
      }
      
      if (actualMutationCount > 0) {
        const estimatedMutationCores = Math.min(actualMutationCount * 1.0, cores * 0.3);
        mutationCpuPct = Math.min(totalCpuPct - queryCpuPct - mergeCpuPct, (estimatedMutationCores / cores) * 100);
      }
      
      // Everything else goes to "other"
      otherCpuPct = Math.max(0, totalCpuPct - queryCpuPct - mergeCpuPct - mutationCpuPct);
    }

    return {
      cpu: {
        totalPct: Math.round(totalCpuPct * 10) / 10,
        cores,
        breakdown: {
          queries: Math.round(queryCpuPct * 10) / 10,
          merges: Math.round(mergeCpuPct * 10) / 10,
          mutations: Math.round(mutationCpuPct * 10) / 10,
          other: Math.round(otherCpuPct * 10) / 10,
        },
      },
      memory: this.calculateMemoryAttribution(asyncMetrics, queries, merges, pkMemory, dictMemory, clusterTotalRAM),
      io: this.calculateIOAttribution(ioRates, queries, merges),
    };
  }

  private calculateMemoryAttribution(
    asyncMetrics: Map<string, number>,
    queries: RunningQueryInfo[],
    merges: ActiveMergeInfo[],
    pkMemory: number,
    dictMemory: number,
    clusterTotalRAM?: number,
  ): ResourceAttribution['memory'] {
    const totalRSS = asyncMetrics.get('MemoryResident') || 0;
    // Use cluster-wide RAM if provided, otherwise fall back to local cgroup-aware value
    let totalRAM: number;
    if (clusterTotalRAM && clusterTotalRAM > 0) {
      totalRAM = clusterTotalRAM;
    } else {
      // OSMemoryTotal reports host RAM — in containers, use cgroup-aware effective memory
      totalRAM = asyncMetrics.get('OSMemoryTotal') || 0;
      const envInfo = this.envDetector?.getCached();
      if (envInfo && envInfo.effectiveMemoryBytes > 0) {
        totalRAM = envInfo.effectiveMemoryBytes;
      }
    }
    const markCache = asyncMetrics.get('MarkCacheBytes') || 0;
    const uncompCache = asyncMetrics.get('UncompressedCacheBytes') || 0;

    const queryMemory = queries.reduce((sum, q) => sum + q.memoryUsage, 0);
    const mergeMemory = merges.reduce((sum, m) => sum + m.memoryUsage, 0);

    const trackedMemory = queryMemory + mergeMemory + markCache + uncompCache + pkMemory + dictMemory;
    const otherMemory = Math.max(0, totalRSS - trackedMemory);

    return {
      totalRSS,
      totalRAM,
      tracked: trackedMemory,
      breakdown: {
        queries: queryMemory,
        merges: mergeMemory,
        markCache,
        uncompressedCache: uncompCache,
        primaryKeys: pkMemory,
        dictionaries: dictMemory,
        other: otherMemory,
      },
    };
  }

  private calculateIOAttribution(
    ioRates: { readBytesPerSec: number; writeBytesPerSec: number } | undefined,
    queries: RunningQueryInfo[],
    merges: ActiveMergeInfo[]
  ): ResourceAttribution['io'] {
    const totalRead = ioRates?.readBytesPerSec ?? 0;
    const totalWrite = ioRates?.writeBytesPerSec ?? 0;

    // Query I/O: profileEvents are cumulative bytes, convert to rate using ioReadRate (already a rate)
    const queryReadRate = queries.reduce((sum, q) => sum + (q.ioReadRate || 0), 0);
    // For write rate, convert cumulative bytes to rate via elapsed
    const queryWriteRate = queries.reduce((sum, q) => {
      const writeBytes = q.profileEvents?.osWriteBytes || 0;
      return sum + (q.elapsed > 0 ? writeBytes / q.elapsed : 0);
    }, 0);
    // Merge I/O rates are already bytes/sec
    const mergeReadRate = merges.reduce((sum, m) => sum + m.readBytesPerSec, 0);
    const mergeWriteRate = merges.reduce((sum, m) => sum + m.writeBytesPerSec, 0);

    return {
      readBytesPerSec: totalRead,
      writeBytesPerSec: totalWrite,
      breakdown: {
        queryRead: queryReadRate,
        queryWrite: queryWriteRate,
        mergeRead: mergeReadRate,
        mergeWrite: mergeWriteRate,
        replicationRead: 0,
        replicationWrite: 0,
      },
    };
  }


  private getReadonlyReplicaAlerts(replication: ReplicationSummary): AlertInfo[] {
    if (replication.readonlyReplicas === 0) return [];
    return [{
      severity: 'crit' as const,
      message: `${replication.readonlyReplicas} replica(s) in readonly mode`,
      source: 'replica' as const,
      details: {
        value: replication.readonlyReplicas,
      },
    }];
  }
}
