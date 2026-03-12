import type { IClickHouseAdapter } from '../adapters/types.js';
import type { ServerMetrics, HistoricalMetricsPoint, ClusterHistoricalMetricsPoint } from '../types/metrics.js';
import { METRICS_QUERY, MEMORY_INFO_QUERY, CPU_METRICS_QUERY } from '../queries/metrics-queries.js';
import { 
  SERVER_CPU_TIMESERIES, 
  SERVER_MEMORY_TIMESERIES, 
  SERVER_DISK_IO_TIMESERIES,
  SERVER_NETWORK_TIMESERIES,
  SERVER_TOTAL_RAM,
  SERVER_CPU_CORES,
  SERVER_CPU_CORES_FALLBACK,
  SERVER_CPU_CORES_FALLBACK2,
  SERVER_CGROUP_CPU,
  SERVER_MAX_THREADS,
  CLUSTER_CPU_TIMESERIES,
  CLUSTER_MEMORY_TIMESERIES,
  CLUSTER_DISK_IO_TIMESERIES,
  CLUSTER_NETWORK_TIMESERIES,
  CLUSTER_TOTAL_RAM,
  CLUSTER_CGROUP_MEMORY,
  CLUSTER_CPU_CORES,
  CLUSTER_CGROUP_CPU,
} from '../queries/timeline-queries.js';
import { buildQuery, tagQuery } from '../queries/builder.js';
import { TAB_OVERVIEW, sourceTag } from '../queries/source-tags.js';

export class MetricsCollectionError extends Error {
  constructor(message: string, public readonly cause?: Error) {
    super(message);
    this.name = 'MetricsCollectionError';
  }
}

export class MetricsCollector {
  constructor(private adapter: IClickHouseAdapter) {}

  async getServerMetrics(): Promise<ServerMetrics> {
    try {
      const [metricsRows, memoryRows, cpuRows] = await Promise.all([
        this.adapter.executeQuery<{ metric: string; value: number }>(tagQuery(METRICS_QUERY, sourceTag(TAB_OVERVIEW, 'serverMetrics'))),
        this.adapter.executeQuery<{ metric: string; value: number }>(tagQuery(MEMORY_INFO_QUERY, sourceTag(TAB_OVERVIEW, 'memoryInfo'))),
        this.adapter.executeQuery<{ metric: string; value: number }>(tagQuery(CPU_METRICS_QUERY, sourceTag(TAB_OVERVIEW, 'cpuMetrics'))),
      ]);

      const metrics = new Map(metricsRows.map(r => [String(r.metric), Number(r.value)]));
      const memory = new Map(memoryRows.map(r => [String(r.metric), Number(r.value)]));
      const cpu = new Map(cpuRows.map(r => [String(r.metric), Number(r.value)]));

      // In containers, OSMemoryTotal reports host RAM — use cgroup limit if available
      const hostMemory = memory.get('OSMemoryTotal') ?? 0;
      const cgroupMemLimit = memory.get('CGroupMemoryTotal') ?? memory.get('CGroupMemoryLimit') ?? 0;
      const effectiveMemory = (cgroupMemLimit > 0 && cgroupMemLimit < 1e18 && cgroupMemLimit < hostMemory)
        ? cgroupMemLimit
        : hostMemory;

      return {
        timestamp: new Date().toISOString(),
        cpu_usage: this.calculateCpuUsage(cpu),
        memory_used: metrics.get('MemoryTracking') ?? 0,
        memory_total: effectiveMemory,
        disk_read_bytes: metrics.get('ReadBufferFromFileDescriptorReadBytes') ?? 0,
        disk_write_bytes: metrics.get('WriteBufferFromFileDescriptorWriteBytes') ?? 0,
        uptime_seconds: metrics.get('uptime') ?? 0,
      };
    } catch (error) {
      throw new MetricsCollectionError('Failed to get server metrics', error as Error);
    }
  }

  /**
   * Get historical metrics from ClickHouse metric logs.
   * Uses the same queries as TimelineService for consistency.
   * Converts cumulative counters to rates.
   */
  /**
     * Get historical metrics from ClickHouse metric logs.
     * Uses per-host queries and aggregates across nodes:
     * - CPU & memory: averaged across hosts
     * - Disk & network IO: summed across hosts
     */
    async getHistoricalMetrics(
      fromTime: Date,
      toTime: Date,
      _intervalSeconds: number = 10
    ): Promise<HistoricalMetricsPoint[]> {
      try {
        const { hosts, data } = await this.getClusterHistoricalMetrics(fromTime, toTime);

        if (data.length === 0) return [];

        // Single host — no aggregation needed
        if (hosts.length <= 1) {
          return data.map(({ hostname: _h, ...rest }) => rest);
        }

        // Group by timestamp and aggregate across hosts
        const byTime = new Map<number, ClusterHistoricalMetricsPoint[]>();
        for (const p of data) {
          const arr = byTime.get(p.timestamp) || [];
          arr.push(p);
          byTime.set(p.timestamp, arr);
        }

        const results: HistoricalMetricsPoint[] = [];
        for (const [ts, points] of [...byTime.entries()].sort(([a], [b]) => a - b)) {
          const n = points.length;
          results.push({
            timestamp: ts,
            // Average percentages across hosts
            cpu_usage: Math.round(points.reduce((s, p) => s + p.cpu_usage, 0) / n * 100) / 100,
            memory_used: points.reduce((s, p) => s + p.memory_used, 0),
            memory_total: points.reduce((s, p) => s + p.memory_total, 0),
            // Sum IO rates across hosts
            disk_read_rate: Math.round(points.reduce((s, p) => s + p.disk_read_rate, 0)),
            disk_write_rate: Math.round(points.reduce((s, p) => s + p.disk_write_rate, 0)),
            network_send_rate: Math.round(points.reduce((s, p) => s + (p.network_send_rate ?? 0), 0)),
            network_recv_rate: Math.round(points.reduce((s, p) => s + (p.network_recv_rate ?? 0), 0)),
          });
        }

        return results;
      } catch (error) {
        throw new MetricsCollectionError('Failed to get historical metrics', error as Error);
      }
    }

  /**
   * Get historical metrics per host for cluster views.
   * Returns data grouped by hostname so the UI can show per-server or aggregated views.
   */
  async getClusterHistoricalMetrics(
    fromTime: Date,
    toTime: Date,
  ): Promise<{ hosts: string[]; data: ClusterHistoricalMetricsPoint[] }> {
    try {
      const params = {
        start_time: this.toClickHouseDateTime(fromTime),
        end_time: this.toClickHouseDateTime(toTime),
      };

      const [cpuRows, memoryRows, diskRows, networkRows, ramRows, cgroupMemRows, coreRows, cgroupCpuRows] = await Promise.all([
        this.fetchHostTimeseries(CLUSTER_CPU_TIMESERIES, params),
        this.fetchHostTimeseries(CLUSTER_MEMORY_TIMESERIES, params),
        this.fetchHostDualTimeseries(CLUSTER_DISK_IO_TIMESERIES, params, 'read_v', 'write_v'),
        this.fetchHostDualTimeseries(CLUSTER_NETWORK_TIMESERIES, params, 'send_v', 'recv_v'),
        this.fetchHostScalar(CLUSTER_TOTAL_RAM, params),
        this.fetchHostScalar(CLUSTER_CGROUP_MEMORY, params),
        this.fetchHostScalar(CLUSTER_CPU_CORES, params),
        this.fetchHostScalar(CLUSTER_CGROUP_CPU, params),
      ]);

      if (cpuRows.length === 0 && memoryRows.length === 0) {
        return { hosts: [], data: [] };
      }

      // Build per-host lookup maps
      const memoryMap = new Map(memoryRows.map(r => [`${r.host}|${r.t}`, r.v]));
      const diskReadMap = new Map(diskRows.map(r => [`${r.host}|${r.t}`, r.v1]));
      const diskWriteMap = new Map(diskRows.map(r => [`${r.host}|${r.t}`, r.v2]));
      const netSendMap = new Map(networkRows.map(r => [`${r.host}|${r.t}`, r.v1]));
      const netRecvMap = new Map(networkRows.map(r => [`${r.host}|${r.t}`, r.v2]));

      const baseRows = cpuRows.length > 0 ? cpuRows : memoryRows;
      const hosts = [...new Set(baseRows.map(r => r.host))].sort();

      const results: ClusterHistoricalMetricsPoint[] = [];

      for (const row of baseRows) {
        const key = `${row.host}|${row.t}`;
        const timestamp = this.parseTimestamp(row.t);
        // Prefer cgroup CPU limit (k8s/container) over host physical cores
        const hostCores = coreRows.get(row.host) ?? 1;
        const cgroupCpu = cgroupCpuRows.get(row.host) ?? 0;
        const cpuCores = (cgroupCpu > 0 && cgroupCpu < hostCores) ? cgroupCpu : hostCores;
        const hostRam = ramRows.get(row.host) ?? 0;
        const cgroupMem = cgroupMemRows.get(row.host) ?? 0;
        // In containers, OSMemoryTotal reports host RAM — use cgroup limit if available
        const totalRam = (cgroupMem > 0 && cgroupMem < 1e18 && cgroupMem < hostRam)
          ? cgroupMem
          : hostRam;
        const memoryUsed = memoryMap.get(key) ?? 0;

        const intervalSec = (row.interval_ms && row.interval_ms > 0) ? row.interval_ms / 1000 : 1;
        const cpuUsage = cpuCores > 0
          ? Math.min(100, (row.v / (cpuCores * 1_000_000 * intervalSec)) * 100)
          : 0;

        results.push({
          hostname: row.host,
          timestamp,
          cpu_usage: Math.round(cpuUsage * 100) / 100,
          memory_used: memoryUsed,
          memory_total: totalRam,
          disk_read_rate: Math.round(diskReadMap.get(key) ?? 0),
          disk_write_rate: Math.round(diskWriteMap.get(key) ?? 0),
          network_send_rate: Math.round(netSendMap.get(key) ?? 0),
          network_recv_rate: Math.round(netRecvMap.get(key) ?? 0),
        });
      }

      return { hosts, data: results };
    } catch (error) {
      throw new MetricsCollectionError('Failed to get cluster historical metrics', error as Error);
    }
  }

  /**
   * Get distinct hostnames from the cluster.
   *
   * Must use hostname() — NOT host_name from system.clusters — because
   * timeline queries filter with `AND hostname() = '...'`. The host_name
   * column in system.clusters reflects the cluster config (DNS names, IPs)
   * which can differ from what hostname() returns (OS-level hostname),
   * especially in Docker/Kubernetes.
   */
  async getClusterHosts(): Promise<string[]> {
    try {
      const sql = `SELECT DISTINCT hostname() AS host FROM {{cluster_aware:system.one}} ORDER BY host`;
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_OVERVIEW, 'clusterHosts')));
      return rows.map(r => String((r as Record<string, unknown>).host || '')).filter(Boolean);
    } catch {
      return [];
    }
  }

  private toClickHouseDateTime(date: Date): string {
    const year = date.getUTCFullYear();
    const month = String(date.getUTCMonth() + 1).padStart(2, '0');
    const day = String(date.getUTCDate()).padStart(2, '0');
    const hours = String(date.getUTCHours()).padStart(2, '0');
    const minutes = String(date.getUTCMinutes()).padStart(2, '0');
    const seconds = String(date.getUTCSeconds()).padStart(2, '0');
    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
  }

  private parseTimestamp(s: string): number {
    const normalized = s.trim().replace(' ', 'T');
    const withTz = normalized.includes('Z') || normalized.includes('+') ? normalized : normalized + 'Z';
    return new Date(withTz).getTime();
  }

  private async fetchTimeseries(query: string, params: Record<string, string>): Promise<Array<{ t: string; v: number; interval_ms?: number }>> {
    try {
      const sql = buildQuery(query, params);
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_OVERVIEW, 'timeseries')));
      return rows.map(r => ({
        t: String((r as Record<string, unknown>).t || ''),
        v: Number((r as Record<string, unknown>).v || 0),
        interval_ms: (r as Record<string, unknown>).interval_ms !== undefined 
          ? Number((r as Record<string, unknown>).interval_ms) 
          : undefined,
      }));
    } catch (e) {
      console.error('[MetricsCollector] timeseries query error:', e);
      return [];
    }
  }

  private async fetchDualTimeseries(
    query: string, 
    params: Record<string, string>,
    key1: string,
    key2: string
  ): Promise<Array<{ t: string; v1: number; v2: number }>> {
    try {
      const sql = buildQuery(query, params);
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_OVERVIEW, 'timeseries')));
      return rows.map(r => {
        const row = r as Record<string, unknown>;
        return {
          t: String(row.t || ''),
          v1: Number(row[key1] || 0),
          v2: Number(row[key2] || 0),
        };
      });
    } catch (e) {
      console.error('[MetricsCollector] dual timeseries query error:', e);
      return [];
    }
  }

  private async fetchHostTimeseries(query: string, params: Record<string, string>): Promise<Array<{ t: string; host: string; v: number; interval_ms?: number }>> {
    try {
      const sql = buildQuery(query, params);
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_OVERVIEW, 'timeseries')));
      return rows.map(r => {
        const row = r as Record<string, unknown>;
        return {
          t: String(row.t || ''),
          host: String(row.host || ''),
          v: Number(row.v || 0),
          interval_ms: row.interval_ms !== undefined ? Number(row.interval_ms) : undefined,
        };
      });
    } catch (e) {
      console.error('[MetricsCollector] host timeseries query error:', e);
      return [];
    }
  }

  private async fetchHostDualTimeseries(
    query: string,
    params: Record<string, string>,
    key1: string,
    key2: string
  ): Promise<Array<{ t: string; host: string; v1: number; v2: number }>> {
    try {
      const sql = buildQuery(query, params);
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_OVERVIEW, 'timeseries')));
      return rows.map(r => {
        const row = r as Record<string, unknown>;
        return {
          t: String(row.t || ''),
          host: String(row.host || ''),
          v1: Number(row[key1] || 0),
          v2: Number(row[key2] || 0),
        };
      });
    } catch (e) {
      console.error('[MetricsCollector] host dual timeseries query error:', e);
      return [];
    }
  }

  private async fetchHostScalar(query: string, params: Record<string, string>): Promise<Map<string, number>> {
    try {
      const sql = buildQuery(query, params);
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_OVERVIEW, 'timeseries')));
      const map = new Map<string, number>();
      for (const r of rows) {
        const row = r as Record<string, unknown>;
        map.set(String(row.host || ''), Number(row.value || 0));
      }
      return map;
    } catch (e) {
      console.error('[MetricsCollector] host scalar query error:', e);
      return new Map();
    }
  }

  private async fetchTotalRam(params: Record<string, string>): Promise<number> {
    try {
      const sql = buildQuery(SERVER_TOTAL_RAM, params);
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_OVERVIEW, 'totalRam')));
      if (rows.length > 0) {
        return Number((rows[0] as Record<string, unknown>).value || 0);
      }
    } catch (e) {
      console.error('[MetricsCollector] total_ram error:', e);
    }
    return 0;
  }

  private async fetchCpuCores(params: Record<string, string>): Promise<number> {
    // 1. Try cgroup-aware detection first (Kubernetes / containerized environments)
    const cgroupCores = await this.fetchCgroupCpuLimit();
    if (cgroupCores > 0) return cgroupCores;

    // 2. Try asynchronous_metric_log first (same as TimelineService)
    try {
      const sql = buildQuery(SERVER_CPU_CORES, params);
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_OVERVIEW, 'cpuCores')));
      if (rows.length > 0) {
        const val = Number((rows[0] as Record<string, unknown>).value || 0);
        if (val > 0) return val;
      }
    } catch (e) {
      console.error('[MetricsCollector] cpu_cores from log error:', e);
    }

    // 3. Fallback to asynchronous_metrics
    try {
      const rows = await this.adapter.executeQuery(tagQuery(SERVER_CPU_CORES_FALLBACK, sourceTag(TAB_OVERVIEW, 'cpuCores')));
      if (rows.length > 0) {
        const val = Number((rows[0] as Record<string, unknown>).value || 0);
        if (val > 0) return val;
      }
    } catch (e) {
      console.error('[MetricsCollector] cpu_cores from metrics error:', e);
    }

    // 4. Fallback to counting OSUserTimeCPU metrics
    try {
      const rows = await this.adapter.executeQuery(tagQuery(SERVER_CPU_CORES_FALLBACK2, sourceTag(TAB_OVERVIEW, 'cpuCores')));
      if (rows.length > 0) {
        const val = Number(Object.values(rows[0] as Record<string, unknown>)[0] || 0);
        if (val > 0) return val;
      }
    } catch (e) {
      console.error('[MetricsCollector] cpu_cores from OSUserTimeCPU count error:', e);
    }

    console.warn('[MetricsCollector] Could not determine CPU cores, defaulting to 1');
    return 1;
  }

  private async fetchCgroupCpuLimit(): Promise<number> {
    try {
      const rows = await this.adapter.executeQuery(
        tagQuery(SERVER_CGROUP_CPU, sourceTag(TAB_OVERVIEW, 'cgroupCpu'))
      );
      if (rows.length > 0) {
        const val = Number((rows[0] as Record<string, unknown>).value || 0);
        if (val > 0) return Math.round(val);
      }
    } catch (err) {
      console.warn('[MetricsCollector] CGroup CPU metric not available:', err);
    }

    try {
      const rows = await this.adapter.executeQuery(
        tagQuery(SERVER_MAX_THREADS, sourceTag(TAB_OVERVIEW, 'maxThreads'))
      );
      if (rows.length > 0) {
        const val = parseInt(String((rows[0] as Record<string, unknown>).value || '0'), 10);
        if (val > 0 && val <= 256) return val;
      }
    } catch (err) {
      console.warn('[MetricsCollector] system.settings not accessible:', err);
    }

    return 0;
  }

  private calculateCpuUsage(cpu: Map<string, number>): number {
    // Try load average relative to CPU cores first (matches Python implementation)
    const loadAvg1 = cpu.get('LoadAverage1') ?? 0;
    const numCores = cpu.get('NumberOfPhysicalCPUCores') ?? 0;

    if (numCores > 0 && loadAvg1 >= 0) {
      const usage = Math.min(100, (loadAvg1 / numCores) * 100);
      return Math.round(usage * 100) / 100;
    }

    // Fallback: calculate from OS time metrics
    const userTime = cpu.get('OSUserTime') ?? 0;
    const systemTime = cpu.get('OSSystemTime') ?? 0;
    const idleTime = cpu.get('OSIdleTime') ?? 0;
    const totalTime = userTime + systemTime + idleTime;

    if (totalTime > 0) {
      const usage = Math.min(100, ((userTime + systemTime) / totalTime) * 100);
      return Math.round(usage * 100) / 100;
    }

    return 0;
  }
}
