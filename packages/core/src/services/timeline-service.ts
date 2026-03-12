/**
 * TimelineService - Fetches server metrics and activity data for time travel visualization.
 */
import type { IClickHouseAdapter } from '../adapters/types.js';
import type {
  MemoryTimeline,
  TimeseriesPoint,
  QuerySeries,
  MergeSeries,
  MutationSeries,
  TimelineOptions,
  CpuSpike,
  CpuSpikeAnalysis,
} from '../types/timeline.js';
import { buildQuery, tagQuery } from '../queries/builder.js';
import { classifyMergeHistory, classifyActiveMerge } from '../utils/merge-classification.js';
import { TAB_TIME_TRAVEL, sourceTag } from '../queries/source-tags.js';
import {
  SERVER_MEMORY_TIMESERIES,
  SERVER_CPU_TIMESERIES,
  SERVER_NETWORK_TIMESERIES,
  SERVER_DISK_IO_TIMESERIES,
  SERVER_TOTAL_RAM,
  SERVER_CPU_CORES,
  SERVER_CPU_CORES_FALLBACK,
  SERVER_CPU_CORES_FALLBACK2,
  SERVER_CGROUP_CPU,
  SERVER_MAX_THREADS,
  ACTIVE_QUERIES,
  ACTIVE_QUERIES_COUNT,
  ACTIVE_MERGES_COUNT,
  ACTIVE_MERGES_DETAIL,
  ACTIVE_MERGES_PROFILE,
  ACTIVE_MUTATIONS_COUNT,
  ACTIVE_MUTATIONS_DETAIL,
  ACTIVE_MUTATIONS_PROFILE,
  RUNNING_QUERIES_TIMELINE,
  RUNNING_MERGES_TIMELINE,
  CPU_SPIKE_TIMESERIES,
  CLUSTER_CPU_TIMESERIES,
} from '../queries/timeline-queries.js';

export class TimelineServiceError extends Error {
  constructor(message: string, public readonly cause?: Error) {
    super(message);
    this.name = 'TimelineServiceError';
  }
}

/**
 * Convert Date to ClickHouse DateTime format string.
 */
function toClickHouseDateTime(date: Date): string {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, '0');
  const day = String(date.getUTCDate()).padStart(2, '0');
  const hours = String(date.getUTCHours()).padStart(2, '0');
  const minutes = String(date.getUTCMinutes()).padStart(2, '0');
  const seconds = String(date.getUTCSeconds()).padStart(2, '0');
  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
}

/**
 * Parse ClickHouse timestamp string to Date.
 */
function parseChTime(s: string): Date {
  const normalized = s.trim().replace(' ', 'T');
  const withTz = normalized.includes('Z') || normalized.includes('+') ? normalized : normalized + 'Z';
  return new Date(withTz);
}

export class TimelineService {
  private _cachedRam: { ram: number; hostCount: number } | null = null;
  private _cachedCpuCores: number | null = null;

  constructor(private adapter: IClickHouseAdapter) {}

  async getTimeline(options: TimelineOptions): Promise<MemoryTimeline> {
    const { timestamp, windowSeconds, includeRunning = true, hostname = null, activityLimit = 100, activeMetric = 'memory' } = options;

    const start = new Date(timestamp.getTime() - windowSeconds * 1000);
    const end = new Date(timestamp.getTime() + windowSeconds * 1000);
    const startDate = start.toISOString().split('T')[0];

    // ORDER BY expressions per metric, per query type.
    // Queries use query_log columns; merges/mutations use part_log columns.
    const QUERY_ORDER: Record<string, string> = {
      memory: 'memory_usage',
      cpu: "ProfileEvents['OSCPUVirtualTimeMicroseconds']",
      network: "ProfileEvents['NetworkSendBytes'] + ProfileEvents['NetworkReceiveBytes']",
      disk: 'read_bytes + written_bytes',
    };
    const MERGE_ORDER: Record<string, string> = {
      memory: 'peak_memory_usage',
      cpu: "ProfileEvents['OSCPUVirtualTimeMicroseconds']",
      network: "ProfileEvents['NetworkSendBytes'] + ProfileEvents['NetworkReceiveBytes']",
      disk: "ProfileEvents['OSReadBytes'] + ProfileEvents['OSWriteBytes']",
    };
    const queryOrderBy = QUERY_ORDER[activeMetric] || 'memory_usage';
    const mergeOrderBy = MERGE_ORDER[activeMetric] || 'peak_memory_usage';

    // Client-side sort to match the SQL ORDER BY metric
    type HasMetrics = { peak_memory: number; cpu_us: number; net_send: number; net_recv: number; disk_read: number; disk_write: number };
    const sortByMetric = (a: HasMetrics, b: HasMetrics): number => {
      switch (activeMetric) {
        case 'cpu': return b.cpu_us - a.cpu_us;
        case 'network': return (b.net_send + b.net_recv) - (a.net_send + a.net_recv);
        case 'disk': return (b.disk_read + b.disk_write) - (a.disk_read + a.disk_write);
        default: return b.peak_memory - a.peak_memory;
      }
    };

    const params: Record<string, string | number> = {
      start_time: toClickHouseDateTime(start),
      end_time: toClickHouseDateTime(end),
      start_date: startDate,
      activity_limit: activityLimit,
    };

    // Replace ORDER BY placeholders with raw SQL expressions.
    // Applied after buildQuery() since these are column expressions, not quoted values.
    // Values are hardcoded above (not user input), so no injection risk.
    const applyOrder = (sql: string): string =>
      sql.replaceAll('{query_order_by}', queryOrderBy)
         .replaceAll('{merge_order_by}', mergeOrderBy);

    // Helper: inject hostname filter into a built SQL string.
    // For nested subqueries (e.g. CPU timeseries), we need to insert the filter
    // inside the innermost WHERE clause, not at the outer query level.
    const withHost = (sql: string): string => {
      if (!hostname) return sql;
      // Sanitize hostname — only allow safe characters
      const safe = hostname.replace(/[^a-zA-Z0-9._\-]/g, '');
      if (!safe) return sql;
      const filter = `AND hostname() = '${safe}'`;
      
      // Find the deepest-nested WHERE clause by tracking parenthesis depth.
      // Insert the filter after the conditions of that WHERE clause.
      let maxDepth = 0;
      let deepestWhereIdx = -1;
      let depth = 0;
      const whereRe = /\bWHERE\b/gi;
      let match;
      
      // Track paren depth at each position
      const depthAt: number[] = new Array(sql.length);
      for (let i = 0; i < sql.length; i++) {
        if (sql[i] === '(') depth++;
        depthAt[i] = depth;
        if (sql[i] === ')') depth--;
      }
      
      // Find the WHERE at the greatest depth
      while ((match = whereRe.exec(sql)) !== null) {
        const d = depthAt[match.index];
        if (d >= maxDepth) {
          maxDepth = d;
          deepestWhereIdx = match.index;
        }
      }
      
      if (deepestWhereIdx >= 0 && maxDepth > 0) {
        // There's a WHERE inside a subquery. Insert the filter after the WHERE
        // conditions but before GROUP BY / ORDER BY / closing paren at the same depth.
        let insertPos = sql.length;

        // First, look for GROUP BY or ORDER BY at the same depth (comes before closing paren)
        const clauseRe = /\b(GROUP\s+BY|ORDER\s+BY)\b/gi;
        let clauseMatch;
        while ((clauseMatch = clauseRe.exec(sql)) !== null) {
          if (clauseMatch.index > deepestWhereIdx && depthAt[clauseMatch.index] === maxDepth) {
            insertPos = clauseMatch.index;
            break;
          }
        }

        // Fall back to closing paren at the same depth
        if (insertPos === sql.length) {
          for (let i = deepestWhereIdx; i < sql.length; i++) {
            if (sql[i] === ')' && depthAt[i] === maxDepth) {
              insertPos = i;
              break;
            }
          }
        }
        return sql.slice(0, insertPos) + `\n    ${filter}\n  ` + sql.slice(insertPos);
      }
      
      // No nested WHERE — insert before GROUP BY or ORDER BY (whichever comes first)
      const groupIdx = sql.search(/\bGROUP\s+BY\b/i);
      const orderIdx = sql.search(/\bORDER\s+BY\b/i);
      const candidates = [groupIdx, orderIdx].filter(i => i > 0);
      if (candidates.length > 0) {
        const insertIdx = Math.min(...candidates);
        return sql.slice(0, insertIdx) + `${filter}\n  ` + sql.slice(insertIdx);
      }
      return sql + `\n  ${filter}`;
    };

    // Check if the time window includes "now" (within 30 seconds of current time)
    // Only fetch in-flight data if includeRunning is enabled
    const now = new Date();
    const includesNow = includeRunning && end.getTime() >= now.getTime() - 30000;

    // Fetch all data in parallel where possible
    const [
      serverMemory,
      serverCpu,
      networkData,
      diskData,
      ramResult,
      cpuCores,
      perHostCpu,
      queries,
      queryCount,
      mergeStats,
      merges,
      mutationCount,
      mutations,
      runningQueries,
      runningMergesAndMutations,
    ] = await Promise.all([
      activeMetric === 'memory' ? this.fetchServerMemory(params, withHost) : Promise.resolve([]),
      activeMetric === 'cpu' ? this.fetchServerCpu(params, withHost) : Promise.resolve([]),
      activeMetric === 'network' ? this.fetchNetworkData(params, withHost) : Promise.resolve({ send: [], recv: [] }),
      activeMetric === 'disk' ? this.fetchDiskData(params, withHost) : Promise.resolve({ read: [], write: [] }),
      this._cachedRam ? Promise.resolve(this._cachedRam) : this.fetchTotalRam(params, withHost),
      this._cachedCpuCores !== null ? Promise.resolve(this._cachedCpuCores) : this.fetchCpuCores(params, withHost),
      // Per-host CPU breakdown for cluster tooltip (only in "All" mode)
      !hostname && activeMetric === 'cpu' ? this.fetchPerHostCpu(params) : Promise.resolve({}),
      this.fetchQueries(params, start, end, (sql) => applyOrder(withHost(sql))),
      this.fetchQueryCount(params, withHost),
      this.fetchMergeStats(params, withHost),
      this.fetchMerges(params, start, end, (sql) => applyOrder(withHost(sql))),
      this.fetchMutationCount(params, withHost),
      this.fetchMutations(params, start, end, (sql) => applyOrder(withHost(sql))),
      includesNow ? this.fetchRunningQueries(start, end, activityLimit, queryOrderBy, withHost) : Promise.resolve([]),
      includesNow ? this.fetchRunningMergesAndMutations(start, end, activityLimit, withHost) : Promise.resolve({ merges: [], mutations: [] }),
    ]);

    // Merge completed and running queries (dedupe by query_id)
    const completedQueryIds = new Set(queries.map(q => q.query_id));
    const allQueries = [
      ...queries,
      ...runningQueries.filter(q => !completedQueryIds.has(q.query_id)),
    ];
    allQueries.sort(sortByMetric);

    // Merge completed and running merges (dedupe by part_name)
    const completedMergeNames = new Set(merges.map(m => m.part_name));
    const allMerges = [
      ...merges,
      ...runningMergesAndMutations.merges.filter(m => !completedMergeNames.has(m.part_name)),
    ];
    allMerges.sort(sortByMetric);

    // Merge completed and running mutations (dedupe by part_name)
    const completedMutationNames = new Set(mutations.map(m => m.part_name));
    const allMutations = [
      ...mutations,
      ...runningMergesAndMutations.mutations.filter(m => !completedMutationNames.has(m.part_name)),
    ];
    allMutations.sort(sortByMetric);

    // Update counts to include running operations
    const totalQueryCount = queryCount + runningQueries.length;
    const totalMergeCount = mergeStats.count + runningMergesAndMutations.merges.length;
    const totalMergePeak = mergeStats.peakTotal + 
      runningMergesAndMutations.merges.reduce((sum, m) => sum + m.peak_memory, 0);
    const totalMutationCount = mutationCount + runningMergesAndMutations.mutations.length;

    // Cache static values after first fetch
    if (!this._cachedRam) this._cachedRam = ramResult;
    if (this._cachedCpuCores === null) this._cachedCpuCores = cpuCores;

    const totalRam = ramResult.ram;
    const hostCount = ramResult.hostCount;

    return {
      window_start: start.toISOString(),
      window_end: end.toISOString(),
      target: timestamp.toISOString(),
      server_memory: serverMemory,
      // Clamp CPU values: under heavy load, metric_log collection can be delayed,
      // causing accumulated CPU µs to exceed the reported wall-clock interval.
      // Max µs/s = cpuCores × 1,000,000. See docs/metrics/cpu.md for details.
      server_cpu: cpuCores > 0
        ? serverCpu.map(p => ({
            t: p.t,
            v: Math.min(p.v, cpuCores * 1_000_000),
          }))
        : serverCpu,
      server_network_send: networkData.send,
      server_network_recv: networkData.recv,
      server_disk_read: diskData.read,
      server_disk_write: diskData.write,
      server_total_ram: totalRam,
      cpu_cores: cpuCores,
      host_count: hostCount,
      per_host_cpu: Object.keys(perHostCpu).length > 1 ? perHostCpu : undefined,
      queries: allQueries,
      merges: allMerges,
      mutations: allMutations,
      query_count: totalQueryCount,
      merge_count: totalMergeCount,
      merge_peak_total: totalMergePeak,
      mutation_count: totalMutationCount,
    };
  }

  private async fetchServerMemory(params: Record<string, string | number>, xform: (s: string) => string): Promise<TimeseriesPoint[]> {
    try {
      const sql = xform(buildQuery(SERVER_MEMORY_TIMESERIES, params));
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_TIME_TRAVEL, 'serverMemory')));
      return rows.map(r => ({
        t: String((r as Record<string, unknown>).t || ''),
        v: Number((r as Record<string, unknown>).v || 0),
      }));
    } catch (e) {
      console.error('[TimelineService] metric_log error:', e);
      return [];
    }
  }

  private async fetchServerCpu(params: Record<string, string | number>, xform: (s: string) => string): Promise<TimeseriesPoint[]> {
    try {
      const sql = xform(buildQuery(SERVER_CPU_TIMESERIES, params));
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_TIME_TRAVEL, 'serverCpu')));
      
      return rows.map(r => {
        const row = r as Record<string, unknown>;
        const v = Number(row.v || 0);
        const intervalMs = Number(row.interval_ms || 1000);
        // Normalize to per-second rate if interval is not 1 second
        // Guard: if interval is suspiciously small (< 500ms), use 1000ms to avoid
        // amplifying values from metric_log collection jitter under heavy load.
        const safeIntervalMs = intervalMs >= 500 ? intervalMs : 1000;
        const normalizedV = (v / safeIntervalMs) * 1000;
        return {
          t: String(row.t || ''),
          v: normalizedV,
        };
      });
    } catch (e) {
      console.error('[TimelineService] cpu metric_log error:', e);
      return [];
    }
  }

  /** Fetch per-host CPU timeseries for cluster tooltip breakdown */
  private async fetchPerHostCpu(params: Record<string, string | number>): Promise<Record<string, TimeseriesPoint[]>> {
    try {
      const sql = buildQuery(CLUSTER_CPU_TIMESERIES, params);
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_TIME_TRAVEL, 'perHostCpu')));
      const byHost: Record<string, TimeseriesPoint[]> = {};
      for (const r of rows) {
        const row = r as Record<string, unknown>;
        const host = String(row.host || '');
        const v = Number(row.v || 0);
        const intervalMs = Number(row.interval_ms || 1000);
        const safeIntervalMs = intervalMs >= 500 ? intervalMs : 1000;
        const normalizedV = (v / safeIntervalMs) * 1000;
        if (!byHost[host]) byHost[host] = [];
        byHost[host].push({ t: String(row.t || ''), v: normalizedV });
      }
      return byHost;
    } catch (e) {
      console.error('[TimelineService] per-host cpu error:', e);
      return {};
    }
  }


  private async fetchNetworkData(params: Record<string, string | number>, xform: (s: string) => string): Promise<{ send: TimeseriesPoint[]; recv: TimeseriesPoint[] }> {
    try {
      const sql = xform(buildQuery(SERVER_NETWORK_TIMESERIES, params));
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_TIME_TRAVEL, 'networkData')));
      const send: TimeseriesPoint[] = [];
      const recv: TimeseriesPoint[] = [];
      for (const r of rows) {
        const row = r as Record<string, unknown>;
        const t = String(row.t || '');
        send.push({ t, v: Number(row.send_v || 0) });
        recv.push({ t, v: Number(row.recv_v || 0) });
      }
      return { send, recv };
    } catch (e) {
      console.error('[TimelineService] network metric_log error:', e);
      return { send: [], recv: [] };
    }
  }

  private async fetchDiskData(params: Record<string, string | number>, xform: (s: string) => string): Promise<{ read: TimeseriesPoint[]; write: TimeseriesPoint[] }> {
    try {
      const sql = xform(buildQuery(SERVER_DISK_IO_TIMESERIES, params));
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_TIME_TRAVEL, 'diskData')));
      const read: TimeseriesPoint[] = [];
      const write: TimeseriesPoint[] = [];
      for (const r of rows) {
        const row = r as Record<string, unknown>;
        const t = String(row.t || '');
        read.push({ t, v: Number(row.read_v || 0) });
        write.push({ t, v: Number(row.write_v || 0) });
      }
      return { read, write };
    } catch (e) {
      console.error('[TimelineService] disk io metric_log error:', e);
      return { read: [], write: [] };
    }
  }

  private async fetchTotalRam(params: Record<string, string | number>, xform: (s: string) => string): Promise<{ ram: number; hostCount: number }> {
      try {
        const sql = xform(buildQuery(SERVER_TOTAL_RAM, params));
        const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_TIME_TRAVEL, 'totalRam')));
        if (rows.length === 0) return { ram: 0, hostCount: 1 };
        // Query returns per-host rows. For option 4 (per-host attribution),
        // return per-host RAM (use min across hosts) and the host count.
        const values = rows.map(r => Number((r as Record<string, unknown>).value || 0)).filter(v => v > 0);
        const hostCount = values.length || 1;
        const perHostRam = values.length > 0 ? Math.min(...values) : 0;

        // In containers, OSMemoryTotal reports host RAM — use cgroup limit if available
        const cgroupMem = await this.fetchCgroupMemoryLimit();
        if (cgroupMem > 0 && cgroupMem < perHostRam) return { ram: cgroupMem, hostCount };

        return { ram: perHostRam, hostCount };
      } catch (e) {
        console.error('[TimelineService] total_ram error:', e);
      }
      return { ram: 0, hostCount: 1 };
    }

  /**
   * Fetch cgroup memory limit from CGroupMemoryTotal (CH 26+) or CGroupMemoryLimit (CH 23.8–25.x).
   * Returns 0 if no cgroup limit is detected.
   */
  private async fetchCgroupMemoryLimit(): Promise<number> {
    try {
      const rows = await this.adapter.executeQuery(
        tagQuery(
          `SELECT value FROM system.asynchronous_metrics WHERE metric IN ('CGroupMemoryTotal', 'CGroupMemoryLimit') LIMIT 1`,
          sourceTag(TAB_TIME_TRAVEL, 'cgroupMem')
        )
      );
      if (rows.length > 0) {
        const val = Number((rows[0] as Record<string, unknown>).value || 0);
        // CGroupMemoryLimit/Total returns a very large number when no limit is set
        if (val > 0 && val < 1e18) return val;
      }
    } catch (err) {
      console.warn('[TimelineService] fetchMemoryLimit metric not available:', err);
    }
    return 0;
  }

  private async fetchCpuCores(params: Record<string, string | number>, xform: (s: string) => string): Promise<number> {
      // 1. Try asynchronous_metric_log first (returns per-host rows, cluster-aware)
      try {
        const sql = xform(buildQuery(SERVER_CPU_CORES, params));
        const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_TIME_TRAVEL, 'cpuCores')));
        if (rows.length > 0) {
          const values = rows.map(r => Number((r as Record<string, unknown>).value || 0)).filter(v => v > 0);
          if (values.length > 0) {
            // For option 4 (per-host attribution), return per-host cores (min across hosts).
            // In containers, NumberOfCPUCores reports host cores — cap at cgroup limit.
            const cgroupCores = await this.fetchCgroupCpuLimit();
            const perHost = Math.min(...values);
            return cgroupCores > 0 ? Math.min(perHost, cgroupCores) : perHost;
          }
        }
      } catch (e) {
        console.error('[TimelineService] cpu_cores from log error:', e);
      }

      // 2. Fallback: cgroup-aware detection (single-node containerized environments)
      const cgroupCores = await this.fetchCgroupCpuLimit();
      if (cgroupCores > 0) return cgroupCores;

      // 3. Fallback to asynchronous_metrics (local node only)
      try {
        const rows = await this.adapter.executeQuery(tagQuery(SERVER_CPU_CORES_FALLBACK, sourceTag(TAB_TIME_TRAVEL, 'cpuCores')));
        if (rows.length > 0) {
          const val = Number((rows[0] as Record<string, unknown>).value || 0);
          if (val > 0) return val;
        }
      } catch (e) {
        console.error('[TimelineService] cpu_cores from metrics error:', e);
      }

      // 4. Fallback to counting OSUserTimeCPU metrics
      try {
        const rows = await this.adapter.executeQuery(tagQuery(SERVER_CPU_CORES_FALLBACK2, sourceTag(TAB_TIME_TRAVEL, 'cpuCores')));
        if (rows.length > 0) {
          return Number(Object.values(rows[0] as Record<string, unknown>)[0] || 0);
        }
      } catch (e) {
        console.error('[TimelineService] cpu_cores from OSUserTimeCPU count error:', e);
      }

      return 0;
    }

  /**
   * Detect cgroup CPU limit for containerized environments (Kubernetes).
   * Returns 0 if no cgroup limit is detected.
   */
  private async fetchCgroupCpuLimit(): Promise<number> {
    // Try CGroupMaxCPU async metric (ClickHouse >= 23.8)
    try {
      const rows = await this.adapter.executeQuery(
        tagQuery(SERVER_CGROUP_CPU, sourceTag(TAB_TIME_TRAVEL, 'cgroupCpu'))
      );
      if (rows.length > 0) {
        const val = Number((rows[0] as Record<string, unknown>).value || 0);
        if (val > 0) return Math.round(val);
      }
    } catch (err) {
      console.warn('[TimelineService] fetchCpuCores cgroup metric not available:', err);
    }

    // Fallback: max_threads from system.settings
    try {
      const rows = await this.adapter.executeQuery(
        tagQuery(SERVER_MAX_THREADS, sourceTag(TAB_TIME_TRAVEL, 'maxThreads'))
      );
      if (rows.length > 0) {
        const val = parseInt(String((rows[0] as Record<string, unknown>).value || '0'), 10);
        // max_threads defaults to the detected CPU count; only use it if it seems
        // like a cgroup limit (i.e., it's a reasonable number, not 0 or absurdly high)
        if (val > 0 && val <= 256) return val;
      }
    } catch (err) {
      console.warn('[TimelineService] fetchCpuCores system.settings not accessible:', err);
    }

    return 0;
  }

  private async fetchQueries(params: Record<string, string | number>, start: Date, end: Date, xform: (s: string) => string): Promise<QuerySeries[]> {
    try {
      const sql = xform(buildQuery(ACTIVE_QUERIES, params));
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_TIME_TRAVEL, 'queries')));
      
      const queries: QuerySeries[] = [];
      for (const r of rows) {
        const row = r as Record<string, unknown>;
        const qst = parseChTime(String(row.qst || ''));
        const qet = parseChTime(String(row.qet || ''));
        const peak = Number(row.memory_usage || 0);
        
        // Clamp to window
        const visStart = new Date(Math.max(qst.getTime(), start.getTime()));
        const visEnd = new Date(Math.min(qet.getTime(), end.getTime()));
        if (visStart >= visEnd) continue;

        const points: TimeseriesPoint[] = [
          { t: toClickHouseDateTime(visStart), v: peak },
          { t: toClickHouseDateTime(visEnd), v: peak },
        ];

        queries.push({
          query_id: String(row.query_id || ''),
          label: String(row.query_short || ''),
          user: String(row.user || ''),
          hostname: row.host ? String(row.host) : undefined,
          peak_memory: peak,
          duration_ms: Number(row.query_duration_ms || 0),
          cpu_us: Number(row.cpu_us || 0),
          net_send: Number(row.net_send || 0),
          net_recv: Number(row.net_recv || 0),
          disk_read: Number(row.disk_read || 0),
          disk_write: Number(row.disk_write || 0),
          start_time: qst.toISOString(),
          end_time: qet.toISOString(),
          status: String(row.status || ''),
          query_kind: row.query_kind ? String(row.query_kind) : undefined,
          exception_code: Number(row.exception_code || 0),
          exception: row.exception ? String(row.exception) : undefined,
          points,
        });
      }
      
      return queries;
    } catch (e) {
      console.error('[TimelineService] query_log error:', e);
      return [];
    }
  }

  private async fetchMergeStats(params: Record<string, string | number>, xform: (s: string) => string): Promise<{ count: number; peakTotal: number }> {
    try {
      const sql = xform(buildQuery(ACTIVE_MERGES_COUNT, params));
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_TIME_TRAVEL, 'mergeStats')));
      if (rows.length > 0) {
        const row = rows[0] as Record<string, unknown>;
        const values = Object.values(row);
        return {
          count: Number(values[0] || 0),
          peakTotal: Number(values[1] || 0),
        };
      }
    } catch (e) {
      console.error('[TimelineService] merge count error:', e);
    }
    return { count: 0, peakTotal: 0 };
  }
  private async fetchQueryCount(params: Record<string, string | number>, xform: (s: string) => string): Promise<number> {
    try {
      const sql = xform(buildQuery(ACTIVE_QUERIES_COUNT, params));
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_TIME_TRAVEL, 'queryCount')));
      if (rows.length > 0) {
        const row = rows[0] as Record<string, unknown>;
        return Number(Object.values(row)[0] || 0);
      }
    } catch (e) {
      console.error('[TimelineService] query count error:', e);
    }
    return 0;
  }

  private async fetchMerges(params: Record<string, string | number>, start: Date, end: Date, xform: (s: string) => string): Promise<MergeSeries[]> {
    const merges: MergeSeries[] = [];
    
    try {
      const sql = xform(buildQuery(ACTIVE_MERGES_DETAIL, params));
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_TIME_TRAVEL, 'merges')));
      
      for (const r of rows) {
        const row = r as Record<string, unknown>;
        const msStart = parseChTime(String(row.merge_start || ''));
        const msEnd = parseChTime(String(row.merge_end || ''));
        
        const visStart = new Date(Math.max(msStart.getTime(), start.getTime()));
        const visEnd = new Date(Math.min(msEnd.getTime(), end.getTime()));
        if (visStart >= visEnd) continue;

        merges.push({
          part_name: String(row.part_name || ''),
          table: String(row.tbl || ''),
          hostname: row.host ? String(row.host) : undefined,
          peak_memory: Number(row.peak_memory_usage || 0),
          duration_ms: Number(row.duration_ms || 0),
          cpu_us: 0,
          net_send: 0,
          net_recv: 0,
          disk_read: 0,
          disk_write: 0,
          start_time: visStart.toISOString(),
          end_time: visEnd.toISOString(),
          merge_reason: classifyMergeHistory(String(row.event_type || 'MergeParts'), String(row.merge_reason || '')),
        });
      }
    } catch (e) {
      console.error('[TimelineService] merge detail error:', e);
    }

    // Enrich with ProfileEvents (best-effort)
    if (merges.length > 0) {
      try {
        const sql = xform(buildQuery(ACTIVE_MERGES_PROFILE, params));
        const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_TIME_TRAVEL, 'mergeProfile')));
        const profileMap = new Map<string, Record<string, number>>();
        
        for (const r of rows) {
          const row = r as Record<string, unknown>;
          profileMap.set(String(row.part_name || ''), {
            cpu_us: Number(row.cpu_us || 0),
            net_send: Number(row.net_send || 0),
            net_recv: Number(row.net_recv || 0),
            disk_read: Number(row.disk_read || 0),
            disk_write: Number(row.disk_write || 0),
          });
        }

        for (const m of merges) {
          const pe = profileMap.get(m.part_name);
          if (pe) {
            m.cpu_us = pe.cpu_us;
            m.net_send = pe.net_send;
            m.net_recv = pe.net_recv;
            m.disk_read = pe.disk_read;
            m.disk_write = pe.disk_write;
          }
        }
      } catch (e) {
        console.error('[TimelineService] merge profile events error (non-fatal):', e);
      }
    }

    return merges;
  }

  private async fetchMutationCount(params: Record<string, string | number>, xform: (s: string) => string): Promise<number> {
    try {
      const sql = xform(buildQuery(ACTIVE_MUTATIONS_COUNT, params));
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_TIME_TRAVEL, 'mutationCount')));
      if (rows.length > 0) {
        return Number(Object.values(rows[0] as Record<string, unknown>)[0] || 0);
      }
    } catch (e) {
      console.error('[TimelineService] mutation count error:', e);
    }
    return 0;
  }

  private async fetchMutations(params: Record<string, string | number>, start: Date, end: Date, xform: (s: string) => string): Promise<MutationSeries[]> {
    const mutations: MutationSeries[] = [];
    
    try {
      const sql = xform(buildQuery(ACTIVE_MUTATIONS_DETAIL, params));
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_TIME_TRAVEL, 'mutations')));
      
      for (const r of rows) {
        const row = r as Record<string, unknown>;
        const msStart = parseChTime(String(row.mut_start || ''));
        const msEnd = parseChTime(String(row.mut_end || ''));
        
        const visStart = new Date(Math.max(msStart.getTime(), start.getTime()));
        const visEnd = new Date(Math.min(msEnd.getTime(), end.getTime()));
        if (visStart >= visEnd) continue;

        mutations.push({
          part_name: String(row.part_name || ''),
          table: String(row.tbl || ''),
          hostname: row.host ? String(row.host) : undefined,
          peak_memory: Number(row.peak_memory_usage || 0),
          duration_ms: Number(row.duration_ms || 0),
          cpu_us: 0,
          net_send: 0,
          net_recv: 0,
          disk_read: 0,
          disk_write: 0,
          start_time: visStart.toISOString(),
          end_time: visEnd.toISOString(),
        });
      }
    } catch (e) {
      console.error('[TimelineService] mutation detail error:', e);
    }

    // Enrich with ProfileEvents (best-effort)
    if (mutations.length > 0) {
      try {
        const sql = xform(buildQuery(ACTIVE_MUTATIONS_PROFILE, params));
        const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_TIME_TRAVEL, 'mutationProfile')));
        const profileMap = new Map<string, Record<string, number>>();
        
        for (const r of rows) {
          const row = r as Record<string, unknown>;
          profileMap.set(String(row.part_name || ''), {
            cpu_us: Number(row.cpu_us || 0),
            net_send: Number(row.net_send || 0),
            net_recv: Number(row.net_recv || 0),
            disk_read: Number(row.disk_read || 0),
            disk_write: Number(row.disk_write || 0),
          });
        }

        for (const m of mutations) {
          const pe = profileMap.get(m.part_name);
          if (pe) {
            m.cpu_us = pe.cpu_us;
            m.net_send = pe.net_send;
            m.net_recv = pe.net_recv;
            m.disk_read = pe.disk_read;
            m.disk_write = pe.disk_write;
          }
        }
      } catch (e) {
        console.error('[TimelineService] mutation profile events error (non-fatal):', e);
      }
    }

    return mutations;
  }

  /**
   * Fetch currently running queries from system.processes
   */
  private async fetchRunningQueries(start: Date, end: Date, activityLimit: number = 100, queryOrderBy: string = 'memory_usage', xform: (s: string) => string = s => s): Promise<QuerySeries[]> {
    try {
      const sql = xform(RUNNING_QUERIES_TIMELINE
        .replaceAll('{activity_limit}', String(activityLimit))
        .replaceAll('{query_order_by}', queryOrderBy));
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_TIME_TRAVEL, 'runningQueries')));
      const now = new Date();
      const queries: QuerySeries[] = [];

      for (const r of rows) {
        const row = r as Record<string, unknown>;
        const qst = parseChTime(String(row.qst || ''));
        const peak = Number(row.memory_usage || 0);
        const durationMs = Number(row.query_duration_ms || 0);

        // Running queries have no end time yet - use "now" as the visual end
        const visStart = new Date(Math.max(qst.getTime(), start.getTime()));
        const visEnd = new Date(Math.min(now.getTime(), end.getTime()));
        if (visStart >= visEnd) continue;

        const points: TimeseriesPoint[] = [
          { t: toClickHouseDateTime(visStart), v: peak },
          { t: toClickHouseDateTime(visEnd), v: peak },
        ];

        queries.push({
          query_id: String(row.query_id || ''),
          label: String(row.query_short || ''),
          user: String(row.user || ''),
          hostname: row.host ? String(row.host) : undefined,
          peak_memory: peak,
          duration_ms: durationMs,
          cpu_us: Number(row.cpu_us || 0),
          net_send: Number(row.net_send || 0),
          net_recv: Number(row.net_recv || 0),
          disk_read: Number(row.disk_read || 0),
          disk_write: Number(row.disk_write || 0),
          start_time: qst.toISOString(),
          end_time: now.toISOString(),  // Running - use current time
          status: 'Running',
          query_kind: row.query_kind ? String(row.query_kind) : undefined,
          is_running: true,
          points,
        });
      }

      return queries;
    } catch (e) {
      console.error('[TimelineService] running queries error:', e);
      return [];
    }
  }

  /**
   * Fetch currently running merges and mutations from system.merges
   */
  private async fetchRunningMergesAndMutations(
    start: Date,
    end: Date,
    activityLimit: number = 100,
    xform: (s: string) => string = s => s
  ): Promise<{ merges: MergeSeries[]; mutations: MutationSeries[] }> {
    try {
      const sql = xform(RUNNING_MERGES_TIMELINE.replaceAll('{activity_limit}', String(activityLimit)));
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_TIME_TRAVEL, 'runningMerges')));
      const now = new Date();
      const merges: MergeSeries[] = [];
      const mutations: MutationSeries[] = [];

      for (const r of rows) {
        const row = r as Record<string, unknown>;
        const msStart = parseChTime(String(row.merge_start || ''));
        const peak = Number(row.peak_memory_usage || 0);
        const durationMs = Number(row.duration_ms || 0);
        const progress = Number(row.progress || 0);
        const isMutation = Boolean(row.is_mutation);

        // Running operations have no end time yet - use "now" as the visual end
        const visStart = new Date(Math.max(msStart.getTime(), start.getTime()));
        const visEnd = new Date(Math.min(now.getTime(), end.getTime()));
        if (visStart >= visEnd) continue;

        const item = {
          part_name: String(row.part_name || ''),
          table: String(row.tbl || ''),
          hostname: row.host ? String(row.host) : undefined,
          peak_memory: peak,
          duration_ms: durationMs,
          cpu_us: Number(row.cpu_us || 0),  // Estimated for in-flight merges, see RUNNING_MERGE_CPU_CORES
          net_send: 0,
          net_recv: 0,
          disk_read: Number(row.disk_read || 0),
          disk_write: Number(row.disk_write || 0),
          start_time: msStart.toISOString(),
          end_time: now.toISOString(),  // Running - use current time
          merge_reason: classifyActiveMerge(String(row.merge_type || 'Regular'), isMutation),
          is_running: true,
          progress,
        };

        if (isMutation) {
          mutations.push(item);
        } else {
          merges.push(item);
        }
      }

      return { merges, mutations };
    } catch (e) {
      console.error('[TimelineService] running merges error:', e);
      return { merges: [], mutations: [] };
    }
  }


    /**
     * Analyze CPU spikes in a time window.
     * Finds contiguous periods where CPU exceeds 100% of all cores and classifies
     * them as transient (isolated blips) or sustained (≥ sustainedThresholdSec).
     *
     * @param fromTime - Start of the analysis window
     * @param toTime - End of the analysis window
     * @param sustainedThresholdSec - Minimum duration (seconds) to classify as "sustained". Default: 120 (2 minutes)
     */
    async getCpuSpikeAnalysis(
      fromTime: Date,
      toTime: Date,
      sustainedThresholdSec: number = 120
    ): Promise<CpuSpikeAnalysis> {
      const params = {
        start_time: toClickHouseDateTime(fromTime),
        end_time: toClickHouseDateTime(toTime),
      };

      const identity = (s: string) => s;

      // Fetch CPU timeseries and core count in parallel
      const [rawRows, cpuCores] = await Promise.all([
        this.fetchSpikeTimeseries(params),
        this.fetchCpuCores(params, identity),
      ]);

      const cores = cpuCores > 0 ? cpuCores : 1;
      // 100% = cores × 1_000_000 µs/s
      const threshold100Pct = cores * 1_000_000;

      // Convert raw rows to { timestamp, cpuPct }
      const points = rawRows.map(r => {
        const intervalSec = r.interval_ms > 0 ? r.interval_ms / 1000 : 1;
        const cpuUsPerSec = r.cpu_us / intervalSec;
        const cpuPct = (cpuUsPerSec / threshold100Pct) * 100;
        return {
          t: r.t,
          ts: parseChTime(r.t),
          cpuPct,
        };
      });

      const totalDataPoints = points.length;
      const pointsAbove100 = points.filter(p => p.cpuPct > 100).length;

      // Group contiguous above-100% points into spikes
      const spikes: CpuSpike[] = [];
      let spikeStart: number | null = null;
      let spikePeakPct = 0;
      let spikeSumPct = 0;
      let spikeCount = 0;

      for (let i = 0; i < points.length; i++) {
        const p = points[i];
        if (p.cpuPct > 100) {
          if (spikeStart === null) {
            spikeStart = i;
            spikePeakPct = 0;
            spikeSumPct = 0;
            spikeCount = 0;
          }
          spikePeakPct = Math.max(spikePeakPct, p.cpuPct);
          spikeSumPct += p.cpuPct;
          spikeCount++;
        } else if (spikeStart !== null) {
          // Spike ended — emit it
          const startTs = points[spikeStart].ts;
          const endTs = points[i - 1].ts;
          const durationSec = Math.max(1, (endTs.getTime() - startTs.getTime()) / 1000);
          spikes.push({
            start_time: startTs.toISOString(),
            end_time: endTs.toISOString(),
            duration_seconds: Math.round(durationSec),
            peak_cpu_pct: Math.round(spikePeakPct * 10) / 10,
            avg_cpu_pct: Math.round((spikeSumPct / spikeCount) * 10) / 10,
            data_points: spikeCount,
            classification: durationSec >= sustainedThresholdSec ? 'sustained' : 'transient',
          });
          spikeStart = null;
        }
      }

      // Handle spike that extends to the end of the window
      if (spikeStart !== null) {
        const startTs = points[spikeStart].ts;
        const endTs = points[points.length - 1].ts;
        const durationSec = Math.max(1, (endTs.getTime() - startTs.getTime()) / 1000);
        spikes.push({
          start_time: startTs.toISOString(),
          end_time: endTs.toISOString(),
          duration_seconds: Math.round(durationSec),
          peak_cpu_pct: Math.round(spikePeakPct * 10) / 10,
          avg_cpu_pct: Math.round((spikeSumPct / spikeCount) * 10) / 10,
          data_points: spikeCount,
          classification: durationSec >= sustainedThresholdSec ? 'sustained' : 'transient',
        });
      }

      const overallPeak = points.length > 0
        ? Math.round(Math.max(...points.map(p => p.cpuPct)) * 10) / 10
        : 0;

      const pctTimeAbove100 = totalDataPoints > 0
        ? Math.round((pointsAbove100 / totalDataPoints) * 1000) / 10
        : 0;

      return {
        window_start: fromTime.toISOString(),
        window_end: toTime.toISOString(),
        cpu_cores: cores,
        total_data_points: totalDataPoints,
        points_above_100: pointsAbove100,
        pct_time_above_100: pctTimeAbove100,
        spikes,
        transient_count: spikes.filter(s => s.classification === 'transient').length,
        sustained_count: spikes.filter(s => s.classification === 'sustained').length,
        overall_peak_pct: overallPeak,
      };
    }

    private async fetchSpikeTimeseries(
      params: Record<string, string | number>
    ): Promise<Array<{ t: string; cpu_us: number; interval_ms: number }>> {
      try {
        const sql = buildQuery(CPU_SPIKE_TIMESERIES, params);
        const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_TIME_TRAVEL, 'cpuSpikes')));
        return rows.map(r => {
          const row = r as Record<string, unknown>;
          return {
            t: String(row.t || ''),
            cpu_us: Number(row.cpu_us || 0),
            interval_ms: Number(row.interval_ms || 1000),
          };
        });
      } catch (e) {
        console.error('[TimelineService] spike timeseries error:', e);
        return [];
      }
    }

}
