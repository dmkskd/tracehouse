/**
 * TimelineService - Fetches server metrics and activity data for time travel visualization.
 */
import type { IClickHouseAdapter } from '../adapters/types.js';
import type {
  MemoryTimeline,
  TimeseriesPoint,
  TimelineNavigatorMetric,
  TimelineNavigatorOptions,
  QuerySeries,
  MergeSeries,
  MutationSeries,
  TimelineOptions,
  CpuSpike,
  CpuSpikeAnalysis,
  ZoomSample,
} from '../types/timeline.js';
import { buildQuery, tagQuery, utcDateTime } from '../queries/builder.js';
import type { QueryParameter } from '../queries/builder.js';
import { classifyMergeHistory, classifyActiveMerge } from '../utils/merge-classification.js';
import { TAB_TIME_TRAVEL, sourceTag } from '../queries/source-tags.js';
import {
  buildZoomProcessSamplesSQL,
  buildZoomMergeSamplesSQL,
} from '../queries/zoom-queries.js';
import {
  SERVER_MEMORY_TIMESERIES,
  SERVER_CPU_TIMESERIES,
  SERVER_NETWORK_TIMESERIES,
  SERVER_DISK_IO_TIMESERIES,
  NAVIGATOR_MEMORY_TIMESERIES,
  NAVIGATOR_CPU_TIMESERIES,
  NAVIGATOR_NETWORK_TIMESERIES,
  NAVIGATOR_DISK_TIMESERIES,
  SERVER_TOTAL_RAM,
  SERVER_CPU_CAPACITY_HISTORY,
  SERVER_CPU_CAPACITY_CURRENT,
  ACTIVE_QUERIES,
  ACTIVE_QUERIES_BY_HASH,
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
  private _cachedRam = new Map<string, {
    ram: number;
    totalRam: number;
    hostCount: number;
    hosts: string[];
  }>();
  constructor(private adapter: IClickHouseAdapter) {}

  /**
   * Fetch a single downsampled server metric for the buffered navigator.
   * This intentionally avoids the activity, capacity, and running-operation
   * fan-out performed by getTimeline().
   */
  async getNavigatorMetric(options: TimelineNavigatorOptions): Promise<TimelineNavigatorMetric> {
    const startMs = options.startTime.getTime();
    const endMs = options.endTime.getTime();
    if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs <= startMs) {
      throw new TimelineServiceError('Invalid navigator time range');
    }

    const bucketSeconds = Math.max(1, Math.floor(options.bucketSeconds));
    const hostnames = (Array.isArray(options.hostname)
      ? options.hostname
      : options.hostname
        ? [options.hostname]
        : [])
      .map(host => host.replace(/[^a-zA-Z0-9._\-]/g, ''))
      .filter((host, index, hosts) => host.length > 0 && hosts.indexOf(host) === index);
    const hostnameFilter = hostnames.length === 0
      ? ''
      : hostnames.length === 1
        ? `AND hostname() = '${hostnames[0]}'`
        : `AND hostname() IN (${hostnames.map(host => `'${host}'`).join(', ')})`;

    const template = {
      memory: NAVIGATOR_MEMORY_TIMESERIES,
      cpu: NAVIGATOR_CPU_TIMESERIES,
      network: NAVIGATOR_NETWORK_TIMESERIES,
      disk: NAVIGATOR_DISK_TIMESERIES,
    }[options.metric];
    const serviceName = {
      memory: 'navigatorMemory',
      cpu: 'navigatorCpu',
      network: 'navigatorNetwork',
      disk: 'navigatorDisk',
    }[options.metric];

    try {
      const sql = buildQuery(template, {
        start_time: utcDateTime(options.startTime),
        end_time: utcDateTime(options.endTime),
        bucket_seconds: bucketSeconds,
      }).replaceAll('{hostname_filter}', hostnameFilter);
      const rows = await this.adapter.executeQuery<{
        t: string;
        average_v: number;
        peak_v: number;
      }>(
        tagQuery(sql, sourceTag(TAB_TIME_TRAVEL, serviceName)),
      );
      return {
        window_start: options.startTime.toISOString(),
        window_end: options.endTime.toISOString(),
        bucket_seconds: bucketSeconds,
        points: rows.map(row => ({
          t: String(row.t || ''),
          average_v: Number(row.average_v || 0),
          peak_v: Number(row.peak_v || 0),
        })),
      };
    } catch (error) {
      throw new TimelineServiceError('Failed to fetch navigator metric', error as Error);
    }
  }

  async getTimeline(options: TimelineOptions): Promise<MemoryTimeline> {
    const {
      timestamp,
      windowSeconds,
      includeRunning = true,
      hostname = null,
      activityLimit = 100,
      activeMetric = 'memory',
      normalizedQueryHash,
    } = options;
    const hostnames = (Array.isArray(hostname) ? hostname : hostname ? [hostname] : [])
      .map(host => host.replace(/[^a-zA-Z0-9._\-]/g, ''))
      .filter((host, index, hosts) => host.length > 0 && hosts.indexOf(host) === index);
    const hostCacheKey = [...hostnames].sort().join('\u0000') || 'all-hosts';

    // Validate hash early (before any queries run) to reject injection attempts
    if (normalizedQueryHash && !/^\d+$/.test(normalizedQueryHash)) {
      throw new TimelineServiceError(`Invalid normalized_query_hash: expected numeric UInt64`);
    }

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

    const params: Record<string, QueryParameter> = {
      start_time: utcDateTime(start),
      end_time: utcDateTime(end),
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
      if (hostnames.length === 0) return sql;
      const filter = hostnames.length === 1
        ? `AND hostname() = '${hostnames[0]}'`
        : `AND hostname() IN (${hostnames.map(host => `'${host}'`).join(', ')})`;
      
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
    const capacityWindowIncludesNow = end.getTime() >= now.getTime() - 30000;
    const includesNow = includeRunning && capacityWindowIncludesNow;

    // Fetch all data in parallel where possible
    const [
      serverMemory,
      serverCpu,
      networkData,
      diskData,
      ramResult,
      historicalCpuCapacity,
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
      this._cachedRam.has(hostCacheKey)
        ? Promise.resolve(this._cachedRam.get(hostCacheKey)!)
        : this.fetchTotalRam(params, withHost),
      this.fetchHistoricalCpuCapacity(params, withHost),
      // Per-host CPU breakdown for cluster tooltip (only in "All" mode)
      hostnames.length === 0 && activeMetric === 'cpu' ? this.fetchPerHostCpu(params) : Promise.resolve({}),
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

    // Hash filter overlay: fetch hash-matched queries and merge them in
    if (normalizedQueryHash) {
      const hashParams = { ...params };
      delete hashParams.activity_limit; // hash query has no LIMIT — bounded by time window
      const patternQueries = await this.fetchQueries(
        hashParams,
        start, end,
        (sql) => withHost(sql),
        normalizedQueryHash,
      );
      // Mark hash-matched queries and merge any that aren't already in the top-N
      const existingIds = new Set(allQueries.map(q => q.query_id));
      for (const q of allQueries) {
        if (patternQueries.some(pq => pq.query_id === q.query_id)) {
          q.matched_hash = true;
        }
      }
      for (const pq of patternQueries) {
        if (!existingIds.has(pq.query_id)) {
          pq.matched_hash = true;
          allQueries.push(pq);
        }
      }
    }

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

    // RAM metadata may have been cached before cluster detection completed,
    // when an unfiltered request could only see the local host. Validate the
    // cached host set against every host observed by the current cluster-aware
    // queries and refresh it when it is incomplete.
    const observedHosts = Array.from(new Set([
      ...ramResult.hosts,
      ...Object.keys(historicalCpuCapacity.byHost),
      ...Object.keys(perHostCpu),
    ]));
    let expectedRamHosts = hostnames.length > 0 ? hostnames : observedHosts;
    const cachedRamHosts = new Set(ramResult.hosts);
    const ramMetadataIncomplete = expectedRamHosts.some(host => !cachedRamHosts.has(host));
    let ramRefreshCoveredHosts = new Set<string>();
    let resolvedRamResult = ramMetadataIncomplete
      ? await this.fetchTotalRam(params, withHost)
      : ramResult;
    if (ramMetadataIncomplete) {
      ramRefreshCoveredHosts = new Set(expectedRamHosts);
    }
    if (hostnames.length === 0) {
      expectedRamHosts = Array.from(new Set([
        ...expectedRamHosts,
        ...resolvedRamResult.hosts,
      ]));
    }
    // Fallback filling must not mutate a result that may later be cached or
    // shared by another caller.
    let cpuCapacity = {
      byHost: { ...historicalCpuCapacity.byHost },
      sourceByHost: { ...historicalCpuCapacity.sourceByHost },
    };
    let discoveredCpuHosts = Object.keys(cpuCapacity.byHost);
    let expectedCpuHosts = hostnames.length > 0
      ? hostnames
      : Array.from(new Set([
          ...resolvedRamResult.hosts,
          ...discoveredCpuHosts,
          ...Object.keys(perHostCpu),
        ]));
    let missingCpuCapacityHosts = expectedCpuHosts.filter(
      host => !(Number.isFinite(cpuCapacity.byHost[host]) && cpuCapacity.byHost[host] > 0),
    );
    if (expectedCpuHosts.length === 0 || missingCpuCapacityHosts.length > 0) {
      const currentCpuCapacity = await this.fetchCurrentCpuCapacity(withHost);
      const hostsToFill = hostnames.length === 0
        ? Array.from(new Set([
            ...missingCpuCapacityHosts,
            ...Object.keys(currentCpuCapacity.byHost),
          ]))
        : missingCpuCapacityHosts;
      for (const host of hostsToFill) {
        const historicalCapacity = cpuCapacity.byHost[host];
        if (Number.isFinite(historicalCapacity) && historicalCapacity > 0) continue;
        const currentCapacity = currentCpuCapacity.byHost[host];
        if (Number.isFinite(currentCapacity) && currentCapacity > 0) {
          cpuCapacity.byHost[host] = currentCapacity;
          cpuCapacity.sourceByHost[host] = 'current';
        }
      }
      discoveredCpuHosts = Object.keys(cpuCapacity.byHost);
      if (hostnames.length === 0) {
        expectedCpuHosts = Array.from(new Set([
          ...expectedCpuHosts,
          ...discoveredCpuHosts,
        ]));
      }
      missingCpuCapacityHosts = expectedCpuHosts.filter(
        host => !(Number.isFinite(cpuCapacity.byHost[host]) && cpuCapacity.byHost[host] > 0),
      );
    }
    const cpuCapacityComplete = expectedCpuHosts.length > 0
      && missingCpuCapacityHosts.length === 0;
    const selectedCpuCapacities = cpuCapacityComplete
      ? expectedCpuHosts.map(host => cpuCapacity.byHost[host])
      : [];
    const cpuCores = selectedCpuCapacities.length > 0
      ? Math.min(...selectedCpuCapacities)
      : 0;
    const selectedTotalCpuCores = selectedCpuCapacities.reduce(
      (sum, value) => sum + value,
      0,
    );
    const cpuCapacityApproximate = !capacityWindowIncludesNow
      && expectedCpuHosts.some(host => cpuCapacity.sourceByHost[host] === 'current');

    // Current CPU fallback can discover hosts that were absent from both the
    // cached RAM metadata and historical capacity. Reconcile RAM against the
    // final CPU host set before exposing a memory denominator.
    expectedRamHosts = Array.from(new Set([
      ...expectedRamHosts,
      ...expectedCpuHosts,
    ]));
    let resolvedRamHosts = new Set(resolvedRamResult.hosts);
    const newlyMissingRamHosts = expectedRamHosts.filter(
      host => !resolvedRamHosts.has(host) && !ramRefreshCoveredHosts.has(host),
    );
    if (newlyMissingRamHosts.length > 0) {
      resolvedRamResult = await this.fetchTotalRam(params, withHost);
      resolvedRamHosts = new Set(resolvedRamResult.hosts);
    }
    const missingRamCapacityHosts = expectedRamHosts.filter(
      host => !resolvedRamHosts.has(host),
    );
    const ramCapacityComplete = expectedRamHosts.length > 0
      && missingRamCapacityHosts.length === 0;
    this._cachedRam.set(hostCacheKey, resolvedRamResult);

    const totalRam = ramCapacityComplete ? resolvedRamResult.ram : 0;
    const selectedTotalRam = ramCapacityComplete
      ? resolvedRamResult.totalRam
      : undefined;
    const hostCount = Math.max(
      expectedCpuHosts.length,
      resolvedRamResult.hostCount,
      1,
    );

    return {
      window_start: start.toISOString(),
      window_end: end.toISOString(),
      target: timestamp.toISOString(),
      server_memory: serverMemory,
      // Clamp CPU values: under heavy load, metric_log collection can be delayed,
      // causing accumulated CPU µs to exceed the reported wall-clock interval.
      // Max µs/s = total selected CPU cores × 1,000,000.
      // See docs/metrics/cpu.md for details.
      server_cpu: selectedTotalCpuCores > 0
        ? serverCpu.map(p => ({
            t: p.t,
            v: Math.min(p.v, selectedTotalCpuCores * 1_000_000),
          }))
        : serverCpu,
      server_network_send: networkData.send,
      server_network_recv: networkData.recv,
      server_disk_read: diskData.read,
      server_disk_write: diskData.write,
      server_total_ram: totalRam,
      cpu_cores: cpuCores,
      total_ram: selectedTotalRam,
      ram_capacity_complete: ramCapacityComplete,
      ram_capacity_missing_hosts: missingRamCapacityHosts.length > 0
        ? missingRamCapacityHosts
        : undefined,
      total_cpu_cores: cpuCapacityComplete ? selectedTotalCpuCores : undefined,
      cpu_capacity_complete: cpuCapacityComplete,
      cpu_capacity_missing_hosts: missingCpuCapacityHosts.length > 0
        ? missingCpuCapacityHosts
        : undefined,
      cpu_capacity_approximate: cpuCapacityApproximate,
      host_count: hostCount,
      per_host_cpu: Object.keys(perHostCpu).length > 1 ? perHostCpu : undefined,
      per_host_cpu_cores: Object.keys(perHostCpu).length > 1 ? cpuCapacity.byHost : undefined,
      queries: allQueries,
      merges: allMerges,
      mutations: allMutations,
      query_count: totalQueryCount,
      merge_count: totalMergeCount,
      merge_peak_total: totalMergePeak,
      mutation_count: totalMutationCount,
    };
  }

  /**
   * Fetch per-second sampled data for zoom mode.
   *
   * Enriches the provided MemoryTimeline's queries/merges/mutations with
   * real per-second ZoomSample arrays from processes_history and merges_history.
   * Returns a shallow copy of the timeline with zoomSamples attached.
   *
   * @param timeline - The existing MemoryTimeline (from getTimeline)
   * @param startMs - Zoom window start (epoch ms)
   * @param endMs - Zoom window end (epoch ms)
   * @param hostname - Optional one-or-more-host filter
   */
  async getZoomData(
    timeline: MemoryTimeline,
    startMs: number,
    endMs: number,
    hostname?: string | readonly string[] | null,
  ): Promise<MemoryTimeline> {
    const start = new Date(startMs);
    const end = new Date(endMs);
    const params: Record<string, QueryParameter> = {
      start_time: utcDateTime(start),
      end_time: utcDateTime(end),
    };

    // Fetch process samples and merge samples in parallel
    const [processSamples, mergeSamples] = await Promise.all([
      this.fetchZoomProcessSamples(params, hostname ?? undefined),
      this.fetchZoomMergeSamples(params, hostname ?? undefined),
    ]);

    // Compute ZoomSamples per query_id from raw process samples
    const queryZoom = this.computeQueryZoomSamples(processSamples);

    // Compute ZoomSamples per part_name from raw merge samples
    const mergeZoom = this.computeMergeZoomSamples(mergeSamples);

    // Attach zoom samples to matching series (shallow copy)
    const queries = timeline.queries.map(q => {
      const samples = queryZoom.get(q.query_id);
      return samples ? { ...q, zoomSamples: samples } : q;
    });
    const merges = timeline.merges.map(m => {
      const entry = mergeZoom.get(m.part_name);
      if (!entry || entry.isMutation) return m;
      // Fill in flat CPU estimate — merges_history has no CPU ProfileEvents
      const cpuCores = m.duration_ms > 0 ? (m.cpu_us / 1_000_000) / (m.duration_ms / 1000) : 0;
      const samples = entry.samples.map(s => ({ ...s, cpu_cores: cpuCores }));
      return { ...m, zoomSamples: samples };
    });
    const mutations = timeline.mutations.map(m => {
      const entry = mergeZoom.get(m.part_name);
      if (!entry || !entry.isMutation) return m;
      const cpuCores = m.duration_ms > 0 ? (m.cpu_us / 1_000_000) / (m.duration_ms / 1000) : 0;
      const samples = entry.samples.map(s => ({ ...s, cpu_cores: cpuCores }));
      return { ...m, zoomSamples: samples };
    });

    return { ...timeline, queries, merges, mutations };
  }

  private async fetchZoomProcessSamples(
    params: Record<string, QueryParameter>,
    hostname?: string | readonly string[],
  ): Promise<Array<{ query_id: string; ts_ms: number; memory_usage: number; pe_cpu: number; pe_net_send: number; pe_net_recv: number; read_bytes: number; written_bytes: number }>> {
    try {
      const sql = buildQuery(buildZoomProcessSamplesSQL(hostname), params);
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_TIME_TRAVEL, 'zoomProcess')));
      return rows.map(r => {
        const row = r as Record<string, unknown>;
        return {
          query_id: String(row.query_id || ''),
          ts_ms: Number(row.ts_ms || 0),
          memory_usage: Number(row.memory_usage || 0),
          pe_cpu: Number(row.pe_cpu || 0),
          pe_net_send: Number(row.pe_net_send || 0),
          pe_net_recv: Number(row.pe_net_recv || 0),
          read_bytes: Number(row.read_bytes || 0),
          written_bytes: Number(row.written_bytes || 0),
        };
      });
    } catch (e) {
      console.error('[TimelineService] zoom process samples error:', e);
      return [];
    }
  }

  private async fetchZoomMergeSamples(
    params: Record<string, QueryParameter>,
    hostname?: string | readonly string[],
  ): Promise<Array<{ part_name: string; is_mutation: boolean; ts_ms: number; memory_usage: number; bytes_read: number; bytes_written: number }>> {
    try {
      const sql = buildQuery(buildZoomMergeSamplesSQL(hostname), params);
      const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_TIME_TRAVEL, 'zoomMerge')));
      return rows.map(r => {
        const row = r as Record<string, unknown>;
        return {
          part_name: String(row.part_name || ''),
          is_mutation: Number(row.is_mutation) === 1,
          ts_ms: Number(row.ts_ms || 0),
          memory_usage: Number(row.memory_usage || 0),
          bytes_read: Number(row.bytes_read_uncompressed || 0),
          bytes_written: Number(row.bytes_written_uncompressed || 0),
        };
      });
    } catch (e) {
      console.error('[TimelineService] zoom merge samples error:', e);
      return [];
    }
  }

  /**
   * Convert raw cumulative process samples into per-second ZoomSample arrays.
   * Groups by query_id, then computes deltas between consecutive samples.
   */
  private computeQueryZoomSamples(
    raw: Array<{ query_id: string; ts_ms: number; memory_usage: number; pe_cpu: number; pe_net_send: number; pe_net_recv: number; read_bytes: number; written_bytes: number }>,
  ): Map<string, ZoomSample[]> {
    // Group by query_id (already sorted by query_id, sample_time from SQL)
    const grouped = new Map<string, typeof raw>();
    for (const s of raw) {
      let arr = grouped.get(s.query_id);
      if (!arr) { arr = []; grouped.set(s.query_id, arr); }
      arr.push(s);
    }

    const result = new Map<string, ZoomSample[]>();
    for (const [qid, samples] of grouped) {
      const zoomed: ZoomSample[] = [];
      for (let i = 0; i < samples.length; i++) {
        const cur = samples[i];
        if (i === 0) {
          // First sample: no delta available, use memory only
          zoomed.push({ ms: cur.ts_ms, memory: cur.memory_usage, cpu_cores: 0, net_rate: 0, disk_rate: 0 });
          continue;
        }
        const prev = samples[i - 1];
        const dtSec = Math.max((cur.ts_ms - prev.ts_ms) / 1000, 0.1);

        const cpuDelta = Math.max(cur.pe_cpu - prev.pe_cpu, 0);
        const netDelta = Math.max(cur.pe_net_send - prev.pe_net_send, 0) + Math.max(cur.pe_net_recv - prev.pe_net_recv, 0);
        const diskDelta = Math.max(cur.read_bytes - prev.read_bytes, 0) + Math.max(cur.written_bytes - prev.written_bytes, 0);

        zoomed.push({
          ms: cur.ts_ms,
          memory: cur.memory_usage,
          cpu_cores: cpuDelta / 1_000_000 / dtSec,  // µs → cores
          net_rate: netDelta / dtSec,
          disk_rate: diskDelta / dtSec,
        });
      }
      if (zoomed.length > 0) result.set(qid, zoomed);
    }
    return result;
  }

  /**
   * Convert raw cumulative merge samples into per-second ZoomSample arrays.
   * Merges have memory and I/O but no CPU — cpu_cores is always 0.
   */
  private computeMergeZoomSamples(
    raw: Array<{ part_name: string; is_mutation: boolean; ts_ms: number; memory_usage: number; bytes_read: number; bytes_written: number }>,
  ): Map<string, { isMutation: boolean; samples: ZoomSample[] }> {
    const grouped = new Map<string, typeof raw>();
    for (const s of raw) {
      let arr = grouped.get(s.part_name);
      if (!arr) { arr = []; grouped.set(s.part_name, arr); }
      arr.push(s);
    }

    const result = new Map<string, { isMutation: boolean; samples: ZoomSample[] }>();
    for (const [partName, samples] of grouped) {
      const zoomed: ZoomSample[] = [];
      const isMutation = samples[0]?.is_mutation ?? false;
      for (let i = 0; i < samples.length; i++) {
        const cur = samples[i];
        if (i === 0) {
          zoomed.push({ ms: cur.ts_ms, memory: cur.memory_usage, cpu_cores: 0, net_rate: 0, disk_rate: 0 });
          continue;
        }
        const prev = samples[i - 1];
        const dtSec = Math.max((cur.ts_ms - prev.ts_ms) / 1000, 0.1);
        const diskDelta = Math.max(cur.bytes_read - prev.bytes_read, 0) + Math.max(cur.bytes_written - prev.bytes_written, 0);

        zoomed.push({
          ms: cur.ts_ms,
          memory: cur.memory_usage,
          cpu_cores: 0,  // merges_history has no CPU data
          net_rate: 0,
          disk_rate: diskDelta / dtSec,
        });
      }
      if (zoomed.length > 0) result.set(partName, { isMutation, samples: zoomed });
    }
    return result;
  }

  private async fetchServerMemory(params: Record<string, QueryParameter>, xform: (s: string) => string): Promise<TimeseriesPoint[]> {
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

  private async fetchServerCpu(params: Record<string, QueryParameter>, xform: (s: string) => string): Promise<TimeseriesPoint[]> {
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
  private async fetchPerHostCpu(params: Record<string, QueryParameter>): Promise<Record<string, TimeseriesPoint[]>> {
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


  private async fetchNetworkData(params: Record<string, QueryParameter>, xform: (s: string) => string): Promise<{ send: TimeseriesPoint[]; recv: TimeseriesPoint[] }> {
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

  private async fetchDiskData(params: Record<string, QueryParameter>, xform: (s: string) => string): Promise<{ read: TimeseriesPoint[]; write: TimeseriesPoint[] }> {
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

  private async fetchTotalRam(
    params: Record<string, QueryParameter>,
    xform: (s: string) => string,
  ): Promise<{ ram: number; totalRam: number; hostCount: number; hosts: string[] }> {
      try {
        const sql = xform(buildQuery(SERVER_TOTAL_RAM, params));
        const rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_TIME_TRAVEL, 'totalRam')));
        if (rows.length === 0) {
          return { ram: 0, totalRam: 0, hostCount: 1, hosts: [] };
        }
        // Query returns one capacity row per host. Keep the smallest per-host
        // value for backward compatibility and sum all rows for Overall.
        const capacities = rows
          .map((row) => {
            const record = row as Record<string, unknown>;
            return {
              host: String(record.host || ''),
              value: Number(record.value || 0),
            };
          })
          .filter(({ host, value }) => host.length > 0 && value > 0);
        const hosts = capacities.map(({ host }) => host);
        const values = capacities.map(({ value }) => value);
        const hostCount = values.length || 1;
        const perHostRam = values.length > 0 ? Math.min(...values) : 0;
        const totalRam = values.reduce((sum, value) => sum + value, 0);

        // In containers, OSMemoryTotal reports host RAM — use cgroup limit if available
        const cgroupMem = await this.fetchCgroupMemoryLimit();
        if (cgroupMem > 0 && cgroupMem < perHostRam) {
          return { ram: cgroupMem, totalRam: cgroupMem * hostCount, hostCount, hosts };
        }

        return { ram: perHostRam, totalRam, hostCount, hosts };
      } catch (e) {
        console.error('[TimelineService] total_ram error:', e);
      }
      return { ram: 0, totalRam: 0, hostCount: 1, hosts: [] };
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

  private resolveCpuCapacityRows(
    rows: Record<string, unknown>[],
  ): Record<string, number> {
    const result: Record<string, number> = {};
    const positiveFinite = (value: unknown): number | null => {
      const parsed = Number(value || 0);
      return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
    };

    for (const row of rows) {
      const host = String(row.host || '');
      if (!host) continue;

      const cgroupMaxCpu = positiveFinite(row.cgroup_max_cpu);
      const reportedCores = positiveFinite(row.reported_cores);
      const osCpuCount = positiveFinite(row.os_cpu_count);
      const hostCores = reportedCores ?? osCpuCount;
      const effectiveCapacity = cgroupMaxCpu !== null
        ? hostCores !== null
          ? Math.min(cgroupMaxCpu, hostCores)
          : cgroupMaxCpu
        : hostCores;

      if (effectiveCapacity !== null) {
        // Preserve fractional cgroup quotas such as 2.5 CPUs.
        result[host] = effectiveCapacity;
      }
    }
    return result;
  }

  private async fetchHistoricalCpuCapacity(
    params: Record<string, QueryParameter>,
    xform: (s: string) => string,
  ): Promise<{
    byHost: Record<string, number>;
    sourceByHost: Record<string, 'historical' | 'current'>;
  }> {
    try {
      const sql = xform(buildQuery(SERVER_CPU_CAPACITY_HISTORY, params));
      const rows = await this.adapter.executeQuery(
        tagQuery(sql, sourceTag(TAB_TIME_TRAVEL, 'cpuCapacityHistorical')),
      );
      const byHost = this.resolveCpuCapacityRows(rows);
      return {
        byHost,
        sourceByHost: Object.fromEntries(
          Object.keys(byHost).map(host => [host, 'historical' as const]),
        ),
      };
    } catch (error) {
      console.error('[TimelineService] historical CPU capacity error:', error);
      return { byHost: {}, sourceByHost: {} };
    }
  }

  private async fetchCurrentCpuCapacity(
    xform: (s: string) => string,
  ): Promise<{
    byHost: Record<string, number>;
    sourceByHost: Record<string, 'historical' | 'current'>;
  }> {
    try {
      const sql = xform(SERVER_CPU_CAPACITY_CURRENT);
      const rows = await this.adapter.executeQuery(
        tagQuery(sql, sourceTag(TAB_TIME_TRAVEL, 'cpuCapacityCurrent')),
      );
      const byHost = this.resolveCpuCapacityRows(rows);
      return {
        byHost,
        sourceByHost: Object.fromEntries(
          Object.keys(byHost).map(host => [host, 'current' as const]),
        ),
      };
    } catch (error) {
      console.error('[TimelineService] current CPU capacity fallback error:', error);
      return { byHost: {}, sourceByHost: {} };
    }
  }

  private async fetchQueries(params: Record<string, QueryParameter>, start: Date, end: Date, xform: (s: string) => string, normalizedQueryHash?: string): Promise<QuerySeries[]> {
    try {
      let sql: string;
      if (normalizedQueryHash) {
        // Hash already validated in getTimeline() — safe to inject as raw UInt64
        sql = xform(buildQuery(ACTIVE_QUERIES_BY_HASH, params))
          .replaceAll('{normalized_query_hash}', normalizedQueryHash);
      } else {
        sql = xform(buildQuery(ACTIVE_QUERIES, params));
      }
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
      // For hash-filtered queries, propagate the error so the caller can report it
      // instead of silently returning 0 matches
      if (normalizedQueryHash) throw e;
      console.error('[TimelineService] query_log error:', e);
      return [];
    }
  }

  private async fetchMergeStats(params: Record<string, QueryParameter>, xform: (s: string) => string): Promise<{ count: number; peakTotal: number }> {
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
  private async fetchQueryCount(params: Record<string, QueryParameter>, xform: (s: string) => string): Promise<number> {
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

  private async fetchMerges(params: Record<string, QueryParameter>, start: Date, end: Date, xform: (s: string) => string): Promise<MergeSeries[]> {
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
          merge_reason: classifyMergeHistory(String(row.event_type || 'MergeParts'), String(row.merge_reason || ''), String(row.part_name || '')),
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

  private async fetchMutationCount(params: Record<string, QueryParameter>, xform: (s: string) => string): Promise<number> {
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

  private async fetchMutations(params: Record<string, QueryParameter>, start: Date, end: Date, xform: (s: string) => string): Promise<MutationSeries[]> {
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
          merge_reason: classifyActiveMerge(String(row.merge_type || 'Regular'), isMutation, String(row.part_name || '')),
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
        start_time: utcDateTime(fromTime),
        end_time: utcDateTime(toTime),
      };

      const identity = (s: string) => s;

      // Fetch CPU timeseries and core count in parallel
      const [rawRows, historicalCpuCapacity] = await Promise.all([
        this.fetchSpikeTimeseries(params),
        this.fetchHistoricalCpuCapacity(params, identity),
      ]);

      let capacityValues = Object.values(historicalCpuCapacity.byHost);
      if (capacityValues.length === 0) {
        const currentCpuCapacity = await this.fetchCurrentCpuCapacity(identity);
        capacityValues = Object.values(currentCpuCapacity.byHost);
      }
      const totalCpuCores = capacityValues.length > 0
        ? capacityValues.reduce((sum, value) => sum + value, 0)
        : 0;
      const cores = totalCpuCores > 0 ? totalCpuCores : 1;
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
      params: Record<string, QueryParameter>
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
