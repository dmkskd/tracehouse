/**
 * EngineInternalsService
 * Service for collecting detailed engine metrics for the Engine Internals page
 */

import type { IClickHouseAdapter } from '../adapters/types.js';
import type {
  MemorySubsystem,
  MemoryXRay,
  CPUCoreInfo,
  ThreadPoolInfo,
  PKIndexEntry,
  DictionaryInfo,
  QueryInternals,
  EngineInternalsData,
  CPUSamplingByThread,
  TopCPUFunction,
  CPUSamplingData,
  CoreTimelineSlot,
  CoreTimelineData,
} from '../types/engine-internals.js';
import {
  GET_JEMALLOC_STATS,
  GET_MARK_CACHE_STATS,
  GET_UNCOMPRESSED_CACHE_STATS,
  GET_QUERY_MEMORY,
  GET_MERGE_MEMORY,
  GET_CPU_CORE_METRICS,
  GET_CGROUP_CPU,
  GET_MAX_THREADS,
  GET_THREAD_POOL_METRICS,
  GET_PK_INDEX_BY_TABLE,
  GET_DICTIONARIES,
  GET_ALL_QUERY_INTERNALS,
  GET_ENGINE_SERVER_INFO,
  GET_CPU_SAMPLES_BY_THREAD,
  GET_CPU_SAMPLES_BY_THREAD_FALLBACK,
  GET_TOP_CPU_STACKS,
  GET_TOP_CPU_STACKS_FALLBACK,
  GET_CORE_TIMELINE,
  GET_CORE_TIMELINE_FALLBACK,
  GET_CORE_TIMELINE_NO_CPU_ID,
  GET_CORE_TIMELINE_NO_CPU_ID_NO_THREAD_NAME,
} from '../queries/engine-internals-queries.js';
import { buildQuery, tagQuery } from '../queries/builder.js';
import { TAB_ENGINE, sourceTag } from '../queries/source-tags.js';
import { parseTimeValue } from '../utils/time.js';

// Color constants for memory subsystems
const COLORS = {
  queryMem: '#60a5fa',      // blue-400
  mergeMem: '#fbbf24',      // amber-400
  markCache: '#22d3ee',     // cyan-400
  uncompCache: '#14b8a6',   // teal-500
  primaryKey: '#a78bfa',    // violet-400
  dictionaries: '#fb923c',  // orange-400
  jemalloc: '#818cf8',      // indigo-400
  other: '#94a3b8',         // slate-400
  queries: '#3b82f6',       // blue-500
  merges: '#f59e0b',        // amber-500
  replication: '#8b5cf6',   // purple-500
};

export class EngineInternalsServiceError extends Error {
  constructor(message: string, public readonly cause?: Error) {
    super(message);
    this.name = 'EngineInternalsServiceError';
  }
}

/**
 * Calculate fragmentation percentage
 * Formula: (1 - allocated / resident) * 100
 */
export function calculateFragmentation(allocated: number, resident: number): number {
  if (resident <= 0 || allocated <= 0) return 0;
  return Math.max(0, (1 - allocated / resident) * 100);
}

/**
 * Calculate parallelism factor
 * Formula: (UserTimeMicroseconds + SystemTimeMicroseconds) / RealTimeMicroseconds
 */
export function calculateParallelismFactor(
  userTimeUs: number,
  systemTimeUs: number,
  realTimeUs: number
): number {
  if (realTimeUs <= 0) return 0;
  return (userTimeUs + systemTimeUs) / realTimeUs;
}

/**
 * Calculate index pruning effectiveness
 * Formula: ((TotalMarks - SelectedMarks) / TotalMarks) * 100
 */
export function calculateIndexPruning(totalMarks: number, selectedMarks: number): number {
  if (totalMarks <= 0) return 0;
  return ((totalMarks - selectedMarks) / totalMarks) * 100;
}

/**
 * Calculate average CPU across cores
 */
export function calculateAverageCpu(cores: CPUCoreInfo[]): number {
  if (cores.length === 0) return 0;
  const sum = cores.reduce((acc, core) => acc + core.pct, 0);
  return sum / cores.length;
}

/**
 * Detect if a thread pool is saturated (> 80% utilization)
 */
export function isThreadPoolSaturated(active: number, max: number): boolean {
  if (max <= 0) return false;
  return active / max > 0.8;
}

/**
 * Calculate total query memory from a list of running queries
 * Formula: sum of memory_usage for all queries
 */
export function calculateTotalQueryMemory(queries: Array<{ memoryUsage: number }>): number {
  return queries.reduce((sum, query) => sum + (query.memoryUsage || 0), 0);
}

/**
 * Calculate total merge memory from a list of active merges
 * Formula: sum of memory_usage for all merges
 */
export function calculateTotalMergeMemory(merges: Array<{ memoryUsage: number }>): number {
  return merges.reduce((sum, merge) => sum + (merge.memoryUsage || 0), 0);
}

/**
 * Format bytes to human-readable string
 */
function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

/**
 * Sort items by a specified field and direction
 * Used for sorting queries, merges, PK entries, and dictionaries
 * 
 * @param items - Array of items to sort
 * @param field - The field name to sort by (must be a numeric field)
 * @param direction - Sort direction: 'asc' for ascending, 'desc' for descending
 * @returns A new sorted array (does not mutate the original)
 */
export function sortByField<T extends Record<string, unknown>>(
  items: T[],
  field: keyof T,
  direction: 'asc' | 'desc'
): T[] {
  if (items.length === 0) return [];

  return [...items].sort((a, b) => {
    const aVal = a[field];
    const bVal = b[field];

    // Handle numeric comparison
    const aNum = typeof aVal === 'number' ? aVal : 0;
    const bNum = typeof bVal === 'number' ? bVal : 0;

    if (direction === 'asc') {
      return aNum - bNum;
    } else {
      return bNum - aNum;
    }
  });
}

export class EngineInternalsService {
  constructor(private adapter: IClickHouseAdapter) { }

  /**
   * Get all Engine Internals data in a single call
   */
  async getEngineInternalsData(): Promise<EngineInternalsData> {
    try {
      const [
        serverInfo,
        memoryXRay,
        cpuResult,
        threadPools,
        pkIndex,
        dictionaries,
        queryInternals,
      ] = await Promise.all([
        this.getServerInfo(),
        this.getMemoryXRay(),
        this.getCPUCoreMetricsWithMeta(),
        this.getThreadPoolMetrics(),
        this.getPKIndexByTable(20),
        this.getDictionaries(),
        this.getAllQueryInternals(),
      ]);

      return {
        serverInfo,
        memoryXRay,
        cpuCores: cpuResult.cores,
        cpuCoresMeta: cpuResult.meta,
        threadPools,
        pkIndex,
        dictionaries,
        queryInternals,
        lastPollTime: new Date(),
      };
    } catch (error) {
      throw new EngineInternalsServiceError('Failed to get engine internals data', error as Error);
    }
  }

  // ===========================================================================
  // Memory X-Ray
  // ===========================================================================

  async getMemoryXRay(): Promise<MemoryXRay> {
    try {
      const [
        jemallocStats,
        markCacheStats,
        uncompCacheStats,
        queryMemory,
        mergeMemory,
        pkMemory,
        dictMemory,
      ] = await Promise.all([
        this.getJemallocStats(),
        this.getMarkCacheStats(),
        this.getUncompressedCacheStats(),
        this.getQueryMemory(),
        this.getMergeMemory(),
        this.getPKMemoryTotal(),
        this.getDictMemoryTotal(),
      ]);

      const totalRSS = jemallocStats.resident;
      const totalRAM = jemallocStats.metadata; // Will be overwritten below

      // Get actual total RAM from the same async metrics (already fetched by getJemallocStats)
      // Re-fetch once to get OSMemoryTotal which getJemallocStats doesn't extract
      const asyncRows = await this.adapter.executeQuery<{ metric: string; value: number }>(
        tagQuery(GET_JEMALLOC_STATS, sourceTag(TAB_ENGINE, 'jemalloc'))
      );
      const asyncMetrics = new Map(asyncRows.map(r => [String(r.metric), Number(r.value)]));
      const hostRAM = asyncMetrics.get('OSMemoryTotal') || 0;
      const cgroupMemLimit = asyncMetrics.get('CGroupMemoryTotal') || asyncMetrics.get('CGroupMemoryLimit') || 0;
      const actualTotalRAM = (cgroupMemLimit > 0 && cgroupMemLimit < 1e18 && cgroupMemLimit < hostRAM)
        ? cgroupMemLimit
        : hostRAM;
      const actualRSS = asyncMetrics.get('MemoryResident') || jemallocStats.resident;

      // Build subsystems array
      const subsystems: MemorySubsystem[] = [
        {
          id: 'query',
          label: 'Query Working Memory',
          bytes: queryMemory,
          color: COLORS.queryMem,
          detail: 'Memory used by running queries',
          icon: '',
        },
        {
          id: 'merge',
          label: 'Merge Buffers',
          bytes: mergeMemory,
          color: COLORS.mergeMem,
          detail: 'Memory used by active merges',
          icon: '',
        },
        {
          id: 'markCache',
          label: 'Mark Cache',
          bytes: markCacheStats.bytes,
          color: COLORS.markCache,
          detail: 'Cache for granule offset indices',
          icon: '',
          sub: {
            files: markCacheStats.files,
            hitRate: markCacheStats.hitRate,
            missesPerSec: markCacheStats.missesPerSec,
          },
        },
        {
          id: 'uncompCache',
          label: 'Uncompressed Cache',
          bytes: uncompCacheStats.bytes,
          color: COLORS.uncompCache,
          detail: 'Cache for decompressed column blocks',
          icon: '',
          sub: {
            cells: uncompCacheStats.cells,
            hitRate: uncompCacheStats.hitRate,
            missesPerSec: uncompCacheStats.missesPerSec,
          },
        },
        {
          id: 'primaryKey',
          label: 'Primary Key Index',
          bytes: pkMemory,
          color: COLORS.primaryKey,
          detail: 'Sparse index storing first-row PK values per granule',
          icon: '',
        },
        {
          id: 'dictionaries',
          label: 'Dictionaries',
          bytes: dictMemory,
          color: COLORS.dictionaries,
          detail: 'Loaded external dictionaries',
          icon: '',
        },
      ];

      // Calculate "other" memory
      const trackedMemory = subsystems.reduce((sum, s) => sum + s.bytes, 0);
      const jemallocOverhead = Math.max(0, jemallocStats.resident - jemallocStats.allocated);
      const otherMemory = Math.max(0, actualRSS - trackedMemory - jemallocOverhead);

      subsystems.push({
        id: 'jemalloc',
        label: 'jemalloc Overhead',
        bytes: jemallocOverhead,
        color: COLORS.jemalloc,
        detail: 'Memory allocator overhead and fragmentation',
        icon: '',
      });

      subsystems.push({
        id: 'other',
        label: 'Other',
        bytes: otherMemory,
        color: COLORS.other,
        detail: 'Untracked memory usage',
        icon: '',
      });

      const fragmentationPct = calculateFragmentation(
        jemallocStats.allocated,
        jemallocStats.resident
      );

      return {
        totalRSS: actualRSS,
        totalRAM: actualTotalRAM,
        jemalloc: jemallocStats,
        subsystems,
        osPageCache: 0, // Would need OS-level access
        free: actualTotalRAM - actualRSS,
        fragmentationPct: Math.round(fragmentationPct * 10) / 10,
      };
    } catch (error) {
      console.error('[EngineInternalsService] getMemoryXRay error:', error);
      return {
        totalRSS: 0,
        totalRAM: 0,
        jemalloc: { allocated: 0, resident: 0, mapped: 0, retained: 0, metadata: 0 },
        subsystems: [],
        osPageCache: 0,
        free: 0,
        fragmentationPct: 0,
      };
    }
  }

  async getJemallocStats(): Promise<MemoryXRay['jemalloc']> {
    try {
      const rows = await this.adapter.executeQuery<{ metric: string; value: number }>(
        tagQuery(GET_JEMALLOC_STATS, sourceTag(TAB_ENGINE, 'jemalloc'))
      );
      const metrics = new Map(rows.map(r => [String(r.metric), Number(r.value)]));

      return {
        allocated: metrics.get('jemalloc.allocated') || 0,
        resident: metrics.get('jemalloc.resident') || metrics.get('MemoryResident') || 0,
        mapped: metrics.get('jemalloc.mapped') || 0,
        retained: metrics.get('jemalloc.retained') || 0,
        metadata: metrics.get('jemalloc.metadata') || 0,
      };
    } catch (error) {
      console.error('[EngineInternalsService] getJemallocStats error:', error);
      return { allocated: 0, resident: 0, mapped: 0, retained: 0, metadata: 0 };
    }
  }

  async getMarkCacheStats(): Promise<{ bytes: number; files: number; hitRate: number; missesPerSec: number }> {
    try {
      const rows = await this.adapter.executeQuery<{
        bytes: number;
        files: number;
        hits: number;
        misses: number;
      }>(tagQuery(GET_MARK_CACHE_STATS, sourceTag(TAB_ENGINE, 'markCache')));


      if (rows.length === 0) {
        return { bytes: 0, files: 0, hitRate: 0, missesPerSec: 0 };
      }

      const row = rows[0];
      const hits = Number(row.hits) || 0;
      const misses = Number(row.misses) || 0;
      const total = hits + misses;
      const hitRate = total > 0 ? (hits / total) * 100 : 0;

      return {
        bytes: Number(row.bytes) || 0,
        files: Number(row.files) || 0,
        hitRate: Math.round(hitRate * 10) / 10,
        missesPerSec: 0, // Would need delta calculation
      };
    } catch (error) {
      console.error('[EngineInternalsService] getMarkCacheStats error:', error);
      return { bytes: 0, files: 0, hitRate: 0, missesPerSec: 0 };
    }
  }

  async getUncompressedCacheStats(): Promise<{ bytes: number; cells: number; hitRate: number; missesPerSec: number }> {
    try {
      const rows = await this.adapter.executeQuery<{
        bytes: number;
        cells: number;
        hits: number;
        misses: number;
      }>(tagQuery(GET_UNCOMPRESSED_CACHE_STATS, sourceTag(TAB_ENGINE, 'uncompressedCache')));

      if (rows.length === 0) {
        return { bytes: 0, cells: 0, hitRate: 0, missesPerSec: 0 };
      }

      const row = rows[0];
      const hits = Number(row.hits) || 0;
      const misses = Number(row.misses) || 0;
      const total = hits + misses;
      const hitRate = total > 0 ? (hits / total) * 100 : 0;

      return {
        bytes: Number(row.bytes) || 0,
        cells: Number(row.cells) || 0,
        hitRate: Math.round(hitRate * 10) / 10,
        missesPerSec: 0,
      };
    } catch (error) {
      console.error('[EngineInternalsService] getUncompressedCacheStats error:', error);
      return { bytes: 0, cells: 0, hitRate: 0, missesPerSec: 0 };
    }
  }

  private async getQueryMemory(): Promise<number> {
    try {
      const rows = await this.adapter.executeQuery<{ total_memory: number }>(tagQuery(GET_QUERY_MEMORY, sourceTag(TAB_ENGINE, 'queryMemory')));
      return rows.length > 0 ? Number(rows[0].total_memory) || 0 : 0;
    } catch (error) {
      console.error('[EngineInternalsService] getQueryMemory error:', error);
      return 0;
    }
  }

  private async getMergeMemory(): Promise<number> {
    try {
      const rows = await this.adapter.executeQuery<{ total_memory: number }>(tagQuery(GET_MERGE_MEMORY, sourceTag(TAB_ENGINE, 'mergeMemory')));
      return rows.length > 0 ? Number(rows[0].total_memory) || 0 : 0;
    } catch (error) {
      console.error('[EngineInternalsService] getMergeMemory error:', error);
      return 0;
    }
  }

  private async getPKMemoryTotal(): Promise<number> {
    try {
      // Get from async metrics (works with primary_key_lazy_load=1)
      const asyncSql = tagQuery(`SELECT metric, value FROM system.asynchronous_metrics WHERE metric IN ('TotalPrimaryKeyBytesInMemory', 'TotalPrimaryKeyBytesInMemoryAllocated')`, sourceTag(TAB_ENGINE, 'pkMemory'));
      const asyncRows = await this.adapter.executeQuery<{ metric: string; value: number }>(asyncSql);

      // Prefer TotalPrimaryKeyBytesInMemory (actual loaded), fallback to Allocated
      const metrics = new Map(asyncRows.map(r => [String(r.metric), Number(r.value)]));
      const inMemory = metrics.get('TotalPrimaryKeyBytesInMemory') || 0;
      const allocated = metrics.get('TotalPrimaryKeyBytesInMemoryAllocated') || 0;

      if (inMemory > 0) {
        return inMemory;
      }
      if (allocated > 0) {
        return allocated;
      }

      // Fallback to summing from system.parts (for older versions)
      const sql = buildQuery(GET_PK_INDEX_BY_TABLE, { limit: 1000 });
      const rows = await this.adapter.executeQuery<{ pk_memory: number }>(tagQuery(sql, sourceTag(TAB_ENGINE, 'pkMemory')));
      const fallbackTotal = rows.reduce((sum, r) => sum + (Number(r.pk_memory) || 0), 0);
      return fallbackTotal;
    } catch (error) {
      console.error('[EngineInternalsService] getPKMemoryTotal error:', error);
      return 0;
    }
  }

  private async getDictMemoryTotal(): Promise<number> {
    try {
      const rows = await this.adapter.executeQuery<{ bytes_allocated: number }>(tagQuery(GET_DICTIONARIES, sourceTag(TAB_ENGINE, 'dictionaries')));
      return rows.reduce((sum, r) => sum + (Number(r.bytes_allocated) || 0), 0);
    } catch (error) {
      console.error('[EngineInternalsService] getDictMemoryTotal error:', error);
      return 0;
    }
  }

  // ===========================================================================
  // CPU Core Map
  // ===========================================================================

  async getCPUCoreMetrics(): Promise<CPUCoreInfo[]> {
    const result = await this.getCPUCoreMetricsWithMeta();
    return result.cores;
  }

  private async getCPUCoreMetricsWithMeta(): Promise<{
    cores: CPUCoreInfo[];
    meta?: EngineInternalsData['cpuCoresMeta'];
  }> {
    try {
      const rows = await this.adapter.executeQuery<{ metric: string; value: number }>(
        tagQuery(GET_CPU_CORE_METRICS, sourceTag(TAB_ENGINE, 'cpuCores'))
      );

      const metrics = new Map(rows.map(r => [String(r.metric), Number(r.value)]));

      // Count host-level logical cores from the per-core metrics themselves.
      // NumberOfCPUCores / NumberOfPhysicalCores report the host node's count,
      // which is wrong in Kubernetes where the pod has a cgroup CPU limit.
      let hostCores = metrics.get('NumberOfCPUCores') || metrics.get('NumberOfPhysicalCores') || 0;
      if (hostCores === 0) {
        const coreNumbers = new Set<number>();
        for (const key of metrics.keys()) {
          const match = key.match(/OSUserTimeCPU(\d+)/);
          if (match) coreNumbers.add(parseInt(match[1], 10));
        }
        if (coreNumbers.size > 0) hostCores = Math.max(...coreNumbers) + 1;
      }

      if (hostCores === 0) return { cores: [] };

      // Detect the effective (cgroup-limited) core count for containerized environments.
      const effectiveCores = await this.detectEffectiveCores(hostCores);
      const isCgroupLimited = effectiveCores < hostCores;

      // Build per-host-core data first
      const hostCoreData: { user: number; system: number; iowait: number; idle: number }[] = [];
      for (let i = 0; i < hostCores; i++) {
        hostCoreData.push({
          user: metrics.get(`OSUserTimeCPU${i}`) || 0,
          system: metrics.get(`OSSystemTimeCPU${i}`) || 0,
          iowait: metrics.get(`OSIOWaitTimeCPU${i}`) || 0,
          idle: metrics.get(`OSIdleTimeCPU${i}`) || 0,
        });
      }

      // If effective cores < host cores, aggregate host cores into effective slots.
      const cores: CPUCoreInfo[] = [];

      if (isCgroupLimited) {
        const ratio = hostCores / effectiveCores;
        for (let slot = 0; slot < effectiveCores; slot++) {
          const startIdx = Math.floor(slot * ratio);
          const endIdx = Math.floor((slot + 1) * ratio);
          let user = 0, system = 0, iowait = 0, idle = 0;
          for (let i = startIdx; i < endIdx && i < hostCores; i++) {
            user += hostCoreData[i].user;
            system += hostCoreData[i].system;
            iowait += hostCoreData[i].iowait;
            idle += hostCoreData[i].idle;
          }
          cores.push(this.buildCoreInfo(slot, user, system, iowait, idle));
        }
      } else {
        for (let i = 0; i < hostCores; i++) {
          const d = hostCoreData[i];
          cores.push(this.buildCoreInfo(i, d.user, d.system, d.iowait, d.idle));
        }
      }

      return {
        cores,
        meta: {
          effectiveCores,
          hostCores,
          isCgroupLimited,
        },
      };
    } catch (error) {
      console.error('[EngineInternalsService] getCPUCoreMetrics error:', error);
      return { cores: [] };
    }
  }

  /**
   * Detect the effective CPU core count, respecting cgroup limits in Kubernetes.
   * Returns the cgroup-limited count, or hostCores if no limit is detected.
   */
  private async detectEffectiveCores(hostCores: number): Promise<number> {
    // 1. Try CGroupMaxCPU async metric (ClickHouse >= 23.8)
    try {
      const cgroupRows = await this.adapter.executeQuery<{ metric: string; value: number }>(
        tagQuery(GET_CGROUP_CPU, sourceTag(TAB_ENGINE, 'cgroupCpu'))
      );
      if (cgroupRows.length > 0) {
        const val = Number(cgroupRows[0].value);
        // CGroupMaxCPU returns 0 when there's no cgroup limit
        if (val > 0 && val < hostCores) return Math.round(val);
      }
    } catch (err) {
      console.warn('[EngineInternals] CGroupMaxCPU metric not available:', err);
    }

    // 2. Fallback: max_threads from system.settings
    //    ClickHouse sets this to the detected CPU count at startup, which
    //    respects cgroup cpu.cfs_quota_us / cpu.max.
    try {
      const settingsRows = await this.adapter.executeQuery<{ value: string }>(
        tagQuery(GET_MAX_THREADS, sourceTag(TAB_ENGINE, 'maxThreads'))
      );
      if (settingsRows.length > 0) {
        const val = parseInt(String(settingsRows[0].value), 10);
        if (val > 0 && val < hostCores) return val;
      }
    } catch (err) {
      console.warn('[EngineInternals] system.settings not accessible:', err);
    }

    return hostCores;
  }

  private buildCoreInfo(
    core: number, user: number, system: number, iowait: number, idle: number
  ): CPUCoreInfo {
    const totalTime = user + system + iowait + idle;
    let pct = 0;
    let state: CPUCoreInfo['state'] = 'idle';
    let breakdown = { user: 0, system: 0, iowait: 0, idle: 100 };

    if (totalTime > 0) {
      const busyTime = user + system + iowait;
      pct = (busyTime / totalTime) * 100;
      breakdown = {
        user: (user / totalTime) * 100,
        system: (system / totalTime) * 100,
        iowait: (iowait / totalTime) * 100,
        idle: (idle / totalTime) * 100,
      };
      if (user >= system && user >= iowait) state = 'user';
      else if (system >= iowait) state = 'system';
      else if (iowait > 0) state = 'iowait';
    }

    return {
      core,
      pct: Math.round(pct * 10) / 10,
      state,
      owner: null,
      breakdown: {
        user: Math.round(breakdown.user * 10) / 10,
        system: Math.round(breakdown.system * 10) / 10,
        iowait: Math.round(breakdown.iowait * 10) / 10,
        idle: Math.round(breakdown.idle * 10) / 10,
      },
    };
  }

  // ===========================================================================
  // Thread Pools
  // ===========================================================================

  async getThreadPoolMetrics(): Promise<ThreadPoolInfo[]> {
    try {
      const rows = await this.adapter.executeQuery<{ metric: string; value: number }>(
        tagQuery(GET_THREAD_POOL_METRICS, sourceTag(TAB_ENGINE, 'threadPools'))
      );

      const metrics = new Map(rows.map(r => [String(r.metric), Number(r.value)]));

      const pools: ThreadPoolInfo[] = [
        {
          name: 'Query Execution',
          active: metrics.get('QueryThread') || 0,
          max: 100, // Default max, would need settings query
          color: COLORS.queries,
          metric: 'QueryThread',
          isSaturated: false,
        },
        {
          name: 'Merges & Mutations',
          active: metrics.get('BackgroundMergesAndMutationsPoolTask') || 0,
          max: metrics.get('BackgroundMergesAndMutationsPoolSize') || 16,
          color: COLORS.merges,
          metric: 'BackgroundMergesAndMutationsPoolTask',
          isSaturated: false,
        },
        {
          name: 'Replication Fetches',
          active: metrics.get('BackgroundFetchesPoolTask') || 0,
          max: metrics.get('BackgroundFetchesPoolSize') || 8,
          color: COLORS.replication,
          metric: 'BackgroundFetchesPoolTask',
          isSaturated: false,
        },
        {
          name: 'Schedule Pool',
          active: metrics.get('BackgroundSchedulePoolTask') || 0,
          max: metrics.get('BackgroundSchedulePoolSize') || 128,
          color: '#94a3b8',
          metric: 'BackgroundSchedulePoolTask',
          isSaturated: false,
        },
        {
          name: 'IO Thread Pool',
          active: metrics.get('IOThreadsActive') || 0,
          max: metrics.get('IOThreads') || 64, // Default to 64 if not available
          color: '#22c55e',
          metric: 'IOThreadsActive',
          isSaturated: false,
        },
        {
          name: 'Global Thread Pool',
          active: metrics.get('GlobalThreadActive') || 0,
          // GlobalThread is the current allocated count. The pool grows dynamically.
          // max_thread_pool_size defaults to 10000, but showing allocated vs active is more useful.
          // Don't mark as saturated since the pool can grow.
          max: metrics.get('GlobalThread') || 1000,
          color: '#64748b',
          metric: 'GlobalThreadActive',
          isSaturated: false, // Global pool grows dynamically, so never mark saturated
        },
      ];

      // Calculate saturation (skip GlobalThreadPool as it grows dynamically)
      return pools.map(pool => ({
        ...pool,
        isSaturated: pool.metric === 'GlobalThreadActive' ? false : isThreadPoolSaturated(pool.active, pool.max),
      }));
    } catch (error) {
      console.error('[EngineInternalsService] getThreadPoolMetrics error:', error);
      return [];
    }
  }

  // ===========================================================================
  // Primary Key Index
  // ===========================================================================

  async getPKIndexByTable(limit: number = 20): Promise<PKIndexEntry[]> {
    try {
      const sql = buildQuery(GET_PK_INDEX_BY_TABLE, { limit });
      const rows = await this.adapter.executeQuery<{
        database: string;
        table: string;
        pk_memory: number;
        pk_allocated: number;
        parts: number;
        total_rows: number;
        granules: number;
      }>(tagQuery(sql, sourceTag(TAB_ENGINE, 'pkIndex')));

      // Check if all pk_memory values are 0 (lazy load enabled)
      const allZero = rows.every(r => Number(r.pk_memory) === 0);

      if (allZero && rows.length > 0) {
        // Get total PK memory from async metrics
        const asyncSql = tagQuery(`SELECT metric, value FROM system.asynchronous_metrics WHERE metric IN ('TotalPrimaryKeyBytesInMemory', 'TotalPrimaryKeyBytesInMemoryAllocated')`, sourceTag(TAB_ENGINE, 'pkMemory'));
        const asyncRows = await this.adapter.executeQuery<{ metric: string; value: number }>(asyncSql);
        const metrics = new Map(asyncRows.map(r => [String(r.metric), Number(r.value)]));

        // Use actual in-memory value, or allocated as fallback
        const totalPkMemory = metrics.get('TotalPrimaryKeyBytesInMemory') || metrics.get('TotalPrimaryKeyBytesInMemoryAllocated') || 0;

        // Estimate per-table memory proportionally based on granules (marks)
        const totalGranules = rows.reduce((sum, r) => sum + Number(r.granules), 0);

        return rows.map(row => {
          const granules = Number(row.granules) || 0;
          // Estimate: distribute total PK memory proportionally by granule count
          const estimatedPkMemory = totalGranules > 0
            ? Math.round((granules / totalGranules) * totalPkMemory)
            : 0;

          return {
            database: String(row.database),
            table: String(row.table),
            pkMemory: estimatedPkMemory,
            pkAllocated: estimatedPkMemory, // Same estimate for allocated
            parts: Number(row.parts) || 0,
            rows: Number(row.total_rows) || 0,
            granules,
          };
        }).sort((a, b) => b.pkMemory - a.pkMemory); // Re-sort by estimated memory
      }

      return rows.map(row => ({
        database: String(row.database),
        table: String(row.table),
        pkMemory: Number(row.pk_memory) || 0,
        pkAllocated: Number(row.pk_allocated) || 0,
        parts: Number(row.parts) || 0,
        rows: Number(row.total_rows) || 0,
        granules: Number(row.granules) || 0,
      }));
    } catch (error) {
      console.error('[EngineInternalsService] getPKIndexByTable error:', error);
      return [];
    }
  }

  // ===========================================================================
  // Dictionaries
  // ===========================================================================

  async getDictionaries(): Promise<DictionaryInfo[]> {
    try {
      const rows = await this.adapter.executeQuery<{
        name: string;
        type: string;
        bytes_allocated: number;
        element_count: number;
        load_factor: number;
        source: string;
        loading_status: string;
        last_successful_update_time: string;
      }>(tagQuery(GET_DICTIONARIES, sourceTag(TAB_ENGINE, 'dictionaries')));

      return rows.map(row => ({
        name: String(row.name),
        type: String(row.type),
        bytesAllocated: Number(row.bytes_allocated) || 0,
        elementCount: Number(row.element_count) || 0,
        loadFactor: Number(row.load_factor) || 0,
        source: String(row.source),
        loadingStatus: String(row.loading_status),
        lastSuccessfulUpdate: row.last_successful_update_time
          ? String(row.last_successful_update_time)
          : null,
      }));
    } catch (error) {
      console.error('[EngineInternalsService] getDictionaries error:', error);
      return [];
    }
  }

  // ===========================================================================
  // Query Internals
  // ===========================================================================

  async getAllQueryInternals(): Promise<QueryInternals[]> {
    try {
      const rows = await this.adapter.executeQuery<{
        query_id: string;
        user: string;
        elapsed: number;
        memory_usage: number;
        query_kind: string;
        query: string;
        read_rows: number;
        read_bytes: number;
        total_rows_approx: number;
        user_time_us: number;
        system_time_us: number;
        real_time_us: number;
        io_wait_us: number;
        read_compressed_bytes: number;
        selected_parts: number;
        selected_marks: number;
        mark_cache_hits: number;
        mark_cache_misses: number;
        thread_count: number;
      }>(tagQuery(GET_ALL_QUERY_INTERNALS, sourceTag(TAB_ENGINE, 'queryInternals')));

      return rows.map(row => {
        const userTimeUs = Number(row.user_time_us) || 0;
        const systemTimeUs = Number(row.system_time_us) || 0;
        const realTimeUs = Number(row.real_time_us) || 0;
        const selectedMarks = Number(row.selected_marks) || 0;
        const markCacheHits = Number(row.mark_cache_hits) || 0;
        const markCacheMisses = Number(row.mark_cache_misses) || 0;

        // Estimate total marks from cache activity
        const totalMarks = selectedMarks > 0 ? selectedMarks : markCacheHits + markCacheMisses;

        return {
          queryId: String(row.query_id),
          kind: String(row.query_kind) || 'Unknown',
          user: String(row.user),
          elapsed: Number(row.elapsed) || 0,
          query: String(row.query),
          totalMemory: Number(row.memory_usage) || 0,
          memoryBreakdown: [], // Would need more detailed tracking
          pipeline: [], // Would need EXPLAIN PIPELINE
          profileEvents: {
            userTimeMicroseconds: userTimeUs,
            systemTimeMicroseconds: systemTimeUs,
            realTimeMicroseconds: realTimeUs,
            osIOWaitMicroseconds: Number(row.io_wait_us) || 0,
            readCompressedBytes: Number(row.read_compressed_bytes) || 0,
            selectedParts: Number(row.selected_parts) || 0,
            selectedMarks,
            totalMarks,
            markCacheHits,
            markCacheMisses,
          },
          threads: Number(row.thread_count) || 0,
          maxThreads: 0, // Would need settings
        };
      });
    } catch (error) {
      console.error('[EngineInternalsService] getAllQueryInternals error:', error);
      return [];
    }
  }

  // ===========================================================================
  // Server Info
  // ===========================================================================

  async getServerInfo(): Promise<EngineInternalsData['serverInfo']> {
    try {
      const [infoRows, jemallocStats] = await Promise.all([
        this.adapter.executeQuery<{ hostname: string; version: string }>(tagQuery(GET_ENGINE_SERVER_INFO, sourceTag(TAB_ENGINE, 'serverInfo'))),
        this.getJemallocStats(),
      ]);

      const info = infoRows.length > 0 ? infoRows[0] : { hostname: 'unknown', version: 'unknown' };

      // Get cores and RAM from async metrics
      const asyncRows = await this.adapter.executeQuery<{ metric: string; value: number }>(
        tagQuery(GET_JEMALLOC_STATS, sourceTag(TAB_ENGINE, 'jemalloc'))
      );
      const asyncMetrics = new Map(asyncRows.map(r => [String(r.metric), Number(r.value)]));

      // In containers, OSMemoryTotal reports host RAM — use cgroup limit if available
      const hostRAM = asyncMetrics.get('OSMemoryTotal') || 0;
      const cgroupMemLimit = asyncMetrics.get('CGroupMemoryTotal') || asyncMetrics.get('CGroupMemoryLimit') || 0;
      const effectiveRAM = (cgroupMemLimit > 0 && cgroupMemLimit < 1e18 && cgroupMemLimit < hostRAM)
        ? cgroupMemLimit
        : hostRAM;

      return {
        hostname: String(info.hostname),
        version: String(info.version),
        cores: 0, // Will be populated from CPU metrics
        totalRAM: effectiveRAM,
      };
    } catch (error) {
      console.error('[EngineInternalsService] getServerInfo error:', error);
      return {
        hostname: 'unknown',
        version: 'unknown',
        cores: 0,
        totalRAM: 0,
      };
    }
  }

  // ===========================================================================
  // CPU Sampling Attribution (from system.trace_log)
  // ===========================================================================

  /**
   * Get CPU sampling data from trace_log, aggregated by thread pool.
   * Returns null if trace_log is not available.
   */
  async getCPUSamplingData(windowSeconds: number = 15, offsetSeconds: number = 0): Promise<CPUSamplingData | null> {
    try {
      // Try with thread_name column first, fall back to thread_id for older versions
      let rows: { thread_name: string; cpu_samples: number; query_samples: number; background_samples: number }[];
      let useFallbackStacks = false;
      const queryParams = { window_seconds: windowSeconds, offset_seconds: offsetSeconds };
      try {
        const sql = buildQuery(GET_CPU_SAMPLES_BY_THREAD, queryParams);
        rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_ENGINE, 'cpuSampling')));
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        if (msg.includes('thread_name') || msg.includes('Unknown expression')) {
          const fallbackSql = buildQuery(GET_CPU_SAMPLES_BY_THREAD_FALLBACK, queryParams);
          rows = await this.adapter.executeQuery(tagQuery(fallbackSql, sourceTag(TAB_ENGINE, 'cpuSampling')));
          useFallbackStacks = true;
        } else {
          throw e;
        }
      }

      if (rows.length === 0) {
        return { byThread: [], topFunctions: [], totalSamples: 0, windowSeconds };
      }

      const byThread: CPUSamplingByThread[] = rows.map(row => ({
        threadName: String(row.thread_name),
        cpuSamples: Number(row.cpu_samples) || 0,
        querySamples: Number(row.query_samples) || 0,
        backgroundSamples: Number(row.background_samples) || 0,
        pool: classifyThreadPool(String(row.thread_name)),
      }));

      const totalSamples = byThread.reduce((sum, t) => sum + t.cpuSamples, 0);

      // Try to get top functions (may fail if introspection functions not allowed)
      let topFunctions: TopCPUFunction[] = [];
      try {
        const stackTemplate = useFallbackStacks ? GET_TOP_CPU_STACKS_FALLBACK : GET_TOP_CPU_STACKS;
        const stackSql = buildQuery(stackTemplate, queryParams);
        const stackRows = await this.adapter.executeQuery<{
          thread_name: string;
          top_function: string;
          samples: number;
        }>(tagQuery(stackSql, sourceTag(TAB_ENGINE, 'cpuStacks')));

        topFunctions = stackRows.map(row => ({
          threadName: String(row.thread_name),
          functionName: cleanFunctionName(String(row.top_function)),
          samples: Number(row.samples) || 0,
        }));
      } catch (err) {
        console.warn('[EngineInternals] Introspection functions not available:', err);
      }

      return {
        byThread,
        topFunctions,
        totalSamples,
        windowSeconds,
      };
    } catch (err) {
      // Surface the error instead of silently returning null
      console.error('[EngineInternalsService] getCPUSamplingData error:', err);
      throw err;
    }
  }

  // ===========================================================================
  // Per-Core Timeline (from system.trace_log)
  // ===========================================================================

  /**
   * Get per-core CPU timeline from trace_log.
   * Returns time-slotted samples per physical CPU core for swimlane visualization.
   */
  async getCoreTimeline(windowSeconds: number = 15, cpuOnly: boolean = false): Promise<CoreTimelineData | null> {
    try {
      let rows: { core: number; slot: string; thread_name: string; query_id: string; samples: number; cpu_samples: number; real_samples: number }[];
      let syntheticCores = false;
      // When cpuOnly, narrow the trace_type filter at the DB level to avoid fetching Real samples
      const narrow = (sql: string) => cpuOnly ? sql.replace("trace_type IN ('CPU', 'Real')", "trace_type = 'CPU'") : sql;
      try {
        const sql = buildQuery(narrow(GET_CORE_TIMELINE), { window_seconds: windowSeconds });
        rows = await this.adapter.executeQuery(tagQuery(sql, sourceTag(TAB_ENGINE, 'coreTimeline')));
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        if (msg.includes('cpu_id') || msg.includes('Unknown expression')) {
          // cpu_id not available (e.g. ClickHouse Cloud) — use thread_id modulo core count
          syntheticCores = true;
          const coreCount = await this.estimateCoreCount();
          try {
            const noCpuSql = buildQuery(narrow(GET_CORE_TIMELINE_NO_CPU_ID), { window_seconds: windowSeconds, core_count: coreCount });
            rows = await this.adapter.executeQuery(tagQuery(noCpuSql, sourceTag(TAB_ENGINE, 'coreTimeline')));
          } catch (e2) {
            const msg2 = e2 instanceof Error ? e2.message : String(e2);
            if (msg2.includes('thread_name')) {
              const noCpuNoThreadSql = buildQuery(narrow(GET_CORE_TIMELINE_NO_CPU_ID_NO_THREAD_NAME), { window_seconds: windowSeconds, core_count: coreCount });
              rows = await this.adapter.executeQuery(tagQuery(noCpuNoThreadSql, sourceTag(TAB_ENGINE, 'coreTimeline')));
            } else {
              throw e2;
            }
          }
        } else if (msg.includes('thread_name')) {
          const fallbackSql = buildQuery(narrow(GET_CORE_TIMELINE_FALLBACK), { window_seconds: windowSeconds });
          rows = await this.adapter.executeQuery(tagQuery(fallbackSql, sourceTag(TAB_ENGINE, 'coreTimeline')));
        } else {
          throw e;
        }
      }

      if (rows.length === 0) {
        return { slots: [], coreCount: 0, windowSeconds, minTime: '', maxTime: '', totalSamples: 0, syntheticCores };
      }

      // Deduplicate: for each (core, slot) keep only the row with the most samples (dominant thread)
      // Also accumulate cpu/real sample counts across all threads in the slot
      const slotMap = new Map<string, CoreTimelineSlot>();
      const slotCpuReal = new Map<string, { cpu: number; real: number }>();
      let totalSamples = 0;

      for (const row of rows) {
        const key = `${row.core}:${row.slot}`;
        const existing = slotMap.get(key);
        const samples = Number(row.samples) || 0;
        const cpuSamples = Number(row.cpu_samples) || 0;
        const realSamples = Number(row.real_samples) || 0;
        totalSamples += samples;

        // Accumulate cpu/real across all threads in this slot
        const prev = slotCpuReal.get(key) || { cpu: 0, real: 0 };
        slotCpuReal.set(key, { cpu: prev.cpu + cpuSamples, real: prev.real + realSamples });

        if (!existing || samples > existing.samples) {
          const threadName = String(row.thread_name);
          const queryId = String(row.query_id || '');

          const { timeMs: parsedMs, timeStr } = parseTimeValue(row.slot);

          slotMap.set(key, {
            core: Number(row.core),
            time: timeStr,
            timeMs: parsedMs,
            threadName,
            isQuery: queryId !== '',
            queryId,
            pool: classifyThreadPool(threadName, queryId),
            samples,
            traceType: 'Mixed', // will be resolved below
            cpuSamples,
            realSamples,
          });
        }
      }

      // Resolve traceType per slot using accumulated totals
      for (const [key, slot] of slotMap) {
        const totals = slotCpuReal.get(key)!;
        slot.cpuSamples = totals.cpu;
        slot.realSamples = totals.real;
        if (totals.cpu > 0 && totals.real === 0) slot.traceType = 'CPU';
        else if (totals.real > 0 && totals.cpu === 0) slot.traceType = 'Real';
        else slot.traceType = 'Mixed';
      }

      const slots = Array.from(slotMap.values());
      slots.sort((a, b) => a.core - b.core || a.time.localeCompare(b.time));

      const cores = new Set(slots.map(s => s.core));
      const times = slots.map(s => s.time);

      return {
        slots,
        coreCount: cores.size,
        windowSeconds,
        minTime: times[0] || '',
        maxTime: times[times.length - 1] || '',
        totalSamples,
        syntheticCores,
      };
    } catch (err) {
      console.error('[EngineInternalsService] getCoreTimeline error:', err);
      throw err;
    }
  }

  /**
   * Estimate the number of CPU cores for the thread_id modulo fallback.
   * Tries NumberOfCPUCores from async metrics first, falls back to 16.
   */
  private async estimateCoreCount(): Promise<number> {
    try {
      const rows = await this.adapter.executeQuery<{ value: number }>(
        `SELECT value FROM system.asynchronous_metrics WHERE metric = 'NumberOfCPUCores' LIMIT 1`
      );
      if (rows.length > 0 && Number(rows[0].value) > 0) {
        return Math.min(Number(rows[0].value), 64);
      }
    } catch { /* ignore */ }
    return 16;
  }
}

/**
 * Classify a thread name into a pool category for coloring/grouping.
 * Covers both self-hosted names (e.g. "QueryPipelineEx") and ClickHouse Cloud
 * names which may use shorter abbreviations (e.g. "BgSchPool", "MrgMut").
 * When thread_name is a numeric thread_id (Cloud fallback), falls back to
 * query_id presence: threads with a query_id are classified as queries.
 */
function classifyThreadPool(threadName: string, queryId?: string): CPUSamplingByThread['pool'] {
  const name = threadName.toLowerCase();

  // If thread_name is purely numeric (thread_id fallback), use query_id heuristic
  if (/^\d+$/.test(name)) {
    return queryId ? 'queries' : 'schedule';
  }

  // Query execution threads
  if (name.includes('querypipeline') || name.includes('querypool')
    || name.includes('parallel') || name.includes('paralpars')
    || name.includes('aggr') || name.includes('hashjoin')
    || name.includes('sortingtransf') || name.includes('querythread')
    || name.includes('execute') || name.includes('interserv')) return 'queries';

  // MergeMutate is ClickHouse's shared pool for both background merges and mutations
  if ((name.includes('merge') && name.includes('mutat'))
    || name.includes('mrgmut') || name.includes('mergemut')) return 'merge_mutate';

  // Merges
  if (name.includes('merge') || name.includes('mrg')
    || name.includes('mergetree') || name.includes('bkgpool')) return 'merges';

  // Mutations
  if (name.includes('mutat') || name.includes('mut')) return 'mutations';

  // Replication
  if (name.includes('fetch') || name.includes('replic') || name.includes('repl')
    || name.includes('zookeeper') || name.includes('keeper')
    || name.includes('distrsend')) return 'replication';

  // IO threads
  if (name.includes('iopool') || name.includes('iothrd') || name.includes('io_')
    || name.includes('disk') || name.includes('readpool') || name.includes('writepool')
    || name.includes('s3') || name.includes('aio') || name.includes('asyncio')
    || name.includes('fileio') || name.includes('backgrcomm')) return 'io';

  // Scheduling / background
  if (name.includes('sched') || name.includes('bgsch') || name.includes('bkgsched')
    || name.includes('bgpool') || name.includes('backgr')) return 'schedule';

  // Connection handlers
  if (name.includes('http') || name.includes('tcp') || name.includes('handler')
    || name.includes('accept') || name.includes('grpc')
    || name.includes('mysql') || name.includes('postgre')) return 'handler';

  return 'other';
}

/**
 * Clean up demangled C++ function names to something readable.
 */
function cleanFunctionName(name: string): string {
  if (!name || name === '??') return '(unknown)';
  // Remove template parameters for readability
  let clean = name.replace(/<[^>]*>/g, '<…>');
  // Remove parameter lists
  clean = clean.replace(/\([^)]*\)/g, '()');
  // Truncate very long names
  if (clean.length > 80) clean = clean.substring(0, 77) + '...';
  return clean;
}
