/**
 * Merge Store - Zustand store for managing merge tracking state
 * 
 * This store handles active merge operations and merge history with filtering.
 * 
 */

import { create } from 'zustand';
import type {
  MergeInfo,
  MergeHistoryRecord,
  MutationInfo,
  MutationHistoryRecord,
  BackgroundPoolMetrics,
  MutationDependencyInfo,
  MergeTracker,
  MergeTextLog,
} from '@tracehouse/core';
import { useGlobalLastUpdatedStore } from './refreshSettingsStore';

// Re-export core types for consumers that import from this store
export type {
  MergeInfo,
  MergeHistoryRecord,
  MutationInfo,
  MutationHistoryRecord,
  BackgroundPoolMetrics,
  MutationDependencyInfo,
  MergeTextLog,
};

// Filter options for merge history
export interface MergeHistoryFilter {
  database?: string;
  table?: string;
  minDurationMs?: number;
  minSizeBytes?: number;
  excludeSystemDatabases?: boolean;
  /** Merge category pushed to server-side SQL (e.g. 'TTLDelete', 'Mutation'). */
  category?: string;
  /** ClickHouse interval string (e.g. '1 DAY') or 'CUSTOM:start,end'. Default '1 DAY'. */
  timeRange?: string | null;
  limit: number;
}

// Sort options for merge history
export type MergeHistorySortField = 
  | 'event_time' 
  | 'duration_ms' 
  | 'rows' 
  | 'size_in_bytes'
  | 'throughput';

export type SortDirection = 'asc' | 'desc';

export interface MergeHistorySort {
  field: MergeHistorySortField;
  direction: SortDirection;
}

// Merge statistics summary
export interface MergeStatistics {
  activeMergeCount: number;
  totalBytesBeingMerged: number;
  averageProgress: number;
  tablesWithMerges: string[];
}

// Merge store state
interface MergeState {
  activeMerges: MergeInfo[];
  
  mergeHistory: MergeHistoryRecord[];
  
  // Mutations from system.mutations
  mutations: MutationInfo[];

  // Tracks initial parts_to_do per mutation key (db.table.mutation_id) for progress calc.
  // When a mutation first appears, we snapshot parts_to_do as the denominator.
  mutationInitialParts: Map<string, number>;

  // Mutation history from system.part_log
  mutationHistory: MutationHistoryRecord[];
  
  // Background pool metrics
  poolMetrics: BackgroundPoolMetrics | null;
  
  selectedMerge: MergeInfo | null;
  
  // Filter and sort state
  historyFilter: MergeHistoryFilter;
  historySort: MergeHistorySort;
  
  // Statistics
  statistics: MergeStatistics | null;
  
  // Loading states
  isLoadingMerges: boolean;
  isLoadingHistory: boolean;
  isLoadingMutations: boolean;
  isLoadingMutationHistory: boolean;
  isLoadingPoolMetrics: boolean;
  
  // Last updated timestamp
  lastUpdated: Date | null;
  
  // Error state
  error: string | null;
  
  // Actions
  setActiveMerges: (merges: MergeInfo[]) => void;
  setMergeHistory: (history: MergeHistoryRecord[]) => void;
  setMutations: (mutations: MutationInfo[]) => void;
  setMutationHistory: (history: MutationHistoryRecord[]) => void;
  setPoolMetrics: (metrics: BackgroundPoolMetrics | null) => void;
  selectMerge: (merge: MergeInfo | null) => void;
  setHistoryFilter: (filter: Partial<MergeHistoryFilter>) => void;
  setHistorySort: (sort: MergeHistorySort) => void;
  setIsLoadingMerges: (loading: boolean) => void;
  setIsLoadingHistory: (loading: boolean) => void;
  setIsLoadingMutations: (loading: boolean) => void;
  setIsLoadingMutationHistory: (loading: boolean) => void;
  setIsLoadingPoolMetrics: (loading: boolean) => void;
  setError: (error: string | null) => void;
  clearError: () => void;
  clearAll: () => void;
}

export const useMergeStore = create<MergeState>((set) => ({
  // Initial state
  activeMerges: [],
  mergeHistory: [],
  mutations: [],
  mutationInitialParts: new Map(),
  mutationHistory: [],
  poolMetrics: null,
  selectedMerge: null,
  
  // Default filter
  historyFilter: {
    excludeSystemDatabases: true,
    timeRange: '1 HOUR',
    limit: 100,
  },
  
  // Default sort: by event time descending
  historySort: {
    field: 'event_time',
    direction: 'desc',
  },
  
  statistics: null,
  isLoadingMerges: false,
  isLoadingHistory: false,
  isLoadingMutations: false,
  isLoadingMutationHistory: false,
  isLoadingPoolMetrics: false,
  lastUpdated: null,
  error: null,

  // Actions
  setActiveMerges: (merges) => {
    // Calculate statistics
    const statistics: MergeStatistics = {
      activeMergeCount: merges.length,
      totalBytesBeingMerged: merges.reduce((sum, m) => sum + m.total_size_bytes_compressed, 0),
      averageProgress: merges.length > 0 
        ? merges.reduce((sum, m) => sum + m.progress, 0) / merges.length 
        : 0,
      tablesWithMerges: [...new Set(merges.map(m => `${m.database}.${m.table}`))],
    };
    
    set({ 
      activeMerges: merges, 
      statistics,
      lastUpdated: new Date(),
    });
    useGlobalLastUpdatedStore.getState().touch();
  },
  
  setMergeHistory: (history) => set({ mergeHistory: history }),
  
  setMutations: (mutations) => set((state) => {
    const prev = state.mutationInitialParts;
    const next = new Map(prev);
    const currentKeys = new Set<string>();

    for (const m of mutations) {
      const key = `${m.database}.${m.table}.${m.mutation_id}`;
      currentKeys.add(key);
      if (!next.has(key)) {
        // First time seeing this mutation — snapshot parts_to_do as initial total
        next.set(key, m.parts_to_do);
      }
      // Compute progress: parts completed / initial total
      const initial = next.get(key)!;
      if (initial > 0) {
        m.parts_done = initial - m.parts_to_do;
        m.total_parts = initial;
        m.progress = m.parts_done / initial;
      }
    }

    // Prune mutations that disappeared (completed)
    for (const key of prev.keys()) {
      if (!currentKeys.has(key)) next.delete(key);
    }

    return { mutations, mutationInitialParts: next };
  }),
  
  setMutationHistory: (history) => set({ mutationHistory: history }),
  
  setPoolMetrics: (metrics) => set({ poolMetrics: metrics }),
  
  selectMerge: (merge) => set({ selectedMerge: merge }),
  
  setHistoryFilter: (filter) => set((state) => ({
    historyFilter: { ...state.historyFilter, ...filter },
  })),
  
  setHistorySort: (sort) => set({ historySort: sort }),
  
  setIsLoadingMerges: (loading) => set({ isLoadingMerges: loading }),
  
  setIsLoadingHistory: (loading) => set({ isLoadingHistory: loading }),
  
  setIsLoadingMutations: (loading) => set({ isLoadingMutations: loading }),
  
  setIsLoadingMutationHistory: (loading) => set({ isLoadingMutationHistory: loading }),
  
  setIsLoadingPoolMetrics: (loading) => set({ isLoadingPoolMetrics: loading }),
  
  setError: (error) => set({ error }),
  
  clearError: () => set({ error: null }),
  
  clearAll: () => set({
    activeMerges: [],
    mergeHistory: [],
    mutations: [],
    mutationHistory: [],
    poolMetrics: null,
    selectedMerge: null,
    statistics: null,
    lastUpdated: null,
    error: null,
  }),
}));

/**
 * API functions for merge operations.
 * 
 * Each function accepts a MergeTracker service instance (from ClickHouseProvider)
 * instead of a connectionId string.
 */
export const mergeApi = {
  /**
   * Fetch active merges
   */
  async fetchActiveMerges(service: MergeTracker): Promise<MergeInfo[]> {
    return service.getActiveMerges();
  },

  /**
   * Fetch merge history with filters
   */
  async fetchMergeHistory(
    service: MergeTracker,
    filter: MergeHistoryFilter
  ): Promise<MergeHistoryRecord[]> {
    return service.getMergeHistory({
      database: filter.database,
      table: filter.table,
      minDurationMs: filter.minDurationMs,
      minSizeBytes: filter.minSizeBytes,
      excludeSystemDatabases: filter.excludeSystemDatabases,
      category: filter.category,
      timeRange: filter.timeRange,
      limit: filter.limit || 100,
    });
  },

  /**
   * Fetch mutations from system.mutations
   */
  async fetchMutations(service: MergeTracker): Promise<MutationInfo[]> {
    return service.getMutations();
  },

  /**
   * Fetch mutation history from system.part_log
   */
  async fetchMutationHistory(
    service: MergeTracker,
    filter: MergeHistoryFilter
  ): Promise<MutationHistoryRecord[]> {
    return service.getMutationHistory({
      database: filter.database,
      table: filter.table,
      minDurationMs: filter.minDurationMs,
      minSizeBytes: filter.minSizeBytes,
      excludeSystemDatabases: filter.excludeSystemDatabases,
      timeRange: filter.timeRange,
      limit: filter.limit || 100,
    });
  },

  /**
   * Fetch background pool metrics
   */
  async fetchPoolMetrics(service: MergeTracker): Promise<BackgroundPoolMetrics> {
    return service.getBackgroundPoolMetrics();
  },

  /**
   * Fetch text_log messages correlated to a merge/mutation event.
   * Uses query_id when available, falls back to time-window correlation.
   */
  async fetchMergeEventTextLogs(
    service: MergeTracker,
    record: {
      query_id?: string;
      event_time: string;
      duration_ms: number;
      database: string;
      table: string;
      part_name: string;
    },
  ): Promise<import('@tracehouse/core').MergeTextLog[]> {
    return service.getMergeEventTextLogs(record);
  },
};

// Re-export shared formatters for backward compatibility
export { formatBytes, formatBytesPerSec, formatDuration, formatDurationMs, formatNumber } from '../utils/formatters';

/**
 * Sort merge history items
 */
export function sortMergeHistory(
  items: MergeHistoryRecord[],
  sort: MergeHistorySort
): MergeHistoryRecord[] {
  return [...items].sort((a, b) => {
    let aVal: number | string;
    let bVal: number | string;
    
    switch (sort.field) {
      case 'event_time':
        aVal = new Date(a.event_time).getTime();
        bVal = new Date(b.event_time).getTime();
        break;
      case 'duration_ms':
        aVal = a.duration_ms;
        bVal = b.duration_ms;
        break;
      case 'rows':
        aVal = a.rows;
        bVal = b.rows;
        break;
      case 'size_in_bytes':
        aVal = a.size_in_bytes;
        bVal = b.size_in_bytes;
        break;
      case 'throughput':
        aVal = a.duration_ms > 0 ? a.size_in_bytes / a.duration_ms : 0;
        bVal = b.duration_ms > 0 ? b.size_in_bytes / b.duration_ms : 0;
        break;
      default:
        return 0;
    }
    
    if (sort.direction === 'asc') {
      return aVal < bVal ? -1 : aVal > bVal ? 1 : 0;
    } else {
      return aVal > bVal ? -1 : aVal < bVal ? 1 : 0;
    }
  });
}

export default useMergeStore;
