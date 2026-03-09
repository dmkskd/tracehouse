/**
 * Query Store - Zustand store for managing query monitoring state
 * 
 * This store handles running queries and query history with filtering/sorting.
 * 
 */

import { create } from 'zustand';
import type {
  QueryMetrics,
  QueryHistoryItem,
  QueryAnalyzer,
} from '@tracehouse/core';
import { useGlobalLastUpdatedStore } from './refreshSettingsStore';
import { DEFAULT_INTERVAL_MS } from '../services/pollingService';

// Re-export core types for consumers that import from this store.
// RunningQuery is an alias for QueryMetrics (core name) to preserve
// backward compatibility with existing component code.
export type RunningQuery = QueryMetrics;
export type { QueryHistoryItem };

/** Default history lookback window in milliseconds (1 hour). */
export const DEFAULT_HISTORY_WINDOW_MS = 1 * 60 * 60 * 1000;

// Filter options for query history
export interface QueryHistoryFilter {
  startTime?: string;
  endTime?: string;
  user?: string;
  queryId?: string;
  queryText?: string;
  minDurationMs?: number;
  minMemoryBytes?: number;
  hostname?: string;
  limit?: number;
  excludeAppQueries?: boolean;
  queryKind?: string;
  status?: string;
  database?: string;
  table?: string;
}

// Sort options for query history
export type SortField = 
  | 'query_start_time' 
  | 'query_duration_ms' 
  | 'read_rows' 
  | 'read_bytes' 
  | 'result_rows' 
  | 'memory_usage'
  | 'efficiency_score';

export type SortDirection = 'asc' | 'desc';

export interface QueryHistorySort {
  field: SortField;
  direction: SortDirection;
}

// WebSocket connection status (kept for backward compatibility)
export type WebSocketStatus = 'disconnected' | 'connecting' | 'connected' | 'error';

// Legacy WebSocket message types (kept for backward compatibility)
export type QueryWebSocketMessage =
  | { type: 'queries'; data: { queries: RunningQuery[] } | RunningQuery[]; timestamp: string }
  | { type: 'error'; error: string; timestamp: string }
  | { type: 'connected'; connectionId: string; timestamp: string };

// Query store state
interface QueryState {
  runningQueries: RunningQuery[];
  
  queryHistory: QueryHistoryItem[];
  
  selectedQuery: RunningQuery | QueryHistoryItem | null;
  selectedQueryType: 'running' | 'history' | null;
  
  historyFilter: QueryHistoryFilter;
  historySort: QueryHistorySort;
  
  // Polling/connection state (kept for backward compatibility)
  wsStatus: WebSocketStatus;
  wsError: string | null;
  lastUpdated: Date | null;
  
  // Loading states
  isLoadingHistory: boolean;
  isKillingQuery: boolean;
  
  // Error state
  error: string | null;
  
  // Actions
  setRunningQueries: (queries: RunningQuery[]) => void;
  setQueryHistory: (history: QueryHistoryItem[]) => void;
  selectQuery: (query: RunningQuery | QueryHistoryItem | null, type: 'running' | 'history' | null) => void;
  setHistoryFilter: (filter: Partial<QueryHistoryFilter>) => void;
  setHistorySort: (sort: QueryHistorySort) => void;
  setWsStatus: (status: WebSocketStatus) => void;
  setWsError: (error: string | null) => void;
  setIsLoadingHistory: (loading: boolean) => void;
  setIsKillingQuery: (killing: boolean) => void;
  setError: (error: string | null) => void;
  clearError: () => void;
  clearQueries: () => void;
}

export const useQueryStore = create<QueryState>((set) => ({
  // Initial state
  runningQueries: [],
  queryHistory: [],
  selectedQuery: null,
  selectedQueryType: null,
  
  // Default filter: last 1 hour, limit 100
  historyFilter: {
    limit: 100,
  },
  
  // Default sort: by start time descending
  historySort: {
    field: 'query_start_time',
    direction: 'desc',
  },
  
  wsStatus: 'connecting',
  wsError: null,
  lastUpdated: null,
  isLoadingHistory: false,
  isKillingQuery: false,
  error: null,

  // Actions
  setRunningQueries: (queries) => {
    set({ 
      runningQueries: queries, 
      lastUpdated: new Date(),
      wsError: null,
    });
    useGlobalLastUpdatedStore.getState().touch();
  },
  
  setQueryHistory: (history) => {
    set({ queryHistory: history });
    useGlobalLastUpdatedStore.getState().touch();
  },
  
  selectQuery: (query, type) => set({ 
    selectedQuery: query, 
    selectedQueryType: type,
  }),
  
  setHistoryFilter: (filter) => set((state) => ({
    historyFilter: { ...state.historyFilter, ...filter },
  })),
  
  setHistorySort: (sort) => set({ historySort: sort }),
  
  setWsStatus: (status) => set({ wsStatus: status }),
  
  setWsError: (error) => set({ 
    wsError: error, 
    wsStatus: error ? 'error' : 'disconnected',
  }),
  
  setIsLoadingHistory: (loading) => set({ isLoadingHistory: loading }),
  
  setIsKillingQuery: (killing) => set({ isKillingQuery: killing }),
  
  setError: (error) => set({ error }),
  
  clearError: () => set({ error: null }),
  
  clearQueries: () => set({
    runningQueries: [],
    queryHistory: [],
    selectedQuery: null,
    selectedQueryType: null,
    lastUpdated: null,
  }),
}));

/**
 * QueryWebSocket — polls QueryAnalyzer service for running queries.
 * Replaces the old WebSocket-based approach with polling via the service layer.
 * Maintains the same external API (connect/disconnect/isConnected) for
 * backward compatibility.
 */
export class QueryWebSocket {
  private intervalId: ReturnType<typeof setInterval> | null = null;
  private queryAnalyzer: QueryAnalyzer;
  private pollingIntervalMs: number;
  private isRunning = false;

  constructor(queryAnalyzer: QueryAnalyzer, pollingIntervalMs: number = DEFAULT_INTERVAL_MS) {
    this.queryAnalyzer = queryAnalyzer;
    this.pollingIntervalMs = pollingIntervalMs;
  }

  /**
   * Start polling for running queries
   */
  connect(): void {
    if (this.isRunning) return;

    this.isRunning = true;
    const store = useQueryStore.getState();
    store.setWsStatus('connected');
    store.setWsError(null);

    // Fetch immediately, then start interval
    this.fetchQueries();
    this.intervalId = setInterval(() => this.fetchQueries(), this.pollingIntervalMs);
  }

  /**
   * Stop polling
   */
  disconnect(): void {
    this.isRunning = false;
    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
    }

    const store = useQueryStore.getState();
    store.setWsStatus('disconnected');
  }

  /**
   * Check if polling is active
   */
  isConnected(): boolean {
    return this.isRunning;
  }

  /**
   * Fetch running queries from the QueryAnalyzer service
   */
  private async fetchQueries(): Promise<void> {
    const store = useQueryStore.getState();
    try {
      const queries = await this.queryAnalyzer.getRunningQueries();
      store.setRunningQueries(queries);
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Failed to fetch queries';
      store.setWsError(errorMessage);
    }
  }
}

/**
 * API functions for query operations.
 * 
 * Each function accepts a QueryAnalyzer service instance (from ClickHouseProvider)
 * instead of a connectionId string.
 */
export const queryApi = {
  /**
   * Fetch running queries
   */
  async fetchRunningQueries(service: QueryAnalyzer): Promise<RunningQuery[]> {
    return service.getRunningQueries();
  },

  /**
   * Fetch query history with filters
   */
  async fetchQueryHistory(
    service: QueryAnalyzer,
    filter: QueryHistoryFilter
  ): Promise<QueryHistoryItem[]> {
    // Build date/time range from filter or use sensible defaults
    const now = new Date();
    const startDate = filter.startTime
      ? filter.startTime.split('T')[0] ?? now.toISOString().split('T')[0]
      : new Date(now.getTime() - DEFAULT_HISTORY_WINDOW_MS).toISOString().split('T')[0];
    const startTime = filter.startTime ?? new Date(now.getTime() - DEFAULT_HISTORY_WINDOW_MS).toISOString();
    const endTime = filter.endTime ?? now.toISOString();

    return service.getQueryHistory({
      start_date: startDate!,
      start_time: startTime,
      end_time: endTime,
      limit: filter.limit ?? 100,
      user: filter.user,
      query_id: filter.queryId,
      query_text: filter.queryText,
      min_duration_ms: filter.minDurationMs,
      min_memory_bytes: filter.minMemoryBytes,
      exclude_app_queries: filter.excludeAppQueries,
      query_kind: filter.queryKind,
      status: filter.status,
      database: filter.database,
      table: filter.table,
    });
  },

  /**
   * Kill a running query
   */
  async killQuery(service: QueryAnalyzer, queryId: string): Promise<{ success: boolean; message: string }> {
    try {
      await service.killQuery(queryId);
      return { success: true, message: 'Query killed successfully' };
    } catch (error) {
      return { success: false, message: error instanceof Error ? error.message : 'Unknown error' };
    }
  },
};

// Re-export shared formatters for backward compatibility
export { formatBytes, formatDuration } from '../utils/formatters';

/**
 * Format number with thousands separator
 */
export function formatNumber(num: number): string {
  return num.toLocaleString();
}

/**
 * Sort query history items
 */
export function sortQueryHistory(
  items: QueryHistoryItem[],
  sort: QueryHistorySort
): QueryHistoryItem[] {
  return [...items].sort((a, b) => {
    let aVal: number | string;
    let bVal: number | string;
    
    switch (sort.field) {
      case 'query_start_time':
        aVal = new Date(a.query_start_time).getTime();
        bVal = new Date(b.query_start_time).getTime();
        break;
      case 'query_duration_ms':
        aVal = a.query_duration_ms;
        bVal = b.query_duration_ms;
        break;
      case 'read_rows':
        aVal = a.read_rows;
        bVal = b.read_rows;
        break;
      case 'read_bytes':
        aVal = a.read_bytes;
        bVal = b.read_bytes;
        break;
      case 'result_rows':
        aVal = a.result_rows;
        bVal = b.result_rows;
        break;
      case 'memory_usage':
        aVal = a.memory_usage;
        bVal = b.memory_usage;
        break;
      case 'efficiency_score':
        aVal = a.efficiency_score ?? -1;
        bVal = b.efficiency_score ?? -1;
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

export default useQueryStore;
