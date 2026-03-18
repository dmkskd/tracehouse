/**
 * Trace Store - Zustand store for managing query tracing state
 * 
 * This store handles trace logs, EXPLAIN results, and OpenTelemetry spans
 * for query debugging and analysis.
 * 
 */

import { create } from 'zustand';
import { formatMicroseconds } from '../utils/formatters';

// Import types from core for use in this file
import type { TraceLog, ExplainType, ExplainResult, OpenTelemetrySpan } from '@tracehouse/core';

// Re-export types from core for convenience
export type { TraceLog, ExplainType, ExplainResult, OpenTelemetrySpan } from '@tracehouse/core';

// Log-plan correlation result
export interface CorrelationResult {
  query_id: string;
  explain_type: string;
  logs: TraceLog[];
  plan: Record<string, unknown> | null;
  correlations: Array<{
    log_index: number;
    stage: string;
    timestamp: string;
  }>;
  timeline: Array<{
    type: 'log' | 'stage';
    timestamp: string;
    content: string;
  }>;
  summary: {
    total_logs: number;
    correlated_logs: number;
    stages_count: number;
  };
}

// Filter options for trace logs
export interface TraceLogFilter {
  logLevels?: string[];
}

// Valid log levels
export const VALID_LOG_LEVELS = [
  'Fatal',
  'Critical',
  'Error',
  'Warning',
  'Notice',
  'Information',
  'Debug',
  'Trace',
] as const;

export type LogLevel = typeof VALID_LOG_LEVELS[number];

// Trace store state
interface TraceState {
  selectedQueryId: string | null;
  selectedQuery: string | null;
  
  traceLogs: TraceLog[];
  
  explainResult: ExplainResult | null;
  selectedExplainType: ExplainType;
  
  // OpenTelemetry spans
  openTelemetrySpans: OpenTelemetrySpan[];
  
  // Correlation result
  correlationResult: CorrelationResult | null;
  
  // Filter state
  logFilter: TraceLogFilter;
  
  // Loading states
  isLoadingLogs: boolean;
  isLoadingExplain: boolean;
  isLoadingSpans: boolean;
  isLoadingCorrelation: boolean;
  
  // Error state
  error: string | null;
  
  // Actions
  setSelectedQueryId: (queryId: string | null) => void;
  setSelectedQuery: (query: string | null) => void;
  setTraceLogs: (logs: TraceLog[]) => void;
  setExplainResult: (result: ExplainResult | null) => void;
  setSelectedExplainType: (type: ExplainType) => void;
  setOpenTelemetrySpans: (spans: OpenTelemetrySpan[]) => void;
  setCorrelationResult: (result: CorrelationResult | null) => void;
  setLogFilter: (filter: Partial<TraceLogFilter>) => void;
  setIsLoadingLogs: (loading: boolean) => void;
  setIsLoadingExplain: (loading: boolean) => void;
  setIsLoadingSpans: (loading: boolean) => void;
  setIsLoadingCorrelation: (loading: boolean) => void;
  setError: (error: string | null) => void;
  clearError: () => void;
  clearTrace: () => void;
  clearAll: () => void;
}

export const useTraceStore = create<TraceState>((set) => ({
  // Initial state
  selectedQueryId: null,
  selectedQuery: null,
  traceLogs: [],
  explainResult: null,
  selectedExplainType: 'PLAN',
  openTelemetrySpans: [],
  correlationResult: null,
  
  // Default filter: all log levels
  logFilter: {},
  
  isLoadingLogs: false,
  isLoadingExplain: false,
  isLoadingSpans: false,
  isLoadingCorrelation: false,
  error: null,

  // Actions
  setSelectedQueryId: (queryId) => set({ 
    selectedQueryId: queryId,
    // Clear previous trace data when query changes
    traceLogs: [],
    explainResult: null,
    openTelemetrySpans: [],
    correlationResult: null,
    error: null,
  }),
  
  setSelectedQuery: (query) => set({ selectedQuery: query }),
  
  setTraceLogs: (logs) => set({ traceLogs: logs }),
  
  setExplainResult: (result) => set({ explainResult: result }),
  
  setSelectedExplainType: (type) => set({ selectedExplainType: type }),
  
  setOpenTelemetrySpans: (spans) => set({ openTelemetrySpans: spans }),
  
  setCorrelationResult: (result) => set({ correlationResult: result }),
  
  setLogFilter: (filter) => set((state) => ({
    logFilter: { ...state.logFilter, ...filter },
  })),
  
  setIsLoadingLogs: (loading) => set({ isLoadingLogs: loading }),
  
  setIsLoadingExplain: (loading) => set({ isLoadingExplain: loading }),
  
  setIsLoadingSpans: (loading) => set({ isLoadingSpans: loading }),
  
  setIsLoadingCorrelation: (loading) => set({ isLoadingCorrelation: loading }),
  
  setError: (error) => set({ error }),
  
  clearError: () => set({ error: null }),
  
  clearTrace: () => set({
    traceLogs: [],
    explainResult: null,
    openTelemetrySpans: [],
    correlationResult: null,
    error: null,
  }),
  
  clearAll: () => set({
    selectedQueryId: null,
    selectedQuery: null,
    traceLogs: [],
    explainResult: null,
    selectedExplainType: 'PLAN',
    openTelemetrySpans: [],
    correlationResult: null,
    logFilter: {},
    error: null,
  }),
}));

// Note: API functions have been migrated to @tracehouse/core TraceService
// Use services.traceService from ClickHouseProvider instead

/**
 * Format timestamp for display
 */
export function formatTimestamp(timestamp: string): string {
  try {
    const date = new Date(timestamp);
    return date.toLocaleTimeString('en-US', {
      hour12: false,
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      fractionalSecondDigits: 3,
    });
  } catch {
    return timestamp;
  }
}

/**
 * Format duration in microseconds to human-readable string
 */
export const formatDurationUs = formatMicroseconds;

/**
 * Get log level color class
 */
export function getLogLevelColor(level: string): string {
  switch (level.toLowerCase()) {
    case 'fatal':
    case 'critical':
      return 'text-red-700 bg-red-100 dark:text-red-300 dark:bg-red-900/30';
    case 'error':
      return 'text-red-600 bg-red-50 dark:text-red-400 dark:bg-red-900/20';
    case 'warning':
      return 'text-yellow-600 bg-yellow-50 dark:text-yellow-400 dark:bg-yellow-900/20';
    case 'notice':
      return 'text-blue-600 bg-blue-50 dark:text-blue-400 dark:bg-blue-900/20';
    case 'information':
      return 'text-green-600 bg-green-50 dark:text-green-400 dark:bg-green-900/20';
    case 'debug':
      return 'text-gray-600 bg-gray-50 dark:text-gray-400 dark:bg-gray-800';
    case 'trace':
      return 'text-gray-500 bg-gray-50 dark:text-gray-500 dark:bg-gray-800';
    default:
      return 'text-gray-600 bg-gray-50 dark:text-gray-400 dark:bg-gray-800';
  }
}

/**
 * Get log level badge color class
 */
export function getLogLevelBadgeColor(level: string): string {
  switch (level.toLowerCase()) {
    case 'fatal':
    case 'critical':
      return 'bg-red-600 text-white';
    case 'error':
      return 'bg-red-500 text-white';
    case 'warning':
      return 'bg-yellow-500 text-white';
    case 'notice':
      return 'bg-blue-500 text-white';
    case 'information':
      return 'bg-green-500 text-white';
    case 'debug':
      return 'bg-gray-500 text-white';
    case 'trace':
      return 'bg-gray-400 text-white';
    default:
      return 'bg-gray-500 text-white';
  }
}

/**
 * Filter trace logs by log levels
 */
export function filterTraceLogs(
  logs: TraceLog[],
  filter: TraceLogFilter
): TraceLog[] {
  if (!filter.logLevels || filter.logLevels.length === 0) {
    return logs;
  }
  
  const normalizedLevels = filter.logLevels.map(l => l.toLowerCase());
  return logs.filter(log => normalizedLevels.includes(log.level.toLowerCase()));
}

export default useTraceStore;
