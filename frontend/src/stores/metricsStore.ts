/**
 * Metrics Store - Zustand store for managing server metrics state
 * 
 * This store handles real-time server metrics including CPU, memory, and disk I/O.
 * instead of WebSocket streaming.
 * 
 */

import { create } from 'zustand';
import type {
  ServerMetrics,
  ThresholdConfig,
  MetricsCollector,
} from '@tracehouse/core';
import { useGlobalLastUpdatedStore } from './refreshSettingsStore';

// Re-export core types for consumers that import from this store
export type { ServerMetrics, ThresholdConfig };

// Default threshold values
export const DEFAULT_THRESHOLDS: ThresholdConfig = {
  cpu_warning: 80,
  memory_warning: 85,
  query_duration_warning: 60,
  parts_count_warning: 300,
};

// Warning state for each metric type
export interface MetricWarnings {
  cpu: boolean;
  memory: boolean;
  queryDuration: boolean;
  partsCount: boolean;
}

/**
 * Check if a metric value exceeds its threshold
 */
export function checkThreshold(value: number, threshold: number): boolean {
  return value > threshold;
}

/**
 * Calculate memory usage percentage
 */
export function calculateMemoryPercentage(used: number, total: number): number {
  if (total <= 0) return 0;
  return (used / total) * 100;
}

/**
 * Get all current metric warnings based on metrics and thresholds
 */
export function getMetricWarnings(
  metrics: ServerMetrics | null,
  thresholds: ThresholdConfig
): MetricWarnings {
  if (!metrics) {
    return {
      cpu: false,
      memory: false,
      queryDuration: false,
      partsCount: false,
    };
  }

  const memoryPercentage = calculateMemoryPercentage(
    metrics.memory_used,
    metrics.memory_total
  );

  return {
    cpu: checkThreshold(metrics.cpu_usage, thresholds.cpu_warning),
    memory: checkThreshold(memoryPercentage, thresholds.memory_warning),
    queryDuration: false, // Will be set by query monitoring
    partsCount: false,    // Will be set by database explorer
  };
}

// Polling status (replaces WebSocket status)
export type PollingStatus = 'stopped' | 'polling' | 'error';

// Keep WebSocketStatus as alias for backward compatibility
export type WebSocketStatus = PollingStatus | 'disconnected' | 'connecting' | 'connected';

// Legacy WebSocket message type (kept for backward compatibility)
export type WebSocketMessage =
  | { type: 'metrics'; data: ServerMetrics; timestamp: string }
  | { type: 'error'; error: string; timestamp: string }
  | { type: 'connected'; connectionId: string; timestamp: string };

import { DEFAULT_INTERVAL_MS as DEFAULT_POLLING_INTERVAL_MS } from '../services/pollingService';

// Metrics store state
interface MetricsState {
  // Current metrics
  metrics: ServerMetrics | null;
  
  // Historical metrics for trend visualization
  metricsHistory: Array<{ metrics: ServerMetrics; timestamp: Date }>;
  
  // Polling state (replaces WebSocket state)
  wsStatus: WebSocketStatus;
  wsError: string | null;
  lastUpdated: Date | null;
  
  // Configuration
  maxHistoryLength: number;
  
  thresholds: ThresholdConfig;
  
  // Current warnings based on thresholds
  warnings: MetricWarnings;
  
  // Actions
  setMetrics: (metrics: ServerMetrics) => void;
  addToHistory: (metrics: ServerMetrics) => void;
  setWsStatus: (status: WebSocketStatus) => void;
  setWsError: (error: string | null) => void;
  clearMetrics: () => void;
  clearHistory: () => void;
  setThresholds: (thresholds: Partial<ThresholdConfig>) => void;
  resetThresholds: () => void;
}

export const useMetricsStore = create<MetricsState>((set, get) => ({
  // Initial state
  metrics: null,
  metricsHistory: [],
  wsStatus: 'stopped',
  wsError: null,
  lastUpdated: null,
  maxHistoryLength: 100,
  
  thresholds: { ...DEFAULT_THRESHOLDS },
  warnings: {
    cpu: false,
    memory: false,
    queryDuration: false,
    partsCount: false,
  },

  // Set current metrics and update warnings
  setMetrics: (metrics: ServerMetrics) => {
    const { thresholds } = get();
    const warnings = getMetricWarnings(metrics, thresholds);
    
    set({ 
      metrics, 
      lastUpdated: new Date(),
      wsError: null,
      warnings,
    });
    useGlobalLastUpdatedStore.getState().touch();
  },

  // Add metrics to history
  addToHistory: (metrics: ServerMetrics) => {
    const { metricsHistory, maxHistoryLength } = get();
    const newEntry = { metrics, timestamp: new Date() };
    
    // Keep only the last maxHistoryLength entries
    const updatedHistory = [...metricsHistory, newEntry].slice(-maxHistoryLength);
    
    set({ metricsHistory: updatedHistory });
  },

  // Set polling status
  setWsStatus: (status: WebSocketStatus) => {
    set({ wsStatus: status });
  },

  // Set polling error
  setWsError: (error: string | null) => {
    set({ wsError: error, wsStatus: error ? 'error' : get().wsStatus });
  },

  // Clear current metrics
  clearMetrics: () => {
    set({ 
      metrics: null, 
      lastUpdated: null,
      warnings: {
        cpu: false,
        memory: false,
        queryDuration: false,
        partsCount: false,
      },
    });
  },

  // Clear metrics history
  clearHistory: () => {
    set({ metricsHistory: [] });
  },

  setThresholds: (newThresholds: Partial<ThresholdConfig>) => {
    const { thresholds, metrics } = get();
    const updatedThresholds = { ...thresholds, ...newThresholds };
    const warnings = getMetricWarnings(metrics, updatedThresholds);
    
    set({ 
      thresholds: updatedThresholds,
      warnings,
    });
  },

  // Reset thresholds to defaults
  resetThresholds: () => {
    const { metrics } = get();
    const warnings = getMetricWarnings(metrics, DEFAULT_THRESHOLDS);
    
    set({ 
      thresholds: { ...DEFAULT_THRESHOLDS },
      warnings,
    });
  },
}));

/**
 * MetricsPoller — polls MetricsCollector service at a configurable interval.
 * 
 * Maintains the same external API shape as the old MetricsWebSocket class
 * (connect/disconnect/isConnected) for backward compatibility.
 */
export class MetricsWebSocket {
  private intervalId: ReturnType<typeof setInterval> | null = null;
  private metricsCollector: MetricsCollector;
  private pollingIntervalMs: number;
  private isRunning = false;

  constructor(metricsCollector: MetricsCollector, pollingIntervalMs: number = DEFAULT_POLLING_INTERVAL_MS) {
    this.metricsCollector = metricsCollector;
    this.pollingIntervalMs = pollingIntervalMs;
  }

  /**
   * Start polling for metrics
   */
  connect(): void {
    if (this.isRunning) return;

    this.isRunning = true;
    const store = useMetricsStore.getState();
    store.setWsStatus('polling');
    store.setWsError(null);

    // Fetch immediately, then start interval
    this.fetchMetrics();
    this.intervalId = setInterval(() => this.fetchMetrics(), this.pollingIntervalMs);
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

    const store = useMetricsStore.getState();
    store.setWsStatus('stopped');
  }

  /**
   * Check if polling is active
   */
  isConnected(): boolean {
    return this.isRunning;
  }

  /**
   * Fetch metrics from the MetricsCollector service
   */
  private async fetchMetrics(): Promise<void> {
    const store = useMetricsStore.getState();
    try {
      const metrics = await this.metricsCollector.getServerMetrics();
      store.setMetrics(metrics);
      store.addToHistory(metrics);
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Failed to fetch metrics';
      store.setWsError(errorMessage);
    }
  }
}

export default useMetricsStore;
