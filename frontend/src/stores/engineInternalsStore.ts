/**
 * Engine Internals Store - Zustand store for managing Engine Internals page state
 * 
 * This store handles detailed engine metrics including memory breakdown,
 * CPU core utilization, thread pools, and per-query analysis.
 */

import { create } from 'zustand';
import type {
  EngineInternalsData,
  MemoryXRay,
  MemorySubsystem,
  CPUCoreInfo,
  ThreadPoolInfo,
  PKIndexEntry,
  DictionaryInfo,
  QueryInternals,
  EngineInternalsService,
} from '@tracehouse/core';
import { useGlobalLastUpdatedStore } from './refreshSettingsStore';

// Re-export core types for consumers
export type {
  EngineInternalsData,
  MemoryXRay,
  MemorySubsystem,
  CPUCoreInfo,
  ThreadPoolInfo,
  PKIndexEntry,
  DictionaryInfo,
  QueryInternals,
};

// Polling status
export type PollingStatus = 'stopped' | 'polling' | 'error';

import { DEFAULT_INTERVAL_MS as DEFAULT_POLLING_INTERVAL_MS } from '../services/pollingService';

// Engine Internals store state
interface EngineInternalsState {
  // Data
  data: EngineInternalsData | null;
  
  // UI state
  selectedQueryId: string | null;
  expandedSubsystem: string | null;
  
  // Polling state
  pollingStatus: PollingStatus;
  lastError: string | null;
  lastUpdated: Date | null;
  
  // Actions
  setData: (data: EngineInternalsData) => void;
  setSelectedQueryId: (queryId: string | null) => void;
  setExpandedSubsystem: (id: string | null) => void;
  toggleExpandedSubsystem: (id: string) => void;
  setPollingStatus: (status: PollingStatus) => void;
  setError: (error: string | null) => void;
  clearData: () => void;
}

export const useEngineInternalsStore = create<EngineInternalsState>((set, get) => ({
  // Initial state
  data: null,
  selectedQueryId: null,
  expandedSubsystem: null,
  pollingStatus: 'stopped',
  lastError: null,
  lastUpdated: null,

  // Set engine internals data
  setData: (data: EngineInternalsData) => {
    set({
      data,
      lastUpdated: new Date(),
      lastError: null,
      pollingStatus: 'polling',
    });
    useGlobalLastUpdatedStore.getState().touch();
  },

  // Set selected query ID for detailed view
  setSelectedQueryId: (queryId: string | null) => {
    set({ selectedQueryId: queryId });
  },

  // Set expanded subsystem in memory X-ray
  setExpandedSubsystem: (id: string | null) => {
    set({ expandedSubsystem: id });
  },

  // Toggle expanded subsystem
  toggleExpandedSubsystem: (id: string) => {
    const { expandedSubsystem } = get();
    set({ expandedSubsystem: expandedSubsystem === id ? null : id });
  },

  // Set polling status
  setPollingStatus: (status: PollingStatus) => {
    set({ pollingStatus: status });
  },

  // Set error
  setError: (error: string | null) => {
    set({
      lastError: error,
      pollingStatus: error ? 'error' : get().pollingStatus,
    });
  },

  // Clear all data
  clearData: () => {
    set({
      data: null,
      lastUpdated: null,
      lastError: null,
      selectedQueryId: null,
      expandedSubsystem: null,
    });
  },
}));

/**
 * EngineInternalsPoller - polls EngineInternalsService at a configurable interval
 */
export class EngineInternalsPoller {
  private intervalId: ReturnType<typeof setInterval> | null = null;
  private engineInternalsService: EngineInternalsService;
  private pollingIntervalMs: number;
  private isRunning = false;

  constructor(
    engineInternalsService: EngineInternalsService,
    pollingIntervalMs: number = DEFAULT_POLLING_INTERVAL_MS
  ) {
    this.engineInternalsService = engineInternalsService;
    this.pollingIntervalMs = pollingIntervalMs;
  }

  /**
   * Start polling for engine internals data
   */
  start(): void {
    if (this.isRunning) return;

    this.isRunning = true;
    const store = useEngineInternalsStore.getState();
    store.setPollingStatus('polling');
    store.setError(null);

    // Fetch immediately, then start interval
    this.fetchData();
    this.intervalId = setInterval(() => this.fetchData(), this.pollingIntervalMs);
  }

  /**
   * Stop polling
   */
  stop(): void {
    this.isRunning = false;
    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
    }

    const store = useEngineInternalsStore.getState();
    store.setPollingStatus('stopped');
  }

  /**
   * Check if polling is active
   */
  isActive(): boolean {
    return this.isRunning;
  }

  /**
   * Update the service instance (e.g., when connection changes)
   */
  updateService(engineInternalsService: EngineInternalsService): void {
    this.engineInternalsService = engineInternalsService;
  }

  /**
   * Fetch data from the EngineInternalsService
   */
  private async fetchData(): Promise<void> {
    const store = useEngineInternalsStore.getState();
    try {
      const data = await this.engineInternalsService.getEngineInternalsData();
      store.setData(data);
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Failed to fetch engine internals data';
      store.setError(errorMessage);
    }
  }
}

// Re-export shared formatters for backward compatibility
export { formatBytes, formatBytesToGB, formatElapsed } from '../utils/formatters';
export { formatNumberCompact as formatNumber } from '../utils/formatters';

/**
 * Format percentage
 */
export function formatPercent(pct: number, precision: number = 1): string {
  return `${pct.toFixed(precision)}%`;
}

/**
 * Get color for CPU state
 */
export function getCpuStateColor(state: CPUCoreInfo['state']): string {
  switch (state) {
    case 'user': return '#3b82f6';    // blue-500
    case 'system': return '#ef4444';  // red-500
    case 'iowait': return '#f59e0b';  // amber-500
    case 'idle': return '#1e293b';    // slate-800
    default: return '#94a3b8';        // slate-400
  }
}

/**
 * Get label for CPU state
 */
export function getCpuStateLabel(state: CPUCoreInfo['state']): string {
  switch (state) {
    case 'user': return 'User';
    case 'system': return 'System';
    case 'iowait': return 'IO Wait';
    case 'idle': return 'Idle';
    default: return 'Unknown';
  }
}

/**
 * Calculate memory percentage of total
 */
export function calculateMemoryPercent(bytes: number, totalRSS: number): number {
  if (totalRSS <= 0) return 0;
  return (bytes / totalRSS) * 100;
}

/**
 * Get thread pool utilization percentage
 */
export function getPoolUtilization(pool: ThreadPoolInfo): number {
  if (pool.max <= 0) return 0;
  return (pool.active / pool.max) * 100;
}

export default useEngineInternalsStore;
