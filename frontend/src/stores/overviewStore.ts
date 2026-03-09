/**
 * Overview Store - Zustand store for managing Overview page state
 * 
 * This store handles real-time resource attribution and active operations monitoring.
 * Uses polling-based data refresh with tiered intervals.
 */

import { create } from 'zustand';
import type {
  OverviewData,
  ResourceAttribution,
  RunningQueryInfo,
  ActiveMergeInfo,
  MutationInfo,
  AlertInfo,
  OverviewService,
} from '@tracehouse/core';
import { useGlobalLastUpdatedStore } from './refreshSettingsStore';

// Re-export core types for consumers
export type {
  OverviewData,
  ResourceAttribution,
  RunningQueryInfo,
  ActiveMergeInfo,
  MutationInfo,
  AlertInfo,
};

// Resource view type for the attribution bar
export type ResourceViewType = 'cpu' | 'memory' | 'io';

// Polling status
export type PollingStatus = 'stopped' | 'polling' | 'error';

import { DEFAULT_INTERVAL_MS as DEFAULT_POLLING_INTERVAL_MS } from '../services/pollingService';

// Max snapshots to keep for sparkline history
const MAX_ATTRIBUTION_HISTORY = 30;

// A lightweight snapshot of cluster-wide resource values (for sparklines)
export interface AttributionSnapshot {
  cpuPct: number;
  memoryPct: number;
  ioReadBps: number;
  ioWriteBps: number;
}

// Overview store state
interface OverviewState {
  // Data
  data: OverviewData | null;
  /** Rolling history of cluster-wide resource snapshots (most recent last) */
  attributionHistory: AttributionSnapshot[];

  // UI state
  selectedResource: ResourceViewType;
  expandedQueryId: string | null;

  // Polling state
  pollingStatus: PollingStatus;
  lastError: string | null;
  lastUpdated: Date | null;

  // Actions
  setData: (data: OverviewData) => void;
  setSelectedResource: (resource: ResourceViewType) => void;
  setExpandedQueryId: (queryId: string | null) => void;
  toggleExpandedQuery: (queryId: string) => void;
  setPollingStatus: (status: PollingStatus) => void;
  setError: (error: string | null) => void;
  clearData: () => void;
}

export const useOverviewStore = create<OverviewState>((set, get) => ({
  // Initial state
  data: null,
  attributionHistory: [],
  selectedResource: 'cpu',
  expandedQueryId: null,
  pollingStatus: 'stopped',
  lastError: null,
  lastUpdated: null,

  // Set overview data
  setData: (data: OverviewData) => {
    const ra = data.resourceAttribution;
    const memPct = ra.memory.totalRAM > 0
      ? (ra.memory.totalRSS / ra.memory.totalRAM) * 100
      : 0;
    const snapshot: AttributionSnapshot = {
      cpuPct: ra.cpu.totalPct,
      memoryPct: memPct,
      ioReadBps: ra.io.readBytesPerSec,
      ioWriteBps: ra.io.writeBytesPerSec,
    };
    const prev = get().attributionHistory;
    const history = prev.length >= MAX_ATTRIBUTION_HISTORY
      ? [...prev.slice(1), snapshot]
      : [...prev, snapshot];
    set({
      data,
      attributionHistory: history,
      lastUpdated: new Date(),
      lastError: null,
      pollingStatus: 'polling',
    });
    useGlobalLastUpdatedStore.getState().touch();
  },

  // Set selected resource type for attribution bar
  setSelectedResource: (resource: ResourceViewType) => {
    set({ selectedResource: resource });
  },

  // Set expanded query ID
  setExpandedQueryId: (queryId: string | null) => {
    set({ expandedQueryId: queryId });
  },

  // Toggle expanded query
  toggleExpandedQuery: (queryId: string) => {
    const { expandedQueryId } = get();
    set({ expandedQueryId: expandedQueryId === queryId ? null : queryId });
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
      attributionHistory: [],
      lastUpdated: null,
      lastError: null,
      expandedQueryId: null,
    });
  },
}));

/**
 * OverviewPoller - polls OverviewService at a configurable interval
 */
export class OverviewPoller {
  private intervalId: ReturnType<typeof setInterval> | null = null;
  private overviewService: OverviewService;
  private pollingIntervalMs: number;
  private isRunning = false;

  constructor(
    overviewService: OverviewService,
    pollingIntervalMs: number = DEFAULT_POLLING_INTERVAL_MS
  ) {
    this.overviewService = overviewService;
    this.pollingIntervalMs = pollingIntervalMs;
  }

  /**
   * Start polling for overview data
   */
  start(): void {
    if (this.isRunning) return;

    this.isRunning = true;
    const store = useOverviewStore.getState();
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

    const store = useOverviewStore.getState();
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
  updateService(overviewService: OverviewService): void {
    this.overviewService = overviewService;
  }

  /**
   * Fetch data from the OverviewService
   */
  private async fetchData(): Promise<void> {
    const store = useOverviewStore.getState();
    try {
      const data = await this.overviewService.getOverviewData();
      store.setData(data);
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Failed to fetch overview data';
      store.setError(errorMessage);
    }
  }
}

// Re-export shared formatters for backward compatibility
export { formatBytes, formatBytesPerSec, formatElapsed } from '../utils/formatters';

/**
 * Format number with K/M/B suffixes
 */
export function formatNumber(num: number): string {
  if (num < 1000) return num.toString();
  if (num < 1_000_000) return `${(num / 1000).toFixed(1)}K`;
  if (num < 1_000_000_000) return `${(num / 1_000_000).toFixed(1)}M`;
  return `${(num / 1_000_000_000).toFixed(1)}B`;
}

/**
 * Format CPU cores to string
 */
export function formatCpuCores(cores: number): string {
  return cores.toFixed(2);
}

/**
 * Format percentage
 */
export function formatPercent(pct: number): string {
  return `${pct.toFixed(1)}%`;
}

/**
 * Get color for alert severity
 */
export function getAlertColor(severity: AlertInfo['severity']): string {
  return severity === 'crit' ? '#ef4444' : '#f59e0b';
}

/**
 * Get background color for alert severity
 */
export function getAlertBgColor(severity: AlertInfo['severity']): string {
  return severity === 'crit' ? 'rgba(239, 68, 68, 0.1)' : 'rgba(245, 158, 11, 0.1)';
}

export default useOverviewStore;
