/**
 * Refresh Settings Store - Global user-selected refresh rate
 * 
 * Persists the user's chosen polling interval across sessions.
 * All pollers read from this store instead of hardcoding intervals.
 * In Grafana mode, the allowed rates come from admin config via RefreshConfigContext.
 */

import { create } from 'zustand';
import { persist } from 'zustand/middleware';

interface RefreshSettingsState {
  /** User-selected refresh rate in seconds (0 = paused) */
  refreshRateSeconds: number;
  setRefreshRate: (seconds: number) => void;
}

export const useRefreshSettingsStore = create<RefreshSettingsState>()(
  persist(
    (set) => ({
      refreshRateSeconds: 5,
      setRefreshRate: (seconds) => set({ refreshRateSeconds: seconds }),
    }),
    { name: 'tracehouse-refresh-settings' }
  )
);

/**
 * Global Last Updated Store - single source of truth for "last data refresh" timestamp.
 * Every poller calls `touch()` when it receives fresh data.
 * The header reads `lastUpdated` to show a consistent indicator.
 */

type PollingStatusGlobal = 'idle' | 'polling' | 'error';

interface GlobalLastUpdatedState {
  lastUpdated: Date | null;
  status: PollingStatusGlobal;
  /** Monotonic counter — bump to trigger an immediate refresh across all pollers */
  manualRefreshTick: number;
  touch: () => void;
  setStatus: (s: PollingStatusGlobal) => void;
  reset: () => void;
  /** Call from the header refresh button to force all pollers to re-fetch now */
  triggerManualRefresh: () => void;
}

export const useGlobalLastUpdatedStore = create<GlobalLastUpdatedState>((set, get) => ({
  lastUpdated: null,
  status: 'idle',
  manualRefreshTick: 0,
  touch: () => set({ lastUpdated: new Date(), status: 'polling' }),
  setStatus: (status) => set({ status }),
  reset: () => set({ lastUpdated: null, status: 'idle' }),
  triggerManualRefresh: () => set({ manualRefreshTick: get().manualRefreshTick + 1 }),
}));
