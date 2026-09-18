/**
 * User Preference Store - Persisted UI preferences
 *
 * Stores user preferences (view mode, feature toggles, etc.) across sessions.
 */

import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import type { QueryXRaySourcePreference } from '@tracehouse/core';

export type ViewMode = '3d' | '2d';

/**
 * localStorage keys owned by OTHER stores, read once by the v0 -> v1 migration
 * below to discover which connections an old browser-wide pin applied to.
 * Named here so a rename in those stores is at least greppable from this one.
 */
const CONNECTIONS_STORAGE_KEY = 'tracehouse-connections';
const DATASOURCE_STORAGE_KEY = 'tracehouse-datasource';

/**
 * Read and parse a foreign persisted store.
 *
 * Absent is normal and returns null quietly: a browser with no saved profiles,
 * or standalone mode with no Grafana datasource. Present but unreadable is NOT
 * normal, and is reported: it means a pin the user chose is about to be dropped
 * on the floor, and silence there is what makes that impossible to diagnose.
 */
function readPersistedJson(key: string): Record<string, unknown> | null {
  let raw: string | null;
  try {
    raw = localStorage.getItem(key);
  } catch (e) {
    console.warn(`[userPreferenceStore] localStorage unavailable, ${key} not read for xraySource migration:`, e);
    return null;
  }
  if (raw === null) return null;
  try {
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === 'object' ? parsed as Record<string, unknown> : null;
  } catch (e) {
    console.warn(`[userPreferenceStore] ${key} is not valid JSON, xraySource pin not migrated to it:`, e);
    return null;
  }
}
export type NavigatorShapeMode = 'trend' | 'peaks' | 'change';

interface UserPreferenceState {
  preferredViewMode: ViewMode;
  setPreferredViewMode: (mode: ViewMode) => void;
  toggleViewMode: () => void;
  /** Allow killing queries from the Active Queries view. Off by default. */
  killQueriesEnabled: boolean;
  setKillQueriesEnabled: (enabled: boolean) => void;
  /** Enable experimental features. Off by default. */
  experimentalEnabled: boolean;
  setExperimentalEnabled: (enabled: boolean) => void;
  /** Hide replica merges (same merge on multiple replicas). Off by default — replica merges are shown. */
  hideReplicaMerges: boolean;
  setHideReplicaMerges: (enabled: boolean) => void;
  /** Show event annotations on the Time Travel 2D chart and navigator. */
  timeTravelEventsVisible: boolean;
  setTimeTravelEventsVisible: (visible: boolean) => void;
  /** Time Travel navigator rollup shape. */
  timeTravelNavigatorShape: NavigatorShapeMode;
  setTimeTravelNavigatorShape: (shape: NavigatorShapeMode) => void;
  /**
   * Which table the Query X-Ray reads. 'auto' lets selectQueryXRaySource() decide
   * from what the connection has and whether the query is still running.
   */
  queryXraySource: Record<string, QueryXRaySourcePreference>;
  setQueryXraySource: (connectionId: string, source: QueryXRaySourcePreference | undefined) => void;
}

export const useUserPreferenceStore = create<UserPreferenceState>()(
  persist(
    (set, get) => ({
      preferredViewMode: '3d',
      setPreferredViewMode: (mode) => set({ preferredViewMode: mode }),
      toggleViewMode: () => set({ preferredViewMode: get().preferredViewMode === '3d' ? '2d' : '3d' }),
      killQueriesEnabled: false,
      setKillQueriesEnabled: (enabled) => set({ killQueriesEnabled: enabled }),
      experimentalEnabled: true,
      setExperimentalEnabled: (enabled) => set({ experimentalEnabled: enabled }),
      hideReplicaMerges: false,
      setHideReplicaMerges: (enabled: boolean) => set({ hideReplicaMerges: enabled }),
      timeTravelEventsVisible: true,
      setTimeTravelEventsVisible: (visible: boolean) => set({ timeTravelEventsVisible: visible }),
      timeTravelNavigatorShape: 'peaks',
      setTimeTravelNavigatorShape: (shape: NavigatorShapeMode) => set({ timeTravelNavigatorShape: shape }),
      queryXraySource: {},
      setQueryXraySource: (connectionId, source) => set(state => {
        const queryXraySource = { ...state.queryXraySource };
        if (source === undefined) delete queryXraySource[connectionId];
        else queryXraySource[connectionId] = source;
        return { queryXraySource };
      }),
    }),
    {
      name: 'tracehouse-view-preference',
      version: 1,
      migrate: (persisted) => {
        const { xraySource, ...state } = persisted as Record<string, unknown>;
        // The legacy preference had no connection identity. Preserve it for
        // profiles already present, without imposing it on future connections.
        const queryXraySource: Record<string, QueryXRaySourcePreference> = {};
        if (xraySource === 'processes_history' || xraySource === 'query_metric_log') {
          const connections = readPersistedJson(CONNECTIONS_STORAGE_KEY);
          const profiles = (connections?.state as { profiles?: unknown } | undefined)?.profiles;
          if (Array.isArray(profiles)) {
            for (const profile of profiles) {
              const id = (profile as { id?: unknown } | null)?.id;
              if (typeof id === 'string' && id) queryXraySource[id] = xraySource;
            }
          } else if (connections !== null) {
            console.warn(`[userPreferenceStore] ${CONNECTIONS_STORAGE_KEY} has no profiles array, xraySource pin not migrated to saved profiles`);
          }

          const datasource = readPersistedJson(DATASOURCE_STORAGE_KEY);
          const uid = datasource?.uid;
          if (typeof uid === 'string' && uid) queryXraySource[uid] = xraySource;
          else if (datasource !== null) {
            console.warn(`[userPreferenceStore] ${DATASOURCE_STORAGE_KEY} has no uid, xraySource pin not migrated to the Grafana datasource`);
          }
        }
        return { ...state, queryXraySource };
      },
    }
  )
);
