/**
 * User Preference Store - Persisted UI preferences
 *
 * Stores user preferences (view mode, feature toggles, etc.) across sessions.
 */

import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import type { QueryXRaySourcePreference } from '@tracehouse/core';

export type ViewMode = '3d' | '2d';

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
      // The old preference was one browser-wide string with no connection
      // identity, so there is nothing faithful to convert it into. Drop it and
      // start everyone on automatic, which now prefers the sampler.
      migrate: (persisted) => {
        const { xraySource: _legacy, ...state } = persisted as Record<string, unknown>;
        return { ...state, queryXraySource: {} };
      },
    }
  )
);
