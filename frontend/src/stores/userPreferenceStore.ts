/**
 * User Preference Store - Persisted UI preferences
 *
 * Stores user preferences (view mode, feature toggles, etc.) across sessions.
 */

import { create } from 'zustand';
import { persist } from 'zustand/middleware';

export type ViewMode = '3d' | '2d';

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
}

export const useUserPreferenceStore = create<UserPreferenceState>()(
  persist(
    (set, get) => ({
      preferredViewMode: '3d',
      setPreferredViewMode: (mode) => set({ preferredViewMode: mode }),
      toggleViewMode: () => set({ preferredViewMode: get().preferredViewMode === '3d' ? '2d' : '3d' }),
      killQueriesEnabled: false,
      setKillQueriesEnabled: (enabled) => set({ killQueriesEnabled: enabled }),
      experimentalEnabled: false,
      setExperimentalEnabled: (enabled) => set({ experimentalEnabled: enabled }),
    }),
    { name: 'tracehouse-view-preference' }
  )
);
