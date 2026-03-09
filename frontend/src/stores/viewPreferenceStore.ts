/**
 * View Preference Store - Global 2D/3D preference
 * 
 * Persists the user's preferred visualization mode across sessions.
 * Used by DatabaseExplorer, ClusterTopology, and any future 3D/2D views.
 */

import { create } from 'zustand';
import { persist } from 'zustand/middleware';

export type ViewMode = '3d' | '2d';

interface ViewPreferenceState {
  preferredViewMode: ViewMode;
  setPreferredViewMode: (mode: ViewMode) => void;
  toggleViewMode: () => void;
}

export const useViewPreferenceStore = create<ViewPreferenceState>()(
  persist(
    (set, get) => ({
      preferredViewMode: '3d',
      setPreferredViewMode: (mode) => set({ preferredViewMode: mode }),
      toggleViewMode: () => set({ preferredViewMode: get().preferredViewMode === '3d' ? '2d' : '3d' }),
    }),
    { name: 'tracehouse-view-preference' }
  )
);
