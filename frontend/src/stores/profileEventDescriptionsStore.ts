/**
 * Profile Event Descriptions Store
 *
 * Caches human-readable descriptions for ClickHouse ProfileEvents,
 * fetched once from system.events at connection time.
 */

import { create } from 'zustand';

interface ProfileEventDescriptionsState {
  /** Map of event name → description (empty until fetched) */
  descriptions: Record<string, string>;
  /** Whether descriptions have been fetched for the current connection */
  loaded: boolean;

  // Actions
  setDescriptions: (descriptions: Record<string, string>) => void;
  reset: () => void;
}

export const useProfileEventDescriptionsStore = create<ProfileEventDescriptionsState>((set) => ({
  descriptions: {},
  loaded: false,

  setDescriptions: (descriptions: Record<string, string>) => {
    set((state) => ({ descriptions: { ...state.descriptions, ...descriptions }, loaded: true }));
  },

  reset: () => {
    set({ descriptions: {}, loaded: false });
  },
}));
