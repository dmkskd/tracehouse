/**
 * Environment Store — global Zustand store for the detected runtime environment.
 *
 * Probed once when a connection is established, then available to all
 * pages/components for consistent container/cgroup-aware rendering.
 */

import { create } from 'zustand';
import type { EnvironmentInfo } from '@tracehouse/core';

interface EnvironmentState {
  /** Detected environment info, or null if not yet probed. */
  info: EnvironmentInfo | null;
  /** True while the initial detection is in progress. */
  probing: boolean;
  /** Set the detected environment. */
  setEnvironment: (info: EnvironmentInfo) => void;
  /** Reset on disconnect. */
  reset: () => void;
  /** Mark probing in progress. */
  setProbing: (v: boolean) => void;
}

export const useEnvironmentStore = create<EnvironmentState>((set) => ({
  info: null,
  probing: false,
  setEnvironment: (info) => set({ info, probing: false }),
  reset: () => set({ info: null, probing: false }),
  setProbing: (v) => set({ probing: v }),
}));
