/**
 * Cluster Store — stores detected cluster topology info.
 * Populated on connection by ClickHouseProvider.
 */

import { create } from 'zustand';
import type { ClusterInfo, AvailableCluster } from '@tracehouse/core';

interface ClusterState extends ClusterInfo {
  /** Whether detection has completed */
  detected: boolean;
  /** Set cluster info after detection */
  setCluster: (info: ClusterInfo) => void;
  /** Switch to a different cluster (or null for single-node) */
  switchCluster: (name: string | null) => void;
  /** Reset on disconnect */
  reset: () => void;
}

export const useClusterStore = create<ClusterState>()((set, get) => ({
  clusterName: null,
  replicaCount: 1,
  shardCount: 1,
  availableClusters: [],
  detected: false,
  setCluster: (info) => set({ ...info, detected: true }),
  switchCluster: (name) => {
    if (name === null) {
      set({ clusterName: null, replicaCount: 1, shardCount: 1 });
      return;
    }
    const match = get().availableClusters.find((c: AvailableCluster) => c.name === name);
    if (match) {
      set({ clusterName: match.name, replicaCount: match.replicaCount, shardCount: match.shardCount });
    }
  },
  reset: () => set({ clusterName: null, replicaCount: 1, shardCount: 1, availableClusters: [], detected: false }),
}));
