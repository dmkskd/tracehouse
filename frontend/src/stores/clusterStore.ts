/**
 * Cluster Store — stores detected cluster topology info.
 * Populated on connection by ClickHouseProvider.
 */

import { create } from 'zustand';
import type { ClusterInfo } from '@tracehouse/core';

interface ClusterState extends ClusterInfo {
  /** Whether detection has completed */
  detected: boolean;
  /** Set cluster info after detection */
  setCluster: (info: ClusterInfo) => void;
  /** Reset on disconnect */
  reset: () => void;
}

export const useClusterStore = create<ClusterState>()((set) => ({
  clusterName: null,
  replicaCount: 1,
  shardCount: 1,
  detected: false,
  setCluster: (info) => set({ ...info, detected: true }),
  reset: () => set({ clusterName: null, replicaCount: 1, shardCount: 1, detected: false }),
}));
