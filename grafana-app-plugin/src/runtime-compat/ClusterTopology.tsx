import type { HostMetrics } from '../../../frontend/src/components/cluster/ClusterTopology';
import { createRuntimeComponent } from './runtimeComponent';

export type { HostMetrics } from '../../../frontend/src/components/cluster/ClusterTopology';

interface ClusterTopologyProps {
  clusterName: string;
  nodes: Array<{
    host_name: string;
    shard_num: number;
    replica_num: number;
    is_local: boolean;
    errors_count?: number;
    estimated_recovery_time?: number;
  }>;
  keeperNodes: Array<{
    host: string;
    port: number;
    is_leader?: boolean;
    is_alive?: boolean;
  }>;
  hostMetrics?: Map<string, HostMetrics>;
}

export const ClusterTopology = createRuntimeComponent<ClusterTopologyProps>('clusterTopology', 'ClusterTopology', 'Loading cluster topology...');
