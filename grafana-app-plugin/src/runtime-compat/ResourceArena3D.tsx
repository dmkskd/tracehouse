import type { ActiveMergeInfo, RunningQueryInfo } from '@tracehouse/core';
import { createRuntimeComponent } from './runtimeComponent';

export interface ResourceArena3DProps {
  queries: RunningQueryInfo[];
  merges: ActiveMergeInfo[];
  cpuUsage: number;
  memoryPct: number;
  onQueryClick?: (queryId: string) => void;
  compact?: boolean;
  splitAvailable?: boolean;
  splitActive?: boolean;
  onSplitToggle?: () => void;
}

export const ResourceArena3D = createRuntimeComponent<ResourceArena3DProps>(
  'resourceArena3D',
  'ResourceArena3D',
  'Loading 3D arena...',
  420
);

export default ResourceArena3D;
