import type { PartInfo } from '../../../frontend/src/stores/databaseStore';
import type { Scene3DProps } from '../../../frontend/src/components/3d/types';
import type {
  PartVisualizationProps,
  PartitionSummary,
} from '../../../frontend/src/components/3d/PartsVisualization';
import type { PipelineVisualizationProps } from '../../../frontend/src/components/3d/PipelineVisualization';
import type { MergeVisualizationProps } from '../../../frontend/src/components/3d/MergeVisualization';
import type { HierarchyVisualizationProps } from '../../../frontend/src/components/3d/HierarchyVisualization';
import { createRuntimeComponent } from './runtimeComponent';

export type {
  Scene3DProps,
  SceneConfig,
  PerformanceContextValue,
} from '../../../frontend/src/components/3d/types';
export type {
  PartVisualizationProps,
  ColorScheme,
  PartitionSummary,
  PartsVisualizationCallbacks,
} from '../../../frontend/src/components/3d/PartsVisualization';
export type { PipelineVisualizationProps } from '../../../frontend/src/components/3d/PipelineVisualization';
export type { MergeVisualizationProps } from '../../../frontend/src/components/3d/MergeVisualization';
export type {
  HierarchyItem,
  HierarchyLevel,
  HierarchyState,
  HierarchyVisualizationProps,
} from '../../../frontend/src/components/3d/HierarchyVisualization';

export interface PipelineNode {
  id: string;
  name: string;
  fullText: string;
  depth: number;
  parentId: string | null;
  childIds: string[];
  hasCorrelatedLogs: boolean;
  correlatedLogCount: number;
  metadata: Record<string, string>;
}

function parseNodeLine(line: string): { name: string; metadata: Record<string, string> } {
  const metadata: Record<string, string> = {};
  if (line.startsWith('(') && line.includes(')')) {
    const match = line.match(/^\(([^)]+)\)/);
    if (match) {
      return { name: match[1], metadata };
    }
  }

  const paramMatch = line.match(/^(\w+)\(([^)]*)\)/);
  if (paramMatch) {
    const name = paramMatch[1];
    for (const pair of paramMatch[2].split(',').map((p) => p.trim())) {
      const [key, value] = pair.split(':').map((s) => s.trim());
      if (key && value) {
        metadata[key] = value;
      }
    }
    return { name, metadata };
  }

  const simpleMatch = line.match(/^(\w+)/);
  return { name: simpleMatch?.[1] ?? line.trim(), metadata };
}

export function parsePipelineOutput(output: string): PipelineNode[] {
  const lines = output.split('\n').filter((line) => line.trim());
  const nodes: PipelineNode[] = [];
  const nodeStack: { id: string; depth: number }[] = [];
  let nodeId = 0;

  for (const line of lines) {
    const trimmedLine = line.trimStart();
    const depth = Math.floor((line.length - trimmedLine.length) / 2);
    const { name, metadata } = parseNodeLine(trimmedLine);
    if (!name) {
      continue;
    }

    const id = `node-${nodeId++}`;
    while (nodeStack.length > 0 && nodeStack[nodeStack.length - 1].depth >= depth) {
      nodeStack.pop();
    }

    const parentId = nodeStack[nodeStack.length - 1]?.id ?? null;
    if (parentId) {
      const parentNode = nodes.find((node) => node.id === parentId);
      parentNode?.childIds.push(id);
    }

    nodes.push({
      id,
      name,
      fullText: trimmedLine,
      depth,
      parentId,
      childIds: [],
      hasCorrelatedLogs: false,
      correlatedLogCount: 0,
      metadata,
    });
    nodeStack.push({ id, depth });
  }

  return nodes;
}

export function createPartitionSummaries(parts: PartInfo[]): PartitionSummary[] {
  const groups = new Map<string, PartInfo[]>();
  for (const part of parts) {
    const id = part.partition_id || 'default';
    const existing = groups.get(id);
    if (existing) {
      existing.push(part);
    } else {
      groups.set(id, [part]);
    }
  }

  return [...groups.entries()]
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([id, partitionParts]) => {
      const totalBytes = partitionParts.reduce((sum, part) => sum + part.bytes_on_disk, 0);
      const partCount = partitionParts.length;
      const unmergedCount = partitionParts.filter((part) => part.level === 0).length;
      const unmergedRatio = partCount > 0 ? unmergedCount / partCount : 0;
      const maxLevel = Math.max(...partitionParts.map((part) => part.level), 0);
      let healthScore = 100 - unmergedRatio * 60;
      if (partCount > 100) {
        healthScore -= 20;
      } else if (partCount > 50) {
        healthScore -= 10;
      }
      healthScore = Math.max(0, Math.min(100, healthScore));
      const healthStatus = healthScore >= 70 ? 'good' : healthScore >= 40 ? 'warning' : 'critical';
      return { id, parts: partitionParts, totalBytes, partCount, unmergedCount, unmergedRatio, maxLevel, healthScore, healthStatus };
    });
}

export const Scene3D = createRuntimeComponent<Scene3DProps>('components3d', 'Scene3D', 'Loading 3D scene...');
export const PartsVisualization = createRuntimeComponent<PartVisualizationProps>('components3d', 'PartsVisualization', 'Loading parts visualization...', 0);
export const PipelineVisualization = createRuntimeComponent<PipelineVisualizationProps>('components3d', 'PipelineVisualization', 'Loading pipeline visualization...', 0);
export const MergeVisualization = createRuntimeComponent<MergeVisualizationProps>('components3d', 'MergeVisualization', 'Loading merge visualization...', 0);
export const HierarchyVisualization = createRuntimeComponent<HierarchyVisualizationProps>('components3d', 'HierarchyVisualization', 'Loading hierarchy visualization...', 0);
