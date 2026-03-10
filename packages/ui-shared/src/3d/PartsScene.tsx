/**
 * PartsScene - Unified 3D scene for parts visualization with merge overlays
 *
 * Combines MergeLanes (level-based part layout), FlowLine (merge connections),
 * and ResultPartGhost (merge result preview) into a single scene component.
 *
 * This component is self-contained and does not depend on any frontend-specific
 * state management. All data is passed via props.
 *
 * Extracted from PartsVisualization.tsx + Scene3DWrapper.tsx.
 */

import React, { useMemo } from 'react';
import { SafeText as Text } from './SafeText.js';
import * as THREE from 'three';
import type { PartInfo, MergeInfo } from '@tracehouse/core';
import { MergeLanes } from './MergeLanes.js';
import { FlowLine } from './FlowLine.js';
import { ResultPartGhost } from './ResultPartGhost.js';
import { calculatePartSizes } from './sizeCalculations.js';

export interface PartsSceneProps {
  /** Parts to display, grouped by merge level in lanes */
  parts: PartInfo[];
  /** Active merges to overlay on the scene */
  activeMerges?: MergeInfo[];
  /** Spacing between level lanes (default 3) */
  levelSpacing?: number;
  /** Spacing between parts within a lane (default 1.8) */
  partSpacing?: number;
  /** Currently hovered part name */
  hoveredPartName?: string | null;
  /** Callback when a part is clicked */
  onPartClick?: (part: PartInfo) => void;
  /** Callback when a part is hovered/unhovered */
  onPartHover?: (part: PartInfo | null) => void;
  /** Whether to enable merge flow animations (default true) */
  enableAnimations?: boolean;
  /** Flow line color (default cyan) */
  flowLineColor?: THREE.Color | string | number;
  /** Result ghost color (default purple) */
  resultGhostColor?: THREE.Color | string | number;
}

/**
 * Build a position lookup for parts based on their level-lane layout.
 */
function buildPartPositionMap(
  parts: PartInfo[],
  levelSpacing: number,
  partSpacing: number
): Map<string, [number, number, number]> {
  const map = new Map<string, [number, number, number]>();

  const partsByLevel = new Map<number, PartInfo[]>();
  for (const part of parts) {
    if (!partsByLevel.has(part.level)) partsByLevel.set(part.level, []);
    partsByLevel.get(part.level)!.push(part);
  }

  const maxLevel = Math.max(...partsByLevel.keys(), 0);

  const visualSizes = calculatePartSizes(
    parts.map(p => ({ name: p.name, bytes_on_disk: p.bytes_on_disk }))
  );
  const sizeMap = new Map<string, number>();
  parts.forEach((p, i) => sizeMap.set(p.name, visualSizes[i].visualScale));

  for (const [level, levelParts] of partsByLevel) {
    const sorted = [...levelParts].sort(
      (a, b) => b.bytes_on_disk - a.bytes_on_disk
    );
    sorted.forEach((part, i) => {
      const vs = sizeMap.get(part.name) ?? 0.5;
      const xPos = level * levelSpacing - (maxLevel * levelSpacing) / 2;
      const zPos = i * partSpacing - (sorted.length - 1) * (partSpacing / 2);
      const yPos = vs / 2;
      map.set(part.name, [xPos, yPos, zPos]);
    });
  }

  return map;
}

/**
 * Compute merge overlays: highlighted source parts, flow lines, and result ghosts.
 */
interface MergeOverlay {
  highlightedParts: Set<string>;
  flowLines: Array<{
    key: string;
    from: [number, number, number];
    to: [number, number, number];
    color: THREE.Color | string | number;
  }>;
  resultGhosts: Array<{
    key: string;
    position: [number, number, number];
    progress: number;
    scale: number;
    color: THREE.Color | string | number;
  }>;
}

function computeMergeOverlays(
  activeMerges: MergeInfo[],
  positionMap: Map<string, [number, number, number]>,
  flowLineColor: THREE.Color | string | number,
  resultGhostColor: THREE.Color | string | number
): MergeOverlay {
  const highlightedParts = new Set<string>();
  const flowLines: MergeOverlay['flowLines'] = [];
  const resultGhosts: MergeOverlay['resultGhosts'] = [];

  for (const merge of activeMerges) {
    // Collect source part positions
    const sourcePositions: Array<{ name: string; pos: [number, number, number] }> = [];
    for (const srcName of merge.source_part_names) {
      highlightedParts.add(srcName);
      const pos = positionMap.get(srcName);
      if (pos) {
        sourcePositions.push({ name: srcName, pos });
      }
    }

    if (sourcePositions.length === 0) continue;

    // Result ghost position: average of source positions, shifted up
    const avgX =
      sourcePositions.reduce((s, p) => s + p.pos[0], 0) / sourcePositions.length;
    const avgZ =
      sourcePositions.reduce((s, p) => s + p.pos[2], 0) / sourcePositions.length;
    const maxY = Math.max(...sourcePositions.map(p => p.pos[1]));
    const ghostPos: [number, number, number] = [avgX, maxY + 1.5, avgZ];

    resultGhosts.push({
      key: `ghost-${merge.result_part_name}`,
      position: ghostPos,
      progress: merge.progress,
      scale: 0.8,
      color: resultGhostColor,
    });

    // Flow lines from each source to the ghost
    for (const src of sourcePositions) {
      flowLines.push({
        key: `flow-${src.name}-${merge.result_part_name}`,
        from: src.pos,
        to: ghostPos,
        color: flowLineColor,
      });
    }
  }

  return { highlightedParts, flowLines, resultGhosts };
}

export const PartsScene: React.FC<PartsSceneProps> = ({
  parts,
  activeMerges = [],
  levelSpacing = 3,
  partSpacing = 1.8,
  hoveredPartName = null,
  onPartClick,
  onPartHover,
  enableAnimations = true,
  flowLineColor = 0x22d3ee,
  resultGhostColor = 0x8b5cf6,
}) => {
  // Build position map for merge overlay calculations
  const positionMap = useMemo(
    () => buildPartPositionMap(parts, levelSpacing, partSpacing),
    [parts, levelSpacing, partSpacing]
  );

  // Compute merge overlays
  const overlay = useMemo(
    () =>
      computeMergeOverlays(
        activeMerges,
        positionMap,
        flowLineColor,
        resultGhostColor
      ),
    [activeMerges, positionMap, flowLineColor, resultGhostColor]
  );

  if (parts.length === 0) {
    return (
      <Text
        position={[0, 0, 0]}
        fontSize={0.4}
        color="white"
        anchorX="center"
        anchorY="middle"
      >
        No parts
      </Text>
    );
  }

  return (
    <group>
      {/* Parts organized by merge level */}
      <MergeLanes
        parts={parts}
        levelSpacing={levelSpacing}
        partSpacing={partSpacing}
        hoveredPartName={hoveredPartName}
        highlightedParts={overlay.highlightedParts}
        onPartClick={onPartClick}
        onPartHover={onPartHover}
      />

      {/* Flow lines connecting source parts to result ghosts */}
      {overlay.flowLines.map(fl => (
        <FlowLine
          key={fl.key}
          from={fl.from}
          to={fl.to}
          color={fl.color}
        />
      ))}

      {/* Result part ghosts */}
      {overlay.resultGhosts.map(ghost => (
        <ResultPartGhost
          key={ghost.key}
          position={ghost.position}
          progress={ghost.progress}
          scale={ghost.scale}
          color={ghost.color}
          enableAnimation={enableAnimations}
        />
      ))}
    </group>
  );
};
