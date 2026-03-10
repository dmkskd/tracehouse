/**
 * MergeLanes - Level-based lane layout for parts grouped by merge level
 *
 * Renders parts organized into lanes by their merge level (L0 through L5+).
 * Parts within each lane are sized proportionally to bytes_on_disk using
 * GlassBox components.
 *
 * Extracted from PartsVisualization.tsx PartitionDetailView.
 */

import React, { useMemo } from 'react';
import { SafeText as Text } from './SafeText.js';
import * as THREE from 'three';
import type { PartInfo } from '@tracehouse/core';
import { GlassBox } from './GlassBox.js';
import { calculatePartSizes, type PartVisualSize } from './sizeCalculations.js';

export interface MergeLanesProps {
  /** Parts to display, grouped by merge level */
  parts: PartInfo[];
  /** Spacing between level lanes (default 3) */
  levelSpacing?: number;
  /** Spacing between parts within a lane (default 1.8) */
  partSpacing?: number;
  /** Currently hovered part name */
  hoveredPartName?: string | null;
  /** Set of part names to highlight (e.g. source parts of active merge) */
  highlightedParts?: Set<string>;
  /** Callback when a part is clicked */
  onPartClick?: (part: PartInfo) => void;
  /** Callback when a part is hovered/unhovered */
  onPartHover?: (part: PartInfo | null) => void;
}

/**
 * Level label rendered above each lane
 */
const LevelMarker: React.FC<{ level: number; xPosition: number; isL0: boolean }> = ({
  level,
  xPosition,
  isL0,
}) => (
  <Text
    position={[xPosition, 0.1, -1.5]}
    fontSize={0.3}
    color={isL0 ? '#f59e0b' : '#3b82f6'}
    anchorX="center"
    anchorY="middle"
    outlineWidth={0.02}
    outlineColor="black"
  >
    L{level}
  </Text>
);

export const MergeLanes: React.FC<MergeLanesProps> = ({
  parts,
  levelSpacing = 3,
  partSpacing = 1.8,
  hoveredPartName = null,
  highlightedParts,
  onPartClick,
  onPartHover,
}) => {
  // Group parts by level
  const partsByLevel = useMemo(() => {
    const groups = new Map<number, PartInfo[]>();
    for (const part of parts) {
      if (!groups.has(part.level)) groups.set(part.level, []);
      groups.get(part.level)!.push(part);
    }
    return new Map([...groups.entries()].sort((a, b) => a[0] - b[0]));
  }, [parts]);

  const maxLevel = useMemo(
    () => Math.max(...partsByLevel.keys(), 0),
    [partsByLevel]
  );

  // Calculate visual sizes for all parts
  const visualSizes = useMemo(
    () =>
      calculatePartSizes(
        parts.map(p => ({ name: p.name, bytes_on_disk: p.bytes_on_disk }))
      ),
    [parts]
  );

  const sizeMap = useMemo(() => {
    const map = new Map<string, PartVisualSize>();
    parts.forEach((p, i) => map.set(p.name, visualSizes[i]));
    return map;
  }, [parts, visualSizes]);

  // Calculate positions for each part
  const partsWithPositions = useMemo(() => {
    const result: Array<{
      part: PartInfo;
      visualSize: PartVisualSize;
      position: [number, number, number];
      color: THREE.Color;
    }> = [];

    for (const [level, levelParts] of partsByLevel) {
      const sorted = [...levelParts].sort(
        (a, b) => b.bytes_on_disk - a.bytes_on_disk
      );
      sorted.forEach((part, i) => {
        const visualSize = sizeMap.get(part.name)!;
        const xPos = level * levelSpacing - (maxLevel * levelSpacing) / 2;
        const zPos = i * partSpacing - (sorted.length - 1) * (partSpacing / 2);
        const yPos = visualSize.visualScale / 2;

        const levelRatio = maxLevel > 0 ? level / maxLevel : 1;
        const color = new THREE.Color().lerpColors(
          new THREE.Color(0xf59e0b),
          new THREE.Color(0x3b82f6),
          levelRatio
        );

        result.push({ part, visualSize, position: [xPos, yPos, zPos], color });
      });
    }
    return result;
  }, [partsByLevel, sizeMap, maxLevel, levelSpacing, partSpacing]);

  // Level markers
  const levelMarkers = useMemo(
    () =>
      [...partsByLevel.keys()].map(level => ({
        level,
        xPos: level * levelSpacing - (maxLevel * levelSpacing) / 2,
      })),
    [partsByLevel, maxLevel, levelSpacing]
  );

  return (
    <group>
      {/* Level labels */}
      {levelMarkers.map(({ level, xPos }) => (
        <LevelMarker
          key={level}
          level={level}
          xPosition={xPos}
          isL0={level === 0}
        />
      ))}

      {/* Part boxes */}
      {partsWithPositions.map(({ part, visualSize, position, color }) => (
        <GlassBox
          key={part.name}
          position={position}
          scale={visualSize.visualScale}
          color={color}
          isHovered={hoveredPartName === part.name}
          isHighlighted={highlightedParts?.has(part.name) ?? false}
          onClick={(e) => {
            (e as unknown as { stopPropagation: () => void }).stopPropagation();
            onPartClick?.(part);
          }}
          onPointerOver={(e) => {
            (e as unknown as { stopPropagation: () => void }).stopPropagation();
            onPartHover?.(part);
          }}
          onPointerOut={(e) => {
            (e as unknown as { stopPropagation: () => void }).stopPropagation();
            onPartHover?.(null);
          }}
        />
      ))}
    </group>
  );
};
