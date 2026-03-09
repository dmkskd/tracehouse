/**
 * Headless hook for 3D parts layout calculations.
 * Groups parts by merge level, sorts within each level by bytes_on_disk,
 * and calculates 3D positions for level-based part positioning.
 */
import { useMemo } from 'react';
import type { PartInfo } from '@tracehouse/core';
import { calculatePartSizes } from '../3d/sizeCalculations.js';
import type { PartVisualSize, SizeCalculationConfig } from '../3d/sizeCalculations.js';

export interface PartPosition {
  name: string;
  x: number;
  y: number;
  z: number;
  visualSize: PartVisualSize;
}

export interface LevelMetadata {
  level: number;
  partCount: number;
  totalBytes: number;
  zPosition: number;
}

export interface UsePartsLayoutResult {
  positions: Map<string, PartPosition>;
  levels: LevelMetadata[];
}

export interface PartsLayoutOptions {
  /** Spacing between parts within a level on the X axis */
  partSpacing?: number;
  /** Spacing between levels on the Z axis */
  levelSpacing?: number;
  /** Size calculation config */
  sizeConfig?: SizeCalculationConfig;
}

export function usePartsLayout(
  parts: PartInfo[],
  options: PartsLayoutOptions = {}
): UsePartsLayoutResult {
  const { partSpacing = 2.0, levelSpacing = 4.0, sizeConfig } = options;

  return useMemo(() => {
    if (parts.length === 0) {
      return { positions: new Map(), levels: [] };
    }

    // Group parts by level
    const levelGroups = new Map<number, PartInfo[]>();
    for (const part of parts) {
      const group = levelGroups.get(part.level) ?? [];
      group.push(part);
      levelGroups.set(part.level, group);
    }

    // Sort levels ascending
    const sortedLevels = [...levelGroups.keys()].sort((a, b) => a - b);

    // Calculate visual sizes for all parts
    const allSizes = calculatePartSizes(
      parts.map(p => ({ name: p.name, bytes_on_disk: p.bytes_on_disk })),
      sizeConfig
    );
    const sizeMap = new Map<string, PartVisualSize>();
    for (const size of allSizes) {
      sizeMap.set(size.name, size);
    }

    const positions = new Map<string, PartPosition>();
    const levels: LevelMetadata[] = [];

    for (let i = 0; i < sortedLevels.length; i++) {
      const level = sortedLevels[i];
      const group = levelGroups.get(level)!;

      // Sort within level by bytes_on_disk descending
      group.sort((a, b) => b.bytes_on_disk - a.bytes_on_disk);

      const zPosition = i * levelSpacing;
      const totalBytes = group.reduce((sum, p) => sum + p.bytes_on_disk, 0);

      levels.push({
        level,
        partCount: group.length,
        totalBytes,
        zPosition,
      });

      // Position parts centered along X axis
      const totalWidth = (group.length - 1) * partSpacing;
      const startX = -totalWidth / 2;

      for (let j = 0; j < group.length; j++) {
        const part = group[j];
        const visualSize = sizeMap.get(part.name)!;

        positions.set(part.name, {
          name: part.name,
          x: startX + j * partSpacing,
          y: 0,
          z: zPosition,
          visualSize,
        });
      }
    }

    return { positions, levels };
  }, [parts, partSpacing, levelSpacing, sizeConfig]);
}
