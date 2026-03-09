/**
 * Tests for PartsVisualization helper logic.
 *
 * Tests color scheme normalization, grid layout, and size proportionality integration.
 */

import { describe, test, expect } from 'vitest';
import * as THREE from 'three';
import type { PartInfo } from '../../../stores/databaseStore';
import { calculatePartSizes } from '../sizeCalculations';

type ColorScheme = 'size' | 'age' | 'level';

const COLOR_PALETTES = {
  size: { low: new THREE.Color(0x3b82f6), mid: new THREE.Color(0xeab308), high: new THREE.Color(0xef4444) },
  age: { low: new THREE.Color(0x22c55e), mid: new THREE.Color(0xf59e0b), high: new THREE.Color(0xf97316) },
  level: { low: new THREE.Color(0xa855f7), mid: new THREE.Color(0x6366f1), high: new THREE.Color(0x0ea5e9) },
};

function getColorForValue(value: number, scheme: ColorScheme): THREE.Color {
  const palette = COLOR_PALETTES[scheme];
  const color = new THREE.Color();
  if (value < 0.5) color.lerpColors(palette.low, palette.mid, value * 2);
  else color.lerpColors(palette.mid, palette.high, (value - 0.5) * 2);
  return color;
}

function getNormalizedValue(part: PartInfo, allParts: PartInfo[], scheme: ColorScheme): number {
  switch (scheme) {
    case 'size': {
      const max = Math.max(...allParts.map(p => p.bytes_on_disk), 1);
      return part.bytes_on_disk / max;
    }
    case 'age': {
      const times = allParts.map(p => new Date(p.modification_time).getTime());
      const range = Math.max(...times) - Math.min(...times) || 1;
      return 1 - (new Date(part.modification_time).getTime() - Math.min(...times)) / range;
    }
    case 'level': {
      const max = Math.max(...allParts.map(p => p.level), 1);
      return part.level / max;
    }
  }
}

function calculateGridPositions(count: number): [number, number, number][] {
  const positions: [number, number, number][] = [];
  const gridSize = Math.ceil(Math.sqrt(count));
  const spacing = 2.5;
  for (let i = 0; i < count; i++) {
    const row = Math.floor(i / gridSize);
    const col = i % gridSize;
    positions.push([
      col * spacing - (gridSize - 1) * spacing / 2,
      0.5,
      row * spacing - (gridSize - 1) * spacing / 2,
    ]);
  }
  return positions;
}

const makePart = (overrides: Partial<PartInfo> = {}): PartInfo => ({
  partition_id: 'p1',
  name: 'part_1',
  rows: 1000,
  bytes_on_disk: 1024,
  modification_time: new Date().toISOString(),
  level: 0,
  primary_key_bytes_in_memory: 100,
  ...overrides,
});

describe('PartsVisualization Logic', () => {
  describe('color normalization', () => {
    test('normalized values are 0–1 for all schemes', () => {
      const parts = [
        makePart({ bytes_on_disk: 100, level: 1 }),
        makePart({ name: 'p2', bytes_on_disk: 10000, level: 5 }),
      ];
      for (const scheme of ['size', 'age', 'level'] as ColorScheme[]) {
        for (const part of parts) {
          const v = getNormalizedValue(part, parts, scheme);
          expect(v).toBeGreaterThanOrEqual(0);
          expect(v).toBeLessThanOrEqual(1);
        }
      }
    });

    test('largest part has normalized size of 1', () => {
      const parts = [
        makePart({ bytes_on_disk: 1024 }),
        makePart({ name: 'big', bytes_on_disk: 10240 }),
      ];
      expect(getNormalizedValue(parts[1], parts, 'size')).toBe(1);
    });

    test('color interpolation produces valid THREE.Color', () => {
      for (const scheme of ['size', 'age', 'level'] as ColorScheme[]) {
        const color = getColorForValue(0.5, scheme);
        expect(color).toBeInstanceOf(THREE.Color);
        expect(color.r).toBeGreaterThanOrEqual(0);
        expect(color.r).toBeLessThanOrEqual(1);
      }
    });
  });

  describe('grid layout', () => {
    test('unique positions and single part at center', () => {
      const positions = calculateGridPositions(9);
      expect(new Set(positions.map(p => p.join(','))).size).toBe(9);

      const [[x, , z]] = calculateGridPositions(1);
      expect(x).toBe(0);
      expect(z).toBe(0);
    });
  });

  describe('size proportionality', () => {
    test('calculatePartSizes returns valid proportions', () => {
      const parts = [
        { name: 'a', bytes_on_disk: 100 },
        { name: 'b', bytes_on_disk: 900 },
      ];
      const sizes = calculatePartSizes(parts);
      expect(sizes).toHaveLength(2);
      expect(sizes[0].proportionalSize).toBeCloseTo(0.1);
      expect(sizes[1].proportionalSize).toBeCloseTo(0.9);
    });

    test('empty input returns empty', () => {
      expect(calculatePartSizes([])).toEqual([]);
    });
  });

  describe('edge cases', () => {
    test('all zero bytes still produces valid normalization', () => {
      const parts = [makePart({ bytes_on_disk: 0 }), makePart({ name: 'p2', bytes_on_disk: 0 })];
      const v = getNormalizedValue(parts[0], parts, 'size');
      expect(v).toBeGreaterThanOrEqual(0);
      expect(v).toBeLessThanOrEqual(1);
    });

    test('same modification time produces valid age normalization', () => {
      const t = new Date().toISOString();
      const parts = [makePart({ modification_time: t }), makePart({ name: 'p2', modification_time: t })];
      const v = getNormalizedValue(parts[0], parts, 'age');
      expect(v).toBeGreaterThanOrEqual(0);
      expect(v).toBeLessThanOrEqual(1);
    });
  });
});
