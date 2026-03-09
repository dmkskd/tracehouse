/**
 * Tests for MergeVisualization helper logic.
 *
 * Tests progress color interpolation, visual scale calculation,
 * grid layout, and formatting functions.
 */

import { describe, test, expect } from 'vitest';
import * as THREE from 'three';
import type { MergeInfo } from '../../../stores/mergeStore';

function createMergeInfo(partial: Partial<MergeInfo>): MergeInfo {
  return {
    database: 'default',
    table: 'test_table',
    elapsed: 10,
    progress: 0.5,
    num_parts: 3,
    source_part_names: ['p1', 'p2', 'p3'],
    result_part_name: 'result',
    total_size_bytes_compressed: 1024 * 1024,
    rows_read: 1000,
    rows_written: 500,
    memory_usage: 1024 * 1024,
    merge_type: 'Regular',
    merge_algorithm: 'Horizontal',
    is_mutation: false,
    bytes_read_uncompressed: 2048 * 1024,
    bytes_written_uncompressed: 1024 * 1024,
    columns_written: 10,
    thread_id: 1,
    ...partial,
  };
}

const PROGRESS_COLORS = {
  start: new THREE.Color(0xef4444),
  mid: new THREE.Color(0xeab308),
  end: new THREE.Color(0x22c55e),
};

function getProgressColor(progress: number): THREE.Color {
  const color = new THREE.Color();
  if (progress < 0.5) {
    color.lerpColors(PROGRESS_COLORS.start, PROGRESS_COLORS.mid, progress * 2);
  } else {
    color.lerpColors(PROGRESS_COLORS.mid, PROGRESS_COLORS.end, (progress - 0.5) * 2);
  }
  return color;
}

function calculateVisualScale(totalBytes: number, allMerges: MergeInfo[]): number {
  const maxBytes = Math.max(...allMerges.map(m => m.total_size_bytes_compressed), 1);
  const minScale = 0.5;
  const maxScale = 2.0;
  const normalizedSize = Math.log10(totalBytes + 1) / Math.log10(maxBytes + 1);
  return minScale + normalizedSize * (maxScale - minScale);
}

function calculateMergePositions(count: number): [number, number, number][] {
  const positions: [number, number, number][] = [];
  const gridSize = Math.ceil(Math.sqrt(count));
  const spacing = 6;
  for (let i = 0; i < count; i++) {
    const row = Math.floor(i / gridSize);
    const col = i % gridSize;
    positions.push([
      col * spacing - (gridSize - 1) * spacing / 2,
      0,
      row * spacing - (gridSize - 1) * spacing / 2,
    ]);
  }
  return positions;
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`;
}

function formatDuration(seconds: number): string {
  if (seconds < 1) return `${Math.round(seconds * 1000)}ms`;
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  const minutes = Math.floor(seconds / 60);
  return `${minutes}m ${Math.round(seconds % 60)}s`;
}

describe('MergeVisualization Logic', () => {
  describe('progress colors', () => {
    test('maps 0/0.5/1 to start/mid/end colors', () => {
      expect(getProgressColor(0).getHex()).toBe(PROGRESS_COLORS.start.getHex());
      expect(getProgressColor(0.5).getHex()).toBe(PROGRESS_COLORS.mid.getHex());
      expect(getProgressColor(1).getHex()).toBe(PROGRESS_COLORS.end.getHex());
    });

    test('transitions smoothly between steps', () => {
      const colors = Array.from({ length: 11 }, (_, i) => getProgressColor(i / 10));
      for (let i = 1; i < colors.length; i++) {
        const dr = Math.abs(colors[i].r - colors[i - 1].r);
        const dg = Math.abs(colors[i].g - colors[i - 1].g);
        const db = Math.abs(colors[i].b - colors[i - 1].b);
        expect(Math.sqrt(dr * dr + dg * dg + db * db)).toBeLessThan(0.5);
      }
    });
  });

  describe('visual scale', () => {
    test('stays within 0.5–2.0 range', () => {
      const merges = [
        createMergeInfo({ total_size_bytes_compressed: 1024 }),
        createMergeInfo({ total_size_bytes_compressed: 1024 * 1024 * 1024 }),
      ];
      for (const m of merges) {
        const scale = calculateVisualScale(m.total_size_bytes_compressed, merges);
        expect(scale).toBeGreaterThanOrEqual(0.5);
        expect(scale).toBeLessThanOrEqual(2.0);
      }
    });

    test('largest merge gets max scale', () => {
      const merges = [
        createMergeInfo({ total_size_bytes_compressed: 1024 }),
        createMergeInfo({ total_size_bytes_compressed: 1024 * 1024 * 1024 }),
      ];
      expect(calculateVisualScale(1024 * 1024 * 1024, merges)).toBe(2.0);
    });
  });

  describe('grid layout', () => {
    test('produces unique positions', () => {
      const positions = calculateMergePositions(9);
      const strings = positions.map(p => p.join(','));
      expect(new Set(strings).size).toBe(9);
    });

    test('single merge at center', () => {
      const [[x, y, z]] = calculateMergePositions(1);
      expect(x).toBe(0);
      expect(y).toBe(0);
      expect(z).toBe(0);
    });

    test('empty returns empty', () => {
      expect(calculateMergePositions(0)).toEqual([]);
    });
  });

  describe('formatting', () => {
    test('formatBytes uses correct units', () => {
      expect(formatBytes(0)).toBe('0 B');
      expect(formatBytes(500)).toMatch(/B$/);
      expect(formatBytes(1024)).toMatch(/KB$/);
      expect(formatBytes(1024 * 1024)).toMatch(/MB$/);
      expect(formatBytes(1024 ** 3)).toMatch(/GB$/);
    });

    test('formatDuration uses correct units', () => {
      expect(formatDuration(0.5)).toMatch(/ms$/);
      expect(formatDuration(30)).toMatch(/s$/);
      expect(formatDuration(120)).toMatch(/m.*s$/);
    });
  });
});
