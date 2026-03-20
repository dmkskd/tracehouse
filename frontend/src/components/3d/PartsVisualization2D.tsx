/**
 * PartsVisualization2D - Clean 2D view of parts organized by merge level
 * 
 * A simpler alternative to the 3D visualization, showing parts as a grid
 * organized by merge level (L0, L1, L2, etc.) with merge flow indicators.
 */

import React, { useState, useMemo, useCallback, useRef } from 'react';
import type { HierarchyItem } from './HierarchyVisualization';
import { formatBytes } from '../../utils/formatters';
import { getPartLevelGroupKey, MUTATION_GROUP_KEY } from '@tracehouse/core';

// Colors for merge levels
const MERGE_LEVEL_COLORS = [
  '#ef4444', // L0 - Red (unmerged)
  '#f97316', // L1 - Orange
  '#eab308', // L2 - Yellow
  '#22c55e', // L3 - Green
  '#14b8a6', // L4 - Teal
  '#3b82f6', // L5 - Blue
  '#8b5cf6', // L6 - Purple
  '#ec4899', // L7+ - Pink
];

const MUTATION_COLOR = '#f43f5e'; // Rose — distinct from merge level colors

// Colors for different merge groups
const MERGE_GROUP_COLORS = [
  '#06b6d4', // Cyan
  '#f59e0b', // Amber
  '#ec4899', // Pink
  '#3b82f6', // Blue
  '#8b5cf6', // Purple
  '#14b8a6', // Teal
  '#f472b6', // Light Pink
  '#a78bfa', // Violet
];

// Parse part name to extract block range: partition_minBlock_maxBlock_level
function parsePartName(name: string): { minBlock: number; maxBlock: number } | null {
  const parts = name.split('_');
  if (parts.length >= 4) {
    const minBlock = parseInt(parts[1], 10);
    const maxBlock = parseInt(parts[2], 10);
    if (!isNaN(minBlock) && !isNaN(maxBlock)) {
      return { minBlock, maxBlock };
    }
  }
  return null;
}

function getMergeLevelFromMetrics(item: HierarchyItem): number {
  const levelStr = item.metrics?.level;
  if (typeof levelStr === 'string' && levelStr.startsWith('L')) {
    return parseInt(levelStr.slice(1), 10) || 0;
  }
  return 0;
}

/** Returns the color for a given level, handling mutation group specially. */
function getLevelColor(level: number): string {
  if (level === MUTATION_GROUP_KEY) return MUTATION_COLOR;
  return MERGE_LEVEL_COLORS[Math.min(level, MERGE_LEVEL_COLORS.length - 1)];
}



interface PartsVisualization2DProps {
  items: HierarchyItem[];
  onItemClick: (item: HierarchyItem) => void;
  onItemHover: (item: HierarchyItem | null) => void;
  highlightedMergeId?: string | null;
  mergeColorMap?: Map<string, number>;
}

export const PartsVisualization2D: React.FC<PartsVisualization2DProps> = ({
  items,
  onItemClick,
  onItemHover,
  highlightedMergeId,
  mergeColorMap = new Map(),
}) => {
  const [hoveredId, setHoveredId] = useState<string | null>(null);
  const [zoom, setZoom] = useState(1);
  const containerRef = useRef<HTMLDivElement>(null);
  
  // Zoom controls
  const handleZoomIn = useCallback(() => {
    setZoom(z => Math.min(z + 0.25, 3));
  }, []);
  
  const handleZoomOut = useCallback(() => {
    setZoom(z => Math.max(z - 0.25, 0.25));
  }, []);
  
  const handleZoomReset = useCallback(() => {
    setZoom(1);
  }, []);
  
  // Mouse wheel zoom
  const handleWheel = useCallback((e: React.WheelEvent) => {
    if (e.ctrlKey || e.metaKey) {
      e.preventDefault();
      const delta = e.deltaY > 0 ? -0.1 : 0.1;
      setZoom(z => Math.max(0.25, Math.min(3, z + delta)));
    }
  }, []);

  // Group items by merge level (mutation levels are collapsed into one group)
  const itemsByLevel = useMemo(() => {
    const groups = new Map<number, HierarchyItem[]>();
    
    items.forEach(item => {
      const key = getPartLevelGroupKey(item.name);
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key)!.push(item);
    });
    
    // Sort each group by minBlock (sequential order)
    groups.forEach(levelItems => {
      levelItems.sort((a, b) => {
        const blockA = parsePartName(a.id);
        const blockB = parsePartName(b.id);
        if (blockA && blockB) {
          return blockA.minBlock - blockB.minBlock;
        }
        // Fallback to name comparison
        return a.id.localeCompare(b.id);
      });
    });
    
    return groups;
  }, [items]);

  const handleHover = (item: HierarchyItem | null) => {
    setHoveredId(item?.id || null);
    onItemHover(item);
  };

  // Group active merges to show destination parts
  const activeMerges = useMemo(() => {
    const merges = new Map<string, { 
      sources: HierarchyItem[]; 
      target: string; 
      progress: number;
      targetLevel: number;
      targetMinBlock: number;
      totalSize: number;
    }>();
    
    items.forEach(item => {
      if (item.merging && item.mergeId && item.mergeTarget) {
        if (!merges.has(item.mergeId)) {
          const targetParts = item.mergeTarget.split('_');
          const targetMinBlock = targetParts.length >= 2 ? parseInt(targetParts[1], 10) || 0 : 0;
          
          merges.set(item.mergeId, {
            sources: [],
            target: item.mergeTarget,
            progress: item.mergeProgress || 0,
            targetLevel: getPartLevelGroupKey(item.mergeTarget || ''),
            targetMinBlock,
            totalSize: 0,
          });
        }
        const merge = merges.get(item.mergeId)!;
        merge.sources.push(item);
        merge.totalSize += item.size;
        if (item.mergeProgress !== undefined) {
          merge.progress = item.mergeProgress;
        }
      }
    });
    
    return merges;
  }, [items]);

  // Get all levels sorted (including levels that only have destination merges)
  // Mutation group (MUTATION_GROUP_KEY = -1) is placed at the end
  const levels = useMemo(() => {
    const allLevels = new Set<number>();
    itemsByLevel.forEach((_, level) => allLevels.add(level));
    activeMerges.forEach(merge => allLevels.add(merge.targetLevel));
    return Array.from(allLevels).sort((a, b) => {
      // Mutation group always goes last
      if (a === MUTATION_GROUP_KEY) return 1;
      if (b === MUTATION_GROUP_KEY) return -1;
      return a - b;
    });
  }, [itemsByLevel, activeMerges]);

  // Create combined items per level (parts + destination merges) sorted by block number
  const combinedByLevel = useMemo(() => {
    const result = new Map<number, Array<{ type: 'part'; item: HierarchyItem } | { type: 'merge'; mergeId: string; merge: typeof activeMerges extends Map<string, infer V> ? V : never }>>();
    
    // Add existing parts
    itemsByLevel.forEach((levelItems, level) => {
      if (!result.has(level)) result.set(level, []);
      levelItems.forEach(item => {
        result.get(level)!.push({ type: 'part', item });
      });
    });
    
    // Add destination merges
    activeMerges.forEach((merge, mergeId) => {
      const level = merge.targetLevel;
      if (!result.has(level)) result.set(level, []);
      result.get(level)!.push({ type: 'merge', mergeId, merge });
    });
    
    // Sort each level by minBlock
    result.forEach(items => {
      items.sort((a, b) => {
        const blockA = a.type === 'part' 
          ? (parsePartName(a.item.id)?.minBlock ?? 0)
          : a.merge.targetMinBlock;
        const blockB = b.type === 'part'
          ? (parsePartName(b.item.id)?.minBlock ?? 0)
          : b.merge.targetMinBlock;
        return blockA - blockB;
      });
    });
    
    return result;
  }, [itemsByLevel, activeMerges]);

  if (items.length === 0) {
    return (
      <div className="h-full flex items-center justify-center text-gray-500">
        No parts found
      </div>
    );
  }

  return (
    <div 
      ref={containerRef}
      className="h-full overflow-auto" 
      style={{ paddingTop: '260px', paddingLeft: '24px', paddingBottom: '80px' }}
      onWheel={handleWheel}
    >
      {/* Zoom Controls - Fixed position, above completed merges */}
      <div 
        className="fixed bottom-20 right-4 flex items-center gap-2 backdrop-blur-sm rounded-lg px-3 py-2"
        style={{ zIndex: 2000, background: 'var(--bg-tertiary)', border: '1px solid var(--border-primary)' }}
      >
        <button
          onClick={handleZoomOut}
          className="w-7 h-7 flex items-center justify-center rounded transition-colors"
          style={{ background: 'var(--bg-hover)', color: 'var(--text-secondary)' }}
          title="Zoom out"
        >
          −
        </button>
        <button
          onClick={handleZoomReset}
          className="px-2 h-7 flex items-center justify-center rounded text-xs font-mono min-w-[50px] transition-colors"
          style={{ background: 'var(--bg-hover)', color: 'var(--text-secondary)' }}
          title="Reset zoom"
        >
          {Math.round(zoom * 100)}%
        </button>
        <button
          onClick={handleZoomIn}
          className="w-7 h-7 flex items-center justify-center rounded transition-colors"
          style={{ background: 'var(--bg-hover)', color: 'var(--text-secondary)' }}
          title="Zoom in"
        >
          +
        </button>
        <span className="text-[9px] ml-1" style={{ color: 'var(--text-disabled)' }}>Ctrl+Scroll</span>
      </div>
      
      {/* Zoomable content */}
      <div 
        className="px-12 pr-12 pb-6"
        style={{ 
          transform: `scale(${zoom})`,
          transformOrigin: 'top left',
          width: `${100 / zoom}%`,
        }}
      >
        {/* Parts Grid by Level */}
        <div className="space-y-6">
        {levels.map(level => {
          const levelItems = itemsByLevel.get(level) || [];
          const combinedItems = combinedByLevel.get(level) || [];
          const color = getLevelColor(level);
          const totalSize = levelItems.reduce((sum, item) => sum + item.size, 0);
          const isMutationGroup = level === MUTATION_GROUP_KEY;
          // Collect distinct raw mutation levels for the header
          const mutationLevelRange = isMutationGroup
            ? Array.from(new Set(levelItems.map(i => getMergeLevelFromMetrics(i)))).sort((a, b) => a - b)
            : [];
          
          return (
            <div key={level} className="relative">
              {/* Level Header */}
              <div className="flex items-center gap-3 mb-3">
                <div
                  className="px-3 py-1 rounded font-bold text-lg"
                  style={{
                    backgroundColor: `${color}20`,
                    color: color,
                    textShadow: `0 0 10px ${color}`,
                  }}
                >
                  {isMutationGroup ? 'MUT' : `L${level}`}
                </div>
                {isMutationGroup && (
                  <span className="text-xs uppercase tracking-wider" style={{ color: MUTATION_COLOR }}>
                    Mutations
                    {mutationLevelRange.length > 0 && (
                      <span className="ml-1 font-mono normal-case" style={{ color: 'var(--text-muted)' }}>
                        (L{mutationLevelRange[0]}
                        {mutationLevelRange.length > 1 && `–L${mutationLevelRange[mutationLevelRange.length - 1]}`})
                      </span>
                    )}
                  </span>
                )}
                <div className="text-sm" style={{ color: 'var(--text-muted)' }}>
                  {levelItems.length} part{levelItems.length !== 1 ? 's' : ''}
                </div>
                <div className="text-sm" style={{ color: 'var(--text-disabled)' }}>
                  {formatBytes(totalSize)}
                </div>
                <div className="flex-1 h-px" style={{ background: 'linear-gradient(to right, var(--border-primary), transparent)' }} />
                {level === 0 && (
                  <span className="text-xs uppercase tracking-wider" style={{ color: 'var(--text-disabled)' }}>Unmerged</span>
                )}
              </div>

              {/* Parts Grid - combined and sorted by block number */}
              <div className="flex flex-wrap gap-2 items-stretch">
                {combinedItems.map((entry) => {
                  if (entry.type === 'merge') {
                    // Destination/Result part being created (ghost box)
                    const { mergeId, merge } = entry;
                    const colorIndex = mergeColorMap.get(mergeId) ?? 0;
                    const mergeColor = MERGE_GROUP_COLORS[colorIndex % MERGE_GROUP_COLORS.length];
                    const isHighlighted = highlightedMergeId === mergeId;
                    const isDimmed = highlightedMergeId && highlightedMergeId !== mergeId;
                    const progressPct = Math.round(merge.progress * 100);

                    return (
                      <div
                        key={`dest-${mergeId}`}
                        className={`
                          relative px-3 py-2 rounded-lg
                          transition-all duration-200
                          ${isDimmed ? 'opacity-30' : ''}
                        `}
                        style={{
                          backgroundColor: 'transparent',
                          border: `2px dashed ${mergeColor}`,
                          boxShadow: isHighlighted ? `0 0 20px ${mergeColor}40` : `0 0 10px ${mergeColor}20`,
                          minWidth: '120px',
                          height: '72px',
                        }}
                      >
                        {/* Merging indicator */}
                        <div className="flex items-center gap-1.5">
                          <span 
                            className="text-xs animate-spin"
                            style={{ color: mergeColor }}
                          >
                            ⟳
                          </span>
                          <span 
                            className="text-[10px] font-medium uppercase tracking-wide"
                            style={{ color: mergeColor }}
                          >
                            Merging
                          </span>
                          <span 
                            className="text-[10px] font-mono font-semibold ml-auto"
                            style={{ color: mergeColor }}
                          >
                            {progressPct}%
                          </span>
                        </div>

                        {/* Target part name */}
                        <div 
                          className="text-xs font-mono truncate max-w-[150px] mt-1"
                          style={{ color: mergeColor }}
                          title={merge.target}
                        >
                          {merge.target.length > 18 
                            ? merge.target.slice(0, 8) + '…' + merge.target.slice(-8) 
                            : merge.target}
                        </div>

                        {/* Info */}
                        <div className="text-[10px] mt-0.5" style={{ color: 'var(--text-muted)' }}>
                          {merge.sources.length} parts → {formatBytes(merge.totalSize)}
                        </div>

                        {/* Progress bar */}
                        <div className="mt-1 h-1 rounded overflow-hidden" style={{ background: 'var(--bg-hover)' }}>
                          <div
                            className="h-full transition-all"
                            style={{
                              width: `${progressPct}%`,
                              backgroundColor: mergeColor,
                            }}
                          />
                        </div>
                      </div>
                    );
                  } else {
                    // Existing part
                    const { item } = entry;
                    const isHovered = hoveredId === item.id;
                    const isMerging = item.merging;
                    const isDimmed = highlightedMergeId && item.mergeId !== highlightedMergeId;
                    const mergeColorIndex = item.mergeId ? mergeColorMap.get(item.mergeId) : undefined;
                    const mergeColor = mergeColorIndex !== undefined 
                      ? MERGE_GROUP_COLORS[mergeColorIndex % MERGE_GROUP_COLORS.length]
                      : null;

                    return (
                      <div
                        key={item.id}
                        className={`
                          relative px-3 py-2 rounded-lg cursor-pointer
                          transition-all duration-200 flex flex-col
                          ${isHovered ? 'scale-105 z-10' : ''}
                          ${isDimmed ? 'opacity-30' : ''}
                        `}
                        style={{
                          backgroundColor: isMerging 
                            ? `${mergeColor || '#f97316'}20`
                            : `${color}10`,
                          border: `1px solid ${isMerging ? (mergeColor || '#f97316') + '60' : color + '30'}`,
                          boxShadow: isHovered 
                            ? `0 4px 20px ${isMerging ? (mergeColor || '#f97316') : color}30`
                            : 'none',
                          height: '72px',
                          minWidth: '120px',
                        }}
                        onClick={() => onItemClick(item)}
                        onMouseEnter={() => handleHover(item)}
                        onMouseLeave={() => handleHover(null)}
                      >
                        {/* Merging indicator */}
                        {isMerging && (
                          <div 
                            className="absolute -top-1 -right-1 w-3 h-3 rounded-full animate-pulse"
                            style={{ backgroundColor: mergeColor || '#f97316' }}
                          />
                        )}

                        {/* Part name */}
                        <div 
                          className="text-xs font-mono truncate max-w-[150px]"
                          style={{ color: isMerging ? (mergeColor || '#f97316') : 'var(--text-secondary)' }}
                          title={item.id}
                        >
                          {item.name}
                        </div>

                        {/* Size + mutation level badge */}
                        <div className="flex items-center gap-1.5">
                          <div className="text-[10px] mt-0.5" style={{ color: 'var(--text-muted)' }}>
                            {formatBytes(item.size)}
                          </div>
                          {isMutationGroup && (
                            <span
                              className="text-[9px] font-mono px-1 rounded mt-0.5"
                              style={{ backgroundColor: `${MUTATION_COLOR}20`, color: MUTATION_COLOR }}
                            >
                              L{getMergeLevelFromMetrics(item)}
                            </span>
                          )}
                        </div>

                        {/* Merge progress bar - always show space for it */}
                        <div className="mt-auto pt-2">
                          {isMerging && item.mergeProgress !== undefined ? (
                            <div className="h-1 rounded overflow-hidden" style={{ background: 'var(--bg-hover)' }}>
                              <div
                                className="h-full transition-all"
                                style={{
                                  width: `${item.mergeProgress * 100}%`,
                                  backgroundColor: mergeColor || '#f97316',
                                }}
                              />
                            </div>
                          ) : (
                            <div className="h-1" /> 
                          )}
                        </div>

                        {/* Hover tooltip */}
                        {isHovered && (
                          <div
                            className="absolute left-0 top-full mt-2 p-3 rounded-lg z-20 min-w-[200px]"
                            style={{
                              backgroundColor: 'var(--bg-tertiary)',
                              border: `1px solid ${color}40`,
                              boxShadow: 'var(--shadow-lg)',
                            }}
                          >
                            <div className="text-xs font-mono mb-2 break-all" style={{ color: 'var(--text-primary)' }}>
                              {item.id}
                            </div>
                            <div className="space-y-1 text-[10px]">
                              {Object.entries(item.metrics).map(([key, value]) => (
                                <div key={key} className="flex justify-between gap-4">
                                  <span style={{ color: 'var(--text-muted)' }}>{key}</span>
                                  <span className="font-mono" style={{ color: 'var(--text-secondary)' }}>{value}</span>
                                </div>
                              ))}
                            </div>
                          </div>
                        )}
                      </div>
                    );
                  }
                })}
              </div>
            </div>
          );
        })}
        </div>
      </div>
    </div>
  );
};

export default PartsVisualization2D;
