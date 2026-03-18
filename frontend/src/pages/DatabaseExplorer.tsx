/**
 * DatabaseExplorer - Hierarchical drill-down visualization
 * 
 * Levels: Databases → Tables → Partitions → Parts
 * Full-screen 3D with overlay UI
 */

import React, { useEffect, useCallback, useState, useMemo, useRef } from 'react';
import { useConnectionStore } from '../stores/connectionStore';
import { useDatabaseStore, databaseApi, formatBytes } from '../stores/databaseStore';
import type { PartDetailInfo } from '../stores/databaseStore';
import { mergeApi } from '../stores/mergeStore';
import type { MergeInfo } from '../stores/mergeStore';
import type { PartInfo } from '../stores/databaseStore';
import { useClickHouseServices } from '../providers/ClickHouseProvider';
import { useRefreshConfig, clampToAllowed } from '@tracehouse/ui-shared';
import { useRefreshSettingsStore, useGlobalLastUpdatedStore } from '../stores/refreshSettingsStore';
import { useUserPreferenceStore } from '../stores/userPreferenceStore';
import {
  Scene3D,
  createSceneConfig,
  ErrorBoundary3D,
  PartsVisualization2D,
  HierarchyVisualization2D,
} from '../components/3d';
import { extractErrorMessage } from '../utils/errorFormatters';
import { useCapabilityCheck } from '../components/shared/RequiresCapability';
import { PermissionGate } from '../components/shared/PermissionGate';
import { 
  HierarchyVisualization, 
  type HierarchyItem, 
  type HierarchyLevel 
} from '../components/3d/HierarchyVisualization';

// Shared UI components from @tracehouse/ui-shared

// Shared PartInspector component
import { PartInspector } from '../components/database/PartInspector';

// ============================================================================
// MERGE GROUP COLORS - Must match HierarchyVisualization.tsx
// ============================================================================

const MERGE_GROUP_COLORS = [
  { main: '#06b6d4', light: '#67e8f9' }, // Cyan
  { main: '#f59e0b', light: '#fcd34d' }, // Amber
  { main: '#ec4899', light: '#f9a8d4' }, // Pink/Magenta
  { main: '#3b82f6', light: '#93c5fd' }, // Blue
  { main: '#8b5cf6', light: '#c4b5fd' }, // Purple
  { main: '#14b8a6', light: '#5eead4' }, // Teal
  { main: '#f472b6', light: '#fbcfe8' }, // Light Pink
  { main: '#a78bfa', light: '#ddd6fe' }, // Violet
  // Extended palette to avoid color reuse
  { main: '#0891b2', light: '#22d3ee' }, // Dark Cyan
  { main: '#d97706', light: '#fbbf24' }, // Dark Amber
  { main: '#db2777', light: '#f472b6' }, // Dark Pink
  { main: '#2563eb', light: '#60a5fa' }, // Dark Blue
  { main: '#7c3aed', light: '#a78bfa' }, // Dark Purple
  { main: '#0d9488', light: '#2dd4bf' }, // Dark Teal
  { main: '#c026d3', light: '#e879f9' }, // Fuchsia
  { main: '#4f46e5', light: '#818cf8' }, // Indigo
  { main: '#0284c7', light: '#38bdf8' }, // Sky Blue
  { main: '#ea580c', light: '#fb923c' }, // Orange
  { main: '#9333ea', light: '#c084fc' }, // Purple 2
  { main: '#0e7490', light: '#06b6d4' }, // Cyan 2
];

// Level colors for breadcrumb - must match HierarchyVisualization.tsx
const LEVEL_COLORS: Record<HierarchyLevel, { main: string; glow: string }> = {
  databases: { main: '#7c3aed', glow: '#c4b5fd' },  // Purple
  tables: { main: '#2563eb', glow: '#93c5fd' },     // Blue
  partitions: { main: '#059669', glow: '#6ee7b7' }, // Emerald
  parts: { main: '#ec4899', glow: '#fbcfe8' },      // Pink/Magenta
};

// ============================================================================
// TYPES
// ============================================================================

interface BreadcrumbItem {
  level: HierarchyLevel;
  id: string;
  name: string;
}

// ============================================================================
// DATA TRANSFORMERS
// ============================================================================

function databasesToHierarchy(
  databases: { name: string; table_count?: number; total_bytes?: number }[],
  activeMerges: MergeInfo[] = []
): HierarchyItem[] {
  // Count merges per database
  const mergesByDb = new Map<string, number>();
  for (const m of activeMerges) {
    mergesByDb.set(m.database, (mergesByDb.get(m.database) || 0) + 1);
  }
  
  return databases
    .filter(db => !db.name.startsWith('_'))
    .map(db => {
      const mergeCount = mergesByDb.get(db.name) || 0;
      const tableCount = db.table_count ?? 0;
      const totalBytes = db.total_bytes ?? 0;
      // Health: 100 if no merges, decreases with merge-to-table ratio
      const healthScore = tableCount > 0
        ? Math.max(0, Math.round(100 - (mergeCount / tableCount) * 50))
        : (mergeCount > 0 ? 50 : 100);
      return {
        id: db.name,
        name: db.name,
        size: Math.max(totalBytes, 1000000),
        health: (mergeCount > 0 ? 'merging' : 'good') as 'good' | 'merging',
        healthScore,
        metrics: {
          tables: tableCount,
          size: formatBytes(totalBytes),
          ...(mergeCount > 0 ? { '⟳ merges': mergeCount } : {}),
        },
        childCount: tableCount,
        merging: mergeCount > 0,
      };
    });
}

function tablesToHierarchy(
  tables: { name: string; engine: string; total_rows: number; total_bytes: number; is_merge_tree: boolean }[],
  activeMerges: MergeInfo[] = []
): HierarchyItem[] {
  // Count merges per table
  const mergesByTable = new Map<string, number>();
  for (const m of activeMerges) {
    mergesByTable.set(m.table, (mergesByTable.get(m.table) || 0) + 1);
  }
  
  return tables
    .filter(t => t.is_merge_tree)
    .map(t => {
      const mergeCount = mergesByTable.get(t.name) || 0;
      const health = mergeCount > 0 ? 'merging' : t.total_rows > 1000000000 ? 'warning' : 'good';
      return {
        id: t.name,
        name: t.name,
        size: t.total_bytes || 1000,
        health: health as 'good' | 'warning' | 'critical' | 'merging',
        healthScore: health === 'good' ? 90 : health === 'merging' ? 80 : 65,
        metrics: {
          engine: t.engine,
          rows: t.total_rows?.toLocaleString() || '0',
          size: formatBytes(t.total_bytes || 0),
          ...(mergeCount > 0 ? { '⟳ merges': mergeCount } : {}),
        },
        merging: mergeCount > 0,
      };
    });
}

function partsToPartitions(parts: PartInfo[], activeMerges: MergeInfo[] = []): HierarchyItem[] {
  const groups = new Map<string, PartInfo[]>();
  for (const part of parts) {
    const id = part.partition_id || 'default';
    if (!groups.has(id)) groups.set(id, []);
    groups.get(id)!.push(part);
  }
  
  // Helper to extract partition from result_part_name
  const getPartitionFromPartName = (partName: string): string => {
    const parts = partName.split('_');
    return parts[0] || '';
  };
  
  // Count merges per partition
  const mergesByPartition = new Map<string, number>();
  for (const merge of activeMerges) {
    const partition = getPartitionFromPartName(merge.result_part_name);
    mergesByPartition.set(partition, (mergesByPartition.get(partition) || 0) + 1);
  }
  
  return [...groups.entries()]
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([id, partitionParts]) => {
      const totalBytes = partitionParts.reduce((sum, p) => sum + p.bytes_on_disk, 0);
      const totalRows = partitionParts.reduce((sum, p) => sum + p.rows, 0);
      const l0Count = partitionParts.filter(p => p.level === 0).length;
      const l0Ratio = partitionParts.length > 0 ? l0Count / partitionParts.length : 0;
      
      const activeMergeCount = mergesByPartition.get(id) || 0;
      const hasMerges = activeMergeCount > 0;
      
      // If merging, show as 'merging' health status
      const health = hasMerges ? 'merging' : l0Ratio > 0.5 ? 'critical' : l0Ratio > 0.2 ? 'warning' : 'good';
      const healthScore = Math.round(100 - l0Ratio * 80);
      
      return {
        id,
        name: id,
        size: totalBytes,
        health,
        healthScore,
        metrics: {
          parts: partitionParts.length,
          rows: totalRows.toLocaleString(),
          size: formatBytes(totalBytes),
          'L0 (unmerged)': `${l0Count} (${Math.round(l0Ratio * 100)}%)`,
          'max level': Math.max(...partitionParts.map(p => p.level), 0),
          ...(activeMergeCount > 0 ? { '⟳ merges': activeMergeCount } : {}),
        },
        childCount: partitionParts.length,
        // Mark as merging for visual indicator
        merging: hasMerges,
        issues: l0Ratio > 0.3 ? [`High unmerged ratio: ${Math.round(l0Ratio * 100)}%`] : undefined,
      };
    });
}

function partsToHierarchy(
  parts: PartInfo[], 
  partitionId: string,
  activeMerges: MergeInfo[] = []
): HierarchyItem[] {
  // Build a map of parts being merged -> merge info
  const mergingParts = new Map<string, { resultPart: string; progress: number; mergeId: string }>();
  
  for (const merge of activeMerges) {
    for (const sourcePart of merge.source_part_names) {
      mergingParts.set(sourcePart, {
        resultPart: merge.result_part_name,
        progress: merge.progress,
        mergeId: `${merge.database}.${merge.table}:${merge.result_part_name}`,
      });
    }
  }
  
  const filteredParts = parts.filter(p => p.partition_id === partitionId);
  
  return filteredParts
    .sort((a, b) => b.bytes_on_disk - a.bytes_on_disk)
    .map(p => {
      const mergeInfo = mergingParts.get(p.name);
      const isMerging = !!mergeInfo;
      const health = isMerging ? 'merging' : p.level === 0 ? 'warning' : 'good';
      
      return {
        id: p.name,
        name: p.name.length > 20 ? p.name.slice(0, 8) + '...' + p.name.slice(-8) : p.name,
        size: p.bytes_on_disk,
        health: health as 'good' | 'warning' | 'critical' | 'merging',
        healthScore: isMerging ? Math.round(mergeInfo.progress * 100) : p.level === 0 ? 60 : 90,
        metrics: {
          level: `L${p.level}`,
          rows: p.rows.toLocaleString(),
          size: formatBytes(p.bytes_on_disk),
          partition: p.partition_id,
          ...(isMerging ? {
            status: 'MERGING',
            progress: `${Math.round(mergeInfo.progress * 100)}%`,
            'result →': mergeInfo.resultPart.length > 15 
              ? mergeInfo.resultPart.slice(0, 6) + '...' + mergeInfo.resultPart.slice(-6)
              : mergeInfo.resultPart,
          } : {}),
        },
        // Custom data for visualization
        merging: isMerging,
        mergeProgress: mergeInfo?.progress,
        mergeTarget: mergeInfo?.resultPart,
        mergeId: mergeInfo?.mergeId,
      };
    });
}

// ============================================================================
// UI COMPONENTS
// ============================================================================

const Legend: React.FC<{ level: HierarchyLevel; viewMode?: string }> = ({ level: _level, viewMode }) => {
  return (
    <div className="absolute top-20 right-4 backdrop-blur-xl rounded-xl px-4 py-3 text-xs" style={{ background: 'var(--bg-tertiary)', border: '1px solid var(--border-primary)' }}>
      <div className="flex items-center gap-4">
        <div className="flex items-center gap-2">
          <div className="w-2.5 h-2.5 rounded-full bg-green-400 shadow-lg shadow-green-400/50" />
          <span style={{ color: 'var(--text-secondary)' }}>OK</span>
        </div>
        <div className="flex items-center gap-2">
          <div className="w-2.5 h-2.5 rounded-full bg-amber-400 shadow-lg shadow-amber-400/50" />
          <span style={{ color: 'var(--text-secondary)' }}>Warn</span>
        </div>
        <div className="flex items-center gap-2">
          <div className="w-2.5 h-2.5 rounded-full bg-red-400 shadow-lg shadow-red-400/50" />
          <span style={{ color: 'var(--text-secondary)' }}>Crit</span>
        </div>
        <div className="flex items-center gap-2">
          <div className="w-2.5 h-2.5 rounded-full bg-orange-400 shadow-lg shadow-orange-400/50 animate-pulse" />
          <span style={{ color: 'var(--text-secondary)' }}>Merging</span>
        </div>
        {viewMode === '3d' && (
          <div className="pl-4" style={{ borderLeft: '1px solid var(--border-primary)', color: 'var(--text-muted)' }}>
            Height = Size
          </div>
        )}
      </div>
    </div>
  );
};

// ============================================================================
// MERGE INFO WALL - Rolling display of active merges
// ============================================================================

interface MergeInfoWallProps {
  merges: MergeInfo[];
  currentDatabase?: string | null;
  currentTable?: string | null;
  currentPartition?: string | null;
  level: HierarchyLevel;
  highlightedMergeId?: string | null;
  mergeColorMap: Map<string, number>;  // Shared color assignments
  onMergeHover?: (mergeId: string | null) => void;
}

const MergeInfoWall: React.FC<MergeInfoWallProps> = ({ merges, currentDatabase, currentTable, currentPartition, level, highlightedMergeId, mergeColorMap, onMergeHover }) => {
  // Track recently completed merges to show them briefly after completion
  const [recentlyCompleted, setRecentlyCompleted] = useState<Map<string, { merge: MergeInfo; completedAt: number }>>(new Map());
  const prevMergesRef = useRef<Map<string, MergeInfo>>(new Map());
  
  // Detect completed merges (were in previous list but not in current)
  useEffect(() => {
    const currentIds = new Set(merges.map(m => `${m.database}.${m.table}:${m.result_part_name}`));
    const now = Date.now();
    
    // Find merges that completed
    prevMergesRef.current.forEach((merge, id) => {
      if (!currentIds.has(id)) {
        // This merge completed
        setRecentlyCompleted(prev => {
          const next = new Map(prev);
          next.set(id, { merge: { ...merge, progress: 1 }, completedAt: now });
          return next;
        });
      }
    });
    
    // Update previous merges ref
    prevMergesRef.current = new Map(merges.map(m => [`${m.database}.${m.table}:${m.result_part_name}`, m]));
    
    // Clean up old completed merges (older than 8 seconds)
    const cleanup = setInterval(() => {
      setRecentlyCompleted(prev => {
        const next = new Map(prev);
        const cutoff = Date.now() - 8000;
        next.forEach((val, key) => {
          if (val.completedAt < cutoff) next.delete(key);
        });
        return next;
      });
    }, 1000);
    
    return () => clearInterval(cleanup);
  }, [merges]);
  
  // Helper to extract partition from result_part_name (format: partition_minBlock_maxBlock_level)
  const getPartitionFromPartName = (partName: string): string => {
    // Part names are like "202601_426_441_1" where 202601 is the partition
    const parts = partName.split('_');
    return parts[0] || '';
  };
  
  // Filter to relevant merges based on current level
  const relevantMerges = useMemo(() => {
    let filtered = merges;
    
    if (currentDatabase && currentTable) {
      filtered = merges.filter(m => m.database === currentDatabase && m.table === currentTable);
      
      // At parts level, also filter by partition
      if (level === 'parts' && currentPartition) {
        filtered = filtered.filter(m => getPartitionFromPartName(m.result_part_name) === currentPartition);
      }
    } else if (currentDatabase) {
      filtered = merges.filter(m => m.database === currentDatabase);
    }
    // Sort by elapsed time (oldest first = left)
    return [...filtered].sort((a, b) => b.elapsed - a.elapsed);
  }, [merges, currentDatabase, currentTable, currentPartition, level]);
  
  // Get completed merges (sorted by completion time, oldest first)
  const completedMerges = useMemo(() => {
    return Array.from(recentlyCompleted.values())
      .filter(({ merge }) => {
        if (currentDatabase && currentTable) {
          const matchesTable = merge.database === currentDatabase && merge.table === currentTable;
          if (level === 'parts' && currentPartition) {
            return matchesTable && getPartitionFromPartName(merge.result_part_name) === currentPartition;
          }
          return matchesTable;
        } else if (currentDatabase) {
          return merge.database === currentDatabase;
        }
        return true;
      })
      .sort((a, b) => a.completedAt - b.completedAt)
      .slice(0, 6);
  }, [recentlyCompleted, currentDatabase, currentTable, currentPartition, level]);
  
  // Get color for a merge from shared color map
  const getMergeColor = (mergeId: string): number => {
    return mergeColorMap.get(mergeId) ?? 0;
  };
  
  if (relevantMerges.length === 0 && completedMerges.length === 0) return null;
  
  // At databases/tables/partitions level, show only a compact hint badge — not full cards
  if (level !== 'parts') {
    // Summarize by table for a compact breakdown
    const mergesByTable = new Map<string, number>();
    for (const m of relevantMerges) {
      const key = `${m.database}.${m.table}`;
      mergesByTable.set(key, (mergesByTable.get(key) || 0) + 1);
    }
    const avgProgress = relevantMerges.length > 0
      ? Math.round(relevantMerges.reduce((s, m) => s + m.progress, 0) / relevantMerges.length * 100)
      : 0;
    
    return (
      <div 
        className="absolute top-28 right-4 pointer-events-auto"
        style={{ zIndex: 1000 }}
      >
        <div
          style={{
            background: 'var(--bg-tertiary)',
            border: '1px solid rgba(249,115,22,0.3)',
            borderRadius: 10,
            padding: '10px 14px',
            backdropFilter: 'blur(12px)',
            boxShadow: '0 4px 20px rgba(0,0,0,0.15)',
            minWidth: 160,
            maxWidth: 240,
          }}
        >
          {/* Header */}
          <div className="flex items-center gap-2 mb-2">
            <div className="w-2 h-2 rounded-full bg-orange-400 animate-pulse" />
            <span 
              className="text-[11px] font-semibold uppercase tracking-wide"
              style={{ color: '#fb923c' }}
            >
              {relevantMerges.length} Active Merge{relevantMerges.length !== 1 ? 's' : ''}
            </span>
          </div>
          
          {/* Average progress bar */}
          <div style={{ marginBottom: 6 }}>
            <div className="flex items-center justify-between text-[9px] mb-1" style={{ color: 'var(--text-muted)' }}>
              <span>Avg progress</span>
              <span className="font-mono" style={{ color: '#fb923c' }}>{avgProgress}%</span>
            </div>
            <div style={{ height: 3, borderRadius: 2, background: 'var(--bg-secondary)' }}>
              <div style={{
                height: '100%', borderRadius: 2, width: `${avgProgress}%`,
                background: 'linear-gradient(90deg, #f97316, #fb923c)',
                transition: 'width 0.5s ease',
              }} />
            </div>
          </div>
          
          {/* Per-table breakdown (compact) */}
          {mergesByTable.size > 0 && (
            <div className="space-y-0.5">
              {[...mergesByTable.entries()].slice(0, 4).map(([table, count]) => (
                <div key={table} className="flex items-center justify-between text-[9px] font-mono" style={{ color: 'var(--text-tertiary)' }}>
                  <span className="truncate" style={{ maxWidth: 140 }} title={table}>{table}</span>
                  <span style={{ color: '#fb923c' }}>×{count}</span>
                </div>
              ))}
              {mergesByTable.size > 4 && (
                <div className="text-[9px]" style={{ color: 'var(--text-muted)' }}>
                  +{mergesByTable.size - 4} more tables
                </div>
              )}
            </div>
          )}
          
          {/* Completed count if any */}
          {completedMerges.length > 0 && (
            <div className="flex items-center gap-1.5 mt-2 pt-2" style={{ borderTop: '1px solid var(--border-secondary)' }}>
              <div className="w-1.5 h-1.5 rounded-full bg-green-400" />
              <span className="text-[9px]" style={{ color: '#4ade80' }}>
                {completedMerges.length} recently completed
              </span>
            </div>
          )}
        </div>
      </div>
    );
  }
  
  // At parts level (inside a partition), show full merge detail cards
  return (
    <>
      {/* Active Merges - Top horizontal bar */}
      {relevantMerges.length > 0 && (
        <div className="absolute top-28 left-4 right-4 pointer-events-none" style={{ zIndex: 1000 }}>
          <div className="flex items-center gap-2 text-[10px] uppercase tracking-[0.15em] font-medium mb-2" style={{ color: 'var(--text-muted)' }}>
            <div className="w-1.5 h-1.5 rounded-full bg-orange-400 animate-pulse" />
            <span>Active Merges</span>
            <span className="text-orange-400 font-mono">({relevantMerges.length})</span>
            <div className="flex-1 h-px bg-gradient-to-r from-orange-500/30 to-transparent ml-2" />
          </div>
          
          <div 
            className="flex gap-2 overflow-x-auto pb-2 pointer-events-auto scrollbar-thin" 
            style={{ maxWidth: '100%', overscrollBehavior: 'contain' }}
          >
            {relevantMerges.map((merge, i) => {
              const mergeId = `${merge.database}.${merge.table}:${merge.result_part_name}`;
              const colorIndex = getMergeColor(mergeId);
              return (
                <MergeInfoCardCompact 
                  key={mergeId} 
                  merge={merge} 
                  completed={false}
                  isOldest={i === 0}
                  isHighlighted={highlightedMergeId === mergeId}
                  colorIndex={colorIndex}
                  onHover={(hovering) => onMergeHover?.(hovering ? mergeId : null)}
                />
              );
            })}
          </div>
        </div>
      )}
      
      {/* Completed Merges - Bottom horizontal bar */}
      {completedMerges.length > 0 && (
        <div className="absolute bottom-4 left-4 right-4 pointer-events-none" style={{ zIndex: 1000 }}>
          <div className="flex items-center gap-2 text-[10px] uppercase tracking-[0.15em] font-medium mb-2" style={{ color: 'var(--text-muted)' }}>
            <div className="w-1.5 h-1.5 rounded-full bg-green-400" />
            <span>Completed</span>
            <span className="text-green-400 font-mono">({completedMerges.length})</span>
            <div className="flex-1 h-px bg-gradient-to-r from-green-500/30 to-transparent ml-2" />
          </div>
          
          <div 
            className="flex gap-2 overflow-x-auto pb-2 pointer-events-auto scrollbar-thin"
            style={{ overscrollBehavior: 'contain' }}
          >
            {completedMerges.map(({ merge }, i) => {
              const mergeId = `${merge.database}.${merge.table}:${merge.result_part_name}`;
              const colorIndex = getMergeColor(mergeId);
              return (
                <MergeInfoCardCompact 
                  key={`completed-${mergeId}`} 
                  merge={merge} 
                  completed={true}
                  isOldest={i === 0}
                  isHighlighted={highlightedMergeId === mergeId}
                  colorIndex={colorIndex}
                  onHover={(hovering) => onMergeHover?.(hovering ? mergeId : null)}
                />
              );
            })}
          </div>
        </div>
      )}
    </>
  );
};

// Compact horizontal merge card with expandable hover
interface MergeInfoCardCompactProps {
  merge: MergeInfo;
  completed?: boolean;
  isOldest?: boolean;
  isHighlighted?: boolean;
  colorIndex?: number;  // Index into MERGE_GROUP_COLORS
  onHover?: (hovering: boolean) => void;
}

const MergeInfoCardCompact: React.FC<MergeInfoCardCompactProps> = ({ merge, completed, isOldest, isHighlighted, colorIndex = 0, onHover }) => {
  const [isHovered, setIsHovered] = useState(false);
  const progressPct = Math.round(merge.progress * 100);
  
  // Get merge group color
  const mergeColor = MERGE_GROUP_COLORS[colorIndex % MERGE_GROUP_COLORS.length];
  
  // Calculate throughput (bytes/sec) - use compressed bytes * progress since bytes_read_uncompressed may not be populated
  const bytesProcessed = merge.total_size_bytes_compressed * merge.progress;
  const throughput = merge.elapsed > 0 ? bytesProcessed / merge.elapsed : 0;
  
  // Calculate cost estimate based on memory × time × data size
  const memoryGB = merge.memory_usage / (1024 * 1024 * 1024);
  const dataGB = merge.total_size_bytes_compressed / (1024 * 1024 * 1024);
  const cost = merge.elapsed * memoryGB * (1 + dataGB);
  const costLabel = cost < 0.5 ? 'Low' : cost < 5 ? 'Medium' : 'High';
  const costColor = cost < 0.5 ? 'text-cyan-400' : cost < 5 ? 'text-amber-400' : 'text-rose-400';
  const costBg = cost < 0.5 ? 'bg-cyan-500/20' : cost < 5 ? 'bg-amber-500/20' : 'bg-rose-500/20';
  
  const handleMouseEnter = () => {
    setIsHovered(true);
    onHover?.(true);
  };
  
  const handleMouseLeave = () => {
    setIsHovered(false);
    onHover?.(false);
  };
  
  return (
    <div 
      className={`
        flex-shrink-0 rounded border overflow-hidden
        transition-all duration-300 pointer-events-auto cursor-pointer
        ${isHovered ? 'w-[340px]' : 'w-60'}
        ${isHighlighted ? 'ring-2' : ''}
        ${isOldest && !completed && !isHighlighted ? 'ring-1' : ''}
      `}
      style={{
        backgroundColor: 'var(--bg-tertiary)',
        borderColor: completed ? 'rgba(16, 185, 129, 0.4)' : `${mergeColor.main}80`,
        boxShadow: isHighlighted 
          ? `0 0 25px ${mergeColor.main}50` 
          : completed 
            ? '0 0 15px rgba(16, 185, 129, 0.15)' 
            : 'var(--shadow-lg)',
        zIndex: 100,
        position: 'relative',
        ...(isHighlighted ? { '--tw-ring-color': `${mergeColor.main}60` } as React.CSSProperties : {}),
        ...(isOldest && !completed && !isHighlighted ? { '--tw-ring-color': 'var(--border-primary)' } as React.CSSProperties : {}),
      }}
      onMouseEnter={handleMouseEnter}
      onMouseLeave={handleMouseLeave}
    >
      {/* Progress bar at top - uses merge group color */}
      <div style={{ height: 6, background: 'var(--bg-hover)' }}>
        <div 
          className="h-full transition-all duration-300"
          style={{ 
            width: `${progressPct}%`,
            background: completed ? '#10b981' : mergeColor.main,
          }}
        />
      </div>
      
      <div className="p-3">
        {/* Header: Merge type + progress */}
        <div className="flex items-start justify-between gap-2 mb-2">
          <div className="flex-1 min-w-0">
            {/* Merge type with status indicator */}
            <div className="flex items-center gap-2">
              <span 
                className="text-[10px] font-semibold uppercase tracking-wide"
                style={{ color: mergeColor.light }}
              >
                {merge.merge_type} {merge.is_mutation ? '(Mutation)' : ''}
              </span>
              {/* Small status indicator */}
              {completed ? (
                <span className="text-[9px] px-1.5 py-0.5 rounded bg-emerald-500/20 text-emerald-400 font-medium">
                  ✓ Done
                </span>
              ) : (
                <span className="w-1.5 h-1.5 rounded-full bg-orange-400 animate-pulse" title="In progress" />
              )}
            </div>
            {/* Result part name */}
            <div 
              className="text-[11px] font-mono mt-0.5 truncate" 
              style={{ color: mergeColor.light }}
              title={merge.result_part_name}
            >
              → {isHovered 
                  ? merge.result_part_name 
                  : merge.result_part_name.length > 18 
                    ? merge.result_part_name.slice(0, 8) + '…' + merge.result_part_name.slice(-8) 
                    : merge.result_part_name}
            </div>
          </div>
          <div 
            className="text-xl font-bold font-mono tabular-nums"
            style={{ color: completed ? '#10b981' : mergeColor.light }}
          >
            {progressPct}%
          </div>
        </div>
        
        {/* Compact stats row */}
        <div className="flex items-center gap-3 text-[10px] font-mono" style={{ color: 'var(--text-tertiary)' }}>
          <span>{merge.num_parts} parts</span>
          <span style={{ color: 'var(--text-muted)' }}>|</span>
          <span>{formatBytes(merge.total_size_bytes_compressed)}</span>
          <span style={{ color: 'var(--text-muted)' }}>|</span>
          <span>{merge.elapsed.toFixed(1)}s</span>
        </div>
        
        {/* Expanded details on hover */}
        {isHovered && (
          <div className="mt-3 pt-3 space-y-3" style={{ borderTop: '1px solid var(--border-primary)' }}>
            {/* Source parts */}
            <div>
              <div className="text-[9px] uppercase tracking-wide mb-1.5 font-medium" style={{ color: 'var(--text-muted)' }}>Source Parts ({merge.num_parts})</div>
              <div className="flex flex-wrap gap-1 max-h-14 overflow-y-auto">
                {merge.source_part_names.map((part, i) => (
                  <span 
                    key={i}
                    className="px-1.5 py-0.5 rounded text-[9px] font-mono"
                    style={{ 
                      background: `${mergeColor.main}20`,
                      border: `1px solid ${mergeColor.main}40`,
                      color: mergeColor.light,
                    }}
                    title={part}
                  >
                    {part.length > 16 ? part.slice(0, 7) + '…' + part.slice(-7) : part}
                  </span>
                ))}
              </div>
            </div>
            
            {/* Resource usage grid */}
            <div className="grid grid-cols-2 gap-x-4 gap-y-2 text-[10px]">
              <div className="tooltip-trigger" data-tooltip="Current memory used by this merge">
                <div className="mb-0.5" style={{ color: 'var(--text-muted)' }}>Memory</div>
                <div className="font-mono font-medium" style={{ color: 'var(--text-secondary)' }}>{formatBytes(merge.memory_usage)}</div>
              </div>
              <div className="tooltip-trigger tooltip-wrap" data-tooltip="Compressed bytes processed per second">
                <div className="mb-0.5" style={{ color: 'var(--text-muted)' }}>Throughput</div>
                <div className="font-mono font-medium" style={{ color: 'var(--text-secondary)' }}>{formatBytes(throughput)}/s</div>
              </div>
              <div className="tooltip-trigger" data-tooltip="Rows read from source parts">
                <div className="mb-0.5" style={{ color: 'var(--text-muted)' }}>Rows (Target)</div>
                <div className="font-mono font-medium" style={{ color: 'var(--text-secondary)' }}>{merge.rows_read.toLocaleString()}</div>
              </div>
              <div className="tooltip-trigger tooltip-wrap" data-tooltip="Compressed bytes processed so far">
                <div className="mb-0.5" style={{ color: 'var(--text-muted)' }}>Bytes Processed</div>
                <div className="font-mono font-medium" style={{ color: 'var(--text-secondary)' }}>{formatBytes(bytesProcessed)}</div>
              </div>
              {(merge.bytes_read_uncompressed || 0) > 0 && (
                <div className="tooltip-trigger tooltip-wrap" data-tooltip="Uncompressed data read into memory">
                  <div className="mb-0.5" style={{ color: 'var(--text-muted)' }}>Uncompressed</div>
                  <div className="font-mono font-medium" style={{ color: 'var(--text-secondary)' }}>{formatBytes(merge.bytes_read_uncompressed || 0)}</div>
                </div>
              )}
              {merge.merge_algorithm === 'Vertical' && (merge.columns_written || 0) > 0 && (
                <div className="tooltip-trigger" data-tooltip="Columns written in vertical merge">
                  <div className="mb-0.5" style={{ color: 'var(--text-muted)' }}>Columns</div>
                  <div className="font-mono font-medium" style={{ color: 'var(--text-secondary)' }}>{merge.columns_written}</div>
                </div>
              )}
            </div>
            
            {/* Algorithm + Cost footer */}
            <div className="flex items-center justify-between pt-2" style={{ borderTop: '1px solid var(--border-secondary)' }}>
              <div className="flex items-center gap-2">
                <span className="text-[9px] uppercase" style={{ color: 'var(--text-muted)' }}>Algorithm</span>
                <span className="text-[10px] font-mono" style={{ color: 'var(--text-tertiary)' }}>{merge.merge_algorithm}</span>
              </div>
              <div className={`text-[10px] px-2 py-0.5 rounded ${costBg} ${costColor} font-semibold`}>
                {costLabel} Cost
              </div>
            </div>
            
            {/* Thread ID if available */}
            {(merge.thread_id || 0) > 0 && (
              <div className="text-[9px] font-mono" style={{ color: 'var(--text-disabled)' }}>
                Thread: {merge.thread_id}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
};

// ============================================================================
// PART INSPECTOR - Beautiful centered modal with rich visualizations
// ============================================================================

// ============================================================================
// MAIN COMPONENT
// ============================================================================

export const DatabaseExplorer: React.FC = () => {
  const { activeProfileId, profiles, setConnectionFormOpen } = useConnectionStore();
  const {
    databases, tables, tableParts,
    setDatabases, setTables, setTableParts,
    setError, clearAll,
  } = useDatabaseStore();

  // Navigation state
  const [breadcrumb, setBreadcrumb] = useState<BreadcrumbItem[]>([
    { level: 'databases', id: 'root', name: 'Databases' }
  ]);
  const [selectedDatabase, setSelectedDatabase] = useState<string | null>(null);
  const [selectedTable, setSelectedTable] = useState<string | null>(null);
  const [selectedPartition, setSelectedPartition] = useState<string | null>(null);
  
  // View mode state (3D or 2D) - synced with global preference
  const { preferredViewMode } = useUserPreferenceStore();
  const viewMode = preferredViewMode;
  
  // Performance mode state for 3D view
  const [performanceMode, setPerformanceMode] = useState(false);
  
  // Hover state
  const [_hoveredItem, setHoveredItem] = useState<HierarchyItem | null>(null);
  
  // Loading state
  const [isLoading, setIsLoading] = useState(false);
  
  // Active merges for highlighting parts being merged
  const [activeMerges, setActiveMerges] = useState<MergeInfo[]>([]);
  
  // Auto-refresh state
  const refreshConfig = useRefreshConfig();
  const { refreshRateSeconds: globalRate } = useRefreshSettingsStore();
  const manualRefreshTick = useGlobalLastUpdatedStore(s => s.manualRefreshTick);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const refreshInterval = globalRate > 0 ? clampToAllowed(globalRate, refreshConfig) * 1000 : 2000;
  const [_lastRefresh, setLastRefresh] = useState<Date | null>(null);
  const pollingRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Part inspector state
  const [_selectedPart, setSelectedPart] = useState<string | null>(null);
  const [partDetail, setPartDetail] = useState<PartDetailInfo | null>(null);
  const [isLoadingPartDetail, setIsLoadingPartDetail] = useState(false);

  // Highlighted merge state (for hover highlighting in 3D view)
  const [highlightedMergeId, setHighlightedMergeId] = useState<string | null>(null);

  // Shared merge color assignments - ensures cards and 3D view use same colors
  const [mergeColorAssignments, setMergeColorAssignments] = useState<Map<string, number>>(new Map());
  const nextMergeColorRef = useRef(0);

  // Assign colors to merges - shared between cards and 3D view
  useEffect(() => {
    let hasChanges = false;
    const newColors = new Map(mergeColorAssignments);
    
    activeMerges.forEach((merge) => {
      const mergeId = `${merge.database}.${merge.table}:${merge.result_part_name}`;
      if (!newColors.has(mergeId)) {
        newColors.set(mergeId, nextMergeColorRef.current);
        nextMergeColorRef.current = (nextMergeColorRef.current + 1) % MERGE_GROUP_COLORS.length;
        hasChanges = true;
      }
    });
    
    // Clean up old entries if too many
    if (newColors.size > 100) {
      const currentIds = new Set(activeMerges.map(m => `${m.database}.${m.table}:${m.result_part_name}`));
      newColors.forEach((_, id) => {
        if (!currentIds.has(id)) {
          newColors.delete(id);
          hasChanges = true;
        }
      });
    }
    
    if (hasChanges) {
      setMergeColorAssignments(newColors);
    }
  }, [activeMerges, mergeColorAssignments]);

  // Clear highlighted merge if it no longer exists in active merges
  useEffect(() => {
    if (highlightedMergeId && activeMerges.length > 0) {
      const mergeIds = new Set(activeMerges.map(m => `${m.database}.${m.table}:${m.result_part_name}`));
      if (!mergeIds.has(highlightedMergeId)) {
        setHighlightedMergeId(null);
      }
    }
  }, [activeMerges, highlightedMergeId]);

  const activeProfile = profiles.find(p => p.id === activeProfileId);
  const isConnected = activeProfile?.is_connected ?? false;
  
  // Get services from ClickHouseProvider
  const services = useClickHouseServices();
  const { available: hasSystemDatabases, probing: isCapProbing } = useCapabilityCheck(['system_databases']);

  // Current level
  const currentLevel = breadcrumb[breadcrumb.length - 1].level;

  // Fetch databases
  const fetchDatabases = useCallback(async (silent = false) => {
    if (!services || !isConnected) return;
    if (!silent) setIsLoading(true);
    try {
      const dbs = await databaseApi.fetchDatabases(services.databaseExplorer);
      setDatabases(dbs);
      setLastRefresh(new Date());
      useGlobalLastUpdatedStore.getState().touch();
    } catch (err) {
      if (!silent) setError(extractErrorMessage(err, 'Failed to fetch databases'));
    } finally {
      if (!silent) setIsLoading(false);
    }
  }, [services, isConnected]);

  // Fetch tables
  const fetchTables = useCallback(async (database: string, silent = false) => {
    if (!services || !isConnected) return;
    if (!silent) setIsLoading(true);
    try {
      const tbls = await databaseApi.fetchTables(services.databaseExplorer, database);
      setTables(tbls);
      setLastRefresh(new Date());
      useGlobalLastUpdatedStore.getState().touch();
    } catch (err) {
      if (!silent) setError(extractErrorMessage(err, 'Failed to fetch tables'));
    } finally {
      if (!silent) setIsLoading(false);
    }
  }, [services, isConnected]);

  // Fetch parts
  const fetchParts = useCallback(async (database: string, table: string, silent = false) => {
    if (!services || !isConnected) return;
    if (!silent) setIsLoading(true);
    try {
      const parts = await databaseApi.fetchTableParts(services.databaseExplorer, database, table);
      setTableParts(parts);
      setLastRefresh(new Date());
      useGlobalLastUpdatedStore.getState().touch();
    } catch (err) {
      if (!silent) setError(extractErrorMessage(err, 'Failed to fetch parts'));
    } finally {
      if (!silent) setIsLoading(false);
    }
  }, [services, isConnected]);

  // Fetch active merges (for highlighting parts being merged)
  const fetchActiveMerges = useCallback(async () => {
    if (!services || !isConnected) return;
    try {
      const merges = await mergeApi.fetchActiveMerges(services.mergeTracker);
      setActiveMerges(merges);
    } catch (err) {
      // Silent fail - merges are optional enhancement
      console.warn('Failed to fetch active merges:', err);
    }
  }, [services, isConnected]);

  // Initial load
  useEffect(() => {
    if (!services || !isConnected) {
      clearAll();
      return;
    }
    if (isCapProbing || !hasSystemDatabases) return;
    fetchDatabases();
    fetchActiveMerges(); // Also fetch merges for the info wall
  }, [services, isConnected, isCapProbing, hasSystemDatabases]);

  // Auto-refresh effect - refresh data based on current level
  useEffect(() => {
    if (!autoRefresh || globalRate === 0 || !services || !isConnected) {
      if (pollingRef.current) {
        clearInterval(pollingRef.current);
        pollingRef.current = null;
      }
      return;
    }

    const refreshData = () => {
      // Always fetch active merges for the info wall
      fetchActiveMerges();
      
      switch (currentLevel) {
        case 'databases':
          fetchDatabases(true);
          break;
        case 'tables':
          if (selectedDatabase) fetchTables(selectedDatabase, true);
          break;
        case 'partitions':
        case 'parts':
          if (selectedDatabase && selectedTable) {
            fetchParts(selectedDatabase, selectedTable, true);
          }
          break;
      }
    };

    // Start polling
    pollingRef.current = setInterval(refreshData, refreshInterval);

    return () => {
      if (pollingRef.current) {
        clearInterval(pollingRef.current);
        pollingRef.current = null;
      }
    };
  }, [autoRefresh, refreshInterval, globalRate, currentLevel, selectedDatabase, selectedTable, services, isConnected, fetchDatabases, fetchTables, fetchParts, fetchActiveMerges, manualRefreshTick]);

  // Transform data to hierarchy items
  const hierarchyItems = useMemo((): HierarchyItem[] => {
    switch (currentLevel) {
      case 'databases':
        return databasesToHierarchy(databases, activeMerges);
      case 'tables':
        return tablesToHierarchy(tables, activeMerges.filter(m => m.database === selectedDatabase));
      case 'partitions':
        // Pass active merges to show which partitions have merges
        const tableMerges = activeMerges.filter(
          m => m.database === selectedDatabase && m.table === selectedTable
        );
        return partsToPartitions(tableParts, tableMerges);
      case 'parts':
        // Filter merges to only those for the current table
        const relevantMerges = activeMerges.filter(
          m => m.database === selectedDatabase && m.table === selectedTable
        );
        return selectedPartition ? partsToHierarchy(tableParts, selectedPartition, relevantMerges) : [];
      default:
        return [];
    }
  }, [currentLevel, databases, tables, tableParts, selectedPartition, activeMerges, selectedDatabase, selectedTable]);

  // Handle item click (drill down)
  const handleItemClick = useCallback(async (item: HierarchyItem) => {
    switch (currentLevel) {
      case 'databases':
        setSelectedDatabase(item.id);
        // Fetch data FIRST, then update breadcrumb
        await fetchTables(item.id);
        setBreadcrumb(prev => [...prev, { level: 'tables', id: item.id, name: item.name }]);
        break;
      case 'tables':
        setSelectedTable(item.id);
        if (selectedDatabase) {
          // Fetch data FIRST, then update breadcrumb
          await fetchParts(selectedDatabase, item.id);
          setBreadcrumb(prev => [...prev, { level: 'partitions', id: item.id, name: item.name }]);
          // Also fetch active merges to highlight parts being merged
          fetchActiveMerges();
        }
        break;
      case 'partitions':
        setSelectedPartition(item.id);
        setBreadcrumb(prev => [...prev, { level: 'parts', id: item.id, name: item.name }]);
        // Refresh merges when drilling into parts
        fetchActiveMerges();
        break;
      case 'parts':
        // Open part inspector with detailed info
        if (selectedDatabase && selectedTable && services) {
          setSelectedPart(item.id);
          setIsLoadingPartDetail(true);
          try {
            const detail = await databaseApi.fetchPartDetail(
              services.databaseExplorer,
              selectedDatabase,
              selectedTable,
              item.id
            );
            setPartDetail(detail);
          } catch (err) {
            console.error('Failed to fetch part detail:', err);
            setPartDetail(null);
          } finally {
            setIsLoadingPartDetail(false);
          }
        }
        break;
    }
    setHoveredItem(null);
  }, [currentLevel, selectedDatabase, selectedTable, services, fetchTables, fetchParts, fetchActiveMerges]);

  // Close part inspector
  const closePartInspector = useCallback(() => {
    setSelectedPart(null);
    setPartDetail(null);
  }, []);

  // Handle breadcrumb navigation
  const handleNavigate = useCallback((index: number) => {
    const newBreadcrumb = breadcrumb.slice(0, index + 1);
    setBreadcrumb(newBreadcrumb);
    
    const targetLevel = newBreadcrumb[newBreadcrumb.length - 1].level;
    
    // Reset state based on navigation
    if (targetLevel === 'databases') {
      setSelectedDatabase(null);
      setSelectedTable(null);
      setSelectedPartition(null);
      setTables([]);
      setTableParts([]);
    } else if (targetLevel === 'tables') {
      setSelectedTable(null);
      setSelectedPartition(null);
      setTableParts([]);
    } else if (targetLevel === 'partitions') {
      setSelectedPartition(null);
    }
    
    setHoveredItem(null);
  }, [breadcrumb]);

  // No connection
  if (!activeProfileId || !isConnected) {
    return (
      <div className="h-full flex items-center justify-center" style={{ background: 'var(--bg-primary)' }}>
        <div className="text-center">
          <div className="text-4xl mb-4" style={{ color: 'var(--text-muted)' }}>DB</div>
          <h3 className="text-xl font-semibold mb-2" style={{ color: 'var(--text-primary)' }}>No Connection</h3>
          <p className="mb-4" style={{ color: 'var(--text-secondary)' }}>Connect to explore your ClickHouse instance</p>
          <button
            onClick={() => setConnectionFormOpen(true)}
            className="px-4 py-2 rounded-lg"
            style={{ background: 'var(--accent-primary)', color: 'white' }}
          >
            Add Connection
          </button>
        </div>
      </div>
    );
  }

  // Capability gate — show centered message when system.databases is inaccessible
  if (!isCapProbing && !hasSystemDatabases) {
    return (
      <div className="h-full flex flex-col" style={{ background: 'var(--bg-primary)' }}>
        <PermissionGate
          error="Insufficient privileges to access system.databases. Ask your administrator to grant SELECT on this table."
          title="Database Explorer"
          variant="page"
        />
      </div>
    );
  }

  return (
    <div className="h-full flex flex-col overflow-hidden" style={{ background: 'var(--bg-primary)' }}>
      {/* 3D/2D View - Full height */}
      <div className="flex-1 relative">
        {/* Top-right: Auto-refresh controls */}
        <div 
          className="absolute top-4 right-4 z-50 flex items-center gap-2 px-3 py-1.5"
          style={{ 
            background: 'var(--bg-secondary)', 
            border: '1px solid var(--border-primary)',
            borderRadius: '8px',
            boxShadow: '0 2px 8px rgba(0,0,0,0.08)',
            backdropFilter: 'blur(12px)',
          }}
        >
          {autoRefresh && globalRate > 0 && (
            <div 
              className="w-2 h-2 rounded-full bg-green-400 animate-pulse"
              style={{ boxShadow: '0 0 8px rgba(74, 222, 128, 0.6)', flexShrink: 0 }}
            />
          )}
          <button
            onClick={() => setAutoRefresh(!autoRefresh)}
            className="px-2 py-0.5 rounded text-[10px] font-semibold transition-all"
            style={autoRefresh
              ? { background: 'var(--bg-primary)', color: 'var(--text-primary)', boxShadow: '0 1px 2px rgba(0,0,0,0.06)' }
              : { background: 'transparent', color: 'var(--text-muted)' }
            }
          >
            {autoRefresh ? 'Pause' : 'Auto'}
          </button>
        </div>

        {/* 3D Scene or 2D View */}
        <div 
          className="absolute inset-0" 
          style={{ zIndex: 1 }}
          onWheel={(e) => e.stopPropagation()}
        >
          {viewMode === '2d' ? (
            currentLevel === 'parts' ? (
              <PartsVisualization2D
                items={hierarchyItems}
                onItemClick={handleItemClick}
                onItemHover={setHoveredItem}
                highlightedMergeId={highlightedMergeId}
                mergeColorMap={mergeColorAssignments}
              />
            ) : (
              <HierarchyVisualization2D
                items={hierarchyItems}
                level={currentLevel}
                onItemClick={handleItemClick}
                onItemHover={setHoveredItem}
                highlightedMergeId={highlightedMergeId}
                mergeColorMap={mergeColorAssignments}
              />
            )
          ) : (
            <ErrorBoundary3D
              fallback2D={
                <div className="h-full flex items-center justify-center text-gray-500">
                  3D not available
                </div>
              }
              errorTitle="3D Error"
              errorDescription="Unable to render visualization"
            >
              <Scene3D 
                config={createSceneConfig({
                  performanceMode: performanceMode || hierarchyItems.length > 50,
                  enableAnimations: true,
                  cameraPosition: [0, 12, 12],
                  backgroundColor: 0x0a0a1a,
                })}
                onPerformanceModeChange={setPerformanceMode}
              >
                <HierarchyVisualization
                  items={hierarchyItems}
                  level={currentLevel}
                  path={breadcrumb.map(b => b.name)}
                  onItemClick={handleItemClick}
                  onItemHover={setHoveredItem}
                  onPathClick={handleNavigate}
                  highlightedMergeId={highlightedMergeId}
                  mergeColorMap={mergeColorAssignments}
                />
              </Scene3D>
            </ErrorBoundary3D>
          )}
        </div>
        
        {/* Breadcrumb Navigation - HTML overlay for visibility (hidden when Part Inspector is open) */}
        {!partDetail && !isLoadingPartDetail && (
        <div 
          className="absolute top-6 left-6 pointer-events-auto"
          style={{ zIndex: 100 }}
        >
          <div style={{
            fontFamily: "'Orbitron', 'Rajdhani', 'Share Tech Mono', monospace",
            textTransform: 'uppercase',
            letterSpacing: '3px',
            display: 'flex',
            alignItems: 'baseline',
            whiteSpace: 'nowrap',
            background: 'var(--bg-tertiary)',
            padding: '8px 16px',
            borderRadius: '8px',
            border: '1px solid var(--border-primary)',
            backdropFilter: 'blur(8px)',
          }}>
            {breadcrumb.map((item, i) => {
              const colors = LEVEL_COLORS[item.level];
              const isLast = i === breadcrumb.length - 1;
              return (
                <span key={i} style={{ display: 'inline-flex', alignItems: 'baseline' }}>
                  {i > 0 && (
                    <span style={{
                      color: 'var(--text-muted)',
                      margin: '0 10px',
                      fontSize: '18px',
                    }}>
                      ›
                    </span>
                  )}
                  <span 
                    onClick={() => !isLast && handleNavigate(i)}
                    style={{
                      color: isLast ? colors.glow : 'var(--text-tertiary)',
                      fontSize: isLast ? '24px' : '16px',
                      fontWeight: isLast ? 900 : 500,
                      textShadow: isLast 
                        ? `0 0 30px ${colors.glow}, 0 0 60px ${colors.main}, 0 0 90px ${colors.main}40`
                        : 'none',
                      cursor: !isLast ? 'pointer' : 'default',
                      transition: 'all 0.2s ease',
                      padding: '4px 8px',
                      borderRadius: '4px',
                    }}
                    onMouseEnter={(e) => {
                      if (!isLast) {
                        e.currentTarget.style.color = colors.glow;
                        e.currentTarget.style.textShadow = `0 0 20px ${colors.glow}, 0 0 40px ${colors.main}`;
                        e.currentTarget.style.background = 'var(--bg-hover)';
                      }
                    }}
                    onMouseLeave={(e) => {
                      if (!isLast) {
                        e.currentTarget.style.color = 'var(--text-tertiary)';
                        e.currentTarget.style.textShadow = 'none';
                        e.currentTarget.style.background = 'transparent';
                      }
                    }}
                  >
                    {item.name}
                  </span>
                </span>
              );
            })}
          </div>
        </div>
        )}
        
        {/* Stats removed - cleaner look */}
        
        {/* Loading indicator */}
        {isLoading && (
          <div className="absolute top-4 left-1/2 -translate-x-1/2 flex items-center gap-2 text-sm px-3 py-1.5 rounded-lg" style={{ zIndex: 50, color: 'var(--text-secondary)', background: 'var(--bg-tertiary)', backdropFilter: 'blur(8px)' }}>
            <span className="animate-spin inline-block w-4 h-4 border-2 rounded-full" style={{ borderColor: 'var(--border-primary)', borderTopColor: 'var(--accent-primary)' }}></span>
            Loading...
          </div>
        )}
        
        {/* Legend - Compact */}
        <Legend level={currentLevel} viewMode={viewMode} />
        
        {/* Merge Info Wall - Rolling display of active merges */}
        <MergeInfoWall 
          merges={activeMerges} 
          currentDatabase={selectedDatabase}
          currentTable={selectedTable}
          currentPartition={selectedPartition}
          level={currentLevel}
          highlightedMergeId={highlightedMergeId}
          mergeColorMap={mergeColorAssignments}
          onMergeHover={setHighlightedMergeId}
        />
        
        {/* Part Inspector - Slide-out panel for part details */}
        <PartInspector 
          partDetail={partDetail}
          isLoading={isLoadingPartDetail}
          onClose={closePartInspector}
          breadcrumbPath={[...breadcrumb.map(b => b.name), partDetail?.name || ''].filter(Boolean)}
          database={selectedDatabase || undefined}
          table={selectedTable || undefined}
        />
      </div>
    </div>
  );
};

export default DatabaseExplorer;
