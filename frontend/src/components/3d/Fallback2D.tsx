/**
 * Fallback2D - 2D fallback components for 3D visualization failures
 * 
 * These components provide table and chart-based representations of data
 * when 3D rendering fails or is not supported.
 */

import React, { useMemo, useState } from 'react';
import type { PartInfo } from '../../stores/databaseStore';
import type { MergeInfo } from '../../stores/mergeStore';
import type { ExplainResult, TraceLog } from '../../stores/traceStore';
import type { PipelineNode } from './PipelineVisualization';
import { parsePipelineOutput } from './PipelineVisualization';
import { formatBytes, formatNumber, formatDuration } from '../../utils/formatters';
import { classifyActiveMerge, getMergeCategoryInfo } from '@tracehouse/core';

// Parts Fallback 2D Component

/**
 * Props for PartsFallback2D component
 */
export interface PartsFallback2DProps {
  /** Array of parts to display */
  parts: PartInfo[];
  /** Callback when a part is clicked */
  onPartClick?: (part: PartInfo) => void;
  /** Optional CSS class */
  className?: string;
}

/**
 * Color scheme for size-based visualization
 */
function getSizeColor(normalizedSize: number): string {
  if (normalizedSize < 0.33) return 'bg-blue-500';
  if (normalizedSize < 0.66) return 'bg-yellow-500';
  return 'bg-red-500';
}

/**
 * PartsFallback2D - 2D table/bar representation of parts data
 * 
 * Displays parts as a sortable table with visual size bars.
 */
export const PartsFallback2D: React.FC<PartsFallback2DProps> = ({
  parts,
  onPartClick,
  className = '',
}) => {
  const [sortField, setSortField] = useState<keyof PartInfo>('bytes_on_disk');
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc');
  const [selectedPart, setSelectedPart] = useState<string | null>(null);

  // Calculate max size for normalization
  const maxSize = useMemo(() => {
    return Math.max(...parts.map(p => p.bytes_on_disk), 1);
  }, [parts]);

  // Sort parts
  const sortedParts = useMemo(() => {
    return [...parts].sort((a, b) => {
      const aVal = a[sortField];
      const bVal = b[sortField];
      const cmp = aVal < bVal ? -1 : aVal > bVal ? 1 : 0;
      return sortDir === 'asc' ? cmp : -cmp;
    });
  }, [parts, sortField, sortDir]);

  const handleSort = (field: keyof PartInfo) => {
    if (field === sortField) {
      setSortDir(d => d === 'asc' ? 'desc' : 'asc');
    } else {
      setSortField(field);
      setSortDir('desc');
    }
  };

  const handlePartClick = (part: PartInfo) => {
    setSelectedPart(prev => prev === part.name ? null : part.name);
    onPartClick?.(part);
  };

  if (parts.length === 0) {
    return (
      <div className={`text-center py-8 text-gray-500 dark:text-gray-400 ${className}`}>
        <div className="text-2xl mb-2 font-light">--</div>
        <p>No parts to display</p>
      </div>
    );
  }

  return (
    <div className={`bg-white dark:bg-gray-800 rounded-lg shadow overflow-hidden ${className}`}>
      {/* Header */}
      <div className="px-4 py-3 border-b border-gray-200 dark:border-gray-700 flex items-center justify-between">
        <h3 className="font-semibold text-gray-800 dark:text-white flex items-center gap-2">
          Parts Table (2D Fallback)
          <span className="text-xs font-normal text-gray-500 dark:text-gray-400">
            {parts.length} parts
          </span>
        </h3>
      </div>

      {/* Table */}
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="bg-gray-50 dark:bg-gray-700/50">
            <tr>
              {[
                { key: 'name', label: 'Part Name' },
                { key: 'partition_id', label: 'Partition' },
                { key: 'rows', label: 'Rows' },
                { key: 'bytes_on_disk', label: 'Size' },
                { key: 'level', label: 'Level' },
              ].map(({ key, label }) => (
                <th
                  key={key}
                  onClick={() => handleSort(key as keyof PartInfo)}
                  className="px-4 py-2 text-left text-gray-600 dark:text-gray-300 cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-600/50"
                >
                  <div className="flex items-center gap-1">
                    {label}
                    {sortField === key && (
                      <span>{sortDir === 'asc' ? '↑' : '↓'}</span>
                    )}
                  </div>
                </th>
              ))}
              <th className="px-4 py-2 text-left text-gray-600 dark:text-gray-300 w-32">
                Size Bar
              </th>
            </tr>
          </thead>
          <tbody>
            {sortedParts.map((part) => {
              const normalizedSize = part.bytes_on_disk / maxSize;
              const isSelected = selectedPart === part.name;
              
              return (
                <tr
                  key={part.name}
                  onClick={() => handlePartClick(part)}
                  className={`
                    border-b border-gray-100 dark:border-gray-700/50 cursor-pointer
                    ${isSelected 
                      ? 'bg-blue-50 dark:bg-blue-900/20' 
                      : 'hover:bg-gray-50 dark:hover:bg-gray-700/30'
                    }
                  `}
                >
                  <td className="px-4 py-2 font-mono text-xs text-gray-800 dark:text-gray-200">
                    {part.name}
                  </td>
                  <td className="px-4 py-2 text-gray-600 dark:text-gray-400">
                    {part.partition_id}
                  </td>
                  <td className="px-4 py-2 text-gray-600 dark:text-gray-400">
                    {formatNumber(part.rows)}
                  </td>
                  <td className="px-4 py-2 text-gray-600 dark:text-gray-400">
                    {formatBytes(part.bytes_on_disk)}
                  </td>
                  <td className="px-4 py-2 text-gray-600 dark:text-gray-400">
                    {part.level}
                  </td>
                  <td className="px-4 py-2">
                    <div className="w-full bg-gray-200 dark:bg-gray-600 rounded-full h-2">
                      <div
                        className={`h-2 rounded-full ${getSizeColor(normalizedSize)}`}
                        style={{ width: `${normalizedSize * 100}%` }}
                      />
                    </div>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
};


// Pipeline Fallback 2D Component

/**
 * Props for PipelineFallback2D component
 */
export interface PipelineFallback2DProps {
  /** EXPLAIN result containing pipeline output */
  pipeline: ExplainResult;
  /** Optional trace logs for correlation */
  traceLogs?: TraceLog[];
  /** Callback when a node is clicked */
  onNodeClick?: (node: PipelineNode) => void;
  /** Optional CSS class */
  className?: string;
}

/**
 * PipelineFallback2D - 2D tree representation of pipeline data
 * 
 * Displays pipeline stages as an indented tree structure.
 */
export const PipelineFallback2D: React.FC<PipelineFallback2DProps> = ({
  pipeline,
  traceLogs,
  onNodeClick,
  className = '',
}) => {
  const [expandedNodes, setExpandedNodes] = useState<Set<string>>(new Set());
  const [selectedNode, setSelectedNode] = useState<string | null>(null);

  // Parse pipeline output
  const nodes = useMemo(() => {
    if (!pipeline?.output) return [];
    return parsePipelineOutput(pipeline.output);
  }, [pipeline?.output]);

  // Correlate with logs
  const nodesWithLogs = useMemo(() => {
    if (!traceLogs || traceLogs.length === 0) return nodes;
    
    return nodes.map(node => {
      const correlatedLogs = traceLogs.filter(log => {
        const message = log.message.toLowerCase();
        const nodeName = node.name.toLowerCase();
        return message.includes(nodeName);
      });
      
      return {
        ...node,
        hasCorrelatedLogs: correlatedLogs.length > 0,
        correlatedLogCount: correlatedLogs.length,
      };
    });
  }, [nodes, traceLogs]);

  const toggleNode = (nodeId: string) => {
    setExpandedNodes(prev => {
      const next = new Set(prev);
      if (next.has(nodeId)) {
        next.delete(nodeId);
      } else {
        next.add(nodeId);
      }
      return next;
    });
  };

  const handleNodeClick = (node: PipelineNode) => {
    setSelectedNode(prev => prev === node.id ? null : node.id);
    onNodeClick?.(node);
  };

  if (!pipeline?.output || nodes.length === 0) {
    return (
      <div className={`text-center py-8 text-gray-500 dark:text-gray-400 ${className}`}>
        <div className="text-2xl mb-2 font-light">--</div>
        <p>No pipeline data to display</p>
      </div>
    );
  }

  return (
    <div className={`bg-white dark:bg-gray-800 rounded-lg shadow overflow-hidden ${className}`}>
      {/* Header */}
      <div className="px-4 py-3 border-b border-gray-200 dark:border-gray-700 flex items-center justify-between">
        <h3 className="font-semibold text-gray-800 dark:text-white flex items-center gap-2">
          Pipeline Tree (2D Fallback)
          <span className="text-xs font-normal text-gray-500 dark:text-gray-400">
            {nodes.length} stages
          </span>
        </h3>
        {traceLogs && traceLogs.length > 0 && (
          <span className="text-xs text-amber-600 dark:text-amber-400">
            * {traceLogs.length} logs
          </span>
        )}
      </div>

      {/* Tree view */}
      <div className="p-4 max-h-[500px] overflow-auto">
        {nodesWithLogs.map((node) => {
          const isSelected = selectedNode === node.id;
          const hasChildren = node.childIds.length > 0;
          const isExpanded = expandedNodes.has(node.id);
          
          return (
            <div
              key={node.id}
              style={{ paddingLeft: `${node.depth * 20}px` }}
              className="py-1"
            >
              <div
                onClick={() => handleNodeClick(node)}
                className={`
                  flex items-center gap-2 px-2 py-1 rounded cursor-pointer
                  ${isSelected 
                    ? 'bg-blue-100 dark:bg-blue-900/30' 
                    : 'hover:bg-gray-100 dark:hover:bg-gray-700/30'
                  }
                `}
              >
                {/* Expand/collapse button */}
                {hasChildren ? (
                  <button
                    onClick={(e) => {
                      e.stopPropagation();
                      toggleNode(node.id);
                    }}
                    className="w-4 h-4 flex items-center justify-center text-gray-400"
                  >
                    {isExpanded ? '▼' : '▶'}
                  </button>
                ) : (
                  <span className="w-4 h-4 flex items-center justify-center text-gray-300">
                    •
                  </span>
                )}
                
                {/* Node indicator */}
                <span
                  className={`
                    w-3 h-3 rounded-full flex-shrink-0
                    ${node.hasCorrelatedLogs 
                      ? 'bg-amber-500' 
                      : node.parentId === null 
                        ? 'bg-purple-500' 
                        : node.childIds.length === 0 
                          ? 'bg-cyan-500' 
                          : 'bg-blue-500'
                    }
                  `}
                />
                
                {/* Node name */}
                <span className="text-sm text-gray-800 dark:text-gray-200 font-medium">
                  {node.name}
                </span>
                
                {/* Log count badge */}
                {node.hasCorrelatedLogs && (
                  <span className="text-xs px-1.5 py-0.5 bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-300 rounded">
                    {node.correlatedLogCount} logs
                  </span>
                )}
                
                {/* Metadata */}
                {Object.keys(node.metadata).length > 0 && (
                  <span className="text-xs text-gray-400 dark:text-gray-500">
                    ({Object.entries(node.metadata).map(([k, v]) => `${k}: ${v}`).join(', ')})
                  </span>
                )}
              </div>
            </div>
          );
        })}
      </div>

      {/* Legend */}
      <div className="px-4 py-2 border-t border-gray-200 dark:border-gray-700 flex gap-4 text-xs">
        <div className="flex items-center gap-1">
          <span className="w-2 h-2 rounded-full bg-purple-500" />
          <span className="text-gray-500 dark:text-gray-400">Root</span>
        </div>
        <div className="flex items-center gap-1">
          <span className="w-2 h-2 rounded-full bg-blue-500" />
          <span className="text-gray-500 dark:text-gray-400">Stage</span>
        </div>
        <div className="flex items-center gap-1">
          <span className="w-2 h-2 rounded-full bg-cyan-500" />
          <span className="text-gray-500 dark:text-gray-400">Leaf</span>
        </div>
        <div className="flex items-center gap-1">
          <span className="w-2 h-2 rounded-full bg-amber-500" />
          <span className="text-gray-500 dark:text-gray-400">Has Logs</span>
        </div>
      </div>
    </div>
  );
};


// Merge Fallback 2D Component

/**
 * Props for MergeFallback2D component
 */
export interface MergeFallback2DProps {
  /** Array of active merges to display */
  merges: MergeInfo[];
  /** Callback when a merge is clicked */
  onMergeClick?: (merge: MergeInfo) => void;
  /** Optional CSS class */
  className?: string;
}

/**
 * Get merge type badge color
 */
function getMergeTypeColor(mergeType: string, isMutation: boolean): string {
  if (isMutation) return 'bg-purple-100 text-purple-700 dark:bg-purple-900/30 dark:text-purple-400';
  switch (mergeType) {
    case 'TTL': return 'bg-orange-100 text-orange-700 dark:bg-orange-900/30 dark:text-orange-400';
    case 'Regular': return 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400';
    default: return 'bg-gray-100 text-gray-700 dark:bg-gray-700 dark:text-gray-300';
  }
}

/**
 * Get progress bar color based on progress value
 */
function getProgressColor(progress: number): string {
  if (progress < 0.33) return 'bg-red-500';
  if (progress < 0.66) return 'bg-yellow-500';
  return 'bg-green-500';
}

/**
 * MergeFallback2D - 2D table representation of merge operations
 * 
 * Displays merges as a table with progress bars.
 */
export const MergeFallback2D: React.FC<MergeFallback2DProps> = ({
  merges,
  onMergeClick,
  className = '',
}) => {
  const [selectedMerge, setSelectedMerge] = useState<number | null>(null);

  const handleMergeClick = (merge: MergeInfo, index: number) => {
    setSelectedMerge(prev => prev === index ? null : index);
    onMergeClick?.(merge);
  };

  if (merges.length === 0) {
    return (
      <div className={`text-center py-8 text-gray-500 dark:text-gray-400 ${className}`}>
        <div className="text-2xl mb-2 font-light">OK</div>
        <p>No active merges</p>
        <p className="text-sm mt-1">Merge operations will appear here when active</p>
      </div>
    );
  }

  // Calculate statistics
  const stats = {
    totalMerges: merges.length,
    avgProgress: merges.reduce((sum, m) => sum + m.progress, 0) / merges.length,
    totalSize: merges.reduce((sum, m) => sum + m.total_size_bytes_compressed, 0),
  };

  return (
    <div className={`bg-white dark:bg-gray-800 rounded-lg shadow overflow-hidden ${className}`}>
      {/* Header */}
      <div className="px-4 py-3 border-b border-gray-200 dark:border-gray-700">
        <div className="flex items-center justify-between">
          <h3 className="font-semibold text-gray-800 dark:text-white flex items-center gap-2">
            Active Merges (2D Fallback)
            <span className="text-xs font-normal text-gray-500 dark:text-gray-400">
              {merges.length} active
            </span>
          </h3>
        </div>
        
        {/* Stats bar */}
        <div className="flex gap-4 mt-2 text-xs text-gray-500 dark:text-gray-400">
          <span>Avg Progress: {(stats.avgProgress * 100).toFixed(1)}%</span>
          <span>Total Size: {formatBytes(stats.totalSize)}</span>
        </div>
      </div>

      {/* Table */}
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="bg-gray-50 dark:bg-gray-700/50">
            <tr>
              <th className="px-4 py-2 text-left text-gray-600 dark:text-gray-300">Table</th>
              <th className="px-4 py-2 text-left text-gray-600 dark:text-gray-300">Type</th>
              <th className="px-4 py-2 text-left text-gray-600 dark:text-gray-300">Progress</th>
              <th className="px-4 py-2 text-left text-gray-600 dark:text-gray-300">Elapsed</th>
              <th className="px-4 py-2 text-left text-gray-600 dark:text-gray-300">Parts</th>
              <th className="px-4 py-2 text-left text-gray-600 dark:text-gray-300">Size</th>
              <th className="px-4 py-2 text-left text-gray-600 dark:text-gray-300">Memory</th>
              <th className="px-4 py-2 text-left text-gray-600 dark:text-gray-300">Rows</th>
            </tr>
          </thead>
          <tbody>
            {merges.map((merge, index) => {
              const isSelected = selectedMerge === index;
              const typeLabel = getMergeCategoryInfo(classifyActiveMerge(merge.merge_type, merge.is_mutation, merge.result_part_name)).label;
              
              return (
                <React.Fragment key={index}>
                  <tr
                    onClick={() => handleMergeClick(merge, index)}
                    className={`
                      border-b border-gray-100 dark:border-gray-700/50 cursor-pointer
                      ${isSelected 
                        ? 'bg-blue-50 dark:bg-blue-900/20' 
                        : 'hover:bg-gray-50 dark:hover:bg-gray-700/30'
                      }
                    `}
                  >
                    <td className="px-4 py-2 font-medium text-gray-800 dark:text-gray-200">
                      {merge.database}.{merge.table}
                    </td>
                    <td className="px-4 py-2">
                      <span className={`px-2 py-0.5 text-xs rounded-full ${getMergeTypeColor(merge.merge_type, merge.is_mutation)}`}>
                        {typeLabel}
                      </span>
                      {merge.merge_algorithm && merge.merge_algorithm !== 'Horizontal' && (
                        <span className="ml-1 text-xs text-gray-400">({merge.merge_algorithm})</span>
                      )}
                    </td>
                    <td className="px-4 py-2">
                      <div className="flex items-center gap-2">
                        <div className="w-24 bg-gray-200 dark:bg-gray-600 rounded-full h-2">
                          <div
                            className={`h-2 rounded-full ${getProgressColor(merge.progress)}`}
                            style={{ width: `${merge.progress * 100}%` }}
                          />
                        </div>
                        <span className="text-xs text-gray-600 dark:text-gray-400">
                          {(merge.progress * 100).toFixed(1)}%
                        </span>
                      </div>
                    </td>
                    <td className="px-4 py-2 text-gray-600 dark:text-gray-400">
                      {formatDuration(merge.elapsed)}
                    </td>
                    <td className="px-4 py-2 text-gray-600 dark:text-gray-400">
                      {merge.num_parts}
                    </td>
                    <td className="px-4 py-2 text-gray-600 dark:text-gray-400">
                      {formatBytes(merge.total_size_bytes_compressed)}
                    </td>
                    <td className="px-4 py-2 text-gray-600 dark:text-gray-400">
                      {formatBytes(merge.memory_usage || 0)}
                    </td>
                    <td className="px-4 py-2 text-gray-600 dark:text-gray-400">
                      {formatNumber(merge.rows_read)} → {formatNumber(merge.rows_written)}
                    </td>
                  </tr>
                  
                  {/* Expanded details */}
                  {isSelected && (
                    <tr className="bg-blue-50/50 dark:bg-blue-900/10">
                      <td colSpan={8} className="px-4 py-3">
                        <div className="grid grid-cols-2 gap-4 text-xs">
                          <div>
                            <span className="text-gray-500 dark:text-gray-400">Source Parts:</span>
                            <div className="mt-1 flex flex-wrap gap-1">
                              {merge.source_part_names.slice(0, 5).map((name, i) => (
                                <span
                                  key={i}
                                  className="px-2 py-0.5 bg-gray-200 dark:bg-gray-700 rounded text-gray-700 dark:text-gray-300 font-mono"
                                >
                                  {name}
                                </span>
                              ))}
                              {merge.source_part_names.length > 5 && (
                                <span className="text-gray-400">
                                  +{merge.source_part_names.length - 5} more
                                </span>
                              )}
                            </div>
                          </div>
                          <div>
                            <span className="text-gray-500 dark:text-gray-400">Result Part:</span>
                            <div className="mt-1">
                              <span className="px-2 py-0.5 bg-purple-100 dark:bg-purple-900/30 rounded text-purple-700 dark:text-purple-300 font-mono">
                                {merge.result_part_name}
                              </span>
                            </div>
                          </div>
                        </div>
                      </td>
                    </tr>
                  )}
                </React.Fragment>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default {
  PartsFallback2D,
  PipelineFallback2D,
  MergeFallback2D,
};
