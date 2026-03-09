/**
 * LineageVisualization — tree view of a part's merge lineage.
 *
 * Extracted from PartInspector.tsx so the inspector stays focused on
 * orchestrating tabs and the lineage tree is independently maintainable.
 */

import React, { useState, useMemo, useCallback } from 'react';
import type { PartLineageInfo, LineageNode } from '../../stores/databaseStore';
import { formatBytes } from '../../utils/formatters';
import { MergeTimeline, getLevelColor } from './MergeTimeline';

export const LineageVisualization: React.FC<{ lineage: PartLineageInfo }> = ({ lineage }) => {
  const [expandedNodes, setExpandedNodes] = useState<Set<string>>(new Set());
  
  const getAllNodeNames = useCallback((node: LineageNode | null): string[] => {
    if (!node) return [];
    const names = [node.part_name];
    node.children.forEach(child => {
      names.push(...getAllNodeNames(child));
    });
    return names;
  }, []);
  
  const allNodeNames = useMemo(() => getAllNodeNames(lineage.root), [lineage.root, getAllNodeNames]);
  
  const toggleNode = (partName: string) => {
    setExpandedNodes(prev => {
      const next = new Set(prev);
      if (next.has(partName)) {
        next.delete(partName);
      } else {
        next.add(partName);
      }
      return next;
    });
  };
  
  const expandAll = () => setExpandedNodes(new Set(allNodeNames));
  const collapseAll = () => setExpandedNodes(new Set());
  
  const renderNode = (node: LineageNode, depth: number = 0, index: number = 0): React.ReactNode => {
    const hasChildren = node.children.length > 0;
    const isExpanded = expandedNodes.has(node.part_name);
    const levelColor = getLevelColor(node.level);
    const me = node.merge_event;
    const nodeKey = `${node.part_name}-d${depth}-i${index}`;
    
    return (
      <div key={nodeKey} style={{ marginLeft: depth > 0 ? 24 : 0 }}>
        {depth > 0 && (
          <div style={{
            position: 'absolute',
            left: depth * 24 - 12,
            width: 12,
            height: 1,
            background: 'var(--border-primary)',
            marginTop: 20,
          }} />
        )}
        
        <div 
          style={{
            display: 'grid',
            gridTemplateColumns: 'auto auto 1fr auto auto auto auto',
            alignItems: 'center',
            gap: '12px',
            padding: '10px 16px',
            marginBottom: 6,
            borderRadius: 6,
            background: `${levelColor.bg}08`,
            border: `1px solid ${levelColor.border}30`,
          }}
        >
          {hasChildren ? (
            <button
              onClick={() => toggleNode(node.part_name)}
              style={{
                width: 20,
                height: 20,
                borderRadius: 4,
                background: 'var(--bg-hover)',
                border: 'none',
                color: 'var(--text-secondary)',
                cursor: 'pointer',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                fontSize: 12,
              }}
            >
              {isExpanded ? '−' : '+'}
            </button>
          ) : (
            <div style={{ width: 20 }} />
          )}
          
          <div
            style={{
              padding: '3px 10px',
              borderRadius: 4,
              background: levelColor.bg,
              color: 'white',
              fontSize: 11,
              fontWeight: 600,
              fontFamily: 'monospace',
              textAlign: 'center',
              minWidth: 32,
            }}
          >
            L{node.level}
          </div>
          
          <div style={{ minWidth: 0 }}>
            <div style={{ 
              fontSize: 12, 
              fontFamily: 'monospace', 
              color: 'var(--text-primary)',
              fontWeight: 500,
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              whiteSpace: 'nowrap',
              display: 'flex',
              alignItems: 'center',
              gap: 8,
            }}>
              {node.part_name}
              {me?.event_type === 'MutatePart' && (
                <span style={{
                  fontSize: 9,
                  padding: '2px 6px',
                  borderRadius: 3,
                  background: '#f59e0b20',
                  color: '#fbbf24',
                  fontWeight: 600,
                  letterSpacing: '0.5px',
                }}>
                  MUTATION
                </span>
              )}
            </div>
            {me && (
              <div style={{ fontSize: 10, color: 'var(--text-muted)', marginTop: 2 }}>
                {me.merge_algorithm} • {me.merged_from.length} source part{me.merged_from.length !== 1 ? 's' : ''}
              </div>
            )}
          </div>
          
          <div style={{ textAlign: 'right', minWidth: 70 }}>
            <div style={{ fontSize: 9, color: 'var(--text-muted)', marginBottom: 1 }}>Size</div>
            <div style={{ fontSize: 12, fontFamily: 'monospace', color: '#60a5fa', fontWeight: 500 }}>
              {formatBytes(node.size_in_bytes)}
            </div>
          </div>
          
          <div style={{ textAlign: 'right', minWidth: 90 }}>
            <div style={{ fontSize: 9, color: 'var(--text-muted)', marginBottom: 1 }}>Rows</div>
            <div style={{ fontSize: 12, fontFamily: 'monospace', color: 'var(--text-secondary)' }}>
              {node.rows.toLocaleString()}
            </div>
          </div>
          
          <div style={{ textAlign: 'right', minWidth: 80 }}>
            {me ? (
              <>
                <div style={{ fontSize: 9, color: 'var(--text-muted)', marginBottom: 1 }}>Throughput</div>
                <div style={{ fontSize: 12, fontFamily: 'monospace', color: '#fbbf24', fontWeight: 500 }}>
                  {me.duration_ms > 0 ? ((me.read_bytes / me.duration_ms) * 1000 / (1024 * 1024)).toFixed(1) : '0.0'} MB/s
                </div>
              </>
            ) : (
              <div style={{ minWidth: 80 }} />
            )}
          </div>
          
          <div style={{ textAlign: 'right', minWidth: 70 }}>
            {me ? (
              <>
                <div style={{ fontSize: 9, color: 'var(--text-muted)', marginBottom: 1 }}>Memory</div>
                <div style={{ fontSize: 12, fontFamily: 'monospace', color: '#a78bfa', fontWeight: 500 }}>
                  {formatBytes(me.peak_memory_usage)}
                </div>
              </>
            ) : (
              <div style={{ minWidth: 70 }} />
            )}
          </div>
        </div>
        
        {hasChildren && isExpanded && (
          <div style={{ position: 'relative', marginLeft: 10, borderLeft: '1px solid var(--border-primary)', paddingLeft: 14 }}>
            {node.children.map((child, i) => renderNode(child, depth + 1, i))}
          </div>
        )}
      </div>
    );
  };


  
  return (
    <div>
      {/* Summary stats */}
      <div style={{ 
        display: 'grid', 
        gridTemplateColumns: 'repeat(3, 1fr)', 
        gap: 12, 
        marginBottom: 16,
      }}>
        <div style={{ background: 'var(--bg-card)', border: '1px solid var(--border-secondary)', borderRadius: 6, padding: '12px 14px' }}>
          <div style={{ fontSize: 9, color: 'var(--text-muted)', marginBottom: 6, textTransform: 'uppercase', letterSpacing: '1px' }}>Merges</div>
          <div style={{ fontSize: 18, color: '#a78bfa', fontFamily: 'monospace', fontWeight: 600 }}>{lineage.total_merges}</div>
        </div>
        <div style={{ background: 'var(--bg-card)', border: '1px solid var(--border-secondary)', borderRadius: 6, padding: '12px 14px' }}>
          <div style={{ fontSize: 9, color: 'var(--text-muted)', marginBottom: 6, textTransform: 'uppercase', letterSpacing: '1px' }}>Original Parts (L0)</div>
          <div style={{ fontSize: 18, color: '#ec4899', fontFamily: 'monospace', fontWeight: 600 }}>{lineage.total_original_parts}</div>
        </div>
        <div style={{ background: 'var(--bg-card)', border: '1px solid var(--border-secondary)', borderRadius: 6, padding: '12px 14px' }}>
          <div style={{ fontSize: 9, color: 'var(--text-muted)', marginBottom: 6, textTransform: 'uppercase', letterSpacing: '1px' }}>Total Merge Time</div>
          <div style={{ fontSize: 18, color: '#22d3ee', fontFamily: 'monospace', fontWeight: 600 }}>
            {lineage.total_time_ms < 1000 ? `${lineage.total_time_ms}ms` : `${(lineage.total_time_ms / 1000).toFixed(1)}s`}
          </div>
        </div>
      </div>
      
      {/* Size reduction bar */}
      <div style={{ marginBottom: 16 }}>
        {lineage.original_total_size > 0 ? (
          (() => {
            const overallSpaceSavingsPercent = lineage.original_total_size > 0 
              ? ((lineage.original_total_size - lineage.final_size) / lineage.original_total_size) * 100 
              : 0;
            return (
              <div style={{ 
                background: 'var(--bg-card)', 
                border: '1px solid var(--border-secondary)', 
                borderRadius: 8, 
                padding: 16,
              }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
                  <div style={{ fontSize: 9, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '1px' }}>
                    Size Reduction ({lineage.total_original_parts} L0 parts → 1 part)
                  </div>
                  <div style={{ 
                    fontSize: 14, 
                    fontFamily: 'monospace', 
                    fontWeight: 600,
                    color: overallSpaceSavingsPercent > 0 ? '#34d399' : '#f87171',
                  }}>
                    {overallSpaceSavingsPercent > 0 ? '+' : ''}{overallSpaceSavingsPercent.toFixed(1)}% saved
                  </div>
                </div>
                
                <div style={{ height: 32, background: 'var(--bg-hover)', borderRadius: 6, overflow: 'hidden', position: 'relative' }}>
                  <div style={{ 
                    position: 'absolute', 
                    inset: 0, 
                    background: 'rgba(236, 72, 153, 0.15)',
                    borderRadius: 6,
                  }} />
                  <div style={{ 
                    position: 'absolute', 
                    top: 0, 
                    bottom: 0, 
                    left: 0,
                    width: `${Math.min(100, (lineage.final_size / lineage.original_total_size) * 100)}%`,
                    background: 'linear-gradient(90deg, #10b981, #34d399)',
                    borderRadius: 6,
                  }} />
                  <div style={{ 
                    position: 'absolute', 
                    inset: 0, 
                    display: 'flex', 
                    alignItems: 'center', 
                    justifyContent: 'space-between',
                    padding: '0 14px',
                    fontSize: 12,
                    fontFamily: 'monospace',
                  }}>
                    <span style={{ color: 'white', fontWeight: 600, textShadow: '0 1px 2px rgba(0,0,0,0.5)' }}>
                      {formatBytes(lineage.final_size)}
                    </span>
                    <span style={{ color: 'var(--text-tertiary)', fontWeight: 500 }}>
                      {formatBytes(lineage.original_total_size)}
                    </span>
                  </div>
                </div>
                
                <div style={{ display: 'flex', gap: 16, marginTop: 10, fontSize: 10 }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                    <div style={{ width: 12, height: 12, borderRadius: 3, background: 'linear-gradient(90deg, #10b981, #34d399)' }} />
                    <span style={{ color: 'var(--text-tertiary)' }}>Current size</span>
                  </div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                    <div style={{ width: 12, height: 12, borderRadius: 3, background: 'rgba(236, 72, 153, 0.3)' }} />
                    <span style={{ color: 'var(--text-tertiary)' }}>Original L0 size (sum)</span>
                  </div>
                </div>
              </div>
            );
          })()
        ) : (
          <div style={{ 
            background: 'var(--bg-card)', 
            border: '1px solid var(--border-secondary)', 
            borderRadius: 8, 
            padding: 16,
            textAlign: 'center',
          }}>
            <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>
              Size data unavailable — L0 parts have been merged away and purged from part_log
            </div>
          </div>
        )}
      </div>
      
      <MergeTimeline lineage={lineage} />
      
      {/* Lineage tree */}
      <div style={{ 
        borderRadius: 8, 
        border: '1px solid var(--border-secondary)',
        padding: 16,
        background: 'var(--bg-card)',
      }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
          <div style={{ fontSize: 9, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '1px' }}>
            Merge History Tree
          </div>
          <div style={{ display: 'flex', gap: 8 }}>
            <button
              onClick={expandAll}
              style={{
                padding: '4px 10px',
                fontSize: 10,
                background: 'var(--bg-hover)',
                border: '1px solid var(--border-primary)',
                borderRadius: 4,
                color: 'var(--text-secondary)',
                cursor: 'pointer',
              }}
            >
              Expand All
            </button>
            <button
              onClick={collapseAll}
              style={{
                padding: '4px 10px',
                fontSize: 10,
                background: 'var(--bg-hover)',
                border: '1px solid var(--border-primary)',
                borderRadius: 4,
                color: 'var(--text-secondary)',
                cursor: 'pointer',
              }}
            >
              Collapse All
            </button>
          </div>
        </div>
        {lineage.root && lineage.root.children.length === 0 && !lineage.root.merge_event ? (
          <div style={{ 
            padding: 20, 
            textAlign: 'center', 
            color: 'var(--text-muted)', 
            fontSize: 12,
            background: 'var(--bg-card)',
            borderRadius: 8,
          }}>
            <div style={{ marginBottom: 8, fontSize: 14 }}>Lineage data not available</div>
            <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>
              This part's merge history has been rotated out of system.part_log.
              <br />
              ClickHouse only keeps recent merge events.
            </div>
          </div>
        ) : lineage.root ? renderNode(lineage.root) : (
          <div style={{ color: 'var(--text-muted)', fontSize: 12 }}>No lineage data</div>
        )}
      </div>
    </div>
  );
};

export default LineageVisualization;
