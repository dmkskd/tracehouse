/**
 * MutationDependencyDiagram - Visual dependency graph for mutation/merge/part relationships
 * 
 * Renders an SVG diagram showing:
 * - The selected mutation as a central node
 * - Parts as nodes, color-coded by status (mutating/merging/idle)
 * - Active merges connected to the parts they process
 * - Co-dependent mutations connected via shared parts
 */

import React, { useEffect, useMemo, useState } from 'react';
import type { MutationDependencyInfo, MutationInfo } from '../../stores/mergeStore';

interface Props {
  dependency: MutationDependencyInfo;
  mutation: MutationInfo;
  onClose: () => void;
}

// Node types for the diagram
interface DiagramNode {
  id: string;
  type: 'mutation' | 'part' | 'merge' | 'co-mutation';
  label: string;
  sublabel?: string;
  color: string;
  x: number;
  y: number;
  width: number;
  height: number;
}

interface DiagramEdge {
  from: string;
  to: string;
  color: string;
  dashed?: boolean;
  label?: string;
}

const STATUS_COLORS = {
  mutating: '#c084fc',
  merging: '#f0883e',
  idle: '#6b7280',
};

const MERGE_COLOR = '#3b82f6';
const CO_MUT_COLOR = '#f778ba';
const MAIN_MUT_COLOR = '#a855f7';

/** Shorten a part name for display: show partition + block range */
function shortPartName(name: string): string {
  // Part names look like: 202401_1_5_2 or all_1_5_2_7
  if (name.length <= 24) return name;
  const segments = name.split('_');
  if (segments.length >= 4) {
    return `${segments[0]}_${segments[1]}_…_${segments[segments.length - 1]}`;
  }
  return name.slice(0, 12) + '…' + name.slice(-8);
}

function shortMutationId(id: string): string {
  // mutation_2725.txt -> mut_2725
  const match = id.match(/mutation_(\d+)/);
  return match ? `mut_${match[1]}` : id.slice(0, 12);
}

export const MutationDependencyDiagram: React.FC<Props> = ({ dependency, mutation, onClose }) => {
  useEffect(() => {
    const handleKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    document.addEventListener('keydown', handleKey);
    return () => document.removeEventListener('keydown', handleKey);
  }, [onClose]);

  const { nodes, edges, svgWidth, svgHeight } = useMemo(() => {
    const { part_statuses, co_dependent_mutations } = dependency;

    // Collect unique merges from parts
    const mergeMap = new Map<string, { progress?: number; elapsed?: number }>();
    for (const ps of part_statuses) {
      if (ps.merge_result_part) {
        mergeMap.set(ps.merge_result_part, {
          progress: ps.merge_progress,
          elapsed: ps.merge_elapsed,
        });
      }
    }
    const merges = Array.from(mergeMap.entries());

    // Layout constants
    const colMutation = 80;
    const colParts = 300;
    const colMerges = 520;
    const rowHeight = 40;
    const topPad = 50;

    const partCount = part_statuses.length;
    const mergeCount = merges.length;
    const coMutCount = co_dependent_mutations.length;

    // Parts column (center column, drives height)
    const partsStartY = topPad;

    // Main mutation node — vertically centered on parts
    const mutY = partsStartY + Math.max(0, (partCount - 1) * rowHeight) / 2;

    const allNodes: DiagramNode[] = [];
    const allEdges: DiagramEdge[] = [];

    // 1. Main mutation node
    const mainMutId = `mut-${mutation.mutation_id}`;
    allNodes.push({
      id: mainMutId,
      type: 'mutation',
      label: shortMutationId(mutation.mutation_id),
      sublabel: `${mutation.database}.${mutation.table}`,
      color: MAIN_MUT_COLOR,
      x: colMutation,
      y: mutY,
      width: 120,
      height: 36,
    });

    // 2. Part nodes
    part_statuses.forEach((ps, i) => {
      const partId = `part-${ps.part_name}`;
      allNodes.push({
        id: partId,
        type: 'part',
        label: shortPartName(ps.part_name),
        sublabel: ps.status,
        color: STATUS_COLORS[ps.status],
        x: colParts,
        y: partsStartY + i * rowHeight,
        width: 140,
        height: 26,
      });

      // Edge: mutation -> part
      allEdges.push({
        from: mainMutId,
        to: partId,
        color: STATUS_COLORS[ps.status],
      });

      // Edge: part -> merge (if applicable)
      if (ps.merge_result_part) {
        allEdges.push({
          from: partId,
          to: `merge-${ps.merge_result_part}`,
          color: ps.status === 'mutating' ? MAIN_MUT_COLOR : MERGE_COLOR,
        });
      }
    });

    // 3. Merge nodes — vertically centered on their connected parts
    merges.forEach(([ resultPart, info ], i) => {
      const mergeId = `merge-${resultPart}`;
      // Find connected parts to center vertically
      const connectedParts = part_statuses
        .map((ps, idx) => ps.merge_result_part === resultPart ? idx : -1)
        .filter(idx => idx >= 0);
      const avgPartIdx = connectedParts.length > 0
        ? connectedParts.reduce((a, b) => a + b, 0) / connectedParts.length
        : i;
      const mergeY = partsStartY + avgPartIdx * rowHeight;

      const pctLabel = info.progress !== undefined ? `${(info.progress * 100).toFixed(0)}%` : '';
      allNodes.push({
        id: mergeId,
        type: 'merge',
        label: `Merge ${pctLabel}`,
        sublabel: shortPartName(resultPart),
        color: MERGE_COLOR,
        x: colMerges,
        y: mergeY,
        width: 130,
        height: 36,
      });
    });

    // 4. Co-dependent mutation nodes — placed below the main mutation
    const coMutStartY = mutY + 60;
    co_dependent_mutations.forEach((cd, i) => {
      const coId = `co-${cd.mutation_id}`;
      allNodes.push({
        id: coId,
        type: 'co-mutation',
        label: shortMutationId(cd.mutation_id),
        sublabel: `${cd.shared_parts_count} shared part${cd.shared_parts_count !== 1 ? 's' : ''}`,
        color: CO_MUT_COLOR,
        x: colMutation,
        y: coMutStartY + i * rowHeight,
        width: 120,
        height: 36,
      });

      // Edges: co-mutation -> shared parts (dashed)
      for (const sp of cd.shared_parts) {
        const partId = `part-${sp}`;
        if (allNodes.find(n => n.id === partId)) {
          allEdges.push({
            from: coId,
            to: partId,
            color: CO_MUT_COLOR,
            dashed: true,
          });
        }
      }
    });

    // Compute SVG dimensions
    const maxY = Math.max(
      partsStartY + partCount * rowHeight,
      coMutStartY + coMutCount * rowHeight,
      partsStartY + mergeCount * rowHeight,
    );
    const w = colMerges + 180;
    const h = maxY + 40;

    return { nodes: allNodes, edges: allEdges, svgWidth: w, svgHeight: h };
  }, [dependency, mutation]);

  // Find node center for edge drawing
  const nodeCenter = (node: DiagramNode, side: 'left' | 'right'): [number, number] => {
    const cx = side === 'right' ? node.x + node.width / 2 : node.x - node.width / 2;
    return [cx, node.y];
  };

  const nodeMap = useMemo(() => {
    const m = new Map<string, DiagramNode>();
    for (const n of nodes) m.set(n.id, n);
    return m;
  }, [nodes]);

  const [hoveredColor, setHoveredColor] = useState<string | null>(null);

  const colorMatches = (elementColor: string, legendColor: string): boolean => {
    return elementColor.startsWith(legendColor);
  };

  const nodeOpacity = (node: DiagramNode): number => {
    if (!hoveredColor) return 1;
    return colorMatches(node.color, hoveredColor) ? 1 : 0.15;
  };

  const edgeOpacity = (edge: DiagramEdge): number => {
    if (!hoveredColor) return edge.dashed ? 0.5 : 0.5;
    return colorMatches(edge.color, hoveredColor) ? 0.8 : 0.07;
  };

  return (
    <div
      style={{
        position: 'fixed', inset: 0, zIndex: 9999,
        background: 'rgba(0,0,0,0.7)', backdropFilter: 'blur(4px)',
        display: 'flex', alignItems: 'center', justifyContent: 'center',
      }}
      onClick={onClose}
    >
      <div
        style={{
          background: 'var(--bg-secondary, #1a1a2e)',
          borderRadius: 12, border: '1px solid var(--border-primary, #333)',
          maxWidth: '90vw', maxHeight: '85vh', overflow: 'auto',
          padding: 24, minWidth: 600,
        }}
        onClick={e => e.stopPropagation()}
      >
        {/* Header */}
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 16 }}>
          <div>
            <div style={{ fontSize: 14, fontWeight: 600, color: 'var(--text-primary, #e0e0e0)' }}>
              Dependency Map
            </div>
            <div style={{ fontSize: 11, color: 'var(--text-muted, #888)', marginTop: 2 }}>
              {mutation.database}.{mutation.table} — {mutation.mutation_id}
            </div>
          </div>
          <button
            onClick={onClose}
            style={{
              background: 'none', border: 'none', color: 'var(--text-muted, #888)',
              cursor: 'pointer', fontSize: 18, padding: '4px 8px',
            }}
          >
            ✕
          </button>
        </div>

        {/* Legend */}
        <div style={{ display: 'flex', gap: 16, marginBottom: 16, flexWrap: 'wrap' }}>
          {[
            { color: MAIN_MUT_COLOR, label: 'This mutation' },
            { color: STATUS_COLORS.mutating, label: 'Mutating (active)' },
            { color: STATUS_COLORS.merging, label: 'In regular merge' },
            { color: STATUS_COLORS.idle, label: 'Idle / waiting' },
            { color: MERGE_COLOR, label: 'Active merge' },
            { color: CO_MUT_COLOR, label: 'Co-dependent mutation' },
          ].map(({ color, label }) => (
            <div
              key={label}
              style={{
                display: 'flex', alignItems: 'center', gap: 5, fontSize: 10,
                color: 'var(--text-muted, #888)', cursor: 'pointer',
                opacity: hoveredColor && hoveredColor !== color ? 0.3 : 1,
                transition: 'opacity 0.15s',
              }}
              onMouseEnter={() => setHoveredColor(color)}
              onMouseLeave={() => setHoveredColor(null)}
            >
              <div style={{ width: 10, height: 10, borderRadius: 2, background: color, flexShrink: 0 }} />
              {label}
            </div>
          ))}
        </div>

        {/* SVG Diagram */}
        <svg
          width={svgWidth}
          height={svgHeight}
          viewBox={`0 0 ${svgWidth} ${svgHeight}`}
          style={{ display: 'block' }}
        >
          {/* Column labels */}
          <text x={80} y={20} textAnchor="middle" fill="var(--text-muted, #888)" fontSize={10} fontWeight={500}>
            Mutations
          </text>
          <text x={300} y={20} textAnchor="middle" fill="var(--text-muted, #888)" fontSize={10} fontWeight={500}>
            Parts
          </text>
          <text x={520} y={20} textAnchor="middle" fill="var(--text-muted, #888)" fontSize={10} fontWeight={500}>
            Merges
          </text>

          {/* Edges */}
          {edges.map((edge, i) => {
            const fromNode = nodeMap.get(edge.from);
            const toNode = nodeMap.get(edge.to);
            if (!fromNode || !toNode) return null;

            const [x1, y1] = nodeCenter(fromNode, 'right');
            const [x2, y2] = nodeCenter(toNode, 'left');

            // Bezier curve for smooth edges
            const midX = (x1 + x2) / 2;

            return (
              <path
                key={i}
                d={`M ${x1} ${y1} C ${midX} ${y1}, ${midX} ${y2}, ${x2} ${y2}`}
                fill="none"
                stroke={edge.color}
                strokeWidth={1.5}
                strokeOpacity={edgeOpacity(edge)}
                strokeDasharray={edge.dashed ? '4 3' : undefined}
                style={{ transition: 'stroke-opacity 0.15s' }}
              />
            );
          })}

          {/* Nodes */}
          {nodes.map(node => {
            const rx = node.width / 2;
            const ry = node.height / 2;

            if (node.type === 'part') {
              // Parts: rounded rectangle (pill)
              return (
                <g key={node.id} opacity={nodeOpacity(node)} style={{ transition: 'opacity 0.15s' }}>
                  <rect
                    x={node.x - rx} y={node.y - ry}
                    width={node.width} height={node.height}
                    rx={ry} ry={ry}
                    fill={`${node.color}18`}
                    stroke={node.color}
                    strokeWidth={1.5}
                  />
                  <text
                    x={node.x} y={node.y + 1}
                    textAnchor="middle" dominantBaseline="middle"
                    fill={node.color} fontSize={8} fontFamily="monospace"
                  >
                    {node.label}
                  </text>
                  {/* Status badge */}
                  <text
                    x={node.x + rx - 4} y={node.y - ry - 4}
                    textAnchor="end" fill={node.color} fontSize={7} fontWeight={500}
                  >
                    {node.sublabel}
                  </text>
                </g>
              );
            }

            if (node.type === 'merge') {
              // Merges: diamond-ish hexagon
              const cx = node.x;
              const cy = node.y;
              const hw = rx;
              const hh = ry;
              const indent = 10;
              const points = [
                `${cx - hw + indent},${cy}`,
                `${cx - hw / 2},${cy - hh}`,
                `${cx + hw / 2},${cy - hh}`,
                `${cx + hw - indent},${cy}`,
                `${cx + hw / 2},${cy + hh}`,
                `${cx - hw / 2},${cy + hh}`,
              ].join(' ');

              return (
                <g key={node.id} opacity={nodeOpacity(node)} style={{ transition: 'opacity 0.15s' }}>
                  <polygon
                    points={points}
                    fill={`${node.color}18`}
                    stroke={node.color}
                    strokeWidth={1.5}
                  />
                  <text
                    x={cx} y={cy - 3}
                    textAnchor="middle" dominantBaseline="middle"
                    fill={node.color} fontSize={9} fontWeight={600}
                  >
                    {node.label}
                  </text>
                  <text
                    x={cx} y={cy + 9}
                    textAnchor="middle" dominantBaseline="middle"
                    fill={`${node.color}99`} fontSize={7} fontFamily="monospace"
                  >
                    {node.sublabel}
                  </text>
                </g>
              );
            }

            // Mutations (main + co-dependent): rounded rectangle
            const isMain = node.type === 'mutation';
            return (
              <g key={node.id} opacity={nodeOpacity(node)} style={{ transition: 'opacity 0.15s' }}>
                <rect
                  x={node.x - rx} y={node.y - ry}
                  width={node.width} height={node.height}
                  rx={6} ry={6}
                  fill={`${node.color}18`}
                  stroke={node.color}
                  strokeWidth={isMain ? 2 : 1.5}
                  strokeDasharray={isMain ? undefined : '4 3'}
                />
                <text
                  x={node.x} y={node.y - 3}
                  textAnchor="middle" dominantBaseline="middle"
                  fill={node.color} fontSize={9} fontWeight={isMain ? 700 : 500}
                >
                  {node.label}
                </text>
                {node.sublabel && (
                  <text
                    x={node.x} y={node.y + 9}
                    textAnchor="middle" dominantBaseline="middle"
                    fill={`${node.color}88`} fontSize={7} fontFamily="monospace"
                  >
                    {node.sublabel}
                  </text>
                )}
              </g>
            );
          })}
        </svg>
      </div>
    </div>
  );
};
