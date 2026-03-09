/**
 * MergeDependencyDiagram - Visual dependency graph for a merge and all mutations it affects
 *
 * The inverse of MutationDependencyDiagram: shows a merge as the central node
 * with source parts fanning left and affected mutations fanning right.
 */

import React, { useEffect, useMemo, useState } from 'react';
import type { MergeInfo, MutationInfo } from '../../stores/mergeStore';

interface Props {
  merge: MergeInfo;
  /** All mutations affected by this merge */
  affectedMutations: MutationInfo[];
  onClose: () => void;
}

interface DiagramNode {
  id: string;
  type: 'merge' | 'part' | 'mutation';
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
}

const MERGE_COLOR = '#3b82f6';
const PART_COLOR = '#f0883e';
const MUTATION_COLOR = '#a855f7';
const PART_IN_MUTATION_COLOR = '#ec4899';

function shortPartName(name: string): string {
  if (name.length <= 24) return name;
  const segments = name.split('_');
  if (segments.length >= 4) {
    return `${segments[0]}_${segments[1]}_…_${segments[segments.length - 1]}`;
  }
  return name.slice(0, 12) + '…' + name.slice(-8);
}

function shortMutationId(id: string): string {
  const match = id.match(/mutation_(\d+)/);
  return match ? `mut_${match[1]}` : id.slice(0, 12);
}

export const MergeDependencyDiagram: React.FC<Props> = ({ merge, affectedMutations, onClose }) => {
  useEffect(() => {
    const handleKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    document.addEventListener('keydown', handleKey);
    return () => document.removeEventListener('keydown', handleKey);
  }, [onClose]);

  const { nodes, edges, svgWidth, svgHeight } = useMemo(() => {
    const sourceParts = merge.source_part_names;
    const mutCount = affectedMutations.length;
    const partCount = sourceParts.length;

    // Layout: Parts (left) → Merge (center) → Mutations (right)
    const colParts = 100;
    const colMerge = 340;
    const colMutations = 580;
    const rowHeight = 36;
    const topPad = 50;

    const allNodes: DiagramNode[] = [];
    const allEdges: DiagramEdge[] = [];

    // Build a set of parts that are in-progress for any mutation (to color-code)
    const partsInMutation = new Set<string>();
    for (const mut of affectedMutations) {
      for (const p of mut.parts_in_progress_names) partsInMutation.add(p);
      for (const p of mut.parts_to_do_names) partsInMutation.add(p);
    }

    // Compute column heights to find the tallest, then center everything
    const partsHeight = Math.max(1, partCount) * rowHeight;
    const mutsHeight = Math.max(1, mutCount) * rowHeight;
    const tallestColumn = Math.max(partsHeight, mutsHeight);
    const centerY = topPad + tallestColumn / 2;

    // 1. Source part nodes (left column) — centered vertically
    const partsStartY = centerY - partsHeight / 2 + rowHeight / 2;
    sourceParts.forEach((partName, i) => {
      const isMutationPart = partsInMutation.has(partName);
      allNodes.push({
        id: `part-${partName}`,
        type: 'part',
        label: shortPartName(partName),
        sublabel: isMutationPart ? 'in mutation' : undefined,
        color: isMutationPart ? PART_IN_MUTATION_COLOR : PART_COLOR,
        x: colParts,
        y: partsStartY + i * rowHeight,
        width: 150,
        height: 26,
      });
    });

    // 2. Central merge node — at vertical center
    const mergeY = centerY;
    const pctLabel = `${(merge.progress * 100).toFixed(0)}%`;
    allNodes.push({
      id: 'merge-center',
      type: 'merge',
      label: `Merge ${pctLabel}`,
      sublabel: shortPartName(merge.result_part_name),
      color: MERGE_COLOR,
      x: colMerge,
      y: mergeY,
      width: 140,
      height: 40,
    });

    // Edges: parts → merge
    sourceParts.forEach(partName => {
      const isMutationPart = partsInMutation.has(partName);
      allEdges.push({
        from: `part-${partName}`,
        to: 'merge-center',
        color: isMutationPart ? PART_IN_MUTATION_COLOR : PART_COLOR,
      });
    });

    // 3. Mutation nodes (right column) — centered vertically
    const mutStartY = centerY - mutsHeight / 2 + rowHeight / 2;

    affectedMutations.forEach((mut, i) => {
      const mutId = `mut-${mut.mutation_id}`;
      const totalParts = mut.parts_to_do + mut.parts_in_progress;
      allNodes.push({
        id: mutId,
        type: 'mutation',
        label: shortMutationId(mut.mutation_id),
        sublabel: `${totalParts} part${totalParts !== 1 ? 's' : ''} pending`,
        color: MUTATION_COLOR,
        x: colMutations,
        y: mutStartY + i * rowHeight,
        width: 130,
        height: 36,
      });

      // Edge: merge → mutation
      allEdges.push({
        from: 'merge-center',
        to: mutId,
        color: MUTATION_COLOR,
      });

      // Dashed edges: mutation → its specific source parts
      const mutParts = new Set([...mut.parts_in_progress_names, ...mut.parts_to_do_names]);
      for (const partName of sourceParts) {
        if (mutParts.has(partName)) {
          allEdges.push({
            from: mutId,
            to: `part-${partName}`,
            color: `${MUTATION_COLOR}55`,
            dashed: true,
          });
        }
      }
    });

    const w = colMutations + 180;
    const h = topPad + tallestColumn + rowHeight + 20;

    return { nodes: allNodes, edges: allEdges, svgWidth: w, svgHeight: h };
  }, [merge, affectedMutations]);

  const nodeMap = useMemo(() => {
    const m = new Map<string, DiagramNode>();
    for (const n of nodes) m.set(n.id, n);
    return m;
  }, [nodes]);

  const nodeCenter = (node: DiagramNode, side: 'left' | 'right'): [number, number] => {
    const cx = side === 'right' ? node.x + node.width / 2 : node.x - node.width / 2;
    return [cx, node.y];
  };

  const [hoveredColor, setHoveredColor] = useState<string | null>(null);

  const colorMatches = (elementColor: string, legendColor: string): boolean => {
    return elementColor.startsWith(legendColor);
  };

  const nodeOpacity = (node: DiagramNode): number => {
    if (!hoveredColor) return 1;
    return colorMatches(node.color, hoveredColor) ? 1 : 0.15;
  };

  const edgeOpacity = (edge: DiagramEdge): number => {
    if (!hoveredColor) return edge.dashed ? 0.35 : 0.5;
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
              Merge Dependency Map
            </div>
            <div style={{ fontSize: 11, color: 'var(--text-muted, #888)', marginTop: 2 }}>
              {merge.database}.{merge.table} → {merge.result_part_name}
              <span style={{ marginLeft: 8, color: MUTATION_COLOR }}>
                {affectedMutations.length} mutation{affectedMutations.length !== 1 ? 's' : ''} affected
              </span>
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
            { color: MERGE_COLOR, label: 'This merge' },
            { color: PART_COLOR, label: 'Source part' },
            { color: PART_IN_MUTATION_COLOR, label: 'Part in mutation' },
            { color: MUTATION_COLOR, label: 'Affected mutation' },
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
          <text x={100} y={20} textAnchor="middle" fill="var(--text-muted, #888)" fontSize={10} fontWeight={500}>
            Source Parts
          </text>
          <text x={340} y={20} textAnchor="middle" fill="var(--text-muted, #888)" fontSize={10} fontWeight={500}>
            Merge
          </text>
          <text x={580} y={20} textAnchor="middle" fill="var(--text-muted, #888)" fontSize={10} fontWeight={500}>
            Mutations
          </text>

          {/* Edges */}
          {edges.map((edge, i) => {
            const fromNode = nodeMap.get(edge.from);
            const toNode = nodeMap.get(edge.to);
            if (!fromNode || !toNode) return null;

            // Determine direction based on node positions
            const goingRight = fromNode.x < toNode.x;
            const [x1, y1] = nodeCenter(fromNode, goingRight ? 'right' : 'left');
            const [x2, y2] = nodeCenter(toNode, goingRight ? 'left' : 'right');
            const midX = (x1 + x2) / 2;

            return (
              <path
                key={i}
                d={`M ${x1} ${y1} C ${midX} ${y1}, ${midX} ${y2}, ${x2} ${y2}`}
                fill="none"
                stroke={edge.color}
                strokeWidth={edge.dashed ? 1 : 1.5}
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
                  {node.sublabel && (
                    <text
                      x={node.x + rx - 4} y={node.y - ry - 4}
                      textAnchor="end" fill={node.color} fontSize={7} fontWeight={500}
                    >
                      {node.sublabel}
                    </text>
                  )}
                </g>
              );
            }

            if (node.type === 'merge') {
              const cx = node.x;
              const cy = node.y;
              const hw = rx;
              const hh = ry;
              const indent = 12;
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
                    strokeWidth={2}
                  />
                  <text
                    x={cx} y={cy - 4}
                    textAnchor="middle" dominantBaseline="middle"
                    fill={node.color} fontSize={10} fontWeight={700}
                  >
                    {node.label}
                  </text>
                  <text
                    x={cx} y={cy + 10}
                    textAnchor="middle" dominantBaseline="middle"
                    fill={`${node.color}99`} fontSize={7} fontFamily="monospace"
                  >
                    {node.sublabel}
                  </text>
                </g>
              );
            }

            // Mutation nodes: rounded rectangle
            return (
              <g key={node.id} opacity={nodeOpacity(node)} style={{ transition: 'opacity 0.15s' }}>
                <rect
                  x={node.x - rx} y={node.y - ry}
                  width={node.width} height={node.height}
                  rx={6} ry={6}
                  fill={`${node.color}18`}
                  stroke={node.color}
                  strokeWidth={1.5}
                />
                <text
                  x={node.x} y={node.y - 3}
                  textAnchor="middle" dominantBaseline="middle"
                  fill={node.color} fontSize={9} fontWeight={600}
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
