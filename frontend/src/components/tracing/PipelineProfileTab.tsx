

/**
 * PipelineProfileTab - EXPLAIN PIPELINE + processors_profile_log correlation.
 *
 * Auto-fetches on mount. Shows a d3 DAG of the pipeline with processor
 * profile stats overlaid, plus a bar chart and table view.
 * Only shown for SELECT queries.
 */

import React, { useState, useCallback, useEffect, useRef, useMemo } from 'react';
import * as d3 from 'd3';
import type { ExplainResult, ProcessorProfile } from '@tracehouse/core';
import { useClickHouseServices } from '../../providers/ClickHouseProvider';
import { useCapabilityCheck } from '../shared/RequiresCapability';

interface PipelineProfileTabProps {
  querySQL: string;
  queryId: string;
  /** Database context the query originally ran in (from query_log current_database) */
  database?: string;
  /** Optional query_start_time for precise partition pruning */
  eventDate?: string;
}

/** Extract a readable message from any caught error value */
function errMsg(e: unknown, fallback: string): string {
  if (e instanceof Error) return e.message;
  if (e && typeof e === 'object') {
    const obj = e as Record<string, unknown>;
    if (typeof obj.message === 'string') return obj.message;
    const data = obj.data as Record<string, unknown> | undefined;
    if (data && typeof data.message === 'string') return data.message;
    return JSON.stringify(e);
  }
  return fallback;
}
/** Parsed pipeline node for d3 layout */
interface PipeNode {
  id: string;
  name: string;
  fullText: string;
  depth: number;
  parentId: string | null;
  childIds: string[];
  /** The parent stage name (Expression, Sorting, etc.) — null if none */
  stage: string | null;
}

/** Distinct colors for pipeline stage groupings */
const STAGE_COLORS: Record<string, string> = {
  Expression:          '#8b5cf6', // violet
  Aggregating:         '#06b6d4', // cyan
  Filter:              '#f59e0b', // amber
  ReadFromMergeTree:   '#10b981', // emerald
  ReadFromRemote:      '#14b8a6', // teal
  Sorting:             '#f97316', // orange
  Limit:               '#6366f1', // indigo
  Distinct:            '#ec4899', // pink
  Join:                '#ef4444', // red
  Union:               '#84cc16', // lime
  CreatingSets:        '#a855f7', // purple
  MergingSorted:       '#0ea5e9', // sky
  Totals:              '#78716c', // stone
  Rollup:              '#d946ef', // fuchsia
  Cube:                '#e11d48', // rose
  FillingRightJoinSide:'#fb923c', // orange-light
  Window:              '#2dd4bf', // teal-light
  Resize:              '#94a3b8', // slate
};

function fmtUs(us: number): string {
  if (us < 1000) return `${us}µs`;
  if (us < 1_000_000) return `${(us / 1000).toFixed(1)}ms`;
  return `${(us / 1_000_000).toFixed(2)}s`;
}

function fmtBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

function fmtRows(rows: number): string {
  if (rows < 1000) return rows.toLocaleString();
  if (rows < 1_000_000) return `${(rows / 1000).toFixed(1)}K`;
  return `${(rows / 1_000_000).toFixed(1)}M`;
}

/** Brief descriptions for common pipeline stage headers */
const STAGE_DESCRIPTIONS: Record<string, string> = {
  Expression: 'Evaluates expressions (column transforms, aliases, functions)',
  Aggregating: 'Groups rows and computes aggregate functions (GROUP BY)',
  Filter: 'Filters rows based on WHERE / HAVING conditions',
  ReadFromMergeTree: 'Reads data from MergeTree table parts',
  ReadFromRemote: 'Reads data from remote shards',
  Sorting: 'Sorts rows (ORDER BY)',
  Limit: 'Limits output row count (LIMIT)',
  Distinct: 'Removes duplicate rows (DISTINCT)',
  Join: 'Joins two data streams (JOIN)',
  Union: 'Combines multiple streams (UNION)',
  CreatingSets: 'Builds sets for IN subqueries',
  MergingSorted: 'Merges pre-sorted streams',
  Totals: 'Computes WITH TOTALS aggregates',
  Rollup: 'Computes WITH ROLLUP aggregates',
  Cube: 'Computes WITH CUBE aggregates',
  FillingRightJoinSide: 'Builds hash table for the right side of a JOIN',
  Window: 'Evaluates window functions (OVER)',
  Resize: 'Adjusts parallelism (changes number of streams)',
};

/** Infer the pipeline stage from a processor name.
 *  Maps processor names like "ExpressionTransform", "AggregatingTransform",
 *  "FilterTransform" etc. back to their logical stage grouping.
 */
function inferStageFromName(name: string): string | null {
  const n = name.toLowerCase();
  if (n.includes('expression')) return 'Expression';
  if (n.includes('aggregat')) return 'Aggregating';
  if (n.includes('filter')) return 'Filter';
  if (n.includes('sort') || n.includes('mergesort')) return 'Sorting';
  if (n.includes('limit')) return 'Limit';
  if (n.includes('distinct')) return 'Distinct';
  if (n.includes('join')) return 'Join';
  if (n.includes('union')) return 'Union';
  if (n.includes('window')) return 'Window';
  if (n.includes('mergetree') || n.includes('mergetreeselect') || n.includes('readfrom')) return 'ReadFromMergeTree';
  if (n.includes('remote')) return 'ReadFromRemote';
  if (n.includes('creatingsetsfor') || n.includes('creatingsets')) return 'CreatingSets';
  if (n.includes('mergingsorted')) return 'MergingSorted';
  if (n.includes('totals')) return 'Totals';
  if (n.includes('rollup')) return 'Rollup';
  if (n.includes('cube')) return 'Cube';
  if (n.includes('resize')) return 'Resize';
  if (n.includes('lazy')) return 'Limit'; // LazyMaterializingTransform is limit-related
  return null;
}

/** Parse EXPLAIN PIPELINE text into a node tree.
 *  Stage headers like (Expression) are NOT emitted as nodes — instead they
 *  become the `stage` label on the processor nodes they contain.
 */
function parsePipeline(output: string): PipeNode[] {
  const lines = output.split('\n').filter(l => l.trim());

  // First pass: build raw nodes including stages
  interface RawNode {
    id: string; name: string; fullText: string; depth: number;
    parentId: string | null; childIds: string[]; isStage: boolean; stageName: string | null;
  }
  const raw: RawNode[] = [];
  const stack: { id: string; depth: number }[] = [];
  let idx = 0;

  for (const line of lines) {
    const trimmed = line.trimStart();
    const spaces = line.length - trimmed.length;
    const depth = Math.floor(spaces / 2);

    const parenWrap = trimmed.match(/^\(([^)]+)\)$/);
    const isStage = !!parenWrap;

    let name = trimmed;
    if (parenWrap) {
      name = parenWrap[1];
    } else {
      const paramMatch = trimmed.match(/^(\w+)/);
      if (paramMatch) name = paramMatch[1];
    }
    if (!name) continue;

    const id = `n${idx++}`;
    while (stack.length > 0 && stack[stack.length - 1].depth >= depth) stack.pop();

    let parentId: string | null = null;
    if (stack.length > 0) {
      parentId = stack[stack.length - 1].id;
      const parent = raw.find(n => n.id === parentId);
      if (parent) parent.childIds.push(id);
    }

    raw.push({ id, name, fullText: trimmed, depth, parentId, childIds: [], isStage, stageName: null });
    stack.push({ id, depth });
  }

  // Second pass: propagate stage names down through the entire subtree.
  // Walk the tree top-down; when we hit a stage node we remember its name
  // and assign it to every processor descendant until a *different* stage
  // is encountered (which starts a new scope).
  const rawMap = new Map(raw.map(n => [n.id, n]));

  function propagateStage(nodeId: string, currentStage: string | null) {
    const node = rawMap.get(nodeId);
    if (!node) return;
    if (node.isStage) {
      // This stage becomes the new scope for all descendants
      for (const cid of node.childIds) propagateStage(cid, node.name);
    } else {
      node.stageName = currentStage;
      for (const cid of node.childIds) propagateStage(cid, currentStage);
    }
  }

  // Start from root nodes (those with no parent)
  for (const node of raw) {
    if (!node.parentId) propagateStage(node.id, null);
  }

  // Third pass: build final nodes, skipping stages and re-linking parents
  function findNonStageAncestor(id: string | null): string | null {
    while (id) {
      const n = rawMap.get(id);
      if (!n) return null;
      if (!n.isStage) return id;
      id = n.parentId;
    }
    return null;
  }

  const processors = raw.filter(n => !n.isStage);
  // Re-assign depths contiguously
  const depthMap = new Map<number, number>();
  let nextDepth = 0;
  const sortedDepths = [...new Set(processors.map(n => n.depth))].sort((a, b) => a - b);
  for (const d of sortedDepths) depthMap.set(d, nextDepth++);

  const nodes: PipeNode[] = [];
  for (const p of processors) {
    const newParent = findNonStageAncestor(p.parentId);
    // If no explicit stage was assigned, infer from the processor name
    let stage = p.stageName;
    if (!stage) {
      stage = inferStageFromName(p.name);
    }
    nodes.push({
      id: p.id,
      name: p.name,
      fullText: p.fullText,
      depth: depthMap.get(p.depth) ?? p.depth,
      parentId: newParent,
      childIds: [], // rebuilt below
      stage,
    });
  }

  // Rebuild childIds
  const nodeMap = new Map(nodes.map(n => [n.id, n]));
  for (const n of nodes) {
    if (n.parentId) {
      const parent = nodeMap.get(n.parentId);
      if (parent) parent.childIds.push(n.id);
    }
  }

  return nodes;
}

/** Stage legend shown above the DAG */
const StageLegend: React.FC<{ stages: string[] }> = ({ stages }) => (
  <div style={{
    marginBottom: 14, padding: '10px 14px',
    background: 'var(--bg-code)', border: '1px solid var(--border-primary)', borderRadius: 8,
  }}>
    <div style={{
      display: 'flex', alignItems: 'center', justifyContent: 'space-between',
      marginBottom: stages.length > 0 ? 8 : 0,
    }}>
      <span style={{
        fontSize: 9, textTransform: 'uppercase', letterSpacing: '1px',
        color: 'var(--text-muted)', fontWeight: 600,
      }}>Pipeline Stages</span>
      {/* Timing legend */}
      <span style={{ display: 'flex', gap: 10, alignItems: 'center' }}>
        <span style={{ display: 'flex', alignItems: 'center', gap: 3 }}>
          <span style={{ width: 8, height: 8, borderRadius: 2, background: '#3b82f6' }} />
          <span style={{ fontSize: 9, color: 'var(--text-muted)' }}>Active (real work)</span>
        </span>
        <span style={{ display: 'flex', alignItems: 'center', gap: 3 }}>
          <span style={{ width: 8, height: 8, borderRadius: 2, background: '#f59e0b' }} />
          <span style={{ fontSize: 9, color: 'var(--text-muted)' }}>Input wait (blocked)</span>
        </span>
        <span style={{ display: 'flex', alignItems: 'center', gap: 3 }}>
          <span style={{ width: 8, height: 8, borderRadius: 2, background: '#ef4444' }} />
          <span style={{ fontSize: 9, color: 'var(--text-muted)' }}>Output wait (backpressure)</span>
        </span>
      </span>
    </div>
    {stages.length > 0 && (
      <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
        {stages.map(s => {
          const color = STAGE_COLORS[s] || '#94a3b8';
          const desc = STAGE_DESCRIPTIONS[s] || '';
          return (
            <div key={s} style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <span style={{
                width: 12, height: 12, borderRadius: 3, background: color, flexShrink: 0,
              }} />
              <span style={{
                fontSize: 11, fontFamily: 'ui-monospace, monospace', fontWeight: 600,
                color: 'var(--text-primary)', minWidth: 140,
              }}>{s}</span>
              {desc && (
                <span style={{ fontSize: 10, color: 'var(--text-muted)' }}>{desc}</span>
              )}
            </div>
          );
        })}
      </div>
    )}
  </div>
);

/** D3 DAG visualization of the pipeline */
const PipelineDAG: React.FC<{
  nodes: PipeNode[];
  findProfile: (name: string) => ProcessorProfile | undefined;
  maxElapsed: number;
}> = ({ nodes, findProfile, maxElapsed }) => {
  const svgRef = useRef<SVGSVGElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  // Collect unique stages for the legend
  const stages = useMemo(() => {
    const set = new Set<string>();
    for (const n of nodes) if (n.stage) set.add(n.stage);
    return [...set];
  }, [nodes]);

  useEffect(() => {
    if (!svgRef.current || !containerRef.current || nodes.length === 0) return;

    const svg = d3.select(svgRef.current);
    svg.selectAll('*').remove();

    // Layout constants — generous sizing for readability
    const nodeW = 360;
    const nodeH = 100;
    const hGap = 40;
    const vGap = 48;
    const padTop = 24;
    const padSide = 40;
    const borderW = 5; // colored left border width

    // Group nodes by depth
    const byDepth = new Map<number, PipeNode[]>();
    for (const n of nodes) {
      const arr = byDepth.get(n.depth) || [];
      arr.push(n);
      byDepth.set(n.depth, arr);
    }
    const maxDepth = Math.max(...nodes.map(n => n.depth), 0);

    // Calculate positions: top-down, centered per layer
    const positions = new Map<string, { x: number; y: number }>();
    let maxX = 0;
    for (let d = 0; d <= maxDepth; d++) {
      const layer = byDepth.get(d) || [];
      const layerWidth = layer.length * (nodeW + hGap) - hGap;
      const startX = -layerWidth / 2;
      layer.forEach((n, i) => {
        const x = startX + i * (nodeW + hGap) + nodeW / 2;
        const y = d * (nodeH + vGap) + nodeH / 2;
        positions.set(n.id, { x, y });
        maxX = Math.max(maxX, Math.abs(x) + nodeW / 2);
      });
    }

    const totalH = (maxDepth + 1) * (nodeH + vGap) + padTop * 2;
    const totalW = maxX * 2 + padSide * 2;
    const cw = containerRef.current.clientWidth;
    const width = Math.max(totalW, cw);
    const offsetX = width / 2;

    svg.attr('width', width).attr('height', totalH);

    // Arrow marker
    const defs = svg.append('defs');
    defs.append('marker')
      .attr('id', 'arrow')
      .attr('viewBox', '0 0 10 6')
      .attr('refX', 10)
      .attr('refY', 3)
      .attr('markerWidth', 8)
      .attr('markerHeight', 6)
      .attr('orient', 'auto')
      .append('path')
      .attr('d', 'M0,0 L10,3 L0,6 Z')
      .attr('fill', '#6b7280');

    const g = svg.append('g').attr('transform', `translate(${offsetX}, ${padTop})`);

    // Draw edges
    for (const node of nodes) {
      if (!node.parentId) continue;
      const from = positions.get(node.parentId);
      const to = positions.get(node.id);
      if (!from || !to) continue;

      const y1 = from.y + nodeH / 2;
      const y2 = to.y - nodeH / 2;
      const midY = (y1 + y2) / 2;

      g.append('path')
        .attr('d', `M${from.x},${y1} C${from.x},${midY} ${to.x},${midY} ${to.x},${y2}`)
        .attr('fill', 'none')
        .attr('stroke', '#6b7280')
        .attr('stroke-width', 1.5)
        .attr('marker-end', 'url(#arrow)');
    }

    // Draw processor nodes
    for (const node of nodes) {
      const pos = positions.get(node.id);
      if (!pos) continue;
      const profile = findProfile(node.name);
      const hasProfile = !!profile;
      const stageColor = node.stage ? (STAGE_COLORS[node.stage] || '#94a3b8') : '#d1d5db';

      const ng = g.append('g')
        .attr('transform', `translate(${pos.x - nodeW / 2}, ${pos.y - nodeH / 2})`);

      // Unique clip id for this node's left border
      const clipId = `clip-${node.id}`;
      defs.append('clipPath')
        .attr('id', clipId)
        .append('rect')
        .attr('x', 0).attr('y', 0)
        .attr('width', borderW + 1).attr('height', nodeH)
        .attr('rx', 8);

      // Card background with stage-colored left stroke
      ng.append('rect')
        .attr('width', nodeW)
        .attr('height', nodeH)
        .attr('rx', 8)
        .attr('fill', '#f8fafc')
        .attr('stroke', stageColor)
        .attr('stroke-width', 1);

      // Thick colored left border accent (clipped to left edge)
      ng.append('rect')
        .attr('x', 0).attr('y', 0)
        .attr('width', borderW).attr('height', nodeH)
        .attr('fill', stageColor)
        .attr('clip-path', `url(#${clipId})`);

      // Stage badge (colored tag)
      if (node.stage) {
        const badgeText = node.stage;
        const badgeW = Math.min(badgeText.length * 7.5 + 16, 160);
        ng.append('rect')
          .attr('x', borderW + 8)
          .attr('y', 6)
          .attr('width', badgeW)
          .attr('height', 18)
          .attr('rx', 4)
          .attr('fill', stageColor)
          .attr('opacity', 0.18);
        ng.append('rect')
          .attr('x', borderW + 8)
          .attr('y', 6)
          .attr('width', badgeW)
          .attr('height', 18)
          .attr('rx', 4)
          .attr('fill', 'none')
          .attr('stroke', stageColor)
          .attr('stroke-width', 1)
          .attr('opacity', 0.6);
        ng.append('text')
          .attr('x', borderW + 8 + badgeW / 2)
          .attr('y', 15)
          .attr('text-anchor', 'middle')
          .attr('dominant-baseline', 'central')
          .attr('font-family', 'ui-monospace, monospace')
          .attr('font-size', 10)
          .attr('font-weight', 700)
          .attr('fill', stageColor)
          .text(badgeText);
      }

      // Processor name
      const displayName = node.fullText.length > 42 ? node.fullText.slice(0, 40) + '…' : node.fullText;
      ng.append('text')
        .attr('x', borderW + 10)
        .attr('y', node.stage ? 40 : 28)
        .attr('dominant-baseline', 'central')
        .attr('font-family', 'ui-monospace, monospace')
        .attr('font-size', 13)
        .attr('font-weight', 600)
        .attr('fill', '#1e293b')
        .text(displayName);

      // Mini stacked timing bar + stats
      // elapsed_us = active CPU work, input_wait_us / output_wait_us = blocked time
      // Total wall time ≈ elapsed + input_wait + output_wait
      if (hasProfile) {
        const barY = node.stage ? 54 : 44;
        const barH = 10;
        const labelW = 55;
        const barX = borderW + 10;
        const barMaxW = nodeW - barX - labelW - 10;
        const wallTime = profile.elapsed_us + profile.input_wait_us + profile.output_wait_us;
        const activePct = wallTime > 0 ? profile.elapsed_us / wallTime : 1;
        const inPct = wallTime > 0 ? profile.input_wait_us / wallTime : 0;
        const outPct = wallTime > 0 ? profile.output_wait_us / wallTime : 0;
        // Scale bar width relative to max wall time
        const barW = Math.min(maxElapsed > 0 ? (wallTime / maxElapsed) * barMaxW : barMaxW, barMaxW);

        // Bar background
        ng.append('rect')
          .attr('x', barX)
          .attr('y', barY)
          .attr('width', barMaxW)
          .attr('height', barH)
          .attr('rx', 2)
          .attr('fill', '#e2e8f0');

        // Active segment (blue)
        let cx = barX;
        const activeW = activePct * barW;
        if (activeW > 0) {
          ng.append('rect')
            .attr('x', cx).attr('y', barY)
            .attr('width', activeW).attr('height', barH)
            .attr('rx', 2).attr('fill', '#3b82f6');
          cx += activeW;
        }
        // Input wait segment (amber)
        const inW = inPct * barW;
        if (inW > 0) {
          ng.append('rect')
            .attr('x', cx).attr('y', barY)
            .attr('width', inW).attr('height', barH)
            .attr('fill', '#f59e0b');
          cx += inW;
        }
        // Output wait segment (red)
        const outW = outPct * barW;
        if (outW > 0) {
          ng.append('rect')
            .attr('x', cx).attr('y', barY)
            .attr('width', outW).attr('height', barH)
            .attr('fill', '#ef4444');
        }

        // Timing label to the right of the bar
        ng.append('text')
          .attr('x', nodeW - 10)
          .attr('y', barY + barH / 2)
          .attr('text-anchor', 'end')
          .attr('dominant-baseline', 'central')
          .attr('font-family', 'ui-monospace, monospace')
          .attr('font-size', 10)
          .attr('fill', '#475569')
          .attr('font-weight', 500)
          .text(fmtUs(wallTime));

        // Rows throughput below bar
        const rowY = barY + barH + 12;
        const rowText = `${fmtRows(profile.input_rows)} → ${fmtRows(profile.output_rows)} rows`;
        const extra = profile.instances > 1 ? `  ×${profile.instances}` : '';
        ng.append('text')
          .attr('x', borderW + 10)
          .attr('y', rowY)
          .attr('dominant-baseline', 'central')
          .attr('font-family', 'ui-monospace, monospace')
          .attr('font-size', 10)
          .attr('fill', '#94a3b8')
          .text(rowText + extra);
      }

      // Tooltip
      const tipLines = [node.fullText];
      if (node.stage) {
        const desc = STAGE_DESCRIPTIONS[node.stage];
        tipLines.push(`Stage: ${node.stage}${desc ? ' — ' + desc : ''}`);
      }
      if (hasProfile) {
        const wallTime = profile.elapsed_us + profile.input_wait_us + profile.output_wait_us;
        tipLines.push(
          `Wall time: ${fmtUs(wallTime)}`,
          `Active (real work): ${fmtUs(profile.elapsed_us)}`,
          `Input wait (blocked): ${fmtUs(profile.input_wait_us)}`,
          `Output wait (backpressure): ${fmtUs(profile.output_wait_us)}`,
          `Rows: ${fmtRows(profile.input_rows)} → ${fmtRows(profile.output_rows)}`,
          `Bytes: ${fmtBytes(profile.input_bytes)} → ${fmtBytes(profile.output_bytes)}`,
          `Instances: ${profile.instances}`,
        );
      }
      ng.append('title').text(tipLines.join('\n'));
    }
  }, [nodes, findProfile, maxElapsed]);

  return (
    <div>
      {stages.length > 0 && <StageLegend stages={stages} />}
      <div ref={containerRef} style={{ overflow: 'auto' }}>
        <svg ref={svgRef} style={{ display: 'block', margin: '0 auto' }} />
      </div>
    </div>
  );
};

/** Bar chart for a single processor */
const ProcessorBar: React.FC<{
  profile: ProcessorProfile;
  maxElapsed: number;
}> = ({ profile, maxElapsed }) => {
  // Wall time = active work + input wait + output wait
  const wallTime = profile.elapsed_us + profile.input_wait_us + profile.output_wait_us;
  const pct = maxElapsed > 0 ? (wallTime / maxElapsed) * 100 : 0;
  const activePct = wallTime > 0 ? (profile.elapsed_us / wallTime) * 100 : 100;
  const inputWaitPct = wallTime > 0 ? (profile.input_wait_us / wallTime) * 100 : 0;
  const outputWaitPct = wallTime > 0 ? (profile.output_wait_us / wallTime) * 100 : 0;

  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 8, minHeight: 26 }}>
      <div style={{
        width: 200, flexShrink: 0, fontFamily: 'monospace', fontSize: 11,
        color: 'var(--text-secondary)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
      }} title={profile.name}>
        {profile.name}
        {profile.instances > 1 && <span style={{ color: 'var(--text-muted)', fontSize: 10 }}> ×{profile.instances}</span>}
      </div>
      <div style={{ flex: 1, height: 16, background: 'var(--bg-tertiary)', borderRadius: 3, overflow: 'hidden', position: 'relative' }}>
        <div style={{ width: `${pct}%`, height: '100%', display: 'flex', minWidth: pct > 0 ? 2 : 0 }}>
          {activePct > 0 && (
            <div style={{ flex: `${activePct} 0 0%`, background: '#3b82f6', minWidth: 1 }}
              title={`Active: ${fmtUs(profile.elapsed_us)}`} />
          )}
          {inputWaitPct > 0 && (
            <div style={{ flex: `${inputWaitPct} 0 0%`, background: '#f59e0b', minWidth: 1 }}
              title={`Input wait: ${fmtUs(profile.input_wait_us)}`} />
          )}
          {outputWaitPct > 0 && (
            <div style={{ flex: `${outputWaitPct} 0 0%`, background: '#ef4444', minWidth: 1 }}
              title={`Output wait: ${fmtUs(profile.output_wait_us)}`} />
          )}
        </div>
      </div>
      <div style={{ width: 65, flexShrink: 0, fontFamily: 'monospace', fontSize: 11, color: 'var(--text-muted)', textAlign: 'right' }}>
        {fmtUs(wallTime)}
      </div>
    </div>
  );
};

/** Table view */
const ProcessorTable: React.FC<{ profiles: ProcessorProfile[] }> = ({ profiles }) => (
  <div style={{ background: 'var(--bg-code)', border: '1px solid var(--border-primary)', borderRadius: 8, overflow: 'auto' }}>
    <table style={{ width: '100%', borderCollapse: 'collapse', fontFamily: 'monospace', fontSize: 11 }}>
      <thead>
        <tr style={{ borderBottom: '1px solid var(--border-secondary)' }}>
          {['Processor', 'Wall Time', 'Active', 'Input Wait', 'Output Wait', 'In Rows', 'Out Rows', 'In Bytes', 'Out Bytes', '×'].map(h => (
            <th key={h} style={{
              padding: '8px 10px', textAlign: h === 'Processor' ? 'left' : 'right',
              fontSize: 9, textTransform: 'uppercase', letterSpacing: '1px', color: 'var(--text-muted)', fontWeight: 500,
            }}>{h}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {profiles.map(p => {
          const wallTime = p.elapsed_us + p.input_wait_us + p.output_wait_us;
          return (
            <tr key={p.name} style={{ borderBottom: '1px solid var(--border-primary)' }}>
              <td style={{ padding: '6px 10px', color: 'var(--text-secondary)' }}>{p.name}</td>
              <td style={{ padding: '6px 10px', textAlign: 'right', color: 'var(--text-primary)', fontWeight: 500 }}>{fmtUs(wallTime)}</td>
              <td style={{ padding: '6px 10px', textAlign: 'right', color: '#3b82f6' }}>{fmtUs(p.elapsed_us)}</td>
              <td style={{ padding: '6px 10px', textAlign: 'right', color: p.input_wait_us > 0 ? '#f59e0b' : 'var(--text-muted)' }}>{fmtUs(p.input_wait_us)}</td>
              <td style={{ padding: '6px 10px', textAlign: 'right', color: p.output_wait_us > 0 ? '#ef4444' : 'var(--text-muted)' }}>{fmtUs(p.output_wait_us)}</td>
              <td style={{ padding: '6px 10px', textAlign: 'right', color: 'var(--text-muted)' }}>{fmtRows(p.input_rows)}</td>
              <td style={{ padding: '6px 10px', textAlign: 'right', color: 'var(--text-muted)' }}>{fmtRows(p.output_rows)}</td>
              <td style={{ padding: '6px 10px', textAlign: 'right', color: 'var(--text-muted)' }}>{fmtBytes(p.input_bytes)}</td>
              <td style={{ padding: '6px 10px', textAlign: 'right', color: 'var(--text-muted)' }}>{fmtBytes(p.output_bytes)}</td>
              <td style={{ padding: '6px 10px', textAlign: 'right', color: 'var(--text-muted)' }}>{p.instances}</td>
            </tr>
          );
        })}
      </tbody>
    </table>
  </div>
);

export const PipelineProfileTab: React.FC<PipelineProfileTabProps> = ({
  querySQL,
  queryId,
  database,
  eventDate,
}) => {
  const services = useClickHouseServices();
  const { available: hasProcessorProfileLog } = useCapabilityCheck(['processors_profile_log']);

  const [pipeline, setPipeline] = useState<ExplainResult | null>(null);
  const [isLoadingPipeline, setIsLoadingPipeline] = useState(false);
  const [pipelineError, setPipelineError] = useState<string | null>(null);

  const [profiles, setProfiles] = useState<ProcessorProfile[]>([]);
  const [isLoadingProfiles, setIsLoadingProfiles] = useState(false);
  const [profilesError, setProfilesError] = useState<string | null>(null);

  const [viewMode, setViewMode] = useState<'dag' | 'bars' | 'table'>('dag');
  const [showRawPlan, setShowRawPlan] = useState(false);

  const fetchPipeline = useCallback(async () => {
    if (!services) return;
    setIsLoadingPipeline(true);
    setPipelineError(null);
    try {
      // querySQL may be truncated (substring(query,1,120) from time travel).
      // Fetch the full query text by query_id to avoid unclosed-string errors.
      let fullSQL = querySQL;
      let db = database;
      try {
        const detail = await services.queryAnalyzer.getQueryDetail(queryId, eventDate);
        if (detail?.query) {
          fullSQL = detail.query;
        }
        if (!db && detail?.current_database) {
          db = detail.current_database;
        }
      } catch (err) {
        console.warn('[PipelineProfileTab] Failed to fetch full query text:', err);
      }

      const result = await services.traceService.executeExplainWithFallback(fullSQL, 'PIPELINE', db);
      setPipeline(result);
    } catch (e) {
      setPipelineError(errMsg(e, 'Failed to run EXPLAIN PIPELINE'));
    } finally {
      setIsLoadingPipeline(false);
    }
  }, [services, querySQL, queryId, database, eventDate]);

  const fetchProfiles = useCallback(async () => {
    if (!services) return;
    setIsLoadingProfiles(true);
    setProfilesError(null);
    try {
      const result = await services.traceService.getProcessorProfiles(queryId, eventDate);
      setProfiles(result);
    } catch (e) {
      setProfilesError(errMsg(e, 'Failed to fetch processor profiles'));
    } finally {
      setIsLoadingProfiles(false);
    }
  }, [services, queryId, eventDate]);

  // Auto-fetch on mount
  useEffect(() => {
    fetchPipeline();
  }, [fetchPipeline]);

  useEffect(() => {
    if (hasProcessorProfileLog) fetchProfiles();
  }, [hasProcessorProfileLog, fetchProfiles]);

  const pipelineNodes = useMemo(
    () => (pipeline ? parsePipeline(pipeline.output) : []),
    [pipeline],
  );
  const profileMap = useMemo(() => new Map(profiles.map(p => [p.name, p])), [profiles]);

  // Build a secondary lookup that maps the first word of a profile name to the profile,
  // so "MergeTreeSelect" matches "MergeTreeSelect(pool: ReadPool, algorithm: Thread)"
  const profileByPrefix = useMemo(() => {
    const map = new Map<string, ProcessorProfile>();
    for (const p of profiles) {
      const firstWord = p.name.match(/^(\w+)/)?.[1];
      if (firstWord && !map.has(firstWord)) map.set(firstWord, p);
    }
    return map;
  }, [profiles]);

  /** Look up a profile by node name — tries exact match, then prefix */
  const findProfile = useCallback((nodeName: string): ProcessorProfile | undefined => {
    return profileMap.get(nodeName) ?? profileByPrefix.get(nodeName);
  }, [profileMap, profileByPrefix]);
  const maxElapsed = useMemo(
    () => (profiles.length > 0 ? Math.max(...profiles.map(p => p.elapsed_us + p.input_wait_us + p.output_wait_us)) : 0),
    [profiles],
  );

  // Profiles that exist in processors_profile_log but not in EXPLAIN PIPELINE
  const unmatchedProfiles = useMemo(() => {
    if (pipelineNodes.length === 0 || profiles.length === 0) return [];
    const dagNames = new Set(pipelineNodes.map(n => n.name));
    return profiles.filter(p => {
      const firstName = p.name.match(/^(\w+)/)?.[1] || p.name;
      return !dagNames.has(p.name) && !dagNames.has(firstName);
    });
  }, [pipelineNodes, profiles]);

  const isLoading = isLoadingPipeline || isLoadingProfiles;

  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      {/* Toolbar */}
      <div style={{
        padding: '8px 16px',
        borderBottom: '1px solid var(--border-secondary)',
        background: 'var(--bg-card)',
        display: 'flex',
        alignItems: 'center',
        gap: 8,
        flexShrink: 0,
      }}>
        {/* EXPLAIN PIPELINE badge */}
        <span style={{
          fontFamily: 'monospace', fontSize: 10, textTransform: 'uppercase', letterSpacing: '1.2px',
          padding: '3px 8px', borderRadius: 4,
          background: 'rgba(var(--accent-primary-rgb), 0.12)',
          color: 'var(--accent-primary)',
          border: '1px solid rgba(var(--accent-primary-rgb), 0.25)',
          whiteSpace: 'nowrap',
        }}>
          EXPLAIN PIPELINE
        </span>

        <button
          onClick={() => { fetchPipeline(); if (hasProcessorProfileLog) fetchProfiles(); }}
          disabled={isLoading}
          style={{
            padding: '5px 12px', fontSize: 11, fontFamily: 'monospace', borderRadius: 6,
            border: '1px solid var(--border-accent)',
            background: 'rgba(var(--accent-primary-rgb), 0.15)',
            color: 'var(--text-primary)',
            cursor: isLoading ? 'wait' : 'pointer',
            opacity: isLoading ? 0.6 : 1,
          }}
        >
          {isLoading ? 'Loading…' : '↻ Refresh'}
        </button>

        {pipeline && (
          <button
            onClick={() => setShowRawPlan(v => !v)}
            style={{
              padding: '5px 12px', fontSize: 11, fontFamily: 'monospace', borderRadius: 6,
              border: '1px solid var(--border-secondary)',
              background: showRawPlan ? 'var(--bg-tertiary)' : 'transparent',
              color: 'var(--text-secondary)',
              cursor: 'pointer',
            }}
          >
            {showRawPlan ? '▾ Hide Plan Text' : '▸ Show Plan Text'}
          </button>
        )}

        {!hasProcessorProfileLog && (
          <span style={{ fontSize: 10, color: 'var(--text-muted)', fontStyle: 'italic' }}>
            processors_profile_log unavailable — enable log_processors_profiles=1 for timing data
          </span>
        )}

        {/* View toggle */}
        <div style={{ marginLeft: 'auto', display: 'flex', gap: 2 }}>
          {(['dag', 'bars', 'table'] as const).map(mode => (
            <button
              key={mode}
              onClick={() => setViewMode(mode)}
              style={{
                padding: '4px 10px', fontSize: 10, fontFamily: 'monospace', textTransform: 'uppercase',
                borderRadius: 4,
                border: viewMode === mode ? '1px solid var(--border-accent)' : '1px solid transparent',
                background: viewMode === mode ? 'rgba(var(--accent-primary-rgb), 0.1)' : 'transparent',
                color: viewMode === mode ? 'var(--text-primary)' : 'var(--text-muted)',
                cursor: 'pointer',
              }}
            >
              {mode}
            </button>
          ))}
        </div>
      </div>

      {/* Content */}
      <div style={{ flex: 1, overflow: 'auto', padding: 16 }}>
        {/* Errors */}
        {pipelineError && (
          <div style={{
            padding: '10px 14px', borderRadius: 6, marginBottom: 12,
            background: 'rgba(var(--color-error-rgb), 0.08)',
            border: '1px solid rgba(var(--color-error-rgb), 0.2)',
            color: 'var(--color-error)', fontSize: 12, fontFamily: 'monospace',
          }}>{pipelineError}</div>
        )}
        {profilesError && (
          <div style={{
            padding: '10px 14px', borderRadius: 6, marginBottom: 12,
            background: 'rgba(var(--color-warning-rgb), 0.08)',
            border: '1px solid rgba(var(--color-warning-rgb), 0.2)',
            color: 'var(--color-warning)', fontSize: 12, fontFamily: 'monospace',
          }}>{profilesError}</div>
        )}

        {/* Loading */}
        {isLoading && !pipeline && (
          <div style={{ textAlign: 'center', padding: '48px 0', color: 'var(--text-muted)', fontSize: 12 }}>
            Loading pipeline…
          </div>
        )}

        {/* DAG view */}
        {viewMode === 'dag' && pipelineNodes.length > 0 && (
          <>
            <PipelineDAG nodes={pipelineNodes} findProfile={findProfile} maxElapsed={maxElapsed} />
            {/* Raw EXPLAIN PIPELINE text */}
            {showRawPlan && pipeline && (
              <div style={{ marginTop: 16 }}>
                <div style={{
                  fontSize: 9, color: 'var(--text-muted)', textTransform: 'uppercase',
                  letterSpacing: '1px', marginBottom: 6,
                }}>
                  Raw EXPLAIN PIPELINE Output
                </div>
                <pre style={{
                  background: 'var(--bg-code)', border: '1px solid var(--border-primary)',
                  borderRadius: 8, padding: '12px 14px', margin: 0,
                  fontFamily: 'monospace', fontSize: 11, lineHeight: 1.6,
                  color: 'var(--text-secondary)', overflow: 'auto', maxHeight: 400,
                  whiteSpace: 'pre', tabSize: 2,
                }}>
                  {pipeline.output}
                </pre>
              </div>
            )}
            {/* Unmatched processors — in profile log but not in EXPLAIN PIPELINE */}
            {unmatchedProfiles.length > 0 && (
              <div style={{ marginTop: 16 }}>
                <div style={{
                  fontSize: 9, color: 'var(--text-muted)', textTransform: 'uppercase',
                  letterSpacing: '1px', marginBottom: 6,
                }}>
                  Additional Runtime Processors ({unmatchedProfiles.length})
                  <span style={{ textTransform: 'none', letterSpacing: 'normal', fontStyle: 'italic', marginLeft: 8 }}>
                    — present in processors_profile_log but not in EXPLAIN PIPELINE
                  </span>
                </div>
                <div style={{ background: 'var(--bg-code)', border: '1px solid var(--border-primary)', borderRadius: 8, padding: '10px 14px' }}>
                  {unmatchedProfiles.map(p => <ProcessorBar key={p.name} profile={p} maxElapsed={maxElapsed} />)}
                </div>
              </div>
            )}
          </>
        )}

        {/* Bars view */}
        {viewMode === 'bars' && profiles.length > 0 && (
          <div>
            <div style={{
              fontSize: 9, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '1px',
              marginBottom: 8, display: 'flex', alignItems: 'center', gap: 12,
            }}>
              <span>Processor Profile ({profiles.length})</span>
              <span style={{ display: 'flex', gap: 8, fontSize: 9 }}>
                <span style={{ display: 'flex', alignItems: 'center', gap: 3 }}>
                  <span style={{ width: 8, height: 8, borderRadius: 2, background: '#3b82f6', display: 'inline-block' }} /> Active (real work)
                </span>
                <span style={{ display: 'flex', alignItems: 'center', gap: 3 }}>
                  <span style={{ width: 8, height: 8, borderRadius: 2, background: '#f59e0b', display: 'inline-block' }} /> Input wait (blocked)
                </span>
                <span style={{ display: 'flex', alignItems: 'center', gap: 3 }}>
                  <span style={{ width: 8, height: 8, borderRadius: 2, background: '#ef4444', display: 'inline-block' }} /> Output wait (backpressure)
                </span>
              </span>
            </div>
            <div style={{ background: 'var(--bg-code)', border: '1px solid var(--border-primary)', borderRadius: 8, padding: '10px 14px' }}>
              {profiles.map(p => <ProcessorBar key={p.name} profile={p} maxElapsed={maxElapsed} />)}
            </div>
          </div>
        )}
        {viewMode === 'bars' && profiles.length === 0 && !isLoading && (
          <div style={{ textAlign: 'center', padding: '32px 0', color: 'var(--text-muted)', fontSize: 12 }}>
            {hasProcessorProfileLog ? 'No processor profile data for this query.' : 'Enable log_processors_profiles=1 to see processor timing.'}
          </div>
        )}

        {/* Table view */}
        {viewMode === 'table' && profiles.length > 0 && (
          <ProcessorTable profiles={profiles} />
        )}
        {viewMode === 'table' && profiles.length === 0 && !isLoading && (
          <div style={{ textAlign: 'center', padding: '32px 0', color: 'var(--text-muted)', fontSize: 12 }}>
            {hasProcessorProfileLog ? 'No processor profile data for this query.' : 'Enable log_processors_profiles=1 to see processor timing.'}
          </div>
        )}

        {/* Raw plan text for bars/table views */}
        {viewMode !== 'dag' && showRawPlan && pipeline && (
          <div style={{ marginTop: 16 }}>
            <div style={{
              fontSize: 9, color: 'var(--text-muted)', textTransform: 'uppercase',
              letterSpacing: '1px', marginBottom: 6,
            }}>
              Raw EXPLAIN PIPELINE Output
            </div>
            <pre style={{
              background: 'var(--bg-code)', border: '1px solid var(--border-primary)',
              borderRadius: 8, padding: '12px 14px', margin: 0,
              fontFamily: 'monospace', fontSize: 11, lineHeight: 1.6,
              color: 'var(--text-secondary)', overflow: 'auto', maxHeight: 400,
              whiteSpace: 'pre', tabSize: 2,
            }}>
              {pipeline.output}
            </pre>
          </div>
        )}
      </div>
    </div>
  );
};

export default PipelineProfileTab;
