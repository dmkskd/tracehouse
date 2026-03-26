/**
 * MergeHealthSunburst — hover-to-expand radial health visualization
 *
 * Derives a health tree from live merge store data (active merges, pool metrics,
 * mutations). Inner rings show category-level health (green/yellow/red); hovering
 * reveals deeper rings with table-level and per-merge detail.
 *
 * Hovering outer segments also zooms the entire sunburst (scale + translate on the
 * SVG group) so smaller arcs become easier to read. Moving away smoothly zooms back.
 *
 * Architecture: The D3 sunburst is rendered once into the SVG on mount (and on
 * resize). Mouse interaction uses a single mousemove listener with polar-coordinate
 * hit-testing to avoid SVG event-fighting between adjacent arcs.
 */

import React, { useRef, useEffect, useState, useMemo } from 'react';
import * as d3 from 'd3';
import type { MergeInfo, MutationInfo, BackgroundPoolMetrics } from '../../stores/mergeStore';
import type { MergeThroughputEstimate } from '@tracehouse/core';
import { pickThroughputEstimate } from '@tracehouse/core';
import { isMergeStuck } from './ActiveMergeList';
import { formatBytes, formatBytesPerSec } from '../../stores/mergeStore';
import { useClickHouseServices } from '../../providers/ClickHouseProvider';

// ── Types ──────────────────────────────────────────────────────────

type Health = 'green' | 'yellow' | 'red';

interface HealthNode {
  name: string;
  health: Health;
  metric: string;
  size?: number;
  children?: HealthNode[];
}

type PartitionedNode = d3.HierarchyRectangularNode<HealthNode>;

// ── Health derivation from real data ───────────────────────────────

/** Map of "database.table" → throughput estimates from part_log */
type ThroughputMap = Map<string, MergeThroughputEstimate[]>;

function mergeThroughputHealth(
  m: MergeInfo,
  estimates: ThroughputMap,
): { health: Health; metric: string } {
  const key = `${m.database}.${m.table}`;
  const tableEstimates = estimates.get(key);
  const est = tableEstimates
    ? pickThroughputEstimate(tableEstimates, m.merge_algorithm, m.total_size_bytes_compressed)
    : null;

  const liveRate = m.elapsed > 0 ? (m.total_size_bytes_compressed * m.progress) / m.elapsed : 0;
  const pct = (m.progress * 100).toFixed(0);

  if (!est || est.merge_count < 3) {
    // No historical data — fall back to simple elapsed check
    let health: Health = 'green';
    let reason = '';
    if (m.progress < 0.1 && m.elapsed > 60) { health = 'red'; reason = ' — <10% after 60s, no baseline data'; }
    else if (m.progress < 0.3 && m.elapsed > 30) { health = 'yellow'; reason = ' — slow start, no baseline data'; }
    return {
      health,
      metric: `${pct}% in ${m.elapsed.toFixed(0)}s — ${formatBytes(m.total_size_bytes_compressed)}${reason}`,
    };
  }

  const expected = est.median_bytes_per_sec;
  const ratio = expected > 0 ? liveRate / expected : 1;
  const rateStr = liveRate > 0 ? formatBytesPerSec(liveRate) : 'starting';
  const expectedStr = formatBytesPerSec(expected);
  const pctOfExpected = (ratio * 100).toFixed(0);

  let health: Health = 'green';
  let reason = '';
  if (ratio < 0.25 && m.elapsed > 30) {
    health = 'red';
    reason = ` — ${pctOfExpected}% of expected rate (${expectedStr})`;
  } else if (ratio < 0.5 && m.elapsed > 10) {
    health = 'yellow';
    reason = ` — ${pctOfExpected}% of expected rate (${expectedStr})`;
  }

  return {
    health,
    metric: `${pct}% in ${m.elapsed.toFixed(0)}s — ${rateStr}${reason}`,
  };
}

function deriveHealth(
  activeMerges: MergeInfo[],
  mutations: MutationInfo[],
  poolMetrics: BackgroundPoolMetrics | null,
  throughputEstimates: ThroughputMap,
): HealthNode {
  const mergesByTable = new Map<string, MergeInfo[]>();
  for (const m of activeMerges) {
    const key = `${m.database}.${m.table}`;
    const arr = mergesByTable.get(key) || [];
    arr.push(m);
    mergesByTable.set(key, arr);
  }

  // Part Count Pressure
  const partCountChildren: HealthNode[] = [];
  for (const [table, merges] of mergesByTable) {
    const totalParts = merges.reduce((s, m) => s + m.num_parts, 0);
    const stuckCount = merges.filter(isMergeStuck).length;
    const health: Health = stuckCount > 0 ? 'red' : totalParts > 20 ? 'yellow' : 'green';
    const tableReason = stuckCount > 0 ? ` — ${stuckCount} stuck merge(s)` : totalParts > 20 ? ' — high part count, merges may be falling behind' : '';
    partCountChildren.push({
      name: table, health,
      metric: `${merges.length} active merges, ${totalParts} source parts${tableReason}`,
      children: merges.map(m => {
        const stuck = isMergeStuck(m);
        const mergeHealth: Health = stuck ? 'red' : m.num_parts > 20 ? 'yellow' : 'green';
        const reason = stuck ? ' — merge appears stuck (no progress)' : m.num_parts > 20 ? ' — merging many parts' : '';
        return {
          name: m.result_part_name,
          health: mergeHealth,
          metric: `${(m.progress * 100).toFixed(0)}% — ${m.num_parts} parts → ${m.result_part_name}${reason}`,
          size: 1,
        };
      }),
    });
  }
  if (partCountChildren.length === 0) {
    partCountChildren.push({ name: 'no tables', health: 'green', metric: 'No active merges', size: 1 });
  }

  // Merge Throughput — compared against historical expectations from part_log
  const throughputChildren: HealthNode[] = [];
  for (const [table, merges] of mergesByTable) {
    const mergeHealths = merges.map(m => mergeThroughputHealth(m, throughputEstimates));
    const tableHealth = worstHealth(mergeHealths.map(h => ({ health: h.health, name: '', metric: '' })));
    throughputChildren.push({
      name: table, health: tableHealth,
      metric: `${merges.length} active merges`,
      children: merges.map((m, i) => ({
        name: m.result_part_name,
        health: mergeHealths[i].health,
        metric: mergeHealths[i].metric,
        size: 1,
      })),
    });
  }
  if (throughputChildren.length === 0) {
    throughputChildren.push({ name: 'idle', health: 'green', metric: 'No active merges', size: 1 });
  }

  // Mutations
  const mutationsByTable = new Map<string, MutationInfo[]>();
  for (const m of mutations) {
    const key = `${m.database}.${m.table}`;
    const arr = mutationsByTable.get(key) || [];
    arr.push(m);
    mutationsByTable.set(key, arr);
  }
  const mutChildren: HealthNode[] = [];
  for (const [table, muts] of mutationsByTable) {
    const failedCount = muts.filter(m => m.latest_fail_reason).length;
    const health: Health = failedCount > 0 ? 'red' : muts.length > 5 ? 'yellow' : 'green';
    const tableReason = failedCount > 0 ? ` — ${failedCount} failed` : muts.length > 5 ? ' — mutation queue building up' : '';
    mutChildren.push({
      name: table, health,
      metric: `${muts.length} pending mutations${tableReason}`,
      children: muts.map(m => ({
        name: m.mutation_id,
        health: m.latest_fail_reason ? 'red' : 'green',
        metric: m.latest_fail_reason
          ? `FAILED: ${m.latest_fail_reason.slice(0, 80)}`
          : m.command.slice(0, 60) + (m.command.length > 60 ? '...' : ''),
        size: 1,
      })),
    });
  }
  if (mutChildren.length === 0) {
    mutChildren.push({ name: 'none', health: 'green', metric: 'No pending mutations', size: 1 });
  }

  // Pool Saturation
  const poolChildren: HealthNode[] = [];
  if (poolMetrics) {
    for (const p of [
      { name: 'Merge/Mutation', active: poolMetrics.merge_pool_active, total: poolMetrics.merge_pool_size },
      { name: 'Move', active: poolMetrics.move_pool_active, total: poolMetrics.move_pool_size },
      { name: 'Fetch', active: poolMetrics.fetch_pool_active, total: poolMetrics.fetch_pool_size },
      { name: 'Schedule', active: poolMetrics.schedule_pool_active, total: poolMetrics.schedule_pool_size },
      { name: 'Common', active: poolMetrics.common_pool_active, total: poolMetrics.common_pool_size },
      { name: 'Distributed', active: poolMetrics.distributed_pool_active, total: poolMetrics.distributed_pool_size },
    ].filter(p => p.total > 0)) {
      const util = p.total > 0 ? p.active / p.total : 0;
      const poolHealth: Health = util > 0.8 ? 'red' : util > 0.5 ? 'yellow' : 'green';
      const reason = util > 0.8 ? ' — near saturation, new merges may queue'
        : util > 0.5 ? ' — over 50% utilized' : '';
      poolChildren.push({
        name: p.name,
        health: poolHealth,
        metric: `${p.active}/${p.total} threads (${(util * 100).toFixed(0)}%)${reason}`,
        size: 1,
      });
    }
  }
  if (poolChildren.length === 0) {
    poolChildren.push({ name: 'no data', health: 'green', metric: 'Pool metrics unavailable', size: 1 });
  }

  // Resources
  const totalBytes = activeMerges.reduce((s, m) => s + m.total_size_bytes_compressed, 0);
  const totalMemory = activeMerges.reduce((s, m) => s + m.memory_usage, 0);
  const bytesHealth: Health = totalBytes > 10e9 ? 'yellow' : 'green';
  const memHealth: Health = totalMemory > 4e9 ? 'red' : totalMemory > 1e9 ? 'yellow' : 'green';
  const diskChildren: HealthNode[] = [
    { name: 'Merge bytes', health: bytesHealth, metric: `${formatBytes(totalBytes)} being merged${totalBytes > 10e9 ? ' — over 10 GB in-flight' : ''}`, size: 1 },
    { name: 'Memory', health: memHealth, metric: `${formatBytes(totalMemory)} merge memory${totalMemory > 4e9 ? ' — over 4 GB, risk of OOM' : totalMemory > 1e9 ? ' — over 1 GB memory pressure' : ''}`, size: 1 },
  ];
  if (poolMetrics) {
    const cleanupHealth: Health = poolMetrics.outdated_parts > 500 ? 'red' : poolMetrics.outdated_parts > 100 ? 'yellow' : 'green';
    diskChildren.push({
      name: 'Pending cleanup',
      health: cleanupHealth,
      metric: `${poolMetrics.outdated_parts} outdated parts (${formatBytes(poolMetrics.outdated_parts_bytes)})${poolMetrics.outdated_parts > 500 ? ' — cleanup falling behind' : poolMetrics.outdated_parts > 100 ? ' — parts accumulating' : ''}`,
      size: 1,
    });
  }

  const categories: HealthNode[] = [
    { name: 'Part Count', health: worstHealth(partCountChildren), metric: `Active parts across ${mergesByTable.size} tables`, children: partCountChildren },
    { name: 'Throughput', health: worstHealth(throughputChildren), metric: 'Merge progress vs elapsed time', children: throughputChildren },
    { name: 'Mutations', health: worstHealth(mutChildren), metric: `${mutations.length} pending mutations`, children: mutChildren },
    { name: 'Pool Usage', health: worstHealth(poolChildren), metric: 'Background thread pool saturation', children: poolChildren },
    { name: 'Resources', health: worstHealth(diskChildren), metric: 'Memory and disk pressure', children: diskChildren },
  ];

  return { name: 'Merge Health', health: worstHealth(categories), metric: 'Overall merge subsystem health', children: categories };
}

function worstHealth(nodes: HealthNode[]): Health {
  if (nodes.some(n => n.health === 'red')) return 'red';
  if (nodes.some(n => n.health === 'yellow')) return 'yellow';
  return 'green';
}

// ── Color palette ──────────────────────────────────────────────────

const healthColorMap = {
  green:  { base: '#22c55e', dim: '#16532e' },
  yellow: { base: '#eab308', dim: '#5c4a0a' },
  red:    { base: '#ef4444', dim: '#5c1a1a' },
};

const CFG = {
  ringWidth: 42,
  padAngle: 0.02,
  cornerRadius: 4,
  centerRadius: 54,
  transitionMs: 300,
  opacityHidden: 0,
  opacityDim: 0.25,
  retractDelay: 500,
  panDuration: 900,
};

// ── Component ──────────────────────────────────────────────────────

export interface MergeHealthSunburstProps {
  activeMerges: MergeInfo[];
  mutations: MutationInfo[];
  poolMetrics: BackgroundPoolMetrics | null;
}

export const MergeHealthSunburst: React.FC<MergeHealthSunburstProps> = ({
  activeMerges,
  mutations,
  poolMetrics,
}) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const svgRef = useRef<SVGSVGElement>(null);
  const tooltipRef = useRef<HTMLDivElement>(null);
  const services = useClickHouseServices();

  // Fetch throughput estimates for all tables with active merges
  const [throughputEstimates, setThroughputEstimates] = useState<ThroughputMap>(new Map());
  const activeTables = useMemo(() => {
    const tables = new Set<string>();
    for (const m of activeMerges) tables.add(`${m.database}\t${m.table}`);
    return tables;
  }, [activeMerges]);

  useEffect(() => {
    if (!services || activeTables.size === 0) return;
    let cancelled = false;
    const map: ThroughputMap = new Map();
    const fetches = [...activeTables].map(key => {
      const [db, tbl] = key.split('\t');
      return services.mergeTracker.getMergeThroughputEstimate(db, tbl)
        .then(estimates => { if (!cancelled) map.set(`${db}.${tbl}`, estimates); })
        .catch(() => {}); // silently skip — deriveHealth falls back to elapsed-based thresholds
    });
    Promise.all(fetches).then(() => { if (!cancelled) setThroughputEstimates(map); });
    return () => { cancelled = true; };
  }, [services, activeTables]);

  const dataRef = useRef({ activeMerges, mutations, poolMetrics, throughputEstimates });
  dataRef.current = { activeMerges, mutations, poolMetrics, throughputEstimates };

  useEffect(() => {
    const container = containerRef.current;
    const svgNode = svgRef.current;
    if (!container || !svgNode) return;

    let hoverTimeout: ReturnType<typeof setTimeout> | null = null;
    let destroyed = false;

    function build() {
      if (destroyed) return;

      const { activeMerges: am, mutations: mu, poolMetrics: pm, throughputEstimates: te } = dataRef.current;
      const tree = deriveHealth(am, mu, pm, te);

      const width = container.clientWidth;
      const height = container.clientHeight;
      if (width === 0 || height === 0) return;

      const { ringWidth: rw, padAngle, cornerRadius, centerRadius,
              opacityHidden, opacityDim } = CFG;
      const maxRings = 4;
      const radius = Math.min(width, height) / 2 - 20;
      const ringWidth = Math.min(rw, (radius - centerRadius) / maxRings);
      const cx = width / 2;
      const cy = height / 2;

      const svg = d3.select(svgNode);
      svg.selectAll('*').remove();
      svg.attr('width', width).attr('height', height)
        .style('overflow', 'hidden');

      const g = svg.append('g')
        .attr('transform', `translate(${cx},${cy}) scale(1)`);

      // ── D3 hierarchy ──────────────────────────────────────────
      const root = d3.hierarchy<HealthNode>(tree)
        .sum(d => d.size || (d.children ? 0 : 30))
        .sort((a, b) => (b.value || 0) - (a.value || 0));

      if (root.children) {
        const equalVal = 1;
        for (const cat of root.children) {
          cat.value = equalVal;
          const catSum = cat.leaves().reduce((s, l) => s + (l.value || 1), 0);
          cat.eachAfter(n => {
            if (n === cat) return;
            if (!n.children || n.children.length === 0) {
              n.value = (n.value || 1) / catSum * equalVal;
            } else {
              n.value = n.children.reduce((s, c) => s + (c.value || 0), 0);
            }
          });
        }
        root.value = root.children.reduce((s, c) => s + (c.value || 0), 0);
      }

      d3.partition<HealthNode>().size([2 * Math.PI, maxRings])(root);

      const arc = d3.arc<PartitionedNode>()
        .startAngle(d => d.x0)
        .endAngle(d => d.x1)
        .padAngle(d => Math.min(padAngle, (d.x1 - d.x0) / 2))
        .padRadius(centerRadius * 1.5)
        .innerRadius(d => centerRadius + d.y0 * ringWidth)
        .outerRadius(d => centerRadius + d.y0 * ringWidth + ringWidth - 2)
        .cornerRadius(cornerRadius);

      // ── State ──────────────────────────────────────────────────
      const revealed = new Set<PartitionedNode>();
      revealed.add(root as unknown as PartitionedNode);
      root.children?.forEach(c => revealed.add(c as unknown as PartitionedNode));

      // ── Center circle ──────────────────────────────────────────
      g.append('circle')
        .attr('r', centerRadius - 2)
        .attr('fill', healthColorMap[tree.health].dim)
        .attr('stroke', healthColorMap[tree.health].base)
        .attr('stroke-width', 2);
      g.append('text').attr('text-anchor', 'middle').attr('dy', '-0.2em')
        .attr('fill', healthColorMap[tree.health].base)
        .attr('font-size', '11px').attr('font-weight', '700').attr('font-family', 'system-ui')
        .text('MERGE');
      g.append('text').attr('text-anchor', 'middle').attr('dy', '1em')
        .attr('fill', healthColorMap[tree.health].base)
        .attr('font-size', '11px').attr('font-weight', '700').attr('font-family', 'system-ui')
        .text('HEALTH');

      // ── Arcs ──────────────────────────────────────────────────
      const allNodes = root.descendants().filter(d => d.depth > 0) as PartitionedNode[];
      const paths = g.selectAll<SVGPathElement, PartitionedNode>('path.arc')
        .data(allNodes).join('path')
        .attr('class', 'arc')
        .attr('d', arc as unknown as (d: PartitionedNode) => string)
        .attr('fill', d => healthColorMap[d.data.health].base)
        .attr('opacity', d => revealed.has(d) ? 1 : opacityHidden)
        .attr('stroke', 'none')
        .style('pointer-events', 'none')
        .style('transition', `opacity ${CFG.transitionMs}ms ease`);

      // ── Category labels ──────────────────────────────────────
      const depth1 = (root.children || []) as PartitionedNode[];
      g.selectAll<SVGTextElement, PartitionedNode>('text.cat-label')
        .data(depth1.filter(d => (d.x1 - d.x0) > 0.4)).join('text')
        .attr('class', 'cat-label').attr('dy', '0.35em')
        .attr('font-size', '11px').attr('font-family', 'system-ui').attr('font-weight', '600')
        .attr('fill', d => d.data.health === 'yellow' ? '#1a1a2e' : '#fff')
        .attr('text-anchor', 'middle').attr('pointer-events', 'none')
        .attr('transform', d => {
          const angle = (d.x0 + d.x1) / 2;
          const r = centerRadius + d.y0 * ringWidth + ringWidth / 2;
          let rot = (angle * 180) / Math.PI;
          if (rot > 90 && rot < 270) rot += 180;
          return `translate(${Math.sin(angle) * r},${-Math.cos(angle) * r}) rotate(${rot})`;
        })
        .text(d => d.data.name);

      // ── Hit testing via polar coordinates ──────────────────────
      const angleTol = 0.02;

      function hitTest(mx: number, my: number): PartitionedNode | null {
        const dist = Math.sqrt(mx * mx + my * my);
        let angle = Math.atan2(mx, -my);
        if (angle < 0) angle += 2 * Math.PI;

        let best: PartitionedNode | null = null;
        for (const node of allNodes) {
          if (!revealed.has(node)) continue;
          const innerR = centerRadius + node.y0 * ringWidth;
          const outerR = innerR + ringWidth - 2;
          if (dist < innerR || dist > outerR) continue;
          if (angle >= node.x0 - angleTol && angle <= node.x1 + angleTol) {
            if (!best || node.depth > best.depth) best = node;
          }
        }
        return best;
      }

      // ── Tooltip ────────────────────────────────────────────────
      const tooltip = d3.select(tooltipRef.current);

      function showTooltip(ev: MouseEvent, d: PartitionedNode) {
        const c = healthColorMap[d.data.health];
        // Build breadcrumb from ancestors (skip root)
        const crumbs = d.ancestors().reverse().slice(1, -1); // skip root and self
        const breadcrumb = crumbs.length > 0
          ? `<div style="color:var(--text-muted,#555570);font-size:10px;margin-bottom:4px">${crumbs.map(a => a.data.name).join(' › ')}</div>`
          : '';
        tooltip.style('opacity', '1').html(
          breadcrumb +
          `<div style="display:flex;align-items:center;gap:6px;margin-bottom:4px">` +
          `<span style="width:8px;height:8px;border-radius:50%;background:${c.base};flex-shrink:0"></span>` +
          `<span style="font-weight:600;color:var(--text-primary,#e0e0f0)">${d.data.name}</span></div>` +
          `<div style="color:var(--text-muted,#8888aa);font-size:11px">${d.data.metric}</div>` +
          (d.children ? '<div style="color:var(--text-muted,#555570);font-size:10px;margin-top:4px">Hover to expand</div>' : ''),
        );
        moveTooltip(ev);
      }

      function moveTooltip(ev: MouseEvent) {
        const el = tooltipRef.current;
        if (!el) return;
        const pad = 14;
        let x = ev.clientX + pad, y = ev.clientY + pad;
        const r = el.getBoundingClientRect();
        if (x + r.width > window.innerWidth) x = ev.clientX - r.width - pad;
        if (y + r.height > window.innerHeight) y = ev.clientY - r.height - pad;
        tooltip.style('left', x + 'px').style('top', y + 'px');
      }

      function hideTooltip() { tooltip.style('opacity', '0'); }

      // ── Edge-pan: only move when revealed arcs overflow container ──
      let currentFocus: PartitionedNode | null = null;
      let currentPanX = 0;
      let currentPanY = 0;

      function applyPan(panX: number, panY: number) {
        if (panX === currentPanX && panY === currentPanY) return;
        currentPanX = panX;
        currentPanY = panY;
        g.transition().duration(CFG.panDuration).ease(d3.easeCubicOut)
          .attr('transform', `translate(${cx + panX},${cy + panY})`);
      }

      function resetPan() {
        applyPan(0, 0);
      }

      function panIfNeeded(focus: PartitionedNode) {
        // Compute bounding box of all revealed arcs in the focus subtree
        const nodesToCheck = focus.descendants().filter(d => revealed.has(d as unknown as PartitionedNode));
        if (nodesToCheck.length === 0) { resetPan(); return; }

        let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;
        for (const node of nodesToCheck) {
          const outerR = centerRadius + node.y0 * ringWidth + ringWidth;
          // Sample arc endpoints and midpoint at outer radius
          for (const angle of [node.x0, (node.x0 + node.x1) / 2, node.x1]) {
            const x = Math.sin(angle) * outerR;
            const y = -Math.cos(angle) * outerR;
            minX = Math.min(minX, x); maxX = Math.max(maxX, x);
            minY = Math.min(minY, y); maxY = Math.max(maxY, y);
          }
        }

        // Convert to screen coords (relative to container).
        // Only pan when overflow exceeds the dead zone — small clipping is
        // better than bouncing the inner rings around.
        const margin = 10;
        const deadZone = 30; // ignore overflow smaller than this
        let panX = 0, panY = 0;
        const overR = cx + maxX - (width - margin);
        const overL = margin - (cx + minX);
        const overB = cy + maxY - (height - margin);
        const overT = margin - (cy + minY);
        if (overR > deadZone) panX = -overR;
        if (overL > deadZone) panX = overL;
        if (overB > deadZone) panY = -overB;
        if (overT > deadZone) panY = overT;

        if (panX === 0 && panY === 0) { resetPan(); return; }
        applyPan(panX, panY);
      }

      function setFocus(focus: PartitionedNode | null) {
        currentFocus = focus;
        const focusAncestors = focus ? new Set(focus.ancestors()) : null;
        const focusDescendants = focus ? new Set(focus.descendants()) : null;

        paths.each(function(p: PartitionedNode) {
          const el = d3.select(this);
          let op: number;
          if (!revealed.has(p)) {
            op = opacityHidden;
          } else if (!focus) {
            op = p.depth <= 1 ? 1 : opacityDim;
          } else if (p === focus || focusAncestors!.has(p) || focusDescendants!.has(p)) {
            op = 1;
          } else {
            op = opacityDim;
          }
          el.attr('opacity', op);
        });
      }

      // ── Mouse handlers ─────────────────────────────────────────

      function onMove(ev: MouseEvent) {
        if (hoverTimeout) { clearTimeout(hoverTimeout); hoverTimeout = null; }

        const rect = svgNode.getBoundingClientRect();
        // Invert the current pan to get sunburst-local coords
        const mx = ev.clientX - rect.left - cx - currentPanX;
        const my = ev.clientY - rect.top - cy - currentPanY;

        const hit = hitTest(mx, my);

        if (hit === currentFocus) {
          if (hit) moveTooltip(ev);
          return;
        }

        if (hit) {
          if (hit.children) hit.children.forEach(c => revealed.add(c as unknown as PartitionedNode));
          setFocus(hit);
          showTooltip(ev, hit);
          panIfNeeded(hit);
        } else {
          hideTooltip();
          hoverTimeout = setTimeout(() => {
            setFocus(null);
            resetPan();
          }, CFG.retractDelay);
        }
      }

      function onLeave() {
        hideTooltip();
        if (hoverTimeout) clearTimeout(hoverTimeout);
        hoverTimeout = setTimeout(() => {
          setFocus(null);
          resetPan();
        }, CFG.retractDelay);
      }

      svgNode.addEventListener('mousemove', onMove);
      svgNode.addEventListener('mouseleave', onLeave);

      return () => {
        svgNode.removeEventListener('mousemove', onMove);
        svgNode.removeEventListener('mouseleave', onLeave);
        if (hoverTimeout) clearTimeout(hoverTimeout);
      };
    }

    let cleanupBuild = build();

    let lastW = container.clientWidth;
    let lastH = container.clientHeight;
    const ro = new ResizeObserver(() => {
      const w = container.clientWidth;
      const h = container.clientHeight;
      if (w === lastW && h === lastH) return;
      lastW = w;
      lastH = h;
      cleanupBuild?.();
      cleanupBuild = build();
    });
    ro.observe(container);

    return () => {
      destroyed = true;
      cleanupBuild?.();
      ro.disconnect();
    };
  }, []);

  return (
    <div
      ref={containerRef}
      style={{
        width: '100%', height: '100%', minHeight: 300,
        position: 'relative', display: 'flex',
        alignItems: 'center', justifyContent: 'center',
        overflow: 'hidden',
      }}
    >
      <svg ref={svgRef} style={{ overflow: 'visible' }} />

      <div
        ref={tooltipRef}
        style={{
          position: 'fixed', pointerEvents: 'none',
          background: 'var(--bg-secondary, #1a1a30)',
          border: '1px solid var(--border-primary, #2a2a50)',
          borderRadius: 8, padding: '10px 14px',
          fontSize: 12, lineHeight: 1.5,
          boxShadow: '0 8px 32px rgba(0,0,0,0.5)',
          zIndex: 100, opacity: 0, transition: 'opacity 0.15s',
          maxWidth: 280,
        }}
      />

      <div style={{
        position: 'absolute', bottom: 8, left: 0, right: 0,
        display: 'flex', justifyContent: 'center', gap: 16,
        fontSize: 10, color: 'var(--text-muted, #8888aa)',
      }}>
        {(['green', 'yellow', 'red'] as const).map(h => (
          <span key={h} style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
            <span style={{ width: 6, height: 6, borderRadius: '50%', background: healthColorMap[h].base }} />
            {h === 'green' ? 'Healthy' : h === 'yellow' ? 'Warning' : 'Critical'}
          </span>
        ))}
      </div>

      <div style={{
        position: 'absolute', top: 8, left: 0, right: 0,
        textAlign: 'center', fontSize: 10,
        color: 'var(--text-muted, #555570)', pointerEvents: 'none',
      }}>
        Experimental: visual overview of merge health — hover to explore
      </div>
    </div>
  );
};
