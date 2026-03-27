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

import React, { useRef, useEffect } from 'react';
import * as d3 from 'd3';
import type { MergeInfo, MutationInfo, BackgroundPoolMetrics } from '../../stores/mergeStore';
import { deriveHealth } from '@tracehouse/core';
import type { HealthNode, ThroughputMap } from '@tracehouse/core';

type PartitionedNode = d3.HierarchyRectangularNode<HealthNode>;

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
  throughputEstimates: ThroughputMap;
  /** Called when a leaf node is clicked. `category` is the top-level health category (e.g. "Part Count", "Mutations"). Return 'not-found' to show a transient message. */
  onLeafClick?: (name: string, category: string) => void | 'not-found';
}

export const MergeHealthSunburst: React.FC<MergeHealthSunburstProps> = ({
  activeMerges,
  mutations,
  poolMetrics,
  throughputEstimates,
  onLeafClick,
}) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const svgRef = useRef<SVGSVGElement>(null);
  const tooltipRef = useRef<HTMLDivElement>(null);
  const focusRef = useRef<{ name: string; depth: number; category: string } | null>(null);
  const onLeafClickRef = useRef(onLeafClick);
  onLeafClickRef.current = onLeafClick;

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

      const width = container!.clientWidth;
      const height = container!.clientHeight;
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
          (cat as { value: number }).value = equalVal;
          const catSum = cat.leaves().reduce((s, l) => s + (l.value || 1), 0);
          cat.eachAfter(n => {
            if (n === cat) return;
            if (!n.children || n.children.length === 0) {
              (n as { value: number }).value = (n.value || 1) / catSum * equalVal;
            } else {
              (n as { value: number }).value = n.children.reduce((s, c) => s + (c.value || 0), 0);
            }
          });
        }
        (root as { value: number }).value = root.children.reduce((s, c) => s + (c.value || 0), 0);
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
          (d.children ? '<div style="color:var(--text-muted,#555570);font-size:10px;margin-top:4px">Hover to expand</div>'
            : d.depth >= 3 ? '<div style="color:var(--text-muted,#555570);font-size:10px;margin-top:4px">Click to open details</div>'
            : ''),
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

        const rect = svgNode!.getBoundingClientRect();
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
          // Track focused leaf for click handling
          const isLeaf = !hit.children || hit.children.length === 0;
          const category = hit.ancestors().find(a => a.depth === 1)?.data.name || '';
          focusRef.current = isLeaf ? { name: hit.data.name, depth: hit.depth, category } : null;
          svgNode!.style.cursor = isLeaf && hit.depth >= 3 ? 'pointer' : '';
          // Don't pan when hovering clickable leaf nodes — keeps the arc stable for clicking
          if (!isLeaf || hit.depth < 3) panIfNeeded(hit);
        } else {
          focusRef.current = null;
          svgNode!.style.cursor = '';
          hideTooltip();
          hoverTimeout = setTimeout(() => {
            setFocus(null);
            resetPan();
          }, CFG.retractDelay);
        }
      }

      function onLeave() {
        focusRef.current = null;
        svgNode!.style.cursor = '';
        hideTooltip();
        if (hoverTimeout) clearTimeout(hoverTimeout);
        hoverTimeout = setTimeout(() => {
          setFocus(null);
          resetPan();
        }, CFG.retractDelay);
      }

      svgNode!.addEventListener('mousemove', onMove);
      svgNode!.addEventListener('mouseleave', onLeave);

      return () => {
        svgNode!.removeEventListener('mousemove', onMove);
        svgNode!.removeEventListener('mouseleave', onLeave);
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
      onClick={() => {
        const f = focusRef.current;
        if (f && f.depth >= 3 && onLeafClickRef.current) {
          const result = onLeafClickRef.current(f.name, f.category);
          if (result === 'not-found' && tooltipRef.current) {
            const el = tooltipRef.current;
            el.innerHTML =
              '<div style="color:var(--text-muted,#8888aa);font-size:11px">' +
              'Merge or mutation no longer active — it may have completed.' +
              '</div>';
            el.style.opacity = '1';
            setTimeout(() => { el.style.opacity = '0'; }, 2500);
          }
        }
      }}
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
