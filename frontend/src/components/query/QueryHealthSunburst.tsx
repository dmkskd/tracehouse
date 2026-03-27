/**
 * QueryHealthSunburst — hover-to-expand radial health visualization for queries
 *
 * Mirrors MergeHealthSunburst architecture: derives a health tree from live data
 * (running queries, recent history, concurrency) and renders an interactive D3
 * sunburst with polar-coordinate hit-testing and hover-to-reveal.
 */

import React, { useRef, useEffect } from 'react';
import * as d3 from 'd3';
import { deriveQueryHealth } from '@tracehouse/core';
import type { HealthNode, QueryMetrics, QueryHistoryItem, QueryConcurrency } from '@tracehouse/core';

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

// Query IDs are UUIDs — detect them to enable click-to-open and truncate for display
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-/i;
const isQueryId = (name: string) => UUID_RE.test(name);
const truncateId = (name: string) => isQueryId(name) ? name.slice(0, 12) : name;

export interface QueryHealthSunburstProps {
  runningQueries: QueryMetrics[];
  recentHistory: QueryHistoryItem[];
  concurrency: QueryConcurrency | null;
  /** Return 'not-found' to show a transient message on the sunburst */
  onQueryClick?: (queryId: string) => void | 'not-found';
}

export const QueryHealthSunburst: React.FC<QueryHealthSunburstProps> = ({
  runningQueries,
  recentHistory,
  concurrency,
  onQueryClick,
}) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const svgRef = useRef<SVGSVGElement>(null);
  const tooltipRef = useRef<HTMLDivElement>(null);

  const dataRef = useRef({ runningQueries, recentHistory, concurrency });
  dataRef.current = { runningQueries, recentHistory, concurrency };
  const onQueryClickRef = useRef(onQueryClick);
  onQueryClickRef.current = onQueryClick;
  const focusRef = useRef<{ name: string; depth: number } | null>(null);

  useEffect(() => {
    const container = containerRef.current;
    const svgNode = svgRef.current;
    if (!container || !svgNode) return;

    let hoverTimeout: ReturnType<typeof setTimeout> | null = null;
    let destroyed = false;

    function build() {
      if (destroyed) return;

      const { runningQueries: rq, recentHistory: rh, concurrency: cc } = dataRef.current;
      const tree = deriveQueryHealth(rq, rh, cc);

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
        .text('QUERY');
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
        const crumbs = d.ancestors().reverse().slice(1, -1);
        const breadcrumb = crumbs.length > 0
          ? `<div style="color:var(--text-muted,#555570);font-size:10px;margin-bottom:4px">${crumbs.map(a => truncateId(a.data.name)).join(' › ')}</div>`
          : '';
        const displayName = truncateId(d.data.name);
        const hint = d.depth >= 3
          ? '<div style="color:#58a6ff;font-size:10px;margin-top:4px;font-weight:600">Click to open query details</div>'
          : d.depth < 3 && d.data.name !== 'Query Health'
          ? '<div style="color:var(--text-muted,#555570);font-size:10px;margin-top:4px">Hover to expand</div>'
          : '';
        tooltip.style('opacity', '1').html(
          breadcrumb +
          `<div style="display:flex;align-items:center;gap:6px;margin-bottom:4px">` +
          `<span style="width:8px;height:8px;border-radius:50%;background:${c.base};flex-shrink:0"></span>` +
          `<span style="font-weight:600;color:var(--text-primary,#e0e0f0)">${displayName}</span></div>` +
          `<div style="color:var(--text-muted,#8888aa);font-size:11px">${d.data.metric}</div>` +
          hint,
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

      // ── Edge-pan ──────────────────────────────────────────────
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

      function resetPan() { applyPan(0, 0); }

      function panIfNeeded(focus: PartitionedNode) {
        const nodesToCheck = focus.descendants().filter(d => revealed.has(d as unknown as PartitionedNode));
        if (nodesToCheck.length === 0) { resetPan(); return; }

        let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;
        for (const node of nodesToCheck) {
          const outerR = centerRadius + node.y0 * ringWidth + ringWidth;
          for (const angle of [node.x0, (node.x0 + node.x1) / 2, node.x1]) {
            const x = Math.sin(angle) * outerR;
            const y = -Math.cos(angle) * outerR;
            minX = Math.min(minX, x); maxX = Math.max(maxX, x);
            minY = Math.min(minY, y); maxY = Math.max(maxY, y);
          }
        }

        const margin = 10;
        const deadZone = 30;
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
        focusRef.current = focus ? { name: focus.data.name, depth: focus.depth } : null;
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
        const mx = ev.clientX - rect.left - cx - currentPanX;
        const my = ev.clientY - rect.top - cy - currentPanY;

        const hit = hitTest(mx, my);

        if (hit === currentFocus) {
          if (hit) {
            moveTooltip(ev);
            svgNode!.style.cursor = hit.depth >= 3 ? 'pointer' : '';
          }
          return;
        }

        if (hit) {
          if (hit.children) hit.children.forEach(c => revealed.add(c as unknown as PartitionedNode));
          setFocus(hit);
          showTooltip(ev, hit);
          // Don't pan when hovering clickable leaf nodes — keeps the arc stable for clicking
          if (hit.depth < 3) panIfNeeded(hit);
          svgNode!.style.cursor = hit.depth >= 3 ? 'pointer' : '';
        } else {
          svgNode!.style.cursor = '';
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
        if (f && f.depth >= 3 && onQueryClick) {
          const result = onQueryClick(f.name);
          if (result === 'not-found' && tooltipRef.current) {
            const el = tooltipRef.current;
            el.innerHTML =
              '<div style="color:var(--text-muted,#8888aa);font-size:11px">' +
              'Query no longer available — it may have finished.' +
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
        Experimental: visual overview of query health — hover to explore
      </div>
    </div>
  );
};
