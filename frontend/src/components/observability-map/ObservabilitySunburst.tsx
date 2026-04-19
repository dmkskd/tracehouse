/**
 * ObservabilitySunburst — interactive D3 sunburst of ClickHouse system tables
 *
 * Ported from K8s Compass SunburstView with ClickHouse-specific adaptations.
 * Features: drag-to-spin, scroll-to-zoom, click-to-focus, search highlighting.
 */

import React, { useRef, useEffect, useState, useMemo } from 'react';
import * as d3 from 'd3';
import { OBSERVABILITY_DATA, buildHierarchy } from './data';
import type { SunburstNodeData, ObservabilityData } from './data';
// eslint-disable-next-line @typescript-eslint/no-require-imports
require('./ObservabilitySunburst.css');

// ─── Types ───────────────────────────────────────────────────

type PartitionedNode = d3.HierarchyRectangularNode<SunburstNodeData>;

export interface ObservabilitySunburstProps {
  searchQuery: string;
  selectedNode: SunburstNodeData | null;
  onHoverNode: (node: SunburstNodeData | null) => void;
  onSelectNode: (node: SunburstNodeData | null) => void;
  enrichedData?: ObservabilityData;
}

// ─── Color helpers ───────────────────────────────────────────

const defaultCategoryColors: Record<string, string> = {};
OBSERVABILITY_DATA.children.forEach(c => { defaultCategoryColors[c.name] = c.color; });

function getColor(d: PartitionedNode): string {
  if (d.depth === 0) return 'var(--bg-secondary, #1e293b)';
  // Walk up to category (depth 1)
  let node: PartitionedNode = d;
  while (node.depth > 1 && node.parent) node = node.parent;
  const base = d3.color(defaultCategoryColors[node.data.name] || '#64748b');
  if (!base) return '#64748b';
  // Subtle brightness variation — depth conveyed via opacity, not heavy color shifts
  if (d.depth === 1) return base.toString();
  if (d.depth === 2) return base.brighter(0.25).toString();
  return base.brighter(0.5).toString();
}

// ─── Component ───────────────────────────────────────────────

export const ObservabilitySunburst: React.FC<ObservabilitySunburstProps> = ({
  searchQuery,
  selectedNode: selectedNodeProp,
  onHoverNode,
  onSelectNode,
  enrichedData,
}) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const svgRef = useRef<SVGSVGElement>(null);
  const zoomBehaviorRef = useRef<d3.ZoomBehavior<SVGSVGElement, unknown> | null>(null);
  const rotationRef = useRef<{ angle: number; velocity: number; animationId: number | null }>({
    angle: 0, velocity: 0, animationId: null,
  });
  const dragRef = useRef<{
    startAngle: number; startRotation: number;
    lastAngle: number; lastTime: number;
  } | null>(null);

  const [tooltip, setTooltip] = useState<{
    x: number; y: number;
    content: { name: string; type: string; desc?: string };
  } | null>(null);
  const [focusedNode, setFocusedNode] = useState<PartitionedNode | null>(null);
  const [isDragging, setIsDragging] = useState(false);

  // Hierarchy data — use enriched data when available
  const sourceData = enrichedData || OBSERVABILITY_DATA;
  const hierarchyData = useMemo(() => buildHierarchy(sourceData), [sourceData]);

  // Search matching — searches everything: names, descriptions, columns, query labels, SQL
  const matchingNodes = useMemo(() => {
    if (!searchQuery || !searchQuery.trim()) return null;
    const q = searchQuery.toLowerCase().trim();
    const matches = new Set<string>();
    sourceData.children.forEach(cat => {
      cat.children.forEach(table => {
        const hit =
          table.name.toLowerCase().includes(q) ||
          cat.name.toLowerCase().includes(q) ||
          table.desc.toLowerCase().includes(q) ||
          table.cols.some(c => c.toLowerCase().includes(q)) ||
          table.children.some(col =>
            col.name.toLowerCase().includes(q) || col.desc.toLowerCase().includes(q)
          ) ||
          table.queries.some(qr =>
            qr.label.toLowerCase().includes(q) || qr.sql.toLowerCase().includes(q)
          ) ||
          (table.since && table.since.includes(q)) ||
          (q === 'cloud' && table.cloudOnly) ||
          (q === 'unavailable' && table.available === false) ||
          (q === 'available' && table.available === true);
        if (hit) matches.add(table.name);
      });
    });
    return matches;
  }, [searchQuery]);

  // ─── Main D3 effect ──────────────────────────────────────

  useEffect(() => {
    if (!containerRef.current || !svgRef.current) return;

    const container = containerRef.current;
    const width = container.clientWidth;
    const height = container.clientHeight;
    const radius = Math.min(width, height) / 2 - 40;

    // Clear
    const svg = d3.select(svgRef.current);
    svg.selectAll('*').remove();
    svg.attr('width', width).attr('height', height);

    // Three-layer structure: position → zoom → rotation
    const outerG = svg.append('g')
      .attr('transform', `translate(${width / 2}, ${height / 2})`);
    const zoomG = outerG.append('g').attr('class', 'zoom-container');
    const g = zoomG.append('g')
      .style('transform-origin', '0 0');

    // Zoom (scroll only)
    const zoom = d3.zoom<SVGSVGElement, unknown>()
      .scaleExtent([1, 5])
      .filter(event => event.type === 'wheel')
      .on('zoom', event => {
        zoomG.attr('transform', `scale(${event.transform.k})`);
      });
    svg.call(zoom);
    zoomBehaviorRef.current = zoom;

    // Drag-to-spin with momentum
    let currentRotation = rotationRef.current.angle;

    const getCurrentCSSRotation = (): number => {
      const node = g.node() as SVGGElement;
      if (!node) return 0;
      const transform = window.getComputedStyle(node).transform;
      if (transform === 'none') return rotationRef.current.angle;
      const values = transform.match(/matrix\(([^)]+)\)/);
      if (values) {
        const parts = values[1].split(', ');
        return Math.atan2(parseFloat(parts[1]), parseFloat(parts[0])) * (180 / Math.PI);
      }
      return rotationRef.current.angle;
    };

    const startSlowRotation = () => {
      const speed = 0.006;
      const animate = () => {
        const rot = rotationRef.current;
        rot.angle -= speed;
        g.style('transform', `rotate(${rot.angle}deg)`);
        rot.animationId = requestAnimationFrame(animate);
      };
      rotationRef.current.animationId = requestAnimationFrame(animate);
    };

    const drag = d3.drag<SVGSVGElement, unknown>()
      .on('start', event => {
        if (rotationRef.current.animationId) {
          cancelAnimationFrame(rotationRef.current.animationId);
          rotationRef.current.animationId = null;
        }
        currentRotation = getCurrentCSSRotation();
        rotationRef.current.angle = currentRotation;
        g.style('animation', 'none');
        g.style('transform', `rotate(${currentRotation}deg)`);

        const cx = width / 2, cy = height / 2;
        const startAngle = Math.atan2(event.y - cy, event.x - cx) * (180 / Math.PI);
        dragRef.current = { startAngle, startRotation: currentRotation, lastAngle: startAngle, lastTime: performance.now() };
        setIsDragging(true);
      })
      .on('drag', event => {
        if (!dragRef.current) return;
        const cx = width / 2, cy = height / 2;
        const curAngle = Math.atan2(event.y - cy, event.x - cx) * (180 / Math.PI);
        const curTime = performance.now();

        let delta = curAngle - dragRef.current.startAngle;
        if (delta > 180) delta -= 360;
        if (delta < -180) delta += 360;
        currentRotation = dragRef.current.startRotation + delta;
        rotationRef.current.angle = currentRotation;
        g.style('transform', `rotate(${currentRotation}deg)`);

        const dt = curTime - dragRef.current.lastTime;
        if (dt > 0) {
          let ad = curAngle - dragRef.current.lastAngle;
          if (ad > 180) ad -= 360;
          if (ad < -180) ad += 360;
          rotationRef.current.velocity = (ad / dt) * 16;
        }
        dragRef.current.lastAngle = curAngle;
        dragRef.current.lastTime = curTime;
      })
      .on('end', () => {
        dragRef.current = null;
        setIsDragging(false);
        if (Math.abs(rotationRef.current.velocity) > 0.5) {
          const friction = 0.92;
          const animate = () => {
            const rot = rotationRef.current;
            if (Math.abs(rot.velocity) < 0.1) { startSlowRotation(); return; }
            rot.angle += rot.velocity;
            rot.velocity *= friction;
            currentRotation = rot.angle;
            g.style('transform', `rotate(${rot.angle}deg)`);
            rot.animationId = requestAnimationFrame(animate);
          };
          rotationRef.current.animationId = requestAnimationFrame(animate);
        } else {
          startSlowRotation();
        }
      });

    svg.call(drag as unknown as d3.DragBehavior<SVGSVGElement, unknown, unknown>);

    // Start gentle idle rotation
    startSlowRotation();

    // Build D3 hierarchy
    const root = d3.hierarchy<SunburstNodeData>(hierarchyData)
      .sum(d => d.value || (!d.children ? 1 : 0))
      .sort((a, b) => (b.value || 0) - (a.value || 0));

    const partition = d3.partition<SunburstNodeData>().size([2 * Math.PI, radius]);
    const partitionedRoot = partition(root) as PartitionedNode;

    // Determine focus node
    let focus: PartitionedNode = partitionedRoot;
    if (focusedNode) {
      const found = partitionedRoot.descendants().find(d =>
        d.data.name === focusedNode.data.name &&
        d.data.meta?.type === focusedNode.data.meta?.type
      );
      if (found) focus = found;
    }

    // Arc generator relative to focus
    const arc = d3.arc<PartitionedNode>()
      .startAngle(d => {
        const range = focus.x1 - focus.x0;
        return ((d.x0 - focus.x0) / range) * 2 * Math.PI;
      })
      .endAngle(d => {
        const range = focus.x1 - focus.x0;
        return ((d.x1 - focus.x0) / range) * 2 * Math.PI;
      })
      .padAngle(0.002)
      .padRadius(radius / 2)
      .innerRadius(d => {
        const y0f = focus.y0;
        const yr = radius - y0f;
        return Math.max(0, ((d.y0 - y0f) / yr) * radius);
      })
      .outerRadius(d => {
        const y0f = focus.y0;
        const yr = radius - y0f;
        return Math.max(0, ((d.y1 - y0f) / yr) * radius - 1);
      });

    // Visible nodes
    const visibleNodes = partitionedRoot.descendants().filter(d => {
      if (d === partitionedRoot && focus === partitionedRoot) return false;
      if (focus === partitionedRoot) return d !== partitionedRoot;
      return d.ancestors().includes(focus) && d !== focus;
    });

    // Compute opacity — softer depth layering for premium feel
    const getOpacity = (d: PartitionedNode): number => {
      const depthFromFocus = d.depth - focus.depth;
      let base = depthFromFocus === 1 ? 0.88 : depthFromFocus === 2 ? 0.7 : 0.82;
      // Dim unavailable tables (and their children)
      const isUnavailable =
        (d.data.meta?.type === 'table' && d.data.meta.available === false) ||
        (d.data.meta?.type === 'column' && d.parent?.data.meta?.available === false);
      if (isUnavailable) return 0.2;
      if (matchingNodes !== null) {
        const tableName = d.data.meta?.type === 'table'
          ? d.data.name
          : d.data.meta?.type === 'column'
            ? (d.parent?.data.name || '')
            : '';
        if (d.data.meta?.type === 'table') {
          return matchingNodes.has(d.data.name) ? 1 : 0.15;
        }
        if (d.data.meta?.type === 'column') {
          return matchingNodes.has(tableName) ? 1 : 0.15;
        }
        // Category: check if any child table matches
        const hasMatch = d.leaves().some(leaf => {
          const tn = leaf.parent?.data.name || leaf.data.name;
          return matchingNodes.has(tn);
        });
        return hasMatch ? base : 0.15;
      }
      return base;
    };

    // Check if a node (or its ancestor) matches the currently selected node
    const isSelected = (d: PartitionedNode): boolean => {
      if (!selectedNodeProp) return false;
      return d.data.name === selectedNodeProp.name && d.data.meta?.type === selectedNodeProp.meta?.type;
    };
    const isSelectedOrParent = (d: PartitionedNode): boolean => {
      if (!selectedNodeProp) return false;
      if (isSelected(d)) return true;
      // Check if this node is an ancestor of the selected node
      const selType = selectedNodeProp.meta?.type;
      if (selType === 'column' && d.data.meta?.type === 'table') {
        return d.children?.some(c => c.data.name === selectedNodeProp.name && c.data.meta?.type === 'column') ?? false;
      }
      if ((selType === 'table' || selType === 'column') && d.data.meta?.type === 'category') {
        if (selType === 'table') {
          return d.children?.some(c => c.data.name === selectedNodeProp.name) ?? false;
        }
        // column: check grandchildren
        return d.children?.some(t => t.children?.some(c => c.data.name === selectedNodeProp.name && c.data.meta?.type === 'column')) ?? false;
      }
      return false;
    };

    const defaultStroke = '#030712';
    const selectedStroke = '#f8fafc';

    // Draw arcs
    g.selectAll('path.arc')
      .data(visibleNodes)
      .join('path')
      .attr('class', 'arc')
      .attr('d', arc as unknown as (d: PartitionedNode) => string)
      .attr('fill', d => getColor(d))
      .attr('fill-opacity', d => isSelectedOrParent(d) ? 1 : getOpacity(d))
      .attr('stroke', d => isSelectedOrParent(d) ? selectedStroke : defaultStroke)
      .attr('stroke-width', d => isSelected(d) ? 2 : isSelectedOrParent(d) ? 1.5 : 0.5)
      .style('cursor', 'pointer')
      .on('mouseover', function(event, d) {
        d3.select(this)
          .transition().duration(150)
          .attr('fill-opacity', 1)
          .attr('stroke', selectedStroke)
          .attr('stroke-width', 1.5);

        // Tooltip
        const rect = container.getBoundingClientRect();
        const meta = d.data.meta;
        let type = meta?.category || '';
        let desc: string | undefined;
        if (meta?.type === 'category') {
          type = 'Category';
          desc = `${d.children?.length || 0} system tables`;
        } else if (meta?.type === 'table') {
          desc = meta.desc ? (meta.desc.length > 120 ? meta.desc.slice(0, 117) + '...' : meta.desc) : undefined;
        } else if (meta?.type === 'column') {
          desc = meta.desc;
        }
        setTooltip({
          x: event.clientX - rect.left,
          y: event.clientY - rect.top,
          content: { name: d.data.name, type, desc },
        });

        // Notify parent
        onHoverNode(d.data);
      })
      .on('mouseout', function(_, d) {
        const selOrParent = isSelectedOrParent(d);
        const sel = isSelected(d);
        d3.select(this)
          .transition().duration(150)
          .attr('fill-opacity', selOrParent ? 1 : getOpacity(d))
          .attr('stroke', selOrParent ? selectedStroke : defaultStroke)
          .attr('stroke-width', sel ? 2 : selOrParent ? 1.5 : 0.5);
        setTooltip(null);
      })
      .on('click', (event, d) => {
        event.stopPropagation();
        const meta = d.data.meta;
        if (meta?.type === 'category') {
          setFocusedNode(d);
        } else if (meta?.type === 'table' || meta?.type === 'column') {
          onSelectNode(d.data);
        }
      });

    // ─── Labels ────────────────────────────────────────────

    const isZoomed = focus !== partitionedRoot;

    const getLabelTransform = (d: PartitionedNode): string => {
      const focusRange = focus.x1 - focus.x0;
      const x0 = ((d.x0 - focus.x0) / focusRange) * 2 * Math.PI;
      const x1 = ((d.x1 - focus.x0) / focusRange) * 2 * Math.PI;
      const y0f = focus.y0;
      const yr = radius - y0f;
      const y0 = ((d.y0 - y0f) / yr) * radius;
      const y1 = ((d.y1 - y0f) / yr) * radius;
      const angle = ((x0 + x1) / 2) * (180 / Math.PI) - 90;
      const r = (y0 + y1) / 2;
      const flip = angle > 90 || angle < -90;
      return `rotate(${angle}) translate(${r}, 0) rotate(${flip ? 180 : 0})`;
    };

    const getArcMetrics = (d: PartitionedNode) => {
      const focusRange = focus.x1 - focus.x0;
      const x0 = ((d.x0 - focus.x0) / focusRange) * 2 * Math.PI;
      const x1 = ((d.x1 - focus.x0) / focusRange) * 2 * Math.PI;
      const y0f = focus.y0;
      const yr = radius - y0f;
      const y0 = ((d.y0 - y0f) / yr) * radius;
      const y1 = ((d.y1 - y0f) / yr) * radius;
      return { arcLength: (x1 - x0) * ((y0 + y1) / 2), arcWidth: y1 - y0 };
    };

    const labelsG = g.append('g').attr('class', 'labels');

    // Helper: truncate text to fit within available pixel width
    const truncateToFit = (el: SVGTextElement, name: string, availableWidth: number) => {
      el.textContent = name;
      if (el.getComputedTextLength() <= availableWidth) return;
      let lo = 0, hi = name.length;
      while (lo < hi) {
        const mid = (lo + hi + 1) >> 1;
        el.textContent = name.slice(0, mid) + '\u2026';
        if (el.getComputedTextLength() <= availableWidth) lo = mid;
        else hi = mid - 1;
      }
      el.textContent = lo > 0 ? name.slice(0, lo) + '\u2026' : '';
    };

    const fitLabel = (el: SVGTextElement, d: PartitionedNode, minPx: number, factor: number) => {
      const { arcLength, arcWidth } = getArcMetrics(d);
      const available = Math.min(arcLength, arcWidth) * factor;
      if (available < minPx) { el.textContent = ''; return; }
      truncateToFit(el, d.data.name, available);
    };

    // Category labels (depth 1 from focus)
    labelsG.selectAll('text.cat-label')
      .data(visibleNodes.filter(d => d.depth - focus.depth === 1))
      .join('text')
      .attr('class', 'cat-label')
      .attr('transform', getLabelTransform)
      .attr('text-anchor', 'middle')
      .attr('dominant-baseline', 'middle')
      .attr('fill', '#f8fafc')
      .attr('font-size', isZoomed ? '14px' : '11px')
      .attr('font-weight', '500')
      .attr('font-family', "'DM Sans', system-ui, sans-serif")
      .attr('pointer-events', 'none')
      .text(d => d.data.name)
      .nodes().forEach((el, i) => {
        const d = visibleNodes.filter(d => d.depth - focus.depth === 1)[i];
        fitLabel(el as SVGTextElement, d, 20, 0.85);
      });

    // Table labels (depth 2 from focus)
    const tableNodes = visibleNodes.filter(d => d.depth - focus.depth === 2);
    labelsG.selectAll('text.table-label')
      .data(tableNodes)
      .join('text')
      .attr('class', 'table-label')
      .attr('transform', getLabelTransform)
      .attr('text-anchor', 'middle')
      .attr('dominant-baseline', 'middle')
      .attr('fill', '#f8fafc')
      .attr('font-size', isZoomed ? '11px' : '9px')
      .attr('font-weight', '400')
      .attr('font-family', "'JetBrains Mono', monospace")
      .attr('pointer-events', 'none')
      .text(d => d.data.name)
      .nodes().forEach((el, i) => {
        fitLabel(el as SVGTextElement, tableNodes[i], 16, 0.85);
      });

    // Column labels (depth 3 from focus)
    const colNodes = visibleNodes.filter(d => d.depth - focus.depth === 3);
    labelsG.selectAll('text.col-label')
      .data(colNodes)
      .join('text')
      .attr('class', 'col-label')
      .attr('transform', getLabelTransform)
      .attr('text-anchor', 'middle')
      .attr('dominant-baseline', 'middle')
      .attr('fill', '#f8fafc')
      .attr('font-size', isZoomed ? '10px' : '8px')
      .attr('font-weight', '400')
      .attr('font-family', "'DM Sans', system-ui, sans-serif")
      .attr('pointer-events', 'none')
      .text(d => d.data.name)
      .nodes().forEach((el, i) => {
        fitLabel(el as SVGTextElement, colNodes[i], 14, 0.8);
      });

    // ─── Center label ──────────────────────────────────────

    const centerG = zoomG.append('g').attr('class', `center-label${focusedNode ? '' : ' center-unfocused'}`);

    if (focusedNode) {
      centerG.append('circle')
        .attr('r', 40)
        .attr('fill', 'var(--bg-tertiary, rgba(3,7,18,0.8))')
        .attr('stroke', 'var(--border-primary, #334155)')
        .attr('stroke-width', 1)
        .style('cursor', 'pointer')
        .on('click', () => setFocusedNode(null));

      centerG.append('text')
        .attr('text-anchor', 'middle')
        .attr('dominant-baseline', 'middle')
        .attr('y', -6)
        .attr('fill', 'var(--text-primary, #f8fafc)')
        .attr('font-size', '12px')
        .attr('font-family', "'JetBrains Mono', monospace")
        .attr('font-weight', '600')
        .style('cursor', 'pointer')
        .text(focusedNode.data.name.length > 14 ? focusedNode.data.name.slice(0, 12) + '\u2026' : focusedNode.data.name)
        .on('click', () => setFocusedNode(null));

      centerG.append('text')
        .attr('class', 'center-muted')
        .attr('text-anchor', 'middle')
        .attr('dominant-baseline', 'middle')
        .attr('y', 12)
        .attr('fill', 'var(--text-muted, #64748b)')
        .attr('font-size', '9px')
        .attr('font-family', "'JetBrains Mono', monospace")
        .style('cursor', 'pointer')
        .text('\u2190 back')
        .on('click', () => setFocusedNode(null));
    } else {
      centerG.append('text')
        .attr('text-anchor', 'middle')
        .attr('dominant-baseline', 'middle')
        .attr('y', -6)
        .attr('fill', 'var(--text-primary, #030712)')
        .attr('font-size', '13px')
        .attr('font-family', "'JetBrains Mono', monospace")
        .attr('font-weight', '700')
        .text('ClickHouse');

      centerG.append('text')
        .attr('class', 'center-muted')
        .attr('text-anchor', 'middle')
        .attr('dominant-baseline', 'middle')
        .attr('y', 12)
        .attr('fill', 'var(--text-muted, #64748b)')
        .attr('font-size', '9px')
        .attr('font-family', "'JetBrains Mono', monospace")
        .text('click to zoom');
    }

    // Cleanup
    return () => {
      if (rotationRef.current.animationId) {
        cancelAnimationFrame(rotationRef.current.animationId);
      }
    };
  }, [hierarchyData, focusedNode, matchingNodes, selectedNodeProp, onHoverNode, onSelectNode]);

  // ESC key handler
  useEffect(() => {
    const handleKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        if (focusedNode) {
          setFocusedNode(null);
        } else if (zoomBehaviorRef.current && svgRef.current) {
          d3.select(svgRef.current)
            .transition().duration(300)
            .call(zoomBehaviorRef.current.transform, d3.zoomIdentity);
        }
      }
    };
    window.addEventListener('keydown', handleKey);
    return () => window.removeEventListener('keydown', handleKey);
  }, [focusedNode]);

  return (
    <div
      ref={containerRef}
      className="obs-sunburst-container"
      style={{ cursor: isDragging ? 'grabbing' : 'grab' }}
    >
      <svg ref={svgRef} />

      {tooltip && (
        <div
          className="obs-sunburst-tooltip"
          style={{ left: tooltip.x + 12, top: tooltip.y - 12 }}
        >
          <span className="obs-sunburst-tooltip-name">{tooltip.content.name}</span>
          <span className="obs-sunburst-tooltip-type">{tooltip.content.type}</span>
          {tooltip.content.desc && (
            <span className="obs-sunburst-tooltip-desc">{tooltip.content.desc}</span>
          )}
        </div>
      )}

      {/* Legend */}
      <div className="obs-sunburst-legend">
        {sourceData.children.map(cat => (
          <div key={cat.name} className="obs-sunburst-legend-item">
            <div className="obs-sunburst-legend-dot" style={{ background: cat.color }} />
            {cat.name}
          </div>
        ))}
      </div>

      <div className="obs-sunburst-instructions">
        {focusedNode
          ? 'ESC or click center to zoom out \u2022 Drag to spin \u2022 Scroll to zoom'
          : 'Click groups to zoom in \u2022 Drag to spin \u2022 Scroll to zoom \u2022 Click tables to explore'
        }
      </div>
    </div>
  );
};
