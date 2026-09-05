/**
 * DistributedFlowDiagram — isometric "who asked whom" view of a distributed query.
 *
 * The timeline view places participants on a time axis. This one places them on
 * the request graph instead: coordinator, shard coordinators, readers, with the
 * rows and bytes that travelled back up each edge. Geometry comes from
 * buildDistributedFlowDiagram in @tracehouse/core; this file only paints it.
 */

import React, { useEffect, useId, useMemo, useRef, useState } from 'react';
import {
  computeTimeBreakdown,
  distributedNodeRoleLabel,
  participantCoordinate,
  topologyRoleTitle,
  TIME_BREAKDOWN_EVENTS,
  type DistributedTopology,
} from '@tracehouse/core';
import {
  buildDistributedFlowDiagram,
  computeFlowGauges,
  type DistributedFlowEdge,
  type DistributedFlowNode,
  type FlowNodeGauge,
} from './distributedFlowLayout';
import { formatBytes } from '../../../../stores/databaseStore';
import { formatDurationMs } from '../../../../utils/formatters';
import {
  COORD_COLOR,
  ERROR_COLOR,
  HOVER_TRANSITION,
  participantColor,
  shadeColor,
  shardColor,
} from './distributedTopologyPresentation';
import { SEGMENT_COLORS, SEGMENT_HINTS, pct } from './timeBreakdownDisplay';
import { TimeBreakdownPopover } from './TimeBreakdownPopover';

interface DistributedFlowDiagramProps {
  topology: DistributedTopology;
  activeQueryId: string;
  onNavigate: (queryId: string) => void;
  /** Short display name for a hostname, shared with the timeline view. */
  hostLabel: (hostname: string) => string;
  failedQueryIds?: string[];
}

/** Cube dimensions. The anchor (x, y) is the horizontal centre of the base. */
const CUBE_WIDTH = 62;
const CUBE_DEPTH = 32;
const CUBE_HEIGHT = 68;
const MUTED_EDGE_COLOR = 'var(--text-muted)';
/** Left edge of the label block, measured from the cube's anchor. */
const LABEL_OFFSET_X = 44;

/**
 * Gauge bars painted across both front faces of the cube, in face-local units:
 * `u` runs 0..1 across the whole front, `v` runs downward from the top of the
 * faces. Bars sit on the lower half so the shard hue still reads as a block of
 * colour above them, and wrapping the corner buys twice the bar length.
 */
const GAUGE_U0 = 0.09;
const GAUGE_U1 = 0.91;
const GAUGE_THICKNESS = 5;
const GAUGE_GAP = 4;
/** Distance from the cube's base to the bottom of the lowest bar. */
const GAUGE_BOTTOM_INSET = 10;
/** A share this small still gets a visible sliver, so "a little" != "none". */
const GAUGE_MIN_FILL = 0.05;

/** How much a hovered cube grows, as a fraction of its size, and how far it rises. */
const HOVER_SCALE = 0.07;
const HOVER_LIFT = 3;

/**
 * The hovered region around a node's anchor: its cube, shadow and label block.
 * Kept inside one row pitch and one column slot so neighbouring targets never
 * overlap, which would make the node under the pointer ambiguous.
 */
const HIT_AREA = { x: -46, y: -80, width: 240, height: 118 };

/** Panel-width granularity the layout is rebuilt at. */
const RESIZE_STEP_PX = 24;

/** 1 means "fit the panel width"; the SVG is sized as a percentage of it. */
const MIN_ZOOM = 0.5;
const MAX_ZOOM = 3;
const ZOOM_STEP = 0.25;

function step(zoom: number, direction: 1 | -1): number {
  return Math.min(MAX_ZOOM, Math.max(MIN_ZOOM, Math.round((zoom + direction * ZOOM_STEP) * 100) / 100));
}

/** Packets are decorative motion, so honour the OS setting rather than animating regardless. */
const prefersReducedMotionNow = (): boolean =>
  typeof window !== 'undefined'
  && typeof window.matchMedia === 'function'
  && window.matchMedia('(prefers-reduced-motion: reduce)').matches;

function usePrefersReducedMotion(): boolean {
  // Read at mount rather than in the effect, so the first paint already respects
  // the setting and no state is written during the effect.
  const [reduced, setReduced] = useState(prefersReducedMotionNow);
  useEffect(() => {
    if (typeof window === 'undefined' || typeof window.matchMedia !== 'function') return;
    const query = window.matchMedia('(prefers-reduced-motion: reduce)');
    const onChange = (event: MediaQueryListEvent) => setReduced(event.matches);
    query.addEventListener('change', onChange);
    return () => query.removeEventListener('change', onChange);
  }, []);
  return reduced;
}

/** Cube faces are three tones of one colour: lit top, shaded sides. */
function face(color: string, amount: number): string {
  return color.startsWith('#') ? shadeColor(color, amount) : color;
}

/**
 * What to call this participant on its second line, and whether that name
 * places it in the cluster or merely identifies the machine.
 *
 * The coordinate comes from system.clusters by way of the topology, not from
 * parsing the hostname. Hostnames are container ids and cloud hashes as often
 * as they are names, so a regex over them answers "s1r2" for some deployments
 * and a twelve-character hash for others, from data that was equally available
 * in both cases. The read-distribution table below the diagram has always used
 * the coordinate; this is the same rule, so the two panels agree.
 *
 * `placed` is false when the topology could not attribute a shard and replica.
 * The name shown then is the machine's, and the caller renders it differently:
 * "we could not place this host" and "this host is called s1r2" should not look
 * like the same statement.
 */
function nodeIdentity(node: DistributedFlowNode): { label: string; placed: boolean } {
  const coordinate = participantCoordinate(node.shardNum, node.replicaNum);
  return coordinate ? { label: coordinate, placed: true } : { label: node.hostLabel, placed: false };
}

function nodeRoleLabel(node: DistributedFlowNode): string {
  if (node.isFolded) {
    return node.shardNum != null && node.replicaNum != null
      ? `Local read · s${node.shardNum}r${node.replicaNum}`
      : 'Local read';
  }
  return distributedNodeRoleLabel(node.role, node.shardNum);
}

/** Two lines at most: work on the first, memory on the second. Anything taller
 * would run into the cube of the row below. */
function nodeDetails(node: DistributedFlowNode): string[] {
  const work: string[] = [];
  if (node.metrics.durationMs > 0) work.push(formatDurationMs(node.metrics.durationMs));
  work.push(node.metrics.readRows > 0 ? `${node.metrics.readRows.toLocaleString()} rows` : 'no rows read');
  const details = [work.join(' · ')];
  if (node.metrics.memoryUsage > 0) details.push(formatBytes(node.metrics.memoryUsage));
  return details;
}

function edgeLabel(edge: DistributedFlowEdge): string {
  if (edge.rows == null && edge.bytes == null) return '';
  const parts: string[] = [];
  if (edge.rows != null) parts.push(`${edge.rows.toLocaleString()} rows`);
  if (edge.bytes != null) parts.push(formatBytes(edge.bytes));
  return parts.join(' · ');
}

/** The ClickHouse wordmark drawn on the cluster boundary box, as in the pattern explorer. */
const ClickHouseMark: React.FC<{ x: number; y: number }> = ({ x, y }) => (
  <g transform={`translate(${x} ${y})`} fill="var(--text-muted)" opacity={0.8}>
    <rect width="3" height="20" />
    <rect x="5.5" width="3" height="20" />
    <rect x="11" width="3" height="20" />
    <rect x="16.5" width="3" height="20" />
    <rect x="22" y="7" width="3" height="6" />
  </g>
);

/**
 * A point on one of the cube's two front faces. `u` runs 0..1 across the whole
 * front of the cube — the first half on the left face, the second on the right —
 * and `v` runs downward from the top of the faces. Both faces are
 * parallelograms, so moving along u also moves the point vertically.
 */
function frontFacePoint(x: number, base: number, u: number, v: number): [number, number] {
  const halfWidth = CUBE_WIDTH / 2;
  const topOfFaces = base - CUBE_HEIGHT;
  if (u <= 0.5) {
    const t = u * 2;
    return [x - halfWidth + t * halfWidth, topOfFaces + CUBE_DEPTH / 2 + t * (CUBE_DEPTH / 2) + v];
  }
  const t = u * 2 - 1;
  return [x + t * halfWidth, topOfFaces + CUBE_DEPTH - t * (CUBE_DEPTH / 2) + v];
}

/**
 * Polygon points for one isometric bar segment lying on a front face. Segments
 * stop at the cube's front corner: a single quad spanning both faces would cut
 * the corner off and stop looking like paint on a solid.
 */
function frontFaceBar(x: number, base: number, u0: number, u1: number, v: number): string {
  return [
    frontFacePoint(x, base, u0, v),
    frontFacePoint(x, base, u1, v),
    frontFacePoint(x, base, u1, v + GAUGE_THICKNESS),
    frontFacePoint(x, base, u0, v + GAUGE_THICKNESS),
  ]
    .map(([px, py]) => `${px.toFixed(2)},${py.toFixed(2)}`)
    .join(' ');
}

/** Splits a bar at the front corner, so each piece sits on exactly one face. */
function barSegments(u0: number, u1: number): { face: 'left' | 'right'; u0: number; u1: number }[] {
  const segments: { face: 'left' | 'right'; u0: number; u1: number }[] = [];
  if (u0 < 0.5) segments.push({ face: 'left', u0, u1: Math.min(u1, 0.5) });
  if (u1 > 0.5) segments.push({ face: 'right', u0: Math.max(u0, 0.5), u1 });
  return segments;
}

/** Vertical offset of the nth bar, counted so the stack sits above the base. */
function gaugeRowY(index: number, count: number): number {
  const fromBottom = (count - index) * GAUGE_THICKNESS + (count - 1 - index) * GAUGE_GAP;
  return CUBE_HEIGHT - GAUGE_BOTTOM_INSET - fromBottom;
}

/**
 * Where a bar's caption goes: level with the bar, off the cube's left corner,
 * which is the side no label block occupies.
 */
function gaugeLabelAnchor(x: number, base: number, index: number, count: number): [number, number] {
  const [px, py] = frontFacePoint(x, base, GAUGE_U0, gaugeRowY(index, count) + GAUGE_THICKNESS / 2);
  return [px - 8, py];
}

/** The unfilled track and the filled part, per face, so the fold stays visible. */
const GAUGE_TRACK_FILL: Record<'left' | 'right', string> = {
  left: 'rgba(0, 0, 0, 0.34)',
  right: 'rgba(0, 0, 0, 0.24)',
};
const GAUGE_BAR_FILL: Record<'left' | 'right', string> = {
  left: 'rgba(255, 255, 255, 0.72)',
  right: 'rgba(255, 255, 255, 0.92)',
};

const Cube: React.FC<{
  x: number;
  y: number;
  color: string;
  dimmed?: boolean;
  /** Painted across the front faces, in order, as share-of-the-largest bars. */
  gauges?: FlowNodeGauge[];
}> = ({ x, y, color, dimmed, gauges }) => {
  const base = y;
  const topY = base - CUBE_HEIGHT;
  const halfWidth = CUBE_WIDTH / 2;
  return (
    <g opacity={dimmed ? 0.45 : 1}>
      <polygon
        points={`${x - halfWidth},${topY + CUBE_DEPTH / 2} ${x},${topY} ${x + halfWidth},${topY + CUBE_DEPTH / 2} ${x},${topY + CUBE_DEPTH}`}
        fill={face(color, 0.24)}
      />
      <polygon
        points={`${x - halfWidth},${topY + CUBE_DEPTH / 2} ${x},${topY + CUBE_DEPTH} ${x},${base + CUBE_DEPTH} ${x - halfWidth},${base + CUBE_DEPTH / 2}`}
        fill={face(color, -0.34)}
      />
      <polygon
        points={`${x},${topY + CUBE_DEPTH} ${x + halfWidth},${topY + CUBE_DEPTH / 2} ${x + halfWidth},${base + CUBE_DEPTH / 2} ${x},${base + CUBE_DEPTH}`}
        fill={face(color, -0.12)}
      />
      {/* Bars are painted in the faces' own planes and wrap around the front
          corner, so a cube keeps reading as a solid object with markings on it
          rather than as a chart pasted over a drawing. */}
      {gauges?.map((gauge, index) => {
        const v = gaugeRowY(index, gauges.length);
        const fillEnd = GAUGE_U0 + Math.max(gauge.share, GAUGE_MIN_FILL) * (GAUGE_U1 - GAUGE_U0);
        return (
          <g key={gauge.key}>
            {barSegments(GAUGE_U0, GAUGE_U1).map(segment => (
              <polygon
                key={`track-${segment.face}`}
                data-gauge-track={gauge.key}
                points={frontFaceBar(x, base, segment.u0, segment.u1, v)}
                fill={GAUGE_TRACK_FILL[segment.face]}
              />
            ))}
            {gauge.share > 0 && barSegments(GAUGE_U0, fillEnd).map(segment => (
              <polygon
                key={`bar-${segment.face}`}
                data-gauge={gauge.key}
                points={frontFaceBar(x, base, segment.u0, segment.u1, v)}
                fill={GAUGE_BAR_FILL[segment.face]}
              />
            ))}
          </g>
        );
      })}
    </g>
  );
};

export const DistributedFlowDiagram: React.FC<DistributedFlowDiagramProps> = ({
  topology,
  activeQueryId,
  onNavigate,
  hostLabel,
  failedQueryIds,
}) => {
  const instanceId = useId().replace(/:/g, '');
  const [zoom, setZoom] = useState(1);
  const [hovered, setHovered] = useState<{ id: string; rect: DOMRect } | null>(null);
  const prefersReducedMotion = usePrefersReducedMotion();

  /** ProfileEvents stay on the topology node; the geometry model carries only numbers. */
  const profileEventsByNodeId = useMemo(() => {
    const events = new Map<string, Record<string, number | string | undefined>>();
    for (const node of topology.nodes) events.set(node.id, node.profileEvents);
    return events;
  }, [topology.nodes]);

  // The layout stretches to the panel, so it has to be measured first. Width is
  // taken at zoom 1: zooming magnifies the result rather than relaying it out.
  const panelRef = useRef<HTMLDivElement>(null);
  const [panelWidth, setPanelWidth] = useState(0);
  useEffect(() => {
    const element = panelRef.current;
    if (!element || typeof ResizeObserver === 'undefined') return;
    const observer = new ResizeObserver(entries => {
      const width = entries[0]?.contentRect.width ?? 0;
      if (width <= 0) return;
      // Quantised: a drag fires this continuously, and a sub-pixel change is
      // not worth relaying out and repainting every cube for.
      setPanelWidth(Math.round(width / RESIZE_STEP_PX) * RESIZE_STEP_PX);
    });
    observer.observe(element);
    return () => observer.disconnect();
  }, []);

  const diagram = useMemo(
    () => buildDistributedFlowDiagram(topology, { hostLabel, failedQueryIds, availableWidth: panelWidth }),
    [topology, hostLabel, failedQueryIds, panelWidth],
  );

  const nodeById = useMemo(
    () => new Map(diagram.nodes.map(node => [node.id, node])),
    [diagram.nodes],
  );

  const gaugesByNodeId = useMemo(() => computeFlowGauges(diagram.nodes), [diagram.nodes]);

  /**
   * `currentColor` inside a marker resolves against the marker itself, not
   * against the path that references it, so a shared marker would always render
   * black. One marker per colour actually in use keeps arrowheads on-palette.
   */
  const edgeColorById = useMemo(() => {
    const colors = new Map<string, string>();
    for (const edge of diagram.edges) {
      const target = nodeById.get(edge.targetId);
      colors.set(
        edge.id,
        edge.kind === 'folded_local_read'
          ? MUTED_EDGE_COLOR
          : participantColor(target?.role, target?.shardNum, target?.replicaNum, target?.hasError),
      );
    }
    return colors;
  }, [diagram.edges, nodeById]);

  /**
   * In a wrapped fan-out the edges run past several cubes, so their labels land
   * on top of each other. Every number they carry is already on the target's own
   * label, so drop them there and keep the stroke weight as the volume cue.
   */
  const hideEdgeLabels = useMemo(
    () => diagram.nodes.some(node => node.subColumn > 0),
    [diagram.nodes],
  );

  const markerIds = useMemo(() => {
    const ids = new Map<string, string>();
    [...new Set(edgeColorById.values())].forEach((color, index) => ids.set(color, `${instanceId}-arrow-${index}`));
    return ids;
  }, [edgeColorById, instanceId]);

  if (diagram.nodes.length === 0) {
    return (
      <div style={{ fontSize: 11, color: 'var(--text-muted)', padding: '8px 0' }}>
        No participants to draw for this query.
      </div>
    );
  }

  const viewTop = 60;
  const hoveredNode = hovered ? nodeById.get(hovered.id) : undefined;
  const shardsInPlay = [...new Set(
    diagram.nodes.filter(node => node.shardNum != null).map(node => node.shardNum as number),
  )].sort((a, b) => a - b);

  return (
    <div>
      {hoveredNode && (
        <NodePopover
          anchor={hovered!.rect}
          node={hoveredNode}
          profileEvents={profileEventsByNodeId.get(hoveredNode.id)}
        />
      )}
      <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 4, marginBottom: 4 }}>
        <ZoomButton label="−" title="Zoom out" disabled={zoom <= MIN_ZOOM} onClick={() => setZoom(step(zoom, -1))} />
        <button
          type="button"
          onClick={() => setZoom(1)}
          title="Fit to width"
          style={{
            border: '1px solid var(--border-secondary)',
            background: 'transparent',
            color: 'var(--text-secondary)',
            borderRadius: 4,
            fontSize: 10,
            padding: '2px 8px',
            cursor: 'pointer',
            fontFamily: 'var(--font-mono, monospace)',
            minWidth: 48,
          }}
        >
          {Math.round(zoom * 100)}%
        </button>
        <ZoomButton label="+" title="Zoom in" disabled={zoom >= MAX_ZOOM} onClick={() => setZoom(step(zoom, 1))} />
      </div>
      <div ref={panelRef} style={{ width: '100%', maxHeight: 620, overflow: 'auto' }}>
      <svg
        viewBox={`0 ${viewTop} ${diagram.width} ${diagram.height - viewTop}`}
        preserveAspectRatio="xMinYMin meet"
        // The viewBox already matches the panel, so zoom 1 draws 1:1 and the
        // cubes keep their size however wide the panel is.
        style={{ display: 'block', width: `${zoom * 100}%`, height: 'auto' }}
        role="img"
        aria-label="Distributed query resource flow"
      >
        <defs>
          {[...markerIds].map(([color, id]) => (
            <marker
              key={id}
              id={id}
              viewBox="0 0 10 10"
              refX="9"
              refY="5"
              markerWidth="5"
              markerHeight="5"
              orient="auto-start-reverse"
            >
              <path d="M0 0L10 5L0 10z" fill={color} />
            </marker>
          ))}
          <filter id={`${instanceId}-shadow`}>
            <feGaussianBlur stdDeviation="7" />
          </filter>
        </defs>

        {diagram.groups.map(group => (
          <g key={group.id}>
            <rect
              x={group.x}
              y={group.y}
              width={group.width}
              height={group.height}
              rx={22}
              fill="var(--bg-secondary, transparent)"
              fillOpacity={0.35}
              stroke="var(--border-primary)"
              strokeDasharray="6 5"
            />
            <ClickHouseMark x={group.x + 16} y={group.y + 12} />
            <text
              x={group.x + 50}
              y={group.y + 27}
              fontSize={11}
              letterSpacing="1.2"
              fill="var(--text-muted)"
              fontFamily="var(--font-mono, monospace)"
            >
              {group.label}
            </text>
          </g>
        ))}

        {/* Column captions: which of the coordinator's queries this column ran. */}
        {diagram.columnLabels.map(label => (
          <text
            key={label.id}
            x={label.x}
            y={label.y}
            fontSize={10}
            letterSpacing="0.5"
            fill="var(--text-secondary)"
            fontFamily="var(--font-mono, monospace)"
          >
            {label.label}
          </text>
        ))}

        {/* Edges sit under the cubes so an arrow never crosses a face. */}
        <g fill="none">
          {diagram.edges.map((edge, index) => {
            const target = nodeById.get(edge.targetId);
            const color = edgeColorById.get(edge.id) ?? MUTED_EDGE_COLOR;
            const marker = `url(#${markerIds.get(color)})`;
            const share = target?.metrics.rowShare ?? 0;
            const label = hideEdgeLabels ? '' : edgeLabel(edge);
            const pathId = `${instanceId}-edge-${index}`;
            return (
              <g key={edge.id}>
                <path
                  id={pathId}
                  d={edge.path}
                  stroke={color}
                  strokeWidth={1.3 + share * 2.6}
                  strokeOpacity={edge.kind === 'folded_local_read' ? 0.45 : 0.75}
                  // Dashed, so the packet riding the curve reads as movement
                  // along a route rather than a dot sliding down a solid pipe.
                  strokeDasharray={edge.kind === 'folded_local_read' ? '4 5' : '7 6'}
                  markerEnd={marker}
                  markerStart={edge.kind === 'dispatch' ? marker : undefined}
                />
                {/* A packet riding the curve, from the child back to the
                    coordinator: the direction the result rows actually travel. */}
                {!prefersReducedMotion && edge.kind !== 'folded_local_read' && (
                  <circle r={2.6 + share * 2} fill={color} fillOpacity={0.9}>
                    <animateMotion
                      dur={`${2.4 + (index % 5) * 0.22}s`}
                      begin={`-${index * 0.29}s`}
                      repeatCount="indefinite"
                      keyPoints="1;0"
                      keyTimes="0;1"
                      calcMode="linear"
                    >
                      <mpath href={`#${pathId}`} />
                    </animateMotion>
                  </circle>
                )}
                {label && (
                  <text
                    x={edge.labelX}
                    y={edge.labelY}
                    textAnchor="middle"
                    fontSize={10}
                    fill="var(--text-muted)"
                    fontFamily="var(--font-mono, monospace)"
                  >
                    {label}
                  </text>
                )}
              </g>
            );
          })}
        </g>

        {diagram.nodes.map(node => {
          const color = participantColor(node.role, node.shardNum, node.replicaNum, node.hasError);
          const isActive = node.queryId === activeQueryId && !node.isFolded;
          const details = nodeDetails(node);
          const identity = nodeIdentity(node);
          const gauges = gaugesByNodeId.get(node.id);
          const isHovered = hovered?.id === node.id;
          return (
            <g
              key={node.id}
              onClick={node.isFolded ? undefined : () => onNavigate(node.queryId)}
              cursor={node.isFolded ? 'default' : 'pointer'}
              tabIndex={node.isFolded ? undefined : 0}
              role={node.isFolded ? undefined : 'button'}
              aria-label={`${identity.label} · ${nodeRoleLabel(node)} · ${node.hostname}`}
              onKeyDown={event => {
                if (node.isFolded) return;
                if (event.key === 'Enter' || event.key === ' ') {
                  event.preventDefault();
                  onNavigate(node.queryId);
                }
              }}
              onMouseEnter={event => setHovered({ id: node.id, rect: event.currentTarget.getBoundingClientRect() })}
              onMouseLeave={() => setHovered(current => (current?.id === node.id ? null : current))}
            >
              {/* The hover target, and the only one: it never moves, while the
                  drawing inside it grows. A cube that carried its own hit area
                  would shrink out from under the pointer as it grew, fire
                  mouseleave, grow back into it, and flicker. */}
              <rect
                x={node.x + HIT_AREA.x}
                y={node.y + HIT_AREA.y}
                width={HIT_AREA.width}
                height={HIT_AREA.height}
                fill="transparent"
                pointerEvents="all"
              />
              <g
                data-node-visual
                // Lifted and enlarged about the cube's anchor: with cubes this
                // close together, the popover alone does not say which one it
                // describes. Both states are written as the same chain of
                // functions so the browser interpolates them; dropping to no
                // transform at rest leaves nothing to animate from and the cube
                // snaps back instead.
                style={{
                  transform: `translate(${node.x}px, ${node.y}px) `
                    + `scale(${isHovered ? 1 + HOVER_SCALE : 1}) `
                    + `translate(${-node.x}px, ${-node.y - (isHovered ? HOVER_LIFT : 0)}px)`,
                  transition: `transform ${HOVER_TRANSITION}`,
                  pointerEvents: 'none',
                }}
              >
              <ellipse
                cx={node.x}
                cy={node.y + 33}
                rx={46}
                ry={11}
                fill="rgba(0, 0, 0, 0.22)"
                filter={`url(#${instanceId}-shadow)`}
              />
              {isActive && (
                <ellipse
                  cx={node.x}
                  cy={node.y + 33}
                  rx={54}
                  ry={16}
                  fill="none"
                  stroke={color}
                  strokeWidth={1.5}
                  strokeDasharray="4 4"
                />
              )}
              <Cube x={node.x} y={node.y} color={color} dimmed={node.isFolded} gauges={gauges} />
              {/* Which stripe is which, on demand. Painting three captions on
                  every cube would cost more ink than the bars themselves, so
                  the key in the legend carries the resting case. */}
              {isHovered && gauges?.map((gauge, index) => {
                const [labelX, labelY] = gaugeLabelAnchor(node.x, node.y, index, gauges.length);
                return (
                  <text
                    key={gauge.key}
                    x={labelX}
                    y={labelY}
                    textAnchor="end"
                    dominantBaseline="middle"
                    fontSize={9}
                    fill="var(--text-secondary)"
                    fontFamily="var(--font-mono, monospace)"
                    // Knocked out of whatever runs behind: an edge into the
                    // cube passes exactly where these captions sit.
                    stroke="var(--bg-secondary, #fff)"
                    strokeWidth={3}
                    paintOrder="stroke"
                  >
                    {gauge.label}
                  </text>
                );
              })}
              {/* Labels sit beside the cube, not under it: a row then costs the
                  height of a cube instead of a cube plus four lines of text.
                  The role leads and the hostname follows it: what a cube does
                  in this query is the thing being read, and one host can appear
                  twice under two roles, so the name alone does not identify a
                  participant. */}
              <text
                x={node.x + LABEL_OFFSET_X}
                y={node.y - 44}
                fontSize={12}
                fontWeight={600}
                fill="var(--text-primary)"
              >
                {nodeRoleLabel(node)}
                {node.hasError && <tspan fill={ERROR_COLOR} fontWeight={400}> · failed</tspan>}
              </text>
              <text
                x={node.x + LABEL_OFFSET_X}
                y={node.y - 28}
                fontSize={10}
                // An unplaced host is dimmer and italic: it is the machine's own
                // name standing in for a coordinate we could not resolve, not a
                // name for its part in the query.
                fill={identity.placed ? 'var(--text-secondary)' : 'var(--text-muted)'}
                fontStyle={identity.placed ? undefined : 'italic'}
                fontFamily="var(--font-mono, monospace)"
              >
                {identity.label}
              </text>
              <text
                x={node.x + LABEL_OFFSET_X}
                y={node.y - 10}
                fontSize={10}
                fill="var(--text-muted)"
                fontFamily="var(--font-mono, monospace)"
              >
                {details.map((detail, index) => (
                  <tspan key={detail} x={node.x + LABEL_OFFSET_X} dy={index === 0 ? 0 : 12}>{detail}</tspan>
                ))}
              </text>
              </g>
            </g>
          );
        })}
        {diagram.groups[0] && shardsInPlay.length > 0 && (
          <Legend
            x={diagram.groups[0].x + 20}
            y={diagram.groups[0].y + diagram.groups[0].height - 18}
            shards={shardsInPlay}
          />
        )}
      </svg>
      </div>
    </div>
  );
};

/**
 * Colour legend, drawn inside the cluster box next to its wordmark rather than
 * as HTML under the canvas: it describes the cubes, so it pans and zooms with
 * them. Laid out by hand in monospace, where an advance is a known fraction of
 * the font size and no measuring pass is needed.
 */
const LEGEND_FONT_SIZE = 10;
const LEGEND_CHAR_WIDTH = LEGEND_FONT_SIZE * 0.6;
const LEGEND_SWATCH = 8;
/** Swatch to its own label, label to the next entry. */
const LEGEND_LABEL_GAP = 6;
const LEGEND_ENTRY_GAP = 16;

const Legend: React.FC<{ x: number; y: number; shards: number[] }> = ({ x, y, shards }) => {
  const entries = [
    { color: COORD_COLOR, label: topologyRoleTitle('coordinator') },
    ...shards.map(shard => ({ color: shardColor(shard), label: `Shard ${shard}` })),
  ];
  let cursor = x;
  const placed = entries.map(entry => {
    const at = cursor;
    cursor += LEGEND_SWATCH + LEGEND_LABEL_GAP + entry.label.length * LEGEND_CHAR_WIDTH + LEGEND_ENTRY_GAP;
    return { ...entry, x: at };
  });
  return (
    <g fontFamily="var(--font-mono, monospace)" fontSize={LEGEND_FONT_SIZE}>
      {placed.map(entry => (
        <g key={entry.label}>
          <rect
            x={entry.x}
            y={y - LEGEND_SWATCH + 1}
            width={LEGEND_SWATCH}
            height={LEGEND_SWATCH}
            rx={2}
            fill={entry.color}
          />
          <text x={entry.x + LEGEND_SWATCH + LEGEND_LABEL_GAP} y={y} fill="var(--text-secondary)">
            {entry.label}
          </text>
        </g>
      ))}
      <text x={cursor} y={y} fill="var(--text-muted)">replicas are lighter shades</text>
    </g>
  );
};

/** The same panel the timeline bars use, narrowed: this one carries no layers. */
const NodePopover: React.FC<{
  anchor: DOMRect;
  node: DistributedFlowNode;
  profileEvents?: Record<string, number | string | undefined>;
}> = ({ anchor, node, profileEvents }) => {
  const breakdown = useMemo(
    () => computeTimeBreakdown(profileEvents, { wallClockMs: node.metrics.durationMs }),
    [profileEvents, node.metrics.durationMs],
  );

  // The cube already carries duration, rows and memory beside it, so the panel
  // is for what the cube cannot say: which query this was, and where its time
  // went. Metrics stay, but folded into one line per kind of work rather than
  // one row each, which is what made the panel taller than the diagram.
  const facts = [
    { label: 'query_id', value: node.queryId },
    { label: 'host', value: node.hostname },
    ...(node.shapeLabel ? [{ label: 'query', value: node.shapeLabel }] : []),
    {
      label: 'read',
      value: `${node.metrics.readRows.toLocaleString()} rows · ${formatBytes(node.metrics.readBytes)}`
        + (node.metrics.selectedParts ? ` · ${node.metrics.selectedParts.toLocaleString()} parts` : ''),
    },
    {
      label: 'cost',
      value: `${formatDurationMs(node.metrics.durationMs)} · ${formatBytes(node.metrics.memoryUsage)} peak`,
    },
    ...(node.metrics.rowShare != null
      ? [{ label: 'share', value: `${pct(node.metrics.rowShare)} of rows read` }]
      : []),
  ];

  return (
    <TimeBreakdownPopover
      anchor={anchor}
      title={node.isFolded ? 'Folded local read' : nodeRoleLabel(node)}
      facts={facts}
      segments={breakdown.segments.map(segment => ({
        label: segment.label,
        color: SEGMENT_COLORS[segment.key],
        pct: pct(segment.share),
        hint: SEGMENT_HINTS[segment.key],
        source: TIME_BREAKDOWN_EVENTS[segment.key],
      }))}
      layers={[]}
      caveats={breakdown.normalized ? ['! counters overlapped past total; scaled to fit'] : []}
      // Transparent to the pointer: the panel would otherwise sit over the very
      // cubes being compared and block moving on to the next one.
      interactive={false}
      // Frosted and narrow: this panel opens over the diagram, and an opaque
      // slab the width of the segment sources hid four cubes to describe one.
      variant="overlay"
    />
  );
};

const ZoomButton: React.FC<{
  label: string;
  title: string;
  disabled: boolean;
  onClick: () => void;
}> = ({ label, title, disabled, onClick }) => (
  <button
    type="button"
    onClick={onClick}
    disabled={disabled}
    title={title}
    aria-label={title}
    style={{
      border: '1px solid var(--border-secondary)',
      background: 'transparent',
      color: disabled ? 'var(--text-muted)' : 'var(--text-secondary)',
      borderRadius: 4,
      fontSize: 12,
      lineHeight: 1,
      width: 22,
      height: 20,
      cursor: disabled ? 'default' : 'pointer',
    }}
  >
    {label}
  </button>
);

export default DistributedFlowDiagram;
