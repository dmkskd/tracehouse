/**
 * TimelineChart3DSurface — Fluid 3D visualization that faithfully reproduces
 * the 2D stacked-area aesthetic in three dimensions.
 *
 * Each band is a smooth extruded ribbon:
 *   - The XY profile is the exact same stacked-area shape as the 2D SVG chart
 *     (top curve + bottom curve, no gaps — bands sit flush on each other)
 *   - Extruded along Z by log-scaled peak memory (thicker = more memory)
 *
 * X = time, Y = stacked metric, Z = memory depth.
 * The result looks like the 2D chart given physical depth — a tunnel of fluid ribbons.
 */
import React, { useMemo, useState, useCallback, useEffect, useRef } from 'react';
import { Canvas, useThree } from '@react-three/fiber';
import { useThemeDetection } from '../../hooks/useThemeDetection';
import { OrbitControls, Text, Line, Html } from '@react-three/drei';
import * as THREE from 'three';
import type { MemoryTimeline, QuerySeries, MergeSeries, MutationSeries } from '@tracehouse/core';
import { parseTimestamp, formatBytes } from '../../utils/formatters';
import { type MetricMode, type HighlightedItem, Q_COLORS, M_COLORS, MUT_COLORS, METRIC_CONFIG } from './timeline-constants';

/* ── Constants ──────────────────────────────────────────────────────── */

const RUNWAY_LENGTH = 24;
const CEILING_Y = 8;
const MAX_Z_DEPTH = 10;
const MIN_Z_DEPTH = 0.25;

/* ── Types ──────────────────────────────────────────────────────────── */

interface BandInfo {
  type: 'query' | 'merge' | 'mutation';
  idx: number;
  id: string;
  color: string;
  peakMemory: number;
  isRunning: boolean;
  label: string;
  item: QuerySeries | MergeSeries | MutationSeries;
}

interface TimelineChart3DSurfaceProps {
  data: MemoryTimeline;
  metricMode: MetricMode;
  height?: number;
  hiddenCategories?: Set<'query' | 'merge' | 'mutation'>;
  onBandClick?: (band: { type: 'query' | 'merge' | 'mutation'; idx: number }) => void;
  onHighlightItem?: (item: HighlightedItem) => void;
}

/* ── Helpers ─────────────────────────────────────────────────────────── */

import { formatDurationMs as fmtMs } from '../../utils/formatters';

function getRate(item: { cpu_us: number; peak_memory: number; duration_ms: number; net_send: number; net_recv: number; disk_read: number; disk_write: number }, mode: MetricMode): number {
  const durS = Math.max(item.duration_ms / 1000, 0.001);
  if (mode === 'memory') return item.peak_memory;
  if (mode === 'cpu') return item.cpu_us / durS;
  if (mode === 'network') return (item.net_send + item.net_recv) / durS;
  return (item.disk_read + item.disk_write) / durS;
}

/* ── Camera ──────────────────────────────────────────────────────────── */

const CameraSetup: React.FC = () => {
  const { camera } = useThree();
  useMemo(() => {
    camera.position.set(RUNWAY_LENGTH * 0.55, CEILING_Y * 0.9, MAX_Z_DEPTH + 8);
    camera.lookAt(RUNWAY_LENGTH / 2, CEILING_Y / 3, MAX_Z_DEPTH / 2);
  }, [camera]);
  return null;
};

/* ── Floor grid ──────────────────────────────────────────────────────── */

const FloorGrid: React.FC = () => (
  <group position={[RUNWAY_LENGTH / 2, 0, MAX_Z_DEPTH / 2]}>
    <gridHelper args={[Math.max(RUNWAY_LENGTH + 2, MAX_Z_DEPTH + 2), 40, 0x1e293b, 0x0f172a]} />
  </group>
);

/* ── Ceiling plane ──────────────────────────────────────────────────── */

const CeilingPlane: React.FC = () => {
  const halfZ = MAX_Z_DEPTH / 2 + 1;
  return (
    <group position={[RUNWAY_LENGTH / 2, CEILING_Y, MAX_Z_DEPTH / 2]}>
      <mesh rotation={[-Math.PI / 2, 0, 0]}>
        <planeGeometry args={[RUNWAY_LENGTH + 1, MAX_Z_DEPTH + 2]} />
        <meshStandardMaterial color="#ef4444" transparent opacity={0.08} side={THREE.DoubleSide} depthWrite={false} />
      </mesh>
      <Line
        points={[
          [-RUNWAY_LENGTH / 2 - 0.5, 0, -halfZ],
          [RUNWAY_LENGTH / 2 + 0.5, 0, -halfZ],
          [RUNWAY_LENGTH / 2 + 0.5, 0, halfZ],
          [-RUNWAY_LENGTH / 2 - 0.5, 0, halfZ],
          [-RUNWAY_LENGTH / 2 - 0.5, 0, -halfZ],
        ]}
        color="#ef4444" lineWidth={1.5} opacity={0.35} transparent
      />
      <Text position={[RUNWAY_LENGTH / 2 + 1.5, 0, 0]} fontSize={0.3} color="#ef4444" anchorX="left">
        100%
      </Text>
    </group>
  );
};

/* ── Time labels ─────────────────────────────────────────────────────── */

const TimeLabels: React.FC<{ tMin: number; tMax: number }> = ({ tMin, tMax }) => {
  const labels = useMemo(() => {
    const count = 8;
    const result: { x: number; text: string }[] = [];
    for (let i = 0; i <= count; i++) {
      const frac = i / count;
      const ms = tMin + frac * (tMax - tMin);
      const d = new Date(ms);
      result.push({
        x: frac * RUNWAY_LENGTH,
        text: `${d.getHours().toString().padStart(2, '0')}:${d.getMinutes().toString().padStart(2, '0')}:${d.getSeconds().toString().padStart(2, '0')}`,
      });
    }
    return result;
  }, [tMin, tMax]);

  return (
    <>
      {labels.map((l, i) => (
        <Text key={i} position={[l.x, -0.25, MAX_Z_DEPTH + 1.2]} fontSize={0.22} color="#94a3b8" anchorX="center">
          {l.text}
        </Text>
      ))}
    </>
  );
};

/* ── Y-axis markers ──────────────────────────────────────────────────── */

const YAxisMarkers: React.FC<{ maxStack: number; metricMode: MetricMode }> = ({ maxStack, metricMode }) => {
  const ticks = useMemo(() => {
    if (maxStack <= 0) return [];
    const count = 4;
    const result: { y: number; label: string }[] = [];
    for (let i = 1; i <= count; i++) {
      const frac = i / count;
      const y = frac * CEILING_Y;
      const val = frac * maxStack;
      let label: string;
      if (metricMode === 'cpu') {
        label = val >= 1e6 ? `${(val / 1e6).toFixed(1)}s/s` : `${(val / 1e3).toFixed(0)}ms/s`;
      } else if (metricMode === 'memory') {
        label = formatBytes(val);
      } else {
        label = formatBytes(val) + '/s';
      }
      result.push({ y, label });
    }
    return result;
  }, [maxStack, metricMode]);

  return (
    <>
      {ticks.map((t, i) => (
        <group key={i}>
          <Text position={[-0.6, t.y, 0]} fontSize={0.2} color="#64748b" anchorX="right">
            {t.label}
          </Text>
          <Line
            points={[[0, t.y, -0.3], [RUNWAY_LENGTH, t.y, -0.3]]}
            color="#334155" lineWidth={0.5} opacity={0.2} transparent
          />
        </group>
      ))}
    </>
  );
};

/* ── Z-axis memory labels ────────────────────────────────────────────── */

const ZAxisLabels: React.FC<{ memMin: number; memMax: number; memToDepth: (m: number) => number }> = ({ memMin, memMax, memToDepth }) => {
  const ticks = useMemo(() => {
    if (memMax <= 0) return [];
    const logMin = memMin > 0 ? Math.log10(memMin) : 0;
    const logMax = memMax > 0 ? Math.log10(memMax) : 1;
    const logRange = logMax - logMin || 1;
    const count = 4;
    const result: { z: number; label: string }[] = [];
    for (let i = 1; i <= count; i++) {
      const frac = i / count;
      const logVal = logMin + frac * logRange;
      const val = Math.pow(10, logVal);
      result.push({ z: memToDepth(val), label: formatBytes(val) });
    }
    return result;
  }, [memMin, memMax, memToDepth]);

  return (
    <>
      {ticks.map((t, i) => (
        <group key={i}>
          <Text position={[-0.6, -0.25, t.z]} fontSize={0.2} color="#8b5cf6" anchorX="right">
            {t.label}
          </Text>
          <Line
            points={[[0, 0.01, t.z], [RUNWAY_LENGTH, 0.01, t.z]]}
            color="#8b5cf6" lineWidth={0.5} opacity={0.12} transparent
          />
        </group>
      ))}
      <Line
        points={[[-0.2, 0, 0], [-0.2, 0, MAX_Z_DEPTH]]}
        color="#8b5cf6" lineWidth={1} opacity={0.3} transparent
      />
    </>
  );
};

/* ── Server metric line ──────────────────────────────────────────────── */

const ServerMetricLine: React.FC<{
  serverPts: { ms: number; v: number }[];
  tMin: number; tRange: number; maxY: number;
  color: string;
}> = ({ serverPts, tMin, tRange, maxY, color }) => {
  const points = useMemo(() => {
    if (serverPts.length < 2 || maxY <= 0) return null;
    return serverPts.map(p => {
      const x = ((p.ms - tMin) / tRange) * RUNWAY_LENGTH;
      const y = (p.v / maxY) * CEILING_Y;
      return new THREE.Vector3(x, Math.max(y, 0), -0.3);
    });
  }, [serverPts, tMin, tRange, maxY]);

  if (!points) return null;
  return <Line points={points} color={color} lineWidth={2.5} opacity={0.85} transparent />;
};

/* ── Axis labels ─────────────────────────────────────────────────────── */

const AxisLabels: React.FC<{ metricMode: MetricMode }> = ({ metricMode }) => (
  <>
    <Text position={[RUNWAY_LENGTH / 2, -0.6, MAX_Z_DEPTH + 1.5]} fontSize={0.25} color="#64748b">
      TIME
    </Text>
    <Text position={[-1.2, CEILING_Y / 2, 0]} fontSize={0.25} color="#64748b" rotation={[0, 0, Math.PI / 2]}>
      {metricMode.toUpperCase()}
    </Text>
    <Text position={[-0.3, -0.6, MAX_Z_DEPTH / 2]} fontSize={0.25} color="#8b5cf6" rotation={[0, Math.PI / 2, 0]}>
      MEMORY (depth)
    </Text>
  </>
);

/* ── Legend ───────────────────────────────────────────────────────────── */

const Legend: React.FC<{ hasQueries: boolean; hasMerges: boolean; hasMutations: boolean }> = ({ hasQueries, hasMerges, hasMutations }) => {
  const items = [
    ...(hasQueries ? [{ label: 'Queries', color: Q_COLORS[0] }] : []),
    ...(hasMerges ? [{ label: 'Merges', color: M_COLORS[0] }] : []),
    ...(hasMutations ? [{ label: 'Mutations', color: MUT_COLORS[0] }] : []),
  ];
  return (
    <>
      {items.map((item, i) => (
        <group key={i} position={[RUNWAY_LENGTH + 1.2, CEILING_Y - i * 0.55, 0]}>
          <mesh scale={[0.25, 0.25, 0.25]}>
            <boxGeometry />
            <meshStandardMaterial color={item.color} transparent opacity={0.6} />
          </mesh>
          <Text position={[0.35, 0, 0]} fontSize={0.2} color="#94a3b8" anchorX="left">
            {item.label}
          </Text>
        </group>
      ))}
    </>
  );
};

/* ── Fluid ribbon: extruded 2D stacked-area profile ──────────────────── */

/**
 * Creates a BufferGeometry for one band ribbon.
 * The ribbon is defined by top[] and bot[] Y values at each X point,
 * extruded from z=0 to z=depth.
 *
 * Faces: front (z=0), back (z=depth), top surface, bottom surface,
 * left cap, right cap.
 */
function buildRibbonGeometry(
  xPositions: number[],
  topY: number[],
  botY: number[],
  depth: number,
): THREE.BufferGeometry {
  const n = xPositions.length;
  if (n < 2) return new THREE.BufferGeometry();

  // We build a set of quads for each segment between adjacent X points.
  // Each segment has 6 faces (front, back, top, bottom) = 4 quads × 2 triangles each
  // Plus left and right end caps.

  const positions: number[] = [];
  const normals: number[] = [];

  function pushTri(
    ax: number, ay: number, az: number,
    bx: number, by: number, bz: number,
    cx: number, cy: number, cz: number,
  ) {
    positions.push(ax, ay, az, bx, by, bz, cx, cy, cz);
    // Compute face normal
    const ux = bx - ax, uy = by - ay, uz = bz - az;
    const vx = cx - ax, vy = cy - ay, vz = cz - az;
    const nx = uy * vz - uz * vy;
    const ny = uz * vx - ux * vz;
    const nz = ux * vy - uy * vx;
    const len = Math.sqrt(nx * nx + ny * ny + nz * nz) || 1;
    normals.push(nx / len, ny / len, nz / len);
    normals.push(nx / len, ny / len, nz / len);
    normals.push(nx / len, ny / len, nz / len);
  }

  function pushQuad(
    ax: number, ay: number, az: number,
    bx: number, by: number, bz: number,
    cx: number, cy: number, cz: number,
    dx: number, dy: number, dz: number,
  ) {
    pushTri(ax, ay, az, bx, by, bz, cx, cy, cz);
    pushTri(ax, ay, az, cx, cy, cz, dx, dy, dz);
  }

  for (let i = 0; i < n - 1; i++) {
    const x0 = xPositions[i], x1 = xPositions[i + 1];
    const t0 = topY[i], t1 = topY[i + 1];
    const b0 = botY[i], b1 = botY[i + 1];
    const z0 = 0, z1 = depth;

    // Front face (z=0): quad from (x0,b0,0) to (x1,t1,0)
    pushQuad(
      x0, b0, z0, x1, b1, z0, x1, t1, z0, x0, t0, z0,
    );

    // Back face (z=depth): reversed winding
    pushQuad(
      x0, t0, z1, x1, t1, z1, x1, b1, z1, x0, b0, z1,
    );

    // Top surface: quad connecting top edges at z0 and z1
    pushQuad(
      x0, t0, z0, x0, t0, z1, x1, t1, z1, x1, t1, z0,
    );

    // Bottom surface: quad connecting bottom edges at z0 and z1
    pushQuad(
      x0, b0, z1, x0, b0, z0, x1, b1, z0, x1, b1, z1,
    );
  }

  // Left end cap (i=0)
  {
    const x = xPositions[0], t = topY[0], b = botY[0];
    pushQuad(x, b, depth, x, t, depth, x, t, 0, x, b, 0);
  }

  // Right end cap (i=n-1)
  {
    const x = xPositions[n - 1], t = topY[n - 1], b = botY[n - 1];
    pushQuad(x, b, 0, x, t, 0, x, t, depth, x, b, depth);
  }

  const geo = new THREE.BufferGeometry();
  geo.setAttribute('position', new THREE.Float32BufferAttribute(positions, 3));
  geo.setAttribute('normal', new THREE.Float32BufferAttribute(normals, 3));
  return geo;
}

/**
 * Build a wireframe outline for the ribbon (top and bottom edges on front+back face).
 */
function buildRibbonWireframe(
  xPositions: number[],
  topY: number[],
  botY: number[],
  depth: number,
): THREE.Vector3[] {
  const pts: THREE.Vector3[] = [];
  const n = xPositions.length;
  if (n < 2) return pts;

  // Front face outline: top edge → right cap → bottom edge (reversed) → left cap
  for (let i = 0; i < n; i++) pts.push(new THREE.Vector3(xPositions[i], topY[i], 0));
  // Right cap down
  pts.push(new THREE.Vector3(xPositions[n - 1], botY[n - 1], 0));
  // Bottom edge reversed
  for (let i = n - 1; i >= 0; i--) pts.push(new THREE.Vector3(xPositions[i], botY[i], 0));
  // Close front face
  pts.push(new THREE.Vector3(xPositions[0], topY[0], 0));

  // Back face outline
  for (let i = 0; i < n; i++) pts.push(new THREE.Vector3(xPositions[i], topY[i], depth));
  pts.push(new THREE.Vector3(xPositions[n - 1], botY[n - 1], depth));
  for (let i = n - 1; i >= 0; i--) pts.push(new THREE.Vector3(xPositions[i], botY[i], depth));
  pts.push(new THREE.Vector3(xPositions[0], topY[0], depth));

  return pts;
}

/* ── Band ribbon component ───────────────────────────────────────────── */

const BandRibbon: React.FC<{
  xPositions: number[];
  topY: number[];
  botY: number[];
  zDepth: number;
  color: string;
  hovered: boolean;
  bandIdx: number;
  onHover: (idx: number | null) => void;
  onClick?: () => void;
}> = ({ xPositions, topY, botY, zDepth, color, hovered, bandIdx, onHover, onClick }) => {
  const pointerDownPos = useRef<{ x: number; y: number } | null>(null);
  const threeColor = useMemo(() => new THREE.Color(color), [color]);

  const geometry = useMemo(
    () => buildRibbonGeometry(xPositions, topY, botY, zDepth),
    [xPositions, topY, botY, zDepth],
  );

  const wireframePts = useMemo(
    () => buildRibbonWireframe(xPositions, topY, botY, zDepth),
    [xPositions, topY, botY, zDepth],
  );

  const opacity = hovered ? 0.65 : 0.4;
  const emissiveIntensity = hovered ? 0.35 : 0.1;

  // Check if band has any visible area
  const hasArea = useMemo(() => {
    for (let i = 0; i < topY.length; i++) {
      if (topY[i] - botY[i] > 0.001) return true;
    }
    return false;
  }, [topY, botY]);

  if (!hasArea) return null;

  return (
    <group
      onPointerEnter={(e) => { e.stopPropagation(); onHover(bandIdx); }}
      onPointerLeave={(e) => { e.stopPropagation(); onHover(null); }}
      onPointerDown={(e) => { pointerDownPos.current = { x: e.clientX, y: e.clientY }; }}
      onPointerUp={(e) => {
        if (!pointerDownPos.current || !onClick) return;
        const dx = e.clientX - pointerDownPos.current.x;
        const dy = e.clientY - pointerDownPos.current.y;
        if (dx * dx + dy * dy < 25) { e.stopPropagation(); onClick(); }
        pointerDownPos.current = null;
      }}
    >
      <mesh geometry={geometry}>
        <meshStandardMaterial
          color={threeColor}
          transparent
          opacity={opacity}
          roughness={0.15}
          metalness={0.2}
          emissive={threeColor}
          emissiveIntensity={emissiveIntensity}
          side={THREE.DoubleSide}
          depthWrite={false}
        />
      </mesh>
      {wireframePts.length > 2 && (
        <Line
          points={wireframePts}
          color={color}
          lineWidth={hovered ? 2 : 1}
          opacity={hovered ? 0.85 : 0.45}
          transparent
        />
      )}
    </group>
  );
};

/* ── Hover tooltip ────────────────────────────────────────────────────── */

const HoverTooltip: React.FC<{
  band: BandInfo | null;
  position: [number, number, number];
}> = ({ band, position }) => {
  if (!band) return null;
  const item = band.item;
  return (
    <Html position={position} center style={{ pointerEvents: 'none' }}>
      <div style={{
        background: 'rgba(15, 23, 42, 0.95)',
        border: '1px solid rgba(148, 163, 184, 0.2)',
        borderRadius: 8,
        padding: '8px 12px',
        fontSize: 11,
        color: '#e2e8f0',
        whiteSpace: 'nowrap',
        backdropFilter: 'blur(8px)',
        minWidth: 180,
      }}>
        <div style={{ fontWeight: 600, color: band.color, marginBottom: 4 }}>
          {band.type === 'query' ? ('query_kind' in item ? (item as QuerySeries).query_kind || 'Select' : 'Select') : band.type === 'merge' ? 'Merge' : 'Mutation'}
          {band.isRunning && <span style={{ marginLeft: 6, color: '#3fb950', fontSize: 10 }}>RUNNING</span>}
        </div>
        <div style={{ color: '#94a3b8', fontSize: 10, marginBottom: 4, maxWidth: 260, overflow: 'hidden', textOverflow: 'ellipsis' }}>
          {band.label.length > 60 ? band.label.slice(0, 57) + '...' : band.label}
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: 'auto 1fr auto', gap: '2px 8px', alignItems: 'center' }}>
          {[
            { label: 'CPU', color: '#3fb950', val: item.cpu_us, fmt: item.cpu_us >= 1e6 ? `${(item.cpu_us / 1e6).toFixed(1)}s` : `${(item.cpu_us / 1e3).toFixed(0)}ms` },
            { label: 'MEM', color: '#58a6ff', val: item.peak_memory, fmt: formatBytes(item.peak_memory) },
            { label: 'DISK', color: '#bc8cff', val: item.disk_read + item.disk_write, fmt: formatBytes(item.disk_read + item.disk_write) },
            { label: 'NET', color: '#d29922', val: item.net_send + item.net_recv, fmt: formatBytes(item.net_send + item.net_recv) },
          ].map(m => (
            <React.Fragment key={m.label}>
              <span style={{ color: m.color, fontSize: 10 }}>{m.label}</span>
              <div style={{ height: 4, borderRadius: 2, background: '#1e293b' }}>
                <div style={{ height: '100%', borderRadius: 2, background: m.color, width: `${Math.min(100, Math.max(2, (m.val / 1e9) * 100))}%` }} />
              </div>
              <span style={{ fontSize: 10 }}>{m.fmt}</span>
            </React.Fragment>
          ))}
        </div>
        <div style={{ marginTop: 4, color: '#64748b', fontSize: 10 }}>
          Duration: {fmtMs(item.duration_ms)} · Mem depth: {formatBytes(item.peak_memory)}
        </div>
      </div>
    </Html>
  );
};

/* ── Main component ──────────────────────────────────────────────────── */

export const TimelineChart3DSurface: React.FC<TimelineChart3DSurfaceProps> = ({
  data,
  metricMode,
  height = 500,
  hiddenCategories,
  onBandClick,
  onHighlightItem,
}) => {
  const [hoveredBand, setHoveredBand] = useState<number | null>(null);
  const theme = useThemeDetection();
  const bg3d = theme === 'light' ? '#2a2e3e' : '#0a0a1a';

  // Build band info list (same order as 2D: queries, merges, mutations)
  const bands: BandInfo[] = useMemo(() => {
    const result: BandInfo[] = [];
    const hideQ = hiddenCategories?.has('query') ?? false;
    const hideM = hiddenCategories?.has('merge') ?? false;
    const hideMut = hiddenCategories?.has('mutation') ?? false;

    if (!hideQ) {
      data.queries.forEach((q, i) => {
        result.push({
          type: 'query', idx: i, id: q.query_id,
          color: Q_COLORS[i % Q_COLORS.length],
          peakMemory: q.peak_memory,
          isRunning: q.is_running ?? false,
          label: q.label || q.query_id.slice(0, 12),
          item: q,
        });
      });
    }
    if (!hideM) {
      data.merges.forEach((m, i) => {
        result.push({
          type: 'merge', idx: i, id: `${m.table}:${m.part_name}:${m.hostname ?? ''}`,
          color: M_COLORS[i % M_COLORS.length],
          peakMemory: m.peak_memory,
          isRunning: m.is_running ?? false,
          label: `${m.table}:${m.part_name}`.slice(0, 24),
          item: m,
        });
      });
    }
    if (!hideMut) {
      (data.mutations ?? []).forEach((m, i) => {
        result.push({
          type: 'mutation', idx: i, id: `${m.table}:${m.part_name}:${m.hostname ?? ''}`,
          color: MUT_COLORS[i % MUT_COLORS.length],
          peakMemory: m.peak_memory,
          isRunning: m.is_running ?? false,
          label: `${m.table}:${m.part_name}`.slice(0, 24),
          item: m,
        });
      });
    }
    return result;
  }, [data, hiddenCategories]);

  // Server-level metric points
  const serverPts = useMemo(() => {
    let pts;
    if (metricMode === 'memory') pts = data.server_memory;
    else if (metricMode === 'cpu') pts = data.server_cpu;
    else if (metricMode === 'network') {
      pts = data.server_network_send.map((p, i) => ({
        t: p.t, v: p.v + (data.server_network_recv[i]?.v ?? 0),
      }));
    } else {
      pts = (data.server_disk_read ?? []).map((p, i) => ({
        t: p.t, v: p.v + (data.server_disk_write?.[i]?.v ?? 0),
      }));
    }
    return pts.map(p => ({ ms: parseTimestamp(p.t), v: p.v }));
  }, [data, metricMode]);

  const tMin = serverPts.length > 0 ? serverPts[0].ms : new Date(data.window_start).getTime();
  const tMax = serverPts.length > 0 ? serverPts[serverPts.length - 1].ms : new Date(data.window_end).getTime();
  const tRange = tMax - tMin || 1;

  // Per-band metric rate
  const bandRates = useMemo(() => bands.map(b => getRate(b.item as any, metricMode)), [bands, metricMode]);

  // Band time ranges
  const bandTimeRanges = useMemo(() => bands.map(b => ({
    startMs: parseTimestamp(b.item.start_time),
    endMs: parseTimestamp(b.item.end_time),
  })), [bands]);

  // Build cumulative stacks at each server metric point (identical to 2D)
  const { xPositions, cumStacks, maxStack } = useMemo(() => {
    if (serverPts.length === 0 || bands.length === 0) {
      return { xPositions: [] as number[], cumStacks: [] as number[][], maxStack: 0 };
    }

    const xPos = serverPts.map(sp => ((sp.ms - tMin) / tRange) * RUNWAY_LENGTH);
    const nBands = bands.length;

    // cumStacks[bandIdx] = array of cumulative heights at each server point
    const stacks: number[][] = [];
    let globalMax = 0;

    for (let bi = 0; bi < nBands; bi++) {
      const row: number[] = [];
      const tr = bandTimeRanges[bi];
      const rate = bandRates[bi];
      for (let pi = 0; pi < serverPts.length; pi++) {
        const t = serverPts[pi].ms;
        const val = (t >= tr.startMs && t <= tr.endMs) ? rate : 0;
        const prev = bi > 0 ? stacks[bi - 1][pi] : 0;
        const cum = prev + val;
        row.push(cum);
        if (cum > globalMax) globalMax = cum;
      }
      stacks.push(row);
    }

    return { xPositions: xPos, cumStacks: stacks, maxStack: globalMax };
  }, [serverPts, bands, bandRates, bandTimeRanges, tMin, tRange]);

  // maxY for server line scaling
  const maxY = useMemo(() => {
    const sMax = serverPts.length > 0 ? Math.max(...serverPts.map(p => p.v)) : 0;
    return Math.max(sMax, maxStack, 1);
  }, [serverPts, maxStack]);

  // Y scale: map raw cumulative value → world Y coordinate
  const yScale = maxStack > 0 ? CEILING_Y / maxStack : 1;

  // Per-band scaled top/bot curves
  const bandCurves = useMemo(() => {
    if (cumStacks.length === 0) return [];
    return bands.map((_, bi) => {
      const topY = cumStacks[bi].map(v => v * yScale);
      const botY = bi > 0 ? cumStacks[bi - 1].map(v => v * yScale) : cumStacks[bi].map(() => 0);
      return { topY, botY };
    });
  }, [cumStacks, bands, yScale]);

  // Memory range for Z-axis
  const { memMin, memMax } = useMemo(() => {
    const mems = bands.map(b => b.peakMemory).filter(m => m > 0);
    if (mems.length === 0) return { memMin: 0, memMax: 1 };
    return { memMin: Math.min(...mems), memMax: Math.max(...mems) };
  }, [bands]);

  const memToDepth = useMemo(() => {
    const logMin = memMin > 0 ? Math.log10(memMin) : 0;
    const logMax = memMax > 0 ? Math.log10(memMax) : 1;
    const logRange = logMax - logMin || 1;
    return (mem: number) => {
      if (mem <= 0) return MIN_Z_DEPTH;
      const frac = (Math.log10(mem) - logMin) / logRange;
      return MIN_Z_DEPTH + frac * (MAX_Z_DEPTH - MIN_Z_DEPTH);
    };
  }, [memMin, memMax]);

  // Tooltip position
  const tooltipPos = useMemo((): [number, number, number] | null => {
    if (hoveredBand === null || bandCurves.length === 0) return null;
    const curves = bandCurves[hoveredBand];
    if (!curves) return null;
    // Find the midpoint of the band's active region
    let sumX = 0, sumY = 0, count = 0;
    for (let i = 0; i < xPositions.length; i++) {
      if (curves.topY[i] - curves.botY[i] > 0.001) {
        sumX += xPositions[i];
        sumY += curves.topY[i];
        count++;
      }
    }
    if (count === 0) return null;
    const depth = memToDepth(bands[hoveredBand].peakMemory);
    return [sumX / count, sumY / count + 1.2, depth / 2];
  }, [hoveredBand, bandCurves, xPositions, bands, memToDepth]);

  const handleBandHover = useCallback((idx: number | null) => setHoveredBand(idx), []);

  // Sync hovered band to parent for table row highlighting
  useEffect(() => {
    if (!onHighlightItem) return;
    onHighlightItem(hoveredBand !== null && bands[hoveredBand] ? { type: bands[hoveredBand].type, idx: bands[hoveredBand].idx, id: bands[hoveredBand].id } : null);
  }, [hoveredBand]); // eslint-disable-line react-hooks/exhaustive-deps

  return (
    <div style={{ width: '100%', height, position: 'relative' }}>
      <Canvas
        shadows
        dpr={[1, 2]}
        gl={{ antialias: true, powerPreference: 'high-performance' }}
        frameloop="demand"
      >
        <color attach="background" args={[bg3d]} />
        <CameraSetup />

        <OrbitControls
          makeDefault enablePan enableZoom enableRotate enableDamping
          dampingFactor={0.05}
          maxPolarAngle={Math.PI * 0.85}
          minPolarAngle={Math.PI * 0.05}
          target={[RUNWAY_LENGTH / 2, CEILING_Y / 3, MAX_Z_DEPTH / 2]}
        />

        {/* Lighting */}
        <ambientLight intensity={0.35} />
        <directionalLight position={[10, 15, 10]} intensity={0.9} />
        <directionalLight position={[-10, 10, -10]} intensity={0.35} color="#a78bfa" />
        <pointLight position={[RUNWAY_LENGTH / 2, CEILING_Y * 1.5, MAX_Z_DEPTH / 2]} intensity={0.4} color="#60a5fa" />

        <FloorGrid />
        <CeilingPlane />

        {/* Fluid band ribbons */}
        {bandCurves.map((curves, bi) => (
          <BandRibbon
            key={`${bands[bi].type}-${bands[bi].idx}`}
            xPositions={xPositions}
            topY={curves.topY}
            botY={curves.botY}
            zDepth={memToDepth(bands[bi].peakMemory)}
            color={bands[bi].color}
            hovered={hoveredBand === bi}
            bandIdx={bi}
            onHover={handleBandHover}
            onClick={onBandClick ? () => onBandClick({ type: bands[bi].type, idx: bands[bi].idx }) : undefined}
          />
        ))}

        {/* Server metric line */}
        <ServerMetricLine
          serverPts={serverPts}
          tMin={tMin} tRange={tRange} maxY={maxY}
          color={METRIC_CONFIG[metricMode].color}
        />

        {/* Labels & axes */}
        <TimeLabels tMin={tMin} tMax={tMax} />
        <YAxisMarkers maxStack={maxStack} metricMode={metricMode} />
        <ZAxisLabels memMin={memMin} memMax={memMax} memToDepth={memToDepth} />
        <AxisLabels metricMode={metricMode} />
        <Legend
          hasQueries={data.queries.length > 0 && !(hiddenCategories?.has('query'))}
          hasMerges={data.merges.length > 0 && !(hiddenCategories?.has('merge'))}
          hasMutations={(data.mutations ?? []).length > 0 && !(hiddenCategories?.has('mutation'))}
        />

        {/* Tooltip */}
        {hoveredBand !== null && tooltipPos && (
          <HoverTooltip band={bands[hoveredBand] ?? null} position={tooltipPos} />
        )}
      </Canvas>
    </div>
  );
};
