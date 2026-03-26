/**
 * ResourceSurface — 3D lanes-based resource usage surface with drill-down.
 *
 * Level 1 (system): Time × Tables (lanes) × composite stress
 *   Shows which tables are actually stressing the system.
 *   Height = composite stress (weighted CPU + memory + IO + marks).
 *   Color = heat intensity (same scale).
 *
 * Level 2 (table drill-down): Time × Query patterns (lanes) × same metric
 *   Shows which query patterns drive a specific table's resource usage.
 *   Same normalization baseline (system totals) so scale is comparable.
 *
 * Lane list panel shows mini stacked bars for resource breakdown per lane.
 * Hover a lane (panel or surface) to see exact resource percentages.
 */

import React, { useEffect, useMemo, useRef, useState, useCallback } from 'react';
import { Canvas, useThree, useFrame } from '@react-three/fiber';
import { OrbitControls, Html } from '@react-three/drei';
import * as THREE from 'three';
import type { ResourceLanesData, ResourceLaneRow, ResourceTotalsRow } from '@tracehouse/core';

// ─── Resource channels ───

export type ResourceChannel =
  | 'total_cpu_us'
  | 'total_memory'
  | 'total_read_bytes'
  | 'total_selected_marks'
  | 'total_io_wait_us'
  | 'total_duration_ms'
  | 'query_count'
  | 'total_read_rows';

/** The 4 primary stress components */
const STRESS_COMPONENTS = [
  { key: 'total_cpu_us' as ResourceChannel, label: 'CPU', color: '#ef4444', weight: 0.35 },
  { key: 'total_memory' as ResourceChannel, label: 'Mem', color: '#3b82f6', weight: 0.25 },
  { key: 'total_read_bytes' as ResourceChannel, label: 'IO', color: '#22c55e', weight: 0.25 },
  { key: 'total_selected_marks' as ResourceChannel, label: 'Marks', color: '#f59e0b', weight: 0.15 },
] as const;

type ViewMode = 'stress' | ResourceChannel;

const CHANNEL_OPTIONS: { key: ViewMode; label: string }[] = [
  { key: 'stress', label: 'Stress' },
  { key: 'total_cpu_us', label: 'CPU' },
  { key: 'total_memory', label: 'Memory' },
  { key: 'total_read_bytes', label: 'Read IO' },
  { key: 'total_selected_marks', label: 'Marks' },
  { key: 'total_io_wait_us', label: 'IO Wait' },
  { key: 'total_duration_ms', label: 'Duration' },
  { key: 'query_count', label: 'Queries' },
  { key: 'total_read_rows', label: 'Rows' },
];

// ─── Colorscale (cool blues for low → hot reds/magentas for high share of system) ───

const HEAT_COLORS: [number, THREE.Color][] = [
  [0.00, new THREE.Color(8 / 255, 12 / 255, 30 / 255)],
  [0.05, new THREE.Color(15 / 255, 30 / 255, 80 / 255)],
  [0.15, new THREE.Color(20 / 255, 80 / 255, 140 / 255)],
  [0.30, new THREE.Color(30 / 255, 150 / 255, 120 / 255)],
  [0.45, new THREE.Color(100 / 255, 200 / 255, 60 / 255)],
  [0.60, new THREE.Color(220 / 255, 200 / 255, 30 / 255)],
  [0.75, new THREE.Color(240 / 255, 120 / 255, 20 / 255)],
  [0.90, new THREE.Color(220 / 255, 40 / 255, 25 / 255)],
  [1.00, new THREE.Color(180 / 255, 10 / 255, 60 / 255)],
];

function heatColor(t: number): THREE.Color {
  const clamped = Math.max(0, Math.min(1, t));
  for (let i = 0; i < HEAT_COLORS.length - 1; i++) {
    const [t0, c0] = HEAT_COLORS[i];
    const [t1, c1] = HEAT_COLORS[i + 1];
    if (clamped >= t0 && clamped <= t1) {
      const frac = (clamped - t0) / (t1 - t0);
      return new THREE.Color().lerpColors(c0, c1, frac);
    }
  }
  return HEAT_COLORS[HEAT_COLORS.length - 1][1];
}

function heatColorCSS(t: number): string {
  const c = heatColor(t);
  return `rgb(${Math.round(c.r * 255)}, ${Math.round(c.g * 255)}, ${Math.round(c.b * 255)})`;
}

// ─── Scene constants ───

const SCENE_WIDTH = 10;
const SCENE_DEPTH = 8;
const MAX_HEIGHT = 3.5;

// ─── Data processing ───

/** Per-lane resource breakdown (shares of system for each component) */
interface LaneResourceBreakdown {
  cpu: number;
  memory: number;
  io: number;
  marks: number;
}

interface ProcessedLanes {
  Z: number[][];
  Zraw: number[][];
  laneLabels: string[];
  laneIds: string[];
  timeLabels: string[];
  systemPeak: number;
  laneAvgShare: number[];
  /** Per-lane resource breakdown for stacked bars */
  laneBreakdowns: LaneResourceBreakdown[];
}

/** Build per-channel system totals map: ts → value */
function buildSystemTotals(totals: ResourceTotalsRow[], channel: ResourceChannel): Map<string, number> {
  const m = new Map<string, number>();
  for (const t of totals) {
    m.set(t.ts, Number((t as Record<string, unknown>)[channel] ?? 0));
  }
  return m;
}

/** Compute a single normalized share: lane_value / system_total */
function computeShare(laneVal: number, sysTotal: number): number {
  return sysTotal > 0 ? Math.min(1, laneVal / sysTotal) : 0;
}

function processLanesData(
  data: ResourceLanesData,
  viewMode: ViewMode,
): ProcessedLanes | null {
  const { lanes, totals } = data;
  if (lanes.length === 0 || totals.length === 0) return null;

  const times = totals.map(r => r.ts).sort();
  if (times.length < 2) return null;
  const timeIdx = new Map(times.map((t, i) => [t, i]));
  const nTime = times.length;

  // Build system totals for each stress component
  const sysTotalsByChannel = new Map<ResourceChannel, Map<string, number>>();
  for (const comp of STRESS_COMPONENTS) {
    sysTotalsByChannel.set(comp.key, buildSystemTotals(totals, comp.key));
  }

  // For single-channel mode, also build that channel's totals
  const isStressMode = viewMode === 'stress';
  let singleChannelTotals: Map<string, number> | null = null;
  if (!isStressMode) {
    singleChannelTotals = buildSystemTotals(totals, viewMode as ResourceChannel);
  }

  // Rank lanes by composite stress (always rank by stress, regardless of view mode)
  const laneAgg = new Map<string, { stressTotal: number; label: string; channelTotals: Record<string, number> }>();
  for (const r of lanes) {
    const existing = laneAgg.get(r.lane_id);
    const rec = r as Record<string, unknown>;
    const cpuVal = Number(rec['total_cpu_us'] ?? 0);
    const memVal = Number(rec['total_memory'] ?? 0);
    const ioVal = Number(rec['total_read_bytes'] ?? 0);
    const marksVal = Number(rec['total_selected_marks'] ?? 0);

    if (existing) {
      existing.stressTotal += cpuVal + memVal + ioVal + marksVal; // rough proxy for ranking
      existing.channelTotals['total_cpu_us'] += cpuVal;
      existing.channelTotals['total_memory'] += memVal;
      existing.channelTotals['total_read_bytes'] += ioVal;
      existing.channelTotals['total_selected_marks'] += marksVal;
    } else {
      laneAgg.set(r.lane_id, {
        stressTotal: cpuVal + memVal + ioVal + marksVal,
        label: r.lane_label,
        channelTotals: {
          'total_cpu_us': cpuVal,
          'total_memory': memVal,
          'total_read_bytes': ioVal,
          'total_selected_marks': marksVal,
        },
      });
    }
  }

  const rankedLanes = [...laneAgg.entries()].sort((a, b) => b[1].stressTotal - a[1].stressTotal);
  if (rankedLanes.length === 0) return null;

  const laneIds = rankedLanes.map(([id]) => id);
  const laneLabels = rankedLanes.map(([, info]) => {
    const label = info.label;
    if (label.includes('.')) {
      const parts = label.split('.');
      return parts.length > 1 ? parts[parts.length - 1] : label;
    }
    return label.length > 40 ? label.slice(0, 40) + '…' : label;
  });
  const laneIdxMap = new Map(laneIds.map((id, i) => [id, i]));

  const nLanes = laneIds.length;

  // Build per-lane resource breakdown (average shares across time for each component)
  // Used for stacked bars in the lane list
  const laneBreakdowns: LaneResourceBreakdown[] = rankedLanes.map(([, info]) => {
    const totals = info.channelTotals;
    const sum = (totals['total_cpu_us'] || 0) + (totals['total_memory'] || 0) +
      (totals['total_read_bytes'] || 0) + (totals['total_selected_marks'] || 0);
    if (sum === 0) return { cpu: 0, memory: 0, io: 0, marks: 0 };
    return {
      cpu: (totals['total_cpu_us'] || 0) / sum,
      memory: (totals['total_memory'] || 0) / sum,
      io: (totals['total_read_bytes'] || 0) / sum,
      marks: (totals['total_selected_marks'] || 0) / sum,
    };
  });

  // Build grid
  if (isStressMode) {
    // For stress mode, build per-component grids, normalize each, then combine
    const componentGrids = STRESS_COMPONENTS.map(comp => {
      const sysTotals = sysTotalsByChannel.get(comp.key)!;
      const grid: number[][] = Array.from({ length: nLanes }, () => new Array(nTime).fill(0));
      for (const r of lanes) {
        const li = laneIdxMap.get(r.lane_id);
        const ti = timeIdx.get(r.ts);
        if (li !== undefined && ti !== undefined) {
          const raw = Number((r as Record<string, unknown>)[comp.key] ?? 0);
          const sysTotal = sysTotals.get(times[ti]) ?? 1;
          grid[li][ti] = computeShare(raw, sysTotal);
        }
      }
      return { grid, weight: comp.weight };
    });

    // Combine into composite stress
    const Z: number[][] = Array.from({ length: nLanes }, (_, li) =>
      Array.from({ length: nTime }, (_, ti) => {
        let stress = 0;
        for (const { grid, weight } of componentGrids) {
          stress += grid[li][ti] * weight;
        }
        return Math.min(1, stress);
      }),
    );

    // Zraw = composite (same as Z for stress mode, raw doesn't apply)
    const Zraw = Z;

    const systemPeak = 1;
    const laneAvgShare = Z.map(row => {
      const nonZero = row.filter(v => v > 0);
      return nonZero.length > 0 ? nonZero.reduce((a, b) => a + b, 0) / nonZero.length : 0;
    });

    const timeLabels = times.map(t => {
      const match = t.match(/(\d{2}:\d{2})/);
      return match ? match[1] : t.slice(11, 16);
    });

    return { Z, Zraw, laneLabels, laneIds, timeLabels, systemPeak, laneAvgShare, laneBreakdowns };
  } else {
    // Single channel mode (same as before)
    const channel = viewMode as ResourceChannel;
    const Zraw: number[][] = Array.from({ length: nLanes }, () => new Array(nTime).fill(0));
    for (const r of lanes) {
      const li = laneIdxMap.get(r.lane_id);
      const ti = timeIdx.get(r.ts);
      if (li !== undefined && ti !== undefined) {
        Zraw[li][ti] = Number((r as Record<string, unknown>)[channel] ?? 0);
      }
    }

    const Z: number[][] = Zraw.map(row =>
      row.map((val, ti) => {
        const sysTotal = singleChannelTotals!.get(times[ti]) ?? 1;
        return computeShare(val, sysTotal);
      }),
    );

    const systemPeak = Math.max(...[...(singleChannelTotals?.values() ?? [])], 1);
    const laneAvgShare = Z.map(row => {
      const nonZero = row.filter(v => v > 0);
      return nonZero.length > 0 ? nonZero.reduce((a, b) => a + b, 0) / nonZero.length : 0;
    });

    const timeLabels = times.map(t => {
      const match = t.match(/(\d{2}:\d{2})/);
      return match ? match[1] : t.slice(11, 16);
    });

    return { Z, Zraw, laneLabels, laneIds, timeLabels, systemPeak, laneAvgShare, laneBreakdowns };
  }
}

// ─── Three.js components ───

function LanesMesh({
  processed,
  onLaneClick,
  meshRef,
  highlightedLaneIdx,
  onLaneHover,
}: {
  processed: ProcessedLanes;
  onLaneClick: (laneId: string) => void;
  meshRef: React.RefObject<THREE.Mesh | null>;
  highlightedLaneIdx: number | null;
  onLaneHover: (idx: number | null) => void;
}) {
  const { Z, laneLabels, laneIds, timeLabels } = processed;
  const nLanes = laneLabels.length;
  const nTime = timeLabels.length;

  const geometry = useMemo(() => {
    const geo = new THREE.PlaneGeometry(SCENE_WIDTH, SCENE_DEPTH, nTime - 1, nLanes - 1);
    const positions = geo.attributes.position;
    const colors = new Float32Array(positions.count * 3);

    for (let i = 0; i < positions.count; i++) {
      const ix = i % nTime;
      const iy = Math.floor(i / nTime);
      const share = Z[iy]?.[ix] ?? 0;
      const x = (ix / (nTime - 1) - 0.5) * SCENE_WIDTH;
      const z = (iy / Math.max(nLanes - 1, 1) - 0.5) * SCENE_DEPTH;
      const y = share * MAX_HEIGHT;

      positions.setXYZ(i, x, y, z);

      const isHighlighted = highlightedLaneIdx === null || iy === highlightedLaneIdx;
      const color = heatColor(share);
      const dim = isHighlighted ? 1.0 : 0.25;
      colors[i * 3] = color.r * dim;
      colors[i * 3 + 1] = color.g * dim;
      colors[i * 3 + 2] = color.b * dim;
    }

    geo.setAttribute('color', new THREE.BufferAttribute(colors, 3));
    geo.computeVertexNormals();
    return geo;
  }, [Z, nLanes, nTime, highlightedLaneIdx]);

  const pointToLaneIdx = useCallback((point: THREE.Vector3) => {
    const laneFrac = (point.z / SCENE_DEPTH) + 0.5;
    const idx = Math.round(laneFrac * Math.max(nLanes - 1, 0));
    return (idx >= 0 && idx < nLanes) ? idx : null;
  }, [nLanes]);

  const handlePointerMove = useCallback((e: any) => {
    e.stopPropagation();
    onLaneHover(pointToLaneIdx(e.point));
  }, [pointToLaneIdx, onLaneHover]);

  const handlePointerLeave = useCallback(() => {
    onLaneHover(null);
  }, [onLaneHover]);

  const handleClick = useCallback((e: any) => {
    e.stopPropagation();
    const idx = pointToLaneIdx(e.point);
    if (idx !== null) {
      onLaneClick(laneIds[idx]);
    }
  }, [pointToLaneIdx, laneIds, onLaneClick]);

  return (
    <mesh
      ref={meshRef}
      geometry={geometry}
      onClick={handleClick}
      onPointerMove={handlePointerMove}
      onPointerLeave={handlePointerLeave}
    >
      <meshStandardMaterial
        vertexColors
        side={THREE.DoubleSide}
        metalness={0.1}
        roughness={0.6}
      />
    </mesh>
  );
}

function WireframeOverlay({ processed }: { processed: ProcessedLanes }) {
  const { Z, laneLabels, timeLabels } = processed;
  const nLanes = laneLabels.length;
  const nTime = timeLabels.length;

  const geometry = useMemo(() => {
    const geo = new THREE.PlaneGeometry(SCENE_WIDTH, SCENE_DEPTH, nTime - 1, nLanes - 1);
    const positions = geo.attributes.position;
    for (let i = 0; i < positions.count; i++) {
      const ix = i % nTime;
      const iy = Math.floor(i / nTime);
      const share = Z[iy]?.[ix] ?? 0;
      const x = (ix / (nTime - 1) - 0.5) * SCENE_WIDTH;
      const z = (iy / Math.max(nLanes - 1, 1) - 0.5) * SCENE_DEPTH;
      const y = share * MAX_HEIGHT + 0.01;
      positions.setXYZ(i, x, y, z);
    }
    geo.computeVertexNormals();
    return geo;
  }, [Z, nLanes, nTime]);

  return (
    <mesh geometry={geometry}>
      <meshBasicMaterial wireframe color="#ffffff" transparent opacity={0.06} />
    </mesh>
  );
}

function LaneLabels({ processed, highlightedLaneIdx }: { processed: ProcessedLanes; highlightedLaneIdx: number | null }) {
  const { timeLabels, laneLabels, laneAvgShare } = processed;
  const nLanes = laneLabels.length;
  const nTime = timeLabels.length;

  const timeStep = Math.max(1, Math.floor(nTime / 10));
  const timeIndices: number[] = [];
  for (let i = 0; i < nTime; i += timeStep) timeIndices.push(i);

  const yLevels = [
    { pct: 0, label: '0%' },
    { pct: 0.25, label: '25%' },
    { pct: 0.5, label: '50%' },
    { pct: 0.75, label: '75%' },
    { pct: 1.0, label: '100%' },
  ];

  return (
    <>
      {/* Time labels along X */}
      {timeIndices.map(i => {
        const x = (i / (nTime - 1) - 0.5) * SCENE_WIDTH;
        return (
          <Html key={`t-${i}`} position={[x, -0.15, SCENE_DEPTH / 2 + 1.2]} center style={{ pointerEvents: 'none' }}>
            <div style={{ fontSize: 9, color: '#94a3b8', fontFamily: "'Share Tech Mono', monospace", whiteSpace: 'nowrap' }}>
              {timeLabels[i]}
            </div>
          </Html>
        );
      })}

      {/* Lane labels along Z */}
      {laneLabels.map((label, i) => {
        const z = (i / Math.max(nLanes - 1, 1) - 0.5) * SCENE_DEPTH;
        const shortLabel = label.length > 20 ? label.slice(0, 20) + '…' : label;
        const isHighlighted = highlightedLaneIdx === i;
        const color = isHighlighted ? heatColorCSS(laneAvgShare[i] ?? 0) : '#64748b';
        return (
          <Html key={`l-${i}`} position={[-SCENE_WIDTH / 2 - 1.2, 0, z]} center style={{ pointerEvents: 'none' }}>
            <div style={{
              fontSize: 9, fontFamily: "'Share Tech Mono', monospace",
              whiteSpace: 'nowrap', maxWidth: 120, overflow: 'hidden', textOverflow: 'ellipsis',
              color,
              fontWeight: isHighlighted ? 700 : 400,
              transition: 'color 0.15s ease',
            }}>
              {shortLabel}
            </div>
          </Html>
        );
      })}

      {/* Y-axis: share of system */}
      {yLevels.map(({ pct, label }, i) => {
        const y = pct * MAX_HEIGHT;
        return (
          <group key={`y-${i}`}>
            <Html position={[-SCENE_WIDTH / 2 - 1.0, y, -SCENE_DEPTH / 2]} center style={{ pointerEvents: 'none' }}>
              <div style={{ fontSize: 8, color: '#64748b', fontFamily: "'Share Tech Mono', monospace", whiteSpace: 'nowrap' }}>
                {label}
              </div>
            </Html>
            <line>
              <bufferGeometry>
                <bufferAttribute
                  attach="attributes-position"
                  count={2}
                  array={new Float32Array([-SCENE_WIDTH / 2, y, -SCENE_DEPTH / 2, -SCENE_WIDTH / 2 + 0.3, y, -SCENE_DEPTH / 2])}
                  itemSize={3}
                />
              </bufferGeometry>
              <lineBasicMaterial color="#334155" />
            </line>
          </group>
        );
      })}

      <Html position={[0, -0.4, SCENE_DEPTH / 2 + 2.0]} center style={{ pointerEvents: 'none' }}>
        <div style={{ fontSize: 10, color: '#64748b', fontFamily: "'Share Tech Mono', monospace", fontWeight: 600 }}>Time</div>
      </Html>
    </>
  );
}

function SceneSetup() {
  const { camera } = useThree();
  useEffect(() => {
    if (!(camera instanceof THREE.PerspectiveCamera)) return;
    camera.position.set(-10, 6, 10);
    camera.lookAt(0, 1, 0);
    camera.updateProjectionMatrix();
  }, [camera]);
  return null;
}

function ResourceSurfaceScene({
  processed,
  level,
  onLaneClick,
  highlightedLaneIdx,
  onLaneHover,
}: {
  processed: ProcessedLanes;
  level: 'system' | 'table';
  onLaneClick: (laneId: string) => void;
  highlightedLaneIdx: number | null;
  onLaneHover: (idx: number | null) => void;
}) {
  const surfaceMeshRef = useRef<THREE.Mesh>(null);

  return (
    <>
      <ambientLight intensity={0.4} />
      <directionalLight position={[5, 8, 5]} intensity={0.8} />
      <directionalLight position={[-3, 4, -3]} intensity={0.3} color="#6366f1" />
      <pointLight position={[0, 6, 0]} intensity={0.2} color="#ff6b35" />

      <SceneSetup />
      <LanesMesh processed={processed} onLaneClick={onLaneClick} meshRef={surfaceMeshRef} highlightedLaneIdx={highlightedLaneIdx} onLaneHover={onLaneHover} />
      <WireframeOverlay processed={processed} />
      <LaneLabels processed={processed} highlightedLaneIdx={highlightedLaneIdx} />

      <gridHelper args={[16, 32, '#1e293b', '#0f172a']} position={[0, -0.01, 0]} />

      <OrbitControls
        enablePan
        enableZoom
        enableRotate
        autoRotate
        autoRotateSpeed={0.12}
        maxPolarAngle={Math.PI / 2.1}
      />
    </>
  );
}

// ─── Legend ───

function ResourceLegend({ viewMode }: { viewMode: ViewMode }) {
  const label = viewMode === 'stress' ? 'Composite Stress' : (CHANNEL_OPTIONS.find(c => c.key === viewMode)?.label ?? viewMode);
  return (
    <div style={{
      position: 'absolute', bottom: 16, left: 20, zIndex: 10,
      background: 'var(--bg-3d-overlay)', backdropFilter: 'blur(8px)',
      borderRadius: 8, padding: '10px 14px',
      border: '1px solid var(--bg-3d-overlay-border)',
      display: 'flex', flexDirection: 'column', gap: 8, fontSize: 11,
      fontFamily: "'Share Tech Mono', monospace",
    }}>
      <div style={{ display: 'flex', gap: 12, alignItems: 'center' }}>
        <span style={{ color: 'var(--text-3d-label)' }}>{label}</span>
        <div style={{
          width: 80, height: 8, borderRadius: 3,
          background: 'linear-gradient(90deg, rgb(8,12,30), rgb(20,80,140), rgb(100,200,60), rgb(240,120,20), rgb(180,10,60))',
        }} />
        <span style={{ color: 'var(--text-3d-label)' }}>0% → 100%</span>
      </div>
      {viewMode === 'stress' && (
        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
          {STRESS_COMPONENTS.map(c => (
            <div key={c.key} style={{ display: 'flex', alignItems: 'center', gap: 3 }}>
              <div style={{ width: 6, height: 6, borderRadius: 1, background: c.color }} />
              <span style={{ fontSize: 9, color: '#64748b' }}>{c.label} {Math.round(c.weight * 100)}%</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ─── View mode selector ───

function ViewModeSelector({
  active,
  onChange,
}: {
  active: ViewMode;
  onChange: (mode: ViewMode) => void;
}) {
  return (
    <div style={{
      display: 'flex', gap: 2, padding: 3, borderRadius: 8,
      background: 'var(--bg-3d-overlay)', backdropFilter: 'blur(8px)',
      border: '1px solid var(--bg-3d-overlay-border)',
    }}>
      {CHANNEL_OPTIONS.map(opt => (
        <button
          key={opt.key}
          onClick={() => onChange(opt.key)}
          style={{
            padding: '3px 8px',
            fontSize: 9,
            fontWeight: active === opt.key ? 700 : 400,
            border: 'none',
            borderRadius: 5,
            cursor: 'pointer',
            fontFamily: "'Share Tech Mono', monospace",
            background: active === opt.key
              ? opt.key === 'stress' ? 'rgba(239, 68, 68, 0.15)' : 'rgba(88, 166, 255, 0.2)'
              : 'transparent',
            color: active === opt.key
              ? opt.key === 'stress' ? '#f87171' : '#58a6ff'
              : '#64748b',
            transition: 'all 0.15s ease',
          }}
        >
          {opt.label}
        </button>
      ))}
    </div>
  );
}

// ─── Info tooltip ───

function AlgoInfo({ level, viewMode, laneCount }: { level: 'system' | 'table'; viewMode: ViewMode; laneCount: number }) {
  const [open, setOpen] = useState(false);

  const stressDesc = `Composite stress = weighted combination of CPU (35%), Memory (25%), IO (25%), Marks (15%). ` +
    `Each component is normalized to its share of system-wide total at that minute, then combined.`;

  const description = level === 'system'
    ? `Showing top ${laneCount} tables ranked by total resource usage. ` +
      `Tables are selected from SELECT queries in system.query_log (ARRAY JOIN tables). ` +
      (viewMode === 'stress' ? stressDesc : `Each cell height = table's share of total system ${viewMode} at that minute.`) + ` ` +
      `System tables (system.*, INFORMATION_SCHEMA.*) are excluded by default.`
    : `Showing top ${laneCount} query patterns ranked by total resource usage for this table. ` +
      `Patterns are grouped by normalized_query_hash. ` +
      (viewMode === 'stress' ? stressDesc : '') + ` ` +
      `Heights use the same system-wide baseline as the overview for comparable scale.`;

  return (
    <div style={{ position: 'relative', display: 'inline-block' }}>
      <button
        onClick={() => setOpen(o => !o)}
        style={{
          pointerEvents: 'auto',
          width: 18, height: 18, borderRadius: '50%',
          border: '1px solid var(--bg-3d-overlay-border)',
          background: 'var(--bg-3d-overlay)',
          color: '#64748b', fontSize: 11, cursor: 'pointer',
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          fontFamily: "'Inter', sans-serif", fontWeight: 600,
        }}
        title="How lanes are selected"
      >
        i
      </button>
      {open && (
        <div style={{
          position: 'absolute', top: 24, left: 0, zIndex: 20,
          width: 320, padding: '10px 12px',
          background: 'var(--bg-3d-overlay)', backdropFilter: 'blur(12px)',
          border: '1px solid var(--bg-3d-overlay-border)',
          borderRadius: 6, fontSize: 10, lineHeight: 1.5,
          color: '#94a3b8', fontFamily: "'Share Tech Mono', monospace",
          pointerEvents: 'auto',
        }}>
          {description}
        </div>
      )}
    </div>
  );
}

// ─── Mini stacked bar ───

function StackedBar({ breakdown, width = 80 }: { breakdown: LaneResourceBreakdown; width?: number }) {
  const segments = [
    { frac: breakdown.cpu, color: STRESS_COMPONENTS[0].color },
    { frac: breakdown.memory, color: STRESS_COMPONENTS[1].color },
    { frac: breakdown.io, color: STRESS_COMPONENTS[2].color },
    { frac: breakdown.marks, color: STRESS_COMPONENTS[3].color },
  ];

  return (
    <div style={{ display: 'flex', width, height: 4, borderRadius: 2, overflow: 'hidden', flexShrink: 0 }}>
      {segments.map((seg, i) => (
        seg.frac > 0.01 ? (
          <div
            key={i}
            style={{
              width: `${seg.frac * 100}%`,
              background: seg.color,
              minWidth: seg.frac > 0.05 ? 2 : 0,
            }}
          />
        ) : null
      ))}
    </div>
  );
}

// ─── Lane list panel with stacked bars + hover tooltip ───

function LaneListPanel({
  processed,
  level,
  viewMode,
  highlightedIdx,
  onHover,
  onLeave,
  onClick,
}: {
  processed: ProcessedLanes;
  level: 'system' | 'table';
  viewMode: ViewMode;
  highlightedIdx: number | null;
  onHover: (idx: number) => void;
  onLeave: () => void;
  onClick: (laneId: string) => void;
}) {
  const { laneLabels, laneIds, laneAvgShare, laneBreakdowns } = processed;

  return (
    <div style={{
      position: 'absolute', top: 50, right: 16, zIndex: 10,
      background: 'var(--bg-3d-overlay)', backdropFilter: 'blur(8px)',
      border: '1px solid var(--bg-3d-overlay-border)',
      borderRadius: 6, padding: '6px 0',
      maxHeight: 'calc(100% - 100px)', overflowY: 'auto',
      minWidth: 180,
    }}>
      {laneLabels.map((label, i) => {
        const isHighlighted = highlightedIdx === i;
        const avgShare = laneAvgShare[i] ?? 0;
        const dotColor = heatColorCSS(avgShare);
        const bd = laneBreakdowns[i];

        return (
          <div
            key={laneIds[i]}
            onMouseEnter={() => onHover(i)}
            onMouseLeave={onLeave}
            onClick={() => level === 'system' && onClick(laneIds[i])}
            style={{
              position: 'relative',
              display: 'flex', flexDirection: 'column', gap: 2,
              padding: '4px 10px',
              cursor: level === 'system' ? 'pointer' : 'default',
              background: isHighlighted ? 'rgba(88, 166, 255, 0.1)' : 'transparent',
              transition: 'background 0.1s ease',
            }}
          >
            <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
              <div style={{
                width: 6, height: 6, borderRadius: '50%', flexShrink: 0,
                background: dotColor,
                boxShadow: isHighlighted ? `0 0 6px ${dotColor}` : 'none',
              }} />
              <span
                style={{
                  fontSize: 10, whiteSpace: 'nowrap',
                  overflow: 'hidden', textOverflow: 'ellipsis', maxWidth: 130,
                  fontFamily: "'Share Tech Mono', monospace",
                  color: isHighlighted ? '#e2e8f0' : '#94a3b8',
                  fontWeight: isHighlighted ? 600 : 400,
                  transition: 'color 0.1s ease',
                }}
                title={label}
              >
                {label}
              </span>
              <span style={{
                fontSize: 8, color: '#475569', marginLeft: 'auto', flexShrink: 0,
                fontFamily: "'Share Tech Mono', monospace",
              }}>
                {(avgShare * 100).toFixed(0)}%
              </span>
            </div>
            {/* Mini stacked bar */}
            <div style={{ paddingLeft: 12 }}>
              <StackedBar breakdown={bd} width={100} />
            </div>
            {/* Hover tooltip — resource breakdown */}
            {isHighlighted && (
              <div style={{
                position: 'absolute', right: '100%', top: 0, marginRight: 8,
                background: 'var(--bg-3d-overlay)', backdropFilter: 'blur(12px)',
                border: '1px solid var(--bg-3d-overlay-border)',
                borderRadius: 6, padding: '8px 10px',
                fontSize: 9, lineHeight: 1.6, whiteSpace: 'nowrap',
                fontFamily: "'Share Tech Mono', monospace",
                color: '#94a3b8', zIndex: 20,
                minWidth: 120,
              }}>
                <div style={{ fontWeight: 600, color: '#e2e8f0', marginBottom: 4, fontSize: 10 }}>{label}</div>
                {STRESS_COMPONENTS.map(comp => {
                  const pct = comp.key === 'total_cpu_us' ? bd.cpu
                    : comp.key === 'total_memory' ? bd.memory
                    : comp.key === 'total_read_bytes' ? bd.io
                    : bd.marks;
                  return (
                    <div key={comp.key} style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                      <div style={{ width: 6, height: 6, borderRadius: 1, background: comp.color, flexShrink: 0 }} />
                      <span>{comp.label}</span>
                      <span style={{ marginLeft: 'auto', color: '#64748b' }}>{(pct * 100).toFixed(0)}%</span>
                    </div>
                  );
                })}
                <div style={{ marginTop: 4, borderTop: '1px solid rgba(100,116,139,0.2)', paddingTop: 4, color: '#64748b' }}>
                  Avg stress: {(avgShare * 100).toFixed(1)}%
                </div>
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

// ─── Exported component ───

export interface ResourceSurfaceProps {
  data: ResourceLanesData | null;
  isLoading: boolean;
  error: string | null;
  onDrillDown: (tableFullName: string) => void;
  onDrillUp: () => void;
}

export const ResourceSurface: React.FC<ResourceSurfaceProps> = ({
  data,
  isLoading,
  error,
  onDrillDown,
  onDrillUp,
}) => {
  const [viewMode, setViewMode] = useState<ViewMode>('stress');
  const [highlightedLaneIdx, setHighlightedLaneIdx] = useState<number | null>(null);

  const processed = useMemo(() => {
    if (!data) return null;
    return processLanesData(data, viewMode);
  }, [data, viewMode]);

  if (error) {
    return (
      <div style={{ padding: 24, color: '#f85149', fontSize: 13 }}>
        Failed to load resource surface: {error}
      </div>
    );
  }

  if (isLoading) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%', color: 'var(--text-muted)', fontSize: 13 }}>
        Loading resource data…
      </div>
    );
  }

  if (!data || !processed) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%', color: 'var(--text-muted)', fontSize: 13 }}>
        No query activity found in this time range.
      </div>
    );
  }

  const level = data.level;
  const handleLaneClick = (laneId: string) => {
    if (level === 'system') {
      onDrillDown(laneId);
    }
  };

  const laneCount = processed.laneLabels.length;
  const modeLabel = viewMode === 'stress' ? 'Stress' : (CHANNEL_OPTIONS.find(c => c.key === viewMode)?.label ?? '');

  return (
    <div style={{ width: '100%', height: '100%', position: 'relative', background: `linear-gradient(180deg, var(--bg-3d-from) 0%, var(--bg-3d-to) 100%)`, borderRadius: 8, overflow: 'hidden' }}>
      {/* Header */}
      <div style={{
        position: 'absolute', top: 12, left: 16, zIndex: 10,
        display: 'flex', alignItems: 'flex-start', gap: 8,
      }}>
        <div style={{ pointerEvents: 'none' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            {level === 'table' && (
              <button
                onClick={onDrillUp}
                style={{
                  pointerEvents: 'auto',
                  display: 'inline-flex', alignItems: 'center', gap: 4,
                  padding: '3px 8px', fontSize: 10,
                  color: '#58a6ff', background: 'rgba(88,166,255,0.1)',
                  border: '1px solid rgba(88,166,255,0.25)', borderRadius: 4,
                  cursor: 'pointer', fontFamily: "'Share Tech Mono', monospace",
                }}
              >
                ← All Tables
              </button>
            )}
            <div style={{ fontSize: 14, fontWeight: 700, color: 'var(--text-3d-title)', fontFamily: "'Inter', sans-serif" }}>
              {level === 'system'
                ? 'System Resource Usage'
                : data.drillTable ?? 'Table Resource Usage'}
            </div>
            <AlgoInfo level={level} viewMode={viewMode} laneCount={laneCount} />
          </div>
          <div style={{ fontSize: 11, color: 'var(--text-3d-sublabel)', marginTop: 2 }}>
            {level === 'system'
              ? `Top ${laneCount} tables by ${modeLabel} · hover to inspect · click to drill down`
              : `Top ${laneCount} query patterns by ${modeLabel}`}
          </div>
        </div>
      </div>

      {/* View mode selector — top right, above lane list */}
      <div style={{ position: 'absolute', top: 12, right: 16, zIndex: 11 }}>
        <ViewModeSelector active={viewMode} onChange={setViewMode} />
      </div>

      {/* Lane list panel — right side */}
      <LaneListPanel
        processed={processed}
        level={level}
        viewMode={viewMode}
        highlightedIdx={highlightedLaneIdx}
        onHover={setHighlightedLaneIdx}
        onLeave={() => setHighlightedLaneIdx(null)}
        onClick={handleLaneClick}
      />

      <ResourceLegend viewMode={viewMode} />

      <Canvas camera={{ fov: 50 }} style={{ width: '100%', height: '100%' }}>
        <ResourceSurfaceScene
          processed={processed}
          level={level}
          onLaneClick={handleLaneClick}
          highlightedLaneIdx={highlightedLaneIdx}
          onLaneHover={setHighlightedLaneIdx}
        />
      </Canvas>
    </div>
  );
};
