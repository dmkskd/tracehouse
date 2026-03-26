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
import { Canvas, useThree } from '@react-three/fiber';
import { OrbitControls, Html } from '@react-three/drei';
import * as THREE from 'three';
import type { ResourceLanesData } from '@tracehouse/core';
import { processLanesData, STRESS_COMPONENTS } from '@tracehouse/core';
import type { ViewMode, StressScale, ProcessedLanes, LaneResourceBreakdown } from '@tracehouse/core';

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

/** Presentation metadata for stress components — colors and labels for the UI */
const STRESS_COMPONENT_DISPLAY = [
  { label: 'CPU', color: '#ef4444' },
  { label: 'Mem', color: '#3b82f6' },
  { label: 'IO', color: '#22c55e' },
  { label: 'Marks', color: '#f59e0b' },
] as const;

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

// Data processing (aggregation, normalization, ranking) lives in
// packages/core/src/utils/resource-lanes-processor.ts — imported as processLanesData

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
  meshRef: React.Ref<THREE.Mesh>;
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
      {/* Time labels along X — gradient from dim (oldest) to bright (newest) */}
      {timeIndices.map((ti, idx) => {
        const x = (ti / (nTime - 1) - 0.5) * SCENE_WIDTH;
        const frac = timeIndices.length > 1 ? idx / (timeIndices.length - 1) : 1;
        // Dim grey → bright white
        const r = Math.round(100 + frac * 128);
        const g = Math.round(116 + frac * 112);
        const b = Math.round(139 + frac * 96);
        return (
          <Html key={`t-${ti}`} position={[x, -0.15, SCENE_DEPTH / 2 + 1.2]} center style={{ pointerEvents: 'none' }}>
            <div style={{ fontSize: 9, color: `rgb(${r},${g},${b})`, fontFamily: "'Share Tech Mono', monospace", whiteSpace: 'nowrap' }}>
              {timeLabels[ti]}
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
  level: _level,
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
          {STRESS_COMPONENTS.map((c, i) => (
            <div key={c.key} style={{ display: 'flex', alignItems: 'center', gap: 3 }}>
              <div style={{ width: 6, height: 6, borderRadius: 1, background: STRESS_COMPONENT_DISPLAY[i].color }} />
              <span style={{ fontSize: 9, color: '#64748b' }}>{STRESS_COMPONENT_DISPLAY[i].label} {Math.round(c.weight * 100)}%</span>
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
      alignItems: 'center',
    }}>
      <span style={{ fontSize: 8, color: '#475569', padding: '0 4px', fontFamily: "'Share Tech Mono', monospace" }}>Metric</span>
      {CHANNEL_OPTIONS.map((opt, i) => (
        <React.Fragment key={opt.key}>
          {i === 1 && <span style={{ width: 1, height: 12, background: 'rgba(100,116,139,0.25)', flexShrink: 0 }} />}
          <button
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
        </React.Fragment>
      ))}
    </div>
  );
}

const SCALE_OPTIONS: { key: StressScale; label: string; title: string }[] = [
  { key: 'share', label: 'Share', title: 'How much of the system is each table using right now? 10 tables at 10% each = all flat at 10%, even if system is maxed out.' },
  { key: 'load', label: 'Load', title: 'Uses the busiest minute as the ceiling. Quiet periods are flat, busy periods are tall. Example: if only 3 queries ran at 13:00 but 300 ran at 14:00, the surface at 13:00 is nearly flat. In Share mode both would look the same height.' },
  { key: 'contrast', label: 'Contrast', title: 'Stretch so the busiest lane fills the full height. Example: if the top table uses 8% and the rest use 2%, Share shows them all near the bottom. Contrast stretches 8% to full height so you can see the difference.' },
];

function StressScaleToggle({
  active,
  onChange,
}: {
  active: StressScale;
  onChange: (s: StressScale) => void;
}) {
  return (
    <div style={{
      display: 'flex', gap: 2, padding: 3, borderRadius: 8,
      background: 'var(--bg-3d-overlay)', backdropFilter: 'blur(8px)',
      border: '1px solid var(--bg-3d-overlay-border)',
      alignItems: 'center',
    }}>
      <span style={{ fontSize: 8, color: '#475569', padding: '0 4px', fontFamily: "'Share Tech Mono', monospace" }}>Scale</span>
      {SCALE_OPTIONS.map(opt => (
        <button
          key={opt.key}
          onClick={() => onChange(opt.key)}
          title={opt.title}
          style={{
            padding: '3px 6px',
            fontSize: 9,
            fontWeight: active === opt.key ? 700 : 400,
            border: 'none',
            borderRadius: 5,
            cursor: 'pointer',
            fontFamily: "'Share Tech Mono', monospace",
            background: active === opt.key ? 'rgba(88, 166, 255, 0.2)' : 'transparent',
            color: active === opt.key ? '#58a6ff' : '#64748b',
            transition: 'all 0.15s ease',
          }}
        >
          {opt.label}
        </button>
      ))}
    </div>
  );
}

function LanesToggle({ active, onChange }: { active: number; onChange: (n: number) => void }) {
  return (
    <div style={{
      display: 'flex', gap: 2, padding: 3, borderRadius: 8,
      background: 'var(--bg-3d-overlay)', backdropFilter: 'blur(8px)',
      border: '1px solid var(--bg-3d-overlay-border)',
      alignItems: 'center',
    }}>
      <span style={{ fontSize: 8, color: '#475569', padding: '0 4px', fontFamily: "'Share Tech Mono', monospace" }}>Lanes</span>
      {[5, 10, 15, 20].map(n => (
        <button
          key={n}
          onClick={() => onChange(n)}
          style={{
            padding: '3px 6px',
            fontSize: 9,
            fontWeight: active === n ? 700 : 400,
            border: 'none',
            borderRadius: 5,
            cursor: 'pointer',
            fontFamily: "'Share Tech Mono', monospace",
            background: active === n ? 'rgba(88, 166, 255, 0.2)' : 'transparent',
            color: active === n ? '#58a6ff' : '#64748b',
            transition: 'all 0.15s ease',
          }}
        >
          {n}
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

  const lanesDesc = level === 'system'
    ? `Showing top ${laneCount} tables ranked by total resource usage. ` +
      `Tables are selected from SELECT queries in system.query_log (ARRAY JOIN tables). ` +
      (viewMode === 'stress' ? stressDesc : `Each cell height = table's share of total system ${viewMode} at that minute.`) + ` ` +
      `System tables (system.*, INFORMATION_SCHEMA.*) are excluded by default.`
    : `Showing top ${laneCount} query patterns ranked by total resource usage for this table. ` +
      `Patterns are grouped by normalized_query_hash. ` +
      (viewMode === 'stress' ? stressDesc : '') + ` ` +
      `Heights use the same system-wide baseline as the overview for comparable scale.`;

  const scaleDesc = `Scale controls how tall the surface is:\n` +
    `• Share — what % of system is each table using? With 10 tables at 10% each, all lanes are flat at 10% even if the system is maxed.\n` +
    `• Load — is the system busy? Tall = heavy load, flat = idle. Best for spotting stress periods.\n` +
    `• Contrast — stretches the busiest lane to fill full height. Best for comparing lanes when differences are small.`;

  return (
    <div style={{ position: 'relative', display: 'inline-block', zIndex: 30 }}>
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
        title="How this surface works"
      >
        i
      </button>
      {open && (
        <div style={{
          position: 'absolute', top: 24, left: 0, zIndex: 30,
          width: 340, padding: '10px 12px',
          background: 'var(--bg-3d-overlay)', backdropFilter: 'blur(12px)',
          border: '1px solid var(--bg-3d-overlay-border)',
          borderRadius: 6, fontSize: 10, lineHeight: 1.5,
          color: '#94a3b8', fontFamily: "'Share Tech Mono', monospace",
          pointerEvents: 'auto', whiteSpace: 'pre-line',
        }}>
          {lanesDesc}
          {'\n\n'}
          {scaleDesc}
        </div>
      )}
    </div>
  );
}

// ─── Mini stacked bar ───

function StackedBar({ breakdown, width = 80 }: { breakdown: LaneResourceBreakdown; width?: number }) {
  const segments = [
    { frac: breakdown.cpu, color: STRESS_COMPONENT_DISPLAY[0].color },
    { frac: breakdown.memory, color: STRESS_COMPONENT_DISPLAY[1].color },
    { frac: breakdown.io, color: STRESS_COMPONENT_DISPLAY[2].color },
    { frac: breakdown.marks, color: STRESS_COMPONENT_DISPLAY[3].color },
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
  viewMode: _viewMode,
  highlightedIdx,
  onHover,
  onLeave,
  onClick,
  onOpenQuery,
}: {
  processed: ProcessedLanes;
  level: 'system' | 'table';
  viewMode: ViewMode;
  highlightedIdx: number | null;
  onHover: (idx: number) => void;
  onLeave: () => void;
  onClick: (laneId: string) => void;
  onOpenQuery?: (hash: string) => void;
}) {
  const { laneLabels, laneIds, laneAvgShare, laneBreakdowns } = processed;
  const containerRef = useRef<HTMLDivElement>(null);
  const itemRefs = useRef<(HTMLDivElement | null)[]>([]);
  const hideTimeout = useRef<ReturnType<typeof setTimeout> | null>(null);
  const isOverTooltip = useRef(false);

  const handleLaneEnter = useCallback((idx: number) => {
    if (hideTimeout.current) { clearTimeout(hideTimeout.current); hideTimeout.current = null; }
    onHover(idx);
  }, [onHover]);

  const handleLaneLeave = useCallback(() => {
    hideTimeout.current = setTimeout(() => {
      if (!isOverTooltip.current) onLeave();
    }, 150);
  }, [onLeave]);

  const handleTooltipEnter = useCallback(() => {
    isOverTooltip.current = true;
    if (hideTimeout.current) { clearTimeout(hideTimeout.current); hideTimeout.current = null; }
  }, []);

  const handleTooltipLeave = useCallback(() => {
    isOverTooltip.current = false;
    hideTimeout.current = setTimeout(() => onLeave(), 150);
  }, [onLeave]);

  // Compute tooltip position relative to the outer container
  const tooltipTop = useMemo(() => {
    if (highlightedIdx === null || !containerRef.current) return 0;
    const item = itemRefs.current[highlightedIdx];
    if (!item || !containerRef.current) return 0;
    return item.offsetTop - containerRef.current.scrollTop;
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [highlightedIdx]);

  const highlightedLabel = highlightedIdx !== null ? laneLabels[highlightedIdx] : '';
  const highlightedLaneId = highlightedIdx !== null ? laneIds[highlightedIdx] : '';
  const highlightedBd = highlightedIdx !== null ? laneBreakdowns[highlightedIdx] : null;
  const highlightedAvg = highlightedIdx !== null ? laneAvgShare[highlightedIdx] ?? 0 : 0;
  // At table level, lanes are query patterns (not __merges__) — clickable to open details
  const isQueryLane = level === 'table' && highlightedLaneId !== '__merges__';

  return (
    <div style={{ position: 'absolute', top: 48, right: 16, zIndex: 10 }}>
      {/* Tooltip — rendered outside the scrollable container */}
      {highlightedIdx !== null && highlightedBd && (
        <div
          onMouseEnter={handleTooltipEnter}
          onMouseLeave={handleTooltipLeave}
          style={{
          position: 'absolute', right: '100%', top: tooltipTop, marginRight: 8,
          background: 'var(--bg-3d-overlay)', backdropFilter: 'blur(12px)',
          border: '1px solid var(--bg-3d-overlay-border)',
          borderRadius: 6, padding: '8px 10px',
          fontSize: 9, lineHeight: 1.6, whiteSpace: 'nowrap',
          fontFamily: "'Share Tech Mono', monospace",
          color: '#94a3b8', zIndex: 20,
          minWidth: 120,
        }}>
          <div style={{ fontWeight: 600, color: '#e2e8f0', marginBottom: 4, fontSize: 10 }}>{highlightedLabel}</div>
          {isQueryLane && (
            <div style={{ fontSize: 8, color: '#475569', marginBottom: 4, wordBreak: 'break-all', whiteSpace: 'normal', maxWidth: 200 }}>
              hash: {highlightedLaneId}
            </div>
          )}
          {STRESS_COMPONENTS.map((comp, ci) => {
            const pct = comp.key === 'total_cpu_us' ? highlightedBd.cpu
              : comp.key === 'total_memory' ? highlightedBd.memory
              : comp.key === 'total_read_bytes' ? highlightedBd.io
              : highlightedBd.marks;
            return (
              <div key={comp.key} style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                <div style={{ width: 6, height: 6, borderRadius: 1, background: STRESS_COMPONENT_DISPLAY[ci].color, flexShrink: 0 }} />
                <span>{STRESS_COMPONENT_DISPLAY[ci].label}</span>
                <span style={{ marginLeft: 'auto', color: '#64748b' }}>{(pct * 100).toFixed(0)}%</span>
              </div>
            );
          })}
          <div style={{ marginTop: 4, borderTop: '1px solid rgba(100,116,139,0.2)', paddingTop: 4, color: '#64748b' }}>
            Avg stress: {(highlightedAvg * 100).toFixed(1)}%
          </div>
          {isQueryLane && onOpenQuery && (
            <button
              onClick={() => onOpenQuery(highlightedLaneId)}
              style={{
                marginTop: 6, padding: '3px 8px', fontSize: 9, width: '100%',
                color: '#58a6ff', background: 'rgba(88,166,255,0.1)',
                border: '1px solid rgba(88,166,255,0.25)', borderRadius: 4,
                cursor: 'pointer', fontFamily: "'Share Tech Mono', monospace",
              }}
            >
              View Details
            </button>
          )}
        </div>
      )}
      {/* Scrollable lane list */}
      <div
        ref={containerRef}
        style={{
          background: 'var(--bg-3d-overlay)', backdropFilter: 'blur(8px)',
          border: '1px solid var(--bg-3d-overlay-border)',
          borderRadius: 6, padding: '6px 0',
          maxHeight: 'calc(100vh - 200px)', overflowY: 'auto',
          minWidth: 180,
        }}
      >
        {laneLabels.map((label, i) => {
          const isHighlighted = highlightedIdx === i;
          const avgShare = laneAvgShare[i] ?? 0;
          const dotColor = heatColorCSS(avgShare);
          const bd = laneBreakdowns[i];

          return (
            <div
              key={laneIds[i]}
              ref={el => { itemRefs.current[i] = el; }}
              onMouseEnter={() => handleLaneEnter(i)}
              onMouseLeave={handleLaneLeave}
              onClick={() => level === 'system' && onClick(laneIds[i])}
              style={{
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
              <div style={{ paddingLeft: 12 }}>
                <StackedBar breakdown={bd} width={100} />
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

// ─── Exported component ───

export interface ResourceSurfaceProps {
  data: ResourceLanesData | null;
  isLoading: boolean;
  error: string | null;
  maxLanes: number;
  onMaxLanesChange: (n: number) => void;
  onDrillDown: (tableFullName: string) => void;
  onDrillUp: () => void;
  onOpenQuery?: (hash: string) => void;
}

export const ResourceSurface: React.FC<ResourceSurfaceProps> = ({
  data,
  isLoading,
  error,
  maxLanes,
  onMaxLanesChange,
  onDrillDown,
  onDrillUp,
  onOpenQuery,
}) => {
  const [viewMode, setViewMode] = useState<ViewMode>('stress');
  const [stressScale, setStressScale] = useState<StressScale>('load');
  const [highlightedLaneIdx, setHighlightedLaneIdx] = useState<number | null>(null);

  const processed = useMemo(() => {
    if (!data) return null;
    return processLanesData(data, viewMode, stressScale);
  }, [data, viewMode, stressScale]);

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

  return (
    <div style={{ width: '100%', height: '100%', position: 'relative', background: `linear-gradient(180deg, var(--bg-3d-from) 0%, var(--bg-3d-to) 100%)`, borderRadius: 8, overflow: 'hidden' }}>
      {/* Header bar */}
      <div style={{
        position: 'absolute', top: 0, left: 0, right: 0, zIndex: 12,
        padding: '8px 16px',
        background: 'linear-gradient(to bottom, rgba(10,15,30,0.85) 0%, rgba(10,15,30,0.4) 80%, transparent 100%)',
        display: 'flex', alignItems: 'center', gap: 8,
      }}>
        {level === 'table' && (
          <button
            onClick={onDrillUp}
            style={{
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
        <div style={{ fontSize: 13, fontWeight: 700, color: 'var(--text-3d-title)', fontFamily: "'Inter', sans-serif", whiteSpace: 'nowrap' }}>
          {level === 'system'
            ? 'System Resource Usage'
            : data.drillTable ?? 'Table Resource Usage'}
        </div>
        <AlgoInfo level={level} viewMode={viewMode} laneCount={laneCount} />
        <ViewModeSelector active={viewMode} onChange={setViewMode} />
        <div style={{ flex: 1 }} />
        <StressScaleToggle active={stressScale} onChange={setStressScale} />
        <LanesToggle active={maxLanes} onChange={onMaxLanesChange} />
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
        onOpenQuery={onOpenQuery}
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
