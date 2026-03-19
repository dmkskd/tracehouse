/**
 * StressSurface — 3D surface: Time × Resource dimension × normalized stress.
 *
 * Port of the Python table_stress.py prototype to Three.js.
 * The surface warps and heats up where load concentrates.
 * Insert diamonds sit below, merge markers along the edge.
 */

import React, { useEffect, useMemo } from 'react';
import { Canvas, useThree } from '@react-three/fiber';
import { OrbitControls, Html } from '@react-three/drei';
import * as THREE from 'three';
import type { StressSurfaceData, StressSurfaceRow } from '@tracehouse/core';

// ─── Stress colorscale (matches Python: deep dark → blue → teal → green → yellow → orange → red → magenta) ───

const STRESS_COLORS: [number, THREE.Color][] = [
  [0.00, new THREE.Color(10 / 255, 10 / 255, 25 / 255)],
  [0.08, new THREE.Color(15 / 255, 30 / 255, 80 / 255)],
  [0.20, new THREE.Color(20 / 255, 80 / 255, 140 / 255)],
  [0.35, new THREE.Color(30 / 255, 150 / 255, 100 / 255)],
  [0.50, new THREE.Color(120 / 255, 200 / 255, 50 / 255)],
  [0.65, new THREE.Color(230 / 255, 200 / 255, 30 / 255)],
  [0.80, new THREE.Color(240 / 255, 120 / 255, 20 / 255)],
  [0.92, new THREE.Color(220 / 255, 40 / 255, 25 / 255)],
  [1.00, new THREE.Color(180 / 255, 10 / 255, 60 / 255)],
];

function stressToColor(t: number): THREE.Color {
  const clamped = Math.max(0, Math.min(1, t));
  for (let i = 0; i < STRESS_COLORS.length - 1; i++) {
    const [t0, c0] = STRESS_COLORS[i];
    const [t1, c1] = STRESS_COLORS[i + 1];
    if (clamped >= t0 && clamped <= t1) {
      const frac = (clamped - t0) / (t1 - t0);
      return new THREE.Color().lerpColors(c0, c1, frac);
    }
  }
  return STRESS_COLORS[STRESS_COLORS.length - 1][1];
}

// ─── Resource channels ───

const CHANNEL_DEFS = [
  { key: 'query_count' as const, label: 'Concurrent Load' },
  { key: 'avg_duration_ms' as const, label: 'Avg Latency' },
  { key: 'p95_duration_ms' as const, label: 'P95 Latency' },
  { key: 'total_read_rows' as const, label: 'Rows Scanned' },
  { key: 'total_read_bytes' as const, label: 'Read IO' },
  { key: 'total_memory' as const, label: 'Memory' },
  { key: 'total_cpu_us' as const, label: 'CPU Time' },
  { key: 'total_io_wait_us' as const, label: 'IO Wait' },
  { key: 'total_selected_marks' as const, label: 'Marks Touched' },
];


/** Simple 1D Gaussian blur (box approximation for speed) */
function gaussianSmooth1D(arr: number[], sigma: number): number[] {
  if (sigma <= 0) return [...arr];
  const radius = Math.ceil(sigma * 3);
  const kernel: number[] = [];
  let sum = 0;
  for (let i = -radius; i <= radius; i++) {
    const v = Math.exp(-(i * i) / (2 * sigma * sigma));
    kernel.push(v);
    sum += v;
  }
  kernel.forEach((_, i) => (kernel[i] /= sum));

  const result = new Array(arr.length).fill(0);
  for (let i = 0; i < arr.length; i++) {
    for (let k = 0; k < kernel.length; k++) {
      const j = i + k - radius;
      if (j >= 0 && j < arr.length) {
        result[i] += arr[j] * kernel[k];
      }
    }
  }
  return result;
}

/** 2D Gaussian smoothing on a row-major grid[nRows][nCols] */
function gaussianSmooth2D(grid: number[][], sigmaRow: number, sigmaCol: number): number[][] {
  const nRows = grid.length;
  if (nRows === 0) return grid;
  const nCols = grid[0].length;

  // Smooth along columns (within each row)
  let smoothed = grid.map(row => gaussianSmooth1D(row, sigmaCol));

  // Smooth along rows (within each column)
  for (let c = 0; c < nCols; c++) {
    const col = smoothed.map(row => row[c]);
    const smoothedCol = gaussianSmooth1D(col, sigmaRow);
    for (let r = 0; r < nRows; r++) {
      smoothed[r][c] = smoothedCol[r];
    }
  }
  return smoothed;
}

interface ProcessedSurface {
  /** Z values: [channelIdx][timeIdx], normalized 0..1 after smoothing */
  Z: number[][];
  /** Raw values before normalization (for hover text) */
  Zraw: number[][];
  /** Channel labels that have data */
  channels: string[];
  /** Time labels (HH:MM) */
  timeLabels: string[];
}

function processSurfaceData(rows: StressSurfaceRow[]): ProcessedSurface | null {
  if (rows.length === 0) return null;

  // Filter channels that have nonzero data
  const activeChannels = CHANNEL_DEFS.filter(ch => {
    return rows.some(r => (r[ch.key] as number) > 0);
  });
  if (activeChannels.length === 0) return null;



  // Build raw grid: [channel][time]
  const Zraw: number[][] = activeChannels.map(ch =>
    rows.map(r => Number(r[ch.key]) || 0),
  );

  // Normalize each channel to [0, 1]
  const Znorm: number[][] = Zraw.map(row => {
    const peak = Math.max(...row);
    return peak > 0 ? row.map(v => v / peak) : row.map(() => 0);
  });

  // Gaussian smooth: more across time (sigma=1.2) than channels (sigma=0.6)
  const Zsmooth = gaussianSmooth2D(Znorm, 0.6, 1.2);

  // Re-normalize after smoothing
  const Z = Zsmooth.map(row => {
    const peak = Math.max(...row);
    return peak > 0 ? row.map(v => v / peak) : row;
  });

  const timeLabels = rows.map(r => {
    const s = String(r.ts);
    // Extract HH:MM from timestamp
    const match = s.match(/(\d{2}:\d{2})/);
    return match ? match[1] : s.slice(11, 16);
  });

  return {
    Z,
    Zraw,
    channels: activeChannels.map(ch => ch.label),
    timeLabels,
  };
}

// ─── Three.js scene ───

function SurfaceMesh({ surface }: { surface: ProcessedSurface }) {
  const { channels, timeLabels, Z } = surface;
  const nCh = channels.length;
  const nTime = timeLabels.length;

  // Scene dimensions
  const sceneWidth = 10;  // X axis (time)
  const sceneDepth = 6;   // Z axis (channels)
  const maxHeight = 3;    // Y axis (stress)

  const geometry = useMemo(() => {
    const geo = new THREE.PlaneGeometry(sceneWidth, sceneDepth, nTime - 1, nCh - 1);
    const positions = geo.attributes.position;
    const colors = new Float32Array(positions.count * 3);

    for (let i = 0; i < positions.count; i++) {
      // PlaneGeometry vertices: row-major, width segments along X, height segments along Y
      // But PlaneGeometry is in XY plane, we need to rotate to XZ plane
      const ix = i % nTime;  // time index
      const iy = Math.floor(i / nTime);  // channel index

      const stress = Z[iy]?.[ix] ?? 0;
      const x = (ix / (nTime - 1) - 0.5) * sceneWidth;
      const z = (iy / (nCh - 1) - 0.5) * sceneDepth;
      const y = stress * maxHeight;

      positions.setXYZ(i, x, y, z);

      const color = stressToColor(stress);
      colors[i * 3] = color.r;
      colors[i * 3 + 1] = color.g;
      colors[i * 3 + 2] = color.b;
    }

    geo.setAttribute('color', new THREE.BufferAttribute(colors, 3));
    geo.computeVertexNormals();
    return geo;
  }, [Z, nCh, nTime, sceneWidth, sceneDepth, maxHeight]);

  return (
    <mesh geometry={geometry}>
      <meshStandardMaterial
        vertexColors
        side={THREE.DoubleSide}
        metalness={0.1}
        roughness={0.6}
        wireframe={false}
      />
    </mesh>
  );
}

function WireframeMesh({ surface }: { surface: ProcessedSurface }) {
  const { Z, channels, timeLabels } = surface;
  const nCh = channels.length;
  const nTime = timeLabels.length;
  const sceneWidth = 10;
  const sceneDepth = 6;
  const maxHeight = 3;

  const geometry = useMemo(() => {
    const geo = new THREE.PlaneGeometry(sceneWidth, sceneDepth, nTime - 1, nCh - 1);
    const positions = geo.attributes.position;

    for (let i = 0; i < positions.count; i++) {
      const ix = i % nTime;
      const iy = Math.floor(i / nTime);
      const stress = Z[iy]?.[ix] ?? 0;
      const x = (ix / (nTime - 1) - 0.5) * sceneWidth;
      const z = (iy / (nCh - 1) - 0.5) * sceneDepth;
      const y = stress * maxHeight + 0.01; // slight offset to avoid z-fighting
      positions.setXYZ(i, x, y, z);
    }

    geo.computeVertexNormals();
    return geo;
  }, [Z, nCh, nTime, sceneWidth, sceneDepth, maxHeight]);

  return (
    <mesh geometry={geometry}>
      <meshBasicMaterial wireframe color="rgba(255,255,255,0.08)" transparent opacity={0.08} />
    </mesh>
  );
}

function InsertMarkers({ data }: { data: StressSurfaceData; surface: ProcessedSurface }) {
  const { inserts, queries } = data;
  if (inserts.length === 0) return null;

  const nTime = queries.length;
  const sceneWidth = 10;
  const sceneDepth = 6;

  // Map insert timestamps to time indices
  const tsToIdx = new Map(queries.map((r, i) => [String(r.ts), i]));

  const markers = useMemo(() => {
    const result: { x: number; z: number; size: number; rows: number; count: number }[] = [];
    const maxRows = Math.max(...inserts.map(r => r.inserted_rows), 1);

    for (const ins of inserts) {
      const idx = tsToIdx.get(String(ins.ts));
      if (idx === undefined) continue;
      const x = (idx / (nTime - 1) - 0.5) * sceneWidth;
      const z = -sceneDepth / 2 - 0.5; // below surface
      const size = 0.08 + (ins.inserted_rows / maxRows) * 0.25;
      result.push({ x, z, size, rows: ins.inserted_rows, count: ins.insert_count });
    }
    return result;
  }, [inserts, tsToIdx, nTime, sceneWidth, sceneDepth]);

  return (
    <>
      {markers.map((m, i) => (
        <mesh key={i} position={[m.x, 0.05, m.z]} rotation={[0, Math.PI / 4, 0]}>
          <octahedronGeometry args={[m.size]} />
          <meshStandardMaterial color="#64b4ff" emissive="#64b4ff" emissiveIntensity={0.5} transparent opacity={0.7} />
        </mesh>
      ))}
    </>
  );
}

function MergeMarkers({ data }: { data: StressSurfaceData; surface: ProcessedSurface }) {
  const { merges, queries } = data;
  if (merges.length === 0) return null;

  const nTime = queries.length;
  const sceneWidth = 10;
  const sceneDepth = 6;
  const tsToIdx = new Map(queries.map((r, i) => [String(r.ts), i]));

  const markers = useMemo(() => {
    const result: { x: number; z: number; size: number }[] = [];
    const maxMerges = Math.max(...merges.map(r => r.merges), 1);

    for (const m of merges) {
      if (m.merges === 0) continue;
      const idx = tsToIdx.get(String(m.ts));
      if (idx === undefined) continue;
      const x = (idx / (nTime - 1) - 0.5) * sceneWidth;
      const z = sceneDepth / 2 + 0.5; // far edge
      const size = 0.06 + (m.merges / maxMerges) * 0.18;
      result.push({ x, z, size });
    }
    return result;
  }, [merges, tsToIdx, nTime, sceneWidth, sceneDepth]);

  return (
    <>
      {markers.map((m, i) => (
        <mesh key={i} position={[m.x, 0.05, m.z]}>
          <boxGeometry args={[m.size, m.size, m.size]} />
          <meshStandardMaterial color="#ffb432" emissive="#ffb432" emissiveIntensity={0.5} transparent opacity={0.7} />
        </mesh>
      ))}
    </>
  );
}

function AxisLabels({ surface }: { surface: ProcessedSurface }) {
  const { channels, timeLabels } = surface;
  const nCh = channels.length;
  const nTime = timeLabels.length;
  const sceneWidth = 10;
  const sceneDepth = 6;
  const maxHeight = 3;

  // Show ~10 time labels max
  const timeStep = Math.max(1, Math.floor(nTime / 10));
  const timeIndices: number[] = [];
  for (let i = 0; i < nTime; i += timeStep) timeIndices.push(i);

  // Y-axis stress levels
  const yLevels = [
    { pct: 0, label: 'Idle' },
    { pct: 0.25, label: 'Light' },
    { pct: 0.5, label: 'Active' },
    { pct: 0.75, label: 'Heavy' },
    { pct: 1.0, label: 'Peak' },
  ];

  return (
    <>
      {/* Time labels along X axis — placed at far Z edge, outside the surface */}
      {timeIndices.map(i => {
        const x = (i / (nTime - 1) - 0.5) * sceneWidth;
        return (
          <Html key={`t-${i}`} position={[x, -0.15, sceneDepth / 2 + 1.2]} center occlude="blending" zIndexRange={[100, 0]} style={{ pointerEvents: 'none' }}>
            <div style={{ fontSize: 9, color: '#94a3b8', fontFamily: "'Share Tech Mono', monospace", whiteSpace: 'nowrap' }}>
              {timeLabels[i]}
            </div>
          </Html>
        );
      })}

      {/* Channel labels along Z axis — placed at far X edge, outside the surface */}
      {channels.map((ch, i) => {
        const z = (i / (nCh - 1) - 0.5) * sceneDepth;
        return (
          <Html key={`ch-${i}`} position={[sceneWidth / 2 + 1.2, 0, z]} center occlude="blending" zIndexRange={[100, 0]} style={{ pointerEvents: 'none' }}>
            <div style={{ fontSize: 8, color: '#94a3b8', fontFamily: "'Share Tech Mono', monospace", whiteSpace: 'nowrap' }}>
              {ch}
            </div>
          </Html>
        );
      })}

      {/* Y-axis vertical reference: stress level markers along back-left edge */}
      {yLevels.map(({ pct, label }, i) => {
        const y = pct * maxHeight;
        return (
          <group key={`y-${i}`}>
            <Html position={[-sceneWidth / 2 - 1.0, y, -sceneDepth / 2]} center occlude="blending" zIndexRange={[100, 0]} style={{ pointerEvents: 'none' }}>
              <div style={{ fontSize: 8, color: '#64748b', fontFamily: "'Share Tech Mono', monospace", whiteSpace: 'nowrap' }}>
                {label}
              </div>
            </Html>
            {/* Tick mark */}
            <line>
              <bufferGeometry>
                <bufferAttribute
                  attach="attributes-position"
                  count={2}
                  array={new Float32Array([-sceneWidth / 2, y, -sceneDepth / 2, -sceneWidth / 2 + 0.3, y, -sceneDepth / 2])}
                  itemSize={3}
                />
              </bufferGeometry>
              <lineBasicMaterial color="#334155" />
            </line>
          </group>
        );
      })}

      {/* Axis title: Time */}
      <Html position={[0, -0.4, sceneDepth / 2 + 2.0]} center occlude="blending" zIndexRange={[100, 0]} style={{ pointerEvents: 'none' }}>
        <div style={{ fontSize: 10, color: '#64748b', fontFamily: "'Share Tech Mono', monospace", fontWeight: 600 }}>Time</div>
      </Html>

      {/* Axis title: Stress */}
      <Html position={[-sceneWidth / 2 - 1.8, maxHeight / 2, -sceneDepth / 2]} center occlude="blending" zIndexRange={[100, 0]} style={{ pointerEvents: 'none' }}>
        <div style={{ fontSize: 10, color: '#64748b', fontFamily: "'Share Tech Mono', monospace", fontWeight: 600, writingMode: 'vertical-lr', transform: 'rotate(180deg)' }}>Stress</div>
      </Html>
    </>
  );
}

function StressSurfaceScene({ data }: { data: StressSurfaceData }) {
  const surface = useMemo(() => processSurfaceData(data.queries), [data.queries]);

  const { camera } = useThree();
  useEffect(() => {
    if (!(camera instanceof THREE.PerspectiveCamera)) return;
    camera.position.set(8, 5, 10);
    camera.lookAt(0, 1, 0);
    camera.updateProjectionMatrix();
  }, [camera]);

  if (!surface) return null;

  return (
    <>
      <ambientLight intensity={0.4} />
      <directionalLight position={[5, 8, 5]} intensity={0.8} />
      <directionalLight position={[-3, 4, -3]} intensity={0.3} color="#6366f1" />
      <pointLight position={[0, 6, 0]} intensity={0.2} color="#ff6b35" />

      <SurfaceMesh surface={surface} />
      <WireframeMesh surface={surface} />
      <InsertMarkers data={data} surface={surface} />
      <MergeMarkers data={data} surface={surface} />
      <AxisLabels surface={surface} />

      {/* Ground reference grid */}
      <gridHelper args={[14, 28, '#1e293b', '#0f172a']} position={[0, -0.01, 0]} />

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

function StressLegend() {
  return (
    <div style={{
      position: 'absolute', bottom: 16, left: 20, zIndex: 10,
      background: 'rgba(15,23,42,0.8)', backdropFilter: 'blur(8px)',
      borderRadius: 8, padding: '10px 14px',
      border: '1px solid rgba(255,255,255,0.06)',
      display: 'flex', gap: 16, alignItems: 'center', fontSize: 11,
      fontFamily: "'Share Tech Mono', monospace",
    }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
        <div style={{ width: 8, height: 8, borderRadius: '50%', background: '#64b4ff' }} />
        <span style={{ color: '#94a3b8' }}>Inserts</span>
      </div>
      <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
        <div style={{ width: 8, height: 8, background: '#ffb432' }} />
        <span style={{ color: '#94a3b8' }}>Merges</span>
      </div>
      <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
        <div style={{
          width: 60, height: 8, borderRadius: 3,
          background: 'linear-gradient(90deg, rgb(10,10,25), rgb(20,80,140), rgb(120,200,50), rgb(240,120,20), rgb(180,10,60))',
        }} />
        <span style={{ color: '#94a3b8' }}>Idle → Peak</span>
      </div>
    </div>
  );
}

// ─── Exported component ───

export interface StressSurfaceProps {
  data: StressSurfaceData | null;
  isLoading: boolean;
  error: string | null;
}

export const StressSurface: React.FC<StressSurfaceProps> = ({ data, isLoading, error }) => {
  if (error) {
    return (
      <div style={{ padding: 24, color: '#f85149', fontSize: 13 }}>
        Failed to load stress surface: {error}
      </div>
    );
  }

  if (isLoading) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%', color: 'var(--text-muted)', fontSize: 13 }}>
        Loading stress surface data…
      </div>
    );
  }

  if (!data || data.queries.length === 0) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%', color: 'var(--text-muted)', fontSize: 13 }}>
        No SELECT queries found for this table. Run some queries, then come back.
      </div>
    );
  }

  return (
    <div style={{ width: '100%', height: '100%', position: 'relative', background: 'linear-gradient(180deg, #0c0c16 0%, #020617 100%)', borderRadius: 8, overflow: 'hidden' }}>
      <div style={{
        position: 'absolute', top: 16, left: 20, zIndex: 10, pointerEvents: 'none',
      }}>
        <div style={{ fontSize: 14, fontWeight: 700, color: '#e2e8f0', fontFamily: "'Inter', sans-serif" }}>
          {data.table}
        </div>
        <div style={{ fontSize: 11, color: '#64748b', marginTop: 2 }}>
          {data.queries.length} time buckets · {data.inserts.length > 0 ? `${data.inserts.reduce((s, r) => s + r.insert_count, 0)} inserts · ` : ''}
          {data.merges.length > 0 ? `${data.merges.reduce((s, r) => s + r.merges, 0)} merges` : 'no merge activity'}
        </div>
      </div>

      <StressLegend />

      <Canvas camera={{ fov: 50 }} style={{ width: '100%', height: '100%' }}>
        <StressSurfaceScene data={data} />
      </Canvas>
    </div>
  );
};
