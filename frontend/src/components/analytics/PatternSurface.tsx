/**
 * PatternSurface — 3D surface: Query pattern × Time × actual duration.
 *
 * Port of the Python experiments.py "Pattern Surface" to Three.js.
 * Each row = a query pattern (by normalized_query_hash rank).
 * Height = actual latency in ms. Magma colorscale.
 */

import React, { useEffect, useMemo } from 'react';
import { Canvas, useThree } from '@react-three/fiber';
import { OrbitControls, Html } from '@react-three/drei';
import * as THREE from 'three';
import type { PatternSurfaceRow } from '@tracehouse/core';
import { formatBytes } from '../../utils/formatters';

// ─── Magma colorscale ───

const MAGMA_COLORS: [number, THREE.Color][] = [
  [0.00, new THREE.Color(0.001, 0.0, 0.014)],
  [0.13, new THREE.Color(0.082, 0.035, 0.216)],
  [0.25, new THREE.Color(0.232, 0.059, 0.437)],
  [0.38, new THREE.Color(0.390, 0.100, 0.502)],
  [0.50, new THREE.Color(0.550, 0.161, 0.506)],
  [0.63, new THREE.Color(0.716, 0.255, 0.432)],
  [0.75, new THREE.Color(0.869, 0.389, 0.306)],
  [0.88, new THREE.Color(0.967, 0.571, 0.192)],
  [1.00, new THREE.Color(0.987, 0.991, 0.749)],
];

function magmaColor(t: number): THREE.Color {
  const clamped = Math.max(0, Math.min(1, t));
  for (let i = 0; i < MAGMA_COLORS.length - 1; i++) {
    const [t0, c0] = MAGMA_COLORS[i];
    const [t1, c1] = MAGMA_COLORS[i + 1];
    if (clamped >= t0 && clamped <= t1) {
      const frac = (clamped - t0) / (t1 - t0);
      return new THREE.Color().lerpColors(c0, c1, frac);
    }
  }
  return MAGMA_COLORS[MAGMA_COLORS.length - 1][1];
}

// ─── Gaussian smooth ───

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

function gaussianSmooth2D(grid: number[][], sigmaRow: number, sigmaCol: number): number[][] {
  const nRows = grid.length;
  if (nRows === 0) return grid;
  const nCols = grid[0].length;
  let smoothed = grid.map(row => gaussianSmooth1D(row, sigmaCol));
  for (let c = 0; c < nCols; c++) {
    const col = smoothed.map(row => row[c]);
    const smoothedCol = gaussianSmooth1D(col, sigmaRow);
    for (let r = 0; r < nRows; r++) {
      smoothed[r][c] = smoothedCol[r];
    }
  }
  return smoothed;
}

// ─── Data processing ───

interface PatternStats {
  totalQueryCount: number;
  avgDurationMs: number;
  avgMemory: number;
  sampleQuery: string;
  hash: string;
}

interface ProcessedPattern {
  /** Z values: [patternIdx][timeIdx], actual duration in ms */
  Z: number[][];
  /** Max duration (for normalization to color) */
  maxDuration: number;
  /** Pattern labels: "#1", "#2", etc. */
  patternLabels: string[];
  /** Time labels (HH:MM) */
  timeLabels: string[];
  /** Per-pattern aggregated stats */
  patternStats: PatternStats[];
}

function processPatternData(rows: PatternSurfaceRow[]): ProcessedPattern | null {
  if (rows.length === 0) return null;

  // Get unique patterns ranked by total duration (matches SQL ORDER BY)
  const patternDurations = new Map<string, number>();
  for (const r of rows) {
    patternDurations.set(r.normalized_query_hash, (patternDurations.get(r.normalized_query_hash) ?? 0) + r.avg_duration_ms * r.query_count);
  }
  const patterns = [...patternDurations.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 12)
    .map(([hash]) => hash);

  if (patterns.length < 2) return null;

  // Get unique time buckets
  const timeSet = new Set(rows.map(r => r.ts));
  const times = [...timeSet].sort();
  if (times.length < 2) return null;

  const patternIdx = new Map(patterns.map((h, i) => [h, i]));
  const timeIdx = new Map(times.map((t, i) => [t, i]));

  // Build grid: [pattern][time] = avg_duration_ms
  const grid: number[][] = patterns.map(() => new Array(times.length).fill(0));
  for (const r of rows) {
    const pi = patternIdx.get(r.normalized_query_hash);
    const ti = timeIdx.get(r.ts);
    if (pi !== undefined && ti !== undefined) {
      grid[pi][ti] = r.avg_duration_ms;
    }
  }

  // Smooth
  const smoothed = gaussianSmooth2D(grid, 0.5, 1.0);
  // Clamp negatives
  for (let i = 0; i < smoothed.length; i++) {
    for (let j = 0; j < smoothed[i].length; j++) {
      if (smoothed[i][j] < 0) smoothed[i][j] = 0;
    }
  }

  const maxDuration = Math.max(...smoothed.flat(), 1);

  const timeLabels = times.map(t => {
    const match = t.match(/(\d{2}:\d{2})/);
    return match ? match[1] : t.slice(11, 16);
  });

  const patternLabels = patterns.map((_h, i) => `#${i + 1}`);

  // Compute per-pattern stats
  const patternStats: PatternStats[] = patterns.map(hash => {
    const patternRows = rows.filter(r => r.normalized_query_hash === hash);
    const totalQueryCount = patternRows.reduce((s, r) => s + r.query_count, 0);
    const totalDuration = patternRows.reduce((s, r) => s + r.avg_duration_ms * r.query_count, 0);
    const totalMemory = patternRows.reduce((s, r) => s + r.avg_memory * r.query_count, 0);
    const sampleRow = patternRows.find(r => r.sample_query);
    return {
      totalQueryCount,
      avgDurationMs: totalQueryCount > 0 ? totalDuration / totalQueryCount : 0,
      avgMemory: totalQueryCount > 0 ? totalMemory / totalQueryCount : 0,
      sampleQuery: sampleRow?.sample_query ?? '',
      hash,
    };
  });

  return { Z: smoothed, maxDuration, patternLabels, timeLabels, patternStats };
}

// ─── Three.js scene ───

function PatternMesh({ pattern }: { pattern: ProcessedPattern }) {
  const { Z, maxDuration, patternLabels, timeLabels } = pattern;
  const nPatterns = patternLabels.length;
  const nTime = timeLabels.length;

  const sceneWidth = 10;
  const sceneDepth = 6;
  const maxHeight = 4;

  const geometry = useMemo(() => {
    const geo = new THREE.PlaneGeometry(sceneWidth, sceneDepth, nTime - 1, nPatterns - 1);
    const positions = geo.attributes.position;
    const colors = new Float32Array(positions.count * 3);

    for (let i = 0; i < positions.count; i++) {
      const ix = i % nTime;
      const iy = Math.floor(i / nTime);

      const dur = Z[iy]?.[ix] ?? 0;
      const normalizedDur = dur / maxDuration;

      const x = (ix / (nTime - 1) - 0.5) * sceneWidth;
      const z = (iy / (nPatterns - 1) - 0.5) * sceneDepth;
      const y = normalizedDur * maxHeight;

      positions.setXYZ(i, x, y, z);

      const color = magmaColor(normalizedDur);
      colors[i * 3] = color.r;
      colors[i * 3 + 1] = color.g;
      colors[i * 3 + 2] = color.b;
    }

    geo.setAttribute('color', new THREE.BufferAttribute(colors, 3));
    geo.computeVertexNormals();
    return geo;
  }, [Z, maxDuration, nPatterns, nTime, sceneWidth, sceneDepth, maxHeight]);

  return (
    <mesh geometry={geometry}>
      <meshStandardMaterial
        vertexColors
        side={THREE.DoubleSide}
        metalness={0.1}
        roughness={0.4}
      />
    </mesh>
  );
}

function PatternAxisLabels({ pattern, onOpenQuery }: { pattern: ProcessedPattern; onOpenQuery?: (hash: string) => void }) {
  const { patternLabels, timeLabels, maxDuration, patternStats } = pattern;
  const nPatterns = patternLabels.length;
  const nTime = timeLabels.length;
  const sceneWidth = 10;
  const sceneDepth = 6;
  const maxHeight = 4;

  const timeStep = Math.max(1, Math.floor(nTime / 10));
  const timeIndices: number[] = [];
  for (let i = 0; i < nTime; i += timeStep) timeIndices.push(i);

  // Y-axis reference levels
  const yLevels = [0, 0.25, 0.5, 0.75, 1.0];

  return (
    <>
      {/* Time labels along X — placed outside the surface at far Z edge */}
      {timeIndices.map(i => {
        const x = (i / (nTime - 1) - 0.5) * sceneWidth;
        return (
          <Html key={`t-${i}`} position={[x, -0.15, sceneDepth / 2 + 1.2]} center occlude zIndexRange={[100, 0]} style={{ pointerEvents: 'none' }}>
            <div style={{ fontSize: 9, color: '#94a3b8', fontFamily: "'Share Tech Mono', monospace", whiteSpace: 'nowrap' }}>
              {timeLabels[i]}
            </div>
          </Html>
        );
      })}

      {/* Pattern labels along Z — visual only, interaction via DOM overlay */}
      {patternLabels.map((label, i) => {
        const z = (i / (nPatterns - 1) - 0.5) * sceneDepth;
        return (
          <Html key={`p-${i}`} position={[sceneWidth / 2 + 1.2, 0, z]} center zIndexRange={[100, 0]} style={{ pointerEvents: 'none' }}>
            <div style={{ fontSize: 8, color: '#94a3b8', fontFamily: "'Share Tech Mono', monospace", whiteSpace: 'nowrap' }}>
              {label}
            </div>
          </Html>
        );
      })}

      {/* Y-axis vertical reference: duration markers along left edge */}
      {yLevels.map((pct, i) => {
        const y = pct * maxHeight;
        const durationMs = Math.round(pct * maxDuration);
        return (
          <group key={`y-${i}`}>
            <Html position={[-sceneWidth / 2 - 1.0, y, -sceneDepth / 2]} center occlude zIndexRange={[100, 0]} style={{ pointerEvents: 'none' }}>
              <div style={{ fontSize: 8, color: '#64748b', fontFamily: "'Share Tech Mono', monospace", whiteSpace: 'nowrap' }}>
                {durationMs}ms
              </div>
            </Html>
            {/* Horizontal reference line */}
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

      {/* Axis titles */}
      <Html position={[0, -0.4, sceneDepth / 2 + 2.0]} center occlude zIndexRange={[100, 0]} style={{ pointerEvents: 'none' }}>
        <div style={{ fontSize: 10, color: '#64748b', fontFamily: "'Share Tech Mono', monospace", fontWeight: 600 }}>Time</div>
      </Html>
      <Html position={[-sceneWidth / 2 - 1.8, maxHeight / 2, -sceneDepth / 2]} center occlude zIndexRange={[100, 0]} style={{ pointerEvents: 'none' }}>
        <div style={{ fontSize: 10, color: '#64748b', fontFamily: "'Share Tech Mono', monospace", fontWeight: 600, writingMode: 'vertical-lr', transform: 'rotate(180deg)' }}>Duration (ms)</div>
      </Html>
    </>
  );
}

function PatternSurfaceScene({ data, onOpenQuery }: { data: PatternSurfaceRow[]; onOpenQuery?: (hash: string) => void }) {
  const pattern = useMemo(() => processPatternData(data), [data]);

  const { camera } = useThree();
  useEffect(() => {
    if (!(camera instanceof THREE.PerspectiveCamera)) return;
    camera.position.set(8, 5, 10);
    camera.lookAt(0, 1, 0);
    camera.updateProjectionMatrix();
  }, [camera]);

  if (!pattern) return null;

  return (
    <>
      <ambientLight intensity={0.4} />
      <directionalLight position={[5, 8, 5]} intensity={0.8} />
      <directionalLight position={[-3, 4, -3]} intensity={0.3} color="#a855f7" />
      <pointLight position={[0, 6, 0]} intensity={0.2} color="#f97316" />

      <PatternMesh pattern={pattern} />
      <PatternAxisLabels pattern={pattern} onOpenQuery={onOpenQuery} />

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

// ─── Color scale legend ───

function ColorScaleLegend({ maxDuration }: { maxDuration: number }) {
  return (
    <div style={{
      position: 'absolute', bottom: 16, left: 20, zIndex: 10,
      background: 'rgba(15,23,42,0.8)', backdropFilter: 'blur(8px)',
      borderRadius: 8, padding: '10px 14px',
      border: '1px solid rgba(255,255,255,0.06)',
      display: 'flex', gap: 12, alignItems: 'center', fontSize: 11,
      fontFamily: "'Share Tech Mono', monospace",
    }}>
      <span style={{ color: '#94a3b8' }}>Duration</span>
      <div style={{
        width: 80, height: 8, borderRadius: 3,
        background: 'linear-gradient(90deg, rgb(0,0,4), rgb(59,15,112), rgb(142,41,129), rgb(222,98,78), rgb(252,253,191))',
      }} />
      <span style={{ color: '#94a3b8' }}>0 → {Math.round(maxDuration)}ms</span>
    </div>
  );
}

// ─── Exported component ───

export interface PatternSurfaceProps {
  data: PatternSurfaceRow[] | null;
  isLoading: boolean;
  error: string | null;
  onOpenQuery?: (hash: string) => void;
}

export const PatternSurface: React.FC<PatternSurfaceProps> = ({ data, isLoading, error, onOpenQuery }) => {
  if (error) {
    return (
      <div style={{ padding: 24, color: '#f85149', fontSize: 13 }}>
        Failed to load pattern surface: {error}
      </div>
    );
  }

  if (isLoading) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%', color: 'var(--text-muted)', fontSize: 13 }}>
        Loading pattern surface data…
      </div>
    );
  }

  if (!data || data.length === 0) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%', color: 'var(--text-muted)', fontSize: 13 }}>
        No query patterns found. Need at least 2 distinct query patterns with data.
      </div>
    );
  }

  const processed = processPatternData(data);
  const maxDur = processed?.maxDuration ?? 0;
  const [expandedPattern, setExpandedPattern] = React.useState<number | null>(null);

  return (
    <div style={{ width: '100%', height: '100%', position: 'relative', background: 'linear-gradient(180deg, #0c0c16 0%, #020617 100%)', borderRadius: 8, overflow: 'hidden' }}>
      <div style={{
        position: 'absolute', top: 16, left: 20, zIndex: 10, pointerEvents: 'none',
      }}>
        <div style={{ fontSize: 14, fontWeight: 700, color: '#e2e8f0', fontFamily: "'Inter', sans-serif" }}>
          Query Pattern Surface
        </div>
        <div style={{ fontSize: 11, color: '#64748b', marginTop: 2 }}>
          Each row = a query pattern. Height = duration. Click a pattern to see details.
        </div>
      </div>

      {maxDur > 0 && <ColorScaleLegend maxDuration={maxDur} />}

      {/* Pattern list panel — regular DOM, outside Canvas */}
      {processed && (
        <div style={{
          position: 'absolute', top: 16, right: 16, zIndex: 20,
          maxHeight: 'calc(100% - 32px)', overflowY: 'auto',
          display: 'flex', flexDirection: 'column', gap: 2,
        }}>
          {processed.patternStats.map((stats, i) => {
            const preview = stats.sampleQuery.replace(/\s+/g, ' ').trim();
            const short = preview.length > 120 ? preview.slice(0, 120) + '…' : preview;
            const isOpen = expandedPattern === i;
            return (
              <div key={i}>
                <button
                  onClick={() => setExpandedPattern(isOpen ? null : i)}
                  style={{
                    display: 'flex', alignItems: 'center', gap: 8, width: '100%',
                    padding: '4px 10px', border: 'none', borderRadius: 4,
                    background: isOpen ? 'rgba(88,166,255,0.15)' : 'rgba(15,23,42,0.7)',
                    cursor: 'pointer', textAlign: 'left',
                    fontSize: 10, color: isOpen ? '#58a6ff' : '#94a3b8',
                    fontFamily: "'Share Tech Mono', monospace",
                  }}
                >
                  <span style={{ fontWeight: 600 }}>#{i + 1}</span>
                  <span style={{ color: '#64748b' }}>
                    {stats.avgDurationMs < 1000 ? `${Math.round(stats.avgDurationMs)}ms` : `${(stats.avgDurationMs / 1000).toFixed(1)}s`}
                  </span>
                  <span style={{ color: '#64748b' }}>{stats.totalQueryCount.toLocaleString()} queries</span>
                </button>
                {isOpen && (
                  <div style={{
                    padding: '8px 12px', marginTop: 2, borderRadius: 6,
                    background: 'rgba(15,23,42,0.95)', border: '1px solid rgba(148,163,184,0.2)',
                    backdropFilter: 'blur(8px)', width: 320,
                    fontSize: 10, lineHeight: '16px', color: '#cbd5e1',
                    fontFamily: "'Share Tech Mono', monospace",
                  }}>
                    <div style={{ display: 'flex', gap: 12, marginBottom: 6 }}>
                      <div><span style={{ color: '#64748b' }}>Avg duration </span><span style={{ color: '#e2e8f0' }}>{stats.avgDurationMs < 1000 ? `${Math.round(stats.avgDurationMs)}ms` : `${(stats.avgDurationMs / 1000).toFixed(1)}s`}</span></div>
                      <div><span style={{ color: '#64748b' }}>Queries </span><span style={{ color: '#e2e8f0' }}>{stats.totalQueryCount.toLocaleString()}</span></div>
                      <div><span style={{ color: '#64748b' }}>Avg mem </span><span style={{ color: '#e2e8f0' }}>{formatBytes(stats.avgMemory)}</span></div>
                    </div>
                    {stats.sampleQuery && (
                      <div style={{ color: '#94a3b8', wordBreak: 'break-all', whiteSpace: 'pre-wrap', marginBottom: 6 }}>
                        {short}
                      </div>
                    )}
                    {onOpenQuery && (
                      <button
                        onClick={() => onOpenQuery(stats.hash)}
                        style={{
                          padding: '3px 8px', fontSize: 9, fontWeight: 600,
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
              </div>
            );
          })}
        </div>
      )}

      <Canvas camera={{ fov: 50 }} style={{ width: '100%', height: '100%' }}>
        <PatternSurfaceScene data={data} onOpenQuery={onOpenQuery} />
      </Canvas>
    </div>
  );
};
