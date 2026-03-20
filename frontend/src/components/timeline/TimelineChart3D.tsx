/**
 * TimelineChart3D — 3D "tunnel" visualization of the TimeTravel stacked areas.
 *
 * Takes the exact same data + bucketing as the 2D TimelineChart and renders it
 * as a 3D scene where:
 *   X = time (left → right)
 *   Y = stacked metric (CPU rate / memory / etc.) — same stacking as 2D
 *   Z = peak memory per item (log-scaled depth — thicker = more memory)
 *
 * The result looks like the 2D stacked-area chart extruded into a tunnel:
 * each query ribbon becomes a translucent glass slab with depth proportional
 * to its memory footprint.
 */
import React, { useMemo, useState, useRef, useCallback, useEffect } from 'react';
import { Canvas, useThree } from '@react-three/fiber';
import { useTheme } from '../../providers/ThemeProvider';
import { OrbitControls, Text, Line, Html } from '@react-three/drei';
import * as THREE from 'three';
import type { MemoryTimeline, QuerySeries, MergeSeries, MutationSeries } from '@tracehouse/core';
import { parseTimestamp, formatBytes } from '../../utils/formatters';
import { type MetricMode, type HighlightedItem, Q_COLORS, M_COLORS, MUT_COLORS, METRIC_CONFIG } from './timeline-constants';

/* ── Constants ──────────────────────────────────────────────────────── */

const RUNWAY_LENGTH = 24;      // world X extent
const CEILING_Y = 8;           // Y at 100% capacity
const MAX_Z_DEPTH = 10;        // max Z depth for highest-memory item
const MIN_Z_DEPTH = 0.2;       // min Z so tiny-memory items are visible
const MIN_DIM = 0.02;

/* ── Shared geometries ──────────────────────────────────────────────── */

const _box = new THREE.BoxGeometry(1, 1, 1);
const _edges = new THREE.EdgesGeometry(_box);

/* ── Types ──────────────────────────────────────────────────────────── */

interface BandInfo {
  type: 'query' | 'merge' | 'mutation';
  idx: number;           // index into data.queries / data.merges / data.mutations
  color: string;
  peakMemory: number;
  isRunning: boolean;
  label: string;
  item: QuerySeries | MergeSeries | MutationSeries;
}

interface Bucket {
  ms: number;
  serverVal: number;
  /** Per-band value (metric rate) at this time point — 0 if not overlapping */
  bandVals: number[];
  /** Cumulative stack height per band (inclusive) */
  cumStack: number[];
}

/* ── Props ──────────────────────────────────────────────────────────── */

interface TimelineChart3DProps {
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

/* ── Server metric line (floating above stacks) ──────────────────────── */

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
  return (
    <Line
      points={points}
      color={color}
      lineWidth={2.5}
      opacity={0.85}
      transparent
    />
  );
};

/* ── Slab: one band across all its time buckets ──────────────────────── */

const BandSlab: React.FC<{
  bandIdx: number;
  buckets: Bucket[];
  zDepth: number;
  color: string;
  tMin: number;
  tRange: number;
  maxStack: number;
  onHover: (bandIdx: number | null) => void;
  onClick?: () => void;
  hovered: boolean;
}> = ({ bandIdx, buckets, zDepth, color, tMin, tRange, maxStack, onHover, onClick, hovered }) => {
  const meshRef = useRef<THREE.Mesh>(null);
  const edgeRef = useRef<THREE.LineSegments>(null);
  const pointerDownPos = useRef<{ x: number; y: number } | null>(null);

  // Find contiguous runs where this band has a non-zero value
  const segments = useMemo(() => {
    if (maxStack <= 0) return [];
    const yScale = CEILING_Y / maxStack;
    const segs: { xStart: number; xEnd: number; yBase: number; yTop: number }[] = [];
    let inRun = false;
    let runStart = 0;

    for (let bi = 0; bi < buckets.length; bi++) {
      const val = bandIdx > 0
        ? buckets[bi].cumStack[bandIdx] - buckets[bi].cumStack[bandIdx - 1]
        : buckets[bi].cumStack[0];
      if (val > 0) {
        if (!inRun) { runStart = bi; inRun = true; }
      }
      if ((val <= 0 || bi === buckets.length - 1) && inRun) {
        const runEnd = val > 0 ? bi : bi - 1;
        // Average yBase and yTop across the run for a smooth slab
        let sumBase = 0, sumTop = 0, count = 0;
        for (let ri = runStart; ri <= runEnd; ri++) {
          const base = bandIdx > 0 ? buckets[ri].cumStack[bandIdx - 1] : 0;
          const top = buckets[ri].cumStack[bandIdx];
          sumBase += base;
          sumTop += top;
          count++;
        }
        const xStart = ((buckets[runStart].ms - tMin) / tRange) * RUNWAY_LENGTH;
        const xEnd = ((buckets[runEnd].ms - tMin) / tRange) * RUNWAY_LENGTH;
        segs.push({
          xStart,
          xEnd: Math.max(xEnd, xStart + 0.05),
          yBase: (sumBase / count) * yScale,
          yTop: (sumTop / count) * yScale,
        });
        inRun = false;
      }
    }
    return segs;
  }, [bandIdx, buckets, tMin, tRange, maxStack]);

  const threeColor = useMemo(() => new THREE.Color(color), [color]);
  const opacity = hovered ? 0.7 : 0.4;
  const emissiveIntensity = hovered ? 0.35 : 0.12;

  return (
    <>
      {segments.map((seg, si) => {
        const w = seg.xEnd - seg.xStart;
        const h = Math.max(seg.yTop - seg.yBase, MIN_DIM);
        const d = zDepth;
        const scale: [number, number, number] = [w, h, d];
        const pos: [number, number, number] = [
          seg.xStart + w / 2,
          seg.yBase + h / 2,
          d / 2,
        ];
        return (
          <group key={si} position={pos}
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
            <mesh ref={si === 0 ? meshRef : undefined} geometry={_box} scale={scale}>
              <meshStandardMaterial
                color={threeColor}
                transparent
                opacity={opacity}
                roughness={0.15}
                metalness={0.25}
                emissive={threeColor}
                emissiveIntensity={emissiveIntensity}
                depthWrite={false}
              />
            </mesh>
            <lineSegments ref={si === 0 ? edgeRef : undefined} geometry={_edges} scale={scale}>
              <lineBasicMaterial color={threeColor} transparent opacity={hovered ? 0.9 : 0.55} />
            </lineSegments>
          </group>
        );
      })}
    </>
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
          <span style={{ color: '#3fb950', fontSize: 10 }}>CPU</span>
          <div style={{ height: 4, borderRadius: 2, background: '#1e293b' }}>
            <div style={{ height: '100%', borderRadius: 2, background: '#3fb950', width: `${Math.min(100, (item.cpu_us / 1e6) * 100)}%` }} />
          </div>
          <span style={{ fontSize: 10 }}>{item.cpu_us >= 1e6 ? `${(item.cpu_us / 1e6).toFixed(1)}s` : `${(item.cpu_us / 1e3).toFixed(0)}ms`}</span>

          <span style={{ color: '#58a6ff', fontSize: 10 }}>MEM</span>
          <div style={{ height: 4, borderRadius: 2, background: '#1e293b' }}>
            <div style={{ height: '100%', borderRadius: 2, background: '#58a6ff', width: `${Math.min(100, (item.peak_memory / (1024 * 1024 * 1024)) * 100)}%` }} />
          </div>
          <span style={{ fontSize: 10 }}>{formatBytes(item.peak_memory)}</span>

          <span style={{ color: '#bc8cff', fontSize: 10 }}>DISK</span>
          <div style={{ height: 4, borderRadius: 2, background: '#1e293b' }}>
            <div style={{ height: '100%', borderRadius: 2, background: '#bc8cff', width: `${Math.min(100, ((item.disk_read + item.disk_write) / (1024 * 1024 * 100)) * 100)}%` }} />
          </div>
          <span style={{ fontSize: 10 }}>{formatBytes(item.disk_read + item.disk_write)}</span>

          <span style={{ color: '#d29922', fontSize: 10 }}>NET</span>
          <div style={{ height: 4, borderRadius: 2, background: '#1e293b' }}>
            <div style={{ height: '100%', borderRadius: 2, background: '#d29922', width: `${Math.min(100, ((item.net_send + item.net_recv) / (1024 * 1024 * 100)) * 100)}%` }} />
          </div>
          <span style={{ fontSize: 10 }}>{formatBytes(item.net_send + item.net_recv)}</span>
        </div>
        <div style={{ marginTop: 4, color: '#64748b', fontSize: 10 }}>
          Duration: {fmtMs(item.duration_ms)} · Mem depth: {formatBytes(item.peak_memory)}
        </div>
      </div>
    </Html>
  );
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

/* ── Main component ──────────────────────────────────────────────────── */

export const TimelineChart3D: React.FC<TimelineChart3DProps> = ({
  data,
  metricMode,
  height = 500,
  hiddenCategories,
  onBandClick,
  onHighlightItem,
}) => {
  const [hoveredBand, setHoveredBand] = useState<number | null>(null);
  const { theme } = useTheme();
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
          type: 'query', idx: i,
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
          type: 'merge', idx: i,
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
          type: 'mutation', idx: i,
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

  // Time range
  const tMin = serverPts.length > 0 ? serverPts[0].ms : new Date(data.window_start).getTime();
  const tMax = serverPts.length > 0 ? serverPts[serverPts.length - 1].ms : new Date(data.window_end).getTime();
  const tRange = tMax - tMin || 1;

  // Per-band metric rate (same flat-rate as 2D: peak spread over duration)
  const bandRates = useMemo(() => {
    return bands.map(b => getRate(b.item as any, metricMode));
  }, [bands, metricMode]);

  // Band time ranges
  const bandTimeRanges = useMemo(() => {
    return bands.map(b => ({
      startMs: parseTimestamp(b.item.start_time),
      endMs: parseTimestamp(b.item.end_time),
    }));
  }, [bands]);

  // Build buckets at each server metric point (mirrors 2D bucketing exactly)
  const buckets: Bucket[] = useMemo(() => {
    if (serverPts.length === 0 || bands.length === 0) return [];
    return serverPts.map(sp => {
      const vals = bands.map((_, bi) => {
        const tr = bandTimeRanges[bi];
        return (sp.ms >= tr.startMs && sp.ms <= tr.endMs) ? bandRates[bi] : 0;
      });
      // Cumulative stack
      const cum: number[] = [];
      let acc = 0;
      for (const v of vals) { acc += v; cum.push(acc); }
      return { ms: sp.ms, serverVal: sp.v, bandVals: vals, cumStack: cum };
    });
  }, [serverPts, bands, bandRates, bandTimeRanges]);

  // Global max stack for Y normalization
  const maxStack = useMemo(() => {
    if (buckets.length === 0 || bands.length === 0) return 0;
    let mx = 0;
    for (const b of buckets) {
      const top = b.cumStack[b.cumStack.length - 1];
      if (top > mx) mx = top;
    }
    return mx;
  }, [buckets, bands]);

  // maxY = max of server line and stack
  const maxY = useMemo(() => {
    const sMax = serverPts.length > 0 ? Math.max(...serverPts.map(p => p.v)) : 0;
    return Math.max(sMax, maxStack, 1);
  }, [serverPts, maxStack]);

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

  // Tooltip position: center of hovered band's visible segments
  const tooltipPos = useMemo((): [number, number, number] | null => {
    if (hoveredBand === null || buckets.length === 0 || maxStack <= 0) return null;
    const yScale = CEILING_Y / maxStack;
    // Find middle bucket where this band is active
    let sumX = 0, sumY = 0, count = 0;
    for (const b of buckets) {
      const val = hoveredBand > 0
        ? b.cumStack[hoveredBand] - b.cumStack[hoveredBand - 1]
        : b.cumStack[0];
      if (val > 0) {
        const x = ((b.ms - tMin) / tRange) * RUNWAY_LENGTH;
        const top = b.cumStack[hoveredBand] * yScale;
        sumX += x; sumY += top; count++;
      }
    }
    if (count === 0) return null;
    const depth = memToDepth(bands[hoveredBand].peakMemory);
    return [sumX / count, sumY / count + 1.5, depth / 2];
  }, [hoveredBand, buckets, bands, tMin, tRange, maxStack, memToDepth]);

  const handleBandHover = useCallback((idx: number | null) => setHoveredBand(idx), []);

  // Sync hovered band to parent for table row highlighting
  useEffect(() => {
    if (!onHighlightItem) return;
    onHighlightItem(hoveredBand !== null && bands[hoveredBand] ? { type: bands[hoveredBand].type, idx: bands[hoveredBand].idx } : null);
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
          makeDefault
          enablePan
          enableZoom
          enableRotate
          enableDamping
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

        {/* Band slabs */}
        {bands.map((band, bi) => (
          <BandSlab
            key={`${band.type}-${band.idx}`}
            bandIdx={bi}
            buckets={buckets}
            zDepth={memToDepth(band.peakMemory)}
            color={band.color}
            tMin={tMin}
            tRange={tRange}
            maxStack={maxStack}
            onHover={handleBandHover}
            onClick={onBandClick ? () => onBandClick({ type: band.type, idx: band.idx }) : undefined}
            hovered={hoveredBand === bi}
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
