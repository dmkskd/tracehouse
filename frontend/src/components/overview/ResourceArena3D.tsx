/**
 * ResourceArena3D — Guitar-Hero style scrolling resource timeline
 *
 * X-axis = time (right edge = NOW, scrolls left as time passes)
 * Y-axis = CPU (taller blocks use more CPU)
 * Z-axis = lanes (each query/merge on its own lane) + memory encoded as block depth
 */

import React, { useRef, useState, useCallback, useEffect } from 'react';
import { Canvas, useFrame, useThree } from '@react-three/fiber';
import { OrbitControls } from '@react-three/drei';
import { SafeText as Text } from '@tracehouse/ui-shared';
import type { OrbitControls as OrbitControlsImpl } from 'three-stdlib';
import * as THREE from 'three';
import { Link } from 'react-router-dom';
import type { RunningQueryInfo, ActiveMergeInfo, TraceLog } from '@tracehouse/core';
import { truncateQuery, formatBytes } from '../../utils/formatters';
import { useClickHouseServices } from '../../providers/ClickHouseProvider';
import {
  Ts, Cs, MIN_DIM, LANE_GAP, DECK_GAP,
  DECK_ORDER, deckOf, deckBaseY,
  type BlockEntry, type Deck,
} from './arena-types';
import {
  useCheatMode, useShootingSystem, CheatHUD, CheatSceneElements,
  type CheatModeState, type BlockCheatInfo,
  getOrCreateCheatInfo,
} from './ArenaCheatMode';

/* ── palette ─────────────────────────────────────────── */

const BASE_HSL: Record<string, [number, number, number]> = {
  SELECT:   [217, 0.91, 0.60],
  INSERT:   [160, 0.84, 0.38],
  ALTER:    [38,  0.90, 0.57],
  SYSTEM:   [330, 0.81, 0.57],
  OTHER:    [258, 0.90, 0.58],
  MERGE:    [38,  0.90, 0.57],
  MUTATION: [0,   0.86, 0.56],
};

function strHash01(s: string): number {
  let h = 0;
  for (let i = 0; i < s.length; i++) h = ((h << 5) - h + s.charCodeAt(i)) | 0;
  return ((h & 0x7fffffff) % 1000) / 1000;
}

function colorForBlock(kind: string, tableHint: string): string {
  const base = BASE_HSL[kind.toUpperCase()] ?? BASE_HSL.OTHER;
  const t = strHash01(tableHint);
  const hueShift = (t - 0.5) * 30;
  const lightShift = (t - 0.5) * 0.12;
  const h = ((base[0] + hueShift) % 360 + 360) % 360;
  const s = Math.max(0.3, Math.min(1, base[1]));
  const l = Math.max(0.25, Math.min(0.75, base[2] + lightShift));
  return `hsl(${h}, ${s * 100}%, ${l * 100}%)`;
}

const colorOf = (k: string) => {
  const b = BASE_HSL[k.toUpperCase()] ?? BASE_HSL.OTHER;
  return `hsl(${b[0]}, ${b[1] * 100}%, ${b[2] * 100}%)`;
};

/* ── local constants ─────────────────────────────────── */

const FADE_SECS = 60;
const HORIZON = 120;

/* ── shared geometries ───────────────────────────────── */

const _box = new THREE.BoxGeometry(1, 1, 1);
const _edges = new THREE.EdgesGeometry(_box);
const _plate = new THREE.PlaneGeometry(1, 1);

/* ── helpers ─────────────────────────────────────────── */

function fmtRate(bps: number): string {
  if (bps < 1024) return `${bps.toFixed(0)} B/s`;
  if (bps < 1048576) return `${(bps / 1024).toFixed(1)} KB/s`;
  return `${(bps / 1048576).toFixed(1)} MB/s`;
}

function fmtMicro(us: number): string {
  if (us < 1000) return `${us.toFixed(0)}µs`;
  if (us < 1_000_000) return `${(us / 1000).toFixed(1)}ms`;
  return `${(us / 1_000_000).toFixed(2)}s`;
}

function fmtNum(n: number): string {
  if (n < 1000) return n.toFixed(0);
  if (n < 1_000_000) return `${(n / 1000).toFixed(1)}K`;
  if (n < 1_000_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  return `${(n / 1_000_000_000).toFixed(2)}B`;
}

/** Compute 3D anchor position for a card near an entry's right edge */
function entryCardPos(entry: BlockEntry, offsetY: number, offsetZ: number): THREE.Vector3 {
  const now = Date.now();
  const running = entry.endTime === null;
  const baseY = deckBaseY(entry.deck);
  const laneZ = -entry.lane * LANE_GAP;
  const cpuH = Math.max(MIN_DIM, entry.cpu * Cs);
  const GAP = 0.03;
  const rightX = (running ? 0 : -((now - entry.endTime!) / 1000) * Ts) - GAP;
  return new THREE.Vector3(rightX + 0.15, baseY + cpuH + offsetY, laneZ + offsetZ);
}

/* ── Metric history for sparklines ───────────────────── */

interface MetricSnapshot {
  t: number; // timestamp ms
  cpu: number;
  mem: number;
  io: number;
  rows: number;
  write: number;
  progress: number;
  // Profile event cumulative totals (for delta computation)
  userTimeMicro: number;
  sysTimeMicro: number;
  diskRead: number;
  diskWrite: number;
}

const MAX_HISTORY = 120; // ~2 minutes at 1s intervals

function useMetricHistory(entry: BlockEntry | null): MetricSnapshot[] {
  const [history, setHistory] = useState<MetricSnapshot[]>([]);
  const entryRef = useRef<BlockEntry | null>(null);
  entryRef.current = entry;

  // Reset when selection changes
  const entryId = entry?.id ?? null;
  const prevId = useRef<string | null>(null);
  if (entryId !== prevId.current) {
    prevId.current = entryId;
    if (history.length > 0) setHistory([]);
  }

  // Tick every second to sample metrics — stop once query finishes
  const running = entry?.endTime === null;
  useEffect(() => {
    if (!entryId || !running) return;
    const iv = setInterval(() => {
      const e = entryRef.current;
      if (!e) return;
      // Stop accumulating once finished
      if (e.endTime !== null) { clearInterval(iv); return; }
      const ioRate = e.isMerge ? (e.readBytesPerSec || 0) : e.ioReadRate;
      const pe = e.profileEvents;
      setHistory(prev => {
        const next = [...prev, {
          t: Date.now(), cpu: e.cpu, mem: e.mem,
          io: ioRate, rows: e.rowsRead, write: e.writeBytesPerSec || 0,
          progress: e.isMerge ? e.progress * 100 : e.progress,
          userTimeMicro: pe?.userTimeMicroseconds ?? 0,
          sysTimeMicro: pe?.systemTimeMicroseconds ?? 0,
          diskRead: pe?.osReadBytes ?? 0,
          diskWrite: pe?.osWriteBytes ?? 0,
        }];
        return next.length > MAX_HISTORY ? next.slice(-MAX_HISTORY) : next;
      });
    }, 1000);
    return () => clearInterval(iv);
  }, [entryId, running]);

  return history;
}

/* ── Live log tail ──────────────────────────────────── */

const LOG_LEVEL_COLORS: Record<string, string> = {
  Fatal: '#ef4444', Critical: '#ef4444', Error: '#f87171',
  Warning: '#fbbf24', Notice: '#60a5fa', Information: '#818cf8',
  Debug: 'rgba(255,255,255,0.35)', Trace: 'rgba(255,255,255,0.2)',
};

function useLogTail(entry: BlockEntry | null): { logs: TraceLog[]; prefetch: (e: BlockEntry) => void } {
  const services = useClickHouseServices();
  const [logs, setLogs] = useState<TraceLog[]>([]);
  const entryRef = useRef(entry);
  entryRef.current = entry;
  const genRef = useRef(0);
  const entryId = entry?.id ?? null;
  const prevId = useRef<string | null>(null);
  const fetchingRef = useRef(false);
  const servicesRef = useRef(services);
  servicesRef.current = services;

  if (entryId !== prevId.current) {
    prevId.current = entryId;
    genRef.current += 1;
    if (logs.length > 0) setLogs([]);
  }

  // Shared fetch function — usable from both effect and click handler
  const doFetch = useCallback(async (e: BlockEntry, gen: number) => {
    const svc = servicesRef.current;
    if (!svc) return;
    if (fetchingRef.current) return;
    fetchingRef.current = true;
    try {
      let result: TraceLog[];
      if (e.isMerge && e.database && e.table && e.partName) {
        result = await svc.mergeTracker.getMergeEventTextLogs({
          query_id: undefined,
          event_time: new Date(e.startTime).toISOString(),
          duration_ms: e.elapsed * 1000,
          database: e.database,
          table: e.table,
          part_name: e.partName,
        }) as TraceLog[];
      } else if (e.queryId) {
        const eventDate = new Date(e.startTime).toISOString();
        result = await svc.traceService.getQueryLogs(e.queryId, undefined, eventDate, 100);
      } else {
        return;
      }
      if (genRef.current === gen) setLogs(result);
    } catch {
      // silently fail
    } finally {
      fetchingRef.current = false;
    }
  }, []);

  // Prefetch: call from click handler to start fetch before React commits the effect
  const prefetch = useCallback((e: BlockEntry) => {
    genRef.current += 1;
    prevId.current = e.id;
    setLogs([]);
    doFetch(e, genRef.current);
  }, [doFetch]);

  // Polling interval for live updates
  useEffect(() => {
    if (!entryId || !services) return;
    const myGen = genRef.current;
    // Only start the initial fetch if prefetch hasn't already fired for this gen
    if (!fetchingRef.current) doFetch(entryRef.current!, myGen);
    const iv = setInterval(() => {
      if (entryRef.current) doFetch(entryRef.current, myGen);
    }, 3000);
    return () => clearInterval(iv);
  }, [entryId, services, doFetch]);

  return { logs, prefetch };
}

/** Parse a ClickHouse event_time_microseconds string into epoch ms */
function parseLogTime(raw: string): number {
  if (!raw) return 0;
  try {
    // "2025-03-07 01:00:33.860123" or "01:00:33.860123"
    if (raw.includes('-')) return new Date(raw.replace(' ', 'T') + 'Z').getTime();
    const [h, m, rest] = raw.split(':');
    const secs = parseFloat(rest || '0');
    const d = new Date();
    d.setUTCHours(+h, +m, Math.floor(secs), Math.round((secs % 1) * 1000));
    return d.getTime();
  } catch { return 0; }
}

/** Compact duration string */
function fmtDelta(ms: number): string {
  if (ms <= 0) return '';
  const s = ms / 1000;
  if (s < 0.01) return '<10ms';
  if (s < 1) return `${(s * 1000).toFixed(0)}ms`;
  if (s < 60) return `${s.toFixed(1)}s`;
  const m = Math.floor(s / 60);
  return `${m}m${Math.floor(s % 60)}s`;
}

function LogTail({ logs, maxLines = 6, elapsedSec = 0 }: { logs: TraceLog[]; maxLines?: number; elapsedSec?: number }) {
  const visible = logs.slice(-maxLines);

  if (logs.length === 0) {
    return (
      <div style={{
        fontSize: 9, color: 'rgba(255,255,255,0.12)', letterSpacing: 1,
        padding: '8px 0',
      }}>waiting for logs...</div>
    );
  }

  // Pre-parse all log times for delta computation
  const allTimes = logs.map(l => parseLogTime(l.event_time_microseconds || l.event_time));
  const visibleStart = logs.length - visible.length;

  return (
    <div style={{
        fontSize: 12, lineHeight: 1.7, fontFamily: panelFont,
      }}>
      {visible.map((log, i) => {
        const globalIdx = visibleStart + i;
        const levelColor = LOG_LEVEL_COLORS[log.level] ?? 'rgba(255,255,255,0.4)';
        const isLast = i === visible.length - 1;

        // Delta from previous log line
        const thisTime = allTimes[globalIdx];
        const prevTime = globalIdx > 0 ? allTimes[globalIdx - 1] : 0;
        const deltaMs = thisTime > 0 && prevTime > 0 ? thisTime - prevTime : 0;
        const deltaStr = fmtDelta(deltaMs);
        const isSlow = deltaMs > 1000;

        return (
          <div key={i} style={{
            display: 'flex', gap: 8,
            borderBottom: '1px solid rgba(255,255,255,0.05)',
            padding: '3px 0',
            animation: isLast
              ? 'arena-log-pulse 1.5s ease infinite'
              : i >= visible.length - 3 ? 'arena-fade-in 0.3s ease both' : undefined,
            background: isLast
              ? `linear-gradient(90deg, ${levelColor}25 0%, ${levelColor}08 50%, transparent 100%)`
              : undefined,
            backgroundSize: isLast ? '200% 100%' : undefined,
            borderLeft: isLast ? `2px solid ${levelColor}` : '2px solid transparent',
            paddingLeft: 6,
            opacity: isLast ? undefined : 0.5 + 0.5 * (i / visible.length),
          }}>
            {/* Timestamp HH:MM:SS.mmm */}
            <span style={{
              color: 'rgba(255,255,255,0.3)', flexShrink: 0, width: 80,
              fontVariantNumeric: 'tabular-nums', fontSize: 11,
            }}>{(log.event_time_microseconds || log.event_time || '').split(' ').pop()?.substring(0, 12) || ''}</span>
            {/* Step delta */}
            <span style={{
              color: isSlow ? '#fbbf24' : deltaStr ? 'rgba(255,255,255,0.35)' : 'rgba(255,255,255,0.12)',
              flexShrink: 0, width: 46, textAlign: 'right',
              fontWeight: isSlow ? 700 : 400,
              fontVariantNumeric: 'tabular-nums',
              fontSize: 11,
            }}>{deltaStr || '·'}</span>
            <span style={{
              color: levelColor, fontWeight: 700, flexShrink: 0, width: 16,
              textAlign: 'center', fontSize: 11,
            }}>{log.level[0]}</span>
            <span style={{ color: 'rgba(255,255,255,0.75)', flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
              {log.message}
            </span>
          </div>
        );
      })}
      {/* Activity summary: log span vs query elapsed */}
      {allTimes.length >= 2 && (() => {
        const firstTime = allTimes.find(t => t > 0) ?? 0;
        const lastTime = allTimes[allTimes.length - 1];
        const spanMs = lastTime > 0 && firstTime > 0 ? lastTime - firstTime : 0;
        const elapsedMs = elapsedSec * 1000;
        // Gap = query has been running longer than the log window covers
        const silenceMs = elapsedMs > 0 && spanMs > 0 ? Math.max(0, elapsedMs - spanMs) : 0;
        const isSilent = silenceMs > 5000;
        return (
          <div style={{
            fontSize: 10, marginTop: 3, display: 'flex', justifyContent: 'space-between',
            color: 'rgba(255,255,255,0.25)',
          }}>
            <span>{logs.length} logs over {fmtDelta(spanMs) || '<1ms'}</span>
            {isSilent && (
              <span style={{
                color: silenceMs > 30000 ? '#fbbf24' : 'rgba(255,255,255,0.3)',
                animation: silenceMs > 30000 ? 'arena-glow-pulse 2s ease infinite' : undefined,
              }}>
                {fmtDelta(silenceMs)} without logs
              </span>
            )}
          </div>
        );
      })()}
    </div>
  );
}

/* ── SVG Sparkline ──────────────────────────────────── */

function Sparkline({ data, color, width = 140, height = 32, label, currentValue }: {
  data: number[];
  color: string;
  width?: number;
  height?: number;
  label: string;
  currentValue: string;
}) {
  if (data.length < 2) {
    return (
      <div style={{ width, height: height + 20, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <span style={{ fontSize: 8, color: 'rgba(255,255,255,0.15)', letterSpacing: 1 }}>collecting...</span>
      </div>
    );
  }

  const max = Math.max(...data) || 1;
  const min = Math.min(...data);
  const range = max - min || 1;
  const pad = 2;
  const plotW = width - pad * 2;
  const plotH = height - pad * 2;

  const points = data.map((v, i) => {
    const x = pad + (i / (data.length - 1)) * plotW;
    const y = pad + plotH - ((v - min) / range) * plotH;
    return `${x},${y}`;
  });

  const fillPoints = [
    `${pad},${height - pad}`,
    ...points,
    `${pad + plotW},${height - pad}`,
  ].join(' ');

  return (
    <div style={{ marginBottom: 2 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', marginBottom: 1 }}>
        <span style={{ fontSize: 8, color: 'rgba(255,255,255,0.3)', letterSpacing: 1.5, fontWeight: 700 }}>{label}</span>
        <span style={{ fontSize: 11, color, fontWeight: 600, fontVariantNumeric: 'tabular-nums',
          filter: `drop-shadow(0 0 6px ${color}80)`,
        }}>{currentValue}</span>
      </div>
      <svg width={width} height={height} style={{ display: 'block' }}>
        <defs>
          <linearGradient id={`spark-fill-${label}`} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={color} stopOpacity={0.15} />
            <stop offset="100%" stopColor={color} stopOpacity={0} />
          </linearGradient>
        </defs>
        <polygon points={fillPoints} fill={`url(#spark-fill-${label})`} />
        <polyline points={points.join(' ')} fill="none" stroke={color} strokeWidth={1.5}
          strokeLinejoin="round" strokeLinecap="round"
          style={{ filter: `drop-shadow(0 0 6px ${color}90) drop-shadow(0 0 2px ${color})` }} />
        {/* Current value dot — glowing */}
        {data.length > 0 && (() => {
          const lastX = pad + plotW;
          const lastY = pad + plotH - ((data[data.length - 1] - min) / range) * plotH;
          return <circle cx={lastX} cy={lastY} r={3} fill={color}
            style={{ filter: `drop-shadow(0 0 8px ${color}) drop-shadow(0 0 3px ${color})` }} />;
        })()}
      </svg>
    </div>
  );
}

/* ── LiveBlock ───────────────────────────────────────── */

function LiveBlock({ entry, hoveredId, selectedId, onHover, onClick, cheatInfo }: {
  entry: BlockEntry;
  hoveredId: string | null;
  selectedId: string | null;
  onHover: (v: BlockEntry | null) => void;
  onClick: (v: BlockEntry) => void;
  cheatInfo?: BlockCheatInfo;
}) {
  const boxRef = useRef<THREE.Group>(null);
  const plateRef = useRef<THREE.Mesh>(null);
  const matRef = useRef<THREE.MeshStandardMaterial>(null);
  const edgeMatRef = useRef<THREE.LineBasicMaterial>(null);
  const plateMatRef = useRef<THREE.MeshBasicMaterial>(null);
  const downXY = useRef<[number, number] | null>(null);
  const col = new THREE.Color(entry.color);

  const cpuH = Math.max(MIN_DIM, entry.cpu * Cs);
  // Log scale for memory depth so huge allocations don't swallow the scene
  const memGB = entry.mem / 1073741824;
  const memD = Math.max(MIN_DIM * 3, Math.log2(1 + memGB) * 0.4);

  const isHovered = hoveredId === entry.id;
  const isSelected = selectedId === entry.id;
  const isHighlighted = isHovered || isSelected;
  const isDimmed = (hoveredId !== null || selectedId !== null) && !isHighlighted;

  const currentZ = useRef(-entry.lane * LANE_GAP);
  const currentScale = useRef(1);

  useFrame(({ clock }, delta) => {
    const now = Date.now();
    const running = entry.endTime === null;

    // Only pop on hover, not on selected (selected block stays normal size so it doesn't block others)
    const targetScale = isHovered ? 1.4 : 1;
    currentScale.current += (targetScale - currentScale.current) * Math.min(1, 8 * delta);
    const popScale = currentScale.current;

    const targetZ = -entry.lane * LANE_GAP;
    currentZ.current += (targetZ - currentZ.current) * Math.min(1, 3 * delta);
    const laneZ = currentZ.current;
    const baseY = deckBaseY(entry.deck);

    const GAP = 0.03;
    let rightX = (running ? 0 : -((now - entry.endTime!) / 1000) * Ts) - GAP;
    let leftX = -((now - entry.startTime) / 1000) * Ts + GAP;

    // Cheat mode: accumulate extra X offset to rush blocks toward camera
    if (cheatInfo) {
      cheatInfo.xOffset -= delta * Ts * cheatInfo.speedMultiplier;
      rightX += cheatInfo.xOffset;
      leftX += cheatInfo.xOffset;
    }

    const width = Math.max(MIN_DIM, rightX - leftX);
    const centerX = (leftX + rightX) / 2;

    let opacity = 0.36;
    let edgeOpacity = 0.8;
    if (!running) {
      const finAge = (now - entry.endTime!) / 1000;
      const fade = Math.min(1, finAge / FADE_SECS);
      opacity *= (1 - fade);
      edgeOpacity *= (1 - fade);
    }

    if (isHighlighted) {
      opacity = Math.min(0.85, opacity * 2.2);
      edgeOpacity = 1;
    } else if (isDimmed) {
      opacity *= 0.4;
      edgeOpacity *= 0.3;
    }

    // Cheat mode: damage flash — bright white flash when hit
    if (cheatInfo && cheatInfo.damageFlash > 0) {
      const flashAge = (performance.now() / 1000) - cheatInfo.damageFlash;
      if (flashAge < 0.15) {
        opacity = 0.95;
        edgeOpacity = 1;
      }
    }

    const py = baseY + (cpuH * popScale) / 2;

    if (boxRef.current) {
      boxRef.current.position.set(centerX, py, laneZ);
      boxRef.current.scale.set(width, cpuH * popScale, memD * popScale);
    }
    if (plateRef.current) {
      plateRef.current.position.set(centerX, baseY + 0.003, laneZ);
      plateRef.current.scale.set(width, memD * popScale, 1);
    }
    if (matRef.current) {
      const t = clock.elapsedTime;
      // Cheat mode: tint toward white when damaged
      let emissiveIntensity: number;
      if (cheatInfo && cheatInfo.hp < cheatInfo.maxHp) {
        const dmgRatio = 1 - cheatInfo.hp / cheatInfo.maxHp;
        matRef.current.emissive.copy(col).lerp(new THREE.Color('#ffffff'), dmgRatio * 0.5);
        emissiveIntensity = 0.2 + dmgRatio * 0.4;
      } else {
        emissiveIntensity = isHighlighted
          ? 0.5 + Math.sin(t * 3) * 0.15
          : running ? 0.15 + Math.sin(t * 2.2 + laneZ * 2) * 0.05 : 0.02;
      }
      matRef.current.opacity = opacity + (running ? Math.sin(t * 1.8 + laneZ * 3) * 0.06 : 0);
      matRef.current.emissiveIntensity = emissiveIntensity;
    }
    if (edgeMatRef.current) edgeMatRef.current.opacity = edgeOpacity;
    if (plateMatRef.current) plateMatRef.current.opacity = isHighlighted ? 0.25 : running ? 0.12 : opacity * 0.3;
  });

  // Thin hitbox at right edge — so large blocks don't occlude everything behind them
  const hitRef = useRef<THREE.Mesh>(null);
  const HIT_SLICE = 0.5; // width of the clickable zone at the right edge

  useFrame(() => {
    if (!boxRef.current || !hitRef.current) return;
    const p = boxRef.current.position;
    const s = boxRef.current.scale;
    const sliceW = Math.min(s.x, HIT_SLICE);
    // Position at right edge of the block
    hitRef.current.position.set(p.x + (s.x - sliceW) / 2, p.y, p.z);
    hitRef.current.scale.set(sliceW, s.y, s.z);
  });

  return (
    <group>
      {/* Visual box + edges — no raycasting */}
      <group ref={boxRef} raycast={noRay}>
        <mesh geometry={_box} renderOrder={1} raycast={noRay}>
          <meshStandardMaterial
            ref={matRef} color={col} transparent opacity={0.36}
            emissive={col} emissiveIntensity={0.15}
            roughness={0.12} metalness={0.25}
            depthWrite={false}
            polygonOffset polygonOffsetFactor={2} polygonOffsetUnits={2}
          />
        </mesh>
        <lineSegments geometry={_edges} renderOrder={2} raycast={noRay}>
          <lineBasicMaterial ref={edgeMatRef} color={col} transparent opacity={0.8}
            depthWrite={false}
          />
        </lineSegments>
      </group>

      {/* Thin invisible hitbox at right edge */}
      <mesh
        ref={hitRef}
        geometry={_box}
        visible={false}
        onPointerOver={e => { e.stopPropagation(); onHover(entry); }}
        onPointerOut={() => onHover(null)}
        onPointerDown={e => { downXY.current = [e.clientX, e.clientY]; }}
        onClick={e => {
          const down = downXY.current;
          if (down) {
            const dx = e.clientX - down[0], dy = e.clientY - down[1];
            if (dx * dx + dy * dy > 16) return;
          }
          e.stopPropagation();
          onClick(entry);
        }}
      />

      <mesh ref={plateRef} geometry={_plate} rotation={[-Math.PI / 2, 0, 0]} renderOrder={0}
        raycast={noRay}
      >
        <meshBasicMaterial ref={plateMatRef} color={col} transparent opacity={0.12}
          depthWrite={false}
          polygonOffset polygonOffsetFactor={4} polygonOffsetUnits={4}
        />
      </mesh>
    </group>
  );
}

/* ── CSS bar helper for overlay cards ────────────────── */

const panelFont = "'JetBrains Mono', 'Fira Code', 'SF Mono', monospace";

/** Tiny inline bar for the hover card */
function MiniBar({ ratio, color }: { ratio: number; color: string }) {
  const pct = Math.max(2, Math.min(100, ratio * 100));
  return (
    <div style={{
      width: 40, height: 4, borderRadius: 2,
      background: 'rgba(255,255,255,0.06)', overflow: 'hidden', display: 'inline-block',
      verticalAlign: 'middle', marginLeft: 4,
    }}>
      <div style={{
        height: '100%', borderRadius: 2, width: `${pct}%`,
        background: color, boxShadow: `0 0 4px ${color}40`,
      }} />
    </div>
  );
}

/* ── Position bridge: projects 3D positions to DOM overlay positions ── */

function CardPositionBridge({ entry, domRef, offsetY, offsetZ }: {
  entry: BlockEntry | null;
  domRef: React.RefObject<HTMLDivElement | null>;
  offsetY: number;
  offsetZ: number;
}) {
  const { camera, size } = useThree();
  const vec = useRef(new THREE.Vector3());

  useFrame(() => {
    if (!domRef.current || !entry) {
      if (domRef.current) domRef.current.style.display = 'none';
      return;
    }
    const pos = entryCardPos(entry, offsetY, offsetZ);
    vec.current.copy(pos).project(camera);
    const x = (vec.current.x * 0.5 + 0.5) * size.width;
    const y = (-vec.current.y * 0.5 + 0.5) * size.height;
    domRef.current.style.display = '';
    domRef.current.style.transform = `translate(${x}px, ${y}px)`;
  });

  return null;
}

/* ── Immersive HUD: full-viewport cinematic detail overlay ── */

// Inject HUD animation styles once
if (typeof document !== 'undefined' && !document.getElementById('arena3d-hud-styles')) {
  const style = document.createElement('style');
  style.id = 'arena3d-hud-styles';
  style.textContent = `
    @keyframes arena-scan { 0% { top: -2px; } 100% { top: 100%; } }
    @keyframes arena-fade-in { 0% { opacity: 0; transform: translateY(8px); } 100% { opacity: 1; transform: translateY(0); } }
    @keyframes arena-glow-pulse { 0%,100% { opacity: 0.6; } 50% { opacity: 1; } }
    @keyframes arena-bar-fill { 0% { width: 0%; } 100% { width: var(--bar-pct); } }
    @keyframes arena-typing { 0% { width: 0; } 100% { width: 100%; } }
    @keyframes arena-log-pulse { 0%,100% { background-position: 0% 0%; opacity: 0.7; } 50% { background-position: 100% 0%; opacity: 1; } }
  `;
  document.head.appendChild(style);
}

/** Animated bar with fill-in effect */
function HudBar({ label, ratio, color, value, delay = 0 }: {
  label: string; ratio: number; color: string; value: string; delay?: number;
}) {
  const pct = Math.max(1, Math.min(100, ratio * 100));
  return (
    <div style={{
      display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8,
      animation: `arena-fade-in 0.4s ease ${delay}s both`,
    }}>
      <span style={{
        width: 32, fontSize: 9, color: 'rgba(255,255,255,0.3)', textAlign: 'right', flexShrink: 0,
        fontWeight: 700, letterSpacing: 1,
      }}>{label}</span>
      <div style={{
        flex: 1, height: 8, borderRadius: 4,
        background: 'rgba(255,255,255,0.04)', overflow: 'hidden', position: 'relative',
      }}>
        <div style={{
          '--bar-pct': `${pct}%`,
          height: '100%', borderRadius: 4,
          background: `linear-gradient(90deg, ${color}CC, ${color})`,
          boxShadow: `0 0 12px ${color}50, inset 0 1px 0 rgba(255,255,255,0.15)`,
          animation: `arena-bar-fill 0.8s ease ${delay + 0.2}s both`,
        } as React.CSSProperties} />
      </div>
      <span style={{
        width: 80, fontSize: 10, color, textAlign: 'right', flexShrink: 0,
        fontWeight: 600, fontVariantNumeric: 'tabular-nums',
      }}>{value}</span>
    </div>
  );
}

const LEFT_W = 260;
const RIGHT_W = 280;
const BOTTOM_H = 150;

function ImmersiveHUD({ entry, onClose, history, logs }: { entry: BlockEntry; onClose: () => void; history: MetricSnapshot[]; logs: TraceLog[] }) {
  const running = entry.endTime === null;
  // Live elapsed timer — ticks every 100ms while running
  const [liveElapsed, setLiveElapsed] = useState(() =>
    running ? (Date.now() - entry.startTime) / 1000 : entry.elapsed
  );
  useEffect(() => {
    if (!running) { setLiveElapsed(entry.elapsed); return; }
    const id = setInterval(() => setLiveElapsed((Date.now() - entry.startTime) / 1000), 100);
    return () => clearInterval(id);
  }, [running, entry.startTime, entry.elapsed]);
  const pe = entry.profileEvents;
  const cacheTotal = pe ? pe.markCacheHits + pe.markCacheMisses : 0;
  const cacheHitPct = cacheTotal > 0 ? (pe!.markCacheHits / cacheTotal) * 100 : 0;

  const ioRate = entry.isMerge ? (entry.readBytesPerSec || 0) : entry.ioReadRate;
  const progressPct = entry.isMerge ? entry.progress * 100 : entry.progress;
  const accent = entry.color;
  const tableInfo = entry.tableHint !== 'unknown' ? entry.tableHint
    : (entry.database && entry.table ? `${entry.database}.${entry.table}` : '');

  const hasHistory = history.length >= 2;
  const showSparks = running || hasHistory;
  const cpuData = history.map(h => h.cpu);
  const memData = history.map(h => h.mem);
  const ioData = history.map(h => h.io);
  const rowsData = history.map(h => h.rows);
  const writeData = history.map(h => h.write);
  const progressData = history.map(h => h.progress);
  const sparkW = RIGHT_W - 40;

  return (
    <div style={{
      position: 'absolute', inset: 0, zIndex: 20,
      pointerEvents: 'none',
      fontFamily: panelFont,
    }}>

      {/* ─── LEFT PANEL: Identity + Details ─── */}
      <div style={{
        position: 'absolute', top: 0, left: 0, bottom: BOTTOM_H, width: LEFT_W,
        padding: '50px 18px 16px',
        display: 'flex', flexDirection: 'column', gap: 0,
        animation: 'arena-fade-in 0.3s ease both',
        overflowX: 'hidden', overflowY: 'auto',
        scrollbarWidth: 'none',
        pointerEvents: 'auto',
      }}>
        {/* Scan line */}
        <div style={{
          position: 'absolute', left: 0, right: 0, height: 2,
          background: `linear-gradient(90deg, transparent, ${accent}80, transparent)`,
          animation: 'arena-scan 3s linear infinite',
          pointerEvents: 'none',
        }} />

        {/* Kind badge + status */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 10, flexWrap: 'wrap' }}>
          <span style={{
            fontSize: 10, fontWeight: 700, letterSpacing: 2, color: accent,
            background: `${accent}20`, padding: '2px 8px', borderRadius: 4,
            border: `1px solid ${accent}30`,
          }}>{entry.kind}</span>
          {running ? (
            <span style={{
              fontSize: 9, color: '#4ade80', letterSpacing: 1.5,
              animation: 'arena-glow-pulse 2s ease infinite',
            }}>● LIVE</span>
          ) : (
            <span style={{ fontSize: 9, color: '#fbbf24', letterSpacing: 1.5 }}>■ FINISHED</span>
          )}
        </div>

        {/* Table name */}
        {tableInfo && (
          <div style={{
            fontSize: 11, color: 'rgba(255,255,255,0.6)', marginBottom: 2,
            fontWeight: 600, letterSpacing: 0.5,
            animation: 'arena-fade-in 0.4s ease 0.1s both',
            overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
          }}>{tableInfo}</div>
        )}

        {/* Part name (merges) */}
        {entry.isMerge && entry.partName && (
          <div style={{
            fontSize: 10, color: 'rgba(255,255,255,0.5)', marginBottom: 4,
            fontFamily: panelFont, letterSpacing: 0.3,
            animation: 'arena-fade-in 0.4s ease 0.12s both',
            overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
          }}>&rarr; {entry.partName}</div>
        )}

        {/* Query text (skip for merges — table+part already shown above) */}
        {!entry.isMerge && (
          <div style={{
            fontSize: 9, color: 'rgba(255,255,255,0.3)', marginBottom: 12,
            lineHeight: 1.4, maxHeight: 44, overflow: 'hidden',
            animation: 'arena-fade-in 0.4s ease 0.15s both',
          }}>{entry.label}</div>
        )}

        {/* Big elapsed time */}
        <div style={{
          display: 'flex', alignItems: 'baseline', gap: 8, marginBottom: 16,
          animation: 'arena-fade-in 0.4s ease 0.2s both',
        }}>
          <span style={{ fontSize: 32, fontWeight: 700, color: accent, letterSpacing: -1 }}>
            {liveElapsed.toFixed(1)}
          </span>
          <span style={{ fontSize: 12, color: `${accent}80`, fontWeight: 600 }}>sec</span>
        </div>


        {/* Profile events snapshot or merge info */}
        {pe && (
          <div style={{ animation: 'arena-fade-in 0.4s ease 0.5s both' }}>
            <div style={{
              fontSize: 8, color: 'rgba(255,255,255,0.25)', letterSpacing: 2, fontWeight: 700, marginBottom: 6,
            }}>PROFILE EVENTS</div>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '6px 12px', fontSize: 10 }}>
              {[
                { label: 'USR TIME', value: fmtMicro(pe.userTimeMicroseconds), color: '#60a5fa' },
                { label: 'SYS TIME', value: fmtMicro(pe.systemTimeMicroseconds), color: '#f472b6' },
                { label: 'DISK RD', value: formatBytes(pe.osReadBytes), color: '#34d399' },
                { label: 'DISK WR', value: formatBytes(pe.osWriteBytes), color: '#fb923c' },
                { label: 'PARTS', value: fmtNum(pe.selectedParts), color: 'rgba(255,255,255,0.5)' },
                { label: 'MARKS', value: fmtNum(pe.selectedMarks), color: 'rgba(255,255,255,0.5)' },
              ].map((item, i) => (
                <div key={item.label} style={{ animation: `arena-fade-in 0.3s ease ${0.55 + i * 0.04}s both` }}>
                  <div style={{ fontSize: 8, color: 'rgba(255,255,255,0.25)', letterSpacing: 1.2, marginBottom: 1 }}>{item.label}</div>
                  <div style={{ color: item.color, fontWeight: 600, fontVariantNumeric: 'tabular-nums' }}>{item.value}</div>
                </div>
              ))}
            </div>
            {cacheTotal > 0 && (
              <div style={{ marginTop: 8 }}>
                <HudBar label="CACHE" ratio={cacheHitPct / 100}
                  color={cacheHitPct > 80 ? '#4ade80' : cacheHitPct > 50 ? '#fbbf24' : '#ef4444'}
                  value={`${cacheHitPct.toFixed(1)}%`} delay={0.8} />
              </div>
            )}
          </div>
        )}

        {entry.isMerge && !pe && (
          <div style={{ animation: 'arena-fade-in 0.4s ease 0.5s both' }}>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '8px 14px', fontSize: 12 }}>
              {[
                { label: 'ROWS', value: fmtNum(entry.rowsRead), color: 'rgba(255,255,255,0.6)' },
                ...(entry.numParts !== undefined ? [{ label: 'PARTS', value: String(entry.numParts), color: 'rgba(255,255,255,0.6)' }] : []),
                ...(entry.mergeType ? [{ label: 'TYPE', value: entry.mergeType, color: 'rgba(255,255,255,0.6)' }] : []),
              ].map((item, i) => (
                <div key={item.label} style={{ animation: `arena-fade-in 0.3s ease ${0.55 + i * 0.04}s both` }}>
                  <div style={{ fontSize: 9, color: 'rgba(255,255,255,0.3)', letterSpacing: 1.2, marginBottom: 2 }}>{item.label}</div>
                  <div style={{ color: item.color, fontWeight: 600 }}>{item.value}</div>
                </div>
              ))}
            </div>
          </div>
        )}

        {entry.user && (
          <div style={{ marginTop: 'auto', fontSize: 8, color: 'rgba(255,255,255,0.15)' }}>{entry.user}</div>
        )}
      </div>

      {/* ─── RIGHT EDGE: Floating sparklines (no background) ─── */}
      {showSparks && (
        <div style={{
          position: 'absolute', top: 44, right: 12, bottom: BOTTOM_H + 8,
          width: RIGHT_W,
          display: 'flex', flexDirection: 'column', justifyContent: 'center', gap: 2,
          animation: 'arena-fade-in 0.4s ease 0.2s both',
          pointerEvents: 'none',
        }}>
          <Sparkline data={cpuData} color="#60a5fa" label="CPU" width={sparkW} height={28}
            currentValue={`${entry.cpu.toFixed(2)} cores`} />
          <Sparkline data={memData} color="#a78bfa" label="MEM" width={sparkW} height={28}
            currentValue={formatBytes(entry.mem)} />
          <Sparkline data={ioData} color="#34d399" label="I/O" width={sparkW} height={28}
            currentValue={fmtRate(ioRate)} />
          {entry.isMerge ? (
            <Sparkline data={writeData} color="#fb923c" label="THROUGHPUT" width={sparkW} height={28}
              currentValue={fmtRate(entry.writeBytesPerSec || 0)} />
          ) : (
            <Sparkline data={rowsData} color="#f472b6" label="ROWS" width={sparkW} height={28}
              currentValue={fmtNum(entry.rowsRead)} />
          )}
          {progressPct > 0 && (
            <Sparkline data={progressData}
              color={progressPct > 75 ? '#4ade80' : progressPct > 40 ? '#fbbf24' : accent}
              label="PROGRESS" width={sparkW} height={28}
              currentValue={`${progressPct.toFixed(1)}%`} />
          )}
        </div>
      )}

      {/* ─── BOTTOM STRIP: Trace Logs (full width) ─── */}
      <div style={{
        position: 'absolute',
        bottom: 0, left: 0, right: 0,
        height: BOTTOM_H,
        background: 'linear-gradient(0deg, rgba(4,4,12,0.72) 0%, rgba(4,4,12,0.55) 70%, transparent 100%)',
        padding: '24px 20px 14px',
        display: 'flex', flexDirection: 'column',
        animation: 'arena-fade-in 0.4s ease 0.4s both',
        pointerEvents: 'auto',
      }}>
        <div style={{
          fontSize: 8, color: 'rgba(255,255,255,0.15)', letterSpacing: 2, fontWeight: 700, marginBottom: 4, flexShrink: 0,
          display: 'flex', alignItems: 'center', gap: 8,
        }}>
          TRACE LOGS
          {logs.length > 0 && (
            <span style={{ fontSize: 8, color: 'rgba(255,255,255,0.1)', fontWeight: 400, letterSpacing: 0 }}>
              {logs.length} entries
            </span>
          )}
        </div>
        <LogTail logs={logs} maxLines={4} elapsedSec={entry.elapsed} />
      </div>

      {/* ESC hint */}
      <div style={{
        position: 'absolute', bottom: 12, right: 20,
        fontSize: 9, color: 'rgba(255,255,255,0.12)', letterSpacing: 1.5,
        animation: 'arena-fade-in 0.5s ease 1s both',
        pointerEvents: 'auto', cursor: 'pointer',
      }} onClick={onClose}>
        ESC or click to exit
      </div>

      {/* Top-right: query ID */}
      {entry.queryId && (
        <div style={{
          position: 'absolute', top: 14, right: 20,
          fontSize: 8, color: 'rgba(255,255,255,0.1)', letterSpacing: 1,
          animation: 'arena-fade-in 0.4s ease 0.6s both',
        }}>{entry.queryId}</div>
      )}

      {/* Accent border glow on left */}
      <div style={{
        position: 'absolute', top: 0, left: 0, bottom: 0, width: 2,
        background: `linear-gradient(180deg, transparent, ${accent}, transparent)`,
        boxShadow: `0 0 20px ${accent}40`,
      }} />

    </div>
  );
}

/* ── DOM Overlay: Hover Card ─────────────────────────── */

function HoverCardOverlay({ entry, domRef }: {
  entry: BlockEntry;
  domRef: React.RefObject<HTMLDivElement | null>;
}) {
  const running = entry.endTime === null;
  const cpuRatio = Math.min(1, entry.cpu / 8);
  const memRatio = Math.min(1, entry.mem / (4 * 1073741824));
  const ioRate = entry.isMerge ? (entry.readBytesPerSec || 0) : entry.ioReadRate;
  const ioRatio = Math.min(1, ioRate / (100 * 1048576));
  const tableInfo = entry.tableHint !== 'unknown' ? entry.tableHint : (entry.database && entry.table ? `${entry.database}.${entry.table}` : '');
  const accent = entry.color;

  return (
    <div ref={domRef as React.RefObject<HTMLDivElement>} style={{
      position: 'absolute', top: 0, left: 0,
      pointerEvents: 'none', zIndex: 5,
    }}>
      <div style={{
        fontFamily: panelFont,
        fontSize: 11,
        color: 'rgba(255,255,255,0.75)',
        background: 'rgba(8,8,18,0.90)',
        border: `1px solid ${accent}35`,
        borderRadius: 8,
        padding: '10px 14px',
        width: 320,
        boxShadow: `0 0 30px ${accent}20, 0 4px 16px rgba(0,0,0,0.4)`,
        userSelect: 'none',
        transform: 'translate(-50%, -100%)',
      }}>
        {/* Row 1: kind + status + elapsed */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
          <span style={{
            fontSize: 10, fontWeight: 700, letterSpacing: 1.2, color: accent,
            background: `${accent}18`, padding: '2px 6px', borderRadius: 3,
          }}>{entry.kind}</span>
          {running && <span style={{ fontSize: 9, color: '#4ade80' }}>● LIVE</span>}
          <span style={{ marginLeft: 'auto', fontSize: 15, fontWeight: 700, color: accent }}>
            {entry.elapsed.toFixed(1)}s
          </span>
        </div>

        {/* Row 2: table / target */}
        {tableInfo && (
          <div style={{
            fontSize: 10, color: 'rgba(255,255,255,0.4)', marginBottom: 4,
            whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis',
            fontWeight: 600,
          }}>
            {tableInfo}
          </div>
        )}

        {/* Row 3: query text or dest part for merges */}
        {entry.isMerge && entry.partName ? (
          <div style={{
            fontSize: 10, color: 'rgba(255,255,255,0.4)', marginBottom: 8,
            whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis',
            lineHeight: '1.3',
          }}>
            &rarr; {entry.partName}
          </div>
        ) : !entry.isMerge && (
          <div style={{
            fontSize: 10, color: 'rgba(255,255,255,0.5)', marginBottom: 8,
            overflow: 'hidden', textOverflow: 'ellipsis',
            display: '-webkit-box', WebkitLineClamp: 2, WebkitBoxOrient: 'vertical' as const,
            lineHeight: '1.3',
          }}>
            {entry.label}
          </div>
        )}

        {/* Row 4: resource bars */}
        <div style={{
          display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '4px 14px',
          fontSize: 10, color: 'rgba(255,255,255,0.45)',
          borderTop: '1px solid rgba(255,255,255,0.06)', paddingTop: 6,
        }}>
          <div>
            <span style={{ fontSize: 9, color: 'rgba(255,255,255,0.3)' }}>CPU </span>
            <span style={{ fontWeight: 600 }}>{entry.cpu.toFixed(2)}</span>
            <MiniBar ratio={cpuRatio} color="#60a5fa" />
          </div>
          <div>
            <span style={{ fontSize: 9, color: 'rgba(255,255,255,0.3)' }}>MEM </span>
            <span style={{ fontWeight: 600 }}>{formatBytes(entry.mem)}</span>
            <MiniBar ratio={memRatio} color="#a78bfa" />
          </div>
          <div>
            <span style={{ fontSize: 9, color: 'rgba(255,255,255,0.3)' }}>I/O </span>
            <span style={{ fontWeight: 600 }}>{fmtRate(ioRate)}</span>
            <MiniBar ratio={ioRatio} color="#34d399" />
          </div>
          <div>
            <span style={{ fontSize: 9, color: 'rgba(255,255,255,0.3)' }}>ROWS </span>
            <span style={{ fontWeight: 600 }}>{fmtNum(entry.rowsRead)}</span>
          </div>
        </div>

        {/* Row 5: user + hint */}
        <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: 5 }}>
          {entry.user && (
            <span style={{ fontSize: 9, color: 'rgba(255,255,255,0.25)' }}>{entry.user}</span>
          )}
          <span style={{ fontSize: 8, color: 'rgba(255,255,255,0.15)', marginLeft: 'auto' }}>click to inspect</span>
        </div>
      </div>
    </div>
  );
}

/* ── Floor + time markers ─────────────────────────────── */

const noRay = () => {};
const TIME_MARKS = [10, 30, 60, 120, 300];

const DECK_LABELS: Record<Deck, { label: string; color: string }> = {
  select: { label: 'QUERIES', color: '#3B82F6' },
  insert: { label: 'INSERTS', color: '#10B981' },
  merge:  { label: 'MERGES',  color: '#F59E0B' },
};

const FLOOR_FRONT_PAD = 4; // extra Z toward the camera so the floor doesn't end abruptly

function Floor({ maxLane = 0 }: { maxLane?: number }) {
  const horizonX = HORIZON * Ts;
  const floorW = horizonX + 2;
  const targetDepth = Math.max(6, (maxLane + 1) * LANE_GAP + 2) + FLOOR_FRONT_PAD;

  const depthRef = useRef(targetDepth);
  const floorMeshRef = useRef<THREE.Mesh>(null);
  const gridRef = useRef<THREE.GridHelper>(null);
  const nowLineRef = useRef<THREE.Mesh>(null);
  const timeLineRefs = useRef<Map<number, THREE.Mesh>>(new Map());

  useFrame(() => {
    const d = depthRef.current;
    const t = targetDepth;
    if (Math.abs(d - t) > 0.01) {
      depthRef.current += (t - d) * 0.08;
    } else {
      depthRef.current = t;
    }
    const fd = depthRef.current;
    const zCenter = -fd / 2 + FLOOR_FRONT_PAD;
    if (floorMeshRef.current) {
      floorMeshRef.current.position.set(-(floorW / 2), -0.005, zCenter);
      const g = floorMeshRef.current.geometry as THREE.PlaneGeometry;
      if (Math.abs(g.parameters.height - fd) > 0.05) {
        floorMeshRef.current.geometry.dispose();
        floorMeshRef.current.geometry = new THREE.PlaneGeometry(floorW, fd);
      }
    }
    if (gridRef.current) {
      gridRef.current.position.set(-(horizonX / 2), 0, zCenter);
    }
    if (nowLineRef.current) {
      nowLineRef.current.position.set(0, 0.01, zCenter);
      const g = nowLineRef.current.geometry as THREE.PlaneGeometry;
      if (Math.abs(g.parameters.height - fd) > 0.05) {
        nowLineRef.current.geometry.dispose();
        nowLineRef.current.geometry = new THREE.PlaneGeometry(0.04, fd);
      }
    }
    for (const [, mesh] of timeLineRefs.current) {
      mesh.position.set(mesh.position.x, 0.01, zCenter);
      const g = mesh.geometry as THREE.PlaneGeometry;
      if (Math.abs(g.parameters.height - fd) > 0.05) {
        mesh.geometry.dispose();
        mesh.geometry = new THREE.PlaneGeometry(0.02, fd);
      }
    }
  });

  return (
    <>
      <mesh ref={floorMeshRef} rotation={[-Math.PI / 2, 0, 0]} position={[-(floorW / 2), -0.005, -targetDepth / 2 + FLOOR_FRONT_PAD]} raycast={noRay}>
        <planeGeometry args={[floorW, targetDepth]} />
        <meshStandardMaterial color="#0a0a14" roughness={0.9} />
      </mesh>

      <gridHelper
        ref={gridRef}
        args={[horizonX, 60, 0x181830, 0x0e0e1c]}
        position={[-(horizonX / 2), 0, -targetDepth / 2 + FLOOR_FRONT_PAD]}
        raycast={noRay}
      />

      <mesh ref={nowLineRef} position={[0, 0.01, -targetDepth / 2 + FLOOR_FRONT_PAD]} rotation={[-Math.PI / 2, 0, 0]} raycast={noRay}>
        <planeGeometry args={[0.04, targetDepth]} />
        <meshBasicMaterial color="#3B82F6" transparent opacity={0.6} />
      </mesh>

      {TIME_MARKS.map(sec => {
        const x = -sec * Ts;
        return (
          <group key={sec}>
            <mesh
              ref={el => { if (el) timeLineRefs.current.set(sec, el); else timeLineRefs.current.delete(sec); }}
              position={[x, 0.01, -targetDepth / 2 + FLOOR_FRONT_PAD]}
              rotation={[-Math.PI / 2, 0, 0]}
              raycast={noRay}
            >
              <planeGeometry args={[0.02, targetDepth]} />
              <meshBasicMaterial color="#ffffff" transparent opacity={0.08} />
            </mesh>
            <Text
              position={[x, 0.02, 1.5]}
              rotation={[-Math.PI / 2, 0, 0]}
              fontSize={0.22}
              color="#ffffff"
              anchorX="center"
              fillOpacity={0.2}
              raycast={noRay}
            >
              {sec >= 60 ? `${sec / 60}m ago` : `${sec}s ago`}
            </Text>
          </group>
        );
      })}

      <Text
        position={[0.2, 0.02, 1.5]}
        rotation={[-Math.PI / 2, 0, 0]}
        fontSize={0.28}
        color="#3B82F6"
        anchorX="left"
        fillOpacity={0.5}
        fontWeight={700}
        raycast={noRay}
      >
        NOW
      </Text>

      {DECK_ORDER.map(deck => {
        const y = deckBaseY(deck);
        const { label, color } = DECK_LABELS[deck];
        return (
          <group key={deck}>
            <mesh position={[-(floorW / 2), y + 0.002, 0.3]} rotation={[-Math.PI / 2, 0, 0]} raycast={noRay}>
              <planeGeometry args={[floorW, 0.01]} />
              <meshBasicMaterial color={color} transparent opacity={0.12} />
            </mesh>
            <Text
              position={[1.5, y + 0.15, 0.8]}
              fontSize={0.22}
              color={color}
              anchorX="left"
              fillOpacity={0.35}
              fontWeight={700}
              raycast={noRay}
            >
              {label}
            </Text>
          </group>
        );
      })}
    </>
  );
}


/* ── Scene ────────────────────────────────────────────── */

const HOST_LANE_BLOCK = 6;
const HOST_COLORS = ['#60a5fa', '#f472b6', '#4ade80', '#fbbf24', '#a78bfa', '#fb923c'];

function HostLabels({ hosts }: { hosts: string[] }) {
  if (hosts.length < 2) return null;
  const horizonX = HORIZON * Ts;
  return (
    <>
      {hosts.map((host, i) => {
        const laneCenter = -(i * HOST_LANE_BLOCK + HOST_LANE_BLOCK / 2) * LANE_GAP;
        const color = HOST_COLORS[i % HOST_COLORS.length];
        // Separator line between host groups
        const sepZ = i > 0 ? -(i * HOST_LANE_BLOCK - 0.5) * LANE_GAP : null;
        return (
          <group key={host}>
            {/* Label near the NOW line */}
            <Text
              position={[1.2, 0.08, laneCenter]}
              rotation={[-Math.PI / 2, 0, 0]}
              fontSize={0.35}
              color={color}
              anchorX="left"
              fillOpacity={0.7}
              fontWeight={700}
              raycast={noRay}
            >
              {host}
            </Text>
            {sepZ !== null && (
              <mesh position={[-(horizonX / 2), 0.01, sepZ]} rotation={[-Math.PI / 2, 0, 0]} raycast={noRay}>
                <planeGeometry args={[horizonX + 4, 0.02]} />
                <meshBasicMaterial color={color} transparent opacity={0.2} />
              </mesh>
            )}
          </group>
        );
      })}
    </>
  );
}

interface CinematicSettings {
  autoRotate: boolean;
  rotateSpeed: number;
  tilt: number;       // polar angle 0..PI/2
  distance: number;
}

const DEFAULT_CINEMATIC: CinematicSettings = {
  autoRotate: false,
  rotateSpeed: 0.4,
  tilt: 0.85,
  distance: 12,
};

function CinematicOrbit({ settings }: { settings: CinematicSettings }) {
  const controlsRef = useRef<OrbitControlsImpl>(null);

  useFrame(() => {
    const c = controlsRef.current;
    if (!c) return;
    if (settings.autoRotate) {
      c.autoRotate = true;
      c.autoRotateSpeed = settings.rotateSpeed * 2;
    } else {
      c.autoRotate = false;
    }
  });

  return (
    <OrbitControls
      ref={controlsRef}
      target={[-3, DECK_GAP, -1]}
      enableDamping
      dampingFactor={0.05}
      maxPolarAngle={Math.PI / 2.05}
      minDistance={2}
      maxDistance={40}
    />
  );
}

/** Cheat mode bundle passed to ArenaScene — null when cheat is off */
interface CheatBundle {
  state: CheatModeState;
  infoMap: Map<string, BlockCheatInfo>;
  shooting: ReturnType<typeof useShootingSystem>;
  onHit: (entry: BlockEntry, position: THREE.Vector3, killed: boolean) => void;
  onAimUpdate: (entry: BlockEntry | null) => void;
}

function ArenaScene({ entries, hoveredId, selectedId, hoveredEntry, selectedEntry, onHover, onClick, hoverDomRef, hostNames, cinematic, cheat }: {
  entries: BlockEntry[];
  hoveredId: string | null;
  selectedId: string | null;
  hoveredEntry: BlockEntry | null;
  selectedEntry: BlockEntry | null;
  onHover: (v: BlockEntry | null) => void;
  onClick: (v: BlockEntry) => void;
  hoverDomRef: React.RefObject<HTMLDivElement | null>;
  hostNames: string[];
  cinematic: CinematicSettings;
  cheat: CheatBundle | null;
}) {
  const cheating = !!cheat;
  const visibleEntries = cheat
    ? entries.filter(e => !cheat.state.destroyedIds.has(e.id))
    : entries;

  return (
    <>
      <color attach="background" args={[cheating ? '#0a0a18' : '#1a1a2e']} />
      <fog attach="fog" args={[cheating ? '#0a0a18' : '#1a1a2e', cheating ? 20 : 15, cheating ? 45 : 30]} />
      <ambientLight intensity={cheating ? 0.15 : 0.3} />
      <directionalLight position={[5, 15, 10]} intensity={cheating ? 0.5 : 0.8} />
      <directionalLight position={[-5, 8, -5]} intensity={0.3} color="#8B5CF6" />
      <pointLight position={[0, 6, -3]} intensity={0.5} color="#3B82F6" distance={20} />

      <Floor maxLane={entries.reduce((m, e) => Math.max(m, e.lane), 0)} />
      <HostLabels hosts={hostNames} />
      {visibleEntries.map(e => (
        <LiveBlock
          key={e.id}
          entry={e}
          hoveredId={cheating ? null : hoveredId}
          selectedId={cheating ? null : selectedId}
          onHover={cheating ? () => {} : onHover}
          onClick={cheating ? () => {} : onClick}
          cheatInfo={cheat ? getOrCreateCheatInfo(cheat.infoMap, e) : undefined}
        />
      ))}

      {!cheating && (
        <CardPositionBridge
          entry={hoveredEntry && !selectedEntry ? hoveredEntry : null}
          domRef={hoverDomRef}
          offsetY={0.15}
          offsetZ={0.5}
        />
      )}

      {cheat ? (
        <CheatSceneElements
          entries={entries}
          cheatState={cheat.state}
          cheatInfoMap={cheat.infoMap}
          shooting={cheat.shooting}
          onCheatHit={cheat.onHit}
          onAimUpdate={cheat.onAimUpdate}
        />
      ) : (
        <CinematicOrbit settings={cinematic} />
      )}
    </>
  );
}

/* ── Main wrapper ────────────────────────────────────── */

export interface ResourceArena3DProps {
  queries: RunningQueryInfo[];
  merges: ActiveMergeInfo[];
  cpuUsage: number;
  memoryPct: number;
  onQueryClick?: (queryId: string) => void;
  /** Compact mode for split-by-host view — shorter height, no legend */
  compact?: boolean;
  /** Whether split view is available (multi-host cluster) */
  splitAvailable?: boolean;
  /** Whether split view is active */
  splitActive?: boolean;
  /** Toggle split view */
  onSplitToggle?: () => void;
}

const hdrLabel: React.CSSProperties = {
  fontSize: 10, fontFamily: 'monospace', color: 'rgba(255,255,255,0.35)', fontWeight: 600, letterSpacing: 1,
};
const hdrVal: React.CSSProperties = { marginLeft: 6, fontWeight: 700 };

export const ResourceArena3D: React.FC<ResourceArena3DProps> = ({
  queries, merges, cpuUsage, memoryPct, compact,
  splitAvailable, splitActive, onSplitToggle,
}) => {
  const [hovered, setHovered] = useState<BlockEntry | null>(null);
  const [selected, setSelected] = useState<BlockEntry | null>(null);

  const registryRef = useRef<Map<string, BlockEntry>>(new Map());
  const [visibleIds, setVisibleIds] = useState<string[]>([]);

  const hoverCardRef = useRef<HTMLDivElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const [showCinematic, setShowCinematic] = useState(false);
  const [cinematic, setCinematic] = useState<CinematicSettings>({ ...DEFAULT_CINEMATIC });

  // ── Cheat mode ──
  const clearSelection = useCallback(() => { setSelected(null); setHovered(null); }, []);
  const {
    cheatState, cheatInfoMap, shootingSystem, aimedEntry,
    toggleCheatMode, handleCheatHit, handleAimUpdate, handleKeyDown,
  } = useCheatMode({ containerRef, clearSelection });

  const metricHistory = useMetricHistory(selected);
  const { logs: traceLogs, prefetch: prefetchLogs } = useLogTail(selected);

  const handleHover = useCallback((v: BlockEntry | null) => {
    setHovered(v);
  }, []);

  const handleClick = useCallback((v: BlockEntry) => {
    setSelected(prev => {
      if (prev?.id === v.id) return null;
      // Fire log fetch immediately — don't wait for React effect cycle
      prefetchLogs(v);
      return v;
    });
  }, [prefetchLogs]);

  const handleBgClick = useCallback(() => {
    setSelected(null);
    setHovered(null);
  }, []);

  // ESC key to deselect
  useEffect(() => {
    if (!selected) return;
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') { setSelected(null); setHovered(null); } };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [selected]);

  // Fullscreen toggle
  const toggleFullscreen = useCallback(() => {
    const el = containerRef.current;
    if (!el) return;
    if (!document.fullscreenElement) {
      el.requestFullscreen();
    } else {
      document.exitFullscreen();
    }
  }, []);

  useEffect(() => {
    const onFsChange = () => setIsFullscreen(!!document.fullscreenElement);
    document.addEventListener('fullscreenchange', onFsChange);
    return () => document.removeEventListener('fullscreenchange', onFsChange);
  }, []);

  // Keyboard shortcuts: f=fullscreen, c=cinematic, x/ESC=cheat mode
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.ctrlKey || e.metaKey || e.altKey) return;
      if (e.target instanceof HTMLInputElement || e.target instanceof HTMLTextAreaElement) return;
      if (handleKeyDown(e)) return; // cheat mode consumed the key
      if (e.key === 'f') toggleFullscreen();
      if (e.key === 'c') setShowCinematic(v => !v);
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [toggleFullscreen, handleKeyDown]);

  // Update registry when poll data changes
  const prevPollRef = useRef<{ q: RunningQueryInfo[]; m: ActiveMergeInfo[] }>({ q: [], m: [] });
  if (queries !== prevPollRef.current.q || merges !== prevPollRef.current.m) {
    prevPollRef.current = { q: queries, m: merges };
    const registry = registryRef.current;
    const now = Date.now();

    type Item = {
      id: string; kind: string; elapsed: number; cpu: number; mem: number;
      label: string; tableHint: string; isMerge: boolean;
      queryId?: string; user?: string; progress: number;
      ioReadRate: number; rowsRead: number; bytesRead: number;
      profileEvents?: RunningQueryInfo['profileEvents'];
      readBytesPerSec?: number; writeBytesPerSec?: number;
      numParts?: number; mergeType?: string;
      database?: string; table?: string; partName?: string;
      hostname?: string;
    };

    const items: Item[] = [
      ...queries.map(q => {
        const tm = q.query.match(/(?:FROM|INTO|TABLE|JOIN)\s+([`"]?[\w.]+[`"]?)/i);
        const tableHint = tm ? tm[1].replace(/[`"]/g, '') : 'unknown';
        return {
          id: q.queryId, kind: q.queryKind || 'OTHER',
          elapsed: q.elapsed, cpu: q.cpuCores, mem: q.memoryUsage,
          label: truncateQuery(q.query, 50), tableHint, isMerge: false,
          queryId: q.queryId, user: q.user, progress: q.progress,
          ioReadRate: q.ioReadRate, rowsRead: q.rowsRead, bytesRead: q.bytesRead,
          profileEvents: q.profileEvents,
          hostname: q.hostname,
        };
      }),
      ...merges.map(m => ({
        id: `m:${m.hostname ? m.hostname + ':' : ''}${m.database}.${m.table}.${m.partName}`,
        kind: m.isMutation ? 'MUTATION' : 'MERGE',
        elapsed: m.elapsed, cpu: m.cpuEstimate || 0.3, mem: m.memoryUsage,
        label: `${m.database}.${m.table}`, tableHint: `${m.database}.${m.table}`, isMerge: true,
        progress: m.progress,
        ioReadRate: m.readBytesPerSec, rowsRead: m.rowsRead, bytesRead: 0,
        readBytesPerSec: m.readBytesPerSec, writeBytesPerSec: m.writeBytesPerSec,
        numParts: m.numParts, mergeType: m.mergeType,
        database: m.database, table: m.table, partName: m.partName,
        hostname: m.hostname,
      })),
    ];

    const currentIds = new Set(items.map(it => it.id));

    // If poll returns nothing but registry has active (running) entries, this is a
    // connection switch — the stale data from the old server should not fade; clear immediately.
    const hasActive = items.length === 0 && [...registry.values()].some(e => e.endTime === null);
    if (hasActive) {
      registry.clear();
      setSelected(null);
      setVisibleIds([]);
      return;
    }

    for (const [, entry] of registry) {
      if (!currentIds.has(entry.id) && entry.endTime === null) {
        entry.endTime = now;
        // Update selected state once so HUD reflects finished status
        if (selected && selected.id === entry.id) {
          setSelected({ ...entry });
        }
      }
    }

    for (const [id, entry] of registry) {
      if (entry.endTime !== null && (now - entry.endTime) / 1000 > FADE_SECS) {
        registry.delete(id);
      }
    }

    // Build host index map for split-by-host lane grouping
    // Include hosts from both current items AND registry (fading entries) so indices stay stable
    const hostNames = splitActive
      ? [...new Set([
          ...items.map(it => it.hostname || ''),
          ...[...registry.values()].map(e => e.hostname || ''),
        ].filter(Boolean))].sort()
      : [];
    const hostIndex = new Map<string, number>();
    hostNames.forEach((h, i) => hostIndex.set(h, i));

    const usedLanesPerDeck: Record<Deck, Set<number>> = {
      select: new Set(), insert: new Set(), merge: new Set(),
    };
    for (const e of registry.values()) {
      if (e.endTime === null) usedLanesPerDeck[e.deck].add(e.lane);
    }

    for (const it of items) {
      const existing = registry.get(it.id);
      if (existing) {
        existing.cpu = it.cpu;
        existing.mem = it.mem;
        existing.elapsed = it.elapsed;
        existing.endTime = null;
        existing.progress = it.progress;
        existing.ioReadRate = it.ioReadRate;
        existing.rowsRead = it.rowsRead;
        existing.bytesRead = it.bytesRead;
        existing.profileEvents = it.profileEvents;
        existing.readBytesPerSec = it.readBytesPerSec;
        existing.writeBytesPerSec = it.writeBytesPerSec;
        existing.hostname = it.hostname;
        // Re-assign lane when split mode changes
        if (splitActive && existing.hostname) {
          const hIdx = hostIndex.get(existing.hostname) ?? 0;
          const baseOffset = hIdx * HOST_LANE_BLOCK;
          if (existing.lane < baseOffset || existing.lane >= baseOffset + HOST_LANE_BLOCK) {
            const used = usedLanesPerDeck[existing.deck];
            used.delete(existing.lane);
            let lane = baseOffset;
            while (used.has(lane)) lane++;
            existing.lane = lane;
            used.add(lane);
          }
        } else if (!splitActive && existing.lane >= HOST_LANE_BLOCK) {
          // Compact lanes back when split mode is turned off
          const used = usedLanesPerDeck[existing.deck];
          used.delete(existing.lane);
          let lane = 0;
          while (used.has(lane)) lane++;
          existing.lane = lane;
          used.add(lane);
        }
        if (selected && selected.id === existing.id) {
          setSelected({ ...existing });
        }
      } else {
        const deck = deckOf(it.kind, it.isMerge);
        const used = usedLanesPerDeck[deck];
        const hIdx = splitActive && it.hostname ? (hostIndex.get(it.hostname) ?? 0) : 0;
        const baseOffset = splitActive ? hIdx * HOST_LANE_BLOCK : 0;
        let lane = baseOffset;
        while (used.has(lane)) lane++;
        used.add(lane);
        registry.set(it.id, {
          id: it.id, kind: it.kind, color: colorForBlock(it.kind, it.tableHint),
          label: it.label, tableHint: it.tableHint, isMerge: it.isMerge, deck, lane,
          startTime: now - it.elapsed * 1000,
          endTime: null, cpu: it.cpu, mem: it.mem, elapsed: it.elapsed,
          queryId: it.queryId, user: it.user, progress: it.progress,
          ioReadRate: it.ioReadRate, rowsRead: it.rowsRead, bytesRead: it.bytesRead,
          profileEvents: it.profileEvents,
          readBytesPerSec: it.readBytesPerSec, writeBytesPerSec: it.writeBytesPerSec,
          numParts: it.numParts, mergeType: it.mergeType,
          database: it.database, table: it.table, partName: it.partName,
          hostname: it.hostname,
        });
      }
    }

    if (selected && !registry.has(selected.id)) {
      setSelected(null);
    }

    const newIds = [...registry.keys()].sort();
    const oldIds = [...visibleIds].sort();
    if (newIds.length !== oldIds.length || newIds.some((id, i) => id !== oldIds[i])) {
      setVisibleIds(newIds);
    }
  }

  const entries = [...registryRef.current.values()];
  const hasActivity = entries.some(e => e.endTime === null);

  // Compute sorted host names for 3D labels when split is active
  const activeHostNames = splitActive
    ? [...new Set(entries.map(e => e.hostname).filter(Boolean) as string[])].sort()
    : [];

  const showHover = hovered && !selected;

  return (
    <div
      ref={containerRef}
      style={{
        position: 'relative', borderRadius: isFullscreen ? 0 : 10, overflow: 'hidden',
        background: 'linear-gradient(180deg, #1e1e32 0%, #1a1a2e 40%, #141422 100%)',
        border: isFullscreen ? 'none' : '1px solid rgba(255,255,255,0.08)',
        height: isFullscreen ? '100vh' : 520,
      }}
    >
      {/* Header */}
      <div style={{
        position: 'absolute', top: 0, left: 0, right: 0, zIndex: 10,
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        padding: '8px 14px',
        background: 'linear-gradient(180deg, rgba(12,12,20,0.9) 0%, transparent 100%)',
      }}>
        <div style={{ display: 'flex', gap: 12, alignItems: 'center' }}>
          {splitAvailable && onSplitToggle && (
            <button
              onClick={onSplitToggle}
              title="Split 3D arena by server"
              style={{
                display: 'flex', alignItems: 'center', gap: 4,
                fontSize: 9, fontFamily: 'monospace', fontWeight: 600, letterSpacing: 1,
                color: splitActive ? '#60a5fa' : 'rgba(255,255,255,0.45)',
                background: splitActive ? 'rgba(96,165,250,0.12)' : 'transparent',
                border: 'none', borderRadius: 3, padding: '2px 6px',
                cursor: 'pointer', transition: 'all 0.15s',
              }}
            >
              <svg width="10" height="10" viewBox="0 0 10 10">
                <rect x="0" y="0" width="10" height="4" rx="1" fill="currentColor" />
                <rect x="0" y="6" width="10" height="4" rx="1" fill="currentColor" />
              </svg>
              SPLIT
            </button>
          )}
          <span style={hdrLabel}>
            QUERIES <span style={{ ...hdrVal, color: queries.length > 0 ? '#3B82F6' : 'rgba(255,255,255,0.15)' }}>{queries.length}</span>
          </span>
          {merges.length > 0 && (
            <span style={hdrLabel}>
              MERGES <span style={{ ...hdrVal, color: '#F59E0B' }}>{merges.length}</span>
            </span>
          )}
        </div>
        <div style={{ display: 'flex', gap: 14, alignItems: 'center' }}>
          <span style={{ ...hdrLabel, color: `rgba(59,130,246,${0.5 + Math.min(1, cpuUsage / 100) * 0.5})` }}>
            CPU {cpuUsage.toFixed(1)}%
          </span>
          <span style={{ ...hdrLabel, color: `rgba(139,92,246,${0.5 + (memoryPct / 100) * 0.5})` }}>
            MEM {memoryPct.toFixed(1)}%
          </span>
          <Link
            to="/timetravel"
            state={{ from: { path: '/overview', label: 'Overview' } }}
            style={{ fontSize: 9, fontFamily: 'monospace', color: 'rgba(255,255,255,0.2)', textDecoration: 'none' }}
          >
            Time Travel &rarr;
          </Link>
        </div>
      </div>

      {/* Legend — hidden in compact/split mode */}
      {!compact && (
        <div style={{ position: 'absolute', bottom: 8, left: 14, zIndex: 10, display: 'flex', gap: 10, pointerEvents: 'none' }}>
          {['SELECT', 'INSERT', 'MERGE', 'MUTATION'].map(k => (
            <div key={k} style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
              <div style={{ width: 7, height: 7, borderRadius: 2, background: colorOf(k), opacity: 0.7 }} />
              <span style={{ fontSize: 8, fontFamily: 'monospace', color: 'rgba(255,255,255,0.25)' }}>{k}</span>
            </div>
          ))}
          <span style={{ fontSize: 8, fontFamily: 'monospace', color: 'rgba(255,255,255,0.15)', marginLeft: 8 }}>
            length = duration &middot; tall = cpu &middot; deep = memory
          </span>
        </div>
      )}

      {/* Fullscreen button */}
      <button
        onClick={toggleFullscreen}
        title={isFullscreen ? 'Exit fullscreen (f)' : 'Fullscreen (f)'}
        style={{
          position: 'absolute', bottom: 10, right: 10, zIndex: 10,
          width: 28, height: 28, borderRadius: 6,
          background: 'rgba(255,255,255,0.08)',
          border: '1px solid rgba(255,255,255,0.12)',
          color: 'rgba(255,255,255,0.45)',
          cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center',
          transition: 'all 0.15s',
          backdropFilter: 'blur(8px)',
        }}
        onMouseEnter={e => { e.currentTarget.style.background = 'rgba(255,255,255,0.15)'; e.currentTarget.style.color = 'rgba(255,255,255,0.8)'; }}
        onMouseLeave={e => { e.currentTarget.style.background = 'rgba(255,255,255,0.08)'; e.currentTarget.style.color = 'rgba(255,255,255,0.45)'; }}
      >
        {isFullscreen ? (
          <svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
            <polyline points="6,1 6,6 1,6" /><polyline points="10,15 10,10 15,10" />
            <polyline points="15,6 10,6 10,1" /><polyline points="1,10 6,10 6,15" />
          </svg>
        ) : (
          <svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
            <polyline points="1,6 1,1 6,1" /><polyline points="15,10 15,15 10,15" />
            <polyline points="10,1 15,1 15,6" /><polyline points="6,15 1,15 1,10" />
          </svg>
        )}
      </button>

      {/* 3D Canvas */}
      <Canvas
        camera={{ position: [5, 5, 6], fov: 50, near: 0.1, far: 100 }}
        dpr={[1, 2]}
        gl={{ antialias: true, sortObjects: true, alpha: false }}
        onCreated={({ gl }) => { gl.setClearColor('#1a1a2e', 1); }}
        onPointerMissed={handleBgClick}
        style={{ background: '#1a1a2e' }}
      >
        <ArenaScene
          entries={entries}
          hoveredId={hovered?.id ?? null}
          selectedId={selected?.id ?? null}
          hoveredEntry={hovered}
          selectedEntry={selected}
          onHover={handleHover}
          onClick={handleClick}
          hoverDomRef={hoverCardRef}
          hostNames={activeHostNames}
          cinematic={cinematic}
          cheat={cheatState.active ? {
            state: cheatState,
            infoMap: cheatInfoMap,
            shooting: shootingSystem,
            onHit: handleCheatHit,
            onAimUpdate: handleAimUpdate,
          } : null}
        />
      </Canvas>

      {/* Cheat mode HUD */}
      <CheatHUD state={cheatState} onExit={toggleCheatMode} aimedEntry={aimedEntry} cheatInfoMap={cheatInfoMap} />

      {/* DOM overlay cards */}
      {showHover && !cheatState.active && (
        <HoverCardOverlay entry={hovered} domRef={hoverCardRef} />
      )}
      {selected && !cheatState.active && (
        <ImmersiveHUD entry={selected} onClose={handleBgClick} history={metricHistory} logs={traceLogs} />
      )}

      {/* Cinematic camera controller */}
      {showCinematic && !cheatState.active && (
        <div
          style={{
            position: 'absolute', bottom: 44, right: 10, zIndex: 20,
            width: 220, padding: '10px 12px',
            background: 'rgba(10,10,26,0.92)',
            border: '1px solid rgba(255,255,255,0.12)',
            borderRadius: 8,
            backdropFilter: 'blur(12px)',
            fontFamily: 'monospace', fontSize: 9, color: 'rgba(255,255,255,0.6)',
          }}
          onClick={e => e.stopPropagation()}
        >
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 10 }}>
            <span style={{ fontSize: 9, fontWeight: 700, letterSpacing: 1.5, color: 'rgba(255,255,255,0.4)' }}>CINEMATIC</span>
            <button
              onClick={() => setShowCinematic(false)}
              style={{ background: 'none', border: 'none', color: 'rgba(255,255,255,0.3)', cursor: 'pointer', fontSize: 12, padding: 0, lineHeight: 1 }}
            >&times;</button>
          </div>

          {/* Auto-rotate toggle */}
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
            <span>auto-rotate</span>
            <button
              onClick={() => setCinematic(p => ({ ...p, autoRotate: !p.autoRotate }))}
              style={{
                width: 32, height: 16, borderRadius: 8, border: 'none', cursor: 'pointer',
                background: cinematic.autoRotate ? '#3B82F6' : 'rgba(255,255,255,0.15)',
                position: 'relative', transition: 'background 0.2s',
              }}
            >
              <div style={{
                width: 12, height: 12, borderRadius: 6,
                background: '#fff',
                position: 'absolute', top: 2,
                left: cinematic.autoRotate ? 18 : 2,
                transition: 'left 0.2s',
              }} />
            </button>
          </div>

          {/* Rotate speed */}
          <div style={{ marginBottom: 8 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 3 }}>
              <span>speed</span>
              <span style={{ color: 'rgba(255,255,255,0.3)' }}>{cinematic.rotateSpeed.toFixed(2)}</span>
            </div>
            <input
              type="range" min="0.02" max="3" step="0.02"
              value={cinematic.rotateSpeed}
              onChange={e => setCinematic(p => ({ ...p, rotateSpeed: parseFloat(e.target.value) }))}
              style={{ width: '100%', accentColor: '#3B82F6', height: 2 }}
            />
          </div>

          {/* Presets */}
          <div style={{ display: 'flex', gap: 4, marginTop: 4 }}>
            {[
              { label: 'SLOW', speed: 0.06 },
              { label: 'MEDIUM', speed: 0.6 },
              { label: 'FAST', speed: 1.5 },
            ].map(p => (
              <button
                key={p.label}
                onClick={() => setCinematic(prev => ({ ...prev, autoRotate: true, rotateSpeed: p.speed }))}
                style={{
                  flex: 1, padding: '3px 0', fontSize: 8, fontFamily: 'monospace', fontWeight: 600,
                  letterSpacing: 0.5,
                  background: cinematic.autoRotate && cinematic.rotateSpeed === p.speed ? 'rgba(59,130,246,0.25)' : 'rgba(255,255,255,0.06)',
                  color: cinematic.autoRotate && cinematic.rotateSpeed === p.speed ? '#60a5fa' : 'rgba(255,255,255,0.35)',
                  border: '1px solid rgba(255,255,255,0.08)', borderRadius: 4, cursor: 'pointer',
                  transition: 'all 0.15s',
                }}
              >
                {p.label}
              </button>
            ))}
          </div>

          <div style={{ marginTop: 8, fontSize: 8, color: 'rgba(255,255,255,0.2)', textAlign: 'center' }}>
            press <span style={{ color: 'rgba(255,255,255,0.4)' }}>C</span> to toggle
          </div>
        </div>
      )}

      {/* Idle */}
      {!hasActivity && entries.length === 0 && (
        <div style={{ position: 'absolute', inset: 0, display: 'flex', alignItems: 'center', justifyContent: 'center', pointerEvents: 'none' }}>
          <span style={{ fontSize: 12, fontFamily: 'monospace', color: 'rgba(255,255,255,0.1)' }}>idle</span>
        </div>
      )}
    </div>
  );
};
