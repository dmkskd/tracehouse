/**
 * ResourceArenaSwimlane — Pure 2D Gantt-style swimlane view of the resource arena.
 *
 * Same data as ResourceArena3D but rendered as flat horizontal bars in swim lanes.
 * No Three.js — pure HTML/CSS with requestAnimationFrame for smooth scrolling.
 *
 * Fully self-contained — can be deleted without affecting other arena views.
 */

import React, { useRef, useState, useCallback, useEffect } from 'react';
import type { RunningQueryInfo, ActiveMergeInfo } from '@tracehouse/core';
import { truncateQuery, formatBytes } from '../../utils/formatters';

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
  const h = ((base[0] + hueShift) % 360 + 360) % 360;
  const s = Math.max(0.3, Math.min(1, base[1]));
  const l = Math.max(0.25, Math.min(0.75, base[2] + (t - 0.5) * 0.12));
  return `hsl(${h}, ${(s * 100).toFixed(0)}%, ${(l * 100).toFixed(0)}%)`;
}

function colorOf(kind: string): string {
  const base = BASE_HSL[kind.toUpperCase()] ?? BASE_HSL.OTHER;
  return `hsl(${base[0]}, ${(base[1] * 100).toFixed(0)}%, ${(base[2] * 100).toFixed(0)}%)`;
}

/* ── constants ───────────────────────────────────────── */

const FADE_SECS = 6;
const HORIZON = 300;
const BAR_HEIGHT = 16;
const LANE_GAP = 2;
const DECK_GAP = 6;
const TIME_HEADER_H = 24;
const DECK_LABEL_W = 60;
const HEADER_H = 34;
const LEGEND_H = 28;

type Deck = 'select' | 'insert' | 'merge';
const DECK_ORDER: Deck[] = ['select', 'insert', 'merge'];

function deckOf(kind: string, isMerge: boolean): Deck {
  if (isMerge) return 'merge';
  const k = kind.toUpperCase();
  if (k === 'INSERT') return 'insert';
  return 'select';
}

/* ── swim entry ──────────────────────────────────────── */

interface SwimEntry {
  id: string;
  kind: string;
  color: string;
  label: string;
  tableHint: string;
  isMerge: boolean;
  deck: Deck;
  lane: number;
  startTime: number;
  endTime: number | null;
  cpu: number;
  mem: number;
  elapsed: number;
  queryId?: string;
  user?: string;
  progress: number;
  ioReadRate: number;
  rowsRead: number;
  hostname?: string;
  readBytesPerSec?: number;
  writeBytesPerSec?: number;
  numParts?: number;
  mergeType?: string;
  database?: string;
  table?: string;
  partName?: string;
}

/* ── helpers ─────────────────────────────────────────── */

function fmtRate(bps: number): string {
  if (bps < 1024) return `${bps.toFixed(0)} B/s`;
  if (bps < 1048576) return `${(bps / 1024).toFixed(1)} KB/s`;
  return `${(bps / 1048576).toFixed(1)} MB/s`;
}

function fmtNum(n: number): string {
  if (n < 1000) return n.toFixed(0);
  if (n < 1_000_000) return `${(n / 1000).toFixed(1)}K`;
  return `${(n / 1_000_000).toFixed(1)}M`;
}

const TIME_MARKS = [10, 30, 60, 120, 300];

const DECK_LABELS: Record<Deck, { label: string; color: string }> = {
  select: { label: 'QUERIES', color: '#3B82F6' },
  insert: { label: 'INSERTS', color: '#10B981' },
  merge:  { label: 'MERGES',  color: '#F59E0B' },
};

const panelFont = "'JetBrains Mono', 'Fira Code', 'SF Mono', monospace";

/* ── inject animation styles ─────────────────────────── */

if (typeof document !== 'undefined' && !document.getElementById('arena-swim-styles')) {
  const style = document.createElement('style');
  style.id = 'arena-swim-styles';
  style.textContent = `
    @keyframes swim-live-pulse {
      0%,100% { filter: brightness(1); }
      50% { filter: brightness(1.35); }
    }
    @keyframes swim-fade-in { 0% { opacity: 0; transform: translateY(6px); } 100% { opacity: 1; transform: translateY(0); } }
    @keyframes swim-scan { 0% { top: -2px; } 100% { top: 100%; } }
    @keyframes swim-glow-pulse { 0%,100% { opacity: 0.6; } 50% { opacity: 1; } }
  `;
  document.head.appendChild(style);
}

/* ── SwimBar component ───────────────────────────────── */

const SwimBar: React.FC<{
  entry: SwimEntry;
  containerWidth: number;
  pxPerSec: number;
  now: number;
  isHovered: boolean;
  isSelected: boolean;
  onHover: (e: SwimEntry | null) => void;
  onClick: (e: SwimEntry) => void;
  deckTopY: number;
}> = ({ entry, containerWidth, pxPerSec, now, isHovered, isSelected, onHover, onClick, deckTopY }) => {
  const running = entry.endTime === null;

  const rightEdgeSec = running ? 0 : (now - entry.endTime!) / 1000;
  const leftEdgeSec = (now - entry.startTime) / 1000;

  const rightPx = containerWidth - DECK_LABEL_W - rightEdgeSec * pxPerSec;
  const leftPx = containerWidth - DECK_LABEL_W - leftEdgeSec * pxPerSec;

  const topPx = deckTopY + entry.lane * (BAR_HEIGHT + LANE_GAP);

  // Simple opacity: full for live, fade for finished
  let opacity = 0.85;
  if (!running) {
    const finAge = (now - entry.endTime!) / 1000;
    opacity = 0.5 * Math.pow(1 - Math.min(1, finAge / FADE_SECS), 1.5);
  }
  if (isHovered || isSelected) opacity = 1;

  const isHighlighted = isHovered || isSelected;
  const showLabel = (rightPx - Math.max(DECK_LABEL_W, leftPx)) > 70;

  return (
    <div
      style={{
        position: 'absolute',
        left: Math.max(DECK_LABEL_W, leftPx),
        top: topPx,
        width: Math.max(3, rightPx - Math.max(DECK_LABEL_W, leftPx)),
        height: BAR_HEIGHT,
        background: entry.color,
        opacity,
        borderRadius: 3,
        cursor: 'pointer',
        overflow: 'hidden',
        transition: 'left 1s linear, width 1s linear, opacity 0.3s ease',
        border: isHighlighted
          ? '1px solid rgba(255,255,255,0.5)'
          : '1px solid rgba(255,255,255,0.06)',
        animation: running ? 'swim-live-pulse 2s ease infinite' : undefined,
        display: 'flex',
        alignItems: 'center',
        paddingLeft: 5,
        paddingRight: 3,
        boxShadow: isSelected ? `0 0 12px ${entry.color}60` : undefined,
        zIndex: isHighlighted ? 5 : undefined,
      } as React.CSSProperties}
      onMouseEnter={() => onHover(entry)}
      onMouseLeave={() => onHover(null)}
      onClick={(e) => { e.stopPropagation(); onClick(entry); }}
    >
      {showLabel && (
        <span style={{
          fontSize: 8, color: 'rgba(0,0,0,0.55)', fontFamily: panelFont,
          fontWeight: 600, overflow: 'hidden', textOverflow: 'ellipsis',
          whiteSpace: 'nowrap', flex: 1, lineHeight: 1,
        }}>
          {entry.tableHint !== 'unknown' ? entry.tableHint : entry.label}
        </span>
      )}
      {showLabel && (
        <span style={{
          fontSize: 7, color: 'rgba(0,0,0,0.4)', fontFamily: panelFont,
          fontWeight: 700, flexShrink: 0, marginLeft: 3, lineHeight: 1,
        }}>
          {entry.elapsed.toFixed(1)}s
        </span>
      )}
    </div>
  );
};

/* ── Detail Panel (click to inspect) ─────────────────── */

function DetailPanel({ entry, onClose }: { entry: SwimEntry; onClose: () => void }) {
  const running = entry.endTime === null;
  const accent = entry.color;
  const tableInfo = entry.tableHint !== 'unknown' ? entry.tableHint
    : (entry.database && entry.table ? `${entry.database}.${entry.table}` : '');

  const [liveElapsed, setLiveElapsed] = useState(() =>
    running ? (Date.now() - entry.startTime) / 1000 : entry.elapsed
  );
  useEffect(() => {
    if (!running) { setLiveElapsed(entry.elapsed); return; }
    const id = setInterval(() => setLiveElapsed((Date.now() - entry.startTime) / 1000), 100);
    return () => clearInterval(id);
  }, [running, entry.startTime, entry.elapsed]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);

  const ioRate = entry.isMerge ? (entry.readBytesPerSec || 0) : entry.ioReadRate;
  const progressPct = entry.isMerge ? entry.progress * 100 : entry.progress;

  return (
    <div
      style={{
        position: 'absolute', bottom: LEGEND_H, left: 0, right: 0,
        zIndex: 20,
        background: 'rgba(6,6,14,0.95)',
        borderTop: `2px solid ${accent}50`,
        padding: '12px 16px 10px',
        fontFamily: panelFont,
        animation: 'swim-fade-in 0.2s ease both',
        backdropFilter: 'blur(12px)',
      }}
      onClick={e => e.stopPropagation()}
    >
      {/* Scan line */}
      <div style={{
        position: 'absolute', left: 0, right: 0, height: 1,
        background: `linear-gradient(90deg, transparent, ${accent}60, transparent)`,
        animation: 'swim-scan 3s linear infinite',
        pointerEvents: 'none',
      }} />

      <div style={{ display: 'flex', gap: 24, alignItems: 'flex-start' }}>
        {/* Left: identity */}
        <div style={{ flex: '0 0 auto', minWidth: 180, maxWidth: 300 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 6 }}>
            <span style={{
              fontSize: 10, fontWeight: 700, letterSpacing: 1.5, color: accent,
              background: `${accent}20`, padding: '2px 8px', borderRadius: 4,
              border: `1px solid ${accent}30`,
            }}>{entry.kind}</span>
            {running ? (
              <span style={{ fontSize: 9, color: '#4ade80', animation: 'swim-glow-pulse 2s ease infinite' }}>● LIVE</span>
            ) : (
              <span style={{ fontSize: 9, color: 'rgba(255,255,255,0.2)' }}>■ DONE</span>
            )}
          </div>
          {tableInfo && (
            <div style={{
              fontSize: 11, color: 'rgba(255,255,255,0.6)', fontWeight: 600,
              overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
              marginBottom: 2,
            }}>{tableInfo}</div>
          )}
          {entry.isMerge && entry.partName && (
            <div style={{ fontSize: 10, color: 'rgba(255,255,255,0.4)', marginBottom: 2 }}>
              &rarr; {entry.partName}
            </div>
          )}
          {!entry.isMerge && (
            <div style={{
              fontSize: 9, color: 'rgba(255,255,255,0.25)', marginBottom: 4,
              overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
              maxWidth: 280,
            }}>{entry.label}</div>
          )}
          <div style={{ display: 'flex', alignItems: 'baseline', gap: 6 }}>
            <span style={{ fontSize: 26, fontWeight: 700, color: accent, letterSpacing: -1 }}>
              {liveElapsed.toFixed(1)}
            </span>
            <span style={{ fontSize: 11, color: `${accent}80`, fontWeight: 600 }}>sec</span>
          </div>
        </div>

        {/* Center: resource bars */}
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '6px 16px', fontSize: 10 }}>
            {[
              { label: 'CPU', value: `${entry.cpu.toFixed(2)} cores`, ratio: Math.min(1, entry.cpu / 8), color: '#60a5fa' },
              { label: 'MEM', value: formatBytes(entry.mem), ratio: Math.min(1, entry.mem / (4 * 1073741824)), color: '#a78bfa' },
              { label: 'I/O', value: fmtRate(ioRate), ratio: Math.min(1, ioRate / (100 * 1048576)), color: '#34d399' },
              { label: 'ROWS', value: fmtNum(entry.rowsRead), ratio: Math.min(1, entry.rowsRead / 1_000_000), color: '#f472b6' },
            ].map((item) => (
              <div key={item.label} style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                <span style={{ width: 28, fontSize: 8, color: 'rgba(255,255,255,0.3)', fontWeight: 700, textAlign: 'right', flexShrink: 0 }}>
                  {item.label}
                </span>
                <div style={{
                  flex: 1, height: 6, borderRadius: 3,
                  background: 'rgba(255,255,255,0.04)', overflow: 'hidden',
                }}>
                  <div style={{
                    height: '100%', borderRadius: 3, width: `${Math.max(2, item.ratio * 100)}%`,
                    background: `linear-gradient(90deg, ${item.color}CC, ${item.color})`,
                    boxShadow: `0 0 8px ${item.color}40`,
                    transition: 'width 0.3s ease',
                  }} />
                </div>
                <span style={{ width: 70, fontSize: 10, color: item.color, fontWeight: 600, textAlign: 'right', flexShrink: 0 }}>
                  {item.value}
                </span>
              </div>
            ))}
          </div>
          {progressPct > 0 && (
            <div style={{ marginTop: 6, display: 'flex', alignItems: 'center', gap: 6 }}>
              <span style={{ width: 28, fontSize: 8, color: 'rgba(255,255,255,0.3)', fontWeight: 700, textAlign: 'right' }}>PROG</span>
              <div style={{ flex: 1, height: 6, borderRadius: 3, background: 'rgba(255,255,255,0.04)', overflow: 'hidden' }}>
                <div style={{
                  height: '100%', borderRadius: 3, width: `${progressPct}%`,
                  background: progressPct > 75 ? '#4ade80' : progressPct > 40 ? '#fbbf24' : accent,
                  transition: 'width 0.3s ease',
                }} />
              </div>
              <span style={{ width: 70, fontSize: 10, color: progressPct > 75 ? '#4ade80' : '#fbbf24', fontWeight: 600, textAlign: 'right' }}>
                {progressPct.toFixed(1)}%
              </span>
            </div>
          )}
        </div>

        {/* Right: meta + close */}
        <div style={{ flex: '0 0 auto', display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: 4 }}>
          {entry.user && (
            <span style={{ fontSize: 9, color: 'rgba(255,255,255,0.2)' }}>{entry.user}</span>
          )}
          {entry.hostname && (
            <span style={{ fontSize: 8, color: 'rgba(255,255,255,0.15)' }}>{entry.hostname}</span>
          )}
          {entry.queryId && (
            <span style={{ fontSize: 7, color: 'rgba(255,255,255,0.1)', fontFamily: panelFont }}>
              {entry.queryId.substring(0, 12)}...
            </span>
          )}
          <button
            onClick={onClose}
            style={{
              marginTop: 4, fontSize: 8, color: 'rgba(255,255,255,0.25)', cursor: 'pointer',
              background: 'none', border: '1px solid rgba(255,255,255,0.08)',
              borderRadius: 3, padding: '2px 8px', fontFamily: panelFont,
            }}
          >
            ESC
          </button>
        </div>
      </div>

      {/* Accent border glow on left */}
      <div style={{
        position: 'absolute', top: 0, left: 0, bottom: 0, width: 2,
        background: `linear-gradient(180deg, ${accent}, ${accent}40)`,
        boxShadow: `0 0 12px ${accent}30`,
      }} />
    </div>
  );
}

/* ── Tooltip ─────────────────────────────────────────── */

function SwimTooltip({ entry, mousePos, containerWidth, containerHeight }: {
  entry: SwimEntry;
  mousePos: { x: number; y: number };
  containerWidth: number;
  containerHeight: number;
}) {
  const running = entry.endTime === null;
  const accent = entry.color;
  const tableInfo = entry.tableHint !== 'unknown' ? entry.tableHint : '';
  const tooltipW = 250;
  const tooltipH = 140;

  // Clamp tooltip to stay within container
  let left = mousePos.x + 14;
  let top = mousePos.y - 10;
  if (left + tooltipW > containerWidth - 8) left = mousePos.x - tooltipW - 8;
  if (top + tooltipH > containerHeight - 8) top = containerHeight - tooltipH - 8;
  if (top < 40) top = 40;

  return (
    <div style={{
      position: 'absolute', left, top,
      pointerEvents: 'none', zIndex: 25,
    }}>
      <div style={{
        fontFamily: panelFont,
        fontSize: 11,
        color: 'rgba(255,255,255,0.8)',
        background: 'rgba(8,8,18,0.94)',
        border: `1px solid ${accent}40`,
        borderRadius: 6,
        padding: '8px 12px',
        width: tooltipW,
        boxShadow: `0 0 20px ${accent}15, 0 4px 12px rgba(0,0,0,0.5)`,
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 4 }}>
          <span style={{
            fontSize: 9, fontWeight: 700, letterSpacing: 1.2, color: accent,
            background: `${accent}18`, padding: '1px 5px', borderRadius: 3,
          }}>{entry.kind}</span>
          {running && <span style={{ fontSize: 9, color: '#4ade80' }}>● LIVE</span>}
          <span style={{ marginLeft: 'auto', fontSize: 14, fontWeight: 700, color: accent }}>
            {entry.elapsed.toFixed(1)}s
          </span>
        </div>
        {tableInfo && (
          <div style={{ fontSize: 10, color: 'rgba(255,255,255,0.4)', marginBottom: 4, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
            {tableInfo}
          </div>
        )}
        {!entry.isMerge && (
          <div style={{
            fontSize: 9, color: 'rgba(255,255,255,0.3)', marginBottom: 6,
            overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
          }}>
            {entry.label}
          </div>
        )}
        <div style={{
          display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '3px 12px', fontSize: 10,
          color: 'rgba(255,255,255,0.5)', borderTop: '1px solid rgba(255,255,255,0.06)', paddingTop: 4,
        }}>
          <div><span style={{ fontSize: 9, color: 'rgba(255,255,255,0.3)' }}>CPU </span>{entry.cpu.toFixed(2)}</div>
          <div><span style={{ fontSize: 9, color: 'rgba(255,255,255,0.3)' }}>MEM </span>{formatBytes(entry.mem)}</div>
          <div><span style={{ fontSize: 9, color: 'rgba(255,255,255,0.3)' }}>I/O </span>{fmtRate(entry.ioReadRate)}</div>
          <div><span style={{ fontSize: 9, color: 'rgba(255,255,255,0.3)' }}>ROWS </span>{fmtNum(entry.rowsRead)}</div>
        </div>
        <div style={{ marginTop: 4, fontSize: 8, color: 'rgba(255,255,255,0.15)', textAlign: 'right' }}>click to inspect</div>
      </div>
    </div>
  );
}

/* ── Main component ──────────────────────────────────── */

export interface ResourceArenaSwimlaneProps {
  queries: RunningQueryInfo[];
  merges: ActiveMergeInfo[];
  cpuUsage: number;
  memoryPct: number;
  onQueryClick?: (queryId: string) => void;
  compact?: boolean;
  splitAvailable?: boolean;
  splitActive?: boolean;
  onSplitToggle?: () => void;
}

const HOST_LANE_BLOCK = 6;

const hdrLabel: React.CSSProperties = {
  fontSize: 10, fontFamily: 'monospace', color: 'rgba(255,255,255,0.35)', fontWeight: 600, letterSpacing: 1,
};
const hdrVal: React.CSSProperties = { marginLeft: 6, fontWeight: 700 };

export const ResourceArenaSwimlane: React.FC<ResourceArenaSwimlaneProps> = ({
  queries, merges, cpuUsage, memoryPct, compact,
  splitAvailable, splitActive, onSplitToggle,
}) => {
  const [hovered, setHovered] = useState<SwimEntry | null>(null);
  const [selected, setSelected] = useState<SwimEntry | null>(null);
  const [mousePos, setMousePos] = useState({ x: 0, y: 0 });
  const registryRef = useRef<Map<string, SwimEntry>>(new Map());
  const [visibleIds, setVisibleIds] = useState<string[]>([]);
  const containerRef = useRef<HTMLDivElement>(null);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const [_tick, setTick] = useState(0);
  const [containerWidth, setContainerWidth] = useState(800);
  const [containerHeight, setContainerHeight] = useState(520);

  // Tick every second — bar positions update via CSS transitions for smoothness
  useEffect(() => {
    const iv = setInterval(() => setTick(Date.now()), 1000);
    return () => clearInterval(iv);
  }, []);

  // Track container size
  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const obs = new ResizeObserver(entries => {
      for (const e of entries) {
        setContainerWidth(e.contentRect.width);
        setContainerHeight(e.contentRect.height);
      }
    });
    obs.observe(el);
    setContainerWidth(el.clientWidth);
    setContainerHeight(el.clientHeight);
    return () => obs.disconnect();
  }, []);

  const handleMouseMove = useCallback((e: React.MouseEvent) => {
    if (!containerRef.current) return;
    const rect = containerRef.current.getBoundingClientRect();
    setMousePos({ x: e.clientX - rect.left, y: e.clientY - rect.top });
  }, []);

  const handleHover = useCallback((v: SwimEntry | null) => setHovered(v), []);
  const handleClick = useCallback((v: SwimEntry) => {
    setSelected(prev => prev?.id === v.id ? null : v);
  }, []);
  const handleBgClick = useCallback(() => {
    setSelected(null);
    setHovered(null);
  }, []);

  // Fullscreen
  const toggleFullscreen = useCallback(() => {
    const el = containerRef.current;
    if (!el) return;
    if (!document.fullscreenElement) el.requestFullscreen();
    else document.exitFullscreen();
  }, []);

  useEffect(() => {
    const onFsChange = () => setIsFullscreen(!!document.fullscreenElement);
    document.addEventListener('fullscreenchange', onFsChange);
    return () => document.removeEventListener('fullscreenchange', onFsChange);
  }, []);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'f' && !e.ctrlKey && !e.metaKey && !e.altKey
        && !(e.target instanceof HTMLInputElement) && !(e.target instanceof HTMLTextAreaElement)) {
        toggleFullscreen();
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [toggleFullscreen]);

  // Registry update — same pattern as 3D
  const prevPollRef = useRef<{ q: RunningQueryInfo[]; m: ActiveMergeInfo[] }>({ q: [], m: [] });
  if (queries !== prevPollRef.current.q || merges !== prevPollRef.current.m) {
    prevPollRef.current = { q: queries, m: merges };
    const registry = registryRef.current;
    const now = Date.now();

    type Item = {
      id: string; kind: string; elapsed: number; cpu: number; mem: number;
      label: string; tableHint: string; isMerge: boolean;
      queryId?: string; user?: string; progress: number;
      ioReadRate: number; rowsRead: number;
      hostname?: string;
      readBytesPerSec?: number; writeBytesPerSec?: number;
      numParts?: number; mergeType?: string;
      database?: string; table?: string; partName?: string;
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
          ioReadRate: q.ioReadRate, rowsRead: q.rowsRead,
          hostname: q.hostname,
        };
      }),
      ...merges.map(m => ({
        id: `m:${m.hostname ? m.hostname + ':' : ''}${m.database}.${m.table}.${m.partName}`,
        kind: m.isMutation ? 'MUTATION' : 'MERGE',
        elapsed: m.elapsed, cpu: m.cpuEstimate || 0.3, mem: m.memoryUsage,
        label: `${m.database}.${m.table}`, tableHint: `${m.database}.${m.table}`, isMerge: true,
        progress: m.progress,
        ioReadRate: m.readBytesPerSec, rowsRead: m.rowsRead,
        hostname: m.hostname,
        readBytesPerSec: m.readBytesPerSec, writeBytesPerSec: m.writeBytesPerSec,
        numParts: m.numParts, mergeType: m.mergeType,
        database: m.database, table: m.table, partName: m.partName,
      })),
    ];

    const currentIds = new Set(items.map(it => it.id));

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
        if (selected && selected.id === entry.id) setSelected({ ...entry, endTime: now });
      }
    }

    for (const [id, entry] of registry) {
      if (entry.endTime !== null && (now - entry.endTime) / 1000 > FADE_SECS) {
        registry.delete(id);
      }
    }

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
        existing.hostname = it.hostname;
        existing.readBytesPerSec = it.readBytesPerSec;
        existing.writeBytesPerSec = it.writeBytesPerSec;
        if (selected && selected.id === existing.id) setSelected({ ...existing });
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
          ioReadRate: it.ioReadRate, rowsRead: it.rowsRead,
          hostname: it.hostname,
          readBytesPerSec: it.readBytesPerSec, writeBytesPerSec: it.writeBytesPerSec,
          numParts: it.numParts, mergeType: it.mergeType,
          database: it.database, table: it.table, partName: it.partName,
        });
      }
    }

    if (selected && !registry.has(selected.id)) setSelected(null);

    const newIds = [...registry.keys()].sort();
    const oldIds = [...visibleIds].sort();
    if (newIds.length !== oldIds.length || newIds.some((id, i) => id !== oldIds[i])) {
      setVisibleIds(newIds);
    }
  }

  const entries = [...registryRef.current.values()];
  const hasActivity = entries.some(e => e.endTime === null);
  const now = Date.now();
  const timelineWidth = containerWidth - DECK_LABEL_W;
  const pxPerSec = timelineWidth / HORIZON;

  // Compute deck layout — only count active lanes (skip empty)
  const lanesPerDeck: Record<Deck, number> = { select: 0, insert: 0, merge: 0 };
  for (const e of entries) {
    lanesPerDeck[e.deck] = Math.max(lanesPerDeck[e.deck], e.lane + 1);
  }
  // Only show decks that have entries; ensure at least 1 lane for active decks
  const activeDecks = DECK_ORDER.filter(d => lanesPerDeck[d] > 0 || entries.some(e => e.deck === d));
  for (const d of activeDecks) lanesPerDeck[d] = Math.max(1, lanesPerDeck[d]);

  const timelineTop = HEADER_H + TIME_HEADER_H;
  let yOffset = 0;
  const deckTops: Record<Deck, number> = { select: 0, insert: 0, merge: 0 };
  for (const deck of DECK_ORDER) {
    deckTops[deck] = yOffset;
    if (lanesPerDeck[deck] > 0) {
      yOffset += lanesPerDeck[deck] * (BAR_HEIGHT + LANE_GAP) + DECK_GAP;
    }
  }
  const contentHeight = yOffset;
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
      onMouseMove={handleMouseMove}
      onClick={handleBgClick}
    >
      {/* Header */}
      <div style={{
        position: 'absolute', top: 0, left: 0, right: 0, zIndex: 10, height: HEADER_H,
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        padding: '0 14px',
        background: 'linear-gradient(180deg, rgba(12,12,20,0.95) 0%, rgba(12,12,20,0.7) 80%, transparent 100%)',
      }}>
        <div style={{ display: 'flex', gap: 12, alignItems: 'center' }}>
          {splitAvailable && onSplitToggle && (
            <button
              onClick={(e) => { e.stopPropagation(); onSplitToggle(); }}
              title="Split arena by server"
              style={{
                display: 'flex', alignItems: 'center', gap: 4,
                fontSize: 9, fontFamily: 'monospace', fontWeight: 600, letterSpacing: 1,
                color: splitActive ? '#60a5fa' : 'rgba(255,255,255,0.45)',
                background: splitActive ? 'rgba(96,165,250,0.12)' : 'transparent',
                border: 'none', borderRadius: 3, padding: '2px 6px',
                cursor: 'pointer',
              }}
            >
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
        </div>
      </div>

      {/* Time axis header (fixed, not scrollable) */}
      <div style={{
        position: 'absolute', top: HEADER_H, left: DECK_LABEL_W, right: 0,
        height: TIME_HEADER_H, zIndex: 8,
        borderBottom: '1px solid rgba(255,255,255,0.06)',
        background: 'rgba(20,20,34,0.9)',
      }}>
        <div style={{
          position: 'absolute', right: 0, top: 0, bottom: 0, width: 2,
          background: '#3B82F6', boxShadow: '0 0 8px rgba(59,130,246,0.4)',
        }} />
        <span style={{
          position: 'absolute', right: 6, top: 4,
          fontSize: 10, fontWeight: 700, color: '#3B82F6',
          fontFamily: panelFont, letterSpacing: 1,
        }}>NOW</span>
        {TIME_MARKS.map(sec => {
          const x = timelineWidth - sec * pxPerSec;
          if (x < 0) return null;
          return (
            <div key={sec} style={{ position: 'absolute', left: x, top: 0, bottom: 0 }}>
              <div style={{ width: 1, height: '100%', background: 'rgba(255,255,255,0.06)' }} />
              <span style={{
                position: 'absolute', top: 5, left: 4,
                fontSize: 9, color: 'rgba(255,255,255,0.2)',
                fontFamily: panelFont, whiteSpace: 'nowrap',
              }}>
                {sec >= 60 ? `${sec / 60}m` : `${sec}s`}
              </span>
            </div>
          );
        })}
      </div>

      {/* Scrollable timeline area */}
      <div
        style={{
          position: 'absolute',
          top: timelineTop,
          left: 0,
          right: 0,
          bottom: LEGEND_H + (selected ? 0 : 0),
          overflowY: 'auto',
          overflowX: 'hidden',
        }}
        onClick={handleBgClick}
      >
        <div style={{ position: 'relative', minHeight: '100%', height: Math.max(contentHeight, 100) }}>
          {/* Vertical grid lines */}
          {TIME_MARKS.map(sec => {
            const x = DECK_LABEL_W + timelineWidth - sec * pxPerSec;
            if (x < DECK_LABEL_W) return null;
            return (
              <div key={`grid-${sec}`} style={{
                position: 'absolute', left: x, top: 0,
                bottom: 0, width: 1,
                background: 'rgba(255,255,255,0.025)',
              }} />
            );
          })}

          {/* NOW vertical line */}
          <div style={{
            position: 'absolute', right: 0, top: 0, bottom: 0,
            width: 2, background: 'rgba(59,130,246,0.12)',
          }} />

          {/* Deck sections */}
          {DECK_ORDER.map(deck => {
            if (lanesPerDeck[deck] === 0) return null;
            const top = deckTops[deck];
            const { label, color } = DECK_LABELS[deck];
            return (
              <div key={deck}>
                <div style={{ position: 'absolute', left: 6, top: top + 1, width: DECK_LABEL_W - 8 }}>
                  <span style={{
                    fontSize: 8, fontWeight: 700, letterSpacing: 1.5,
                    color, fontFamily: panelFont, opacity: 0.45,
                  }}>{label}</span>
                </div>
                <div style={{
                  position: 'absolute', left: DECK_LABEL_W, right: 0,
                  top: top - 1, height: 1,
                  background: `linear-gradient(90deg, ${color}18, ${color}06, transparent)`,
                }} />
                {Array.from({ length: lanesPerDeck[deck] }, (_, i) => (
                  <div key={i} style={{
                    position: 'absolute', left: DECK_LABEL_W, right: 0,
                    top: top + i * (BAR_HEIGHT + LANE_GAP),
                    height: BAR_HEIGHT,
                    background: i % 2 === 0 ? 'rgba(255,255,255,0.008)' : 'transparent',
                    borderRadius: 2,
                  }} />
                ))}
              </div>
            );
          })}

          {/* Bars */}
          {entries.map(entry => (
            <SwimBar
              key={entry.id}
              entry={entry}
              containerWidth={containerWidth}
              pxPerSec={pxPerSec}
              now={now}
              isHovered={hovered?.id === entry.id}
              isSelected={selected?.id === entry.id}
              onHover={handleHover}
              onClick={handleClick}
              deckTopY={deckTops[entry.deck]}
            />
          ))}
        </div>
      </div>

      {/* Legend */}
      {!compact && (
        <div style={{
          position: 'absolute', bottom: 0, left: 0, right: 0, height: LEGEND_H,
          zIndex: 10, display: 'flex', gap: 10, alignItems: 'center',
          padding: '0 14px',
          pointerEvents: 'none',
          background: 'linear-gradient(0deg, rgba(20,20,34,0.95) 0%, transparent 100%)',
        }}>
          {['SELECT', 'INSERT', 'MERGE', 'MUTATION'].map(k => (
            <div key={k} style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
              <div style={{ width: 7, height: 7, borderRadius: 2, background: colorOf(k), opacity: 0.7 }} />
              <span style={{ fontSize: 8, fontFamily: 'monospace', color: 'rgba(255,255,255,0.25)' }}>{k}</span>
            </div>
          ))}
          <span style={{ fontSize: 8, fontFamily: 'monospace', color: 'rgba(255,255,255,0.15)', marginLeft: 8 }}>
            length = duration · brightness = cpu
          </span>
        </div>
      )}

      {/* Fullscreen button */}
      <button
        onClick={(e) => { e.stopPropagation(); toggleFullscreen(); }}
        title={isFullscreen ? 'Exit fullscreen (f)' : 'Fullscreen (f)'}
        style={{
          position: 'absolute', bottom: 10, right: 10, zIndex: 10,
          width: 28, height: 28, borderRadius: 6,
          background: 'rgba(255,255,255,0.08)',
          border: '1px solid rgba(255,255,255,0.12)',
          color: 'rgba(255,255,255,0.45)',
          cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center',
        }}
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

      {/* Hover tooltip */}
      {showHover && (
        <SwimTooltip
          entry={hovered}
          mousePos={mousePos}
          containerWidth={containerWidth}
          containerHeight={containerHeight}
        />
      )}

      {/* Detail panel (click to inspect) */}
      {selected && (
        <DetailPanel entry={selected} onClose={() => setSelected(null)} />
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
