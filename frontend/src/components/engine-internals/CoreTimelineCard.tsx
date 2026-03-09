/**
 * CoreTimelineCard - Per-core CPU swimlane visualization from trace_log.
 * Shows what each physical CPU core was doing over time, colored by thread pool.
 * Supports mouse-wheel zoom (centered on cursor) and pan via drag.
 * Slots sourced from Real-only samples get a diagonal stripe pattern to
 * distinguish wall-clock activity (may include IO wait) from on-CPU execution.
 */

import { useState, useEffect, useRef, useMemo, useCallback } from 'react';
import { useClickHouseServices } from '../../providers/ClickHouseProvider';
import { useRefreshConfig, clampToAllowed } from '@tracehouse/ui-shared';
import { useRefreshSettingsStore } from '../../stores/refreshSettingsStore';
import { useCapabilityCheck } from '../shared/RequiresCapability';
import { useMonitoringCapabilitiesStore } from '../../stores/monitoringCapabilitiesStore';
import { EngineInternalsService } from '@tracehouse/core';
import type { CoreTimelineData, CoreTimelineSlot, CPUSamplingByThread, EngineInternalsData, IClickHouseAdapter } from '@tracehouse/core';

type Pool = CPUSamplingByThread['pool'];
type TraceMode = 'both' | 'cpu';

const POOL_COLORS: Record<Pool, string> = {
  queries: '#3b82f6', merges: '#f59e0b', mutations: '#ef4444',
  merge_mutate: '#e87830', replication: '#8b5cf6', io: '#22c55e',
  schedule: '#06b6d4', handler: '#64748b', other: '#94a3b8',
};
const POOL_LABELS: Record<Pool, string> = {
  queries: 'Queries', merges: 'Merges', mutations: 'Mutations',
  merge_mutate: 'Merge+Mutate', replication: 'Replication', io: 'IO',
  schedule: 'Schedule', handler: 'Handlers', other: 'Other',
};

interface CoreTimelineCardProps {
  className?: string;
  /** Cgroup CPU metadata from Engine Internals — used to show host-core-ID hint in containers */
  cpuCoresMeta?: EngineInternalsData['cpuCoresMeta'];
  /** Override adapter for host-targeted queries in cluster mode */
  adapter?: IClickHouseAdapter;
}

function parseSlotTime(s: string): number {
  const n = s.trim().replace(' ', 'T');
  return new Date(n.includes('Z') || n.includes('+') ? n : n + 'Z').getTime();
}

function fmtTime(ms: number): string {
  const d = new Date(ms);
  return `${d.getUTCHours().toString().padStart(2, '0')}:${d.getUTCMinutes().toString().padStart(2, '0')}:${d.getUTCSeconds().toString().padStart(2, '0')}.${Math.floor(d.getUTCMilliseconds() / 100)}`;
}

export function CoreTimelineCard({ className = '', cpuCoresMeta, adapter: adapterOverride }: CoreTimelineCardProps) {
  const services = useClickHouseServices();
  const refreshConfig = useRefreshConfig();
  const { refreshRateSeconds } = useRefreshSettingsStore();
  const { available: hasTraceLog, missing, probing } = useCapabilityCheck(['trace_log']);
  const { flags } = useMonitoringCapabilitiesStore();
  const [data, setData] = useState<CoreTimelineData | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [windowSec, setWindowSec] = useState(15);
  const [traceMode, setTraceMode] = useState<TraceMode>('cpu');
  const [highlightedPools, setHighlightedPools] = useState<Set<Pool>>(new Set());
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Zoom state: visible time range [viewStart, viewEnd] in epoch ms
  const [viewRange, setViewRange] = useState<[number, number] | null>(null);

  // Tooltip state
  const [hoveredSlot, setHoveredSlot] = useState<CoreTimelineSlot | null>(null);
  const [tooltipPos, setTooltipPos] = useState<{ x: number; y: number } | null>(null);

  // Pan state
  const [isPanning, setIsPanning] = useState(false);
  const panStartRef = useRef<{ x: number; range: [number, number] } | null>(null);

  // Refs
  const swimlaneRef = useRef<HTMLDivElement>(null);

  // Fetch data
  useEffect(() => {
    if (!services || !hasTraceLog) { setData(null); return; }
    const svc = new EngineInternalsService(adapterOverride ?? services.adapter);
    let cancelled = false;
    const doFetch = async () => {
      try {
        const result = await svc.getCoreTimeline(windowSec, traceMode === 'cpu');
        if (!cancelled) { setData(result); setError(null); }
      } catch (err) {
        if (!cancelled) setError(err instanceof Error ? err.message : 'Failed to fetch core timeline');
      }
    };
    doFetch();
    const effectiveRate = refreshRateSeconds > 0 ? Math.max(10, refreshRateSeconds) : 10;
    intervalRef.current = setInterval(doFetch, clampToAllowed(effectiveRate, refreshConfig) * 1000);
    return () => { cancelled = true; if (intervalRef.current) clearInterval(intervalRef.current); };
  }, [services, hasTraceLog, refreshRateSeconds, refreshConfig, windowSec, traceMode, adapterOverride]);

  // Reset view range when data changes
  useEffect(() => { setViewRange(null); }, [data]);

  // Build grid
  const grid = useMemo(() => {
    if (!data || data.slots.length === 0) return null;
    const filteredSlots = data.slots;
    if (filteredSlots.length === 0) return null;
    const timeSet = new Set<string>();
    const coreSet = new Set<number>();
    for (const s of filteredSlots) { timeSet.add(s.time); coreSet.add(s.core); }
    const times: string[] = Array.from(timeSet).sort();
    const cores: number[] = Array.from(coreSet).sort((a, b) => a - b);
    const timeMsArr = times.map(parseSlotTime);
    const coreToIdx = new Map<number, number>(cores.map((c, i) => [c, i]));
    // Build per-core slot arrays sorted by timeMs
    const perCore: CoreTimelineSlot[][] = cores.map(() => []);
    for (const slot of filteredSlots) {
      const ci = coreToIdx.get(slot.core);
      if (ci !== undefined) perCore[ci].push(slot);
    }
    for (const arr of perCore) arr.sort((a, b) => a.timeMs - b.timeMs);
    const fullMin = timeMsArr[0];
    const fullMax = timeMsArr[timeMsArr.length - 1];
    return { times, cores, perCore, fullMin, fullMax, filteredSlots };
  }, [data]);

  // Effective view range
  const fullMin = grid?.fullMin ?? 0;
  const fullMax = grid?.fullMax ?? 0;
  const [vStart, vEnd] = viewRange ?? [fullMin, fullMax];
  const vRange = vEnd - vStart || 1;
  const isZoomed = viewRange !== null && (vStart > fullMin || vEnd < fullMax);

  // Mouse wheel zoom centered on cursor
  useEffect(() => {
    const el = swimlaneRef.current;
    if (!el || !grid) return;
    const handler = (e: WheelEvent) => {
      e.preventDefault();
      const rect = el.getBoundingClientRect();
      const LABEL_W = 52;
      const relX = e.clientX - rect.left - LABEL_W;
      const chartW = rect.width - LABEL_W;
      if (relX < 0 || relX > chartW) return;
      // Time under cursor
      const ratio = relX / chartW;
      const cursorMs = vStart + ratio * vRange;
      const zoomFactor = e.deltaY < 0 ? 0.7 : 1.4;
      const newRange = vRange * zoomFactor;
      // Clamp: don't zoom below 500ms or beyond full range
      const minRange = 500;
      const maxRange = fullMax - fullMin;
      if (newRange < minRange || newRange > maxRange) return;
      // Keep cursor anchored
      const newStart = cursorMs - ratio * newRange;
      const newEnd = newStart + newRange;
      const cStart = Math.max(fullMin, newStart);
      const cEnd = Math.min(fullMax, newEnd);
      if (cEnd - cStart < minRange) return;
      setViewRange([cStart, cEnd]);
    };
    el.addEventListener('wheel', handler, { passive: false });
    return () => el.removeEventListener('wheel', handler);
  }, [grid, vStart, vEnd, vRange, fullMin, fullMax]);

  // Pan via mouse drag
  const handleMouseDown = useCallback((e: React.MouseEvent) => {
    if (!grid) return;
    setIsPanning(true);
    panStartRef.current = { x: e.clientX, range: [vStart, vEnd] };
  }, [grid, vStart, vEnd]);

  const handleMouseMove = useCallback((e: React.MouseEvent) => {
    setTooltipPos({ x: e.clientX, y: e.clientY });
    if (!isPanning || !panStartRef.current || !swimlaneRef.current) return;
    const rect = swimlaneRef.current.getBoundingClientRect();
    const chartW = rect.width - 52;
    const dx = e.clientX - panStartRef.current.x;
    const msPerPx = (panStartRef.current.range[1] - panStartRef.current.range[0]) / chartW;
    const shift = -dx * msPerPx;
    let newStart = panStartRef.current.range[0] + shift;
    let newEnd = panStartRef.current.range[1] + shift;
    // Clamp to full bounds
    if (newStart < fullMin) { newEnd += fullMin - newStart; newStart = fullMin; }
    if (newEnd > fullMax) { newStart -= newEnd - fullMax; newEnd = fullMax; }
    newStart = Math.max(fullMin, newStart);
    newEnd = Math.min(fullMax, newEnd);
    setViewRange([newStart, newEnd]);
  }, [isPanning, fullMin, fullMax]);

  const handleMouseUp = useCallback(() => {
    setIsPanning(false);
    panStartRef.current = null;
  }, []);

  // Hit-test: find slot under cursor
  const handleCellHover = useCallback((coreIdx: number, relX: number, chartW: number) => {
    if (!grid) { setHoveredSlot(null); return; }
    const ratio = relX / chartW;
    const cursorMs = vStart + ratio * vRange;
    // Find nearest slot in this core's data
    const coreSlots = grid.perCore[coreIdx];
    if (!coreSlots || coreSlots.length === 0) { setHoveredSlot(null); return; }
    // Binary search for closest slot
    let lo = 0, hi = coreSlots.length - 1;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (coreSlots[mid].timeMs < cursorMs) lo = mid + 1; else hi = mid;
    }
    // Check lo and lo-1 for closest
    let best = coreSlots[lo];
    if (lo > 0 && Math.abs(coreSlots[lo - 1].timeMs - cursorMs) < Math.abs(best.timeMs - cursorMs)) {
      best = coreSlots[lo - 1];
    }
    // Only show if within half a slot width (~50ms)
    const slotHalfWidth = 50;
    if (Math.abs(best.timeMs - cursorMs) <= slotHalfWidth && best.timeMs >= vStart && best.timeMs <= vEnd) {
      setHoveredSlot(best);
    } else {
      setHoveredSlot(null);
    }
  }, [grid, vStart, vEnd, vRange]);

  // Unavailable / error / empty states
  if (!hasTraceLog && !probing) {
    return (
      <div className={`rounded-lg border ${className}`} style={{ background: 'var(--bg-card)', borderColor: 'var(--border-primary)' }}>
        <div style={{ padding: '12px 16px', borderBottom: '1px solid var(--border-secondary)' }}>
          <h3 style={{ fontSize: 11, fontWeight: 500, color: 'var(--text-secondary)' }}>Core Timeline</h3>
        </div>
        <div style={{ padding: 16, textAlign: 'center', fontSize: 11, color: 'var(--text-muted)' }}>
          Requires {missing.map(m => `system.${m}`).join(', ')}
        </div>
      </div>
    );
  }
  if (error) {
    return (
      <div className={`rounded-lg border ${className}`} style={{ background: 'var(--bg-card)', borderColor: 'var(--border-primary)' }}>
        <div style={{ padding: '12px 16px', borderBottom: '1px solid var(--border-secondary)' }}>
          <h3 style={{ fontSize: 11, fontWeight: 500, color: 'var(--text-secondary)' }}>Core Timeline</h3>
        </div>
        <div style={{ padding: 16, textAlign: 'center', fontSize: 11, color: 'var(--accent-red)' }}>{error}</div>
      </div>
    );
  }
  if (!data || data.totalSamples === 0) {
    return (
      <div className={`rounded-lg border ${className}`} style={{ background: 'var(--bg-card)', borderColor: 'var(--border-primary)' }}>
        <div style={{ padding: '12px 16px', borderBottom: '1px solid var(--border-secondary)', display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: 8 }}>
          <h3 style={{ fontSize: 11, fontWeight: 500, color: 'var(--text-secondary)' }}>Core Timeline</h3>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 10 }}>
            <div style={{ display: 'flex', borderRadius: 4, overflow: 'hidden', border: '1px solid var(--border-secondary)' }}>
              {([['cpu', 'CPU only'], ['both', 'CPU+Real']] as const).map(([mode, label]) => (
                <button key={mode} onClick={() => setTraceMode(mode)}
                  style={{
                    background: traceMode === mode ? 'var(--bg-tertiary)' : 'none',
                    border: 'none', borderRight: mode === 'cpu' ? '1px solid var(--border-secondary)' : 'none',
                    padding: '2px 6px', cursor: 'pointer',
                    color: traceMode === mode ? 'var(--text-primary)' : 'var(--text-muted)', fontSize: 10,
                  }}
                >{label}</button>
              ))}
            </div>
            <span style={{ color: 'var(--border-secondary)' }}>|</span>
            {([15, 30, 60] as const).map(w => (
              <button key={w} onClick={() => setWindowSec(w)}
                style={{
                  background: windowSec === w ? 'var(--bg-tertiary)' : 'none',
                  border: windowSec === w ? '1px solid var(--border-secondary)' : '1px solid transparent',
                  borderRadius: 4, padding: '2px 6px', cursor: 'pointer',
                  color: windowSec === w ? 'var(--text-primary)' : 'var(--text-muted)', fontSize: 10,
                }}
              >{w}s</button>
            ))}
          </div>
        </div>
        <div style={{ padding: 16, textAlign: 'center', fontSize: 11, color: 'var(--text-muted)' }}>
          No CPU samples in the last {windowSec}s — system may be idle or profiler disabled
        </div>
      </div>
    );
  }
  if (!grid) {
    // Data exists but all slots filtered out (e.g. CPU-only mode on a mostly-idle server)
    return (
      <div className={`rounded-lg border ${className}`} style={{ background: 'var(--bg-card)', borderColor: 'var(--border-primary)' }}>
        <div style={{ padding: '12px 16px', borderBottom: '1px solid var(--border-secondary)', display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: 8 }}>
          <h3 style={{ fontSize: 11, fontWeight: 500, color: 'var(--text-secondary)' }}>Core Timeline</h3>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 10 }}>
            <div style={{ display: 'flex', borderRadius: 4, overflow: 'hidden', border: '1px solid var(--border-secondary)' }}>
              {([['both', 'CPU+Real'], ['cpu', 'CPU only']] as const).map(([mode, label]) => (
                <button key={mode} onClick={() => setTraceMode(mode)}
                  style={{
                    background: traceMode === mode ? 'var(--bg-tertiary)' : 'none',
                    border: 'none', borderRight: mode === 'both' ? '1px solid var(--border-secondary)' : 'none',
                    padding: '2px 6px', cursor: 'pointer',
                    color: traceMode === mode ? 'var(--text-primary)' : 'var(--text-muted)', fontSize: 10,
                  }}
                >{label}</button>
              ))}
            </div>
          </div>
        </div>
        <div style={{ padding: 16, textAlign: 'center', fontSize: 11, color: 'var(--text-muted)' }}>
          {data.totalSamples.toLocaleString()} samples exist but none match current filters
          {traceMode === 'cpu' && ' — try CPU+Real mode'}
        </div>
      </div>
    );
  }

  const { cores, perCore } = grid;
  const LANE_HEIGHT = 20;
  const LABEL_WIDTH = 52;

  // Pool summary for legend
  const poolCounts = new Map<Pool, number>();
  for (const slot of grid.filteredSlots) poolCounts.set(slot.pool, (poolCounts.get(slot.pool) || 0) + slot.samples);
  const activePools: [Pool, number][] = Array.from(poolCounts.entries()).sort((a, b) => b[1] - a[1]);

  // Visible slots count for the header
  const visibleSamples = (grid?.filteredSlots ?? []).filter(s => s.timeMs >= vStart && s.timeMs <= vEnd).reduce((sum, s) => sum + s.samples, 0);

  return (
    <div className={`rounded-lg border ${className}`} style={{ background: 'var(--bg-card)', borderColor: 'var(--border-primary)' }}>
      {/* Header */}
      <div style={{ padding: '12px 16px', borderBottom: '1px solid var(--border-secondary)', display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: 8 }}>
        <h3 style={{ fontSize: 11, fontWeight: 500, color: 'var(--text-secondary)', display: 'flex', alignItems: 'center', gap: 4 }}>
          Core Timeline
          <span
            title="Physical CPU core view from trace_log sampling. Scroll to zoom (centered on cursor), drag to pan. Each row is a CPU core, colored by dominant thread pool. Toggle CPU+Real vs CPU-only to filter trace types. Striped slots = Real-only (wall-clock, may include IO wait)."
            style={{
              display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
              width: 14, height: 14, borderRadius: '50%', fontSize: 9, fontWeight: 600,
              color: 'var(--text-muted)', border: '1px solid var(--border-secondary)',
              cursor: 'help', flexShrink: 0,
            }}
          >?</span>
          <span style={{ fontWeight: 400, color: 'var(--text-muted)', marginLeft: 2, fontSize: 10 }}>
            per-core · 100ms slots · scroll to zoom
          </span>
        </h3>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 10 }}>
          {isZoomed && (
            <button
              onClick={() => setViewRange(null)}
              style={{
                background: 'none', border: '1px solid var(--border-secondary)',
                borderRadius: 4, padding: '2px 6px', cursor: 'pointer',
                color: 'var(--text-muted)', fontSize: 10,
              }}
            >Reset zoom</button>
          )}
          {/* Trace mode toggle */}
          <div style={{ display: 'flex', borderRadius: 4, overflow: 'hidden', border: '1px solid var(--border-secondary)' }}>
            {([['cpu', 'CPU only'], ['both', 'CPU+Real']] as const).map(([mode, label]) => (
              <button key={mode} onClick={() => setTraceMode(mode)}
                title={mode === 'cpu'
                  ? 'Show only CPU profiler samples (strict on-CPU execution). Hides IO-wait / sleeping threads.'
                  : 'Show all profiler samples (CPU execution + wall-clock). Real-only slots shown with stripes.'}
                style={{
                  background: traceMode === mode ? 'var(--bg-tertiary)' : 'none',
                  border: 'none', borderRight: mode === 'cpu' ? '1px solid var(--border-secondary)' : 'none',
                  padding: '2px 6px', cursor: 'pointer',
                  color: traceMode === mode ? 'var(--text-primary)' : 'var(--text-muted)', fontSize: 10,
                }}
              >{label}</button>
            ))}
          </div>
          <span style={{ color: 'var(--border-secondary)' }}>|</span>
          {([15, 30, 60] as const).map(w => (
            <button key={w} onClick={() => setWindowSec(w)}
              style={{
                background: windowSec === w ? 'var(--bg-tertiary)' : 'none',
                border: windowSec === w ? '1px solid var(--border-secondary)' : '1px solid transparent',
                borderRadius: 4, padding: '2px 6px', cursor: 'pointer',
                color: windowSec === w ? 'var(--text-primary)' : 'var(--text-muted)', fontSize: 10,
              }}
            >{w}s</button>
          ))}
          <span style={{ color: 'var(--text-muted)', marginLeft: 4 }}>
            <span style={{ fontFamily: 'monospace', color: 'var(--text-primary)' }}>{visibleSamples.toLocaleString()}</span>
            {isZoomed && <span style={{ color: 'var(--text-muted)' }}> / {data.totalSamples.toLocaleString()}</span>}
            {' '}samples · {cores.length} cores
            {data?.syntheticCores && (
              <span
                title="Physical cpu_id not available — lanes use thread_id % core_count distribution"
                style={{ color: 'var(--accent-amber, #f59e0b)', cursor: 'help' }}
              > (virtual lanes)</span>
            )}
            {cpuCoresMeta?.isCgroupLimited && !data?.syntheticCores && (
              <span
                title={`Container has ${cpuCoresMeta.effectiveCores} vCPUs but core IDs are from the ${cpuCoresMeta.hostCores}-core host node. The OS scheduler migrates threads across host cores — IDs may be non-contiguous.`}
                style={{ color: 'var(--accent-amber, #f59e0b)', cursor: 'help' }}
              > (host IDs)</span>
            )}
            {isZoomed && <span style={{ color: 'var(--text-muted)' }}> · {((vEnd - vStart) / 1000).toFixed(1)}s visible</span>}
          </span>
        </div>
      </div>

      {/* Cloud info banner */}
      {data?.syntheticCores && (
        <div style={{
          padding: '6px 16px',
          fontSize: 10,
          color: 'var(--text-muted)',
          background: 'var(--bg-tertiary)',
          borderBottom: '1px solid var(--border-secondary)',
          display: 'flex',
          alignItems: 'center',
          gap: 6,
        }}>
          <span style={{ color: 'var(--accent-amber, #f59e0b)' }}>⚠</span>
          {flags.isCloudService
            ? 'ClickHouse Cloud does not expose physical CPU core IDs. Lanes show threads distributed by thread_id modulo — useful for concurrency patterns but not physical core affinity.'
            : 'Physical cpu_id not available on this server. Lanes use synthetic distribution (thread_id mod N).'}
        </div>
      )}

      {/* Swimlane area */}
      <div
        ref={swimlaneRef}
        style={{ padding: '12px 16px', cursor: isPanning ? 'grabbing' : hoveredSlot ? 'crosshair' : 'grab', userSelect: 'none' }}
        onMouseDown={handleMouseDown}
        onMouseMove={handleMouseMove}
        onMouseUp={handleMouseUp}
        onMouseLeave={() => { handleMouseUp(); setHoveredSlot(null); }}
      >
        {/* Time axis */}
        <TimeAxis vStart={vStart} vEnd={vEnd} labelWidth={LABEL_WIDTH} />

        {/* Core lanes */}
        {cores.map((core, ci) => (
          <CoreLane
            key={core}
            core={core}
            slots={perCore[ci]}
            vStart={vStart}
            vEnd={vEnd}
            vRange={vRange}
            laneHeight={LANE_HEIGHT}
            labelWidth={LABEL_WIDTH}
            traceMode={traceMode}
            highlightedPools={highlightedPools}
            onHover={(relX, chartW) => handleCellHover(ci, relX, chartW)}
            onLeave={() => setHoveredSlot(null)}
          />
        ))}
      </div>

      {/* Enhanced tooltip */}
      {hoveredSlot && tooltipPos && !isPanning && (
        <div style={{
          position: 'fixed', left: tooltipPos.x + 14, top: tooltipPos.y - 60,
          background: '#0f172a', border: '1px solid #334155',
          borderRadius: 6, padding: '8px 12px', fontSize: 10, zIndex: 9999,
          pointerEvents: 'none', color: '#e2e8f0',
          boxShadow: '0 4px 16px rgba(0,0,0,0.4)', minWidth: 180, maxWidth: 320,
        }}>
          {/* Header: core + pool */}
          <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 4 }}>
            <div style={{ width: 8, height: 8, borderRadius: 2, background: POOL_COLORS[hoveredSlot.pool] }} />
            <span style={{ fontWeight: 600, color: '#f1f5f9' }}>Core {hoveredSlot.core}</span>
            <span style={{ color: '#64748b' }}>·</span>
            <span style={{ color: POOL_COLORS[hoveredSlot.pool], fontWeight: 500 }}>{POOL_LABELS[hoveredSlot.pool]}</span>
          </div>
          {/* Details grid */}
          <div style={{ display: 'grid', gridTemplateColumns: 'auto 1fr', gap: '2px 8px', fontSize: 9, fontFamily: 'monospace' }}>
            <span style={{ color: '#64748b' }}>time</span>
            <span style={{ color: '#cbd5e1' }}>{fmtTime(hoveredSlot.timeMs)}</span>
            <span style={{ color: '#64748b' }}>thread</span>
            <span style={{ color: POOL_COLORS[hoveredSlot.pool] }}>{hoveredSlot.threadName}</span>
            <span style={{ color: '#64748b' }}>samples</span>
            <span style={{ color: '#cbd5e1' }}>{hoveredSlot.samples} in 100ms slot</span>
            <span style={{ color: '#64748b' }}>type</span>
            <span style={{ color: '#cbd5e1' }}>{hoveredSlot.isQuery ? 'query execution' : 'background work'}</span>
            <span style={{ color: '#64748b' }}>trace</span>
            <span style={{ color: hoveredSlot.traceType === 'CPU' ? '#34d399' : hoveredSlot.traceType === 'Real' ? '#fbbf24' : '#cbd5e1' }}>
              {hoveredSlot.traceType === 'CPU' ? '● CPU' : hoveredSlot.traceType === 'Real' ? '◐ Real (wall-clock)' : '● Mixed'}
              {hoveredSlot.traceType !== 'CPU' && <span style={{ color: '#64748b' }}> — {hoveredSlot.cpuSamples}cpu/{hoveredSlot.realSamples}real</span>}
            </span>
            {hoveredSlot.isQuery && hoveredSlot.queryId && (
              <>
                <span style={{ color: '#64748b' }}>query_id</span>
                <span style={{ color: '#cbd5e1', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}
                  title={hoveredSlot.queryId}>{hoveredSlot.queryId.slice(0, 16)}…</span>
              </>
            )}
          </div>
        </div>
      )}

      {/* Legend — click to highlight */}
      <div style={{ padding: '8px 16px 12px', display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 10, borderTop: '1px solid var(--border-secondary)' }}>
        {activePools.map(([pool]) => {
          const isHighlighted = highlightedPools.has(pool);
          const isDimmed = highlightedPools.size > 0 && !isHighlighted;
          return (
            <div
              key={pool}
              onClick={(e) => {
                const multi = e.metaKey || e.ctrlKey;
                setHighlightedPools(prev => {
                  if (multi) {
                    // Ctrl/Cmd+click: toggle this pool in/out of multi-selection
                    const next = new Set(prev);
                    if (next.has(pool)) next.delete(pool); else next.add(pool);
                    return next;
                  } else {
                    // Single click: highlight only this pool, or clear if already the sole selection
                    if (prev.size === 1 && prev.has(pool)) return new Set();
                    return new Set([pool]);
                  }
                });
              }}
              style={{
                display: 'flex', alignItems: 'center', gap: 4, fontSize: 10, cursor: 'pointer',
                opacity: isDimmed ? 0.3 : 1,
                transition: 'opacity 0.15s ease',
              }}
              title={isHighlighted ? `Click to deselect · ⌘/Ctrl+click to multi-select` : `Click to highlight ${POOL_LABELS[pool]} · ⌘/Ctrl+click to add`}
            >
              <div style={{
                width: 8, height: 8, borderRadius: 2, background: POOL_COLORS[pool],
                opacity: 0.8,
                boxShadow: isHighlighted ? `0 0 0 1.5px ${POOL_COLORS[pool]}` : 'none',
                transition: 'box-shadow 0.15s ease',
              }} />
              <span style={{
                color: isHighlighted ? 'var(--text-primary)' : 'var(--text-muted)',
                fontWeight: isHighlighted ? 600 : 400,
                transition: 'color 0.15s ease, font-weight 0.15s ease',
              }}>{POOL_LABELS[pool]}</span>
            </div>
          );
        })}
        <div style={{ display: 'flex', alignItems: 'center', gap: 4, fontSize: 10 }}>
          <div style={{ width: 8, height: 8, borderRadius: 2, background: 'var(--bg-tertiary)' }} />
          <span style={{ color: 'var(--text-muted)' }}>Idle</span>
        </div>
        {traceMode === 'both' && (
          <div style={{ display: 'flex', alignItems: 'center', gap: 4, fontSize: 10 }}>
            <div style={{
              width: 8, height: 8, borderRadius: 2,
              background: 'repeating-linear-gradient(135deg, #94a3b8 0px, #94a3b8 2px, transparent 2px, transparent 4px)',
            }} />
            <span style={{ color: 'var(--text-muted)' }}>Real only (wall-clock)</span>
          </div>
        )}
        {/* Hint text */}
        <span style={{ marginLeft: 'auto', fontSize: 9, color: 'var(--text-muted)', opacity: 0.6, fontStyle: 'italic', whiteSpace: 'nowrap' }}>
          click to highlight · ⌘/ctrl+click to multi-select
        </span>
      </div>
    </div>
  );
}

/** Single core lane — renders visible slots as positioned divs */
function CoreLane({ core, slots, vStart, vEnd, vRange, laneHeight, labelWidth, traceMode, highlightedPools, onHover, onLeave }: {
  core: number;
  slots: CoreTimelineSlot[];
  vStart: number; vEnd: number; vRange: number;
  laneHeight: number; labelWidth: number;
  traceMode: TraceMode;
  highlightedPools: Set<Pool>;
  onHover: (relX: number, chartW: number) => void;
  onLeave: () => void;
}) {
  const laneRef = useRef<HTMLDivElement>(null);

  const handleMouseMove = useCallback((e: React.MouseEvent) => {
    if (!laneRef.current) return;
    const rect = laneRef.current.getBoundingClientRect();
    onHover(e.clientX - rect.left, rect.width);
  }, [onHover]);

  // Filter to visible slots and compute positions
  const visibleSlots = useMemo(() => {
    const result: { slot: CoreTimelineSlot; leftPct: number; widthPct: number }[] = [];
    // Slot duration is 100ms
    const slotDuration = 100;
    for (const slot of slots) {
      const slotEnd = slot.timeMs + slotDuration;
      if (slotEnd < vStart || slot.timeMs > vEnd) continue;
      const leftPct = Math.max(0, ((slot.timeMs - vStart) / vRange) * 100);
      const rightPct = Math.min(100, ((slotEnd - vStart) / vRange) * 100);
      const widthPct = rightPct - leftPct;
      if (widthPct > 0.05) result.push({ slot, leftPct, widthPct });
    }
    return result;
  }, [slots, vStart, vEnd, vRange]);

  return (
    <div style={{ display: 'flex', alignItems: 'center', height: laneHeight, marginBottom: 1 }}>
      <div style={{
        width: labelWidth, flexShrink: 0, fontSize: 9, fontFamily: 'monospace',
        color: 'var(--text-muted)', textAlign: 'right', paddingRight: 8,
      }}>
        core {core}
      </div>
      <div
        ref={laneRef}
        style={{
          flex: 1, height: laneHeight - 2, borderRadius: 2, overflow: 'hidden',
          background: 'var(--bg-tertiary)', position: 'relative',
        }}
        onMouseMove={handleMouseMove}
        onMouseLeave={onLeave}
      >
        {visibleSlots.map(({ slot, leftPct, widthPct }, i) => {
          const isRealOnly = slot.traceType === 'Real';
          const color = POOL_COLORS[slot.pool];
          const baseOpacity = Math.min(0.4 + slot.samples * 0.2, 1);
          // When a highlight is active, dim non-highlighted slots significantly
          const isActive = highlightedPools.size === 0 || highlightedPools.has(slot.pool);
          const opacity = isActive ? baseOpacity : baseOpacity * 0.12;
          return (
            <div
              key={i}
              style={{
                position: 'absolute',
                left: `${leftPct}%`,
                width: `${Math.max(widthPct, 0.3)}%`,
                height: '100%',
                background: isRealOnly && traceMode === 'both'
                  ? `repeating-linear-gradient(135deg, ${color} 0px, ${color} 2px, transparent 2px, transparent 4px)`
                  : color,
                opacity,
                transition: 'opacity 0.15s ease',
              }}
            />
          );
        })}
      </div>
    </div>
  );
}

/** Time axis with adaptive labels */
function TimeAxis({ vStart, vEnd, labelWidth }: {
  vStart: number; vEnd: number; labelWidth: number;
}) {
  const range = vEnd - vStart || 1;
  const labelCount = 6;
  const labels: { pct: number; text: string }[] = [];
  for (let i = 0; i <= labelCount; i++) {
    const pct = i / labelCount;
    const ms = vStart + pct * range;
    // Show sub-second precision when zoomed in enough
    const showMs = range < 10000;
    const d = new Date(ms);
    const hh = d.getUTCHours().toString().padStart(2, '0');
    const mm = d.getUTCMinutes().toString().padStart(2, '0');
    const ss = d.getUTCSeconds().toString().padStart(2, '0');
    const text = showMs
      ? `${hh}:${mm}:${ss}.${Math.floor(d.getUTCMilliseconds() / 100)}`
      : `${hh}:${mm}:${ss}`;
    labels.push({ pct, text });
  }

  return (
    <div style={{ display: 'flex', height: 16, marginBottom: 4, position: 'relative' }}>
      <div style={{ width: labelWidth, flexShrink: 0 }} />
      <div style={{ flex: 1, position: 'relative' }}>
        {labels.map((l, i) => (
          <span key={i} style={{
            position: 'absolute', left: `${l.pct * 100}%`, transform: 'translateX(-50%)',
            fontSize: 8, fontFamily: 'monospace', color: 'var(--text-muted)', whiteSpace: 'nowrap',
          }}>{l.text}</span>
        ))}
      </div>
    </div>
  );
}

export default CoreTimelineCard;
