/**
 * TimelineChart — Interactive SVG chart with stacked query/merge/mutation areas,
 * server metric line, hover tooltips, click-to-pin, drag-to-zoom, and scroll-to-zoom.
 *
 * Extracted from TimeTravelPage for clarity.
 */
import React, { useState, useCallback, useMemo, useRef, useEffect } from 'react';
import type { MemoryTimeline } from '@tracehouse/core';
import { getMergeCategoryInfo, type MergeCategory } from '@tracehouse/core';
import { formatBytes, parseTimestamp } from '../../utils/formatters';
import {
  type MetricMode, type HighlightedItem,
  Q_COLORS, M_COLORS, MUT_COLORS, METRIC_CONFIG, METRIC_BAR_CONFIG,
} from './timeline-constants';

import { formatDurationMs as fmtMs } from '../../utils/formatters';

interface Rng { startMs: number; endMs: number; peak: number; realPeak: number; }

export const TimelineChart: React.FC<{
  data: MemoryTimeline; metricMode: MetricMode; height?: number;
  hoverMs: number | null; pinnedMs: number | null;
  onHover: (ms: number | null) => void; onPin: (ms: number) => void;
  zoomRange: [number, number] | null;
  onZoom: (range: [number, number] | null) => void;
  highlightedItem: HighlightedItem;
  onHighlightItem?: (item: HighlightedItem) => void;
  onBandClick?: (band: { type: 'query' | 'merge' | 'mutation'; idx: number }) => void;
  hiddenCategories?: Set<'query' | 'merge' | 'mutation'>;
}> = ({ data, metricMode, height = 380, hoverMs, pinnedMs, onHover, onPin, zoomRange, onZoom, highlightedItem, onHighlightItem, onBandClick, hiddenCategories }) => {
  const W = 1000, H = height;
  const padTop = 12, padRight = 90, padBottom = 30, padLeft = 52;
  const cw = W - padLeft - padRight, ch = H - padTop - padBottom;
  const svgRef = useRef<SVGSVGElement>(null);
  const [dragStart, setDragStart] = useState<number | null>(null);
  const [dragCurrent, setDragCurrent] = useState<number | null>(null);
  const [localSvgX, setLocalSvgX] = useState<number | null>(null);
  const [localSvgY, setLocalSvgY] = useState<number | null>(null);
  const hoveredBandRef = useRef<{ type: 'query' | 'merge' | 'mutation'; idx: number } | null>(null);
  const cfg = METRIC_CONFIG[metricMode];

  // Pick the right server-level timeseries based on mode
  const rawServerPts = useMemo(() => {
    if (metricMode === 'memory') return data.server_memory;
    if (metricMode === 'cpu') return data.server_cpu;
    if (metricMode === 'network') {
      if (data.server_network_send.length > 0) {
        return data.server_network_send.map((p, i) => ({
          t: p.t, v: p.v + (data.server_network_recv[i]?.v ?? 0),
        }));
      }
      return [];
    }
    // disk
    if (data.server_disk_read && data.server_disk_read.length > 0) {
      return data.server_disk_read.map((p, i) => ({
        t: p.t, v: p.v + (data.server_disk_write?.[i]?.v ?? 0),
      }));
    }
    return [];
  }, [data, metricMode]);

  const serverPts = useMemo(() => rawServerPts.map(p => ({ ms: parseTimestamp(p.t), v: p.v })), [rawServerPts]);

  // Per-host CPU data for cluster tooltip breakdown (pre-parsed into ms-indexed arrays)
  const perHostCpuPts = useMemo(() => {
    if (!data.per_host_cpu || metricMode !== 'cpu') return null;
    const result: { host: string; pts: { ms: number; v: number }[] }[] = [];
    for (const [host, points] of Object.entries(data.per_host_cpu)) {
      result.push({ host, pts: points.map(p => ({ ms: parseTimestamp(p.t), v: p.v })) });
    }
    // Sort by host name for consistent ordering
    result.sort((a, b) => a.host.localeCompare(b.host));
    return result;
  }, [data.per_host_cpu, metricMode]);

  // Host count for cluster context (used in tooltips, not for band scaling).
  // Bands show real values — the avg server line + per-host tooltip bars explain the full picture.
  const hc = data.host_count || 1;

  // Compute query/merge ranges for stacked areas
  const qRanges: Rng[] = useMemo(() => {
    return data.queries.map(q => {
      const durS = Math.max(q.duration_ms / 1000, 0.001);
      let realPeak = 0;
      if (metricMode === 'memory') realPeak = q.peak_memory;
      else if (metricMode === 'cpu') realPeak = q.cpu_us / durS;
      else if (metricMode === 'network') realPeak = (q.net_send + q.net_recv) / durS;
      else realPeak = (q.disk_read + q.disk_write) / durS;
      return { startMs: parseTimestamp(q.start_time), endMs: parseTimestamp(q.end_time), peak: realPeak, realPeak };
    });
  }, [data.queries, metricMode]);
  const mRanges: Rng[] = useMemo(() => {
    return data.merges.map(m => {
      const durS = Math.max(m.duration_ms / 1000, 0.001);
      let realPeak = 0;
      if (metricMode === 'memory') realPeak = m.peak_memory;
      else if (metricMode === 'cpu') realPeak = m.cpu_us / durS;
      else if (metricMode === 'network') realPeak = (m.net_send + m.net_recv) / durS;
      else realPeak = (m.disk_read + m.disk_write) / durS;
      return { startMs: parseTimestamp(m.start_time), endMs: parseTimestamp(m.end_time), peak: realPeak, realPeak };
    });
  }, [data.merges, metricMode]);
  const mutRanges: Rng[] = useMemo(() => {
    return (data.mutations ?? []).map(m => {
      const durS = Math.max(m.duration_ms / 1000, 0.001);
      let realPeak = 0;
      if (metricMode === 'memory') realPeak = m.peak_memory;
      else if (metricMode === 'cpu') realPeak = m.cpu_us / durS;
      else if (metricMode === 'network') realPeak = (m.net_send + m.net_recv) / durS;
      else realPeak = (m.disk_read + m.disk_write) / durS;
      return { startMs: parseTimestamp(m.start_time), endMs: parseTimestamp(m.end_time), peak: realPeak, realPeak };
    });
  }, [data.mutations, metricMode]);

  // For dual-line modes (network: send/recv, disk: read/write)
  const dualLine1 = useMemo(() => {
    if (metricMode === 'network') return data.server_network_send.map(p => ({ ms: parseTimestamp(p.t), v: p.v }));
    if (metricMode === 'disk' && data.server_disk_read) return data.server_disk_read.map(p => ({ ms: parseTimestamp(p.t), v: p.v }));
    return [];
  }, [data, metricMode]);
  const dualLine2 = useMemo(() => {
    if (metricMode === 'network') return data.server_network_recv.map(p => ({ ms: parseTimestamp(p.t), v: p.v }));
    if (metricMode === 'disk' && data.server_disk_write) return data.server_disk_write.map(p => ({ ms: parseTimestamp(p.t), v: p.v }));
    return [];
  }, [data, metricMode]);
  const isDualMode = metricMode === 'network' || metricMode === 'disk';
  const dualLabels = metricMode === 'network' ? ['Send', 'Recv'] : ['Read', 'Write'];
  const dualColors = metricMode === 'network' ? ['#3fb950', '#58a6ff'] : ['#bc8cff', '#f0883e'];

  const fullTMin = serverPts.length > 0 ? serverPts[0].ms : new Date(data.window_start).getTime();
  const fullTMax = serverPts.length > 0 ? serverPts[serverPts.length - 1].ms : new Date(data.window_end).getTime();
  const tMin = zoomRange ? zoomRange[0] : fullTMin;
  const tMax = zoomRange ? zoomRange[1] : fullTMax;
  const tRange = tMax - tMin || 1;

  const maxY = useMemo(() => {
    const visPts = serverPts.filter(p => p.ms >= tMin && p.ms <= tMax);
    const sMax = visPts.length > 0 ? Math.max(...visPts.map(p => p.v)) : 0;
    let stackMax = 0;
    for (const sp of visPts) {
      let stack = 0;
      for (const qr of qRanges) { if (sp.ms >= qr.startMs && sp.ms <= qr.endMs) stack += qr.peak; }
      for (const mr of mRanges) { if (sp.ms >= mr.startMs && sp.ms <= mr.endMs) stack += mr.peak; }
      for (const mu of mutRanges) { if (sp.ms >= mu.startMs && sp.ms <= mu.endMs) stack += mu.peak; }
      if (stack > stackMax) stackMax = stack;
    }
    return Math.max(sMax, stackMax, 1) * 1.15;
  }, [serverPts, qRanges, mRanges, mutRanges, tMin, tMax]);

  const xScale = useCallback((ms: number) => padLeft + ((ms - tMin) / tRange) * cw, [tMin, tRange, cw, padLeft]);
  const yScale = useCallback((v: number) => padTop + ch - (v / maxY) * ch, [maxY, ch, padTop]);
  const nq = data.queries.length, nm = data.merges.length, nmut = (data.mutations ?? []).length;

  // Per-metric maximums across all items — used for proportional tooltip bars
  const metricMaximums = useMemo(() => {
    const allItems = [...data.queries, ...data.merges, ...(data.mutations ?? [])];
    if (allItems.length === 0) return { cpu: 1, memory: 1, disk: 1, network: 1 };
    return {
      cpu: Math.max(1, ...allItems.map(it => it.cpu_us)),
      memory: Math.max(1, ...allItems.map(it => it.peak_memory)),
      disk: Math.max(1, ...allItems.map(it => it.disk_read + it.disk_write)),
      network: Math.max(1, ...allItems.map(it => it.net_send + it.net_recv)),
    };
  }, [data.queries, data.merges, data.mutations]);

  // Stacked areas
  const buckets = useMemo(() => {
    if (serverPts.length === 0) return [];
    const hideQ = hiddenCategories?.has('query') ?? false;
    const hideM = hiddenCategories?.has('merge') ?? false;
    const hideMut = hiddenCategories?.has('mutation') ?? false;
    return serverPts.map(sp => {
      const t = sp.ms;
      const qv = hideQ ? qRanges.map(() => 0) : qRanges.map(qr => (t >= qr.startMs && t <= qr.endMs) ? qr.peak : 0);
      const mv = hideM ? mRanges.map(() => 0) : mRanges.map(mr => (t >= mr.startMs && t <= mr.endMs) ? mr.peak : 0);
      const mutv = hideMut ? mutRanges.map(() => 0) : mutRanges.map(mu => (t >= mu.startMs && t <= mu.endMs) ? mu.peak : 0);
      return { t, serverMem: sp.v, qv, mv, mutv };
    });
  }, [serverPts, qRanges, mRanges, mutRanges, hiddenCategories]);

  // Precompute cumulative stack heights per bucket per band (data-only, no SVG coords).
  // This is O(bands × buckets) but only recomputes when data changes, not on zoom.
  const cumStacks = useMemo(() => {
    if (buckets.length === 0) return [];
    const total = nq + nm + nmut;
    // cumStacks[bandIdx][bucketIdx] = cumulative height including this band
    const stacks: number[][] = [];
    for (let idx = 0; idx < total; idx++) {
      stacks.push(buckets.map((b, bi) => {
        const prev = idx > 0 ? stacks[idx - 1][bi] : 0;
        let val: number;
        if (idx < nq) val = b.qv[idx];
        else if (idx < nq + nm) val = b.mv[idx - nq];
        else val = b.mutv[idx - nq - nm];
        return prev + val;
      }));
    }
    return stacks;
  }, [buckets, nq, nm, nmut]);

  const areas = useMemo(() => {
    if (buckets.length < 2 || cumStacks.length === 0) return [];
    const total = nq + nm + nmut;
    const res: { d: string; color: string; isRunning: boolean }[] = [];
    for (let idx = 0; idx < total; idx++) {
      const top = cumStacks[idx];
      const bot = idx > 0 ? cumStacks[idx - 1] : null;
      const topPts = buckets.map((b, bi) => `${xScale(b.t)},${yScale(top[bi])}`).join(' L');
      const botPts = buckets.map((b, bi) => `${xScale(b.t)},${yScale(bot ? bot[bi] : 0)}`).reverse().join(' L');
      let color: string;
      let isRunning = false;
      if (idx < nq) {
        color = Q_COLORS[idx % Q_COLORS.length];
        isRunning = data.queries[idx]?.is_running ?? false;
      } else if (idx < nq + nm) {
        color = M_COLORS[(idx - nq) % M_COLORS.length];
        isRunning = data.merges[idx - nq]?.is_running ?? false;
      } else {
        color = MUT_COLORS[(idx - nq - nm) % MUT_COLORS.length];
        isRunning = (data.mutations ?? [])[idx - nq - nm]?.is_running ?? false;
      }
      res.push({ d: `M${topPts} L${botPts} Z`, color, isRunning });
    }
    return res;
  }, [buckets, cumStacks, nq, nm, nmut, xScale, yScale, data.queries, data.merges, data.mutations]);

  const serverLine = serverPts.length >= 2 ? 'M' + serverPts.map(p => `${xScale(p.ms)},${yScale(p.v)}`).join(' L') : '';
  const line1Path = dualLine1.length >= 2 ? 'M' + dualLine1.map(p => `${xScale(p.ms)},${yScale(p.v)}`).join(' L') : '';
  const line2Path = dualLine2.length >= 2 ? 'M' + dualLine2.map(p => `${xScale(p.ms)},${yScale(p.v)}`).join(' L') : '';

  const yTicks = [0, 0.25, 0.5, 0.75, 1].map(f => f * maxY);
  const xTicks = Array.from({ length: 6 }, (_, i) => tMin + (tRange * i) / 5);
  const ramY = metricMode === 'memory' && data.server_total_ram > 0 && data.server_total_ram <= maxY ? yScale(data.server_total_ram) : null;
  // CPU 100% line: cpu_cores * 1_000_000 µs/s = full utilization
  const cpuFullUs = data.cpu_cores * 1_000_000;
  const cpuY = metricMode === 'cpu' && data.cpu_cores > 0 ? yScale(cpuFullUs) : null;

  const toSvgXY = useCallback((e: React.MouseEvent): { x: number; y: number } | null => {
    const svg = svgRef.current; if (!svg) return null;
    const r = svg.getBoundingClientRect();
    const svgX = ((e.clientX - r.left) / r.width) * W;
    const svgY = ((e.clientY - r.top) / r.height) * H;
    if (svgX < padLeft || svgX > W - padRight) return null;
    return { x: svgX, y: svgY };
  }, [W, H, padLeft, padRight]);

  const svgXtoMs = useCallback((svgX: number) => {
    return tMin + ((svgX - padLeft) / cw) * tRange;
  }, [tMin, padLeft, cw, tRange]);

  const handleMouseDown = useCallback((e: React.MouseEvent) => {
    const pt = toSvgXY(e);
    if (pt) { const ms = svgXtoMs(pt.x); setDragStart(ms); setDragCurrent(ms); }
  }, [toSvgXY, svgXtoMs]);

  const handleMouseMove = useCallback((e: React.MouseEvent) => {
    const pt = toSvgXY(e);
    if (pt) {
      setLocalSvgX(pt.x); setLocalSvgY(pt.y);
      const ms = svgXtoMs(pt.x);
      onHover(ms);
      if (dragStart !== null) setDragCurrent(ms);
    } else { setLocalSvgX(null); setLocalSvgY(null); onHover(null); }
  }, [toSvgXY, svgXtoMs, onHover, dragStart]);

  const handleMouseUp = useCallback((e: React.MouseEvent) => {
    if (dragStart !== null && dragCurrent !== null) {
      const lo = Math.min(dragStart, dragCurrent);
      const hi = Math.max(dragStart, dragCurrent);
      if (hi - lo > 2000) { onZoom([lo, hi]); }
      else {
        const pt = toSvgXY(e);
        if (pt) {
          // If clicking on a band, open its detail modal without pinning
          if (hoveredBandRef.current && onBandClick) {
            onBandClick(hoveredBandRef.current);
          } else {
            onPin(svgXtoMs(pt.x));
          }
        }
      }
    }
    setDragStart(null); setDragCurrent(null);
  }, [dragStart, dragCurrent, toSvgXY, svgXtoMs, onZoom, onPin, onBandClick]);

  const handleMouseLeave = useCallback(() => {
    onHover(null); setLocalSvgX(null); setLocalSvgY(null); setDragStart(null); setDragCurrent(null);
    onHighlightItem?.(null);
  }, [onHover, onHighlightItem]);

  // Mouse wheel zoom: scroll up = zoom in, scroll down = zoom out, centered on cursor
  // Throttled via requestAnimationFrame to avoid recomputing SVG paths on every wheel tick
  const pendingZoomRef = useRef<[number, number] | null>(null);
  const rafIdRef = useRef<number | null>(null);
  useEffect(() => {
    const svg = svgRef.current;
    if (!svg) return;
    const handler = (e: WheelEvent) => {
      e.preventDefault();
      const r = svg.getBoundingClientRect();
      const svgX = ((e.clientX - r.left) / r.width) * W;
      if (svgX < padLeft || svgX > W - padRight) return;
      // ms under cursor
      const cursorMs = tMin + ((svgX - padLeft) / cw) * tRange;
      const zoomFactor = e.deltaY < 0 ? 0.7 : 1.4; // scroll up = zoom in
      const newRange = tRange * zoomFactor;
      // Don't zoom in below 5 seconds or out beyond the full data range
      const minRange = 5000;
      const maxRange = fullTMax - fullTMin;
      if (newRange < minRange || newRange > maxRange) return;
      // Keep cursor position anchored: ratio of cursor within the range stays the same
      const ratio = (cursorMs - tMin) / tRange;
      const newStart = cursorMs - ratio * newRange;
      const newEnd = newStart + newRange;
      // Clamp to full data bounds
      const clampedStart = Math.max(fullTMin, newStart);
      const clampedEnd = Math.min(fullTMax, newEnd);
      if (clampedEnd - clampedStart < minRange) return;
      pendingZoomRef.current = [clampedStart, clampedEnd];
      if (rafIdRef.current === null) {
        rafIdRef.current = requestAnimationFrame(() => {
          if (pendingZoomRef.current) onZoom(pendingZoomRef.current);
          pendingZoomRef.current = null;
          rafIdRef.current = null;
        });
      }
    };
    svg.addEventListener('wheel', handler, { passive: false });
    return () => {
      svg.removeEventListener('wheel', handler);
      if (rafIdRef.current !== null) cancelAnimationFrame(rafIdRef.current);
    };
  }, [W, padLeft, padRight, cw, tMin, tRange, fullTMin, fullTMax, onZoom]);

  const snap = useMemo(() => {
    if (hoverMs === null || serverPts.length === 0) return null;
    let near = serverPts[0], best = Math.abs(serverPts[0].ms - hoverMs);
    for (const sp of serverPts) { const d = Math.abs(sp.ms - hoverMs); if (d < best) { best = d; near = sp; } }
    const aq = qRanges.map((qr, i) => (hoverMs >= qr.startMs && hoverMs <= qr.endMs) ? { i, p: qr.peak, rp: qr.realPeak } : null).filter(Boolean) as { i: number; p: number; rp: number }[];
    const am = mRanges.map((mr, i) => (hoverMs >= mr.startMs && hoverMs <= mr.endMs) ? { i, p: mr.peak, rp: mr.realPeak } : null).filter(Boolean) as { i: number; p: number; rp: number }[];
    const amut = mutRanges.map((mu, i) => (hoverMs >= mu.startMs && hoverMs <= mu.endMs) ? { i, p: mu.peak, rp: mu.realPeak } : null).filter(Boolean) as { i: number; p: number; rp: number }[];
    let line1Val = 0, line2Val = 0;
    if (isDualMode && dualLine1.length > 0) {
      let ni = dualLine1[0], nb = Math.abs(dualLine1[0].ms - hoverMs);
      let ri = dualLine2[0];
      for (let idx = 0; idx < dualLine1.length; idx++) {
        const d = Math.abs(dualLine1[idx].ms - hoverMs);
        if (d < nb) { nb = d; ni = dualLine1[idx]; ri = dualLine2[idx]; }
      }
      line1Val = ni.v; line2Val = ri?.v ?? 0;
    }
    // Detect which band the mouse Y is in by interpolating between bucket points
    // (matches the linear interpolation the SVG path rendering uses)
    let hoveredBand: { type: 'query' | 'merge' | 'mutation'; idx: number } | null = null;
    if (localSvgY !== null && buckets.length >= 2) {
      const yVal = ((padTop + ch - localSvgY) / ch) * maxY;
      if (yVal >= 0) {
        // Find the two surrounding bucket points for interpolation
        let bi = 0;
        for (let k = 0; k < buckets.length - 1; k++) {
          if (hoverMs >= buckets[k].t && hoverMs <= buckets[k + 1].t) { bi = k; break; }
          if (k === buckets.length - 2) bi = k; // clamp to last segment
        }
        const b0 = buckets[bi], b1 = buckets[bi + 1];
        const segLen = b1.t - b0.t;
        const frac = segLen > 0 ? (hoverMs - b0.t) / segLen : 0;

        // Interpolate each band's value and build cumulative stack
        const total = nq + nm + nmut;
        let cumulative = 0;
        for (let idx = 0; idx < total; idx++) {
          let v0: number, v1: number;
          let bandType: 'query' | 'merge' | 'mutation';
          let bandIdx: number;
          if (idx < nq) {
            v0 = b0.qv[idx]; v1 = b1.qv[idx];
            bandType = 'query'; bandIdx = idx;
          } else if (idx < nq + nm) {
            v0 = b0.mv[idx - nq]; v1 = b1.mv[idx - nq];
            bandType = 'merge'; bandIdx = idx - nq;
          } else {
            v0 = b0.mutv[idx - nq - nm]; v1 = b1.mutv[idx - nq - nm];
            bandType = 'mutation'; bandIdx = idx - nq - nm;
          }
          const interpVal = v0 + (v1 - v0) * frac;
          if (interpVal <= 0) continue;
          const prev = cumulative;
          cumulative += interpVal;
          if (yVal >= prev && yVal < cumulative) {
            hoveredBand = { type: bandType, idx: bandIdx };
            break;
          }
        }
      }
    }
    // Per-host CPU values at this timestamp
    let hostCpuValues: { host: string; v: number }[] | null = null;
    if (perHostCpuPts) {
      hostCpuValues = perHostCpuPts.map(({ host, pts }) => {
        if (pts.length === 0) return { host, v: 0 };
        let nearest = pts[0], bestDist = Math.abs(pts[0].ms - hoverMs);
        for (const p of pts) {
          const d = Math.abs(p.ms - hoverMs);
          if (d < bestDist) { bestDist = d; nearest = p; }
        }
        return { host, v: nearest.v };
      });
    }
    return { t: hoverMs, srv: near.v, aq, am, amut, qt: aq.reduce((s, x) => s + x.p, 0), mt: am.reduce((s, x) => s + x.p, 0), mutt: amut.reduce((s, x) => s + x.p, 0), line1Val, line2Val, hoveredBand, hostCpuValues };
  }, [hoverMs, serverPts, qRanges, mRanges, mutRanges, isDualMode, dualLine1, dualLine2, localSvgY, padTop, ch, maxY, buckets, nq, nm, nmut, perHostCpuPts]);

  // Sync chart hover to parent so table rows highlight when hovering graph bands
  useEffect(() => {
    hoveredBandRef.current = snap?.hoveredBand ?? null;
    if (!onHighlightItem) return;
    onHighlightItem(snap?.hoveredBand ?? null);
  }, [snap?.hoveredBand]);

  const crossX = localSvgX;
  const pinX = pinnedMs !== null ? xScale(pinnedMs) : null;

  const dragRect = useMemo(() => {
    if (dragStart === null || dragCurrent === null) return null;
    const x1 = xScale(Math.min(dragStart, dragCurrent));
    const x2 = xScale(Math.max(dragStart, dragCurrent));
    return { x: Math.max(x1, padLeft), width: Math.min(x2, W - padRight) - Math.max(x1, padLeft) };
  }, [dragStart, dragCurrent, xScale, padLeft, W, padRight]);

  if (serverPts.length < 2 && nq === 0 && nm === 0) {
    return <div className="flex items-center justify-center text-sm" style={{ height, color: 'var(--text-muted)' }}>No data for {cfg.label}</div>;
  }

  return (
    <div style={{ position: 'relative' }}>
      <svg ref={svgRef} width="100%" height={H} viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none"
        style={{ cursor: snap?.hoveredBand ? 'pointer' : 'crosshair', userSelect: 'none' }}
        onMouseDown={handleMouseDown} onMouseMove={handleMouseMove}
        onMouseUp={handleMouseUp} onMouseLeave={handleMouseLeave}>
        <defs>
          <linearGradient id="serverFill" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={cfg.color} stopOpacity="0.25" />
            <stop offset="100%" stopColor={cfg.color} stopOpacity="0.02" />
          </linearGradient>
          <linearGradient id="dualFill1" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={dualColors[0]} stopOpacity="0.2" />
            <stop offset="100%" stopColor={dualColors[0]} stopOpacity="0.02" />
          </linearGradient>
          <linearGradient id="dualFill2" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={dualColors[1]} stopOpacity="0.15" />
            <stop offset="100%" stopColor={dualColors[1]} stopOpacity="0.02" />
          </linearGradient>
          <filter id="glow">
            <feGaussianBlur stdDeviation="2" result="blur" />
            <feMerge><feMergeNode in="blur" /><feMergeNode in="SourceGraphic" /></feMerge>
          </filter>
        </defs>
        {/* Grid */}
        {yTicks.map((v, i) => (
          <g key={i}>
            <line x1={padLeft} y1={yScale(v)} x2={W - padRight} y2={yScale(v)} stroke="var(--border-primary)" strokeWidth="0.5" strokeDasharray="4,4" />
          </g>
        ))}
        {ramY !== null && (
          <line x1={padLeft} y1={ramY} x2={W - padRight} y2={ramY} stroke="#f85149" strokeWidth="1" strokeDasharray="6,3" opacity="0.5" />
        )}
        {cpuY !== null && cpuY >= padTop && cpuY <= padTop + ch && (
          <line x1={padLeft} y1={cpuY} x2={W - padRight} y2={cpuY} stroke="#f85149" strokeWidth="1" strokeDasharray="6,3" opacity="0.5" />
        )}
        {/* Stacked areas — query/merge/mutation bands */}
        {areas.map((a, i) => {
          // Determine if this area is the hovered one (from chart hover or table hover)
          let isHovered = false;
          // Check table highlight first (takes priority)
          if (highlightedItem) {
            const { type, idx } = highlightedItem;
            if (type === 'query' && i === idx) isHovered = true;
            else if (type === 'merge' && i === nq + idx) isHovered = true;
            else if (type === 'mutation' && i === nq + nm + idx) isHovered = true;
          } else if (snap?.hoveredBand) {
            // Fall back to chart hover
            const { type, idx } = snap.hoveredBand;
            if (type === 'query' && i === idx) isHovered = true;
            else if (type === 'merge' && i === nq + idx) isHovered = true;
            else if (type === 'mutation' && i === nq + nm + idx) isHovered = true;
          }
          const hasAnyHighlight = highlightedItem !== null || snap?.hoveredBand !== null;
          const baseOpacity = isHovered ? 1 : (hasAnyHighlight ? 0.3 : 0.6);
          return (
            <path
              key={i}
              d={a.d}
              fill={a.color}
              opacity={baseOpacity}
              stroke={isHovered ? '#fff' : 'none'}
              strokeWidth={isHovered ? 1.5 : 0}
              filter={isHovered ? 'url(#glow)' : undefined}
              style={a.isRunning ? {
                animation: 'bandPulse 4.5s ease-in-out infinite',
              } : undefined}
            />
          );
        })}
        {/* Server line fill + stroke */}
        {!isDualMode && serverLine && (
          <>
            <path d={serverLine + ` L${xScale(serverPts[serverPts.length - 1].ms)},${yScale(0)} L${xScale(serverPts[0].ms)},${yScale(0)} Z`}
              fill="url(#serverFill)" />
            <path d={serverLine} fill="none" stroke={cfg.color} strokeWidth="1.8" filter="url(#glow)" />
          </>
        )}
        {isDualMode && (
          <>
            {line1Path && dualLine1.length >= 2 && (
              <>
                <path d={line1Path + ` L${xScale(dualLine1[dualLine1.length - 1].ms)},${yScale(0)} L${xScale(dualLine1[0].ms)},${yScale(0)} Z`}
                  fill="url(#dualFill1)" />
                <path d={line1Path} fill="none" stroke={dualColors[0]} strokeWidth="1.8" filter="url(#glow)" />
              </>
            )}
            {line2Path && dualLine2.length >= 2 && (
              <>
                <path d={line2Path + ` L${xScale(dualLine2[dualLine2.length - 1].ms)},${yScale(0)} L${xScale(dualLine2[0].ms)},${yScale(0)} Z`}
                  fill="url(#dualFill2)" />
                <path d={line2Path} fill="none" stroke={dualColors[1]} strokeWidth="1.8" filter="url(#glow)" />
              </>
            )}
          </>
        )}
        {pinX !== null && (
          <>
            <line x1={pinX} y1={padTop} x2={pinX} y2={padTop + ch} stroke="#3fb950" strokeWidth="2" opacity="0.8">
              <title>Pinned at {new Date(pinnedMs!).toLocaleTimeString()} — click elsewhere to move, or unpin from the toolbar</title>
            </line>
            <line x1={pinX} y1={padTop} x2={pinX} y2={padTop + ch} stroke="transparent" strokeWidth="10" style={{ cursor: 'help' }}>
              <title>Pinned at {new Date(pinnedMs!).toLocaleTimeString()} — click elsewhere to move, or unpin from the toolbar</title>
            </line>
          </>
        )}
        {crossX !== null && dragStart === null && <line x1={crossX} y1={padTop} x2={crossX} y2={padTop + ch} stroke="rgba(255,255,255,0.5)" strokeWidth="1" pointerEvents="none" />}
        {/* Hover dot - on hovered band or server line */}
        {crossX !== null && dragStart === null && snap && localSvgY !== null && (
          snap.hoveredBand ? (
            // Show dot at mouse position with band color
            <circle
              cx={crossX}
              cy={localSvgY}
              r="5"
              fill={
                snap.hoveredBand.type === 'query' ? Q_COLORS[snap.hoveredBand.idx % Q_COLORS.length] :
                  snap.hoveredBand.type === 'merge' ? M_COLORS[snap.hoveredBand.idx % M_COLORS.length] :
                    MUT_COLORS[snap.hoveredBand.idx % MUT_COLORS.length]
              }
              stroke="#0d1117"
              strokeWidth="2"
              filter="url(#glow)"
            />
          ) : !isDualMode ? (
            // Show dot on server line when not hovering a band
            <circle cx={crossX} cy={yScale(snap.srv)} r="3.5" fill={cfg.color} stroke="#0d1117" strokeWidth="1.5" />
          ) : null
        )}
        {dragRect !== null && dragRect.width > 0 && (
          <rect x={dragRect.x} y={padTop} width={dragRect.width} height={ch} fill="rgba(88,166,255,0.15)" stroke="rgba(88,166,255,0.5)" strokeWidth="1" pointerEvents="none" rx="2" />
        )}
        {/* X axis labels removed from SVG — rendered as HTML overlays */}
        {/* Dual-line legend */}
        {isDualMode && (
          <g>
            <line x1={padLeft + 10} y1={padTop + 10} x2={padLeft + 30} y2={padTop + 10} stroke={dualColors[0]} strokeWidth="2" />
            <line x1={padLeft + 70} y1={padTop + 10} x2={padLeft + 90} y2={padTop + 10} stroke={dualColors[1]} strokeWidth="2" />
          </g>
        )}
      </svg>
      {/* Y-axis labels as HTML overlays (not stretched) */}
      {yTicks.map((v, i) => {
        let label: string;
        if (metricMode === 'cpu' && data.cpu_cores > 0) {
          label = `${((v / (data.cpu_cores * 1_000_000)) * 100).toFixed(0)}%`;
        } else if (metricMode === 'memory' && data.server_total_ram > 0) {
          const pct = ((v / data.server_total_ram) * 100).toFixed(0);
          label = `${cfg.fmtVal(v)} (${pct}%)`;
        } else {
          label = cfg.fmtVal(v);
        }
        return (
          <div key={`yl${i}`} style={{
            position: 'absolute', right: `${100 - (padLeft - 6) / W * 100}%`,
            top: `${(yScale(v) / H) * 100}%`, transform: 'translateY(-50%)',
            fontSize: 9, color: 'var(--text-muted)', pointerEvents: 'none', whiteSpace: 'nowrap',
          }}>{label}</div>
        );
      })}
      {ramY !== null && (
        <div style={{
          position: 'absolute', left: `${((W - padRight + 6) / W) * 100}%`,
          top: `${(ramY / H) * 100}%`, transform: 'translateY(-50%)',
          fontSize: 9, color: '#f85149', opacity: 0.7, pointerEvents: 'none', whiteSpace: 'nowrap',
        }}>RAM</div>
      )}
      {cpuY !== null && cpuY >= padTop && cpuY <= padTop + ch && (
        <div style={{
          position: 'absolute', left: `${((W - padRight + 6) / W) * 100}%`,
          top: `${(cpuY / H) * 100}%`, transform: 'translateY(-50%)',
          fontSize: 9, color: '#f85149', opacity: 0.7, pointerEvents: 'none', whiteSpace: 'nowrap',
        }}>100% ({data.cpu_cores} cores)</div>
      )}
      {/* X-axis labels as HTML overlays */}
      {xTicks.map((ms, i) => (
        <div key={`xl${i}`} style={{
          position: 'absolute', left: `${(xScale(ms) / W) * 100}%`,
          bottom: 0, transform: 'translateX(-50%)',
          fontSize: 9, color: 'var(--text-muted)', pointerEvents: 'none', whiteSpace: 'nowrap',
        }}>{new Date(ms).toLocaleTimeString()}</div>
      ))}
      {/* Dual-line legend labels */}
      {isDualMode && (
        <>
          <div style={{
            position: 'absolute', left: `${((padLeft + 34) / W) * 100}%`,
            top: `${((padTop + 8) / H) * 100}%`,
            fontSize: 9, color: dualColors[0], pointerEvents: 'none',
          }}>{dualLabels[0]}</div>
          <div style={{
            position: 'absolute', left: `${((padLeft + 94) / W) * 100}%`,
            top: `${((padTop + 8) / H) * 100}%`,
            fontSize: 9, color: dualColors[1], pointerEvents: 'none',
          }}>{dualLabels[1]}</div>
        </>
      )}
      {/* Tooltip */}
      {snap && crossX !== null && localSvgY !== null && dragStart === null && (() => {
        // Resolve hovered item and its accent color
        const band = snap.hoveredBand;
        const hovItem = band
          ? band.type === 'query' ? data.queries[band.idx]
            : band.type === 'merge' ? data.merges[band.idx]
              : (data.mutations ?? [])[band.idx]
          : null;
        const accentColor = band
          ? band.type === 'query' ? Q_COLORS[band.idx % Q_COLORS.length]
            : band.type === 'merge' ? M_COLORS[band.idx % M_COLORS.length]
              : MUT_COLORS[band.idx % MUT_COLORS.length]
          : null;
        return (
          <div style={{
            position: 'absolute',
            top: localSvgY < H * 0.5 ? `${(localSvgY / H) * 100 + 2}%` : undefined,
            bottom: localSvgY >= H * 0.5 ? `${100 - (localSvgY / H) * 100 + 2}%` : undefined,
            left: crossX > W * 0.6 ? undefined : `${(crossX / W) * 100 + 1}%`,
            right: crossX > W * 0.6 ? `${100 - (crossX / W) * 100 + 1}%` : undefined,
            background: 'color-mix(in srgb, var(--bg-secondary), transparent 35%)',
            border: '1px solid var(--border-primary)',
            borderRadius: 10, padding: '10px 14px', pointerEvents: 'none', zIndex: 10,
            width: 240, fontSize: 10,
            backdropFilter: 'blur(12px)', WebkitBackdropFilter: 'blur(12px)',
            boxShadow: '0 8px 32px rgba(0,0,0,0.45), 0 1px 4px rgba(0,0,0,0.2)',
          }}>
            <div style={{ color: 'var(--text-primary)', fontWeight: 600, marginBottom: 6, fontSize: 12 }}>
              {new Date(snap.t).toLocaleTimeString()}
            </div>
            {band && hovItem ? (
              /* Hovering on a specific band — rich multi-metric card */
              <>
                {/* Header: type dot + label + duration */}
                <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 6 }}>
                  <span style={{ width: 6, height: 6, borderRadius: '50%', background: accentColor!, flexShrink: 0 }} />
                  <span style={{ color: accentColor!, fontWeight: 600, fontSize: 11 }}>
                    {band.type === 'query'
                      ? ((hovItem as any).query_kind || 'Query')
                      : band.type === 'merge'
                        ? (() => { const info = getMergeCategoryInfo(((hovItem as any).merge_reason || 'Regular') as MergeCategory); return `Merge · ${info ? info.label : 'Regular'}`; })()
                        : 'Mutation'}
                  </span>
                  <span style={{ color: 'var(--text-muted)', fontSize: 9 }}>·</span>
                  <span style={{ color: 'var(--text-secondary)', fontSize: 9 }}>{fmtMs(hovItem.duration_ms)}</span>
                  {/* Running indicator with progress for merges/mutations */}
                  {hovItem.is_running && (
                    <span style={{ fontSize: 9, color: '#3fb950', fontWeight: 600 }}>
                      Running{'progress' in hovItem && typeof hovItem.progress === 'number' ? ` ${(hovItem.progress * 100).toFixed(0)}%` : ''}
                    </span>
                  )}
                </div>
                {/* Identifier */}
                {band.type === 'query' ? (
                  <>
                    <div style={{ fontSize: 10, fontFamily: 'monospace', color: '#58a6ff', marginBottom: 2, letterSpacing: '-0.3px' }}>
                      {(hovItem as any).query_id?.slice(0, 14)}
                    </div>
                    <div style={{ color: 'var(--text-muted)', fontSize: 9, marginBottom: 6, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{(hovItem as any).user}</div>
                  </>
                ) : (
                  <>
                    <div style={{ fontSize: 10, fontFamily: 'monospace', color: 'var(--text-secondary)', marginBottom: 1, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{(hovItem as any).table}</div>
                    <div style={{ color: 'var(--text-muted)', fontSize: 9, marginBottom: 6, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{(hovItem as any).part_name}</div>
                  </>
                )}
                {/* Multi-metric bar chart grid */}
                <div style={{ display: 'grid', gridTemplateColumns: '30px 1fr 50px', gap: '2px 6px', alignItems: 'center' }}>
                  {METRIC_BAR_CONFIG.map(mc => {
                    const val = mc.getValue(hovItem);
                    const max = metricMaximums[mc.key];
                    const pct = Math.min((val / max) * 100, 100);
                    const isActive = metricMode === mc.key;
                    return (
                      <React.Fragment key={mc.key}>
                        <span style={{ fontSize: 9, fontWeight: isActive ? 700 : 500, color: isActive ? mc.color : 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.5px' }}>
                          {mc.label}{hovItem.is_running && mc.key === 'cpu' && band.type !== 'query' ? <span style={{ fontSize: 8, fontStyle: 'italic', fontWeight: 400, textTransform: 'none', opacity: 0.7 }}> est.</span> : ''}
                        </span>
                        <div style={{ height: 6, background: 'rgba(255,255,255,0.06)', borderRadius: 3, overflow: 'hidden', border: isActive ? `1px solid ${mc.color}33` : '1px solid transparent' }}>
                          <div style={{
                            width: `${Math.max(pct, val > 0 ? 2 : 0)}%`, height: '100%', borderRadius: 3,
                            background: isActive ? mc.color : `${mc.color}88`,
                            opacity: isActive ? 1 : 0.6,
                          }} />
                        </div>
                        <span style={{ fontSize: 10, color: isActive ? 'var(--text-primary)' : 'var(--text-secondary)', fontFamily: 'monospace', textAlign: 'right', fontWeight: isActive ? 600 : 400 }}>
                          {hovItem.is_running && mc.key === 'cpu' && band.type !== 'query' ? `~${mc.fmt(val)}` : mc.fmt(val)}
                        </span>
                      </React.Fragment>
                    );
                  })}
                </div>
                {/* Query text preview */}
                {band.type === 'query' && (hovItem as any).label && (
                  <div style={{ color: 'var(--text-muted)', fontSize: 10, marginTop: 6, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                    {(hovItem as any).label}
                  </div>
                )}
                {/* Click hint */}
                <div style={{ color: 'var(--text-muted)', fontSize: 9, marginTop: 6, opacity: 0.6 }}>Click for details</div>
              </>
            ) : (
              /* Hovering on empty space — show full breakdown */
              <>
                {isDualMode ? (
                  <>
                    <div style={{ color: dualColors[0] }}>{dualLabels[0]}: {formatBytes(snap.line1Val)}/s</div>
                    <div style={{ color: dualColors[1] }}>{dualLabels[1]}: {formatBytes(snap.line2Val)}/s</div>
                    <div style={{ color: 'var(--text-secondary)' }}>Total: {formatBytes(snap.line1Val + snap.line2Val)}/s</div>
                  </>
                ) : (
                  <>
                    <div style={{ color: 'var(--text-secondary)' }}>
                      {snap.hostCpuValues ? 'Cluster avg' : 'Server'}: {cfg.fmtVal(snap.srv)}{metricMode === 'cpu' ? '/s' : ''}{metricMode === 'cpu' && data.cpu_cores > 0 ? ` (${((snap.srv / (data.cpu_cores * 1_000_000)) * 100).toFixed(1)}%)` : ''}
                    </div>
                    {snap.hostCpuValues && data.cpu_cores > 0 && (
                      <div style={{ marginTop: 6, marginBottom: 2, padding: '6px 0' }}>
                        {snap.hostCpuValues.map(({ host, v }) => {
                          const pct = Math.min((v / (data.cpu_cores * 1_000_000)) * 100, 100);
                          const barGlow = pct > 80 ? '0 0 6px rgba(248,81,73,0.4)' : pct > 50 ? '0 0 6px rgba(240,136,62,0.3)' : 'none';
                          const shortHost = host.length > 20 ? host.replace(/^[^-]*-/, '').slice(-16) : host;
                          return (
                            <div key={host} style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 10, lineHeight: '18px' }}>
                              <span style={{ color: pct > 80 ? '#f85149' : pct > 50 ? '#f0883e' : 'var(--text-muted)', width: 84, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', flexShrink: 0, textAlign: 'right', fontFamily: 'monospace', fontSize: 9 }}>{shortHost}</span>
                              <div style={{ flex: 1, height: 8, background: 'rgba(255,255,255,0.04)', borderRadius: 4, overflow: 'hidden', minWidth: 70, border: '1px solid rgba(255,255,255,0.06)' }}>
                                <div style={{
                                  width: `${Math.max(pct, 1)}%`, height: '100%', borderRadius: 4,
                                  background: pct > 80 ? 'linear-gradient(90deg, #f85149, #ff6b6b)' : pct > 50 ? 'linear-gradient(90deg, #f0883e, #ffb347)' : 'linear-gradient(90deg, #238636, #3fb950)',
                                  boxShadow: barGlow,
                                  transition: 'width 0.15s ease',
                                }} />
                              </div>
                              <span style={{ color: pct > 80 ? '#f85149' : pct > 50 ? '#f0883e' : 'var(--text-muted)', width: 34, textAlign: 'right', flexShrink: 0, fontWeight: pct > 50 ? 600 : 400 }}>{pct.toFixed(0)}%</span>
                            </div>
                          );
                        })}
                      </div>
                    )}
                  </>
                )}
                {snap.aq.length > 0 && (
                  <div style={{ marginTop: 6 }}>
                    <div style={{ color: '#58a6ff', fontWeight: 500 }}>Queries: {snap.aq.length} ({cfg.fmtVal(snap.aq.reduce((s, x) => s + x.rp, 0))}{metricMode !== 'memory' ? '/s' : ''})</div>
                    {snap.aq.slice(0, 4).map(a => {
                      const q = data.queries[a.i];
                      const hostTag = hc > 1 && q.hostname ? ` [${q.hostname.replace(/^[^-]*-/, '').slice(-14)}]` : '';
                      return (
                        <div key={a.i} style={{ color: 'var(--text-muted)', paddingLeft: 8, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                          {q.query_id.slice(0, 8)} {cfg.fmtVal(a.rp)} {q.label.slice(0, 30)}{hostTag}
                        </div>
                      );
                    })}
                    {snap.aq.length > 4 && <div style={{ color: 'var(--text-muted)', paddingLeft: 8 }}>+{snap.aq.length - 4} more</div>}
                  </div>
                )}
                {snap.am.length > 0 && (
                  <div style={{ marginTop: 4 }}>
                    <div style={{ color: '#f0883e', fontWeight: 500 }}>Merges: {snap.am.length} ({cfg.fmtVal(snap.am.reduce((s, x) => s + x.rp, 0))}{metricMode !== 'memory' ? '/s' : ''})</div>
                    {snap.am.slice(0, 3).map(a => {
                      const m = data.merges[a.i];
                      const hostTag = hc > 1 && m.hostname ? ` [${m.hostname.replace(/^[^-]*-/, '').slice(-14)}]` : '';
                      return (
                        <div key={a.i} style={{ color: 'var(--text-muted)', paddingLeft: 8, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                          {m.table} {cfg.fmtVal(a.rp)}/s{hostTag}
                        </div>
                      );
                    })}
                  </div>
                )}
                {snap.amut.length > 0 && (
                  <div style={{ marginTop: 4 }}>
                    <div style={{ color: '#f778ba', fontWeight: 500 }}>Mutations: {snap.amut.length} ({cfg.fmtVal(snap.amut.reduce((s, x) => s + x.rp, 0))}{metricMode !== 'memory' ? '/s' : ''})</div>
                    {snap.amut.slice(0, 3).map(a => {
                      const mu = (data.mutations ?? [])[a.i];
                      const hostTag = hc > 1 && mu?.hostname ? ` [${mu.hostname.replace(/^[^-]*-/, '').slice(-14)}]` : '';
                      return (
                        <div key={a.i} style={{ color: 'var(--text-muted)', paddingLeft: 8, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                          {mu?.table} {cfg.fmtVal(a.rp)}/s{hostTag}
                        </div>
                      );
                    })}
                  </div>
                )}
              </>
            )}
          </div>
        );
      })()}
    </div>
  );
};

