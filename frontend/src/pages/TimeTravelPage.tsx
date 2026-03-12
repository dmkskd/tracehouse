/**
 * TimeTravelPage - Memory/CPU/Network/Disk IO timeline with interactive hover + click + drag-to-zoom.
 * Toggle buttons switch Y-axis metric. Same time axis, hover, pin, zoom across all views.
 */
import React, { useState, useCallback, useMemo, useRef, useEffect } from 'react';
import { useConnectionStore } from '../stores/connectionStore';
import { useClickHouseServices } from '../providers/ClickHouseProvider';
import { useClusterStore } from '../stores/clusterStore';
import { useRefreshConfig, clampToAllowed } from '@tracehouse/ui-shared';
import { useRefreshSettingsStore } from '../stores/refreshSettingsStore';
import { useGlobalLastUpdatedStore } from '../stores/refreshSettingsStore';
import { useCapabilityCheck } from '../components/shared/RequiresCapability';
import type { MemoryTimeline, QuerySeries, MergeSeries, MutationSeries } from '@tracehouse/core';
import { TIMELINE_ACTIVITY_LIMIT } from '@tracehouse/core';
import { TimelineNavigator } from '../components/shared/TimelineNavigator';
import { QueryDetailModal } from '../components/query/QueryDetailModal';
import { MergeDetailModal, MutationDetailModal } from '../components/merge/MergeDetailModal';
import { TruncatedHost } from '../components/common/TruncatedHost';
import { formatBytes, parseTimestamp } from '../utils/formatters';
import { TimelineChart } from '../components/timeline/TimelineChart';
import { QueryTable, MergeTable } from '../components/timeline/TimelineTable';
import {
  type MetricMode, type HighlightedItem,
  Q_COLORS, M_COLORS, MUT_COLORS, METRIC_CONFIG, getMetricValue,
} from '../components/timeline/timeline-constants';

// CSS animation for pulse effect
const pulseKeyframes = `
  @keyframes pulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.5; }
  }
  @keyframes bandPulse {
    0%, 100% { opacity: 0.7; }
    50% { opacity: 0.45; }
  }
`;
if (typeof document !== 'undefined') {
  const style = document.createElement('style');
  style.textContent = pulseKeyframes;
  document.head.appendChild(style);
}

type SortField = 'metric' | 'duration' | 'started';
type SortDir = 'asc' | 'desc';

export const TimeTravelPage: React.FC = () => {
  const { activeProfileId, profiles } = useConnectionStore();
  const services = useClickHouseServices();
  const { detected: clusterDetected } = useClusterStore();
  const refreshConfig = useRefreshConfig();
  const { refreshRateSeconds } = useRefreshSettingsStore();
  const manualRefreshTick = useGlobalLastUpdatedStore(s => s.manualRefreshTick);
  const { available: hasMetricLog, missing: missingCaps, probing: isCapProbing } = useCapabilityCheck(['metric_log', 'query_log']);
  const [windowSec, setWindowSec] = useState(150);
  const [isLive, setIsLive] = useState(true);
  const [autoRefresh, setAutoRefresh] = useState(false);
  const [customEndTime, setCustomEndTime] = useState<string | null>(null);
  const [data, setData] = useState<MemoryTimeline | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hoverMs, setHoverMs] = useState<number | null>(null);
  const [pinnedMs, setPinnedMs] = useState<number | null>(null);
  const [zoomRange, setZoomRange] = useState<[number, number] | null>(null);
  const [metricMode, setMetricMode] = useState<MetricMode>('cpu');
  const [highlightedItem, setHighlightedItem] = useState<HighlightedItem>(null);
  const [hiddenCategories, setHiddenCategories] = useState<Set<'query' | 'merge' | 'mutation'>>(new Set());
  const toggleCategory = useCallback((cat: 'query' | 'merge' | 'mutation') => {
    setHiddenCategories(prev => {
      const next = new Set(prev);
      if (next.has(cat)) next.delete(cat); else next.add(cat);
      return next;
    });
  }, []);
  const [activityLimit, setActivityLimit] = useState(TIMELINE_ACTIVITY_LIMIT);
  const autoRefreshRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const prevDataEndRef = useRef<number | null>(null);

  // Cluster host selector
  const [clusterHosts, setClusterHosts] = useState<string[]>([]);
  const [selectedHost, setSelectedHost] = useState<string | null>(null);

  // Split view: show one chart per host stacked vertically
  const [splitView, setSplitView] = useState(false);
  const [perHostData, setPerHostData] = useState<Map<string, MemoryTimeline>>(new Map());
  const [splitLoading, setSplitLoading] = useState(false);

  // Modal state
  const [selectedTimelineQuery, setSelectedTimelineQuery] = useState<QuerySeries | null>(null);
  const [selectedTimelineMerge, setSelectedTimelineMerge] = useState<MergeSeries | null>(null);
  const [selectedTimelineMutation, setSelectedTimelineMutation] = useState<MutationSeries | null>(null);

  // Navigator state
  const NAVIGATOR_HOURS = 1;
  const [navigatorData, setNavigatorData] = useState<MemoryTimeline | null>(null);
  const [navigatorLoading, setNavigatorLoading] = useState(false);
  const lastNavigatorFetchTime = useRef<string | null>(null);
  const lastNavigatorMetric = useRef<MetricMode | null>(null);

  // Dragging state: visual-only viewport position during drag (no main chart fetch)
  const [dragEndMs, setDragEndMs] = useState<number | null>(null);
  const dragEndMsRef = useRef<number | null>(null);

  const navigatorDataRange = useMemo(() => {
    if (!navigatorData) return null;
    return {
      startMs: new Date(navigatorData.window_start).getTime(),
      endMs: new Date(navigatorData.window_end).getTime(),
    };
  }, [navigatorData]);

  // During drag, extend the navigator range just enough to fit the viewport (no extra padding)
  // Frozen rangeMs in drag state ensures delta calc is immune to range changes
  const navigatorRange = useMemo(() => {
    if (!navigatorDataRange) return null;
    if (dragEndMs == null) return navigatorDataRange;
    const vpStart = dragEndMs - windowSec * 2 * 1000;
    return {
      startMs: Math.min(navigatorDataRange.startMs, vpStart),
      endMs: Math.max(navigatorDataRange.endMs, Math.min(dragEndMs, Date.now())),
    };
  }, [navigatorDataRange, dragEndMs, windowSec]);

  // Sort state
  const [sortField, setSortField] = useState<SortField>('metric');
  const [sortDir, setSortDir] = useState<SortDir>('desc');
  const [includeRunning, setIncludeRunning] = useState(true);

  const handleSort = (field: SortField) => {
    if (sortField === field) setSortDir(sortDir === 'desc' ? 'asc' : 'desc');
    else { setSortField(field); setSortDir('desc'); }
  };

  let activeProfile = profiles.find(p => p.id === activeProfileId);
  if (!activeProfile && profiles.length > 0) activeProfile = profiles.find(p => p.is_connected);
  const isConnected = activeProfile?.is_connected ?? false;

  const fetchData = useCallback(async () => {
    if (!services) return;
    setIsLoading(true); setError(null);
    try {
      const endDate = isLive ? new Date() : (customEndTime ? new Date(customEndTime) : new Date());
      const centerDate = new Date(endDate.getTime() - windowSec * 1000);
      const result = await services.timelineService.getTimeline({
        timestamp: centerDate,
        windowSeconds: windowSec,
        includeRunning,
        hostname: selectedHost,
        activityLimit,
        activeMetric: metricMode,
      });
      // In live mode, slide zoom/pin forward to follow the advancing time window
      const newEndMs = new Date(result.window_end).getTime();
      if (isLive && prevDataEndRef.current != null) {
        const delta = newEndMs - prevDataEndRef.current;
        if (delta > 0) {
          setZoomRange(prev => prev ? [prev[0] + delta, prev[1] + delta] : null);
          setPinnedMs(prev => prev != null ? prev + delta : null);
        }
      }
      prevDataEndRef.current = newEndMs;
      setData(result);
      useGlobalLastUpdatedStore.getState().touch();
    } catch (e) {
      const msg = e instanceof Error ? e.message : 'Failed to fetch timeline';
      console.error('[TimeTravelPage] Error:', msg, e);
      setError(msg);
    }
    finally { setIsLoading(false); }
  }, [services, isLive, customEndTime, windowSec, includeRunning, selectedHost, activityLimit, metricMode]);

  // Fetch cluster hosts on connect (after cluster detection completes)
  useEffect(() => {
    if (!services || !isConnected || !clusterDetected) { setClusterHosts([]); return; }
    services.metricsCollector.getClusterHosts().then(hosts => setClusterHosts(hosts));
  }, [services, isConnected, clusterDetected]);

  // Fetch per-host data when split view is active
  const fetchSplitData = useCallback(async () => {
    if (!services || clusterHosts.length < 2 || !splitView) return;
    setSplitLoading(true);
    try {
      const endDate = isLive ? new Date() : (customEndTime ? new Date(customEndTime) : new Date());
      const centerDate = new Date(endDate.getTime() - windowSec * 1000);
      const results = await Promise.all(
        clusterHosts.map(async (host) => {
          const result = await services.timelineService.getTimeline({
            timestamp: centerDate,
            windowSeconds: windowSec,
            includeRunning,
            hostname: host,
            activityLimit,
            activeMetric: metricMode,
          });
          return [host, result] as const;
        })
      );
      setPerHostData(new Map(results));
    } catch (e) {
      console.error('[TimeTravelPage] Split view fetch error:', e);
    } finally { setSplitLoading(false); }
  }, [services, clusterHosts, splitView, isLive, customEndTime, windowSec, includeRunning, metricMode]);

  const splitFetchTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  useEffect(() => {
    if (splitView && services && isConnected && clusterHosts.length > 1) {
      if (splitFetchTimeoutRef.current) clearTimeout(splitFetchTimeoutRef.current);
      splitFetchTimeoutRef.current = setTimeout(() => fetchSplitData(), 250);
    }
    return () => { if (splitFetchTimeoutRef.current) clearTimeout(splitFetchTimeoutRef.current); };
  }, [splitView, services, isConnected, windowSec, isLive, customEndTime, includeRunning, fetchSplitData]); // eslint-disable-line react-hooks/exhaustive-deps

  // Auto-analyze when params change (debounced)
  const fetchDataTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  useEffect(() => {
    if (services && isConnected && !isLoading) {
      // Clear zoom/pin when user changes time parameters
      setZoomRange(null); setPinnedMs(null); prevDataEndRef.current = null;
      if (fetchDataTimeoutRef.current) clearTimeout(fetchDataTimeoutRef.current);
      fetchDataTimeoutRef.current = setTimeout(() => fetchData(), 200);
    }
    return () => { if (fetchDataTimeoutRef.current) clearTimeout(fetchDataTimeoutRef.current); };
  }, [services, isConnected, windowSec, isLive, customEndTime, includeRunning, selectedHost, activityLimit, metricMode]); // eslint-disable-line react-hooks/exhaustive-deps

  // Manual refresh from header button
  useEffect(() => {
    if (manualRefreshTick > 0 && services && isConnected) {
      fetchData();
      if (splitView) fetchSplitData();
    }
  }, [manualRefreshTick]); // eslint-disable-line react-hooks/exhaustive-deps

  // Auto-refresh timer
  useEffect(() => {
    if (autoRefreshRef.current) { clearInterval(autoRefreshRef.current); autoRefreshRef.current = null; }
    if (autoRefresh && refreshRateSeconds > 0 && isLive && services && isConnected) {
      const intervalMs = clampToAllowed(refreshRateSeconds, refreshConfig) * 1000;
      autoRefreshRef.current = setInterval(() => { fetchData(); if (splitView) fetchSplitData(); }, intervalMs);
    }
    return () => { if (autoRefreshRef.current) { clearInterval(autoRefreshRef.current); autoRefreshRef.current = null; } };
  }, [autoRefresh, refreshRateSeconds, refreshConfig, isLive, services, isConnected, fetchData, splitView, fetchSplitData]);

  // Fetch extended navigator data (2 hours)
  const fetchNavigatorData = useCallback(async (force = false) => {
    if (!services) return;
    const endDate = isLive ? new Date() : (customEndTime ? new Date(customEndTime) : new Date());
    const endTimeKey = `${endDate.toISOString().slice(0, 13)}_${metricMode}`;
    if (!force && lastNavigatorFetchTime.current === endTimeKey) return;
    if (!force && navigatorData && lastNavigatorMetric.current === metricMode) {
      const navStart = new Date(navigatorData.window_start).getTime();
      const navEnd = new Date(navigatorData.window_end).getTime();
      const viewportEnd = endDate.getTime();
      const viewportStart = viewportEnd - windowSec * 2 * 1000;
      const margin = (navEnd - navStart) * 0.1;
      if (viewportStart >= navStart - margin && viewportEnd <= navEnd + margin) return;
    }
    setNavigatorLoading(true);
    try {
      const navigatorWindowSec = NAVIGATOR_HOURS * 60 * 60 / 2;
      const centerDate = new Date(endDate.getTime() - navigatorWindowSec * 1000);
      const result = await services.timelineService.getTimeline({
        timestamp: centerDate,
        windowSeconds: navigatorWindowSec,
        hostname: selectedHost,
        activityLimit,
        activeMetric: metricMode,
      });
      setNavigatorData(result);
      lastNavigatorFetchTime.current = endTimeKey;
      lastNavigatorMetric.current = metricMode;
    } catch (e) {
      console.error('[TimeTravelPage] Navigator fetch error:', e);
    } finally { setNavigatorLoading(false); }
  }, [services, isLive, customEndTime, NAVIGATOR_HOURS, navigatorData, windowSec, selectedHost, activityLimit, metricMode]);

  // Debounced navigator fetch
  const navigatorFetchTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  useEffect(() => {
    if (services && isConnected) {
      if (navigatorFetchTimeoutRef.current) clearTimeout(navigatorFetchTimeoutRef.current);
      navigatorFetchTimeoutRef.current = setTimeout(() => fetchNavigatorData(), 300);
    }
    return () => { if (navigatorFetchTimeoutRef.current) clearTimeout(navigatorFetchTimeoutRef.current); };
  }, [services, isConnected, customEndTime, isLive, fetchNavigatorData]);

  // Auto-refresh navigator in live mode
  const navigatorRefreshRef = useRef<ReturnType<typeof setInterval> | null>(null);
  useEffect(() => {
    if (navigatorRefreshRef.current) { clearInterval(navigatorRefreshRef.current); navigatorRefreshRef.current = null; }
    if (autoRefresh && refreshRateSeconds > 0 && isLive && services && isConnected) {
      const intervalMs = clampToAllowed(refreshRateSeconds, refreshConfig) * 1000;
      navigatorRefreshRef.current = setInterval(() => fetchNavigatorData(true), intervalMs);
    }
    return () => { if (navigatorRefreshRef.current) { clearInterval(navigatorRefreshRef.current); navigatorRefreshRef.current = null; } };
  }, [autoRefresh, refreshRateSeconds, refreshConfig, isLive, services, isConnected, fetchNavigatorData]);

  // During drag: cheap visual-only update (clamp to now)
  const handleNavigatorViewportChange = useCallback((newEndMs: number) => {
    const clamped = Math.min(newEndMs, Date.now());
    dragEndMsRef.current = clamped;
    setDragEndMs(clamped);
  }, []);

  // On drag end: commit position, clear drag state, let normal fetch cycle update navigator
  const handleNavigatorDragEnd = useCallback((endMs: number) => {
    const clampedEnd = Math.min(endMs, Date.now());
    dragEndMsRef.current = null;
    setDragEndMs(null);
    const now = Date.now();
    if (clampedEnd >= now - 30000) { setIsLive(true); setCustomEndTime(null); }
    else { setIsLive(false); setCustomEndTime(new Date(clampedEnd).toISOString().slice(0, 16)); }
  }, []);

  const viewportBounds = useMemo(() => {
    // During drag, use the drag position; otherwise derive from committed time
    const endMs = dragEndMs ?? (isLive ? Date.now() : (customEndTime ? new Date(customEndTime).getTime() : Date.now()));
    return { startMs: endMs - windowSec * 2 * 1000, endMs };
  }, [dragEndMs, isLive, customEndTime, windowSec]);

  const navigatorMetricData = useMemo(() => {
    if (!navigatorData) return [];
    if (metricMode === 'memory') return navigatorData.server_memory;
    if (metricMode === 'cpu') return navigatorData.server_cpu;
    if (metricMode === 'network') {
      if (navigatorData.server_network_send.length > 0) {
        return navigatorData.server_network_send.map((p, i) => ({
          t: p.t, v: p.v + (navigatorData.server_network_recv[i]?.v ?? 0),
        }));
      }
      return [];
    }
    if (navigatorData.server_disk_read && navigatorData.server_disk_read.length > 0) {
      return navigatorData.server_disk_read.map((p, i) => ({
        t: p.t, v: p.v + (navigatorData.server_disk_write?.[i]?.v ?? 0),
      }));
    }
    return [];
  }, [navigatorData, metricMode]);

  const inspectMs = pinnedMs;

  // Sort helper
  const sortItems = <T extends QuerySeries | MergeSeries | MutationSeries>(items: T[]): T[] => {
    return [...items].sort((a, b) => {
      let aVal: number, bVal: number;
      if (sortField === 'metric') { aVal = getMetricValue(a, metricMode); bVal = getMetricValue(b, metricMode); }
      else if (sortField === 'duration') { aVal = a.duration_ms; bVal = b.duration_ms; }
      else { aVal = parseTimestamp(a.start_time); bVal = parseTimestamp(b.start_time); }
      return sortDir === 'desc' ? bVal - aVal : aVal - bVal;
    });
  };

  const filteredQueries = useMemo(() => {
    if (!data) return [];
    let result: QuerySeries[];
    if (inspectMs !== null) {
      result = data.queries.filter(q => { const s = parseTimestamp(q.start_time), e = parseTimestamp(q.end_time); return inspectMs >= s && inspectMs <= e; });
    } else if (zoomRange) {
      result = data.queries.filter(q => { const s = parseTimestamp(q.start_time), e = parseTimestamp(q.end_time); return s <= zoomRange[1] && e >= zoomRange[0]; });
    } else { result = [...data.queries]; }
    return sortItems(result);
  }, [data, inspectMs, zoomRange, metricMode, sortField, sortDir]);

  const filteredMerges = useMemo(() => {
    if (!data) return [];
    let result: MergeSeries[];
    if (inspectMs !== null) {
      result = data.merges.filter(m => { const s = parseTimestamp(m.start_time), e = parseTimestamp(m.end_time); return inspectMs >= s && inspectMs <= e; });
    } else if (zoomRange) {
      result = data.merges.filter(m => { const s = parseTimestamp(m.start_time), e = parseTimestamp(m.end_time); return s <= zoomRange[1] && e >= zoomRange[0]; });
    } else { result = [...data.merges]; }
    return sortItems(result);
  }, [data, inspectMs, zoomRange, metricMode, sortField, sortDir]);

  const filteredMutations = useMemo(() => {
    if (!data) return [];
    let result: MutationSeries[];
    if (inspectMs !== null) {
      result = (data.mutations ?? []).filter(m => { const s = parseTimestamp(m.start_time), e = parseTimestamp(m.end_time); return inspectMs >= s && inspectMs <= e; });
    } else if (zoomRange) {
      result = (data.mutations ?? []).filter(m => { const s = parseTimestamp(m.start_time), e = parseTimestamp(m.end_time); return s <= zoomRange[1] && e >= zoomRange[0]; });
    } else { result = [...(data.mutations ?? [])]; }
    return sortItems(result);
  }, [data, inspectMs, zoomRange, metricMode, sortField, sortDir]);

  // Time range presets (above early returns to keep hooks order stable)
  const TIME_RANGES = [
    { label: 'Now', isLive: true, endTime: null },
    { label: '1h ago', isLive: false, hoursAgo: 1 },
    { label: '3h ago', isLive: false, hoursAgo: 3 },
    { label: '6h ago', isLive: false, hoursAgo: 6 },
    { label: '12h ago', isLive: false, hoursAgo: 12 },
    { label: '1d ago', isLive: false, hoursAgo: 24 },
    { label: 'Custom', isLive: false, custom: true },
  ];
  const WINDOW_SIZES = [
    { label: '1m', sec: 30 },
    { label: '5m', sec: 150 },
    { label: '15m', sec: 450 },
    { label: '30m', sec: 900 },
    { label: '1h', sec: 1800 },
    { label: '3h', sec: 5400 },
    { label: '6h', sec: 10800 },
  ];
  const [selectedTimeRange, setSelectedTimeRange] = useState('Now');
  const [showCustomTime, setShowCustomTime] = useState(false);

  if (!services || !isConnected) {
    return (
      <div style={{ display:'flex', flexDirection:'column', alignItems:'center', justifyContent:'center', height:'100%', gap:16, background:'var(--bg-primary)' }}>
        <div style={{ color:'var(--text-primary)', fontSize:18, fontWeight:600 }}>Time Travel</div>
        <div style={{ color:'var(--text-muted)', fontSize:13 }}>Connect to a ClickHouse server to begin.</div>
      </div>
    );
  }

  if (!hasMetricLog && !isCapProbing) {
    return (
      <div style={{ display:'flex', flexDirection:'column', alignItems:'center', justifyContent:'center', height:'100%', gap:16, background:'var(--bg-primary)' }}>
        <div style={{ color:'var(--text-primary)', fontSize:18, fontWeight:600 }}>Time Travel</div>
        <div style={{ color:'var(--text-muted)', fontSize:13, textAlign:'center', maxWidth:400 }}>
          Requires {missingCaps.map(c => `system.${c}`).join(', ')} (not available on this server)
        </div>
      </div>
    );
  }

  const clearDragPosition = () => { dragEndMsRef.current = null; setDragEndMs(null); };

  const handleTimeRangeChange = (rangeLabel: string) => {
    setSelectedTimeRange(rangeLabel);
    clearDragPosition();
    const range = TIME_RANGES.find(r => r.label === rangeLabel);
    if (!range) return;
    if (range.isLive) { setIsLive(true); setCustomEndTime(null); setShowCustomTime(false); }
    else if (range.custom) {
      setIsLive(false); setShowCustomTime(true);
      if (!customEndTime) setCustomEndTime(new Date().toISOString().slice(0, 16));
    } else if (range.hoursAgo) {
      setIsLive(false); setShowCustomTime(false);
      setCustomEndTime(new Date(Date.now() - range.hoursAgo * 60 * 60 * 1000).toISOString().slice(0, 16));
    }
  };

  return (
    <div style={{ height:'100%', display:'flex', flexDirection:'column', overflow:'hidden', background:'var(--bg-primary)' }}>
      {/* Header bar */}
      <div style={{ padding:'12px 16px 10px', borderBottom:'1px solid var(--border-primary)', background:'var(--bg-secondary)', flexShrink:0 }}>
        <div style={{ display:'flex', alignItems:'center', justifyContent:'space-between', marginBottom:12 }}>
          <div style={{ display:'flex', alignItems:'center', gap:12 }}>
            <h2 style={{ color:'var(--text-primary)', fontSize:18, fontWeight:600, margin:0 }}>Time Travel</h2>
            <span style={{ color:'var(--text-muted)', fontSize:12 }}>Hover to inspect · Click to pin · Drag to zoom</span>
          </div>

          {/* Right side: Time range picker */}
          <div style={{ display:'flex', alignItems:'center', gap:8 }}>
            {/* Time range selector */}
            <div style={{ display:'flex', alignItems:'center', gap:0, background:'var(--bg-tertiary)', borderRadius:6, border:'1px solid var(--border-primary)', overflow:'hidden' }}>
              <span style={{ padding:'8px 10px', fontSize:11, color:'var(--text-muted)', borderRight:'1px solid var(--border-primary)', whiteSpace:'nowrap' }}>From</span>
              <select value={selectedTimeRange} onChange={(e) => handleTimeRangeChange(e.target.value)} title="Select time range starting point"
                style={{ background:'transparent', color:'var(--text-primary)', border:'none', padding:'8px 12px', fontSize:13, outline:'none', cursor:'pointer', minWidth:90 }}>
                {TIME_RANGES.map(r => <option key={r.label} value={r.label}>{r.label}</option>)}
              </select>
              {showCustomTime && (
                <input type="datetime-local" value={customEndTime || ''} onChange={(e) => { clearDragPosition(); setCustomEndTime(e.target.value); }} step="1" title="Select custom end time"
                  style={{ background:'transparent', color:'var(--text-primary)', border:'none', borderLeft:'1px solid var(--border-primary)', padding:'8px 12px', fontSize:13, outline:'none' }} />
              )}
            </div>

            {/* Window size selector */}
            <div style={{ display:'flex', alignItems:'center', gap:0, background:'var(--bg-tertiary)', borderRadius:6, border:'1px solid var(--border-primary)', overflow:'hidden' }}>
              <span style={{ padding:'8px 10px', fontSize:11, color:'var(--text-muted)', borderRight:'1px solid var(--border-primary)', whiteSpace:'nowrap' }}>Window</span>
              <select value={windowSec} onChange={(e) => setWindowSec(Number(e.target.value))} title="Select time window duration"
                style={{ background:'transparent', color:'var(--text-primary)', border:'none', padding:'8px 12px', fontSize:13, outline:'none', cursor:'pointer', minWidth:60 }}>
                {WINDOW_SIZES.map(w => <option key={w.sec} value={w.sec}>{w.label}</option>)}
              </select>
            </div>

            {/* Activity limit selector */}
            <div style={{ display:'flex', alignItems:'center', gap:0, background:'var(--bg-tertiary)', borderRadius:6, border:'1px solid var(--border-primary)', overflow:'hidden' }}>
              <span style={{ padding:'8px 10px', fontSize:11, color:'var(--text-muted)', borderRight:'1px solid var(--border-primary)', whiteSpace:'nowrap' }}>Show</span>
              <select value={activityLimit} onChange={(e) => setActivityLimit(Number(e.target.value))} title="Max items per activity type (queries, merges, mutations)"
                style={{ background:'transparent', color:'var(--text-primary)', border:'none', padding:'8px 12px', fontSize:13, outline:'none', cursor:'pointer', minWidth:50 }}>
                {[25, 50, 100, 250, 500].map(n => <option key={n} value={n}>{n}</option>)}
              </select>
            </div>

            {/* Include running toggle */}
            <button onClick={() => isLive && setIncludeRunning(!includeRunning)} disabled={!isLive}
              title={!isLive ? 'In-flight data only available in live mode' : (includeRunning ? 'Showing in-flight queries/merges - click to hide' : 'Hiding in-flight queries/merges - click to show')}
              style={{
                display:'flex', alignItems:'center', gap:6,
                background: !isLive ? 'var(--bg-tertiary)' : (includeRunning ? 'rgba(88,166,255,0.1)' : 'var(--bg-tertiary)'),
                color: !isLive ? 'var(--text-muted)' : (includeRunning ? '#58a6ff' : 'var(--text-muted)'),
                border: !isLive ? '1px solid var(--border-primary)' : (includeRunning ? '1px solid rgba(88,166,255,0.3)' : '1px solid var(--border-primary)'),
                borderRadius:6, padding:'8px 12px', fontSize:11, cursor: isLive ? 'pointer' : 'not-allowed',
                fontWeight: isLive && includeRunning ? 500 : 400, opacity: isLive ? 1 : 0.5,
              }}>
              {isLive && includeRunning && <span style={{ display:'inline-block', width:6, height:6, borderRadius:'50%', background:'#58a6ff', animation:'pulse 1.5s ease-in-out infinite' }} />}
              In-flight
            </button>

            {/* Go Live button */}
            {!isLive && (
              <button onClick={() => { clearDragPosition(); setIsLive(true); setCustomEndTime(null); }} title="Jump to current time"
                style={{ display:'flex', alignItems:'center', gap:6, background:'rgba(63,185,80,0.15)', color:'#3fb950', border:'1px solid rgba(63,185,80,0.4)', borderRadius:6, padding:'8px 12px', fontSize:11, cursor:'pointer', fontWeight:500 }}>
                <span style={{ display:'inline-block', width:6, height:6, borderRadius:'50%', background:'#3fb950' }} />
                Go Live
              </button>
            )}

            {/* Auto-refresh toggle */}
            <button onClick={() => setAutoRefresh(!autoRefresh)} title={autoRefresh ? 'Auto-refresh is on — click to pause' : 'Auto-refresh is off — click to enable'}
              style={{
                display:'flex', alignItems:'center', gap:6,
                background: autoRefresh ? 'rgba(63,185,80,0.1)' : 'var(--bg-tertiary)',
                color: autoRefresh ? '#3fb950' : 'var(--text-muted)',
                border: autoRefresh ? '1px solid rgba(63,185,80,0.3)' : '1px solid var(--border-primary)',
                borderRadius:6, padding:'8px 12px', fontSize:11, cursor:'pointer', fontWeight: autoRefresh ? 500 : 400,
              }}>
              {autoRefresh && <span style={{ display:'inline-block', width:6, height:6, borderRadius:'50%', background:'#3fb950', animation:'pulse 1.5s ease-in-out infinite' }} />}
              Auto
            </button>

            {/* Manual refresh */}
            <button onClick={fetchData} disabled={isLoading} title="Refresh data manually"
              style={{ background:'var(--bg-tertiary)', color:'var(--text-primary)', border:'1px solid var(--border-primary)', borderRadius:6, padding:'8px 14px', fontSize:14, cursor:'pointer', opacity: isLoading ? 0.5 : 1, fontFamily:'monospace' }}>
              {isLoading ? '⋯' : '↻'}
            </button>
          </div>
        </div>

        {/* Second row: Host selector, stats */}
        <div style={{ display:'flex', alignItems:'center', gap:12, flexWrap:'wrap' }}>
          {clusterHosts.length > 1 && (
            <div className="tabs">
              <button className={`tab ${selectedHost === null && !splitView ? 'active' : ''}`} onClick={() => { setSelectedHost(null); setSplitView(false); }}>All</button>
              <button className={`tab ${splitView ? 'active' : ''}`}
                onClick={() => { setSelectedHost(null); setSplitView(!splitView); }}
                title="Split view: one chart per server, stacked vertically"
                style={{ display:'flex', alignItems:'center', gap:4 }}>
                <svg width="10" height="10" viewBox="0 0 10 10" style={{ opacity: 0.7 }}>
                  <rect x="0" y="0" width="10" height="4" rx="1" fill="currentColor" />
                  <rect x="0" y="6" width="10" height="4" rx="1" fill="currentColor" />
                </svg> Split
              </button>
              {clusterHosts.map(host => (
                <button key={host} className={`tab ${selectedHost === host ? 'active' : ''}`} onClick={() => { setSelectedHost(host); setSplitView(false); }}>
                  <TruncatedHost name={host} />
                </button>
              ))}
            </div>
          )}
          {data && (
            <div style={{ display:'flex', gap:8 }}>
              {[
                { label: 'Points', value: data.server_memory.length, color: '#58a6ff' },
                { label: 'Queries', value: `${filteredQueries.length}/${data.query_count ?? data.queries.length}`, color: '#79c0ff' },
                { label: 'Merges', value: `${filteredMerges.length}/${data.merge_count}`, color: '#f0883e' },
                ...((data.mutation_count ?? 0) > 0 ? [{ label: 'Mutations', value: `${filteredMutations.length}/${data.mutation_count}`, color: '#f778ba' }] : []),
                ...(data.server_total_ram > 0 ? [{ label: 'RAM', value: (data.host_count || 1) > 1 ? `${formatBytes(data.server_total_ram)}/host (${data.host_count || 1} hosts)` : formatBytes(data.server_total_ram), color: '#f85149' }] : []),
                ...(data.cpu_cores > 0 ? [{ label: 'CPUs', value: (data.host_count || 1) > 1 ? `${data.cpu_cores}/host (${data.host_count || 1} hosts)` : `${data.cpu_cores}`, color: '#3fb950' }] : []),
              ].map((s, i) => (
                <span key={i} style={{ fontSize:11, padding:'2px 8px', borderRadius:10, background:`${s.color}15`, color:s.color, border:`1px solid ${s.color}33` }}>
                  {s.label}: {s.value}
                </span>
              ))}
            </div>
          )}
        </div>
      </div>

      {error && (
        <div style={{ margin:'12px 24px 0', padding:'10px 14px', borderRadius:8, fontSize:13, background:'rgba(248,81,73,0.08)', color:'#f85149', border:'1px solid rgba(248,81,73,0.2)' }}>
          {error}
        </div>
      )}

      {data && (
        <div style={{ flex:1, overflow:'auto', padding:'12px 16px 20px' }}>
          {/* Metric toggle tabs */}
          <div style={{ display:'flex', gap:4, marginBottom:16, background:'var(--bg-tertiary)', borderRadius:8, padding:3, width:'fit-content', alignItems:'center' }}>
            {(Object.keys(METRIC_CONFIG) as MetricMode[]).map(mode => {
              const c = METRIC_CONFIG[mode];
              const active = metricMode === mode;
              return (
                <button key={mode} onClick={() => setMetricMode(mode)}
                  style={{
                    background: active ? `${c.color}20` : 'transparent',
                    color: active ? c.color : 'var(--text-muted)',
                    border: active ? `1px solid ${c.color}44` : '1px solid transparent',
                    borderRadius:6, padding:'6px 14px', fontSize:12, fontWeight:active ? 600 : 400,
                    cursor:'pointer', transition:'all 0.15s ease',
                  }}>
                  {c.label}
                </button>
              );
            })}
            {metricMode === 'cpu' && (
              <span
                title="CPU % is clamped to 100%. Under heavy load, ClickHouse's metric_log collection can be delayed — when load drops, accumulated CPU time gets attributed to a short interval, producing raw values above 100%. This is amplified on VMs (GCP, AWS, Docker) due to hypervisor scheduling jitter. The Spike Analysis feature preserves unclamped values for diagnostics."
                style={{
                  display:'inline-flex', alignItems:'center', justifyContent:'center',
                  width:18, height:18, borderRadius:'50%', marginLeft:4,
                  fontSize:11, fontWeight:600, cursor:'help',
                  color:'var(--text-muted)', background:'var(--bg-primary)',
                  border:'1px solid var(--border-primary)', transition:'all 0.15s ease',
                }}
                role="img"
                aria-label="CPU metric information: values are clamped to 100% to account for metric collection delays under heavy load"
              >?</span>
            )}
          </div>

          {/* Chart */}
          {splitView && clusterHosts.length > 1 ? (
            /* Split view: one chart per host */
            <div style={{ display:'flex', flexDirection:'column', gap:2 }}>
              {splitLoading && perHostData.size === 0 && (
                <div style={{ padding:20, textAlign:'center', color:'var(--text-muted)', fontSize:13 }}>Loading per-host data…</div>
              )}
              {clusterHosts.map((host) => {
                const hostData = perHostData.get(host);
                if (!hostData) return null;
                const chartHeight = Math.max(140, Math.floor(460 / clusterHosts.length));
                return (
                  <div key={host} style={{
                    borderRadius:8, background:'var(--bg-secondary)', border:'1px solid var(--border-primary)',
                    overflow:'visible', position:'relative',
                  }}
                  onMouseEnter={(e) => { (e.currentTarget as HTMLElement).style.zIndex = '20'; }}
                  onMouseLeave={(e) => { (e.currentTarget as HTMLElement).style.zIndex = '0'; }}
                  >
                    <button
                      onClick={() => { setSelectedHost(host); setSplitView(false); }}
                      title={`Switch to ${host}`}
                      style={{
                        position:'absolute', top:6, left:10, zIndex:10, fontSize:10, fontWeight:600,
                        color:'var(--text-secondary)', background:'var(--bg-tertiary)', padding:'2px 8px',
                        borderRadius:4, border:'1px solid var(--border-primary)', opacity:0.9,
                        cursor:'pointer',
                      }}
                    >
                      <TruncatedHost name={host} />
                      {hostData.cpu_cores > 0 && <span style={{ color:'var(--text-muted)', marginLeft:6 }}>{hostData.cpu_cores} cores</span>}
                      {hostData.server_total_ram > 0 && <span style={{ color:'var(--text-muted)', marginLeft:6 }}>{formatBytes(hostData.server_total_ram)} RAM</span>}
                    </button>
                    <TimelineChart data={hostData} metricMode={metricMode} height={chartHeight}
                      hoverMs={hoverMs} pinnedMs={pinnedMs}
                      onHover={setHoverMs} onPin={setPinnedMs}
                      zoomRange={zoomRange} onZoom={setZoomRange}
                      highlightedItem={highlightedItem}
                      onHighlightItem={setHighlightedItem}
                      hiddenCategories={hiddenCategories}
                      onBandClick={(band) => {
                        if (band.type === 'query' && hostData.queries[band.idx]) setSelectedTimelineQuery(hostData.queries[band.idx]);
                        else if (band.type === 'merge' && hostData.merges[band.idx]) setSelectedTimelineMerge(hostData.merges[band.idx]);
                        else if (band.type === 'mutation' && (hostData.mutations ?? [])[band.idx]) setSelectedTimelineMutation((hostData.mutations ?? [])[band.idx]);
                      }} />
                  </div>
                );
              })}
            </div>
          ) : (
          /* Single chart (All averaged or single host) */
          <div style={{
            borderRadius:10, padding:0, background:'var(--bg-secondary)', border:'1px solid var(--border-primary)',
            boxShadow:'0 1px 3px rgba(0,0,0,0.2)', overflow:'hidden', position:'relative',
          }}>
            {(pinnedMs !== null || zoomRange !== null) && (
              <div style={{ position:'absolute', top:8, right:8, zIndex:10, display:'flex', alignItems:'center', gap:6 }}>
                {pinnedMs !== null && (
                  <button onClick={() => setPinnedMs(null)} style={{ padding:'3px 8px', borderRadius:6, fontSize:11, background:'rgba(63,185,80,0.12)', border:'1px solid rgba(63,185,80,0.25)', color:'#3fb950', cursor:'pointer', backdropFilter:'blur(8px)' }}>
                    ✕ Pinned at {new Date(pinnedMs).toLocaleTimeString()}
                  </button>
                )}
                {zoomRange !== null && (
                  <button onClick={() => setZoomRange(null)} style={{ padding:'3px 8px', borderRadius:6, fontSize:11, background:'rgba(88,166,255,0.12)', border:'1px solid rgba(88,166,255,0.25)', color:'#58a6ff', cursor:'pointer', backdropFilter:'blur(8px)' }}>
                    ✕ Reset zoom
                  </button>
                )}
              </div>
            )}
            {metricMode === 'cpu' && data.server_cpu.length > 0 && data.server_cpu.every(p => p.v === 0) && (
              <div style={{
                position:'absolute', top:8, left:'50%', transform:'translateX(-50%)', zIndex:10, fontSize:11, color:'var(--text-muted)',
                background:'var(--bg-tertiary)', border:'1px solid var(--border-secondary)', borderRadius:6, padding:'4px 12px', pointerEvents:'none', opacity:0.9,
              }}>
                Server CPU reads as 0 — ClickHouse on macOS does not expose CPU metrics. Run in Docker or Linux for CPU data.
              </div>
            )}
            <TimelineChart data={data} metricMode={metricMode} height={500}
              hoverMs={hoverMs} pinnedMs={pinnedMs}
              onHover={setHoverMs} onPin={setPinnedMs}
              zoomRange={zoomRange} onZoom={setZoomRange}
              highlightedItem={highlightedItem}
              onHighlightItem={setHighlightedItem}
              hiddenCategories={hiddenCategories}
              onBandClick={(band) => {
                if (band.type === 'query' && data.queries[band.idx]) setSelectedTimelineQuery(data.queries[band.idx]);
                else if (band.type === 'merge' && data.merges[band.idx]) setSelectedTimelineMerge(data.merges[band.idx]);
                else if (band.type === 'mutation' && (data.mutations ?? [])[band.idx]) setSelectedTimelineMutation((data.mutations ?? [])[band.idx]);
              }} />
          </div>
          )}

          {/* Timeline Navigator */}
          {navigatorRange && (
            <div style={{ marginTop: 12 }}>
              <div style={{ display:'flex', alignItems:'center', gap:8, marginBottom:6, fontSize:11, color:'var(--text-muted)' }}>
                <span style={{ textTransform:'uppercase', letterSpacing:'0.5px' }}>{NAVIGATOR_HOURS}h Overview</span>
                <span style={{ color: METRIC_CONFIG[metricMode].color }}>{METRIC_CONFIG[metricMode].label}</span>
                <span>· Drag window to navigate</span>
              </div>
              <TimelineNavigator
                data={navigatorMetricData} metricMode={metricMode}
                rangeStartMs={navigatorRange.startMs} rangeEndMs={navigatorRange.endMs}
                viewportStartMs={viewportBounds.startMs} viewportEndMs={viewportBounds.endMs}
                onViewportChange={handleNavigatorViewportChange} height={70}
                isLoading={navigatorLoading} totalRam={data.server_total_ram} cpuCores={data.cpu_cores}
                onDragEnd={handleNavigatorDragEnd}
              />
            </div>
          )}

          {/* Detail tables */}
          <div style={{ display:'grid', gridTemplateColumns:'repeat(3, 1fr)', gap:16, marginTop:16 }}>
            <QueryTable
              queries={filteredQueries} allQueries={data.queries}
              totalCount={data.query_count ?? data.queries.length}
              pinnedMs={pinnedMs} metricMode={metricMode}
              colors={Q_COLORS} accentColor="#58a6ff"
              highlightedItem={highlightedItem} onHighlightItem={setHighlightedItem}
              onSelect={setSelectedTimelineQuery}
              sortField={sortField} sortDir={sortDir} onSort={handleSort}
              showHost={clusterHosts.length > 1}
              isHiddenInChart={hiddenCategories.has('query')}
              onToggleChartVisibility={() => toggleCategory('query')}
            />
            <MergeTable
              items={filteredMerges} allItems={data.merges}
              totalCount={data.merge_count}
              pinnedMs={pinnedMs} metricMode={metricMode}
              colors={M_COLORS} accentColor="#f0883e" highlightColor="rgba(240,136,62,0.35)"
              label="Merges" itemType="merge"
              highlightedItem={highlightedItem} onHighlightItem={setHighlightedItem}
              onSelect={(m) => setSelectedTimelineMerge(m as MergeSeries)}
              sortField={sortField} sortDir={sortDir} onSort={handleSort}
              showHost={clusterHosts.length > 1}
              isHiddenInChart={hiddenCategories.has('merge')}
              onToggleChartVisibility={() => toggleCategory('merge')}
            />
            <MergeTable
              items={filteredMutations} allItems={data.mutations ?? []}
              totalCount={data.mutation_count ?? 0}
              pinnedMs={pinnedMs} metricMode={metricMode}
              colors={MUT_COLORS} accentColor="#f778ba" highlightColor="rgba(247,120,186,0.35)"
              label="Mutations" itemType="mutation"
              highlightedItem={highlightedItem} onHighlightItem={setHighlightedItem}
              onSelect={(m) => setSelectedTimelineMutation(m as MutationSeries)}
              sortField={sortField} sortDir={sortDir} onSort={handleSort}
              showHost={clusterHosts.length > 1}
              isHiddenInChart={hiddenCategories.has('mutation')}
              onToggleChartVisibility={() => toggleCategory('mutation')}
            />
          </div>

          {/* Empty state */}
          {filteredQueries.length === 0 && filteredMerges.length === 0 && filteredMutations.length === 0 && data.server_memory.length > 0 && (
            <div style={{ marginTop:16, padding:'20px', borderRadius:10, textAlign:'center', background:'var(--bg-secondary)', border:'1px solid var(--border-primary)', color:'var(--text-muted)', fontSize:13 }}>
              {pinnedMs !== null ? 'No queries, merges, or mutations active at pinned time.' : 'No queries, merges, or mutations found in this window.'}
            </div>
          )}
        </div>
      )}

      {/* Empty state when no data loaded */}
      {!data && !error && !isLoading && (
        <div style={{ flex:1, display:'flex', flexDirection:'column', alignItems:'center', justifyContent:'center', gap:12 }}>
          <div style={{ color:'var(--text-muted)', fontSize:13 }}>Pick a timestamp and hit Analyze to start exploring.</div>
        </div>
      )}

      {/* Modals */}
      <QueryDetailModal query={selectedTimelineQuery} onClose={() => setSelectedTimelineQuery(null)} />
      <MergeDetailModal merge={selectedTimelineMerge} onClose={() => setSelectedTimelineMerge(null)} />
      <MutationDetailModal mutation={selectedTimelineMutation} onClose={() => setSelectedTimelineMutation(null)} />
    </div>
  );
};

export default TimeTravelPage;
