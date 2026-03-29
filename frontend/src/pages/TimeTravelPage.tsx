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
import { RangeSlider } from '../components/shared/RangeSlider';
import { QueryDetailModal } from '../components/query/QueryDetailModal';
import { MergeDetailModal, MutationDetailModal } from '../components/merge/MergeDetailModal';
import { TruncatedHost } from '../components/common/TruncatedHost';
import { formatBytes, parseTimestamp } from '../utils/formatters';
import { useUserPreferenceStore } from '../stores/userPreferenceStore';
import { DocsLink } from '../components/common/DocsLink';
import { TimelineChart } from '../components/timeline/TimelineChart';
import { TimelineChart3D } from '../components/timeline/TimelineChart3D';
import { TimelineChart3DSurface } from '../components/timeline/TimelineChart3DSurface';
import { QueryTable, MergeTable } from '../components/timeline/TimelineTable';
import {
  type MetricMode, type HighlightedItem,
  Q_COLORS, M_COLORS, MUT_COLORS, METRIC_CONFIG, getMetricValue,
} from '../components/timeline/timeline-constants';

// CSS animation for pulse effect + experimental badge tooltip
const pulseKeyframes = `
  @keyframes pulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.5; }
  }
  @keyframes bandPulse {
    0%, 100% { opacity: 0.7; }
    50% { opacity: 0.45; }
  }
  .exp-badge { position: relative; }
  .exp-badge::after {
    content: 'Experimental feature';
    position: absolute;
    top: calc(100% + 6px);
    left: 50%;
    transform: translateX(-50%) scale(0.95);
    white-space: nowrap;
    font-size: 10px;
    font-weight: 500;
    letter-spacing: 0;
    text-transform: none;
    color: #f0883e;
    background: var(--bg-secondary);
    border: 1px solid rgba(240,136,62,0.25);
    border-radius: 5px;
    padding: 3px 8px;
    pointer-events: none;
    opacity: 0;
    transition: opacity 0.15s ease, transform 0.15s ease;
    z-index: 100;
  }
  .exp-badge:hover::after {
    opacity: 1;
    transform: translateX(-50%) scale(1);
  }
`;
if (typeof document !== 'undefined') {
  const style = document.createElement('style');
  style.textContent = pulseKeyframes;
  document.head.appendChild(style);
}

/** Format a Date (or ms timestamp) as a local datetime string matching <input type="datetime-local"> format. */
function toLocalDatetimeStr(d: Date | number): string {
  const date = typeof d === 'number' ? new Date(d) : d;
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

// Time range presets: each means "show last N hours in scrub bar, ending at now"
const TIME_RANGES = [
  { label: '1h', hoursAgo: 1 },
  { label: '3h', hoursAgo: 3 },
  { label: '6h', hoursAgo: 6 },
  { label: '12h', hoursAgo: 12 },
  { label: '1d', hoursAgo: 24 },
];

// Quick-set range durations for the Custom popover
const CUSTOM_RANGE_PRESETS = [
  { label: '1h', ms: 3600000 },
  { label: '3h', ms: 3 * 3600000 },
  { label: '6h', ms: 6 * 3600000 },
  { label: '12h', ms: 12 * 3600000 },
  { label: '1d', ms: 86400000 },
  { label: '2d', ms: 2 * 86400000 },
  { label: '7d', ms: 7 * 86400000 },
];

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
  const { experimentalEnabled } = useUserPreferenceStore();
  const [windowSec, setWindowSec] = useState(150);
  const [isLive, setIsLive] = useState(true);
  const [autoRefresh, setAutoRefresh] = useState(false);
  const [customStartTime, setCustomStartTime] = useState<string | null>(null);  // Custom range start (navigator)
  const [customEndTime, setCustomEndTime] = useState<string | null>(null);      // Custom range end (navigator)
  const [viewportEndTime, setViewportEndTime] = useState<string | null>(null);  // Viewport position within custom range
  const [data, setData] = useState<MemoryTimeline | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hoverMs, setHoverMs] = useState<number | null>(null);
  const [pinnedMs, setPinnedMs] = useState<number | null>(null);
  const [zoomRange, setZoomRange] = useState<[number, number] | null>(null);
  const [metricMode, setMetricMode] = useState<MetricMode>('cpu');
  const [highlightedItem, setHighlightedItem] = useState<HighlightedItem>(null);
  const [viewMode, setViewMode] = useState<'2d' | '3d' | '3d-surface'>('2d');
  // Reset to 2D when experimental is turned off
  useEffect(() => { if (!experimentalEnabled && viewMode !== '2d') setViewMode('2d'); }, [experimentalEnabled]);
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

  // Zoom mode: per-second sampled data from processes_history/merges_history
  const [zoomData, setZoomData] = useState<MemoryTimeline | null>(null);
  const [zoomLoading, setZoomLoading] = useState(false);
  const zoomFetchRef = useRef<ReturnType<typeof setTimeout> | null>(null);

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

  // Navigator state — range derived from selected time preset
  const [selectedTimeRange, setSelectedTimeRange] = useState('1h');
  const [navigatorData, setNavigatorData] = useState<MemoryTimeline | null>(null);
  const [navigatorLoading, setNavigatorLoading] = useState(false);
  const lastNavigatorFetchTime = useRef<string | null>(null);
  const lastNavigatorMetric = useRef<MetricMode | null>(null);

  // Navigator hours derived from selected time range preset, or custom range
  const navigatorHours = useMemo(() => {
    if (selectedTimeRange === 'Custom' && customStartTime && customEndTime) {
      const spanMs = new Date(customEndTime).getTime() - new Date(customStartTime).getTime();
      return Math.max(1, spanMs / 3600000);
    }
    const range = TIME_RANGES.find(r => r.label === selectedTimeRange);
    return range?.hoursAgo ?? 1;
  }, [selectedTimeRange, customStartTime, customEndTime]);

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

  // Effective viewport end: in Custom mode use viewportEndTime (falls back to customEndTime), else customEndTime (set by navigator drag in preset mode)
  const effectiveViewportEnd = selectedTimeRange === 'Custom'
    ? (viewportEndTime ?? customEndTime)
    : customEndTime;

  const fetchData = useCallback(async () => {
    if (!services) return;
    setIsLoading(true); setError(null);
    try {
      const endDate = isLive ? new Date() : (effectiveViewportEnd ? new Date(effectiveViewportEnd) : new Date());
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
  }, [services, isLive, effectiveViewportEnd, windowSec, includeRunning, selectedHost, activityLimit, metricMode]);

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
      const endDate = isLive ? new Date() : (effectiveViewportEnd ? new Date(effectiveViewportEnd) : new Date());
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
  }, [services, clusterHosts, splitView, isLive, effectiveViewportEnd, windowSec, includeRunning, metricMode]);

  const splitFetchTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  useEffect(() => {
    if (splitView && services && isConnected && clusterHosts.length > 1) {
      if (splitFetchTimeoutRef.current) clearTimeout(splitFetchTimeoutRef.current);
      splitFetchTimeoutRef.current = setTimeout(() => fetchSplitData(), 250);
    }
    return () => { if (splitFetchTimeoutRef.current) clearTimeout(splitFetchTimeoutRef.current); };
  }, [splitView, services, isConnected, windowSec, isLive, effectiveViewportEnd, includeRunning, fetchSplitData]); // eslint-disable-line react-hooks/exhaustive-deps

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
  }, [services, isConnected, windowSec, isLive, effectiveViewportEnd, includeRunning, selectedHost, activityLimit, metricMode]); // eslint-disable-line react-hooks/exhaustive-deps

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

  // Zoom mode: fetch per-second sampled data when zoomed into a narrow window (< 10 min)
  const ZOOM_MAX_SPAN_MS = 10 * 60 * 1000; // 10 minutes
  useEffect(() => {
    if (zoomFetchRef.current) { clearTimeout(zoomFetchRef.current); zoomFetchRef.current = null; }
    if (!zoomRange || !data || !services) { setZoomData(null); return; }
    const span = zoomRange[1] - zoomRange[0];
    if (span > ZOOM_MAX_SPAN_MS) { setZoomData(null); return; }

    // Debounce to avoid firing on every scroll-zoom tick
    zoomFetchRef.current = setTimeout(async () => {
      setZoomLoading(true);
      try {
        const enriched = await services.timelineService.getZoomData(
          data, zoomRange[0], zoomRange[1], selectedHost,
        );
        setZoomData(enriched);
      } catch (e) {
        console.error('[TimeTravelPage] Zoom fetch error:', e);
        setZoomData(null);
      } finally {
        setZoomLoading(false);
      }
    }, 300);

    return () => { if (zoomFetchRef.current) clearTimeout(zoomFetchRef.current); };
  }, [zoomRange, data, services, selectedHost]); // eslint-disable-line react-hooks/exhaustive-deps

  // Clear zoom data when base data changes
  useEffect(() => { setZoomData(null); }, [data]);

  // Effective data: use zoom-enriched data when available, else base data
  const effectiveData = zoomData ?? data;

  // Fetch navigator data spanning the selected time range (navigatorHours → now)
  const fetchNavigatorData = useCallback(async (force = false) => {
    if (!services) return;
    // Navigator always ends at "now" for presets; Custom uses customStartTime/customEndTime
    const isCustom = selectedTimeRange === 'Custom';
    const endDate = isCustom && customEndTime ? new Date(customEndTime) : new Date();
    const endTimeKey = `${endDate.toISOString().slice(0, 13)}_${metricMode}_${navigatorHours}`;
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
      const navigatorWindowSec = navigatorHours * 60 * 60 / 2;
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
  }, [services, selectedTimeRange, customStartTime, customEndTime, navigatorHours, navigatorData, windowSec, selectedHost, activityLimit, metricMode]);

  // Debounced navigator fetch
  const navigatorFetchTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  useEffect(() => {
    if (services && isConnected) {
      if (navigatorFetchTimeoutRef.current) clearTimeout(navigatorFetchTimeoutRef.current);
      navigatorFetchTimeoutRef.current = setTimeout(() => fetchNavigatorData(), 300);
    }
    return () => { if (navigatorFetchTimeoutRef.current) clearTimeout(navigatorFetchTimeoutRef.current); };
  }, [services, isConnected, customStartTime, customEndTime, isLive, fetchNavigatorData]);

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
    if (selectedTimeRange === 'Custom') {
      // In Custom mode: only move the viewport, don't change the range
      setIsLive(false);
      setViewportEndTime(toLocalDatetimeStr(clampedEnd));
    } else if (clampedEnd >= now - 30000) {
      setIsLive(true); setCustomEndTime(null); setCustomStartTime(null); setViewportEndTime(null);
    } else {
      setIsLive(false); setCustomEndTime(toLocalDatetimeStr(clampedEnd));
    }
  }, [selectedTimeRange]);

  const viewportBounds = useMemo(() => {
    // During drag, use the drag position; otherwise derive from committed time
    const endMs = dragEndMs ?? (isLive ? Date.now() : (effectiveViewportEnd ? new Date(effectiveViewportEnd).getTime() : Date.now()));
    return { startMs: endMs - windowSec * 2 * 1000, endMs };
  }, [dragEndMs, isLive, effectiveViewportEnd, windowSec]);

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

  const ALL_WINDOW_SIZES = [
    { label: '1m', sec: 30 },
    { label: '5m', sec: 150 },
    { label: '15m', sec: 450 },
    { label: '30m', sec: 900 },
    { label: '1h', sec: 1800 },
    { label: '3h', sec: 5400 },
    { label: '6h', sec: 10800 },
  ];
  // Filter zoom options: displayed span (sec*2) must be ≤ selected Last range
  const maxZoomSec = navigatorHours * 3600;
  const WINDOW_SIZES = ALL_WINDOW_SIZES.filter(w => w.sec * 2 <= maxZoomSec);
  // If current windowSec exceeds the allowed max, clamp it down
  useEffect(() => {
    if (WINDOW_SIZES.length > 0 && !WINDOW_SIZES.some(w => w.sec === windowSec)) {
      setWindowSec(WINDOW_SIZES[WINDOW_SIZES.length - 1].sec);
    }
  }, [navigatorHours]); // eslint-disable-line react-hooks/exhaustive-deps
  const [showCustomPopover, setShowCustomPopover] = useState(false);
  const [sliderZoomMs, setSliderZoomMs] = useState(CUSTOM_RANGE_PRESETS[0].ms); // slider track width
  const customPopoverRef = useRef<HTMLDivElement>(null);

  // Close custom popover on click-outside or Escape
  useEffect(() => {
    if (!showCustomPopover) return;
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') setShowCustomPopover(false); };
    const onClick = (e: MouseEvent) => {
      if (customPopoverRef.current && !customPopoverRef.current.contains(e.target as Node)) setShowCustomPopover(false);
    };
    document.addEventListener('keydown', onKey);
    document.addEventListener('mousedown', onClick);
    return () => { document.removeEventListener('keydown', onKey); document.removeEventListener('mousedown', onClick); };
  }, [showCustomPopover]);

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
    // Invalidate navigator cache so the new range triggers a fresh fetch
    lastNavigatorFetchTime.current = null;
    setNavigatorData(null);
    // All presets: live mode (right edge = now), scrub bar shows last N hours
    setIsLive(true); setCustomEndTime(null); setCustomStartTime(null); setViewportEndTime(null);
    setShowCustomPopover(false);
  };

  const handleCustomToggle = () => {
    if (showCustomPopover) {
      setShowCustomPopover(false);
    } else {
      setShowCustomPopover(true);
      // Pre-fill from/to based on current preset or previous custom range
      if (!customStartTime || !customEndTime) {
        const presetRange = TIME_RANGES.find(r => r.label === selectedTimeRange);
        const rangeMs = (presetRange?.hoursAgo ?? 1) * 3600000;
        const now = new Date();
        setCustomEndTime(toLocalDatetimeStr(now));
        setCustomStartTime(toLocalDatetimeStr(new Date(now.getTime() - rangeMs)));
        // Match slider zoom to current selection
        const zoom = CUSTOM_RANGE_PRESETS.find(z => z.ms >= rangeMs) ?? CUSTOM_RANGE_PRESETS[CUSTOM_RANGE_PRESETS.length - 1];
        setSliderZoomMs(zoom.ms);
      }
    }
  };

  const handleCustomApply = () => {
    if (!customStartTime || !customEndTime) return;
    setSelectedTimeRange('Custom');
    setIsLive(false);
    // Position viewport at the end of the custom range
    setViewportEndTime(customEndTime);
    clearDragPosition();
    lastNavigatorFetchTime.current = null;
    setNavigatorData(null);
    setShowCustomPopover(false);
  };

  return (
    <div style={{ height:'100%', display:'flex', flexDirection:'column', overflow:'hidden', background:'var(--bg-primary)' }}>
      {/* Header bar */}
      <div style={{ padding:'12px 16px 10px', borderBottom:'1px solid var(--border-primary)', background:'var(--bg-secondary)', flexShrink:0 }}>
        <div style={{ display:'flex', alignItems:'center', justifyContent:'space-between', marginBottom:12 }}>
          <div style={{ display:'flex', alignItems:'center', gap:12 }}>
            <h2 style={{ color:'var(--text-primary)', fontSize:18, fontWeight:600, margin:0 }}>Time Travel</h2>
            <DocsLink path="/features/time-travel" />
            <span style={{ color:'var(--text-muted)', fontSize:12 }}>Hover to inspect · Click to pin · Drag to zoom</span>
          </div>

          {/* Right side: Time range picker */}
          <div style={{ display:'flex', alignItems:'center', gap:8 }}>
            {/* Time range tab buttons */}
            <div style={{
              display:'flex', gap:2, padding:3, borderRadius:8,
              background:'var(--bg-tertiary)', border:'1px solid var(--border-primary)',
              position:'relative',
            }}>
              <span style={{ padding:'5px 8px', fontSize:10, color:'var(--text-muted)', fontWeight:600, alignSelf:'center' }}>Last</span>
              {TIME_RANGES.map(r => (
                <button key={r.label} onClick={() => handleTimeRangeChange(r.label)}
                  style={{
                    padding:'5px 10px', fontSize:11, fontWeight:600, border:'none', borderRadius:5, cursor:'pointer',
                    fontFamily:"'Share Tech Mono',monospace",
                    background: selectedTimeRange === r.label ? 'var(--bg-primary)' : 'transparent',
                    color: selectedTimeRange === r.label ? 'var(--text-primary)' : 'var(--text-muted)',
                    boxShadow: selectedTimeRange === r.label ? '0 1px 3px rgba(0,0,0,0.1)' : 'none',
                    transition:'all 0.15s ease',
                  }}>
                  {r.label}
                </button>
              ))}
              <button onClick={handleCustomToggle}
                style={{
                  padding:'5px 10px', fontSize:11, fontWeight:600, border:'none', borderRadius:5, cursor:'pointer',
                  fontFamily:"'Share Tech Mono',monospace",
                  background: selectedTimeRange === 'Custom' || showCustomPopover ? 'var(--bg-primary)' : 'transparent',
                  color: selectedTimeRange === 'Custom' || showCustomPopover ? 'var(--text-primary)' : 'var(--text-muted)',
                  boxShadow: selectedTimeRange === 'Custom' || showCustomPopover ? '0 1px 3px rgba(0,0,0,0.1)' : 'none',
                  transition:'all 0.15s ease',
                }}>
                Custom
              </button>
              {/* Active custom range label (inline inside tab bar) */}
              {selectedTimeRange === 'Custom' && !showCustomPopover && customStartTime && customEndTime && (() => {
                const fmt = (iso: string) => {
                  const d = new Date(iso);
                  const pad = (n: number) => String(n).padStart(2, '0');
                  return `${pad(d.getMonth() + 1)}/${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
                };
                return (
                  <span style={{
                    fontSize:9, color:'var(--text-muted)', fontFamily:"'Share Tech Mono',monospace",
                    whiteSpace:'nowrap', padding:'0 4px', alignSelf:'center',
                  }}>
                    {fmt(customStartTime)} — {fmt(customEndTime)}
                  </span>
                );
              })()}
              {/* Custom range popover — drops below the tab bar */}
              {showCustomPopover && (
                <div ref={customPopoverRef} style={{
                  position:'absolute', top:'calc(100% + 6px)', left:0, zIndex:100,
                  background:'var(--bg-secondary)', border:'1px solid var(--border-primary)',
                  borderRadius:8, padding:'10px 12px', boxShadow:'0 4px 16px rgba(0,0,0,0.25)',
                  display:'flex', flexDirection:'column', gap:8, width:480,
                }}>
                  {/* From / To / Apply row */}
                  <div style={{ display:'flex', alignItems:'center', gap:6 }}>
                    <label style={{ fontSize:9, fontWeight:600, color:'var(--text-muted)' }}>From</label>
                    <input type="datetime-local" value={customStartTime || ''} onChange={e => setCustomStartTime(e.target.value)}
                      style={{ flex:1, padding:'4px 8px', fontSize:11, borderRadius:4, border:'1px solid var(--border-primary)', background:'var(--bg-card)', color:'var(--text-primary)', fontFamily:"'Share Tech Mono',monospace" }} />
                    <label style={{ fontSize:9, fontWeight:600, color:'var(--text-muted)' }}>To</label>
                    <input type="datetime-local" value={customEndTime || ''} onChange={e => setCustomEndTime(e.target.value)}
                      style={{ flex:1, padding:'4px 8px', fontSize:11, borderRadius:4, border:'1px solid var(--border-primary)', background:'var(--bg-card)', color:'var(--text-primary)', fontFamily:"'Share Tech Mono',monospace" }} />
                    <button onClick={handleCustomApply} disabled={!customStartTime || !customEndTime}
                      style={{
                        padding:'4px 12px', fontSize:10, fontWeight:600, borderRadius:5, border:'none',
                        cursor: customStartTime && customEndTime ? 'pointer' : 'not-allowed',
                        background: customStartTime && customEndTime ? 'rgba(99,102,241,0.85)' : 'transparent',
                        color: customStartTime && customEndTime ? '#fff' : 'var(--text-muted)',
                        transition:'all 0.15s ease', whiteSpace:'nowrap',
                      }}>
                      Apply
                    </button>
                  </div>
                  {/* Slider zoom presets — controls track width, not selection */}
                  <div style={{ display:'flex', gap:2, padding:2, borderRadius:6, background:'var(--bg-card)', alignSelf:'center' }}>
                    {CUSTOM_RANGE_PRESETS.map(p => (
                      <button key={p.label} onClick={() => setSliderZoomMs(p.ms)}
                        style={{
                          padding:'3px 10px', fontSize:10, fontWeight:600, border:'none', borderRadius:5, cursor:'pointer',
                          fontFamily:"'Share Tech Mono',monospace",
                          background: sliderZoomMs === p.ms ? 'var(--bg-primary)' : 'transparent',
                          color: sliderZoomMs === p.ms ? 'var(--text-primary)' : 'var(--text-muted)',
                          boxShadow: sliderZoomMs === p.ms ? '0 1px 3px rgba(0,0,0,0.1)' : 'none',
                          transition:'all 0.15s ease',
                        }}>
                        {p.label}
                      </button>
                    ))}
                  </div>
                  {/* Scrubber slider — track spans sliderZoomMs ending at now, handles set From/To */}
                  <RangeSlider
                    rangeStartMs={Date.now() - sliderZoomMs} rangeEndMs={Date.now()}
                    start={customStartTime || ''} end={customEndTime || ''}
                    onStartChange={setCustomStartTime} onEndChange={setCustomEndTime}
                  />
                </div>
              )}
            </div>

            {/* Window size selector */}
            <div style={{ display:'flex', alignItems:'center', gap:0, background:'var(--bg-tertiary)', borderRadius:6, border:'1px solid var(--border-primary)', overflow:'hidden' }}>
              <span style={{ padding:'5px 8px', fontSize:10, color:'var(--text-muted)', borderRight:'1px solid var(--border-primary)', whiteSpace:'nowrap', fontWeight:600 }}>Zoom</span>
              <select value={windowSec} onChange={(e) => setWindowSec(Number(e.target.value))} title="Select time window duration"
                style={{ background:'transparent', color:'var(--text-primary)', border:'none', padding:'5px 10px', fontSize:11, outline:'none', cursor:'pointer', minWidth:60 }}>
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
              <button onClick={() => { clearDragPosition(); setIsLive(true); setCustomEndTime(null); setCustomStartTime(null); setViewportEndTime(null); setSelectedTimeRange('1h'); }} title="Jump to current time"
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
              <span style={{ position: 'relative', display: 'inline-flex' }}>
                <button className={`tab ${splitView ? 'active' : ''}`}
                  onClick={() => { if (viewMode === '2d') { setSelectedHost(null); setSplitView(!splitView); } }}
                  title={viewMode === '2d' ? 'Split view: one chart per server, stacked vertically' : undefined}
                  style={{ display:'flex', alignItems:'center', gap:4, ...(viewMode !== '2d' ? { opacity: 0.35, cursor: 'not-allowed' } : {}) }}>
                  <svg width="10" height="10" viewBox="0 0 10 10" style={{ opacity: 0.7 }}>
                    <rect x="0" y="0" width="10" height="4" rx="1" fill="currentColor" />
                    <rect x="0" y="6" width="10" height="4" rx="1" fill="currentColor" />
                  </svg> Split
                </button>
                {viewMode !== '2d' && (
                  <span className="split-3d-tooltip" style={{
                    position: 'absolute', bottom: '100%', left: '50%', transform: 'translateX(-50%)',
                    marginBottom: 6, padding: '6px 10px', borderRadius: 6, fontSize: 11, lineHeight: '1.4',
                    whiteSpace: 'nowrap', pointerEvents: 'none', opacity: 0, transition: 'opacity 0.15s ease',
                    background: 'var(--bg-tertiary)', color: 'var(--text-secondary)',
                    border: '1px solid var(--border-secondary)', boxShadow: '0 4px 12px rgba(0,0,0,0.3)',
                    zIndex: 50,
                  }}>
                    Split is not available in experimental 3D modes
                  </span>
                )}
              </span>
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
            {experimentalEnabled && (
              <>
                <div style={{ width: 1, height: 20, background: 'var(--border-primary)', margin: '0 4px' }} />
                {([['2d', '2D'], ['3d', '3D'], ['3d-surface', '3D Surface']] as const).map(([mode, label]) => (
                  <button key={mode} onClick={() => { setViewMode(mode); if (mode !== '2d') setSplitView(false); }}
                    style={{
                      position: 'relative',
                      background: viewMode === mode ? 'var(--bg-hover)' : 'transparent',
                      color: viewMode === mode ? 'var(--text-primary)' : 'var(--text-muted)',
                      border: viewMode === mode ? '1px solid var(--border-primary)' : '1px solid transparent',
                      borderRadius: 6, padding: '6px 10px', fontSize: 12, fontWeight: viewMode === mode ? 600 : 400,
                      cursor: 'pointer', transition: 'all 0.15s ease',
                    }}>
                    {label}
                    {mode !== '2d' && (
                      <span
                        className="exp-badge"
                        style={{
                          position: 'absolute', top: -6, right: -4,
                          fontSize: 7, fontWeight: 700, color: '#f0883e',
                          background: 'var(--bg-tertiary)', border: '1px solid rgba(240,136,62,0.3)',
                          borderRadius: 3, padding: '0 3px', lineHeight: '12px',
                          textTransform: 'uppercase', letterSpacing: '0.3px',
                          cursor: 'default',
                        }}
                      >exp</span>
                    )}
                  </button>
                ))}
              </>
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
            {viewMode === '3d' ? (
              <TimelineChart3D data={data} metricMode={metricMode} height={500} hiddenCategories={hiddenCategories}
                onHighlightItem={setHighlightedItem}
                onBandClick={(band) => {
                  if (band.type === 'query' && data.queries[band.idx]) setSelectedTimelineQuery(data.queries[band.idx]);
                  else if (band.type === 'merge' && data.merges[band.idx]) setSelectedTimelineMerge(data.merges[band.idx]);
                  else if (band.type === 'mutation' && (data.mutations ?? [])[band.idx]) setSelectedTimelineMutation((data.mutations ?? [])[band.idx]);
                }} />
            ) : viewMode === '3d-surface' ? (
              <TimelineChart3DSurface data={data} metricMode={metricMode} height={500} hiddenCategories={hiddenCategories}
                onHighlightItem={setHighlightedItem}
                onBandClick={(band) => {
                  if (band.type === 'query' && data.queries[band.idx]) setSelectedTimelineQuery(data.queries[band.idx]);
                  else if (band.type === 'merge' && data.merges[band.idx]) setSelectedTimelineMerge(data.merges[band.idx]);
                  else if (band.type === 'mutation' && (data.mutations ?? [])[band.idx]) setSelectedTimelineMutation((data.mutations ?? [])[band.idx]);
                }} />
            ) : (
            <>
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
                {zoomData && (
                  <span style={{ fontSize:10, color:'#3fb950', opacity: 0.8 }}>
                    {zoomLoading ? 'Loading samples...' : 'Per-second sampled'}
                  </span>
                )}
                {zoomLoading && !zoomData && (
                  <span style={{ fontSize:10, color:'#58a6ff', opacity: 0.8 }}>Loading samples...</span>
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
            <TimelineChart data={effectiveData!} metricMode={metricMode} height={500}
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
            </>
            )}
          </div>
          )}

          {/* Timeline Navigator */}
          {navigatorRange && (
            <div style={{ marginTop: 12 }}>
              <div style={{ display:'flex', alignItems:'center', gap:8, marginBottom:6, fontSize:11, color:'var(--text-muted)' }}>
                <span style={{ textTransform:'uppercase', letterSpacing:'0.5px' }}>{navigatorHours >= 48 ? `${Math.round(navigatorHours / 24)}d` : navigatorHours >= 24 ? '1d' : `${Math.round(navigatorHours)}h`} Overview</span>
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
