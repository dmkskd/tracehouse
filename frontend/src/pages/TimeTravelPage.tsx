/**
 * TimeTravelPage - Memory/CPU/Network/Disk IO timeline with interactive hover + click + drag-to-zoom.
 * Toggle buttons switch Y-axis metric. Same time axis, hover, pin, zoom across all views.
 */
import React, { useState, useCallback, useMemo, useRef, useEffect } from 'react';
import { useNavigate, useSearchParams } from '../hooks/useAppLocation';
import { useConnectionStore } from '../stores/connectionStore';
import { useClickHouseServices } from '../providers/ClickHouseProvider';
import { useClusterStore } from '../stores/clusterStore';
import { useRefreshConfig, clampToAllowed } from '@tracehouse/ui-shared';
import { useRefreshSettingsStore } from '../stores/refreshSettingsStore';
import { useGlobalLastUpdatedStore } from '../stores/refreshSettingsStore';
import { useCapabilityCheck } from '../components/shared/RequiresCapability';
import type {
  EventsResult,
  MemoryTimeline,
  QuerySeries,
  MergeSeries,
  MutationSeries,
  OperationalEvent,
  TimeseriesPoint,
} from '@tracehouse/core';
import {
  getTimelineCpuCapacity,
  getTimelineRamCapacity,
  TIMELINE_ACTIVITY_LIMIT,
} from '@tracehouse/core';
import { TimelineNavigator } from '../components/shared/TimelineNavigator';
import { RangeSlider } from '../components/shared/RangeSlider';
import { QueryDetailModal } from '../components/query/modal/QueryDetailModal';
import { MergeDetailModal, MutationDetailModal } from '../components/merge/MergeDetailModal';
import { useQueryDeepLink } from '../hooks/useQueryDeepLink';
import { TruncatedHost } from '../components/common/TruncatedHost';
import { formatBytes, parseTimestamp } from '../utils/formatters';
import { getUrlParam } from '../utils/urlParams';
import { useUserPreferenceStore } from '../stores/userPreferenceStore';
import { useMonitoringCapabilitiesStore } from '../stores/monitoringCapabilitiesStore';
import { DocsLink } from '../components/common/DocsLink';
import {
  MetricStrip,
  MetricStripDivider,
  MetricStripItem,
} from '../components/common/MetricStrip';
import { TimelineChart } from '../components/timeline/TimelineChart';
import { TimelineChart3D } from '../components/timeline/TimelineChart3D';
import { TimelineChart3DSurface } from '../components/timeline/TimelineChart3DSurface';
import { QueryTable, MergeTable } from '../components/timeline/TimelineTable';
import { TimelineEventControls } from '../components/timeline/TimelineEventControls';
import {
  emptyTimelineEventFilter,
  buildTimelineNavigatorRequestScope,
  buildEventsUrl,
  filterTimelineEvents,
  type TimelineEventFilter,
} from '../components/timeline/timeline-event-model';
import {
  type MetricMode, type HighlightedItem,
  Q_COLORS, M_COLORS, MUT_COLORS, METRIC_CONFIG, getMetricValue,
} from '../components/timeline/timeline-constants';
import {
  createTimeTravelRequestGate,
  timeTravelHostnameFilter,
  timeTravelRowHosts,
  updateTimeTravelHostSelection,
} from '../components/timeline/time-travel-host-selection';
import {
  isRangeCovered,
  mergeCoverage,
  mergeTimelineEvents,
  mergeTimelinePoints,
  navigatorBucketSeconds,
  navigatorChangePoints,
  navigatorCacheBounds,
  navigatorChunkMs,
  panRangeToIncludeViewport,
  uncoveredTimelineRanges,
  type TimelineCoverage,
  type TimelineRange,
} from '../components/timeline/timeline-navigator-buffer';

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

/**
 * Standing reminder of Time Travel's two sampling limits, shown as a tooltip on
 * the activity counts and the breakdown link. Time Travel is a fast first
 * glance; the Analytics "Workload Breakdown" dashboard is the precise view.
 */
const SAMPLING_NOTE = (metric: MetricMode): string =>
  `Time Travel is a sampled first glance: it shows the top ${TIMELINE_ACTIVITY_LIMIT} `
  + `queries / merges / mutations by ${metric.toUpperCase()}, and operations under `
  + `1 MB of memory are not collected (excluded from these totals too).\n\n`
  + `For a complete, precise accounting of CPU / memory / disk split across `
  + `queries, merges, and mutations, open Analytics → Workload Breakdown.`;

/** Read a param from the hash-based URL (/#/path?key=val) or standard search */

export const TimeTravelPage: React.FC = () => {
  const { activeProfileId, profiles } = useConnectionStore();
  const services = useClickHouseServices();
  const { detected: clusterDetected } = useClusterStore();
  const [searchParams, setSearchParams] = useSearchParams();
  const navigate = useNavigate();
  const eventTimeParam = searchParams.get('event_time');
  const initialEventMs = eventTimeParam ? Date.parse(eventTimeParam) : Number.NaN;
  const hasInitialEvent = Number.isFinite(initialEventMs);

  // Query hash filter: highlight/filter timeline to a specific normalized_query_hash (from URL ?nqh=...)
  const [queryHashFilter, setQueryHashFilter] = useState<string | null>(() => getUrlParam('nqh'));
  // When true, show only hash-matched queries (hide everything else including merges/mutations)
  const [queryHashOnly, setQueryHashOnly] = useState(false);

  // Sync queryHashFilter from URL search params (e.g. when navigating from another page)
  useEffect(() => {
    const nqh = searchParams.get('nqh');
    if (nqh !== queryHashFilter) setQueryHashFilter(nqh);
  }, [searchParams]); // eslint-disable-line react-hooks/exhaustive-deps
  const refreshConfig = useRefreshConfig();
  const { refreshRateSeconds } = useRefreshSettingsStore();
  const manualRefreshTick = useGlobalLastUpdatedStore(s => s.manualRefreshTick);
  const { available: hasMetricLog, missing: missingCaps, probing: isCapProbing } = useCapabilityCheck(['metric_log', 'query_log']);
  const monitoringCapabilities = useMonitoringCapabilitiesStore(s => s.capabilities);
  const eventCapabilities = useMemo(
    () => monitoringCapabilities?.capabilities
      .filter(capability => capability.available)
      .map(capability => capability.id),
    [monitoringCapabilities],
  );
  const {
    experimentalEnabled,
    timeTravelEventsVisible,
    setTimeTravelEventsVisible,
    timeTravelNavigatorShape,
    setTimeTravelNavigatorShape,
  } = useUserPreferenceStore();
  const [windowSec, setWindowSec] = useState(150);
  const [isLive, setIsLive] = useState(!hasInitialEvent);
  const [navigatorInteractionEpoch, setNavigatorInteractionEpoch] = useState(0);
  const [autoRefresh, setAutoRefresh] = useState(false);
  const [customStartTime, setCustomStartTime] = useState<string | null>(null);  // Custom range start (navigator)
  const [customEndTime, setCustomEndTime] = useState<string | null>(
    hasInitialEvent ? toLocalDatetimeStr(initialEventMs + 150_000) : null,
  );      // Custom range end (navigator)
  const [viewportEndTime, setViewportEndTime] = useState<string | null>(null);  // Viewport position within custom range
  const [data, setData] = useState<MemoryTimeline | null>(null);
  const fetchDataRequestGateRef = useRef(createTimeTravelRequestGate());
  const [eventData, setEventData] = useState<EventsResult>({
    events: [],
    coverage: [],
  });
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hoverMs, setHoverMs] = useState<number | null>(null);
  const [pinnedMs, setPinnedMs] = useState<number | null>(
    hasInitialEvent ? initialEventMs : null,
  );
  const [selectedEventId, setSelectedEventId] = useState<string | null>(
    searchParams.get('event_id'),
  );
  const [eventFilter, setEventFilter] = useState<TimelineEventFilter>(
    emptyTimelineEventFilter,
  );
  const pendingEventPinRef = useRef<number | null>(null);
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
  const [selectedHosts, setSelectedHosts] = useState<string[]>([]);
  const hostnameFilter = useMemo(
    () => timeTravelHostnameFilter(selectedHosts),
    [selectedHosts],
  );

  // Per-server view: show one chart per selected host stacked vertically.
  const [perServerView, setPerServerView] = useState(false);
  const [perHostData, setPerHostData] = useState<Map<string, MemoryTimeline>>(new Map());
  const [splitLoading, setSplitLoading] = useState(false);
  const rowHosts = useMemo(
    () => timeTravelRowHosts(clusterHosts, selectedHosts, perServerView),
    [clusterHosts, selectedHosts, perServerView],
  );
  const handleHostClick = useCallback((
    event: React.MouseEvent<HTMLButtonElement>,
    host: string,
  ) => {
    const additive = event.shiftKey || event.ctrlKey || event.metaKey;
    const nextSelection = updateTimeTravelHostSelection(selectedHosts, host, additive);
    setSelectedHosts(nextSelection);
    if (additive && nextSelection.length > 1) {
      setPerServerView(true);
      setViewMode('2d');
    }
  }, [selectedHosts]);

  // Modal state
  const [selectedTimelineQuery, setSelectedTimelineQuery] = useState<QuerySeries | null>(null);
  const [selectedTimelineMerge, setSelectedTimelineMerge] = useState<MergeSeries | null>(null);
  const [selectedTimelineMutation, setSelectedTimelineMutation] = useState<MutationSeries | null>(null);

  // Deep-link: sync query detail modal to URL (qd_id param)
  const { query: deepLinkedQuery, onClose: handleQueryClose } = useQueryDeepLink(
    selectedTimelineQuery,
    () => setSelectedTimelineQuery(null),
  );

  // Navigator state — range derived from selected time preset
  const [selectedTimeRange, setSelectedTimeRange] = useState('1h');
  const [navigatorMetricData, setNavigatorMetricData] = useState<{
    trend: TimeseriesPoint[];
    peaks: TimeseriesPoint[];
  }>({ trend: [], peaks: [] });
  const [navigatorEventData, setNavigatorEventData] = useState<EventsResult>({
    events: [],
    coverage: [],
  });
  const [navigatorRange, setNavigatorRange] = useState<TimelineRange | null>(null);
  const navigatorRangeRef = useRef<TimelineRange | null>(null);
  const navigatorCoverageRef = useRef<TimelineCoverage[]>([]);
  const navigatorInFlightRef = useRef<Map<string, TimelineRange>>(new Map());
  const navigatorGenerationRef = useRef(0);
  const [navigatorLoadingCount, setNavigatorLoadingCount] = useState(0);
  const navigatorLoading = navigatorLoadingCount > 0;

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
    const requestId = fetchDataRequestGateRef.current.begin();
    setIsLoading(true); setError(null);
    try {
      const endDate = isLive ? new Date() : (effectiveViewportEnd ? new Date(effectiveViewportEnd) : new Date());
      const centerDate = new Date(endDate.getTime() - windowSec * 1000);
      const startDate = new Date(centerDate.getTime() - windowSec * 1000);
      const endDateForEvents = new Date(centerDate.getTime() + windowSec * 1000);
      const [result, eventsResult] = await Promise.all([
        services.timelineService.getTimeline({
          timestamp: centerDate,
          windowSeconds: windowSec,
          includeRunning,
          hostname: hostnameFilter,
          activityLimit,
          activeMetric: metricMode,
          normalizedQueryHash: queryHashFilter ?? undefined,
        }),
        eventCapabilities
          ? services.eventsService.getEvents({
              startTime: startDate.toISOString(),
              endTime: endDateForEvents.toISOString(),
              hostname: hostnameFilter,
              availableCapabilities: eventCapabilities,
              origin: 'timeTravel',
            })
          : Promise.resolve({ events: [], coverage: [] }),
      ]);
      // A host/time/metric change may have started a newer request while this
      // one was in flight. Never let the older selection overwrite it.
      if (!fetchDataRequestGateRef.current.isLatest(requestId)) return;
      // In live mode, slide zoom/pin forward to follow the advancing time window
      const newEndMs = new Date(result.window_end).getTime();
      if (isLive && prevDataEndRef.current != null) {
        const delta = newEndMs - prevDataEndRef.current;
        if (delta > 0) {
          setZoomRange(prev => prev ? [prev[0] + delta, prev[1] + delta] : null);
          if (!selectedEventId) {
            setPinnedMs(prev => prev != null ? prev + delta : null);
          }
        }
      }
      prevDataEndRef.current = newEndMs;
      setData(result);
      setEventData(eventsResult);
      const pendingEventPin = pendingEventPinRef.current;
      if (
        pendingEventPin != null
        && pendingEventPin >= new Date(result.window_start).getTime()
        && pendingEventPin <= newEndMs
      ) {
        setPinnedMs(pendingEventPin);
        pendingEventPinRef.current = null;
      }
      useGlobalLastUpdatedStore.getState().touch();
    } catch (e) {
      if (!fetchDataRequestGateRef.current.isLatest(requestId)) return;
      const msg = e instanceof Error ? e.message : 'Failed to fetch timeline';
      console.error('[TimeTravelPage] Error:', msg, e);
      setError(msg);
    }
    finally {
      if (fetchDataRequestGateRef.current.isLatest(requestId)) setIsLoading(false);
    }
  }, [services, isLive, effectiveViewportEnd, windowSec, includeRunning, hostnameFilter, activityLimit, metricMode, queryHashFilter, eventCapabilities, selectedEventId]);

  // Fetch cluster hosts on connect (after cluster detection completes)
  useEffect(() => {
    if (!services || !isConnected || !clusterDetected) { setClusterHosts([]); return; }
    services.metricsCollector.getClusterHosts().then(hosts => {
      setClusterHosts(hosts);
      setSelectedHosts(current => current.filter(host => hosts.includes(host)));
    });
  }, [services, isConnected, clusterDetected]);

  // Fetch per-host data when the display is set to Per server.
  const fetchSplitData = useCallback(async () => {
    if (!services || rowHosts.length === 0) return;
    setSplitLoading(true);
    try {
      const endDate = isLive ? new Date() : (effectiveViewportEnd ? new Date(effectiveViewportEnd) : new Date());
      const centerDate = new Date(endDate.getTime() - windowSec * 1000);
      const results = await Promise.all(
        rowHosts.map(async (host) => {
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
      console.error('[TimeTravelPage] Per-server view fetch error:', e);
    } finally { setSplitLoading(false); }
  }, [services, rowHosts, isLive, effectiveViewportEnd, windowSec, includeRunning, metricMode, activityLimit]);

  const splitFetchTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  useEffect(() => {
    if (rowHosts.length > 0 && services && isConnected) {
      if (splitFetchTimeoutRef.current) clearTimeout(splitFetchTimeoutRef.current);
      splitFetchTimeoutRef.current = setTimeout(() => fetchSplitData(), 250);
    }
    return () => { if (splitFetchTimeoutRef.current) clearTimeout(splitFetchTimeoutRef.current); };
  }, [rowHosts, services, isConnected, windowSec, isLive, effectiveViewportEnd, includeRunning, fetchSplitData]);

  // Auto-analyze when params change (debounced)
  const fetchDataTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  useEffect(() => {
    // Invalidate an in-flight response immediately when its request scope
    // changes, including while the previous request is still loading.
    fetchDataRequestGateRef.current.invalidate();
    if (services && isConnected) {
      // Clear zoom/pin when user changes time parameters
      setZoomRange(null);
      setPinnedMs(null);
      if (pendingEventPinRef.current == null) setSelectedEventId(null);
      prevDataEndRef.current = null;
      if (fetchDataTimeoutRef.current) clearTimeout(fetchDataTimeoutRef.current);
      fetchDataTimeoutRef.current = setTimeout(() => fetchData(), 200);
    }
    return () => { if (fetchDataTimeoutRef.current) clearTimeout(fetchDataTimeoutRef.current); };
  }, [services, isConnected, clusterDetected, windowSec, isLive, effectiveViewportEnd, includeRunning, hostnameFilter, activityLimit, metricMode, queryHashFilter, eventCapabilities]); // eslint-disable-line react-hooks/exhaustive-deps

  // Clear hash filter: remove nqh from URL and re-fetch normally
  const clearQueryHashFilter = useCallback(() => {
    setQueryHashFilter(null);
    setSearchParams(prev => {
      const next = new URLSearchParams(prev);
      next.delete('nqh');
      return next;
    }, { replace: true });
  }, [setSearchParams]);

  // Set hash filter mode: write nqh to URL and trigger filtered fetch
  const activateQueryHashFilter = useCallback((hash: string) => {
    setQueryHashFilter(hash);
    setSearchParams(prev => {
      const next = new URLSearchParams(prev);
      next.set('nqh', hash);
      return next;
    }, { replace: true });
  }, [setSearchParams]);

  // Manual refresh from header button
  useEffect(() => {
    if (manualRefreshTick > 0 && services && isConnected) {
      fetchData();
      if (rowHosts.length > 0) fetchSplitData();
    }
  }, [manualRefreshTick]); // eslint-disable-line react-hooks/exhaustive-deps

  // Auto-refresh timer
  useEffect(() => {
    if (autoRefreshRef.current) { clearInterval(autoRefreshRef.current); autoRefreshRef.current = null; }
    if (autoRefresh && refreshRateSeconds > 0 && isLive && services && isConnected) {
      const intervalMs = clampToAllowed(refreshRateSeconds, refreshConfig) * 1000;
      autoRefreshRef.current = setInterval(() => { fetchData(); if (rowHosts.length > 0) fetchSplitData(); }, intervalMs);
    }
    return () => { if (autoRefreshRef.current) { clearInterval(autoRefreshRef.current); autoRefreshRef.current = null; } };
  }, [autoRefresh, refreshRateSeconds, refreshConfig, isLive, services, isConnected, fetchData, rowHosts, fetchSplitData]);

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
          data, zoomRange[0], zoomRange[1], hostnameFilter,
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
  }, [zoomRange, data, services, hostnameFilter]); // eslint-disable-line react-hooks/exhaustive-deps

  // Clear zoom data when base data changes
  useEffect(() => { setZoomData(null); }, [data]);

  // Effective data: use zoom-enriched data when available, else base data
  const effectiveData = zoomData ?? data;

  // Chart data: in queryHashOnly mode, show only hash-matched queries and hide merges/mutations
  const chartData = useMemo(() => {
    if (!effectiveData || !queryHashOnly || !queryHashFilter) return effectiveData;
    return {
      ...effectiveData,
      queries: effectiveData.queries.filter(q => q.matched_hash),
      merges: [],
      mutations: [],
    };
  }, [effectiveData, queryHashOnly, queryHashFilter]);

  const navigatorSpanMs = Math.max(60_000, navigatorHours * 60 * 60_000);
  const navigatorBucketSec = navigatorBucketSeconds(navigatorSpanMs);
  const navigatorChangeData = useMemo(
    () => navigatorChangePoints(navigatorMetricData.trend, navigatorBucketSec * 1000),
    [navigatorMetricData.trend, navigatorBucketSec],
  );
  const navigatorPrefetchChunkMs = navigatorChunkMs(navigatorSpanMs);
  const navigatorRequestScope = useMemo(
    () => buildTimelineNavigatorRequestScope({
      activeMetric: metricMode,
      navigatorHours,
      hostname: hostnameFilter,
      activityLimit,
      eventCapabilities,
    }),
    [metricMode, navigatorHours, hostnameFilter, activityLimit, eventCapabilities],
  );
  const navigatorCustomStart = selectedTimeRange === 'Custom' ? customStartTime : null;
  const navigatorCustomEnd = selectedTimeRange === 'Custom' ? customEndTime : null;

  const loadNavigatorInterval = useCallback(async (requested: TimelineRange) => {
    if (!services || requested.endMs <= requested.startMs) return;
    if (isRangeCovered(navigatorCoverageRef.current, requested)) return;
    if ([...navigatorInFlightRef.current.values()].some(range =>
      range.startMs <= requested.startMs && range.endMs >= requested.endMs
    )) return;

    const generation = navigatorGenerationRef.current;
    const requestKey = `${generation}:${requested.startMs}:${requested.endMs}`;
    navigatorInFlightRef.current.set(requestKey, requested);
    setNavigatorLoadingCount(navigatorInFlightRef.current.size);

    try {
      const [metricResult, eventsResult] = await Promise.all([
        services.timelineService.getNavigatorMetric({
          startTime: new Date(requested.startMs),
          endTime: new Date(requested.endMs),
          metric: metricMode,
          bucketSeconds: navigatorBucketSec,
          hostname: hostnameFilter,
        }),
        eventCapabilities
          ? services.eventsService.getEvents({
              startTime: new Date(requested.startMs).toISOString(),
              endTime: new Date(requested.endMs).toISOString(),
              hostname: hostnameFilter,
              availableCapabilities: eventCapabilities,
              origin: 'timeTravel',
            })
          : Promise.resolve({ events: [], coverage: [] }),
      ]);
      if (generation !== navigatorGenerationRef.current) return;

      const currentRange = navigatorRangeRef.current ?? requested;
      const cacheBounds = navigatorCacheBounds(currentRange);
      setNavigatorMetricData(current => ({
        trend: mergeTimelinePoints(
          current.trend,
          metricResult.points.map(point => ({ t: point.t, v: point.average_v })),
          cacheBounds,
        ),
        peaks: mergeTimelinePoints(
          current.peaks,
          metricResult.points.map(point => ({ t: point.t, v: point.peak_v })),
          cacheBounds,
        ),
      }));
      setNavigatorEventData(current => ({
        events: mergeTimelineEvents(current.events, eventsResult.events, cacheBounds),
        coverage: eventsResult.coverage,
      }));
      navigatorCoverageRef.current = mergeCoverage(
        navigatorCoverageRef.current,
        requested,
        cacheBounds,
      );
    } catch (error) {
      if (generation === navigatorGenerationRef.current) {
        console.error('[TimeTravelPage] Navigator chunk fetch error:', error);
      }
    } finally {
      if (generation === navigatorGenerationRef.current) {
        navigatorInFlightRef.current.delete(requestKey);
        setNavigatorLoadingCount(navigatorInFlightRef.current.size);
      }
    }
  }, [
    services,
    metricMode,
    navigatorBucketSec,
    hostnameFilter,
    eventCapabilities,
  ]);

  const requestNavigatorChunkAt = useCallback((ms: number) => {
    const startMs = Math.floor(ms / navigatorPrefetchChunkMs) * navigatorPrefetchChunkMs;
    const endMs = Math.min(startMs + navigatorPrefetchChunkMs, Date.now());
    if (endMs <= startMs) return;
    const available = [
      ...navigatorCoverageRef.current,
      ...navigatorInFlightRef.current.values(),
    ];
    const bucketMs = navigatorBucketSec * 1000;
    for (const gap of uncoveredTimelineRanges(available, { startMs, endMs })) {
      // Re-read the boundary bucket so a live partial aggregate is replaced
      // with the complete bucket once its remaining samples arrive.
      void loadNavigatorInterval({
        startMs: Math.max(startMs, Math.floor(gap.startMs / bucketMs) * bucketMs),
        endMs: gap.endMs,
      });
    }
  }, [
    loadNavigatorInterval,
    navigatorBucketSec,
    navigatorPrefetchChunkMs,
  ]);

  // Reset the buffer only when its data scope or configured overview changes.
  // Moving the five-minute viewport inside a preset does not invalidate it.
  useEffect(() => {
    if (!services || !isConnected) return;
    navigatorGenerationRef.current += 1;
    navigatorCoverageRef.current = [];
    navigatorInFlightRef.current.clear();
    navigatorRangeRef.current = null;
    setNavigatorLoadingCount(0);
    setNavigatorMetricData({ trend: [], peaks: [] });
    setNavigatorEventData({ events: [], coverage: [] });
    setNavigatorRange(null);

    const now = Date.now();
    const customStartMs = navigatorCustomStart
      ? new Date(navigatorCustomStart).getTime()
      : Number.NaN;
    const customEndMs = navigatorCustomEnd
      ? new Date(navigatorCustomEnd).getTime()
      : Number.NaN;
    const endMs = Number.isFinite(customEndMs) ? customEndMs : now;
    const startMs = Number.isFinite(customStartMs)
      ? customStartMs
      : endMs - navigatorSpanMs;
    if (endMs <= startMs) return;

    const range = { startMs, endMs };
    navigatorRangeRef.current = range;
    setNavigatorRange(range);
    void loadNavigatorInterval(range);
    requestNavigatorChunkAt(startMs - 1);
  }, [
    services,
    isConnected,
    selectedTimeRange,
    navigatorCustomStart,
    navigatorCustomEnd,
    navigatorRequestScope,
    navigatorSpanMs,
    loadNavigatorInterval,
    requestNavigatorChunkAt,
  ]);

  const refreshLiveNavigator = useCallback(() => {
    const currentRange = navigatorRangeRef.current;
    if (!currentRange) return;
    const endMs = Date.now();
    const spanMs = currentRange.endMs - currentRange.startMs;
    const nextRange = { startMs: endMs - spanMs, endMs };
    navigatorRangeRef.current = nextRange;
    setNavigatorRange(nextRange);
    requestNavigatorChunkAt(endMs - 1);
  }, [requestNavigatorChunkAt]);

  // Auto-refresh the right edge in live mode.
  const navigatorRefreshRef = useRef<ReturnType<typeof setInterval> | null>(null);
  useEffect(() => {
    if (navigatorRefreshRef.current) { clearInterval(navigatorRefreshRef.current); navigatorRefreshRef.current = null; }
    if (autoRefresh && refreshRateSeconds > 0 && isLive && services && isConnected) {
      const intervalMs = clampToAllowed(refreshRateSeconds, refreshConfig) * 1000;
      navigatorRefreshRef.current = setInterval(refreshLiveNavigator, intervalMs);
    }
    return () => { if (navigatorRefreshRef.current) { clearInterval(navigatorRefreshRef.current); navigatorRefreshRef.current = null; } };
  }, [autoRefresh, refreshRateSeconds, refreshConfig, isLive, services, isConnected, refreshLiveNavigator]);

  // During drag, pan the fixed-duration overview when the viewport reaches an
  // edge and keep the adjacent chunks prefetched.
  const handleNavigatorViewportChange = useCallback((newEndMs: number) => {
    const clamped = Math.min(newEndMs, Date.now());
    dragEndMsRef.current = clamped;
    setDragEndMs(clamped);
    const currentRange = navigatorRangeRef.current;
    if (!currentRange) return;

    const viewport = {
      startMs: clamped - windowSec * 2 * 1000,
      endMs: clamped,
    };
    const nextRange = panRangeToIncludeViewport(currentRange, viewport, Date.now());
    if (
      nextRange.startMs !== currentRange.startMs
      || nextRange.endMs !== currentRange.endMs
    ) {
      navigatorRangeRef.current = nextRange;
      setNavigatorRange(nextRange);
    }

    const prefetchThresholdMs = (nextRange.endMs - nextRange.startMs) * 0.15;
    if (viewport.startMs - nextRange.startMs <= prefetchThresholdMs) {
      requestNavigatorChunkAt(nextRange.startMs - 1);
    }
    if (
      nextRange.endMs - viewport.endMs <= prefetchThresholdMs
      && nextRange.endMs < Date.now() - 1000
    ) {
      requestNavigatorChunkAt(nextRange.endMs + 1);
    }
  }, [windowSec, requestNavigatorChunkAt]);

  // On drag end: commit the detail viewport. Navigator chunks have already
  // loaded independently during the gesture.
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

  const handleChartPin = useCallback((ms: number) => {
    pendingEventPinRef.current = null;
    setSelectedEventId(null);
    setPinnedMs(ms);
  }, []);

  const handleEventSelect = useCallback((event: OperationalEvent) => {
    const eventMs = new Date(event.occurred_at).getTime();
    if (!Number.isFinite(eventMs)) return;
    pendingEventPinRef.current = null;
    setSelectedEventId(event.id);
    setPinnedMs(eventMs);
    setHoverMs(eventMs);
  }, []);

  const handleClearEventSelection = useCallback(() => {
    pendingEventPinRef.current = null;
    setSelectedEventId(null);
    setPinnedMs(null);
    setHoverMs(null);
  }, []);

  const handleEventVisibilityChange = useCallback((visible: boolean) => {
    setTimeTravelEventsVisible(visible);
    if (!visible) handleClearEventSelection();
  }, [handleClearEventSelection, setTimeTravelEventsVisible]);

  const handleViewEventDetails = useCallback((event: OperationalEvent) => {
    navigate(buildEventsUrl(event));
  }, [navigate]);

  // Hand-off to the precise view: open the Workload Breakdown dashboard in Analytics.
  const openWorkloadBreakdown = useCallback(() => {
    navigate('/analytics?tab=dashboards&fromDashboard=workload-breakdown&from=timetravel');
  }, [navigate]);

  const handleNavigatorEventSelect = useCallback((event: OperationalEvent) => {
    const eventMs = new Date(event.occurred_at).getTime();
    if (!Number.isFinite(eventMs)) return;
    setSelectedEventId(event.id);
    setHoverMs(eventMs);

    if (eventMs >= viewportBounds.startMs && eventMs <= viewportBounds.endMs) {
      pendingEventPinRef.current = null;
      setPinnedMs(eventMs);
      return;
    }

    pendingEventPinRef.current = eventMs;
    const viewportSpanMs = windowSec * 2 * 1000;
    const requestedEnd = eventMs + viewportSpanMs / 2;
    const minEnd = navigatorRange
      ? navigatorRange.startMs + viewportSpanMs
      : requestedEnd;
    const maxEnd = navigatorRange?.endMs ?? Date.now();
    handleNavigatorDragEnd(Math.max(minEnd, Math.min(maxEnd, requestedEnd)));
  }, [viewportBounds, windowSec, navigatorRange, handleNavigatorDragEnd]);

  const activeMetricSampleCount = useMemo(() => {
    if (!data) return 0;
    if (metricMode === 'memory') return data.server_memory.length;
    if (metricMode === 'cpu') return data.server_cpu.length;
    if (metricMode === 'network') {
      return Math.max(data.server_network_send.length, data.server_network_recv.length);
    }
    return Math.max(data.server_disk_read.length, data.server_disk_write.length);
  }, [data, metricMode]);
  const timelineRamCapacity = data ? getTimelineRamCapacity(data) : 0;
  const timelineCpuCapacity = data ? getTimelineCpuCapacity(data) : 0;

  const eventFilterUniverse = useMemo(() => {
    const unique = new Map<string, OperationalEvent>();
    for (const event of eventData.events) unique.set(event.id, event);
    for (const event of navigatorEventData.events) unique.set(event.id, event);
    return [...unique.values()];
  }, [eventData.events, navigatorEventData.events]);

  const filteredWindowEvents = useMemo(
    () => filterTimelineEvents(eventData.events, eventFilter),
    [eventData.events, eventFilter],
  );

  const filteredNavigatorEvents = useMemo(
    () => filterTimelineEvents(navigatorEventData.events, eventFilter),
    [navigatorEventData.events, eventFilter],
  );

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
    // In queryHashOnly mode, show only hash-matched queries
    if (queryHashOnly && queryHashFilter) {
      result = result.filter(q => q.matched_hash);
    }
    const sorted = sortItems(result);
    // In hash filter mode, float matched queries to the top
    if (queryHashFilter && !queryHashOnly) {
      const matched = sorted.filter(q => q.matched_hash);
      const rest = sorted.filter(q => !q.matched_hash);
      return [...matched, ...rest];
    }
    return sorted;
  }, [data, inspectMs, zoomRange, metricMode, sortField, sortDir, queryHashOnly, queryHashFilter]);

  const filteredMerges = useMemo(() => {
    if (!data || (queryHashOnly && queryHashFilter)) return [];
    let result: MergeSeries[];
    if (inspectMs !== null) {
      result = data.merges.filter(m => { const s = parseTimestamp(m.start_time), e = parseTimestamp(m.end_time); return inspectMs >= s && inspectMs <= e; });
    } else if (zoomRange) {
      result = data.merges.filter(m => { const s = parseTimestamp(m.start_time), e = parseTimestamp(m.end_time); return s <= zoomRange[1] && e >= zoomRange[0]; });
    } else { result = [...data.merges]; }
    return sortItems(result);
  }, [data, inspectMs, zoomRange, metricMode, sortField, sortDir, queryHashOnly, queryHashFilter]);

  const filteredMutations = useMemo(() => {
    if (!data || (queryHashOnly && queryHashFilter)) return [];
    let result: MutationSeries[];
    if (inspectMs !== null) {
      result = (data.mutations ?? []).filter(m => { const s = parseTimestamp(m.start_time), e = parseTimestamp(m.end_time); return inspectMs >= s && inspectMs <= e; });
    } else if (zoomRange) {
      result = (data.mutations ?? []).filter(m => { const s = parseTimestamp(m.start_time), e = parseTimestamp(m.end_time); return s <= zoomRange[1] && e >= zoomRange[0]; });
    } else { result = [...(data.mutations ?? [])]; }
    return sortItems(result);
  }, [data, inspectMs, zoomRange, metricMode, sortField, sortDir, queryHashOnly, queryHashFilter]);

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

  const enterLiveMode = (rangeLabel?: string) => {
    // Remounting the navigator cancels any active pointer interaction so a
    // delayed mouse-up cannot restore a historical viewport after this action.
    setNavigatorInteractionEpoch(epoch => epoch + 1);
    clearDragPosition();
    setIsLive(true);
    setCustomEndTime(null);
    setCustomStartTime(null);
    setViewportEndTime(null);
    setSelectedTimeRange(
      rangeLabel ?? (selectedTimeRange === 'Custom' ? '1h' : selectedTimeRange),
    );
  };

  const handleTimeRangeChange = (rangeLabel: string) => {
    // "Last" presets are always anchored to now.
    enterLiveMode(rangeLabel);
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
            <div className="tracehouse-compact-select-control" style={{ display:'flex', alignItems:'center', gap:0, background:'var(--bg-tertiary)', borderRadius:6, border:'1px solid var(--border-primary)', overflow:'hidden' }}>
              <span style={{ padding:'5px 8px', fontSize:10, color:'var(--text-muted)', borderRight:'1px solid var(--border-primary)', whiteSpace:'nowrap', fontWeight:600 }}>Zoom</span>
              <select className="tracehouse-compact-native-select" value={windowSec} onChange={(e) => setWindowSec(Number(e.target.value))} title="Select time window duration"
                style={{ background:'transparent', color:'var(--text-primary)', border:'none', padding:'5px 10px', fontSize:11, outline:'none', cursor:'pointer', minWidth:60 }}>
                {WINDOW_SIZES.map(w => <option key={w.sec} value={w.sec}>{w.label}</option>)}
              </select>
            </div>

            {/* Activity limit selector */}
            <div className="tracehouse-compact-select-control" style={{ display:'flex', alignItems:'center', gap:0, background:'var(--bg-tertiary)', borderRadius:6, border:'1px solid var(--border-primary)', overflow:'hidden' }}>
              <span style={{ padding:'8px 10px', fontSize:11, color:'var(--text-muted)', borderRight:'1px solid var(--border-primary)', whiteSpace:'nowrap' }}>Show</span>
              <select className="tracehouse-compact-native-select" value={activityLimit} onChange={(e) => setActivityLimit(Number(e.target.value))} title="Max items per activity type (queries, merges, mutations)"
                style={{ background:'transparent', color:'var(--text-primary)', border:'none', padding:'8px 12px', fontSize:13, outline:'none', cursor:'pointer', minWidth:50 }}>
                {[25, 50, 100, 250, 500].map(n => <option key={n} value={n}>{n}</option>)}
              </select>
            </div>

            {/* One stable slot: in-flight control while live, return action while historical. */}
            {isLive ? (
              <button onClick={() => setIncludeRunning(!includeRunning)}
                title={includeRunning ? 'Showing in-flight queries/merges - click to hide' : 'Hiding in-flight queries/merges - click to show'}
                style={{
                  display:'flex', alignItems:'center', justifyContent:'center', gap:6, minWidth:88,
                  background: includeRunning ? 'rgba(88,166,255,0.1)' : 'var(--bg-tertiary)',
                  color: includeRunning ? '#58a6ff' : 'var(--text-muted)',
                  border: includeRunning ? '1px solid rgba(88,166,255,0.3)' : '1px solid var(--border-primary)',
                  borderRadius:6, padding:'8px 12px', fontSize:11, cursor:'pointer',
                  fontWeight: includeRunning ? 500 : 400,
                }}>
                {includeRunning && <span style={{ display:'inline-block', width:6, height:6, borderRadius:'50%', background:'#58a6ff', animation:'pulse 1.5s ease-in-out infinite' }} />}
                In-flight
              </button>
            ) : (
              <button onClick={() => enterLiveMode()} title="Jump to current time"
                style={{ display:'flex', alignItems:'center', justifyContent:'center', gap:6, minWidth:88, background:'rgba(63,185,80,0.15)', color:'#3fb950', border:'1px solid rgba(63,185,80,0.4)', borderRadius:6, padding:'8px 12px', fontSize:11, cursor:'pointer', fontWeight:500 }}>
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
            <div className="tabs" style={{ alignItems:'center' }}>
              <span role="group" aria-label="Timeline display" style={{ display:'inline-flex', gap:4 }}>
                <button
                  className={`tab ${!perServerView ? 'active' : ''}`}
                  aria-pressed={!perServerView}
                  onClick={() => setPerServerView(false)}
                  title="Average resource usage across selected servers in one chart"
                  style={{ padding:'6px 10px' }}
                >
                  Overall
                </button>
                <span style={{ position:'relative', display:'inline-flex' }}>
                  <button
                    className={`tab ${perServerView ? 'active' : ''}`}
                    aria-pressed={perServerView}
                    onClick={() => { if (viewMode === '2d') setPerServerView(true); }}
                    title={viewMode === '2d' ? 'Show one chart row for each selected server' : undefined}
                    style={{ display:'flex', alignItems:'center', padding:'6px 10px', ...(viewMode !== '2d' ? { opacity:0.35, cursor:'not-allowed' } : {}) }}
                  >
                    Per server
                  </button>
                  {viewMode !== '2d' && (
                    <span className="split-3d-tooltip" style={{
                      position:'absolute', bottom:'100%', left:'50%', transform:'translateX(-50%)',
                      marginBottom:6, padding:'6px 10px', borderRadius:6, fontSize:11, lineHeight:'1.4',
                      whiteSpace:'nowrap', pointerEvents:'none', opacity:0, transition:'opacity 0.15s ease',
                      background:'var(--bg-tertiary)', color:'var(--text-secondary)',
                      border:'1px solid var(--border-secondary)', boxShadow:'0 4px 12px rgba(0,0,0,0.3)',
                      zIndex:50,
                    }}>
                      Per-server view is not available in experimental 3D modes
                    </span>
                  )}
                </span>
              </span>
              <span
                aria-hidden="true"
                style={{ width:1, height:20, margin:'0 4px', background:'var(--border-primary)', flexShrink:0 }}
              />
              <span role="group" aria-label="Servers" style={{ display:'inline-flex', gap:4 }}>
                <button
                  className={`tab ${selectedHosts.length === 0 ? 'active' : ''}`}
                  aria-pressed={selectedHosts.length === 0}
                  onClick={() => setSelectedHosts([])}
                  title="Select all servers"
                  style={{ padding:'6px 10px' }}
                >
                  All
                </button>
                {clusterHosts.map(host => (
                  <button
                    key={host}
                    className={`tab ${selectedHosts.includes(host) ? 'active' : ''}`}
                    aria-pressed={selectedHosts.includes(host)}
                    onClick={event => handleHostClick(event, host)}
                    title={`Select ${host}. Shift, Ctrl, or Command-click to select multiple servers.`}
                    style={{ padding:'6px 10px' }}
                  >
                    <TruncatedHost name={host} />
                    </button>
                  ))}
              </span>
            </div>
          )}
          {data && (
            <MetricStrip
              ariaLabel="Time Travel summary"
              style={{ flex:1, minWidth:0, gap:'8px 12px' }}
            >
              <MetricStripItem
                label="samples"
                value={activeMetricSampleCount}
                indicatorColor="#58a6ff"
              />
              <MetricStripDivider />
              <MetricStripItem
                label="queries"
                value={`${filteredQueries.length}/${data.query_count ?? data.queries.length}`}
                indicatorColor="#79c0ff"
                title={SAMPLING_NOTE(metricMode)}
              />
              <MetricStripItem
                label="merges"
                value={`${filteredMerges.length}/${data.merge_count}`}
                indicatorColor="#f0883e"
                title={SAMPLING_NOTE(metricMode)}
              />
              {(data.mutation_count ?? 0) > 0 && (
                <MetricStripItem
                  label="mutations"
                  value={`${filteredMutations.length}/${data.mutation_count}`}
                  indicatorColor="#f778ba"
                  title={SAMPLING_NOTE(metricMode)}
                />
              )}
              <MetricStripDivider />
              <button
                type="button"
                onClick={openWorkloadBreakdown}
                title={SAMPLING_NOTE(metricMode)}
                style={{
                  display:'inline-flex', alignItems:'center', gap:5,
                  padding:'2px 8px', fontSize:11, cursor:'pointer',
                  color:'var(--text-muted)', background:'transparent',
                  border:'1px solid var(--border-secondary)', borderRadius:5,
                  fontFamily:'inherit', whiteSpace:'nowrap', transition:'color 0.15s ease, border-color 0.15s ease',
                }}
                onMouseEnter={e => { e.currentTarget.style.color = 'var(--text-primary)'; e.currentTarget.style.borderColor = 'var(--border-primary)'; }}
                onMouseLeave={e => { e.currentTarget.style.color = 'var(--text-muted)'; e.currentTarget.style.borderColor = 'var(--border-secondary)'; }}
              >
                <span aria-hidden="true" style={{ opacity:0.8 }}>ⓘ</span>
                sampled
              </button>
              {(timelineRamCapacity > 0 || timelineCpuCapacity > 0) && (
                <MetricStripDivider />
              )}
              {(timelineRamCapacity > 0 || timelineCpuCapacity > 0) && (
                <MetricStripItem
                  label={(data.host_count || 1) === 1 ? 'host' : 'hosts'}
                  value={data.host_count || 1}
                />
              )}
              {timelineRamCapacity > 0 && (
                <MetricStripItem
                  label="RAM"
                  value={formatBytes(timelineRamCapacity)}
                  indicatorColor="#f85149"
                />
              )}
              {timelineCpuCapacity > 0 && (
                <MetricStripItem
                  label="CPUs"
                  value={timelineCpuCapacity}
                  indicatorColor="#3fb950"
                />
              )}
            </MetricStrip>
          )}
        </div>
      </div>

      {error && (
        <div style={{ margin:'12px 24px 0', padding:'10px 14px', borderRadius:8, fontSize:13, background:'rgba(248,81,73,0.08)', color:'#f85149', border:'1px solid rgba(248,81,73,0.2)' }}>
          {error}
        </div>
      )}

      {queryHashFilter && (
        <div style={{
          margin:'12px 16px 0', padding:'8px 14px', borderRadius:8, fontSize:12,
          background:'rgba(88,166,255,0.08)', color:'#58a6ff', border:'1px solid rgba(88,166,255,0.2)',
          display:'flex', alignItems:'center', justifyContent:'space-between',
        }}>
          <div style={{ display:'flex', alignItems:'center', gap:12 }}>
            <span>
              Tracking query hash <span style={{ fontFamily:'monospace', opacity:0.7 }}>{queryHashFilter?.slice(0, 12)}{(queryHashFilter?.length ?? 0) > 12 ? '…' : ''}</span>
              {data && (() => {
                const matchCount = data.queries.filter(q => q.matched_hash).length;
                return <span style={{ marginLeft:4, opacity:0.8 }}>— {matchCount} {matchCount === 1 ? 'match' : 'matches'} in window</span>;
              })()}
            </span>
            <div style={{
              display:'flex', gap:2, padding:2, borderRadius:6,
              background:'rgba(88,166,255,0.1)', border:'1px solid rgba(88,166,255,0.2)',
            }}>
              <button
                onClick={() => setQueryHashOnly(false)}
                style={{
                  padding:'3px 10px', fontSize:11, fontWeight:600, borderRadius:4, border:'none', cursor:'pointer',
                  background: !queryHashOnly ? 'rgba(88,166,255,0.25)' : 'transparent',
                  color:'#58a6ff',
                }}
              >
                All + highlighted
              </button>
              <button
                onClick={() => setQueryHashOnly(true)}
                style={{
                  padding:'3px 10px', fontSize:11, fontWeight:600, borderRadius:4, border:'none', cursor:'pointer',
                  background: queryHashOnly ? 'rgba(88,166,255,0.25)' : 'transparent',
                  color:'#58a6ff',
                }}
              >
                Matched only
              </button>
            </div>
          </div>
          <button
            onClick={() => { clearQueryHashFilter(); setQueryHashOnly(false); }}
            style={{
              background:'rgba(88,166,255,0.15)', border:'1px solid rgba(88,166,255,0.3)', borderRadius:6,
              color:'#58a6ff', padding:'4px 12px', fontSize:11, cursor:'pointer', fontWeight:600,
            }}
          >
            Clear filter
          </button>
        </div>
      )}

      {data && (
        <div style={{ flex:1, overflow:'auto', padding:'12px 16px 20px' }}>
          {/* Metric/view controls and 2D event controls */}
          <div style={{
            display: 'flex',
            alignItems: 'flex-start',
            justifyContent: 'space-between',
            flexWrap: 'wrap',
            gap: 8,
            marginBottom: 16,
          }}>
            <div style={{ display:'flex', gap:4, background:'var(--bg-tertiary)', borderRadius:8, padding:3, width:'fit-content', alignItems:'center' }}>
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
                  <button key={mode} onClick={() => { setViewMode(mode); if (mode !== '2d') setPerServerView(false); }}
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
            {viewMode === '2d' && (
              <TimelineEventControls
                visible={timeTravelEventsVisible}
                shownCount={filteredWindowEvents.length}
                totalCount={eventData.events.length}
                filterUniverse={eventFilterUniverse}
                coverage={eventData.coverage}
                filter={eventFilter}
                onVisibilityChange={handleEventVisibilityChange}
                onFilterChange={setEventFilter}
              />
            )}
          </div>

          {/* Chart */}
          {rowHosts.length > 0 ? (
            /* Per-server view: one chart per selected host */
            <div style={{ display:'flex', flexDirection:'column', gap:2 }}>
              {splitLoading && rowHosts.every(host => !perHostData.has(host)) && (
                <div style={{ padding:20, textAlign:'center', color:'var(--text-muted)', fontSize:13 }}>Loading per-host data…</div>
              )}
              {rowHosts.map((host) => {
                const hostData = perHostData.get(host);
                if (!hostData) return null;
                const hostCpuCapacity = getTimelineCpuCapacity(hostData);
                const hostRamCapacity = getTimelineRamCapacity(hostData);
                const chartHeight = Math.max(140, Math.floor(460 / rowHosts.length));
                return (
                  <div key={host} style={{
                    borderRadius:8, background:'var(--bg-secondary)', border:'1px solid var(--border-primary)',
                    overflow:'visible', position:'relative',
                  }}
                  onMouseEnter={(e) => { (e.currentTarget as HTMLElement).style.zIndex = '20'; }}
                  onMouseLeave={(e) => { (e.currentTarget as HTMLElement).style.zIndex = '0'; }}
                  >
                    <button
                      onClick={() => { setSelectedHosts([host]); setPerServerView(false); }}
                      title={`Switch to ${host}`}
                      style={{
                        position:'absolute', top:6, left:10, zIndex:10, fontSize:10, fontWeight:600,
                        color:'var(--text-secondary)', background:'var(--bg-tertiary)', padding:'2px 8px',
                        borderRadius:4, border:'1px solid var(--border-primary)', opacity:0.9,
                        cursor:'pointer',
                      }}
                    >
                      <TruncatedHost name={host} />
                      {hostCpuCapacity > 0 && <span style={{ color:'var(--text-muted)', marginLeft:6 }}>{hostCpuCapacity} cores</span>}
                      {hostRamCapacity > 0 && <span style={{ color:'var(--text-muted)', marginLeft:6 }}>{formatBytes(hostRamCapacity)} RAM</span>}
                    </button>
                    <TimelineChart data={hostData} metricMode={metricMode} height={chartHeight}
                      hoverMs={hoverMs} pinnedMs={pinnedMs}
                      onHover={setHoverMs} onPin={handleChartPin}
                      zoomRange={zoomRange} onZoom={setZoomRange}
                      highlightedItem={highlightedItem}
                      onHighlightItem={setHighlightedItem}
                      hiddenCategories={hiddenCategories}
                      queryHashActive={!!queryHashFilter}
                      eventAnnotations={timeTravelEventsVisible
                        ? filterTimelineEvents(
                            eventData.events.filter(event => event.hostname === host),
                            eventFilter,
                          )
                        : []}
                      selectedEventId={selectedEventId}
                      onEventSelect={handleEventSelect}
                      onClearEventSelection={handleClearEventSelection}
                      onViewEventDetails={handleViewEventDetails}
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
          /* Single chart (Overall selected-host total or one server) */
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
                  <button onClick={() => { setPinnedMs(null); setSelectedEventId(null); }} style={{ padding:'3px 8px', borderRadius:6, fontSize:11, background:'rgba(63,185,80,0.12)', border:'1px solid rgba(63,185,80,0.25)', color:'#3fb950', cursor:'pointer', backdropFilter:'blur(8px)' }}>
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
            <TimelineChart data={chartData!} metricMode={metricMode} height={500}
              hoverMs={hoverMs} pinnedMs={pinnedMs}
              onHover={setHoverMs} onPin={handleChartPin}
              zoomRange={zoomRange} onZoom={setZoomRange}
              highlightedItem={highlightedItem}
              onHighlightItem={setHighlightedItem}
              hiddenCategories={hiddenCategories}
              queryHashActive={!!queryHashFilter}
              eventAnnotations={timeTravelEventsVisible ? filteredWindowEvents : []}
              selectedEventId={selectedEventId}
              onEventSelect={handleEventSelect}
              onClearEventSelection={handleClearEventSelection}
              onViewEventDetails={handleViewEventDetails}
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
                <span
                  role="group"
                  aria-label="Navigator shape"
                  style={{
                    display: 'inline-flex',
                    padding: 2,
                    borderRadius: 5,
                    border: '1px solid var(--border-primary)',
                    background: 'var(--bg-secondary)',
                  }}
                >
                  {([
                    ['trend', 'Avg'],
                    ['peaks', 'Max'],
                    ['change', 'Δ'],
                  ] as const).map(([shape, label]) => {
                    const selected = timeTravelNavigatorShape === shape;
                    return (
                      <button
                        key={shape}
                        type="button"
                        aria-pressed={selected}
                        aria-label={shape === 'trend'
                          ? 'Show bucket averages'
                          : shape === 'peaks'
                            ? 'Show bucket peaks'
                            : 'Show change between buckets'}
                        title={shape === 'trend'
                          ? 'Average value per time bucket'
                          : shape === 'peaks'
                            ? 'Maximum value per time bucket'
                            : 'Change from the previous time bucket · amber increases, cyan decreases'}
                        onClick={() => setTimeTravelNavigatorShape(shape)}
                        style={{
                          border: 0,
                          borderRadius: 3,
                          padding: '1px 6px',
                          fontSize: 10,
                          lineHeight: '16px',
                          cursor: 'pointer',
                          color: selected ? 'var(--text-primary)' : 'var(--text-muted)',
                          background: selected ? 'var(--bg-tertiary)' : 'transparent',
                          boxShadow: selected ? '0 1px 2px rgba(0,0,0,0.18)' : 'none',
                        }}
                      >
                        {label}
                      </button>
                    );
                  })}
                </span>
                <span>· Drag to navigate</span>
              </div>
              <TimelineNavigator
                key={navigatorInteractionEpoch}
                data={timeTravelNavigatorShape === 'trend'
                  ? navigatorMetricData.trend
                  : timeTravelNavigatorShape === 'peaks'
                    ? navigatorMetricData.peaks
                    : navigatorChangeData}
                scaleData={timeTravelNavigatorShape === 'change'
                  ? undefined
                  : navigatorMetricData.peaks}
                variant={timeTravelNavigatorShape === 'change' ? 'delta' : 'area'}
                bucketMs={navigatorBucketSec * 1000}
                metricMode={metricMode}
                rangeStartMs={navigatorRange.startMs} rangeEndMs={navigatorRange.endMs}
                viewportStartMs={viewportBounds.startMs} viewportEndMs={viewportBounds.endMs}
                onViewportChange={handleNavigatorViewportChange} height={70}
                isLoading={navigatorLoading}
                totalRam={timelineRamCapacity}
                cpuCores={timelineCpuCapacity}
                onDragEnd={handleNavigatorDragEnd}
                events={timeTravelEventsVisible ? filteredNavigatorEvents : []}
                selectedEventId={selectedEventId}
                onEventSelect={handleNavigatorEventSelect}
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
              queryHashActive={!!queryHashFilter}
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
              queryHashActive={!!queryHashFilter}
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
              queryHashActive={!!queryHashFilter}
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
      <QueryDetailModal query={deepLinkedQuery} onClose={handleQueryClose} onViewInTimeTravel={activateQueryHashFilter} />
      <MergeDetailModal merge={selectedTimelineMerge} onClose={() => setSelectedTimelineMerge(null)} />
      <MutationDetailModal mutation={selectedTimelineMutation} onClose={() => setSelectedTimelineMutation(null)} />
    </div>
  );
};

export default TimeTravelPage;
