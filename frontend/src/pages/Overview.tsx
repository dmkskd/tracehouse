/**
 * Overview - Unified system dashboard
 * 
 * Features:
 * - Live/Metrics toggle for metrics
 * - Resource Attribution Bar (CPU/Memory/IO breakdown)
 * - Summary stat cards (CPU, Memory, Disk, Uptime)
 * - Running Queries, Active Merges, Mutations tables
 * - Replication Summary
 * - Alert Banner
 */

import React, { useEffect, useState, useCallback, useRef, useMemo } from 'react';
import { useAppLocation } from '../hooks/useAppLocation';
import { useClickHouseServices } from '../providers/ClickHouseProvider';
import { useConnectionStore } from '../stores/connectionStore';
import { useRefreshConfig, clampToAllowed } from '@tracehouse/ui-shared';
import { useRefreshSettingsStore } from '../stores/refreshSettingsStore';
import { useGlobalLastUpdatedStore } from '../stores/refreshSettingsStore';
import {
  useMetricsStore,
  MetricsWebSocket,
  calculateMemoryPercentage,
} from '../stores/metricsStore';
import { useTimeSeriesStore } from '../stores/timeSeriesStore';
import { useOverviewStore, OverviewPoller, type AttributionSnapshot } from '../stores/overviewStore';
import { TimeSeriesChart } from '../components/metrics/TimeSeriesChart';
import { OverviewService } from '@tracehouse/core';
import {
  ResourceAttributionBar,
  SparklineStatCard,
  ResourceArena3D,
  ResourceArenaSwimlane,
} from '../components/overview';
import type { ClusterHistoricalMetricsPoint, RunningQueryInfo, ActiveMergeInfo } from '@tracehouse/core';
import type { ChartDataPoint, TrendMetricType, MetricStats } from '../stores/timeSeriesStore';
import { formatBytes, formatDuration } from '../utils/formatters';
import { TruncatedHost } from '../components/common/TruncatedHost';
import { ObservabilitySunburst, DetailSidebar, OBSERVABILITY_DATA, enrichWithAvailability, mergeAvailability, fetchColumnComments } from '../components/observability-map';
import type { SunburstNodeData, QueryResult, ObservabilityData, ColumnCommentMap } from '../components/observability-map';
import { useUserPreferenceStore } from '../stores/userPreferenceStore';
import useMonitoringCapabilitiesStore from '../stores/monitoringCapabilitiesStore';


// No Connection Component
const NoConnection: React.FC<{ onConnect: () => void }> = ({ onConnect }) => (
  <div className="flex flex-col items-center justify-center py-16">
    <div
      className="w-16 h-16 rounded-full flex items-center justify-center text-xl font-bold mb-4"
      style={{ background: 'var(--bg-tertiary)', color: 'var(--text-muted)' }}
    >
      --
    </div>
    <h3 className="text-lg font-semibold mb-2" style={{ color: 'var(--text-primary)' }}>
      No Connection
    </h3>
    <p className="text-sm mb-4 text-center max-w-md" style={{ color: 'var(--text-secondary)' }}>
      Connect to a ClickHouse server to view real-time metrics
    </p>
    <button className="btn btn-primary" onClick={onConnect}>
      Add Connection
    </button>
  </div>
);

// Stable empty arrays to avoid creating new references on every render
const EMPTY_QUERIES: RunningQueryInfo[] = [];
const EMPTY_MERGES: ActiveMergeInfo[] = [];

export const Overview: React.FC = () => {
  const location = useAppLocation();
  const services = useClickHouseServices();
  const activeProfileId = useConnectionStore(s => s.activeProfileId);
  const profiles = useConnectionStore(s => s.profiles);
  const setConnectionFormOpen = useConnectionStore(s => s.setConnectionFormOpen);
  const refreshConfig = useRefreshConfig();
  const { refreshRateSeconds } = useRefreshSettingsStore();
  const { metrics, metricsHistory, clearMetrics, thresholds, warnings } = useMetricsStore();
  const manualRefreshTick = useGlobalLastUpdatedStore(s => s.manualRefreshTick);
  const { viewMode, selectedMetrics, setViewMode, toggleMetric } = useTimeSeriesStore();

  // Restore map view when navigating back from Analytics obs-map link
  useEffect(() => {
    const restoreObsMap = (location.state as { restoreObsMap?: boolean } | null)?.restoreObsMap;
    const savedTable = sessionStorage.getItem('obsmap:selectedTable');
    console.log('[ObsMap restore]', { restoreObsMap, savedTable, viewMode, locationState: location.state });
    if (restoreObsMap) {
      setViewMode('map');
    }
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // Overview store
  const {
    data: liveData,
    attributionHistory,
    selectedResource,
    setSelectedResource,
    toggleExpandedQuery,
    clearData: clearLiveData,
  } = useOverviewStore();


  // Historical metrics state (cluster-aware)
  const [clusterHosts, setClusterHosts] = useState<string[]>([]);
  const [clusterMetrics, setClusterMetrics] = useState<ClusterHistoricalMetricsPoint[]>([]);
  const [selectedHost, setSelectedHost] = useState<string | null>(null);
  const [splitView, setSplitView] = useState(false);
  const [arenaSplitView, setArenaSplitView] = useState(false);
  const preferredViewMode = useUserPreferenceStore(s => s.preferredViewMode);

  // Cluster hostnames from server info (reliable — always returns all nodes)
  const arenaHosts = liveData?.serverInfo?.clusterHosts ?? [];

  const [timeRangeMinutes, setTimeRangeMinutes] = useState(15);
  const [isLoadingHistory, setIsLoadingHistory] = useState(false);
  const [historyError, setHistoryError] = useState<string | null>(null);

  // Observability map state — restore selected table from session if navigating back
  const [mapSearchQuery, setMapSearchQuery] = useState('');
  const [mapClickedNode, setMapClickedNode] = useState<SunburstNodeData | null>(() => {
    const saved = sessionStorage.getItem('obsmap:selectedTable');
    if (saved) {
      // Find the table in data to rebuild the node with full metadata
      for (const cat of OBSERVABILITY_DATA.children) {
        const table = cat.children.find(t => t.name === saved);
        if (table) {
          return {
            name: table.name,
            meta: { type: 'table', category: cat.name, color: cat.color, desc: table.desc, cols: table.cols, queries: table.queries, since: table.since, cloudOnly: table.cloudOnly },
          };
        }
      }
    }
    return null;
  });
  const [mapQueryResult, setMapQueryResult] = useState<QueryResult | null>(null);
  const [mapQueryRunning, setMapQueryRunning] = useState(false);
  const [mapRunQueryIndex, setMapRunQueryIndex] = useState<number | null>(null);
  const [enrichedMapData, setEnrichedMapData] = useState<ObservabilityData | null>(null);
  const [columnComments, setColumnComments] = useState<ColumnCommentMap>(new Map());
  const [mapDetailExpanded, setMapDetailExpanded] = useState(false);

  // Monotonic key that bumps whenever the services object changes (i.e. connection switch/reconnect).
  // Used to force-remount stateful children like ResourceArena3D.
  const servicesGenRef = useRef(0);
  const prevServicesRef = useRef(services);
  if (services !== prevServicesRef.current) {
    prevServicesRef.current = services;
    servicesGenRef.current += 1;
  }

  const wsRef = useRef<MetricsWebSocket | null>(null);
  const livePollerRef = useRef<OverviewPoller | null>(null);

  const activeProfile = profiles.find(p => p.id === activeProfileId);
  const isConnected = activeProfile?.is_connected ?? false;

  // Probe server for available system tables + column comments (once per connection)
  useEffect(() => {
    if (!services) { setEnrichedMapData(null); setColumnComments(new Map()); return; }
    let cancelled = false;
    const exec = services.adapter.executeQuery.bind(services.adapter);
    enrichWithAvailability(exec)
      .then(serverTables => {
        if (!cancelled) {
          setEnrichedMapData(mergeAvailability(OBSERVABILITY_DATA, serverTables));
        }
      });
    fetchColumnComments(exec)
      .then(comments => {
        if (!cancelled) setColumnComments(comments);
      });
    return () => { cancelled = true; };
  }, [services]);

  const handleOpenConnectionForm = useCallback(() => {
    setConnectionFormOpen(true);
  }, [setConnectionFormOpen]);

  const handleRunDiagnosticQuery = useCallback(async (sql: string, queryIndex: number) => {
    if (!services) return;
    setMapQueryRunning(true);
    setMapQueryResult(null);
    setMapRunQueryIndex(queryIndex);
    const t0 = performance.now();
    try {
      const rows = await services.adapter.executeQuery<Record<string, unknown>>(sql);
      const columns = rows.length > 0 ? Object.keys(rows[0]) : [];
      setMapQueryResult({ columns, rows, executionTime: performance.now() - t0 });
    } catch (err) {
      setMapQueryResult({
        columns: [], rows: [], executionTime: performance.now() - t0,
        error: err instanceof Error ? err.message : String(err),
      });
    } finally {
      setMapQueryRunning(false);
    }
  }, [services]);

  // The node being shown in the sidebar (hovered or clicked)
  const [mapHoveredNode, setMapHoveredNode] = useState<SunburstNodeData | null>(null);

  const handleMapHoverNode = useCallback((node: SunburstNodeData | null) => {
    // Only update sidebar on hover if nothing is click-selected
    if (!mapClickedNode) setMapHoveredNode(node);
  }, [mapClickedNode]);

  const handleMapSelectNode = useCallback((node: SunburstNodeData | null) => {
    // Toggle: click same node again to deselect
    if (mapClickedNode && node && mapClickedNode.name === node.name && mapClickedNode.meta?.type === node.meta?.type) {
      setMapClickedNode(null);
      setMapHoveredNode(null);
      sessionStorage.removeItem('obsmap:selectedTable');
    } else {
      setMapClickedNode(node);
      setMapHoveredNode(null);
      if (node?.meta?.type === 'table') {
        sessionStorage.setItem('obsmap:selectedTable', node.name);
      } else {
        sessionStorage.removeItem('obsmap:selectedTable');
      }
    }
    setMapQueryResult(null);
  }, [mapClickedNode]);


  // Fetch historical metrics from ClickHouse (cluster-aware)
  const fetchHistoricalMetrics = useCallback(async () => {
    if (!services) return;
    setIsLoadingHistory(true);
    setHistoryError(null);
    try {
      const toTime = new Date();
      const fromTime = new Date(toTime.getTime() - timeRangeMinutes * 60 * 1000);
      const result = await services.metricsCollector.getClusterHistoricalMetrics(fromTime, toTime);
      setClusterHosts(result.hosts);
      setClusterMetrics(result.data);
    } catch (err) {
      let msg = 'Unknown error';
      if (err instanceof Error) {
        msg = err.message;
        if ('cause' in err && err.cause instanceof Error) {
          msg += `: ${err.cause.message}`;
        }
      }
      setHistoryError(msg);
    } finally {
      setIsLoadingHistory(false);
    }
  }, [services, timeRangeMinutes]);

  // Setup WebSocket for real-time metrics (snapshot mode)
  useEffect(() => {
    if (wsRef.current) {
      wsRef.current.disconnect();
      wsRef.current = null;
    }
    if (!services || !isConnected) {
      clearMetrics();
      return;
    }
    wsRef.current = new MetricsWebSocket(services.metricsCollector);
    wsRef.current.connect();
    return () => {
      if (wsRef.current) {
        wsRef.current.disconnect();
        wsRef.current = null;
      }
    };
  }, [services, isConnected, clearMetrics]);

  // Setup Overview polling
  useEffect(() => {
    if (livePollerRef.current) {
      livePollerRef.current.stop();
      livePollerRef.current = null;
    }

    // Always clear stale data when dependencies change (e.g. connection switch)
    clearLiveData();

    if (!services || !isConnected) {
      return;
    }

    const overviewService = new OverviewService(services.adapter, {}, services.environmentDetector);
    const intervalMs = refreshRateSeconds > 0 ? clampToAllowed(refreshRateSeconds, refreshConfig) * 1000 : 5000;
    livePollerRef.current = new OverviewPoller(overviewService, intervalMs);
    if (refreshRateSeconds > 0) livePollerRef.current.start();

    return () => {
      if (livePollerRef.current) {
        livePollerRef.current.stop();
        livePollerRef.current = null;
      }
    };
  }, [services, isConnected, clearLiveData, refreshRateSeconds, refreshConfig, manualRefreshTick]);

  // Fetch historical data when switching to trends or changing time range
  useEffect(() => {
    if (viewMode === 'trend' && services && isConnected) {
      fetchHistoricalMetrics();
    }
  }, [viewMode, timeRangeMinutes, services, isConnected, fetchHistoricalMetrics]);

  // Convert historical data to chart format (cluster-aware)
  const chartData: ChartDataPoint[] = useMemo(() => {
    if (selectedHost) {
      const filtered = clusterMetrics.filter(p => p.hostname === selectedHost);
      return filtered.map(point => ({
        timestamp: point.timestamp,
        time: new Date(point.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' }),
        cpu_usage: point.cpu_usage,
        memory_percentage: point.memory_total > 0 ? (point.memory_used / point.memory_total) * 100 : 0,
        memory_used: point.memory_used,
        memory_total: point.memory_total,
        disk_read_rate: point.disk_read_rate,
        disk_write_rate: point.disk_write_rate,
        network_send_rate: point.network_send_rate ?? 0,
        network_recv_rate: point.network_recv_rate ?? 0,
      }));
    }

    if (clusterHosts.length <= 1) {
      return clusterMetrics.map(point => ({
        timestamp: point.timestamp,
        time: new Date(point.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' }),
        cpu_usage: point.cpu_usage,
        memory_percentage: point.memory_total > 0 ? (point.memory_used / point.memory_total) * 100 : 0,
        memory_used: point.memory_used,
        memory_total: point.memory_total,
        disk_read_rate: point.disk_read_rate,
        disk_write_rate: point.disk_write_rate,
        network_send_rate: point.network_send_rate ?? 0,
        network_recv_rate: point.network_recv_rate ?? 0,
      }));
    }

    // "All" multi-host view — flatten per-host values into keyed columns
    const byTime = new Map<number, Map<string, ClusterHistoricalMetricsPoint>>();
    for (const p of clusterMetrics) {
      if (!byTime.has(p.timestamp)) byTime.set(p.timestamp, new Map());
      byTime.get(p.timestamp)!.set(p.hostname, p);
    }

    return [...byTime.entries()]
      .sort(([a], [b]) => a - b)
      .map(([ts, hostMap]) => {
        const row: Record<string, unknown> = {
          timestamp: ts,
          time: new Date(ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' }),
          cpu_usage: 0, memory_percentage: 0, memory_used: 0, memory_total: 0,
          disk_read_rate: 0, disk_write_rate: 0, network_send_rate: 0, network_recv_rate: 0,
        };
        for (const host of clusterHosts) {
          const p = hostMap.get(host);
          if (p) {
            row[`cpu_usage__${host}`] = p.cpu_usage;
            row[`memory_percentage__${host}`] = p.memory_total > 0 ? (p.memory_used / p.memory_total) * 100 : 0;
            row[`disk_read_rate__${host}`] = p.disk_read_rate;
            row[`disk_write_rate__${host}`] = p.disk_write_rate;
            row[`network_send_rate__${host}`] = p.network_send_rate ?? 0;
            row[`network_recv_rate__${host}`] = p.network_recv_rate ?? 0;
          }
        }
        return row as unknown as ChartDataPoint;
      });
  }, [clusterMetrics, selectedHost, clusterHosts]);

  const isMultiHostView = !selectedHost && !splitView && clusterHosts.length > 1;

  // Per-host chart data for split view
  const perHostChartData = useMemo<Map<string, ChartDataPoint[]>>(() => {
    if (!splitView || clusterHosts.length < 2) return new Map();
    const map = new Map<string, ChartDataPoint[]>();
    for (const host of clusterHosts) {
      const filtered = clusterMetrics.filter(p => p.hostname === host);
      map.set(host, filtered.map(point => ({
        timestamp: point.timestamp,
        time: new Date(point.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' }),
        cpu_usage: point.cpu_usage,
        memory_percentage: point.memory_total > 0 ? (point.memory_used / point.memory_total) * 100 : 0,
        memory_used: point.memory_used,
        memory_total: point.memory_total,
        disk_read_rate: point.disk_read_rate,
        disk_write_rate: point.disk_write_rate,
        network_send_rate: point.network_send_rate ?? 0,
        network_recv_rate: point.network_recv_rate ?? 0,
      })));
    }
    return map;
  }, [splitView, clusterHosts, clusterMetrics]);

  // Calculate stats from historical data
  const trendStats: Partial<Record<TrendMetricType, MetricStats | null>> = useMemo(() => {
    if (chartData.length === 0) return {};

    const calcStats = (values: number[]): MetricStats => ({
      min: Math.min(...values),
      max: Math.max(...values),
      avg: values.reduce((a, b) => a + b, 0) / values.length,
      current: values[values.length - 1],
    });

    const extractValues = (metric: TrendMetricType): number[] => {
      if (isMultiHostView) {
        const vals: number[] = [];
        for (const d of chartData) {
          const row = d as unknown as Record<string, number>;
          for (const host of clusterHosts) {
            const v = row[`${metric}__${host}`];
            if (v !== undefined) vals.push(v);
          }
        }
        return vals.length > 0 ? vals : [0];
      }
      return chartData.map(d => d[metric] as number);
    };

    return {
      cpu_usage: calcStats(extractValues('cpu_usage')),
      memory_percentage: calcStats(extractValues('memory_percentage')),
      disk_read_rate: calcStats(extractValues('disk_read_rate')),
      disk_write_rate: calcStats(extractValues('disk_write_rate')),
      network_send_rate: calcStats(extractValues('network_send_rate')),
      network_recv_rate: calcStats(extractValues('network_recv_rate')),
    };
  }, [chartData, isMultiHostView, clusterHosts]);

  const memoryPercentage = metrics ? calculateMemoryPercentage(metrics.memory_used, metrics.memory_total) : 0;

  // Cluster mode: use cluster-wide resource attribution data for stat cards
  const isCluster = arenaHosts.length > 1;
  const ra = liveData?.resourceAttribution;
  const clusterCpuPct = ra?.cpu.totalPct ?? 0;
  const clusterCores = ra?.cpu.cores ?? 0;
  const clusterMemPct = ra && ra.memory.totalRAM > 0
    ? (ra.memory.totalRSS / ra.memory.totalRAM) * 100 : 0;

  // Sparkline data: cluster-wide from attribution history when in cluster mode,
  // otherwise single-node from metricsHistory
  const sparkCpu = useMemo(() =>
    isCluster
      ? attributionHistory.map((s: AttributionSnapshot) => s.cpuPct)
      : metricsHistory.map(h => h.metrics.cpu_usage),
    [isCluster, attributionHistory, metricsHistory]);
  const sparkMem = useMemo(() =>
    isCluster
      ? attributionHistory.map((s: AttributionSnapshot) => s.memoryPct)
      : metricsHistory.map(h => h.metrics.memory_total > 0 ? (h.metrics.memory_used / h.metrics.memory_total) * 100 : 0),
    [isCluster, attributionHistory, metricsHistory]);
  const sparkDiskR = useMemo(() =>
    isCluster
      ? attributionHistory.map((s: AttributionSnapshot) => s.ioReadBps)
      : metricsHistory.map(h => h.metrics.disk_read_bytes),
    [isCluster, attributionHistory, metricsHistory]);
  const sparkDiskW = useMemo(() =>
    isCluster
      ? attributionHistory.map((s: AttributionSnapshot) => s.ioWriteBps)
      : metricsHistory.map(h => h.metrics.disk_write_bytes),
    [isCluster, attributionHistory, metricsHistory]);

  // No connection state
  if (!activeProfileId || !isConnected) {
    return (
      <div className="space-y-6" style={{ padding: '24px', background: 'var(--bg-primary)', minHeight: '100%' }}>
        <div className="flex items-center justify-between">
          <h1 className="text-xl font-semibold" style={{ color: 'var(--text-primary)' }}>
            Overview
          </h1>
        </div>
        <div className="card">
          <NoConnection onConnect={handleOpenConnectionForm} />
        </div>
      </div>
    );
  }

  // Degradation summary — list key capabilities that are missing
  const { flags: capFlags, probeStatus } = useMonitoringCapabilitiesStore();
  const degradedFeatures = useMemo(() => {
    if (probeStatus !== 'done') return [];
    const items: string[] = [];
    if (!capFlags.hasQueryLog) items.push('Query history');
    if (!capFlags.hasMetricLog) items.push('Metric trends');
    if (!capFlags.hasSystemMerges) items.push('Merge tracking');
    if (!capFlags.hasSystemProcesses) items.push('Running queries');
    return items;
  }, [probeStatus, capFlags]);

  // Build breakdown segments from resource attribution (for inline bars in stat cards)
  return (
    <div className="page-layout" style={viewMode === 'map' ? { height: '100%', minHeight: 0, overflow: 'hidden', display: 'flex', flexDirection: 'column' } : undefined}>
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-4">
          <h1 className="text-xl font-semibold" style={{ color: 'var(--text-primary)' }}>
            Overview
          </h1>
          {liveData?.serverInfo && (
            <span className="text-sm" style={{ color: 'var(--text-muted)' }}>
              {isCluster
                ? `${arenaHosts.length} nodes • v${liveData.serverInfo.version}`
                : `${liveData.serverInfo.hostname} • v${liveData.serverInfo.version}`}
            </span>
          )}
        </div>
        <div className="flex items-center gap-4">
          {/* View Toggle */}
          <div className="tabs">
            <button
              className={`tab ${viewMode === 'snapshot' ? 'active' : ''}`}
              onClick={() => setViewMode('snapshot')}
            >
              Live
            </button>
            <button
              className={`tab ${viewMode === 'trend' ? 'active' : ''}`}
              onClick={() => setViewMode('trend')}
            >
              Metrics
            </button>
            <button
              className={`tab ${viewMode === 'map' ? 'active' : ''}`}
              onClick={() => setViewMode('map')}
            >
              System Map
            </button>
          </div>
        </div>
      </div>

      {/* Degradation banner — shown when key capabilities are missing */}
      {degradedFeatures.length > 0 && viewMode === 'snapshot' && (
        <div style={{
          padding: '8px 14px',
          borderRadius: 8,
          fontSize: 12,
          background: 'rgba(210,153,34,0.08)',
          color: '#d29922',
          border: '1px solid rgba(210,153,34,0.2)',
          lineHeight: 1.5,
        }}>
          Some features are limited on this server: {degradedFeatures.join(', ')}.
          <span style={{ color: 'var(--text-muted)', marginLeft: 6 }}>
            Check Engine Internals → Monitoring Capabilities for details.
          </span>
        </div>
      )}

      {viewMode === 'map' && (
        <div style={{ display: 'flex', alignItems: 'baseline', justifyContent: 'space-between', gap: 16, marginBottom: 4 }}>
          <div style={{ fontSize: 15, color: 'var(--text-secondary)', lineHeight: 1.4 }}>
            Explore system tables, queries, and metrics to help you understand what's going on in your cluster.
          </div>
          <input
            type="text"
            placeholder="Search..."
            value={mapSearchQuery}
            onChange={e => setMapSearchQuery(e.target.value)}
            className="obs-map-search-input"
          />
        </div>
      )}

      {/* Live Activity Stream — the hero */}
      {viewMode === 'snapshot' && (
        <>
          {/* Compact stat strip */}
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr 1fr 1fr', gap: 12 }}>
            <SparklineStatCard
              label={isCluster ? `CPU (${arenaHosts.length} nodes)` : 'CPU'}
              value={isCluster && ra ? `${clusterCpuPct.toFixed(1)}%` : metrics ? `${metrics.cpu_usage.toFixed(1)}%` : '—'}
              color="#3B82F6"
              warn={isCluster ? clusterCpuPct > (thresholds.cpu_warning ?? 80) : warnings.cpu}
              subtitle={isCluster && ra ? `${clusterCores} total cores` : warnings.cpu ? `Above ${thresholds.cpu_warning}%` : undefined}
              sparklineData={sparkCpu}
            />
            <SparklineStatCard
              label={isCluster ? `Memory (${arenaHosts.length} nodes)` : 'Memory'}
              value={isCluster && ra ? `${clusterMemPct.toFixed(1)}%` : metrics ? `${memoryPercentage.toFixed(1)}%` : '—'}
              color="#8B5CF6"
              warn={isCluster ? clusterMemPct > (thresholds.memory_warning ?? 85) : warnings.memory}
              subtitle={isCluster && ra ? `${formatBytes(ra.memory.totalRSS)} / ${formatBytes(ra.memory.totalRAM)}` : metrics ? `${formatBytes(metrics.memory_used)} / ${formatBytes(metrics.memory_total)}` : undefined}
              sparklineData={sparkMem}
            />
            <SparklineStatCard
              label={isCluster ? `Disk Read (cluster)` : 'Disk Read'}
              value={isCluster && ra ? formatBytes(ra.io.readBytesPerSec) + '/s' : metrics ? formatBytes(metrics.disk_read_bytes) : '—'}
              color="#10B981"
              sparklineData={sparkDiskR}
            />
            <SparklineStatCard
              label={isCluster ? `Disk Write (cluster)` : 'Disk Write'}
              value={isCluster && ra ? formatBytes(ra.io.writeBytesPerSec) + '/s' : metrics ? formatBytes(metrics.disk_write_bytes) : '—'}
              color="#F59E0B"
              sparklineData={sparkDiskW}
            />
            <SparklineStatCard
              label="Uptime"
              value={metrics?.uptime_seconds ? formatDuration(metrics.uptime_seconds) : '—'}
              color="#06b6d4"
              sparklineData={[]}
            />
          </div>

          {/* Resource arena — driven by global 2D/3D preference */}
          {preferredViewMode === '3d' ? (
            <ResourceArena3D
              key={`3d-${activeProfileId}-${servicesGenRef.current}`}
              queries={liveData?.runningQueries ?? EMPTY_QUERIES}
              merges={liveData?.activeMerges ?? EMPTY_MERGES}
              cpuUsage={isCluster ? clusterCpuPct : (metrics?.cpu_usage ?? 0)}
              memoryPct={isCluster ? clusterMemPct : memoryPercentage}
              onQueryClick={toggleExpandedQuery}
              splitAvailable={arenaHosts.length > 1}
              splitActive={arenaSplitView}
              onSplitToggle={() => setArenaSplitView(v => !v)}
            />
          ) : (
            <ResourceArenaSwimlane
              key={`swim-${activeProfileId}-${servicesGenRef.current}`}
              queries={liveData?.runningQueries ?? EMPTY_QUERIES}
              merges={liveData?.activeMerges ?? EMPTY_MERGES}
              cpuUsage={isCluster ? clusterCpuPct : (metrics?.cpu_usage ?? 0)}
              memoryPct={isCluster ? clusterMemPct : memoryPercentage}
              onQueryClick={toggleExpandedQuery}
              splitAvailable={arenaHosts.length > 1}
              splitActive={arenaSplitView}
              onSplitToggle={() => setArenaSplitView(v => !v)}
            />
          )}

          {/* Resource Attribution Bars */}
          {liveData?.resourceAttribution && (
            <div className="grid grid-cols-3 gap-4">
              <div className="card">
                <div className="card-body">
                  <ResourceAttributionBar
                    attribution={liveData.resourceAttribution}
                    selectedResource={selectedResource}
                    onResourceChange={setSelectedResource}
                    showOnly="cpu"
                  />
                </div>
              </div>
              <div className="card">
                <div className="card-body">
                  <ResourceAttributionBar
                    attribution={liveData.resourceAttribution}
                    selectedResource={selectedResource}
                    onResourceChange={setSelectedResource}
                    showOnly="memory"
                  />
                </div>
              </div>
              <div className="card">
                <div className="card-body">
                  <ResourceAttributionBar
                    attribution={liveData.resourceAttribution}
                    selectedResource={selectedResource}
                    onResourceChange={setSelectedResource}
                    showOnly="io"
                  />
                </div>
              </div>
            </div>
          )}
        </>
      )}

      {/* Stats for Trend/Metrics view (compact, no sparklines) */}
      {viewMode === 'trend' && (
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr 1fr 1fr', gap: 12 }}>
          <SparklineStatCard
            label={isCluster ? `CPU (${arenaHosts.length} nodes)` : 'CPU'}
            value={isCluster && ra ? `${clusterCpuPct.toFixed(1)}%` : metrics ? `${metrics.cpu_usage.toFixed(1)}%` : '—'}
            color="#3B82F6"
            warn={isCluster ? clusterCpuPct > (thresholds.cpu_warning ?? 80) : warnings.cpu}
            sparklineData={sparkCpu}
          />
          <SparklineStatCard
            label={isCluster ? `Memory (${arenaHosts.length} nodes)` : 'Memory'}
            value={isCluster && ra ? `${clusterMemPct.toFixed(1)}%` : metrics ? `${memoryPercentage.toFixed(1)}%` : '—'}
            color="#8B5CF6"
            warn={isCluster ? clusterMemPct > (thresholds.memory_warning ?? 85) : warnings.memory}
            sparklineData={sparkMem}
          />
          <SparklineStatCard
            label={isCluster ? `Disk Read (cluster)` : 'Disk Read'}
            value={isCluster && ra ? formatBytes(ra.io.readBytesPerSec) + '/s' : metrics ? formatBytes(metrics.disk_read_bytes) : '—'}
            color="#10B981"
            sparklineData={sparkDiskR}
          />
          <SparklineStatCard
            label={isCluster ? `Disk Write (cluster)` : 'Disk Write'}
            value={isCluster && ra ? formatBytes(ra.io.writeBytesPerSec) + '/s' : metrics ? formatBytes(metrics.disk_write_bytes) : '—'}
            color="#F59E0B"
            sparklineData={sparkDiskW}
          />
          <SparklineStatCard
            label="Uptime"
            value={metrics?.uptime_seconds ? formatDuration(metrics.uptime_seconds) : '—'}
            color="#06b6d4"
            sparklineData={[]}
          />
        </div>
      )}

      {/* Trend Chart */}
      {viewMode === 'trend' && (
        <div className="card">
          <div className="card-header flex items-center justify-between">
            <div className="flex items-center gap-3">
              <span>Metrics</span>
              {isLoadingHistory && (
                <span className="text-xs" style={{ color: 'var(--text-muted)' }}>Loading...</span>
              )}
              {!isLoadingHistory && chartData.length > 0 && (
                <span className="text-xs" style={{ color: 'var(--text-muted)' }}>
                  {chartData.length} points
                </span>
              )}
              <button
                onClick={fetchHistoricalMetrics}
                disabled={isLoadingHistory}
                style={{
                  padding: '2px 8px',
                  fontSize: '11px',
                  borderRadius: '4px',
                  border: 'none',
                  cursor: isLoadingHistory ? 'not-allowed' : 'pointer',
                  background: 'var(--bg-tertiary)',
                  color: 'var(--text-secondary)',
                }}
              >
                ↻
              </button>
            </div>
            {/* Host selector — only show if multiple hosts */}
            <div className="flex items-center gap-3">
              {clusterHosts.length > 1 && (
                <div className="tabs">
                  <button
                    className={`tab ${selectedHost === null && !splitView ? 'active' : ''}`}
                    onClick={() => { setSelectedHost(null); setSplitView(false); }}
                  >
                    All
                  </button>
                  <button
                    className={`tab ${splitView ? 'active' : ''}`}
                    onClick={() => { setSelectedHost(null); setSplitView(!splitView); }}
                    title="Split view: one chart per server, stacked vertically"
                    style={{ display: 'flex', alignItems: 'center', gap: 4 }}
                  >
                    <svg width="10" height="10" viewBox="0 0 10 10" style={{ opacity: 0.7 }}>
                      <rect x="0" y="0" width="10" height="4" rx="1" fill="currentColor" />
                      <rect x="0" y="6" width="10" height="4" rx="1" fill="currentColor" />
                    </svg> Split
                  </button>
                  {clusterHosts.map(host => (
                    <button
                      key={host}
                      className={`tab ${selectedHost === host ? 'active' : ''}`}
                      onClick={() => { setSelectedHost(host); setSplitView(false); }}
                    >
                      <TruncatedHost name={host} />
                    </button>
                  ))}
                </div>
              )}
              {/* Time Range Selector */}
              <div className="tabs">
                {[
                  { label: '5m', minutes: 5 },
                  { label: '15m', minutes: 15 },
                  { label: '1h', minutes: 60 },
                  { label: '6h', minutes: 360 },
                  { label: '24h', minutes: 1440 },
                ].map(({ label, minutes }) => (
                  <button
                    key={label}
                    className={`tab ${timeRangeMinutes === minutes ? 'active' : ''}`}
                    onClick={() => setTimeRangeMinutes(minutes)}
                  >
                    {label}
                  </button>
                ))}
              </div>
            </div>
          </div>
          <div className="card-body">
            {historyError ? (
              <div className="text-center py-10" style={{ color: 'var(--text-muted)' }}>
                {historyError}
              </div>
            ) : chartData.length === 0 && !isLoadingHistory ? (
              <div className="text-center py-10" style={{ color: 'var(--text-muted)' }}>
                No historical data available for this time range
              </div>
            ) : splitView && clusterHosts.length > 1 ? (
              <div>
                <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                  {clusterHosts.map(host => {
                    const hostData = perHostChartData.get(host) ?? [];
                    return (
                      <div key={host} style={{ position: 'relative', borderRadius: 8, overflow: 'hidden', border: '1px solid var(--border-primary)', background: 'var(--bg-secondary)' }}>
                        <button
                          onClick={() => { setSelectedHost(host); setSplitView(false); }}
                          title={`Switch to ${host}`}
                          style={{
                            position: 'absolute', top: 6, left: 10, zIndex: 10, fontSize: 10, fontWeight: 600,
                            color: 'var(--text-secondary)', background: 'var(--bg-tertiary)', padding: '2px 8px',
                            borderRadius: 4, border: '1px solid var(--border-primary)', opacity: 0.9,
                            cursor: 'pointer',
                          }}
                        >
                          <TruncatedHost name={host} />
                        </button>
                        <TimeSeriesChart
                          data={hostData}
                          selectedMetrics={selectedMetrics}
                          height={200}
                          stats={undefined}
                          onToggleMetric={toggleMetric}
                        />
                      </div>
                    );
                  })}
                </div>
                {/* Shared metric filter for split view */}
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4, padding: '8px 0' }}>
                  {([
                    { key: 'cpu_usage' as TrendMetricType, label: 'CPU Usage', color: '#3B82F6' },
                    { key: 'memory_percentage' as TrendMetricType, label: 'Memory Usage', color: '#8B5CF6' },
                    { key: 'disk_read_rate' as TrendMetricType, label: 'Disk Read', color: '#10B981' },
                    { key: 'disk_write_rate' as TrendMetricType, label: 'Disk Write', color: '#F59E0B' },
                    { key: 'network_send_rate' as TrendMetricType, label: 'Net Send', color: '#EC4899' },
                    { key: 'network_recv_rate' as TrendMetricType, label: 'Net Recv', color: '#14B8A6' },
                  ]).map(({ key, label, color }) => {
                    const isSelected = selectedMetrics.includes(key);
                    return (
                      <button
                        key={key}
                        onClick={() => { if (isSelected && selectedMetrics.length === 1) return; toggleMetric(key); }}
                        style={{
                          display: 'flex', alignItems: 'center', gap: 8, fontSize: 12,
                          padding: '6px 12px', borderRadius: 6, border: 'none', cursor: 'pointer',
                          background: isSelected ? 'var(--bg-tertiary)' : 'transparent',
                          opacity: isSelected ? 1 : 0.4, transition: 'all 0.2s',
                        }}
                      >
                        <div style={{ width: 8, height: 8, borderRadius: '50%', backgroundColor: color, flexShrink: 0 }} />
                        <span style={{ color: 'var(--text-primary)', fontWeight: 500 }}>{label}</span>
                      </button>
                    );
                  })}
                </div>
              </div>
            ) : (
              <TimeSeriesChart
                data={chartData}
                selectedMetrics={selectedMetrics}
                height={300}
                stats={trendStats}
                onToggleMetric={toggleMetric}
                hosts={isMultiHostView ? clusterHosts : undefined}
              />
            )}
          </div>
        </div>
      )}


      {/* Observability Map */}
      {viewMode === 'map' && (
        <div style={{ display: 'flex', flex: 1, minHeight: 0, overflow: 'hidden', marginTop: -8, marginLeft: -24, marginRight: -24, marginBottom: -24 }}>
          <div style={{ flex: 1, position: 'relative', minWidth: 0, overflow: 'hidden' }}>
            <ObservabilitySunburst
              searchQuery={mapSearchQuery}
              selectedNode={mapClickedNode}
              onHoverNode={handleMapHoverNode}
              onSelectNode={handleMapSelectNode}
              enrichedData={enrichedMapData ?? undefined}
            />
          </div>
          <DetailSidebar
            selectedNode={mapHoveredNode ?? mapClickedNode}
            queryResult={mapQueryResult}
            runQueryIndex={mapRunQueryIndex}
            isQueryRunning={mapQueryRunning}
            onRunQuery={handleRunDiagnosticQuery}
            enrichedData={enrichedMapData ?? undefined}
            columnComments={columnComments}
            expanded={mapDetailExpanded}
            onToggleExpand={() => setMapDetailExpanded(v => !v)}
          />
        </div>
      )}
    </div>
  );
};

export default Overview;
