import React, { useEffect, useRef, useCallback, useState, useMemo } from 'react';
import { useConnectionStore } from '../stores/connectionStore';
import { useQueryStore, QueryWebSocket, queryApi } from '../stores/queryStore';
import { QueryRunningTable } from '../components/query/QueryRunningTable';
import { QueryHistoryTable } from '../components/query/QueryHistoryTable';
import { QueryDetailModal } from '../components/query/modal/QueryDetailModal';
import { useClickHouseServices } from '../providers/ClickHouseProvider';
import { useRefreshConfig, clampToAllowed } from '@tracehouse/ui-shared';
import { useRefreshSettingsStore } from '../stores/refreshSettingsStore';
import { useGlobalLastUpdatedStore } from '../stores/refreshSettingsStore';
import { useCapabilityCheck } from '../components/shared/RequiresCapability';
import { PermissionGate } from '../components/shared/PermissionGate';
import { BackLink } from '../components/common/BackLink';
import { DocsLink } from '../components/common/DocsLink';
import { useLocation } from 'react-router-dom';
import type { QuerySeries, QueryConcurrency } from '@tracehouse/core';
import { OverviewService } from '@tracehouse/core';
import { useUserPreferenceStore } from '../stores/userPreferenceStore';
import { QueryHealthSunburst } from '../components/query/QueryHealthSunburst';
import { useUrlState } from '../hooks/useUrlState';
import type { UrlSchema } from '../hooks/useUrlState';

// Query type colors matching QueryRunningTable
const QUERY_TYPE_COLORS: Record<string, string> = {
  Select: '#3b82f6',
  Insert: '#f59e0b',
  Alter: '#ef4444',
  Create: '#22c55e',
  Drop: '#f43f5e',
  System: '#8b5cf6',
  Optimize: '#06b6d4',
  Other: '#94a3b8',
};

// All query types to always show
const ALL_QUERY_TYPES = ['Select', 'Insert', 'Alter', 'Create', 'Drop', 'System', 'Optimize', 'Other'];

// Query Type Card - matches StatCard style from MergeTracker
const QueryTypeCard: React.FC<{
  type: string;
  count: number;
  color: string;
}> = ({ type, count, color }) => (
  <div className="stat-card" style={{ flex: 1, minWidth: 100 }}>
    <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
      <div style={{ width: 10, height: 10, borderRadius: 3, background: color, flexShrink: 0 }} />
      <div>
        <div className="stat-value">{count}</div>
        <div className="stat-label">{type}</div>
      </div>
    </div>
  </div>
);

// URL schema for shareable query monitor links
const queryMonitorSchema = {
  tab:       { type: 'string',  default: 'running' },
  qd_id:     { type: 'string' },
  user:      { type: 'string' },
  queryId:   { type: 'string' },
  queryText: { type: 'string' },
  queryKind: { type: 'string' },
  status:    { type: 'string' },
  database:  { type: 'string' },
  tableName: { type: 'string' },
  hostname:  { type: 'string' },
  minDurMs:  { type: 'number' },
  minMemB:   { type: 'number' },
  limit:     { type: 'number',  default: 100 },
  sortField: { type: 'string',  default: 'query_start_time' },
  sortDir:   { type: 'string',  default: 'desc' },
} as const satisfies UrlSchema;

export const QueryMonitor: React.FC = () => {
  const { activeProfileId, profiles, setConnectionFormOpen } = useConnectionStore();
  const { runningQueries, queryHistory, selectedQuery, selectedQueryType: _selectedQueryType, historyFilter, historySort, wsStatus, error, isLoadingHistory, isKillingQuery, setRunningQueries, setQueryHistory, selectQuery, setHistoryFilter, setHistorySort, setIsLoadingHistory, setIsKillingQuery, setError, clearError, clearQueries } = useQueryStore();
  const location = useLocation();
  const locationState = location.state as { tab?: 'running' | 'history'; filter?: Record<string, unknown> } | null;
  const experimentalEnabled = useUserPreferenceStore(s => s.experimentalEnabled);

  // URL-synced state for shareable links
  const { state: urlState, update: updateUrl } = useUrlState(queryMonitorSchema);
  const activeTab = (locationState?.tab || urlState.tab || 'running') as 'running' | 'history' | 'health';
  const setActiveTab = useCallback((tab: 'running' | 'history' | 'health') => {
    updateUrl({ tab }, { push: true });
  }, [updateUrl]);

  // Hydrate store from URL params on mount
  const urlHydrated = useRef(false);
  useEffect(() => {
    if (urlHydrated.current) return;
    urlHydrated.current = true;
    // Navigation state (from Overview widgets) takes precedence over URL
    const navFilter = locationState?.filter as Record<string, unknown> | undefined;
    const patch: Record<string, unknown> = {};
    if (navFilter) {
      Object.assign(patch, navFilter);
    } else {
      if (urlState.user) patch.user = urlState.user;
      if (urlState.queryId) patch.queryId = urlState.queryId;
      if (urlState.queryText) patch.queryText = urlState.queryText;
      if (urlState.queryKind) patch.queryKind = urlState.queryKind;
      if (urlState.status) patch.status = urlState.status;
      if (urlState.database) patch.database = urlState.database;
      if (urlState.tableName) patch.table = urlState.tableName;
      if (urlState.hostname) patch.hostname = urlState.hostname;
      if (urlState.minDurMs) patch.minDurationMs = urlState.minDurMs;
      if (urlState.minMemB) patch.minMemoryBytes = urlState.minMemB;
      if (urlState.limit && urlState.limit !== 100) patch.limit = urlState.limit;
    }
    if (Object.keys(patch).length > 0) setHistoryFilter(patch as any);
    if (urlState.sortField || urlState.sortDir) {
      setHistorySort({
        field: (urlState.sortField || 'query_start_time') as any,
        direction: (urlState.sortDir || 'desc') as any,
      });
    }
  }, []);

  // Wrap filter/sort changes to sync back to URL
  const handleFilterChange = useCallback((filter: Record<string, unknown>) => {
    setHistoryFilter(filter as any);
    const urlPatch: Record<string, unknown> = {};
    if ('user' in filter) urlPatch.user = filter.user || undefined;
    if ('queryId' in filter) urlPatch.queryId = filter.queryId || undefined;
    if ('queryText' in filter) urlPatch.queryText = filter.queryText || undefined;
    if ('queryKind' in filter) urlPatch.queryKind = filter.queryKind || undefined;
    if ('status' in filter) urlPatch.status = filter.status || undefined;
    if ('database' in filter) urlPatch.database = filter.database || undefined;
    if ('table' in filter) urlPatch.tableName = filter.table || undefined;
    if ('hostname' in filter) urlPatch.hostname = filter.hostname || undefined;
    if ('minDurationMs' in filter) urlPatch.minDurMs = filter.minDurationMs || undefined;
    if ('minMemoryBytes' in filter) urlPatch.minMemB = filter.minMemoryBytes || undefined;
    if ('limit' in filter) urlPatch.limit = filter.limit;
    if (Object.keys(urlPatch).length > 0) updateUrl(urlPatch as any);
  }, [setHistoryFilter, updateUrl]);

  const handleSortChange = useCallback((sort: { field: string; direction: string }) => {
    setHistorySort(sort as any);
    updateUrl({ sortField: sort.field, sortDir: sort.direction } as any);
  }, [setHistorySort, updateUrl]);

  const wsRef = useRef<QueryWebSocket | null>(null);
  const services = useClickHouseServices();
  const refreshConfig = useRefreshConfig();
  const { refreshRateSeconds } = useRefreshSettingsStore();
  const manualRefreshTick = useGlobalLastUpdatedStore(s => s.manualRefreshTick);
  const { available: hasQueryLog, probing: isProbing } = useCapabilityCheck(['query_log']);
  const { available: hasProcesses, probing: isProcessesProbing } = useCapabilityCheck(['system_processes']);

  const [historyCoordinatorIds, setHistoryCoordinatorIds] = useState<Set<string>>(new Set());
  const [runningFilteredCount, setRunningFilteredCount] = useState<number | null>(null);
  const [concurrency, setConcurrency] = useState<QueryConcurrency | null>(null);

  // Derive coordinator IDs from the already-fetched running queries
  // (eliminates the separate RUNNING_COORDINATOR_IDS query)
  const runningCoordinatorIds = useMemo(() => {
    const ids = new Set<string>();
    for (const q of runningQueries) {
      if (!q.is_initial_query && q.initial_query_id) {
        ids.add(q.initial_query_id);
      }
    }
    return ids;
  }, [runningQueries]);

  const slotPct = concurrency && concurrency.maxConcurrent > 0
    ? (concurrency.running / concurrency.maxConcurrent) * 100
    : 0;

  let activeProfile = profiles.find(p => p.id === activeProfileId);
  if (!activeProfile && profiles.length > 0) activeProfile = profiles.find(p => p.is_connected);
  const isConnected = activeProfile?.is_connected ?? false;

  const fetchHistory = useCallback(async () => {
    if (!services || !isConnected) return;
    setIsLoadingHistory(true); clearError();
    try {
      const h = await queryApi.fetchQueryHistory(services.queryAnalyzer, historyFilter);
      setQueryHistory(h);
      // Fetch coordinator IDs scoped to the returned query IDs
      const queryIds = h.map(q => q.query_id);
      const startDate = h.length > 0
        ? String(h[h.length - 1].query_start_time).slice(0, 10)
        : new Date().toISOString().slice(0, 10);
      services.queryAnalyzer.getCoordinatorIds(queryIds, startDate!).then(setHistoryCoordinatorIds).catch(() => {});
    }
    catch (e) { setError(e instanceof Error ? e.message : 'Failed'); }
    finally { setIsLoadingHistory(false); }
  }, [services, isConnected, historyFilter]);

  const handleKillQuery = useCallback(async (qid: string) => {
    if (!services) return;
    setIsKillingQuery(true);
    try { const r = await queryApi.killQuery(services.queryAnalyzer, qid); if (r.success) setRunningQueries(runningQueries.filter(q => q.query_id !== qid)); }
    catch (e) { setError(e instanceof Error ? e.message : 'Failed'); }
    finally { setIsKillingQuery(false); }
  }, [services, runningQueries]);

  // Convert query store format to QuerySeries format for TimelineQueryModal
  const convertToQuerySeries = useCallback((query: any): QuerySeries | null => {
    if (!query) return null;
    
    // Handle QueryHistoryItem (from history tab) - has query_start_time, query_duration_ms, memory_usage
    // Handle RunningQuery (from running tab) - has elapsed_seconds, memory_usage
    const isHistoryItem = 'query_start_time' in query;
    const isRunningQuery = 'elapsed_seconds' in query;
    
    let startTime: string;
    let endTime: string;
    let durationMs: number;
    
    if (isHistoryItem) {
      startTime = query.query_start_time;
      durationMs = query.query_duration_ms || 0;
      // Calculate end time from start + duration
      const startDate = new Date(startTime);
      endTime = new Date(startDate.getTime() + durationMs).toISOString();
    } else if (isRunningQuery) {
      // Running query - use current time as reference
      const now = new Date();
      durationMs = Math.round((query.elapsed_seconds || 0) * 1000);
      endTime = now.toISOString();
      startTime = new Date(now.getTime() - durationMs).toISOString();
    } else {
      startTime = query.start_time || new Date().toISOString();
      endTime = query.end_time || new Date().toISOString();
      durationMs = query.duration_ms || 0;
    }
    
    return {
      query_id: query.query_id,
      user: query.user || 'default',
      label: query.query || '',
      start_time: startTime,
      end_time: endTime,
      duration_ms: durationMs,
      peak_memory: query.memory_usage || query.peak_memory || 0,
      cpu_us: query.cpu_time_us || query.cpu_us || 0,
      net_send: query.network_send_bytes || query.net_send || 0,
      net_recv: query.network_receive_bytes || query.net_recv || 0,
      disk_read: query.disk_read_bytes || query.disk_read || query.read_bytes || 0,
      disk_write: query.disk_write_bytes || query.disk_write || 0,
      status: query.type || query.status,
      exception_code: query.exception_code,
      exception: query.exception,
      is_running: isRunningQuery,
      points: [],
    };
  }, []);

  const convertedQuery = convertToQuerySeries(selectedQuery);

  // Deep-link: sync selected query to/from URL (qd_id param) through the
  // same updateUrl path as every other param — avoids race conditions from
  // competing setSearchParams calls.
  const [deepLinkedQuery, setDeepLinkedQuery] = useState<QuerySeries | null>(null);
  const deepLinkFetched = useRef('');

  // When user selects a query, write qd_id to URL
  useEffect(() => {
    if (!convertedQuery) return;
    deepLinkFetched.current = convertedQuery.query_id;
    setDeepLinkedQuery(null);
    updateUrl({ qd_id: convertedQuery.query_id } as any);
  }, [convertedQuery?.query_id, updateUrl]);

  // On mount: if qd_id is in URL but no query selected, fetch the detail
  useEffect(() => {
    const qdId = urlState.qd_id;
    if (!qdId || convertedQuery || !services) return;
    if (deepLinkFetched.current === qdId) return;
    deepLinkFetched.current = qdId;
    services.queryAnalyzer.getQueryDetail(qdId).then((detail: any) => {
      if (!detail) return;
      const durationMs = Number(detail.query_duration_ms) || 0;
      const startMs = new Date(detail.query_start_time).getTime();
      setDeepLinkedQuery({
        query_id: detail.query_id,
        label: detail.query || '',
        user: detail.user,
        peak_memory: Number(detail.memory_usage) || 0,
        duration_ms: durationMs,
        cpu_us: (detail.ProfileEvents?.['UserTimeMicroseconds'] || 0) + (detail.ProfileEvents?.['SystemTimeMicroseconds'] || 0),
        net_send: detail.ProfileEvents?.['NetworkSendBytes'] || 0,
        net_recv: detail.ProfileEvents?.['NetworkReceiveBytes'] || 0,
        disk_read: Number(detail.read_bytes) || 0,
        disk_write: detail.ProfileEvents?.['OSWriteBytes'] || 0,
        start_time: detail.query_start_time,
        end_time: new Date(startMs + durationMs).toISOString(),
        exception_code: detail.exception_code,
        exception: detail.exception,
        points: [],
      });
    }).catch((err: any) => {
      console.error(`[QueryMonitor] Failed to fetch qd_id=${qdId}:`, err);
    });
  }, [urlState.qd_id, convertedQuery, services, updateUrl]);

  const modalQuery = convertedQuery ?? deepLinkedQuery;
  const handleQueryClose = useCallback(() => {
    selectQuery(null, null);
    setDeepLinkedQuery(null);
    deepLinkFetched.current = '';
    updateUrl({ qd_id: undefined } as any);
  }, [selectQuery, updateUrl]);

  // Compute query type counts for running queries
  const queryTypeCounts = useMemo(() => {
    const counts: Record<string, number> = {};
    // Initialize all types with 0
    ALL_QUERY_TYPES.forEach(type => { counts[type] = 0; });
    // Count running queries
    runningQueries.forEach(q => {
      const kind = q.query_kind || 'Other';
      // Capitalize first letter to match our keys
      const normalizedKind = kind.charAt(0).toUpperCase() + kind.slice(1).toLowerCase();
      if (ALL_QUERY_TYPES.includes(normalizedKind)) {
        counts[normalizedKind]++;
      } else {
        counts['Other']++;
      }
    });
    return counts;
  }, [runningQueries]);

  const statsIntervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  useEffect(() => {
    if (wsRef.current) { wsRef.current.disconnect(); wsRef.current = null; }
    if (statsIntervalRef.current) { clearInterval(statsIntervalRef.current); statsIntervalRef.current = null; }
    if (!services || !isConnected) { clearQueries(); return; }
    const queryIntervalMs = refreshRateSeconds > 0 ? clampToAllowed(Math.max(2, refreshRateSeconds), refreshConfig) * 1000 : 2000;
    const statsIntervalMs = refreshRateSeconds > 0 ? clampToAllowed(refreshRateSeconds, refreshConfig) * 1000 : 5000;
    wsRef.current = new QueryWebSocket(services.queryAnalyzer, queryIntervalMs);
    if (refreshRateSeconds > 0) wsRef.current.connect();
    // Lightweight stats poller — single query for concurrency/QPS/rejected
    const overviewService = new OverviewService(services.adapter, {}, services.environmentDetector);
    const pollStats = () => {
      overviewService.getQueryMonitorStats().then(setConcurrency).catch(() => {});
    };
    pollStats();
    if (refreshRateSeconds > 0) statsIntervalRef.current = setInterval(pollStats, statsIntervalMs);
    if (hasQueryLog) fetchHistory();
    return () => {
      if (wsRef.current) wsRef.current.disconnect();
      if (statsIntervalRef.current) clearInterval(statsIntervalRef.current);
    };
  }, [services, isConnected, refreshRateSeconds, refreshConfig, manualRefreshTick]);

  // Fetch history when filter changes or when hasQueryLog becomes available
  useEffect(() => {
    if (services && isConnected && hasQueryLog) fetchHistory();
  }, [historyFilter, fetchHistory, hasQueryLog]);

  if (!activeProfile?.id || !isConnected) {
    return (
      <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', height: '100%', gap: 16, background: 'var(--bg-primary)' }}>
        <div style={{ color: 'var(--text-primary)', fontSize: 18, fontWeight: 600 }}>Query Monitor</div>
        <div style={{ color: 'var(--text-muted)', fontSize: 13 }}>Connect to a ClickHouse server to monitor queries.</div>
        <button onClick={() => setConnectionFormOpen(true)}
          style={{ marginTop: 8, padding: '8px 20px', borderRadius: 6, border: 'none', background: 'var(--accent-primary, #58a6ff)', color: '#fff', fontSize: 13, fontWeight: 600, cursor: 'pointer' }}>
          Add Connection
        </button>
      </div>
    );
  }

  const tabs: { key: 'running' | 'history' | 'health'; label: string; count?: number; badge?: string }[] = [
    { key: 'running', label: 'Running', count: runningFilteredCount ?? runningQueries.length },
    { key: 'history', label: 'History', count: queryHistory.length },
    ...(experimentalEnabled ? [{ key: 'health' as const, label: 'Health Map', badge: 'exp' }] : []),
  ];

  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column', overflow: 'hidden', background: 'var(--bg-primary)' }}>
      {/* Header */}
      <div style={{ padding: '16px 24px 0', flexShrink: 0, background: 'var(--bg-secondary)', borderBottom: '1px solid var(--border-primary)' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 12 }}>
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
              <h2 style={{ color: 'var(--text-primary)', fontSize: 18, fontWeight: 600, margin: 0 }}>Query Monitor</h2>
              <DocsLink path="/features/query-monitor" />
              <BackLink />
            </div>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            {wsStatus === 'connected' && (
              <>
                <div style={{
                  width: 7, height: 7, borderRadius: '50%',
                  background: '#3fb950',
                }} />
                <span style={{ color: '#3fb950', fontSize: 11 }}>live</span>
              </>
            )}
          </div>
        </div>
        
        {/* Query Concurrency + QPS — 4 equal cards */}
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr 1fr', gap: 12, marginBottom: 12 }}>
          {/* 1. QPS Sparkline */}
          <div className="stat-card" style={{ padding: '8px 12px' }}>
            {(() => {
              const points = concurrency?.qpsHistory ?? [];
              if (points.length < 2) return (
                <div>
                  <div className="stat-label" style={{ margin: 0 }}>Queries / sec</div>
                  <div className="stat-value">—</div>
                </div>
              );
              const values = points.map(p => p.qps);
              const maxVal = Math.max(...values, 1);
              const w = 300;
              const h = 36;
              const pad = 1;
              const stepX = (w - pad * 2) / (values.length - 1);
              const pathD = values
                .map((v, i) => {
                  const x = pad + i * stepX;
                  const y = h - pad - ((v / maxVal) * (h - pad * 2));
                  return `${i === 0 ? 'M' : 'L'}${x.toFixed(1)},${y.toFixed(1)}`;
                })
                .join(' ');
              const areaD = `${pathD} L${(pad + (values.length - 1) * stepX).toFixed(1)},${h} L${pad},${h} Z`;
              const latest = values[values.length - 1];
              return (
                <div>
                  <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 4 }}>
                    <div className="stat-label" style={{ margin: 0 }}>Queries / sec (15m)</div>
                    <span style={{ fontSize: 12, fontWeight: 600, fontFamily: 'var(--font-mono, monospace)', color: 'var(--text-primary)' }}>
                      {latest.toFixed(1)} q/s
                    </span>
                  </div>
                  <svg width="100%" height={h} viewBox={`0 0 ${w} ${h}`} preserveAspectRatio="none" style={{ display: 'block' }}>
                    <path d={areaD} fill="rgba(59,130,246,0.1)" />
                    <path d={pathD} fill="none" stroke="#3b82f6" strokeWidth="1.5" strokeLinejoin="round" />
                  </svg>
                </div>
              );
            })()}
          </div>
          {/* 2. Concurrency Slots */}
          <div className="stat-card">
            <div>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 4 }}>
                <div className="stat-label" style={{ margin: 0 }}>Concurrency Slots</div>
                <span style={{ fontSize: 12, fontWeight: 600, fontFamily: 'var(--font-mono, monospace)', color: slotPct > 80 ? '#ef4444' : 'var(--text-primary)' }}>
                  {concurrency ? `${concurrency.running} / ${concurrency.maxConcurrent}` : '— / —'}
                </span>
              </div>
              <div style={{ width: '100%', height: 6, borderRadius: 3, background: 'var(--bg-tertiary)' }}>
                <div style={{
                  width: `${Math.min(slotPct, 100)}%`,
                  height: '100%',
                  borderRadius: 3,
                  background: slotPct > 80 ? '#ef4444' : '#3b82f6',
                  transition: 'width 0.3s ease',
                }} />
              </div>
            </div>
          </div>
          {/* 3. Queued */}
          <div className="stat-card">
            <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
              <div style={{ width: 10, height: 10, borderRadius: 3, background: concurrency && concurrency.queued > 0 ? '#ef4444' : '#3fb950', flexShrink: 0 }} />
              <div>
                <div className="stat-value">{concurrency ? concurrency.queued : '—'}</div>
                <div className="stat-label">Queued</div>
              </div>
            </div>
          </div>
          {/* 4. Rejected */}
          <div className="stat-card">
            <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
              <div style={{ width: 10, height: 10, borderRadius: 3, background: concurrency && concurrency.rejectedRecent > 0 ? '#ef4444' : '#3fb950', flexShrink: 0 }} />
              <div>
                <div className="stat-value">{concurrency ? concurrency.rejectedRecent : '—'}</div>
                <div className="stat-label">Rejected (1h)</div>
              </div>
            </div>
          </div>
        </div>

        {/* Query Type Summary Cards - always show all types */}
        <div style={{ display: 'flex', gap: 12, marginBottom: 16 }}>
          {ALL_QUERY_TYPES.map(type => (
            <QueryTypeCard
              key={type}
              type={type}
              count={queryTypeCounts[type] || 0}
              color={QUERY_TYPE_COLORS[type]}
            />
          ))}
        </div>

        {/* Tabs */}
        <div style={{ display: 'flex', gap: 0 }}>
          {tabs.map(tab => {
            const active = activeTab === tab.key;
            return (
              <button key={tab.key} onClick={() => setActiveTab(tab.key)}
                style={{
                  padding: '8px 16px', fontSize: 12, fontWeight: active ? 600 : 400,
                  color: active ? 'var(--text-primary)' : 'var(--text-muted)',
                  background: 'transparent', border: 'none', cursor: 'pointer',
                  borderBottom: active ? '2px solid #58a6ff' : '2px solid transparent',
                  transition: 'all 0.15s',
                }}>
                {tab.label}
                {tab.count !== undefined && (
                  <span style={{
                    marginLeft: 6, fontSize: 10, padding: '1px 6px', borderRadius: 8,
                    background: active ? 'rgba(88,166,255,0.15)' : 'var(--bg-tertiary)',
                    color: active ? '#58a6ff' : 'var(--text-muted)',
                  }}>
                    {tab.count}
                  </span>
                )}
                {tab.badge && (
                  <span style={{ marginLeft: 6, fontSize: 9, padding: '1px 5px', borderRadius: 4, background: 'rgba(245,158,11,0.2)', color: '#f59e0b', fontWeight: 600 }}>
                    {tab.badge}
                  </span>
                )}
              </button>
            );
          })}
        </div>
      </div>

      {error && (
        <div style={{ margin: '12px 24px 0' }}>
          <PermissionGate error={error} title="Query Monitor" variant="banner" onDismiss={clearError} />
        </div>
      )}

      {/* Degradation banner for running queries */}
      {activeTab === 'running' && !isProcessesProbing && !hasProcesses && (
        <div style={{ margin: '12px 24px 0' }}>
          <PermissionGate
            error="Insufficient privileges to access system.processes. Ask your administrator to grant SELECT on this table."
            title="Running Queries"
            variant="banner"
          />
        </div>
      )}

      {/* Content */}
      <div style={{ flex: 1, display: 'flex', flexDirection: 'column', minHeight: 0, overflow: 'hidden' }}>
        {activeTab === 'health' ? (
          <QueryHealthSunburst
            runningQueries={runningQueries}
            recentHistory={queryHistory}
            concurrency={concurrency ?? null}
            onQueryClick={(queryId) => {
              const running = runningQueries.find(q => q.query_id === queryId);
              if (running) { selectQuery(running, 'running'); return; }
              const history = queryHistory.find(q => q.query_id === queryId);
              if (history) { selectQuery(history, 'history'); return; }
              return 'not-found';
            }}
          />
        ) : (
        <div style={{
          flex: 1,
          overflow: 'auto', padding: '0 24px',
        }}>
          <div style={{ padding: '12px 0' }}>
            {activeTab === 'running' ? (
              <QueryRunningTable queries={runningQueries} selectedQueryId={selectedQuery?.query_id || null}
                onSelectQuery={q => selectQuery(q, 'running')} onKillQuery={handleKillQuery} isKillingQuery={isKillingQuery} coordinatorIds={runningCoordinatorIds} queryAnalyzer={services?.queryAnalyzer} onFilteredCountChange={setRunningFilteredCount} />
            ) : hasQueryLog || isProbing ? (
              <QueryHistoryTable history={queryHistory} selectedQueryId={selectedQuery?.query_id || null}
                onSelectQuery={q => selectQuery(q, 'history')} filter={historyFilter} sort={historySort}
                onFilterChange={handleFilterChange} onSortChange={handleSortChange} isLoading={isLoadingHistory}
                queryAnalyzer={services?.queryAnalyzer} coordinatorIds={historyCoordinatorIds} />
            ) : (
              <PermissionGate
                error="system.query_log is not available on this server. Query History requires query logging to be enabled."
                title="Query History"
                variant="page"
              />
            )}
          </div>
        </div>
        )}
      </div>

      {/* Query Detail Modal */}
      <QueryDetailModal
        query={modalQuery}
        onClose={handleQueryClose}
      />
    </div>
  );
};

export default QueryMonitor;
