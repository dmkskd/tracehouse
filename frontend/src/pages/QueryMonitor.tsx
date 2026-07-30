import React, { useEffect, useRef, useCallback, useState, useMemo } from 'react';
import { useConnectionStore } from '../stores/connectionStore';
import { useQueryStore, QueryWebSocket, queryApi } from '../stores/queryStore';
import { QueryActivityTable } from '../components/query/QueryActivityTable';
import { QueryFilterBar } from '../components/query/QueryFilterBar';
import {
  buildQueryActivityRecords,
  queryActivityKey,
  querySelectionToSeries,
} from '../components/query/query-activity-model';
import { QueryDetailModal } from '../components/query/modal/QueryDetailModal';
import { useClickHouseServices } from '../providers/ClickHouseProvider';
import { useRefreshConfig, clampToAllowed } from '@tracehouse/ui-shared';
import { useRefreshSettingsStore } from '../stores/refreshSettingsStore';
import { useGlobalLastUpdatedStore } from '../stores/refreshSettingsStore';
import { useCapabilityCheck } from '../components/shared/RequiresCapability';
import { PermissionGate } from '../components/shared/PermissionGate';
import { BackLink } from '../components/common/BackLink';
import { DocsLink } from '../components/common/DocsLink';
import {
  MetricStrip,
  MetricStripDivider,
  MetricStripItem,
} from '../components/common/MetricStrip';
import { useLocation } from 'react-router-dom';
import type { QuerySeries, QueryConcurrency } from '@tracehouse/core';
import { OverviewService } from '@tracehouse/core';
import { useUserPreferenceStore } from '../stores/userPreferenceStore';
import { QueryHealthSunburst } from '../components/query/QueryHealthSunburst';
import { useUrlState } from '../hooks/useUrlState';
import type { UrlSchema } from '../hooks/useUrlState';

// Query type colors shared by the activity table and summary strip
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

// URL schema for shareable query monitor links
const queryMonitorSchema = {
  tab:       { type: 'string',  default: 'activity' },
  qd_id:     { type: 'string' },
  user:      { type: 'string[]' },
  queryId:   { type: 'string[]' },
  queryText: { type: 'string' },
  queryKind: { type: 'string[]' },
  status:    { type: 'string[]' },
  quickFilter: { type: 'string' },
  database:  { type: 'string[]' },
  tableName: { type: 'string[]' },
  hostname:  { type: 'string[]' },
  minDurMs:  { type: 'number' },
  minMemB:   { type: 'number' },
  limit:     { type: 'number',  default: 100 },
  sortField: { type: 'string',  default: 'query_start_time' },
  sortDir:   { type: 'string',  default: 'desc' },
} as const satisfies UrlSchema;

const QUERY_MULTI_FILTER_KEYS = [
  'user', 'queryId', 'queryKind', 'status', 'database', 'table', 'hostname',
] as const;

function normalizeQueryFilterArrays(filter: Record<string, unknown>): void {
  QUERY_MULTI_FILTER_KEYS.forEach(key => {
    const value = filter[key];
    if (typeof value === 'string' && value) filter[key] = [value];
  });
}

const nonEmptyValues = (value: unknown): unknown =>
  Array.isArray(value) && value.length === 0 ? undefined : value;

export const QueryMonitor: React.FC = () => {
  const { activeProfileId, profiles, setConnectionFormOpen } = useConnectionStore();
  const { runningQueries, queryHistory, selectedQuery, selectedQueryType, historyFilter, historySort, wsStatus, error, isLoadingHistory, isKillingQuery, setRunningQueries, setQueryHistory, selectQuery, setHistoryFilter, setHistorySort, setIsLoadingHistory, setIsKillingQuery, setError, clearError, clearQueries } = useQueryStore();
  const location = useLocation();
  const locationState = location.state as { tab?: 'activity' | 'running' | 'history'; filter?: Record<string, unknown> } | null;
  const experimentalEnabled = useUserPreferenceStore(s => s.experimentalEnabled);

  // URL-synced state for shareable links
  const { state: urlState, update: updateUrl } = useUrlState(queryMonitorSchema);
  const requestedTab = locationState?.tab || urlState.tab || 'activity';
  const activeTab: 'activity' | 'health' = requestedTab === 'health' ? 'health' : 'activity';
  const setActiveTab = useCallback((tab: 'activity' | 'health') => {
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
      normalizeQueryFilterArrays(patch);
    } else {
      if (urlState.user) patch.user = urlState.user;
      if (urlState.queryId) patch.queryId = urlState.queryId;
      if (urlState.queryText) patch.queryText = urlState.queryText;
      if (urlState.queryKind) patch.queryKind = urlState.queryKind;
      if (urlState.status) patch.status = urlState.status;
      if (urlState.quickFilter && ['running', 'recent', 'failed', 'slow'].includes(urlState.quickFilter)) {
        patch.quickFilter = urlState.quickFilter;
      }
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
    if ('user' in filter) urlPatch.user = nonEmptyValues(filter.user);
    if ('queryId' in filter) urlPatch.queryId = nonEmptyValues(filter.queryId);
    if ('queryText' in filter) urlPatch.queryText = filter.queryText || undefined;
    if ('queryKind' in filter) urlPatch.queryKind = nonEmptyValues(filter.queryKind);
    if ('status' in filter) urlPatch.status = nonEmptyValues(filter.status);
    if ('quickFilter' in filter) urlPatch.quickFilter = filter.quickFilter || undefined;
    if ('database' in filter) urlPatch.database = nonEmptyValues(filter.database);
    if ('table' in filter) urlPatch.tableName = nonEmptyValues(filter.table);
    if ('hostname' in filter) urlPatch.hostname = nonEmptyValues(filter.hostname);
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
  const triggerManualRefresh = useGlobalLastUpdatedStore(s => s.triggerManualRefresh);
  const { available: hasQueryLog, probing: isProbing } = useCapabilityCheck(['query_log']);
  const { available: hasProcesses, probing: isProcessesProbing } = useCapabilityCheck(['system_processes']);

  const [historyCoordinatorIds, setHistoryCoordinatorIds] = useState<Set<string>>(new Set());
  const [concurrency, setConcurrency] = useState<QueryConcurrency | null>(null);
  const activityRecords = useMemo(
    () => buildQueryActivityRecords(
      { live: runningQueries, recent: queryHistory },
      historyFilter,
    ),
    [runningQueries, queryHistory, historyFilter],
  );

  // Preserve the selected row as a live process becomes a terminal log record.
  useEffect(() => {
    if (!selectedQuery || selectedQueryType !== 'running') return;
    const selectedKey = queryActivityKey(selectedQuery);
    const completed = queryHistory.find(query => queryActivityKey(query) === selectedKey);
    if (completed) selectQuery(completed, 'history');
  }, [queryHistory, selectQuery, selectedQuery, selectedQueryType]);

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
  const activityCoordinatorIds = useMemo(
    () => new Set([...runningCoordinatorIds, ...historyCoordinatorIds]),
    [runningCoordinatorIds, historyCoordinatorIds],
  );

  const slotPct = concurrency && concurrency.maxConcurrent > 0
    ? (concurrency.running / concurrency.maxConcurrent) * 100
    : 0;

  let activeProfile = profiles.find(p => p.id === activeProfileId);
  if (!activeProfile && profiles.length > 0) activeProfile = profiles.find(p => p.is_connected);
  const isConnected = activeProfile?.is_connected ?? false;
  const historyFetchInFlightRef = useRef(false);

  const fetchHistory = useCallback(async () => {
    if (!services || !isConnected || historyFetchInFlightRef.current) return;
    historyFetchInFlightRef.current = true;
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
    finally {
      historyFetchInFlightRef.current = false;
      setIsLoadingHistory(false);
    }
  }, [services, isConnected, historyFilter]);

  const handleKillQuery = useCallback(async (qid: string) => {
    if (!services) return;
    setIsKillingQuery(true);
    try { const r = await queryApi.killQuery(services.queryAnalyzer, qid); if (r.success) setRunningQueries(runningQueries.filter(q => q.query_id !== qid)); }
    catch (e) { setError(e instanceof Error ? e.message : 'Failed'); }
    finally { setIsKillingQuery(false); }
  }, [services, runningQueries]);

  const convertedQuery = useMemo(
    () => querySelectionToSeries(selectedQuery, selectedQueryType),
    [selectedQuery, selectedQueryType],
  );

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
  }, [convertedQuery, updateUrl]);

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
      console.error('[QueryMonitor] Failed to fetch query detail:', { qdId, err });
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
  const historyIntervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  useEffect(() => {
    if (wsRef.current) { wsRef.current.disconnect(); wsRef.current = null; }
    if (statsIntervalRef.current) { clearInterval(statsIntervalRef.current); statsIntervalRef.current = null; }
    if (historyIntervalRef.current) { clearInterval(historyIntervalRef.current); historyIntervalRef.current = null; }
    if (!services || !isConnected) { clearQueries(); return; }
    const queryIntervalMs = refreshRateSeconds > 0 ? clampToAllowed(Math.max(2, refreshRateSeconds), refreshConfig) * 1000 : 0;
    const statsIntervalMs = refreshRateSeconds > 0 ? clampToAllowed(refreshRateSeconds, refreshConfig) * 1000 : 5000;
    wsRef.current = new QueryWebSocket(
      services.queryAnalyzer,
      queryIntervalMs,
      historyFilter.limit ?? 100,
    );
    // Always fetch once. A zero interval means paused/manual refresh, not
    // "never load live queries"; manualRefreshTick recreates this poller.
    wsRef.current.connect();
    // Lightweight stats poller — single query for concurrency/QPS/rejected
    const overviewService = new OverviewService(services.adapter, {}, services.environmentDetector);
    const pollStats = () => {
      overviewService.getQueryMonitorStats().then(setConcurrency).catch(() => {});
    };
    pollStats();
    if (refreshRateSeconds > 0) statsIntervalRef.current = setInterval(pollStats, statsIntervalMs);
    if (hasQueryLog) {
      fetchHistory();
      if (refreshRateSeconds > 0) {
        historyIntervalRef.current = setInterval(fetchHistory, Math.max(5_000, queryIntervalMs));
      }
    }
    return () => {
      if (wsRef.current) wsRef.current.disconnect();
      if (statsIntervalRef.current) clearInterval(statsIntervalRef.current);
      if (historyIntervalRef.current) clearInterval(historyIntervalRef.current);
    };
  }, [services, isConnected, refreshRateSeconds, refreshConfig, manualRefreshTick, hasQueryLog, fetchHistory, historyFilter.limit, clearQueries]);

  // Fetch history when filter changes or when hasQueryLog becomes available
  useEffect(() => {
    if (services && isConnected && hasQueryLog) fetchHistory();
  }, [historyFilter, fetchHistory, hasQueryLog, services, isConnected]);

  if (!activeProfile?.id || !isConnected) {
    return (
      <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', height: '100%', gap: 16, background: 'var(--bg-primary)' }}>
        <div style={{ color: 'var(--text-primary)', fontSize: 18, fontWeight: 600 }}>Query Tracker</div>
        <div style={{ color: 'var(--text-muted)', fontSize: 13 }}>Connect to a ClickHouse server to monitor queries.</div>
        <button onClick={() => setConnectionFormOpen(true)}
          style={{ marginTop: 8, padding: '8px 20px', borderRadius: 6, border: 'none', background: 'var(--accent-primary, #58a6ff)', color: '#fff', fontSize: 13, fontWeight: 600, cursor: 'pointer' }}>
          Add Connection
        </button>
      </div>
    );
  }

  const tabs: { key: 'activity' | 'health'; label: string; count?: number; badge?: string }[] = [
    { key: 'activity', label: 'Queries', count: activityRecords.length },
    ...(experimentalEnabled ? [{ key: 'health' as const, label: 'Health Map', badge: 'exp' }] : []),
  ];

  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column', overflow: 'hidden', background: 'var(--bg-primary)' }}>
      {/* Header */}
      <div style={{ padding: '16px 24px 0', flexShrink: 0, background: 'var(--bg-secondary)', borderBottom: '1px solid var(--border-primary)' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 12 }}>
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
              <h2 style={{ color: 'var(--text-primary)', fontSize: 18, fontWeight: 600, margin: 0 }}>Query Tracker</h2>
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
        
        {/* Compact query vitals */}
        <MetricStrip
          ariaLabel="Query monitor summary"
          style={{ marginBottom: 12 }}
        >
          {(() => {
            const points = concurrency?.qpsHistory ?? [];
            if (points.length < 2) {
              return <MetricStripItem label="q/s · 15m" value="—" />;
            }
            const values = points.map(point => point.qps);
            const maxValue = Math.max(...values, 1);
            const width = 150;
            const height = 26;
            const padding = 1;
            const stepX = (width - padding * 2) / (values.length - 1);
            const path = values
              .map((value, index) => {
                const x = padding + index * stepX;
                const y = height - padding - ((value / maxValue) * (height - padding * 2));
                return `${index === 0 ? 'M' : 'L'}${x.toFixed(1)},${y.toFixed(1)}`;
              })
              .join(' ');
            const area = `${path} L${(padding + (values.length - 1) * stepX).toFixed(1)},${height} L${padding},${height} Z`;
            const latest = values[values.length - 1];

            return (
              <span style={{ display: 'inline-flex', alignItems: 'center', gap: 10, whiteSpace: 'nowrap' }}>
                <svg
                  width={width}
                  height={height}
                  viewBox={`0 0 ${width} ${height}`}
                  style={{ display: 'block', flexShrink: 0 }}
                  aria-hidden="true"
                >
                  <path d={area} fill="rgba(59,130,246,0.12)" />
                  <path d={path} fill="none" stroke="#3b82f6" strokeWidth="1.5" strokeLinejoin="round" />
                </svg>
                <span style={{ color: 'var(--text-primary)', fontFamily: 'var(--font-mono, monospace)', fontSize: 13, fontWeight: 600 }}>
                  {latest.toFixed(1)}
                  <span style={{ color: 'var(--text-muted)', fontWeight: 400 }}> q/s · 15m</span>
                </span>
              </span>
            );
          })()}
          <MetricStripDivider />
          <MetricStripItem
            label="slots"
            value={concurrency ? `${concurrency.running} / ${concurrency.maxConcurrent}` : '— / —'}
            color={slotPct > 80 ? '#ef4444' : undefined}
            barPercentage={slotPct}
          />
          <MetricStripItem
            label="queued"
            value={concurrency ? concurrency.queued : '—'}
            color={concurrency && concurrency.queued > 0 ? '#ef4444' : undefined}
            indicatorColor={concurrency && concurrency.queued > 0 ? '#ef4444' : '#3fb950'}
          />
          <MetricStripItem
            label="rejected (1h)"
            value={concurrency ? concurrency.rejectedRecent : '—'}
            color={concurrency && concurrency.rejectedRecent > 0 ? '#ef4444' : undefined}
            indicatorColor={concurrency && concurrency.rejectedRecent > 0 ? '#ef4444' : '#3fb950'}
          />
          <MetricStripDivider />
          {ALL_QUERY_TYPES.map(type => (
            <MetricStripItem
              key={type}
              label={type}
              value={queryTypeCounts[type] || 0}
              indicatorColor={QUERY_TYPE_COLORS[type]}
            />
          ))}
        </MetricStrip>

        {/* Tabs */}
        <div className="page-tabs">
          {tabs.map(tab => {
            const active = activeTab === tab.key;
            return (
              <button key={tab.key} onClick={() => setActiveTab(tab.key)}
                className={`page-tab ${active ? 'active' : ''}`}>
                {tab.label}
                {tab.count !== undefined && (
                  <span className="page-tab-count">
                    {tab.count}
                  </span>
                )}
                {tab.badge && (
                  <span className="page-tab-exp">
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
          <PermissionGate error={error} title="Query Tracker" variant="banner" onDismiss={clearError} />
        </div>
      )}

      {/* Degradation banner for running queries */}
      {activeTab === 'activity' && !isProcessesProbing && !hasProcesses && (
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
            <QueryFilterBar
              filter={historyFilter}
              onFilterChange={handleFilterChange}
              queryAnalyzer={services?.queryAnalyzer}
              onRefresh={triggerManualRefresh}
              isLoading={isLoadingHistory}
            />

            <div style={{ marginTop: 14, border: '1px solid var(--border-primary)', borderRadius: 8 }}>
              <QueryActivityTable
                activity={activityRecords}
                selectedQueryId={selectedQuery?.query_id || null}
                onSelectRunningQuery={q => {
                  if (q) selectQuery(q, 'running');
                }}
                onSelectHistoryQuery={q => selectQuery(q, 'history')}
                onKillQuery={handleKillQuery}
                isKillingQuery={isKillingQuery}
                filter={historyFilter}
                sort={historySort}
                onFilterChange={handleFilterChange}
                onSortChange={handleSortChange}
                isLoading={isLoadingHistory}
                queryAnalyzer={services?.queryAnalyzer}
                coordinatorIds={activityCoordinatorIds}
                showFilterBar={false}
              />
            </div>
            {!hasQueryLog && !isProbing && (
              <PermissionGate
                error="system.query_log is not available on this server. Live queries remain available, but completed activity requires query logging."
                title="Completed query activity unavailable"
                variant="banner"
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
