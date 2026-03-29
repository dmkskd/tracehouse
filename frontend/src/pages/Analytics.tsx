/**
 * Analytics page — table-level analysis starting with ordering key efficiency,
 * plus a "Queries" tab with a preset query explorer.
 */

import React, { useEffect, useState, useCallback, useMemo, useRef } from 'react';
import { useConnectionStore } from '../stores/connectionStore';
import { useGlobalLastUpdatedStore } from '../stores/refreshSettingsStore';
import { useClickHouseServices } from '../providers/ClickHouseProvider';
import { useClusterStore } from '../stores/clusterStore';
import { useCapabilityCheck } from '../components/shared/RequiresCapability';
import { PermissionGate } from '../components/shared/PermissionGate';
import { OrderingKeyTable } from '../components/analytics/OrderingKeyTable';
import { QueryExplorer } from '../components/analytics/QueryExplorer';
import { DashboardViewer } from '../components/analytics/DashboardViewer';
import { PatternSurface } from '../components/analytics/PatternSurface';
import { ResourceSurface } from '../components/analytics/ResourceSurface';
import { TimeRangePicker } from '../components/analytics/TimeRangePicker';
import { loadDashboards } from '../components/analytics/dashboards';
import { useAnalyticsUrlState } from '../hooks/useUrlState';
import { useNavigate } from '../hooks/useAppLocation';
import { useUserPreferenceStore } from '../stores/userPreferenceStore';
import { DocsLink } from '../components/common/DocsLink';
import { QueryDetailModal } from '../components/query/QueryDetailModal';
import type { TableOrderingKeyEfficiency, PatternSurfaceRow, QuerySeries, ResourceLanesData } from '@tracehouse/core';

type AnalyticsTab = 'tables' | 'misc' | 'dashboards' | 'surfaces';
type SurfaceSubTab = 'resource' | 'pattern';

const LOOKBACK_OPTIONS = [
  { label: '1 day', value: 1 },
  { label: '7 days', value: 7 },
  { label: '30 days', value: 30 },
];

const SYSTEM_DBS = new Set(['system', 'INFORMATION_SCHEMA', 'information_schema']);

export const Analytics: React.FC = () => {
  const { activeProfileId, profiles, setConnectionFormOpen } = useConnectionStore();
  const services = useClickHouseServices();
  const { detected: clusterDetected } = useClusterStore();
  const { available: hasQueryLog, probing: isCapProbing } = useCapabilityCheck(['query_log']);
  const { experimentalEnabled } = useUserPreferenceStore();

  const navigate = useNavigate();
  // Capture 'from' on mount before useAnalyticsUrlState strips unknown params.
  // HashRouter puts search params inside the hash, so parse from window.location.hash.
  const [fromPage] = useState(() => {
    const hash = window.location.hash;
    const qIdx = hash.indexOf('?');
    if (qIdx === -1) return null;
    return new URLSearchParams(hash.slice(qIdx)).get('from');
  });
  const fromObsMap = fromPage === 'obsmap';
  const fromMerges = fromPage === 'merges';
  const fromQueries = fromPage === 'queries';

  // URL state — tab, lookback, db filter are persisted in the URL
  const { state: urlState, update: updateUrl, copyShareableUrl } = useAnalyticsUrlState();

  const activeTab: AnalyticsTab = (
    urlState.tab === 'misc' ? 'misc' :
    urlState.tab === 'tables' ? 'tables' :
    urlState.tab === 'surfaces' ? 'surfaces' :
    'dashboards'
  );
  const setActiveTab = useCallback((tab: AnalyticsTab) => updateUrl({ tab, fromDashboard: undefined }, { push: true }), [updateUrl]);

  const lookbackDays = urlState.lookback ?? 7;
  const setLookbackDays = useCallback((days: number) => updateUrl({ lookback: days }), [updateUrl]);

  const selectedDb = urlState.db ?? null;
  const setSelectedDb = useCallback((db: string | null) => updateUrl({ db: db ?? undefined }), [updateUrl]);

  const [data, setData] = useState<TableOrderingKeyEfficiency[]>([]);
  const [allDatabases, setAllDatabases] = useState<string[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [showSystemDbs, setShowSystemDbs] = useState(false);
  const [linkCopied, setLinkCopied] = useState(false);

  // ── Surfaces tab state ──
  const [surfaceSubTab, setSurfaceSubTab] = useState<SurfaceSubTab>('resource');
  const [surfaceDb, setSurfaceDb] = useState<string>('');
  const [surfaceTableName, setSurfaceTableName] = useState<string>('');
  const [surfaceTimeRange, setSurfaceTimeRange] = useState<string | null>('1 DAY');
  const [, setStressData] = useState<unknown>(null);
  const [patternData, setPatternData] = useState<PatternSurfaceRow[] | null>(null);
  const [surfaceLoading, setSurfaceLoading] = useState(false);
  const [surfaceError, setSurfaceError] = useState<string | null>(null);
  const surfaceFetchId = useRef(0);
  const [modalQuery, setModalQuery] = useState<QuerySeries | null>(null);

  // ── Resource lanes state ──
  const [resourceData, setResourceData] = useState<ResourceLanesData | null>(null);
  const [resourceLoading, setResourceLoading] = useState(false);
  const [resourceError, setResourceError] = useState<string | null>(null);
  const resourceFetchId = useRef(0);
  const [resourceDrillTable, setResourceDrillTable] = useState<string | null>(null);
  const [resourceMaxLanes, setResourceMaxLanes] = useState(10);

  // Reset to Tables tab if experimental is disabled while on Surfaces
  useEffect(() => {
    if (!experimentalEnabled && activeTab === 'surfaces') setActiveTab('dashboards');
  }, [experimentalEnabled]);

  const fromDashboardName = useMemo(() => {
    if (!urlState.fromDashboard) return null;
    const dbs = loadDashboards();
    return dbs.find(d => d.id === urlState.fromDashboard)?.title ?? null;
  }, [urlState.fromDashboard]);

  let activeProfile = profiles.find(p => p.id === activeProfileId);
  if (!activeProfile && profiles.length > 0) activeProfile = profiles.find(p => p.is_connected);
  const isConnected = activeProfile?.is_connected ?? false;

  // Fetch all databases that have MergeTree tables
  const fetchDatabases = useCallback(async () => {
    if (!services || !clusterDetected) return;
    try {
      const rows = await services.adapter.executeQuery<{ db: string }>(
        "SELECT DISTINCT database AS db FROM {{cluster_aware:system.tables}} WHERE engine LIKE '%MergeTree%' ORDER BY db"
      );
      setAllDatabases(rows.map(r => String(r.db)));
    } catch { /* ignore — we'll fall back to databases from query data */ }
  }, [services, clusterDetected]);

  const fetchData = useCallback(async () => {
    if (!services || !isConnected || !clusterDetected) return;
    setIsLoading(true);
    setError(null);
    try {
      const result = await services.analyticsService.getTableOrderingKeyEfficiency({
        lookback_days: lookbackDays,
        min_query_count: 1,
      });
      setData(result);
      useGlobalLastUpdatedStore.getState().touch();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load analytics data');
    } finally {
      setIsLoading(false);
    }
  }, [services, isConnected, lookbackDays, clusterDetected]);

  const fetchSurfaceData = useCallback(async () => {
    if (!services || !surfaceDb || !surfaceTableName || !surfaceTimeRange) return;

    const fetchId = ++surfaceFetchId.current;
    setSurfaceLoading(true);
    setSurfaceError(null);
    try {
      // Convert TimeRangePicker value to SurfaceQueryOptions
      let timeOpts: { hours?: number; startTime?: string; endTime?: string };
      if (surfaceTimeRange.startsWith('CUSTOM:')) {
        const [startTime, endTime] = surfaceTimeRange.slice(7).split(',');
        timeOpts = { startTime, endTime };
      } else {
        // Map ClickHouse interval string to hours
        const INTERVAL_HOURS: Record<string, number> = {
          '15 MINUTE': 0.25, '1 HOUR': 1, '6 HOUR': 6,
          '1 DAY': 24, '2 DAY': 48, '7 DAY': 168, '30 DAY': 720,
        };
        timeOpts = { hours: INTERVAL_HOURS[surfaceTimeRange] ?? 24 };
      }
      const opts = { database: surfaceDb, table: surfaceTableName, ...timeOpts };
      const [stress, pattern] = await Promise.all([
        services.analyticsService.getStressSurfaceData(opts),
        services.analyticsService.getPatternSurfaceData(opts),
      ]);
      if (fetchId !== surfaceFetchId.current) return;
      setStressData(stress);
      setPatternData(pattern);
    } catch (e) {
      if (fetchId !== surfaceFetchId.current) return;
      setSurfaceError(e instanceof Error ? e.message : 'Failed to load surface data');
    } finally {
      if (fetchId === surfaceFetchId.current) setSurfaceLoading(false);
    }
  }, [services, surfaceDb, surfaceTableName, surfaceTimeRange]);

  /** Convert surfaceTimeRange to {hours} or {startTime, endTime} */
  const parseTimeRange = useCallback((): { hours?: number; startTime?: string; endTime?: string } => {
    if (!surfaceTimeRange) return { hours: 24 };
    if (surfaceTimeRange.startsWith('CUSTOM:')) {
      const [startTime, endTime] = surfaceTimeRange.slice(7).split(',');
      return { startTime, endTime };
    }
    const INTERVAL_HOURS: Record<string, number> = {
      '15 MINUTE': 0.25, '1 HOUR': 1, '6 HOUR': 6,
      '1 DAY': 24, '2 DAY': 48, '7 DAY': 168, '30 DAY': 720,
    };
    return { hours: INTERVAL_HOURS[surfaceTimeRange] ?? 24 };
  }, [surfaceTimeRange]);

  const fetchResourceLanes = useCallback(async () => {
    if (!services) return;
    const fetchId = ++resourceFetchId.current;
    setResourceLoading(true);
    setResourceError(null);
    try {
      const timeOpts = parseTimeRange();
      let result: ResourceLanesData;
      if (resourceDrillTable) {
        // Drill-down: table level — split db.table
        const dotIdx = resourceDrillTable.indexOf('.');
        const db = dotIdx > 0 ? resourceDrillTable.slice(0, dotIdx) : 'default';
        const tbl = dotIdx > 0 ? resourceDrillTable.slice(dotIdx + 1) : resourceDrillTable;
        result = await services.analyticsService.getTableResourceLanes({
          database: db,
          table: tbl,
          maxLanes: resourceMaxLanes,
          ...timeOpts,
        });
      } else {
        // System level
        result = await services.analyticsService.getSystemResourceLanes({
          maxLanes: resourceMaxLanes,
          ...timeOpts,
        });
      }
      // Discard stale response if a newer fetch was started
      if (fetchId !== resourceFetchId.current) return;
      setResourceData(result);
    } catch (e) {
      if (fetchId !== resourceFetchId.current) return;
      setResourceError(e instanceof Error ? e.message : 'Failed to load resource data');
    } finally {
      if (fetchId === resourceFetchId.current) setResourceLoading(false);
    }
  }, [services, resourceDrillTable, resourceMaxLanes, parseTimeRange]);

  // Open QueryDetailModal for a pattern hash — fetch the most recent query for that hash
  const handleOpenPatternQuery = useCallback(async (hash: string) => {
    if (!services) return;
    try {
      const rows = await services.adapter.executeQuery<Record<string, unknown>>(
        `SELECT query_id, query, user, event_time, query_duration_ms, memory_usage,
                ProfileEvents['RealTimeMicroseconds'] AS cpu_us,
                ProfileEvents['NetworkSendBytes'] AS net_send,
                ProfileEvents['NetworkReceiveBytes'] AS net_recv,
                ProfileEvents['ReadBufferFromFileDescriptorReadBytes'] AS disk_read,
                ProfileEvents['WriteBufferFromFileDescriptorWriteBytes'] AS disk_write,
                type AS status
         FROM system.query_log
         WHERE type = 'QueryFinish'
           AND normalized_query_hash = toUInt64('${hash.replace(/[^0-9]/g, '')}')
         ORDER BY event_time DESC
         LIMIT 1`
      );
      if (rows.length === 0) return;
      const r = rows[0];
      const durationMs = Number(r.query_duration_ms ?? 0);
      const eventTime = String(r.event_time ?? '');
      const startTime = eventTime ? new Date(eventTime + (eventTime.includes('Z') ? '' : 'Z')).toISOString() : new Date().toISOString();
      const endTime = new Date(new Date(startTime).getTime() + durationMs).toISOString();
      setModalQuery({
        query_id: String(r.query_id ?? ''),
        label: String(r.query ?? ''),
        user: String(r.user ?? 'default'),
        start_time: startTime,
        end_time: endTime,
        duration_ms: durationMs,
        peak_memory: Number(r.memory_usage ?? 0),
        cpu_us: Number(r.cpu_us ?? 0),
        net_send: Number(r.net_send ?? 0),
        net_recv: Number(r.net_recv ?? 0),
        disk_read: Number(r.disk_read ?? 0),
        disk_write: Number(r.disk_write ?? 0),
        status: String(r.status ?? 'QueryFinish'),
        points: [],
      });
    } catch (e) {
      console.error('Failed to fetch query for pattern hash:', e);
    }
  }, [services]);

  useEffect(() => { if (hasQueryLog || isCapProbing) fetchDatabases(); }, [fetchDatabases, hasQueryLog, isCapProbing]);
  useEffect(() => { if (hasQueryLog || isCapProbing) fetchData(); }, [fetchData, hasQueryLog, isCapProbing]);

  // Auto-fetch surface data when parameters change
  useEffect(() => {
    if (surfaceDb && surfaceTableName && services) fetchSurfaceData();
  }, [fetchSurfaceData]);

  // Auto-fetch resource lanes when time range or drill state changes
  useEffect(() => {
    if (services && activeTab === 'surfaces' && surfaceSubTab === 'resource') fetchResourceLanes();
  }, [fetchResourceLanes, activeTab, surfaceSubTab]);

  // Auto-pick database/table for surfaces from the Tables Efficiency data
  useEffect(() => {
    if (!surfaceDb && data.length > 0) {
      const top = data.reduce((a, b) => a.query_count > b.query_count ? a : b);
      setSurfaceDb(top.database);
      setSurfaceTableName(top.table_name);
    }
  }, [data, surfaceDb]);

  // Databases and tables available for surfaces (sorted alphabetically, no system dbs)
  const surfaceDatabases = useMemo(() => {
    const dbs = [...new Set(data.map(d => d.database))].filter(db => !SYSTEM_DBS.has(db)).sort();
    return dbs;
  }, [data]);

  const surfaceTables = useMemo(() => {
    if (!surfaceDb) return [];
    return data.filter(d => d.database === surfaceDb).map(d => d.table_name).sort();
  }, [data, surfaceDb]);

  // Merge databases from both sources: actual MergeTree dbs + any dbs in query data
  const databases = useMemo(() => {
    const fromData = data.map(d => d.database);
    const merged = [...new Set([...allDatabases, ...fromData])].sort();
    if (showSystemDbs) return merged;
    return merged.filter(db => !SYSTEM_DBS.has(db));
  }, [allDatabases, data, showSystemDbs]);

  const hiddenSystemCount = useMemo(() => {
    const fromData = data.map(d => d.database);
    const all = [...new Set([...allDatabases, ...fromData])];
    return all.filter(db => SYSTEM_DBS.has(db)).length;
  }, [allDatabases, data]);

  // Reset db filter if the selected db is no longer visible
  useEffect(() => {
    if (selectedDb && !databases.includes(selectedDb)) setSelectedDb(null);
  }, [databases, selectedDb]);

  // Filtered data
  const filtered = useMemo(() => {
    let result = data;
    if (!showSystemDbs) result = result.filter(d => !SYSTEM_DBS.has(d.database));
    if (selectedDb) result = result.filter(d => d.database === selectedDb);
    return result;
  }, [data, selectedDb, showSystemDbs]);

  if (!activeProfile?.id || !isConnected) {
    return (
      <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', height: '100%', gap: 16, background: 'var(--bg-primary)' }}>
        <div style={{ color: 'var(--text-primary)', fontSize: 18, fontWeight: 600 }}>Analytics</div>
        <div style={{ color: 'var(--text-muted)', fontSize: 13 }}>Connect to a ClickHouse server to analyze table efficiency.</div>
        <button onClick={() => setConnectionFormOpen(true)}
          style={{ marginTop: 8, padding: '8px 20px', borderRadius: 6, border: 'none', background: 'var(--accent-primary, #58a6ff)', color: '#fff', fontSize: 13, fontWeight: 600, cursor: 'pointer' }}>
          Add Connection
        </button>
      </div>
    );
  }

  const btnStyle = (active: boolean): React.CSSProperties => ({
    padding: '4px 12px',
    fontSize: 11,
    fontWeight: active ? 600 : 400,
    color: active ? 'var(--text-primary)' : 'var(--text-muted)',
    background: active ? 'var(--bg-card-hover, rgba(88,166,255,0.1))' : 'transparent',
    border: '1px solid var(--border-primary)',
    borderRadius: 4,
    cursor: 'pointer',
    transition: 'all 0.15s',
    fontFamily: "'Share Tech Mono', monospace",
  });

  const tabStyle = (active: boolean): React.CSSProperties => ({
    display: 'inline-flex',
    alignItems: 'center',
    gap: 6,
    padding: '6px 16px',
    fontSize: 12,
    fontWeight: active ? 600 : 400,
    color: active ? 'var(--text-primary)' : 'var(--text-tertiary)',
    background: 'transparent',
    border: 'none',
    borderBottom: active ? '2px solid var(--accent-primary)' : '2px solid transparent',
    cursor: 'pointer',
    transition: 'all 0.15s',
    marginBottom: -1,
  });

  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column', overflow: 'hidden', background: 'var(--bg-primary)' }}>
      {/* Header with tabs */}
      <div style={{ flexShrink: 0, background: 'var(--bg-secondary)', borderBottom: '1px solid var(--border-primary)' }}>
        <div style={{ padding: '12px 24px 0' }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 8 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
              <h2 style={{ color: 'var(--text-primary)', fontSize: 18, fontWeight: 600, margin: 0 }}>Analytics</h2>
              <DocsLink path="/features/analytics" />
              {fromObsMap && activeTab === 'misc' && (
                <button
                  onClick={() => navigate('/overview', { state: { restoreObsMap: true } })}
                  style={{
                    display: 'inline-flex', alignItems: 'center', gap: 4,
                    padding: '2px 8px', fontSize: 11, color: 'var(--text-muted)',
                    background: 'transparent', border: '1px solid var(--border-secondary)',
                    borderRadius: 4, cursor: 'pointer', fontFamily: 'inherit',
                    transition: 'color 0.15s ease, border-color 0.15s ease',
                  }}
                  onMouseEnter={(e) => { e.currentTarget.style.color = 'var(--text-primary)'; e.currentTarget.style.borderColor = 'var(--border-primary)'; }}
                  onMouseLeave={(e) => { e.currentTarget.style.color = 'var(--text-muted)'; e.currentTarget.style.borderColor = 'var(--border-secondary)'; }}
                >
                  <span>←</span>
                  <span>Back to System Map</span>
                </button>
              )}
              {fromQueries && activeTab === 'misc' && (
                <button
                  onClick={() => navigate('/queries')}
                  style={{
                    display: 'inline-flex', alignItems: 'center', gap: 4,
                    padding: '2px 8px', fontSize: 11, color: 'var(--text-muted)',
                    background: 'transparent', border: '1px solid var(--border-secondary)',
                    borderRadius: 4, cursor: 'pointer', fontFamily: 'inherit',
                    transition: 'color 0.15s ease, border-color 0.15s ease',
                  }}
                  onMouseEnter={(e) => { e.currentTarget.style.color = 'var(--text-primary)'; e.currentTarget.style.borderColor = 'var(--border-primary)'; }}
                  onMouseLeave={(e) => { e.currentTarget.style.color = 'var(--text-muted)'; e.currentTarget.style.borderColor = 'var(--border-secondary)'; }}
                >
                  <span>←</span>
                  <span>Back to Query Monitor</span>
                </button>
              )}
              {fromMerges && activeTab === 'misc' && (
                <button
                  onClick={() => navigate('/merges')}
                  style={{
                    display: 'inline-flex', alignItems: 'center', gap: 4,
                    padding: '2px 8px', fontSize: 11, color: 'var(--text-muted)',
                    background: 'transparent', border: '1px solid var(--border-secondary)',
                    borderRadius: 4, cursor: 'pointer', fontFamily: 'inherit',
                    transition: 'color 0.15s ease, border-color 0.15s ease',
                  }}
                  onMouseEnter={(e) => { e.currentTarget.style.color = 'var(--text-primary)'; e.currentTarget.style.borderColor = 'var(--border-primary)'; }}
                  onMouseLeave={(e) => { e.currentTarget.style.color = 'var(--text-muted)'; e.currentTarget.style.borderColor = 'var(--border-secondary)'; }}
                >
                  <span>←</span>
                  <span>Back to Merge Tracker</span>
                </button>
              )}
              {urlState.fromDashboard && activeTab === 'misc' && (
                <button
                  onClick={() => updateUrl({ tab: 'dashboards', preset: undefined, sql: undefined }, { push: true })}
                  style={{
                    display: 'inline-flex', alignItems: 'center', gap: 4,
                    padding: '2px 8px', fontSize: 11, color: 'var(--text-muted)',
                    background: 'transparent', border: '1px solid var(--border-secondary)',
                    borderRadius: 4, cursor: 'pointer', fontFamily: 'inherit',
                    transition: 'color 0.15s ease, border-color 0.15s ease',
                  }}
                  onMouseEnter={(e) => { e.currentTarget.style.color = 'var(--text-primary)'; e.currentTarget.style.borderColor = 'var(--border-primary)'; }}
                  onMouseLeave={(e) => { e.currentTarget.style.color = 'var(--text-muted)'; e.currentTarget.style.borderColor = 'var(--border-secondary)'; }}
                >
                  <span>←</span>
                  <span>Back to {fromDashboardName ?? 'Dashboard'}</span>
                </button>
              )}
            </div>
            <button
              onClick={async () => {
                const ok = await copyShareableUrl();
                if (ok) { setLinkCopied(true); setTimeout(() => setLinkCopied(false), 2000); }
              }}
              title="Copy shareable link"
              style={{
                padding: '4px 10px', fontSize: 11, fontWeight: 500,
                color: linkCopied ? 'var(--accent-green, #3fb950)' : 'var(--text-muted)',
                background: 'var(--bg-card)', border: '1px solid var(--border-primary)',
                borderRadius: 4, cursor: 'pointer', transition: 'all 0.15s',
                fontFamily: "'Share Tech Mono', monospace",
              }}
            >
              {linkCopied ? '✓ Copied' : '⧉ Share'}
            </button>
          </div>
          {/* Tab bar */}
          <div style={{ display: 'flex', gap: 4, borderBottom: '1px solid var(--border-primary)', marginLeft: -4 }}>
            <button onClick={() => setActiveTab('dashboards')} style={tabStyle(activeTab === 'dashboards')}>Dashboards</button>
            <button onClick={() => setActiveTab('misc')} style={tabStyle(activeTab === 'misc')}>Queries</button>
            {experimentalEnabled && (
              <button onClick={() => setActiveTab('surfaces')} style={{ ...tabStyle(activeTab === 'surfaces'), position: 'relative' }}>
                Surfaces
                <span style={{
                  position: 'absolute', top: -4, right: -2,
                  fontSize: 7, fontWeight: 700, color: '#f0883e',
                  background: 'var(--bg-secondary)', border: '1px solid rgba(240,136,62,0.3)',
                  borderRadius: 3, padding: '0 3px', lineHeight: '12px',
                  textTransform: 'uppercase', letterSpacing: '0.3px',
                }}>exp</span>
              </button>
            )}
            <button onClick={() => setActiveTab('tables')} style={tabStyle(activeTab === 'tables')}>Tables Efficiency</button>
          </div>
        </div>

        {/* Tables tab sub-header */}
        {activeTab === 'tables' && (
          <div style={{ padding: '8px 24px 12px' }}>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 4 }}>
              <span style={{ color: 'var(--text-muted)', fontSize: 12, display: 'flex', alignItems: 'center', gap: 8 }}>
                Ordering key efficiency across your query workload
                <span style={{
                  fontSize: 9, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '1.5px',
                  padding: '2px 7px', borderRadius: 4,
                  background: 'rgba(210, 153, 34, 0.2)', color: '#d29922',
                }}>Beta</span>
              </span>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>Lookback:</span>
                <div className="tabs">
                  {LOOKBACK_OPTIONS.map(opt => (
                    <button
                      key={opt.value}
                      className={`tab ${lookbackDays === opt.value ? 'active' : ''}`}
                      onClick={() => setLookbackDays(opt.value)}
                    >
                      {opt.label}
                    </button>
                  ))}
                </div>
                <button onClick={fetchData} disabled={isLoading}
                  style={{
                    padding: '4px 12px', fontSize: 11, color: 'var(--text-muted)',
                    background: 'var(--bg-card)', border: '1px solid var(--border-primary)',
                    borderRadius: 6, cursor: isLoading ? 'not-allowed' : 'pointer',
                    opacity: isLoading ? 0.5 : 1,
                  }}>
                  {isLoading ? 'Loading…' : 'Refresh'}
                </button>
              </div>
            </div>

            {/* Database filter buttons */}
            {databases.length > 0 && (
              <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginTop: 8, marginBottom: 4, flexWrap: 'wrap' }}>
                <span style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.5px', marginRight: 4 }}>Database:</span>
                <button onClick={() => setSelectedDb(null)} style={btnStyle(selectedDb === null)}>
                  All
                </button>
                {databases.map(db => {
                  const count = data.filter(d => d.database === db).length;
                  return (
                    <button key={db} onClick={() => setSelectedDb(db)} style={btnStyle(selectedDb === db)}>
                      {db}{count > 0 ? ` (${count})` : ''}
                    </button>
                  );
                })}
                {hiddenSystemCount > 0 && (
                  <button
                    onClick={() => setShowSystemDbs(!showSystemDbs)}
                    style={{
                      ...btnStyle(false),
                      fontSize: 10,
                      color: 'var(--text-muted)',
                      borderStyle: 'dashed',
                    }}
                  >
                    {showSystemDbs ? 'Hide' : 'Show'} system ({hiddenSystemCount})
                  </button>
                )}
              </div>
            )}

            {/* Summary stats — computed from filtered data */}
            {filtered.length > 0 && (
              <div style={{ display: 'flex', gap: 16, marginTop: 8 }}>
                <SummaryCard label="Tables Analyzed" value={filtered.length} />
                <SummaryCard label="Avg Pruning" value={avgPruning(filtered)} suffix="%" color={pruningColorForAvg(avgPruning(filtered))} />
                <SummaryCard
                  label="Poor Pruning"
                  value={filtered.filter(d => d.avg_pruning_pct != null && d.avg_pruning_pct < 50).length}
                  color={filtered.some(d => d.avg_pruning_pct != null && d.avg_pruning_pct < 50) ? '#f85149' : undefined}
                />
                <SummaryCard label="Total Queries" value={filtered.reduce((s, d) => s + d.query_count, 0)} />
              </div>
            )}
          </div>
        )}
      </div>

      {/* ─── Tables tab content ─── */}
      {activeTab === 'tables' && (
        <>
          {error && (
            <div style={{ margin: '12px 24px 0' }}>
              <PermissionGate error={error} title="Analytics" variant="banner" />
            </div>
          )}

          {!hasQueryLog && !error && !isLoading && !isCapProbing && (
            <div style={{ margin: '12px 24px 0' }}>
              <PermissionGate
                error="system.query_log is not available on this server. Analytics requires query logging to be enabled."
                title="Analytics"
                variant="banner"
              />
            </div>
          )}

          <div style={{ flex: 1, overflow: 'auto', padding: '0 24px' }}>
            <div style={{ padding: '12px 0' }}>
              <OrderingKeyTable data={filtered} isLoading={isLoading} lookbackDays={lookbackDays} />
              {selectedDb && filtered.length === 0 && !isLoading && (
                <div style={{ padding: '24px 0', textAlign: 'center', color: 'var(--text-muted)', fontSize: 12 }}>
                  No SELECT queries found for <span style={{ fontFamily: "'Share Tech Mono', monospace" }}>{selectedDb}</span> in the last {lookbackDays} day{lookbackDays > 1 ? 's' : ''}.
                  <br />
                  <span style={{ fontSize: 11, marginTop: 4, display: 'inline-block' }}>
                    Run some queries against tables in this database, then come back here.
                  </span>
                </div>
              )}
            </div>
          </div>
        </>
      )}

      {/* ─── Misc tab content ─── */}
      {activeTab === 'misc' && (
        <div style={{ flex: 1, overflow: 'hidden' }}>
          <QueryExplorer urlState={urlState} onUrlStateChange={updateUrl} />
        </div>
      )}

      {/* ─── Surfaces tab content ─── */}
      {activeTab === 'surfaces' && experimentalEnabled && (
        <>
          <div style={{ flexShrink: 0, padding: '8px 24px 12px', background: 'var(--bg-secondary)', borderBottom: '1px solid var(--border-primary)' }}>
            <TimeRangePicker value={surfaceTimeRange} onChange={setSurfaceTimeRange} />

            {/* Sub-tabs + db/table selectors */}
            <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginTop: 8, flexWrap: 'wrap' }}>
              <button onClick={() => setSurfaceSubTab('resource')} style={btnStyle(surfaceSubTab === 'resource')}>
                Resource Usage
              </button>
              <button onClick={() => setSurfaceSubTab('pattern')} style={btnStyle(surfaceSubTab === 'pattern')}
                title="Experimental — duration-based ranking may not reflect actual resource impact"
              >
                Query Patterns <span style={{ fontSize: 9, opacity: 0.6 }}>experimental</span>
              </button>

              {surfaceSubTab === 'pattern' && (
                <>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginLeft: 8 }}>
                    <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>Database:</span>
                    <select
                      value={surfaceDb}
                      onChange={e => {
                        setSurfaceDb(e.target.value);
                        const tables = data.filter(d => d.database === e.target.value).map(d => d.table_name).sort();
                        setSurfaceTableName(tables[0] ?? '');
                      }}
                      style={{
                        padding: '3px 8px', fontSize: 11, borderRadius: 4,
                        border: '1px solid var(--border-primary)', background: 'var(--bg-card)',
                        color: 'var(--text-primary)', fontFamily: "'Share Tech Mono', monospace",
                      }}
                    >
                      <option value="">--</option>
                      {surfaceDatabases.map(db => (
                        <option key={db} value={db}>{db}</option>
                      ))}
                    </select>
                  </div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                    <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>Table:</span>
                    <select
                      value={surfaceTableName}
                      onChange={e => setSurfaceTableName(e.target.value)}
                      style={{
                        padding: '3px 8px', fontSize: 11, borderRadius: 4,
                        border: '1px solid var(--border-primary)', background: 'var(--bg-card)',
                        color: 'var(--text-primary)', fontFamily: "'Share Tech Mono', monospace",
                        maxWidth: 280,
                      }}
                    >
                      <option value="">--</option>
                      {surfaceTables.map(tbl => (
                        <option key={tbl} value={tbl}>{tbl}</option>
                      ))}
                    </select>
                  </div>
                </>
              )}
            </div>
          </div>

          <div style={{ flex: 1, overflow: 'hidden', padding: 12, visibility: modalQuery ? 'hidden' : 'visible' }}>
            {surfaceSubTab === 'resource' && (
              <ResourceSurface
                data={resourceData}
                isLoading={resourceLoading}
                error={resourceError}
                maxLanes={resourceMaxLanes}
                onMaxLanesChange={setResourceMaxLanes}
                onDrillDown={(tableFullName) => {
                  setResourceDrillTable(tableFullName);
                }}
                onDrillUp={() => {
                  setResourceDrillTable(null);
                }}
                onOpenQuery={handleOpenPatternQuery}
              />
            )}
            {surfaceSubTab === 'pattern' && (
              <PatternSurface data={patternData} isLoading={surfaceLoading} error={surfaceError} onOpenQuery={handleOpenPatternQuery} />
            )}
          </div>
        </>
      )}

      {/* ─── Dashboards tab content ─── */}
      {activeTab === 'dashboards' && (
        <div style={{ flex: 1, overflow: 'hidden' }}>
          <DashboardViewer initialDashboardId={urlState.fromDashboard} />
        </div>
      )}

      {/* Query Detail Modal — opened from pattern surface hover cards */}
      <QueryDetailModal query={modalQuery} onClose={() => setModalQuery(null)} />
    </div>
  );
};

function avgPruning(data: TableOrderingKeyEfficiency[]): number {
  const withPruning = data.filter(d => d.avg_pruning_pct != null);
  if (withPruning.length === 0) return 0;
  return withPruning.reduce((s, d) => s + (d.avg_pruning_pct ?? 0), 0) / withPruning.length;
}

function pruningColorForAvg(pct: number): string {
  if (pct >= 90) return '#3fb950';
  if (pct >= 50) return '#d29922';
  return '#f85149';
}

const SummaryCard: React.FC<{
  label: string; value: number; suffix?: string; color?: string;
}> = ({ label, value, suffix, color }) => (
  <div className="stat-card" style={{ flex: 1, minWidth: 120 }}>
    <div className="stat-value" style={{ color: color || 'var(--text-primary)' }}>
      {typeof value === 'number' && !Number.isInteger(value) ? value.toFixed(1) : value}{suffix || ''}
    </div>
    <div className="stat-label">{label}</div>
  </div>
);

export default Analytics;
