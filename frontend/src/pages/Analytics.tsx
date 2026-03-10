/**
 * Analytics page — table-level analysis starting with ordering key efficiency,
 * plus a "Queries" tab with a preset query explorer.
 */

import React, { useEffect, useState, useCallback, useMemo } from 'react';
import { useConnectionStore } from '../stores/connectionStore';
import { useGlobalLastUpdatedStore } from '../stores/refreshSettingsStore';
import { useClickHouseServices } from '../providers/ClickHouseProvider';
import { useClusterStore } from '../stores/clusterStore';
import { useCapabilityCheck } from '../components/shared/RequiresCapability';
import { OrderingKeyTable } from '../components/analytics/OrderingKeyTable';
import { QueryExplorer } from '../components/analytics/QueryExplorer';
import { DashboardViewer } from '../components/analytics/DashboardViewer';
import { loadDashboards } from '../components/analytics/dashboards';
import { useAnalyticsUrlState } from '../hooks/useUrlState';
import { useNavigate } from '../hooks/useAppLocation';
import type { TableOrderingKeyEfficiency } from '@tracehouse/core';

type AnalyticsTab = 'tables' | 'misc' | 'dashboards';

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

  const navigate = useNavigate();
  // Capture 'from' on mount before useAnalyticsUrlState strips unknown params.
  // HashRouter puts search params inside the hash, so parse from window.location.hash.
  const [fromObsMap] = useState(() => {
    const hash = window.location.hash; // e.g. #/analytics?tab=misc&from=obsmap
    const qIdx = hash.indexOf('?');
    if (qIdx === -1) { console.log('[Analytics fromObsMap] no ? in hash:', hash); return false; }
    const val = new URLSearchParams(hash.slice(qIdx)).get('from') === 'obsmap';
    console.log('[Analytics fromObsMap]', val, 'hash:', hash);
    return val;
  });

  // URL state — tab, lookback, db filter are persisted in the URL
  const { state: urlState, update: updateUrl, copyShareableUrl } = useAnalyticsUrlState();

  const activeTab: AnalyticsTab = (urlState.tab === 'misc' ? 'misc' : urlState.tab === 'dashboards' ? 'dashboards' : 'tables');
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
        "SELECT DISTINCT database AS db FROM {{cluster_metadata:system.tables}} WHERE engine LIKE '%MergeTree%' ORDER BY db"
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

  useEffect(() => { fetchDatabases(); }, [fetchDatabases]);
  useEffect(() => { fetchData(); }, [fetchData]);

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
            <button onClick={() => setActiveTab('tables')} style={tabStyle(activeTab === 'tables')}>Tables Efficiency</button>
            <button onClick={() => setActiveTab('misc')} style={tabStyle(activeTab === 'misc')}>Queries</button>
            <button onClick={() => setActiveTab('dashboards')} style={tabStyle(activeTab === 'dashboards')}>Dashboards</button>
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
                }}>Experimental</span>
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
            <div style={{ margin: '12px 24px 0', padding: '10px 14px', borderRadius: 8, fontSize: 13, background: 'rgba(248,81,73,0.08)', color: '#f85149', border: '1px solid rgba(248,81,73,0.2)' }}>
              {error}
            </div>
          )}

          {!hasQueryLog && !isLoading && !isCapProbing && (
            <div style={{ margin: '12px 24px 0', padding: '10px 14px', borderRadius: 8, fontSize: 13, background: 'rgba(210,153,34,0.08)', color: '#d29922', border: '1px solid rgba(210,153,34,0.2)' }}>
              system.query_log is not available on this server. Analytics requires query logging to be enabled.
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

      {/* ─── Dashboards tab content ─── */}
      {activeTab === 'dashboards' && (
        <div style={{ flex: 1, overflow: 'hidden' }}>
          <DashboardViewer initialDashboardId={urlState.fromDashboard} />
        </div>
      )}
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
