/**
 * QueryDetailModal — Rich query detail modal used by TimeTravelPage,
 * QueryMonitor, and OrderingKeyTable.
 *
 * Shell only: owns activeTab + queryOverride + cross-query navigation.
 * Per-tab state lives in modal/hooks/, per-tab JSX lives in modal/tabs/.
 */

import React, { useCallback, useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import type { QuerySeries } from '@tracehouse/core';
import { ThreadBreakdownSection } from '../QueryDetail';
import { TraceLogViewer } from '../../tracing/TraceLogViewer';
import { SpeedscopeViewer } from '../../tracing/SpeedscopeViewer';
import { PipelineProfileTab } from '../../tracing/PipelineProfileTab';
import { useClickHouseServices } from '../../../providers/ClickHouseProvider';
import { useCapabilityCheck } from '../../shared/RequiresCapability';
import { ModalWrapper } from '../../shared/ModalWrapper';
import { XRayTab } from './tabs/XRayTab';
import { useUserPreferenceStore } from '../../../stores/userPreferenceStore';
import { SpansTab } from './tabs/SpansTab';
import { HistoryTab } from './tabs/HistoryTab';
import { OverviewTab } from './tabs/OverviewTab';
import { DetailsTab, type DetailsSubTab } from './tabs/DetailsTab';
import { AnalyticsTab, type AnalyticsSubTab } from './tabs/AnalyticsTab';
import { useQueryLogs } from './hooks/useQueryLogs';
import { useQuerySpans } from './hooks/useQuerySpans';
import { useQueryFlamegraph } from './hooks/useQueryFlamegraph';
import { useQueryThreads } from './hooks/useQueryThreads';
import { useQueryDetail } from './hooks/useQueryDetail';
import { useSimilarQueries } from './hooks/useSimilarQueries';
import { useQueryTimelines } from './hooks/useQueryTimelines';
import { useQueryTopology } from './hooks/useQueryTopology';

export interface TimelineQueryModalProps {
  /** The query from timeline data (null to hide modal) */
  query: QuerySeries | null;
  /** Called when modal should close */
  onClose: () => void;
  /** Optional callback to activate pattern mode in Time Travel (passed by TimeTravelPage) */
  onViewInTimeTravel?: (normalizedQueryHash: string) => void;
}

type QueryModalTab = 'overview' | 'details' | 'analytics' | 'history' | 'logs' | 'spans' | 'flamegraph' | 'pipeline' | 'threads' | 'xray';

export const QueryDetailModal: React.FC<TimelineQueryModalProps> = ({
  query,
  onClose,
  onViewInTimeTravel,
}) => {
  const services = useClickHouseServices();
  const { available: hasTraceLog } = useCapabilityCheck(['trace_log']);
  const { available: hasTextLog } = useCapabilityCheck(['text_log']);
  const { available: hasOpenTelemetry } = useCapabilityCheck(['opentelemetry_span_log']);
  const { available: hasQueryLog } = useCapabilityCheck(['query_log']);
  const { available: hasQueryThreadLog } = useCapabilityCheck(['query_thread_log']);
  const { available: hasProcessesHistory } = useCapabilityCheck(['tracehouse_processes_history']);
  const { experimentalEnabled } = useUserPreferenceStore();

  const navigate = useNavigate();
  const [activeTab, setActiveTab] = useState<QueryModalTab>('overview');
  const [detailsSubTab, setDetailsSubTab] = useState<DetailsSubTab>('performance');
  const [analyticsSubTab, setAnalyticsSubTab] = useState<AnalyticsSubTab>('scan_efficiency');

  // Internal query override — when user clicks a query ID in the History tab,
  // we build a QuerySeries from the SimilarQuery and switch to it
  const [queryOverride, setQueryOverride] = useState<QuerySeries | null>(null);
  const activeQuery = queryOverride ?? query;

  // Per-tab state hooks. Each owns its data + fetcher + reset on query change.
  const detail = useQueryDetail(activeQuery);
  const logsHook = useQueryLogs(activeQuery, activeTab === 'logs' || activeTab === 'xray');
  const spansHook = useQuerySpans(activeQuery, activeTab === 'spans');
  const flame = useQueryFlamegraph(activeQuery, activeTab === 'flamegraph');
  const threadsHook = useQueryThreads(activeQuery, activeTab === 'threads');
  const similar = useSimilarQueries(query, activeQuery, detail.queryDetail, activeTab === 'history');
  const timelines = useQueryTimelines(query, similar.similarQueries);
  const topology = useQueryTopology(activeQuery, detail.queryDetail);

  // Reset orchestration state when the root query changes (modal opens with new query)
  useEffect(() => {
    setQueryOverride(null);
    setActiveTab('overview');
    setDetailsSubTab('performance');
    setAnalyticsSubTab('scan_efficiency');
  }, [query?.query_id]);

  // Reset tab state when navigating via history (queryOverride changes)
  useEffect(() => {
    if (!queryOverride) return;
    setActiveTab('overview');
    setDetailsSubTab('performance');
    setAnalyticsSubTab('scan_efficiency');
  }, [queryOverride?.query_id]);

  // Force off X-Ray when experimental is disabled mid-session
  useEffect(() => {
    if (!experimentalEnabled && activeTab === 'xray') setActiveTab('overview');
  }, [experimentalEnabled, activeTab]);

  // Navigate to a related query by ID (parent or child in distributed topology)
  const navigateToQuery = useCallback(async (queryId: string) => {
    if (!services) return;
    try {
      const d = await services.queryAnalyzer.getQueryDetail(queryId);
      if (!d) return;
      const durationMs = Number(d.query_duration_ms) || 0;
      const startMs = new Date(d.query_start_time).getTime();
      setQueryOverride({
        query_id: d.query_id,
        label: d.query || '',
        user: d.user,
        peak_memory: Number(d.memory_usage) || 0,
        duration_ms: durationMs,
        cpu_us: (d.ProfileEvents?.['UserTimeMicroseconds'] || 0) + (d.ProfileEvents?.['SystemTimeMicroseconds'] || 0),
        net_send: d.ProfileEvents?.['NetworkSendBytes'] || 0,
        net_recv: d.ProfileEvents?.['NetworkReceiveBytes'] || 0,
        disk_read: Number(d.read_bytes) || 0,
        disk_write: d.ProfileEvents?.['OSWriteBytes'] || 0,
        start_time: d.query_start_time,
        end_time: new Date(startMs + durationMs).toISOString(),
        exception_code: d.exception_code,
        exception: d.exception,
        points: [],
      });
    } catch { /* ignore navigation errors */ }
  }, [services]);

  if (!query) return null;

  // After the null guard, activeQuery is guaranteed non-null
  const q = activeQuery!;

  const queryDetail = detail.queryDetail;
  const isSelectQuery = queryDetail?.query_kind?.toUpperCase() === 'SELECT';

  const tabs: { key: QueryModalTab; label: string; unavailable?: boolean; reason?: string; experimental?: boolean }[] = [
    { key: 'overview', label: 'Overview' },
    { key: 'details', label: 'Details' },
    { key: 'analytics', label: 'Analytics' },
    { key: 'logs', label: 'Logs', unavailable: !hasTextLog, reason: 'system.text_log' },
    { key: 'history', label: 'History', unavailable: !hasQueryLog, reason: 'system.query_log' },
  ];
  if (isSelectQuery) tabs.push({ key: 'pipeline', label: 'Pipeline' });
  if (experimentalEnabled) {
    tabs.push({ key: 'xray', label: 'X-Ray', unavailable: !hasProcessesHistory, reason: 'tracehouse.processes_history', experimental: true });
  }
  tabs.push(
    { key: 'threads', label: 'Threads', unavailable: !hasQueryThreadLog, reason: 'system.query_thread_log' },
    { key: 'flamegraph', label: 'Flamegraph', unavailable: !hasTraceLog, reason: 'system.trace_log' },
    { key: 'spans', label: 'Spans', unavailable: !hasOpenTelemetry, reason: 'system.opentelemetry_span_log' },
  );

  return (
    <ModalWrapper isOpen={!!query} onClose={onClose} maxWidth={1400}>
      <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
        {/* Header */}
        <div style={{
          padding: '16px 32px 12px 32px',
          borderBottom: '1px solid var(--border-accent)',
          background: 'var(--bg-card)',
          flexShrink: 0,
        }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
              <h2 style={{
                fontSize: 18,
                fontWeight: 500,
                color: 'var(--text-primary)',
                letterSpacing: '0.5px',
                fontFamily: 'monospace',
                margin: 0,
              }}>
                {queryDetail?.query_kind || 'Query'} Details
              </h2>
            </div>
            <button
              onClick={onClose}
              style={{
                padding: 6,
                background: 'transparent',
                border: 'none',
                color: 'var(--text-muted)',
                cursor: 'pointer',
                borderRadius: 8,
                transition: 'all 0.15s ease',
              }}
              onMouseEnter={(e) => {
                e.currentTarget.style.background = 'var(--bg-card-hover)';
                e.currentTarget.style.color = 'var(--text-primary)';
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.background = 'transparent';
                e.currentTarget.style.color = 'var(--text-muted)';
              }}
            >
              <svg width="16" height="16" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          </div>

          {/* Tabs */}
          <div style={{ display: 'flex', gap: 2 }}>
            {tabs.map((tab) => (
              <button
                key={tab.key}
                onClick={() => !tab.unavailable && setActiveTab(tab.key)}
                title={tab.unavailable ? `Requires ${tab.reason} (not available)` : undefined}
                style={{
                  fontFamily: 'monospace',
                  padding: '8px 16px',
                  fontSize: 10,
                  textTransform: 'uppercase',
                  letterSpacing: '1.5px',
                  borderRadius: '6px 6px 0 0',
                  borderTop: activeTab === tab.key ? '1px solid var(--border-accent)' : '1px solid transparent',
                  borderLeft: activeTab === tab.key ? '1px solid var(--border-accent)' : '1px solid transparent',
                  borderRight: activeTab === tab.key ? '1px solid var(--border-accent)' : '1px solid transparent',
                  borderBottom: 'none',
                  background: activeTab === tab.key ? 'rgba(88, 166, 255, 0.12)' : 'transparent',
                  color: tab.unavailable ? 'var(--text-muted)' : activeTab === tab.key ? 'var(--text-primary)' : 'var(--text-muted)',
                  cursor: tab.unavailable ? 'not-allowed' : 'pointer',
                  opacity: tab.unavailable ? 0.4 : 1,
                  transition: 'all 0.2s ease',
                  position: 'relative',
                  marginBottom: -1,
                }}
                onMouseEnter={(e) => {
                  if (activeTab !== tab.key && !tab.unavailable) {
                    e.currentTarget.style.color = 'var(--text-tertiary)';
                    e.currentTarget.style.background = 'var(--bg-card)';
                  }
                }}
                onMouseLeave={(e) => {
                  if (activeTab !== tab.key && !tab.unavailable) {
                    e.currentTarget.style.color = 'var(--text-muted)';
                    e.currentTarget.style.background = 'transparent';
                  }
                }}
              >
                {tab.label}
                {tab.experimental && (
                  <span style={{
                    position: 'absolute', top: -4, right: -2,
                    fontSize: 7, fontWeight: 700, color: '#f0883e',
                    background: 'var(--bg-tertiary)', border: '1px solid rgba(240,136,62,0.3)',
                    borderRadius: 3, padding: '0 3px', lineHeight: '12px',
                    textTransform: 'uppercase', letterSpacing: '0.3px',
                  }}>exp</span>
                )}
                {activeTab === tab.key && (
                  <div style={{
                    position: 'absolute',
                    bottom: 0,
                    left: 0,
                    right: 0,
                    height: 2,
                    background: '#58a6ff',
                    borderRadius: '2px 2px 0 0',
                  }} />
                )}
              </button>
            ))}
          </div>
        </div>

        {/* Content */}
        <div style={{ flex: 1, overflow: activeTab === 'history' ? 'hidden' : 'auto', padding: ['logs', 'spans', 'details', 'pipeline', 'history', 'analytics', 'xray'].includes(activeTab) ? 0 : 24 }}>
          {activeTab === 'overview' && (
            <OverviewTab
              q={q}
              activeQuery={activeQuery!}
              queryDetail={queryDetail}
              isSelectQuery={isSelectQuery}
              topologyCoordinator={topology.coordinator}
              subQueries={topology.subQueries}
              isLoadingSubQueries={topology.isLoading}
              onNavigateToQuery={navigateToQuery}
            />
          )}

          {activeTab === 'logs' && (
            <div style={{ height: '100%' }}>
              <TraceLogViewer
                logs={logsHook.logs}
                isLoading={logsHook.isLoading}
                error={logsHook.error}
                filter={logsHook.filter}
                onFilterChange={(newFilter) => logsHook.setFilter(prev => ({ ...prev, ...newFilter }))}
                onRefresh={logsHook.refresh}
                queryId={activeQuery?.query_id}
                queryStartTime={activeQuery?.start_time}
                queryEndTime={activeQuery?.end_time}
              />
            </div>
          )}

          {activeTab === 'spans' && (
            <div style={{ height: '100%' }}>
              <SpansTab
                spans={spansHook.spans}
                isLoading={spansHook.isLoading}
                error={spansHook.error}
                onRefresh={spansHook.refresh}
              />
            </div>
          )}

          {activeTab === 'flamegraph' && (
            <div style={{ height: '100%' }}>
              <SpeedscopeViewer
                folded={flame.folded}
                isLoading={flame.isLoading}
                error={flame.error}
                unavailableReason={flame.unavailable}
                onRefresh={flame.refresh}
                profileType={flame.type}
                onTypeChange={flame.onTypeChange}
              />
            </div>
          )}

          {activeTab === 'details' && (
            <DetailsTab
              detailsSubTab={detailsSubTab}
              onSubTabChange={setDetailsSubTab}
              queryDetail={queryDetail}
              isLoadingDetail={detail.isLoading}
              onFetchSettingsDefaults={detail.fetchSettingsDefaults}
            />
          )}

          {activeTab === 'analytics' && (
            <AnalyticsTab
              analyticsSubTab={analyticsSubTab}
              onSubTabChange={setAnalyticsSubTab}
              queryDetail={queryDetail}
              isLoadingDetail={detail.isLoading}
            />
          )}

          {activeTab === 'history' && (
            (() => {
              const isRunning = (q as QuerySeries & { is_running?: boolean }).is_running === true;
              if (isRunning) {
                return (
                  <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: 200 }}>
                    <div style={{ textAlign: 'center' }}>
                      <div style={{ fontSize: 14, color: 'var(--text-tertiary)', marginBottom: 8 }}>Query is still running</div>
                      <div style={{ fontSize: 12, color: 'var(--text-muted)' }}>History will be available after the query completes</div>
                    </div>
                  </div>
                );
              }
              return (
                <HistoryTab
                  similarQueries={similar.similarQueries}
                  isLoading={similar.isLoading || (detail.isLoading && !queryDetail)}
                  error={similar.error}
                  onRefresh={similar.refresh}
                  cpuTimeline={timelines.cpuTimeline}
                  memTimeline={timelines.memTimeline}
                  isLoadingCpu={timelines.isLoadingCpu}
                  limit={similar.limit}
                  onLimitChange={similar.changeLimit}
                  hashMode={similar.hashMode}
                  currentQueryId={query?.query_id}
                  onHashModeChange={similar.changeHashMode}
                  onSelectQuery={(sq) => {
                    const durationMs = Number(sq.query_duration_ms) || 0;
                    const startMs = new Date(sq.query_start_time).getTime();
                    setQueryOverride({
                      query_id: sq.query_id,
                      label: q.label ?? '',
                      user: sq.user,
                      peak_memory: Number(sq.memory_usage) || 0,
                      duration_ms: durationMs,
                      cpu_us: Number(sq.cpu_time_us) || 0,
                      net_send: 0,
                      net_recv: 0,
                      disk_read: 0,
                      disk_write: 0,
                      start_time: sq.query_start_time,
                      end_time: new Date(startMs + durationMs).toISOString(),
                      exception_code: sq.exception_code,
                      exception: sq.exception,
                      points: [],
                    });
                  }}
                  onViewInTimeTravel={queryDetail?.normalized_query_hash ? () => {
                    const hash = queryDetail.normalized_query_hash;
                    if (onViewInTimeTravel) {
                      onClose();
                      onViewInTimeTravel(String(hash));
                    } else {
                      onClose();
                      navigate(`/timetravel?nqh=${hash}`);
                    }
                  } : undefined}
                />
              );
            })()
          )}

          {activeTab === 'pipeline' && q && (
            <PipelineProfileTab
              querySQL={q.label}
              queryId={q.query_id}
              database={queryDetail?.current_database}
              eventDate={activeQuery?.start_time}
            />
          )}

          {activeTab === 'threads' && (
            <div style={{ height: '100%' }}>
              <ThreadBreakdownSection
                threads={threadsHook.threads}
                isLoading={threadsHook.isLoading || !threadsHook.fetched}
                error={threadsHook.error}
                onRefresh={threadsHook.refresh}
              />
            </div>
          )}

          {activeTab === 'xray' && q && (
            <div style={{ height: '100%' }}>
              <XRayTab
                queryId={q.query_id}
                logs={logsHook.logs}
                queryStartTime={q.start_time}
              />
            </div>
          )}
        </div>
      </div>
    </ModalWrapper>
  );
};
