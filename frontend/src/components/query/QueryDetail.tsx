/**
 * QueryDetail - Component for displaying detailed information about a selected query
 * 
 * Shows full query text, resource metrics, and trace logs side by side.
 * Uses CSS variables for consistent theming with the rest of the app.
 */

import React, { useState, useEffect, useCallback, useMemo } from 'react';
import type { RunningQuery, QueryHistoryItem } from '../../stores/queryStore';
import { formatBytes, formatDuration, formatNumber } from '../../stores/queryStore';
import { formatDurationMs, formatMicroseconds } from '../../utils/formatters';
import { useClickHouseServices } from '../../providers/ClickHouseProvider';
import { useCapabilityCheck } from '../shared/RequiresCapability';
import type { QueryThreadBreakdown } from '@tracehouse/core';
import type { TraceLog, TraceLogFilter, OpenTelemetrySpan } from '../../stores/traceStore';
import { TraceLogViewer } from '../tracing/TraceLogViewer';
import { SpeedscopeViewer } from '../tracing/SpeedscopeViewer';
import { highlightSQL } from '../../utils/sqlHighlighter';
import DOMPurify from 'dompurify';

/** SQL syntax highlighter using the shared highlightSQL utility */
const SqlHighlight: React.FC<{ sql: string }> = ({ sql }) => {
  const html = useMemo(() => DOMPurify.sanitize(highlightSQL(sql)), [sql]);
  return (
    <pre className="text-xs whitespace-pre-wrap break-all font-mono" style={{ color: 'var(--text-secondary)', margin: 0 }}
      // nosemgrep: react-dangerouslysetinnerhtml -- sanitized via DOMPurify above
      dangerouslySetInnerHTML={{ __html: html }} />
  );
};

interface QueryDetailProps {
  query: RunningQuery | QueryHistoryItem | null;
  queryType: 'running' | 'history' | null;
  onClose: () => void;
  onKillQuery?: (queryId: string) => void;
  isKillingQuery?: boolean;
}

function isRunningQuery(query: RunningQuery | QueryHistoryItem): query is RunningQuery {
  return 'elapsed_seconds' in query && 'progress' in query;
}

/** Metric card matching the stat-card pattern used elsewhere */
const MetricItem: React.FC<{ label: string; value: React.ReactNode; highlight?: boolean }> = ({ label, value, highlight }) => (
  <div className="rounded-lg p-3" style={{ background: 'var(--bg-tertiary)' }}>
    <div className="text-xs" style={{ color: 'var(--text-muted)' }}>{label}</div>
    <div className="font-semibold text-sm mt-0.5" style={{ color: highlight ? '#58a6ff' : 'var(--text-primary)' }}>
      {value}
    </div>
  </div>
);

const ProgressIndicator: React.FC<{ progress: number }> = ({ progress }) => {
  const pct = Math.min(Math.max(progress * 100, 0), 100);
  return (
    <div className="mt-3">
      <div className="flex justify-between text-xs mb-1">
        <span style={{ color: 'var(--text-muted)' }}>Progress</span>
        <span className="font-bold" style={{ color: '#58a6ff' }}>{pct.toFixed(1)}%</span>
      </div>
      <div className="h-2 rounded-full" style={{ background: 'var(--bg-tertiary)' }}>
        <div className="h-2 rounded-full transition-all" style={{ width: `${pct}%`, background: '#58a6ff' }} />
      </div>
    </div>
  );
};

const EfficiencyDisplay: React.FC<{ readRows: number; resultRows: number; efficiencyScore?: number | null }> = ({ readRows, resultRows, efficiencyScore }) => {
  // Pruning effectiveness: % of marks skipped by the primary key index
  const hasPruning = efficiencyScore !== undefined && efficiencyScore !== null;
  const pruningColor = hasPruning
    ? (efficiencyScore >= 80 ? '#3fb950' : efficiencyScore >= 50 ? '#d29922' : '#f85149')
    : 'var(--text-muted)';
  const pruningLabel = hasPruning
    ? (efficiencyScore >= 80 ? 'Excellent' : efficiencyScore >= 50 ? 'Good' : efficiencyScore >= 20 ? 'Fair' : 'Poor')
    : 'N/A';

  const scanRatio = readRows > 0 ? (readRows / Math.max(resultRows, 1)).toFixed(2) : '0';

  return (
    <div className="grid grid-cols-2 gap-3 mt-3">
      <div className="rounded-lg p-3" style={{ background: 'var(--bg-tertiary)' }}>
        <div className="text-xs" style={{ color: 'var(--text-muted)' }}>Index Pruning</div>
        <div className="text-xl font-bold" style={{ color: pruningColor }}>
          {hasPruning ? `${efficiencyScore.toFixed(1)}%` : '—'}
        </div>
        <div className="text-xs" style={{ color: pruningColor }}>{pruningLabel}</div>
      </div>
      <div className="rounded-lg p-3" style={{ background: 'var(--bg-tertiary)' }}>
        <div className="text-xs" style={{ color: 'var(--text-muted)' }}>Scan Ratio</div>
        <div className="text-xl font-bold" style={{ color: 'var(--text-primary)' }}>{scanRatio}:1</div>
        <div className="text-xs" style={{ color: 'var(--text-muted)' }}>rows read per result</div>
      </div>
    </div>
  );
}

const ErrorDisplay: React.FC<{ exception: string }> = ({ exception }) => (
  <div className="mt-3 p-3 rounded-lg" style={{ background: 'rgba(248, 81, 73, 0.1)', border: '1px solid rgba(248, 81, 73, 0.4)' }}>
    <div className="text-xs font-medium mb-1" style={{ color: '#f85149' }}>Error</div>
    <pre className="text-xs whitespace-pre-wrap break-all font-mono" style={{ color: '#ffa198' }}>{exception}</pre>
  </div>
);

const RunningQueryMetrics: React.FC<{ query: RunningQuery; onKill?: () => void; isKilling?: boolean }> = ({ query, onKill, isKilling }) => (
  <div>
    <div className="grid grid-cols-2 gap-3">
      <MetricItem label="Query ID" value={<span className="font-mono text-xs break-all">{query.query_id}</span>} />
      <MetricItem label="User" value={query.user} />
      <MetricItem label="Elapsed" value={formatDuration(query.elapsed_seconds)} />
      <MetricItem label="Memory" value={formatBytes(query.memory_usage)} />
      <MetricItem label="Rows Read" value={formatNumber(query.read_rows)} />
      <MetricItem label="Bytes Read" value={formatBytes(query.read_bytes)} />
    </div>
    {/* Distributed query info */}
    {(query.is_initial_query !== undefined || query.hostname) && (
      <div className="mt-3">
        <div className="text-xs font-medium mb-2" style={{ color: 'var(--text-muted)' }}>Origin</div>
        <div className="grid grid-cols-2 gap-3">
          {query.hostname && (
            <MetricItem label="Server" value={
              <span className="font-mono text-xs">{query.hostname}</span>
            } />
          )}
          <MetricItem label="Role" value={
            <span style={{ 
              color: query.is_initial_query === 1 ? '#58a6ff' : '#d29922',
              fontSize: 11,
            }}>
              {query.is_initial_query === 1 ? 'Coordinator' : 'Shard sub-query'}
            </span>
          } />
          {query.is_initial_query === 0 && query.initial_query_id && (
            <MetricItem label="Parent Query" value={
              <span className="font-mono text-xs break-all" style={{ color: '#58a6ff' }}>
                {query.initial_query_id.slice(0, 12)}…
              </span>
            } />
          )}
        </div>
      </div>
    )}
    <ProgressIndicator progress={query.progress} />
    {onKill && (
      <button
        onClick={onKill}
        disabled={isKilling}
        className="w-full mt-3 px-4 py-2 text-sm font-medium rounded-md transition-colors"
        style={{
          background: isKilling ? 'var(--bg-tertiary)' : '#da3633',
          color: isKilling ? 'var(--text-muted)' : '#fff',
          cursor: isKilling ? 'not-allowed' : 'pointer',
        }}
      >
        {isKilling ? 'Killing...' : 'Kill Query'}
      </button>
    )}
  </div>
);

const HistoryQueryMetrics: React.FC<{ query: QueryHistoryItem }> = ({ query }) => {
  const fmtMs = formatDurationMs;
  const fmtTime = (ts: string) => new Date(ts).toLocaleString();
  
  // Calculate derived metrics
  const markCacheHitRate = (query.mark_cache_hits && query.mark_cache_misses) 
    ? (query.mark_cache_hits / (query.mark_cache_hits + query.mark_cache_misses)) * 100 
    : null;
  
  const parallelism = (query.user_time_us && query.system_time_us && query.real_time_us && query.real_time_us > 0)
    ? (query.user_time_us + query.system_time_us) / query.real_time_us
    : null;
  
  const ioWaitPct = (query.io_wait_us && query.real_time_us && query.real_time_us > 0)
    ? (query.io_wait_us / query.real_time_us) * 100
    : null;

  // Calculate selectivity metrics (lower % = better index pruning)
  const partsSelectivity = (query.selected_parts !== undefined && query.selected_parts_total && query.selected_parts_total > 0)
    ? (query.selected_parts / query.selected_parts_total) * 100
    : null;
  
  const marksSelectivity = (query.selected_marks !== undefined && query.selected_marks_total && query.selected_marks_total > 0)
    ? (query.selected_marks / query.selected_marks_total) * 100
    : null;

  // Selectivity color: lower is better (more pruning)
  const getSelectivityColor = (pct: number) => 
    pct <= 10 ? '#3fb950' : pct <= 50 ? '#d29922' : '#f85149';

  return (
    <div>
      <div className="grid grid-cols-2 gap-3">
        <MetricItem label="Query ID" value={<span className="font-mono text-xs break-all">{query.query_id}</span>} />
        <MetricItem label="User" value={query.user} />
        <MetricItem label="Start Time" value={fmtTime(query.query_start_time)} />
        <MetricItem label="Duration" value={fmtMs(query.query_duration_ms)} />
        <MetricItem label="Status" value={
          <span style={{ color: query.exception ? '#f85149' : '#3fb950' }}>
            {query.exception ? 'Failed' : 'Success'}
          </span>
        } />
        <MetricItem label="Peak Memory" value={formatBytes(query.memory_usage)} />
        <MetricItem label="Rows Read" value={formatNumber(query.read_rows)} />
        <MetricItem label="Bytes Read" value={formatBytes(query.read_bytes)} />
        <MetricItem label="Result Rows" value={formatNumber(query.result_rows)} />
        <MetricItem label="Result Bytes" value={formatBytes(query.result_bytes)} />
      </div>
      
      {/* Client / Distributed query info */}
      <div className="mt-3">
        <div className="text-xs font-medium mb-2" style={{ color: 'var(--text-muted)' }}>Origin</div>
        <div className="grid grid-cols-2 gap-3">
          {query.hostname && (
            <MetricItem label="Server" value={
              <span className="font-mono text-xs">{query.hostname}</span>
            } />
          )}
          {query.client_hostname && (
            <MetricItem label="Client Host" value={
              <span className="font-mono text-xs">{query.client_hostname}</span>
            } />
          )}
          {query.is_initial_query !== undefined && (
            <MetricItem label="Role" value={
              <span style={{ 
                color: query.is_initial_query === 1 ? '#58a6ff' : '#d29922',
                fontSize: 11,
              }}>
                {query.is_initial_query === 1 ? 'Coordinator' : 'Shard sub-query'}
              </span>
            } />
          )}
          {query.is_initial_query === 0 && query.initial_query_id && (
            <MetricItem label="Parent Query" value={
              <span className="font-mono text-xs break-all" style={{ color: '#58a6ff' }}>
                {query.initial_query_id.slice(0, 12)}…
              </span>
            } />
          )}
          {query.is_initial_query === 0 && query.initial_address && (
            <MetricItem label="Coordinator Address" value={
              <span className="font-mono text-xs">{query.initial_address}</span>
            } />
          )}
        </div>
      </div>
      
      <EfficiencyDisplay readRows={query.read_rows} resultRows={query.result_rows} efficiencyScore={query.efficiency_score} />
      
      {/* Index Selectivity - shows how well the primary key pruned data */}
      {(partsSelectivity !== null || marksSelectivity !== null) && (
        <div className="mt-3">
          <div className="text-xs font-medium mb-2" style={{ color: 'var(--text-muted)' }}>Index Selectivity</div>
          <div className="grid grid-cols-2 gap-3">
            {partsSelectivity !== null && (
              <div className="rounded-lg p-3" style={{ background: 'var(--bg-tertiary)' }}>
                <div className="text-xs" style={{ color: 'var(--text-muted)' }}>Parts Scanned</div>
                <div className="text-lg font-bold" style={{ color: getSelectivityColor(partsSelectivity) }}>
                  {partsSelectivity.toFixed(1)}%
                </div>
                <div className="text-xs" style={{ color: 'var(--text-muted)' }}>
                  {formatNumber(query.selected_parts!)} / {formatNumber(query.selected_parts_total!)}
                </div>
              </div>
            )}
            {marksSelectivity !== null && (
              <div className="rounded-lg p-3" style={{ background: 'var(--bg-tertiary)' }}>
                <div className="text-xs" style={{ color: 'var(--text-muted)' }}>Marks Scanned</div>
                <div className="text-lg font-bold" style={{ color: getSelectivityColor(marksSelectivity) }}>
                  {marksSelectivity.toFixed(1)}%
                </div>
                <div className="text-xs" style={{ color: 'var(--text-muted)' }}>
                  {formatNumber(query.selected_marks!)} / {formatNumber(query.selected_marks_total!)}
                </div>
              </div>
            )}
          </div>
        </div>
      )}
      
      {/* Performance Metrics */}
      {(markCacheHitRate !== null || parallelism !== null || ioWaitPct !== null || query.cpu_time_us !== undefined) && (
        <div className="mt-3">
          <div className="text-xs font-medium mb-2" style={{ color: 'var(--text-muted)' }}>Performance</div>
          <div className="grid grid-cols-2 gap-3">
            {markCacheHitRate !== null && (
              <MetricItem 
                label="Mark Cache Hit" 
                value={`${markCacheHitRate.toFixed(1)}%`} 
              />
            )}
            {parallelism !== null && (
              <MetricItem label="Parallelism" value={`${parallelism.toFixed(2)}x`} />
            )}
            {ioWaitPct !== null && (
              <MetricItem 
                label="IO Wait" 
                value={`${ioWaitPct.toFixed(1)}%`} 
              />
            )}
            {query.cpu_time_us !== undefined && (
              <MetricItem label="CPU Time" value={fmtMs(query.cpu_time_us / 1000)} />
            )}
          </div>
        </div>
      )}
      
      {query.exception && <ErrorDisplay exception={query.exception} />}
    </div>
  );
};

const QueryLogsSection: React.FC<{ queryId: string; eventDate?: string; autoExpand?: boolean }> = ({ queryId, eventDate, autoExpand }) => {
  const services = useClickHouseServices();
  const hasQueryThreadLog = useCapabilityCheck(['query_thread_log']).available;
  const hasIntrospectionFunctions = useCapabilityCheck(['introspection_functions']).available;
  const [activeTab, setActiveTab] = useState<'logs' | 'spans' | 'flamegraph' | 'threads'>('logs');
  const [logs, setLogs] = useState<TraceLog[]>([]);
  const [spans, setSpans] = useState<OpenTelemetrySpan[]>([]);
  const [threads, setThreads] = useState<QueryThreadBreakdown[]>([]);
  const [flamegraphFolded, setFlamegraphFolded] = useState('');
  const [flamegraphUnavailable, setFlamegraphUnavailable] = useState<string | undefined>();
  const [flamegraphType, setFlamegraphType] = useState<'CPU' | 'Real' | 'Memory'>('CPU');
  const [isLoadingLogs, setIsLoadingLogs] = useState(false);
  const [isLoadingSpans, setIsLoadingSpans] = useState(false);
  const [isLoadingFlamegraph, setIsLoadingFlamegraph] = useState(false);
  const [isLoadingThreads, setIsLoadingThreads] = useState(false);
  const [logsError, setLogsError] = useState<string | null>(null);
  const [spansError, setSpansError] = useState<string | null>(null);
  const [flamegraphError, setFlamegraphError] = useState<string | null>(null);
  const [threadsError, setThreadsError] = useState<string | null>(null);
  const [filter, setFilter] = useState<TraceLogFilter>({});

  const fetchLogs = useCallback(async () => {
    if (!services) return;
    setIsLoadingLogs(true);
    setLogsError(null);
    try {
      const result = await services.traceService.getQueryLogs(queryId, undefined, eventDate);
      setLogs(result);
    } catch (e) {
      setLogsError(e instanceof Error ? e.message : 'Failed to fetch logs');
    } finally {
      setIsLoadingLogs(false);
    }
  }, [services, queryId, eventDate]);

  const fetchSpans = useCallback(async () => {
    if (!services) return;
    setIsLoadingSpans(true);
    setSpansError(null);
    try {
      const result = await services.traceService.getOpenTelemetrySpans(queryId);
      setSpans(result);
    } catch (e) {
      setSpansError(e instanceof Error ? e.message : 'Failed to fetch spans');
    } finally {
      setIsLoadingSpans(false);
    }
  }, [services, queryId]);

  const fetchFlamegraph = useCallback(async (type: 'CPU' | 'Real' | 'Memory' = flamegraphType) => {
    if (!services) return;
    setIsLoadingFlamegraph(true);
    setFlamegraphError(null);
    setFlamegraphUnavailable(undefined);
    try {
      const result = await services.traceService.getFlamegraphFolded(queryId, type, eventDate);
      setFlamegraphFolded(result.folded);
      setFlamegraphUnavailable(result.unavailableReason);
    } catch (e) {
      setFlamegraphError(e instanceof Error ? e.message : 'Failed to fetch flamegraph data');
    } finally {
      setIsLoadingFlamegraph(false);
    }
  }, [services, queryId, eventDate, flamegraphType]);

  const handleFlamegraphTypeChange = useCallback((newType: 'CPU' | 'Real' | 'Memory') => {
    setFlamegraphType(newType);
    setFlamegraphFolded('');
    setFlamegraphUnavailable(undefined);
    setFlamegraphError(null);
    fetchFlamegraph(newType);
  }, [fetchFlamegraph]);

  const fetchThreads = useCallback(async () => {
    if (!services) return;
    setIsLoadingThreads(true);
    setThreadsError(null);
    try {
      const result = await services.queryAnalyzer.getQueryThreadBreakdown(queryId, eventDate);
      setThreads(result);
    } catch (e) {
      setThreadsError(e instanceof Error ? e.message : 'Failed to fetch thread breakdown');
    } finally {
      setIsLoadingThreads(false);
    }
  }, [services, queryId, eventDate]);

  useEffect(() => {
    setLogs([]);
    setSpans([]);
    setThreads([]);
    setFlamegraphFolded('');
    setFlamegraphUnavailable(undefined);
    setFlamegraphType('CPU');
    setLogsError(null);
    setSpansError(null);
    setFlamegraphError(null);
    setThreadsError(null);
    setFilter({});
    if (autoExpand) {
      fetchLogs();
      fetchSpans();
      fetchFlamegraph();
      if (hasQueryThreadLog) fetchThreads();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps -- fetch callbacks depend on state
    // this effect resets (flamegraphType), so including them would cause an infinite loop
  }, [queryId, autoExpand, hasQueryThreadLog]);

  const fmtDur = (ms: number) => ms >= 1000 ? `${(ms / 1000).toFixed(2)}s` : ms >= 1 ? `${ms.toFixed(2)}ms` : `${(ms * 1000).toFixed(0)}µs`;

  return (
    <div className="h-full flex flex-col">
      {/* Tab header */}
      <div style={{ 
        padding: '8px 12px',
        borderBottom: '1px solid var(--border-primary)',
        background: 'var(--bg-secondary)',
        display: 'flex',
        gap: 4,
      }}>
        <button
          onClick={() => setActiveTab('logs')}
          style={{
            padding: '6px 12px',
            fontSize: 11,
            fontWeight: 500,
            borderRadius: 4,
            border: 'none',
            background: activeTab === 'logs' ? 'rgba(88, 166, 255, 0.15)' : 'transparent',
            color: activeTab === 'logs' ? '#58a6ff' : 'var(--text-muted)',
            cursor: 'pointer',
            transition: 'all 0.15s ease',
          }}
        >
          Logs {logs.length > 0 && `(${logs.length})`}
        </button>
        <button
          onClick={() => setActiveTab('spans')}
          style={{
            padding: '6px 12px',
            fontSize: 11,
            fontWeight: 500,
            borderRadius: 4,
            border: 'none',
            background: activeTab === 'spans' ? 'rgba(88, 166, 255, 0.15)' : 'transparent',
            color: activeTab === 'spans' ? '#58a6ff' : 'var(--text-muted)',
            cursor: 'pointer',
            transition: 'all 0.15s ease',
          }}
        >
          Spans {spans.length > 0 && `(${spans.length})`}
        </button>
        {hasIntrospectionFunctions && (
        <button
          onClick={() => setActiveTab('flamegraph')}
          style={{
            padding: '6px 12px',
            fontSize: 11,
            fontWeight: 500,
            borderRadius: 4,
            border: 'none',
            background: activeTab === 'flamegraph' ? 'rgba(88, 166, 255, 0.15)' : 'transparent',
            color: activeTab === 'flamegraph' ? '#58a6ff' : 'var(--text-muted)',
            cursor: 'pointer',
            transition: 'all 0.15s ease',
          }}
        >
          Flamegraph
        </button>
        )}
        {hasQueryThreadLog && (
          <button
            onClick={() => { setActiveTab('threads'); if (threads.length === 0 && !isLoadingThreads) fetchThreads(); }}
            style={{
              padding: '6px 12px',
              fontSize: 11,
              fontWeight: 500,
              borderRadius: 4,
              border: 'none',
              background: activeTab === 'threads' ? 'rgba(88, 166, 255, 0.15)' : 'transparent',
              color: activeTab === 'threads' ? '#58a6ff' : 'var(--text-muted)',
              cursor: 'pointer',
              transition: 'all 0.15s ease',
            }}
          >
            Threads {threads.length > 0 && `(${threads.length})`}
          </button>
        )}
      </div>

      {/* Content */}
      <div className="flex-1 min-h-0">
        {activeTab === 'logs' && (
          <TraceLogViewer
            logs={logs}
            isLoading={isLoadingLogs}
            error={logsError}
            filter={filter}
            onFilterChange={(partial) => setFilter(prev => ({ ...prev, ...partial }))}
            onRefresh={fetchLogs}
            queryId={queryId}
          />
        )}
        {activeTab === 'spans' && (
          <SpansSection
            spans={spans}
            isLoading={isLoadingSpans}
            error={spansError}
            onRefresh={fetchSpans}
            fmtDur={fmtDur}
          />
        )}
        {activeTab === 'flamegraph' && (
          <SpeedscopeViewer
            folded={flamegraphFolded}
            isLoading={isLoadingFlamegraph}
            error={flamegraphError}
            unavailableReason={flamegraphUnavailable}
            onRefresh={fetchFlamegraph}
            profileType={flamegraphType}
            onTypeChange={handleFlamegraphTypeChange}
          />
        )}
        {activeTab === 'threads' && (
          <ThreadBreakdownSection
            threads={threads}
            isLoading={isLoadingThreads}
            error={threadsError}
            onRefresh={fetchThreads}
          />
        )}
      </div>
    </div>
  );
};

/** Simple spans viewer for QueryDetail - Work in Progress */
const SpansSection: React.FC<{
  spans: OpenTelemetrySpan[];
  isLoading: boolean;
  error: string | null;
  onRefresh: () => void;
  fmtDur: (ms: number) => string;
}> = ({ }) => {
  return (
    <div style={{ padding: 40, textAlign: 'center' }}>
      <div style={{ fontSize: 14, color: 'var(--text-tertiary)', marginBottom: 8 }}>
        Coming Soon
      </div>
      <div style={{ fontSize: 12, color: 'var(--text-muted)', maxWidth: 300, margin: '0 auto' }}>
        OpenTelemetry span visualization is currently under development.
      </div>
    </div>
  );
};

/** Thread breakdown table for per-thread attribution */
export const ThreadBreakdownSection: React.FC<{
  threads: QueryThreadBreakdown[];
  isLoading: boolean;
  error: string | null;
  onRefresh: () => void;
}> = ({ threads, isLoading, error, onRefresh }) => {
  const [sortCol, setSortCol] = useState<keyof QueryThreadBreakdown>('peak_memory_usage');
  const [sortAsc, setSortAsc] = useState(false);
  const [view, setView] = useState<'timeline' | 'table'>('timeline');
  const [showAll, setShowAll] = useState(false);

  const COLLAPSED_LIMIT = 50;
  const hasMany = threads.length > COLLAPSED_LIMIT;

  const handleSort = (col: keyof QueryThreadBreakdown) => {
    if (sortCol === col) setSortAsc(!sortAsc);
    else { setSortCol(col); setSortAsc(false); }
  };

  const sorted = useMemo(() => {
    const copy = [...threads];
    copy.sort((a, b) => {
      const av = a[sortCol], bv = b[sortCol];
      const cmp = typeof av === 'number' && typeof bv === 'number' ? av - bv : String(av).localeCompare(String(bv));
      return sortAsc ? cmp : -cmp;
    });
    return copy;
  }, [threads, sortCol, sortAsc]);

  const maxCpu = useMemo(() => Math.max(1, ...threads.map(t => t.cpu_time_us)), [threads]);
  const maxMem = useMemo(() => Math.max(1, ...threads.map(t => t.peak_memory_usage)), [threads]);

  // Timeline data: compute offsets relative to the true query start (initial_query_start_time)
  const timelineData = useMemo(() => {
    if (threads.length === 0) return { rows: [], totalDurationUs: 0 };

    // Parse microsecond timestamps: "2026-02-20 11:26:43.867493"
    const parseUs = (ts: string): number => {
      if (!ts) return 0;
      const dotIdx = ts.lastIndexOf('.');
      const baseDateStr = dotIdx >= 0 ? ts.substring(0, dotIdx) : ts;
      const usFrac = dotIdx >= 0 ? ts.substring(dotIdx + 1) : '0';
      const baseMs = new Date(baseDateStr.replace(' ', 'T') + 'Z').getTime();
      return baseMs * 1000 + parseInt(usFrac.padEnd(6, '0').substring(0, 6), 10);
    };

    const rows = threads.map(t => {
      const t0 = parseUs(t.initial_query_start_time_us); // true T=0
      const endUs = parseUs(t.event_time_us);
      const durationUs = t.query_duration_ms * 1000;
      const threadEndOffset = endUs - t0;
      const threadStartOffset = threadEndOffset - durationUs;
      return {
        ...t,
        startOffsetUs: Math.max(0, threadStartOffset),
        durationUs,
        endOffsetUs: Math.max(0, threadEndOffset),
      };
    });

    const totalDurationUs = Math.max(1, ...rows.map(r => r.endOffsetUs));
    // Sort by start offset for timeline view
    rows.sort((a, b) => a.startOffsetUs - b.startOffsetUs || b.durationUs - a.durationUs);
    return { rows, totalDurationUs };
  }, [threads]);

  if (isLoading) {
    return (
      <div style={{ padding: 40, textAlign: 'center', color: 'var(--text-muted)', fontSize: 12 }}>
        Loading thread breakdown…
      </div>
    );
  }

  if (error) {
    return (
      <div style={{ padding: 20 }}>
        <div style={{ color: '#f85149', fontSize: 12, marginBottom: 8 }}>{error}</div>
        <button onClick={onRefresh} style={{ fontSize: 11, color: '#58a6ff', background: 'none', border: 'none', cursor: 'pointer' }}>
          Retry
        </button>
      </div>
    );
  }

  if (threads.length === 0) {
    return (
      <div style={{ padding: 40, textAlign: 'center' }}>
        <div style={{ fontSize: 14, color: 'var(--text-tertiary)', marginBottom: 8 }}>No thread data</div>
        <div style={{ fontSize: 12, color: 'var(--text-muted)', maxWidth: 300, margin: '0 auto' }}>
          No per-thread data found. The query may have been too fast, or query_thread_log may not have flushed yet.
        </div>
        <button onClick={onRefresh} style={{ marginTop: 12, fontSize: 11, color: '#58a6ff', background: 'none', border: 'none', cursor: 'pointer' }}>
          Refresh
        </button>
      </div>
    );
  }

  const fmtUs = formatMicroseconds;
  const cols: { key: keyof QueryThreadBreakdown; label: string; fmt: (v: number) => string }[] = [
    { key: 'thread_name', label: 'Thread', fmt: () => '' },
    { key: 'cpu_time_us', label: 'CPU', fmt: fmtUs },
    { key: 'io_wait_us', label: 'IO Wait', fmt: fmtUs },
    { key: 'peak_memory_usage', label: 'Peak Mem', fmt: formatBytes },
    { key: 'read_rows', label: 'Read Rows', fmt: formatNumber },
    { key: 'read_bytes', label: 'Read', fmt: formatBytes },
    { key: 'written_bytes', label: 'Written', fmt: formatBytes },
  ];

  const arrow = (col: keyof QueryThreadBreakdown) => sortCol === col ? (sortAsc ? ' ↑' : ' ↓') : '';

  // Color by thread name
  const threadColors: Record<string, string> = {
    QueryPipelineEx: '#58a6ff',
    TCPHandler: '#f0883e',
    QueryPullPipeEx: '#7ee787',
    MergeTreeIndex: '#bc8cff',
    HTTPHandler: '#f0883e',
  };
  const getColor = (name: string) => threadColors[name] || '#8b949e';

  return (
    <div style={{ height: '100%', overflow: 'auto' }}>
      {/* Header */}
      <div style={{ padding: '8px 12px', display: 'flex', justifyContent: 'space-between', alignItems: 'center', borderBottom: '1px solid var(--border-primary)' }}>
        <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>
          {threads.length} thread{threads.length !== 1 ? 's' : ''}
          {hasMany && !showAll && <span style={{ color: 'var(--text-muted)', fontSize: 10 }}> (showing top {COLLAPSED_LIMIT})</span>}
        </span>
        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
          <div style={{ display: 'flex', gap: 2, background: 'var(--bg-tertiary)', borderRadius: 4, padding: 2 }}>
            {(['timeline', 'table'] as const).map(v => (
              <button
                key={v}
                onClick={() => setView(v)}
                style={{
                  padding: '3px 10px', fontSize: 10, border: 'none', borderRadius: 3, cursor: 'pointer',
                  background: view === v ? 'var(--bg-secondary)' : 'transparent',
                  color: view === v ? 'var(--text-primary)' : 'var(--text-muted)',
                  transition: 'all 0.15s',
                }}
              >
                {v === 'timeline' ? '▸ Timeline' : '▤ Table'}
              </button>
            ))}
          </div>
          <button onClick={onRefresh} style={{ fontSize: 11, color: '#58a6ff', background: 'none', border: 'none', cursor: 'pointer' }}>
            Refresh
          </button>
        </div>
      </div>

      {/* Timeline view */}
      {view === 'timeline' && (
        <div style={{ padding: '12px' }}>
          {/* Time axis */}
          <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4, marginLeft: 140, fontSize: 9, color: 'var(--text-muted)', fontFamily: 'var(--font-mono, monospace)' }}>
            <span>0</span>
            <span>{fmtUs(timelineData.totalDurationUs / 4)}</span>
            <span>{fmtUs(timelineData.totalDurationUs / 2)}</span>
            <span>{fmtUs(timelineData.totalDurationUs * 3 / 4)}</span>
            <span>{fmtUs(timelineData.totalDurationUs)}</span>
          </div>
          <div style={{ marginLeft: 140, height: 1, background: 'var(--border-primary)', marginBottom: 6 }} />

          {/* Thread bars */}
          {(showAll ? timelineData.rows : timelineData.rows.slice(0, COLLAPSED_LIMIT)).map((t, i) => {
            const leftPct = (t.startOffsetUs / timelineData.totalDurationUs) * 100;
            const widthPct = Math.max(0.5, (t.durationUs / timelineData.totalDurationUs) * 100);
            const color = getColor(t.thread_name);
            return (
              <div key={`${t.thread_id}-${i}`} style={{ display: 'flex', alignItems: 'center', height: 20, marginBottom: 2 }}>
                <div style={{
                  width: 136, flexShrink: 0, fontSize: 10, fontFamily: 'var(--font-mono, monospace)',
                  color: 'var(--text-secondary)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', paddingRight: 4,
                }} title={`${t.thread_name} #${t.thread_id}`}>
                  {t.thread_name || 'unknown'}
                  <span style={{ color: 'var(--text-muted)', fontSize: 9 }}> #{t.thread_id}</span>
                </div>
                <div style={{ flex: 1, position: 'relative', height: 14, background: 'var(--bg-tertiary)', borderRadius: 3, overflow: 'hidden' }}>
                  <div
                    title={`${t.thread_name} #${t.thread_id}: ${fmtUs(t.durationUs)} (CPU: ${fmtUs(t.cpu_time_us)}, Mem: ${formatBytes(t.peak_memory_usage)})`}
                    style={{
                      position: 'absolute',
                      left: `${leftPct}%`,
                      width: `${widthPct}%`,
                      height: '100%',
                      background: color,
                      borderRadius: 2,
                      opacity: 0.85,
                      transition: 'opacity 0.15s',
                      cursor: 'default',
                    }}
                    onMouseEnter={e => { e.currentTarget.style.opacity = '1'; }}
                    onMouseLeave={e => { e.currentTarget.style.opacity = '0.85'; }}
                  />
                </div>
                <div style={{ width: 60, flexShrink: 0, textAlign: 'right', fontSize: 10, fontFamily: 'var(--font-mono, monospace)', color: 'var(--text-muted)', paddingLeft: 6 }}>
                  {fmtUs(t.durationUs)}
                </div>
              </div>
            );
          })}

          {/* Show all / collapse toggle */}
          {hasMany && (
            <div style={{ marginTop: 8, marginLeft: 140, textAlign: 'center' }}>
              <button
                onClick={() => setShowAll(!showAll)}
                style={{ fontSize: 11, color: '#58a6ff', background: 'none', border: 'none', cursor: 'pointer', padding: '4px 12px' }}
              >
                {showAll ? `Show top ${COLLAPSED_LIMIT} only` : `Show all ${timelineData.rows.length} threads`}
              </button>
            </div>
          )}

          {/* Legend */}
          <div style={{ marginTop: 12, marginLeft: 140, display: 'flex', gap: 12, flexWrap: 'wrap' }}>
            {Object.entries(threadColors).map(([name, color]) => {
              if (!threads.some(t => t.thread_name === name)) return null;
              return (
                <div key={name} style={{ display: 'flex', alignItems: 'center', gap: 4, fontSize: 10, color: 'var(--text-muted)' }}>
                  <div style={{ width: 8, height: 8, borderRadius: 2, background: color }} />
                  {name}
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* Table view */}
      {view === 'table' && (
        <>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11 }}>
          <thead>
            <tr>
              {cols.map(c => (
                <th
                  key={c.key}
                  onClick={() => handleSort(c.key)}
                  style={{
                    padding: '6px 8px',
                    textAlign: c.key === 'thread_name' ? 'left' : 'right',
                    color: 'var(--text-muted)',
                    fontWeight: 500,
                    borderBottom: '1px solid var(--border-primary)',
                    cursor: 'pointer',
                    whiteSpace: 'nowrap',
                    position: 'sticky',
                    top: 0,
                    background: 'var(--bg-secondary)',
                    userSelect: 'none',
                  }}
                >
                  {c.label}{arrow(c.key)}
                </th>
              ))}
              <th style={{ padding: '6px 8px', textAlign: 'left', color: 'var(--text-muted)', fontWeight: 500, borderBottom: '1px solid var(--border-primary)', position: 'sticky', top: 0, background: 'var(--bg-secondary)', minWidth: 120 }}>
                CPU / Mem
              </th>
            </tr>
          </thead>
          <tbody>
            {(showAll ? sorted : sorted.slice(0, COLLAPSED_LIMIT)).map((t, i) => {
              const cpuPct = (t.cpu_time_us / maxCpu) * 100;
              const memPct = (t.peak_memory_usage / maxMem) * 100;
              return (
                <tr key={`${t.thread_id}-${i}`} style={{ borderBottom: '1px solid var(--border-primary)' }}>
                  <td style={{ padding: '5px 8px', color: 'var(--text-primary)', fontFamily: 'var(--font-mono, monospace)', whiteSpace: 'nowrap' }}>
                    {t.thread_name || 'unknown'}
                    <span style={{ color: 'var(--text-muted)', fontSize: 10, marginLeft: 6 }}>#{t.thread_id}</span>
                  </td>
                  {cols.slice(1).map(c => (
                    <td key={c.key} style={{ padding: '5px 8px', textAlign: 'right', color: 'var(--text-secondary)', fontFamily: 'var(--font-mono, monospace)' }}>
                      {c.fmt(t[c.key] as number)}
                    </td>
                  ))}
                  <td style={{ padding: '5px 8px' }}>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                      <div style={{ height: 4, borderRadius: 2, background: 'var(--bg-tertiary)', overflow: 'hidden' }}>
                        <div style={{ height: '100%', width: `${cpuPct}%`, background: '#58a6ff', borderRadius: 2, transition: 'width 0.2s' }} />
                      </div>
                      <div style={{ height: 4, borderRadius: 2, background: 'var(--bg-tertiary)', overflow: 'hidden' }}>
                        <div style={{ height: '100%', width: `${memPct}%`, background: '#f0883e', borderRadius: 2, transition: 'width 0.2s' }} />
                      </div>
                    </div>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
        {hasMany && (
          <div style={{ textAlign: 'center', padding: '8px 0' }}>
            <button
              onClick={() => setShowAll(!showAll)}
              style={{ fontSize: 11, color: '#58a6ff', background: 'none', border: 'none', cursor: 'pointer', padding: '4px 12px' }}
            >
              {showAll ? `Show top ${COLLAPSED_LIMIT} only` : `Show all ${sorted.length} threads`}
            </button>
          </div>
        )}
        </>
      )}
    </div>
  );
};

export const QueryDetail: React.FC<QueryDetailProps> = ({
  query, queryType, onClose, onKillQuery, isKillingQuery,
}) => {
  if (!query) {
    return (
      <div className="h-full flex items-center justify-center" style={{ color: 'var(--text-muted)' }}>
        <div className="text-center">
          <div className="text-2xl mb-2 font-light">--</div>
          <p className="text-sm">Select a query to view details</p>
        </div>
      </div>
    );
  }

  const isRunning = queryType === 'running' && isRunningQuery(query);

  return (
    <div className="h-full flex flex-col">
      {/* Header bar */}
      <div className="card-header flex items-center justify-between px-4 py-3" style={{ borderBottom: '1px solid var(--border-primary)' }}>
        <div className="flex items-center gap-3">
          <span className="font-semibold text-sm" style={{ color: 'var(--text-primary)' }}>Query Details</span>
          <span
            className="px-2 py-0.5 text-xs font-medium rounded-full"
            style={{
              background: isRunning ? 'rgba(56, 139, 253, 0.15)' : 'var(--bg-tertiary)',
              color: isRunning ? '#58a6ff' : 'var(--text-muted)',
            }}
          >
            {isRunning ? 'Running' : 'Historical'}
          </span>
        </div>
        <button onClick={onClose} className="text-sm" style={{ color: 'var(--text-muted)' }}>✕</button>
      </div>

      {/* Body: metrics left, logs right */}
      <div className="flex-1 flex min-h-0">
        {/* Left panel */}
        <div className="w-[340px] flex-shrink-0 overflow-y-auto p-4 space-y-4" style={{ borderRight: '1px solid var(--border-primary)' }}>
          {/* Query text */}
          <div>
            <div className="text-xs font-medium mb-2" style={{ color: 'var(--text-muted)' }}>SQL</div>
            <div className="rounded-lg p-3 max-h-28 overflow-y-auto" style={{ background: 'var(--bg-primary)', border: '1px solid var(--border-primary)' }}>
              <SqlHighlight sql={query.query} />
            </div>
          </div>

          {/* Metrics */}
          {isRunning ? (
            <RunningQueryMetrics
              query={query as RunningQuery}
              onKill={onKillQuery ? () => onKillQuery(query.query_id) : undefined}
              isKilling={isKillingQuery}
            />
          ) : (
            <HistoryQueryMetrics query={query as QueryHistoryItem} />
          )}
        </div>

        {/* Right panel: logs */}
        <div className="flex-1 min-w-0">
          <QueryLogsSection queryId={query.query_id} eventDate={'query_start_time' in query ? (query as QueryHistoryItem).query_start_time : undefined} autoExpand />
        </div>
      </div>
    </div>
  );
};

export default QueryDetail;
