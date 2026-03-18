/**
 * QueryComparisonPanel - Reusable comparison panel for 2+ queries
 * 
 * Extracted from SimilarQueriesTab to be used across:
 * - Query History (cross-hash comparison)
 * - Similar Queries tab (same-hash comparison)
 * 
 * Renders as a sticky bottom panel so it's always visible regardless of scroll.
 */

import React, { useState, useMemo } from 'react';
import type { ProfileEventComparison, MultiProfileEventRow, TaggedProcessSample, TimelineChartData } from '@tracehouse/core';
import { buildProcessSamplesSQL, mapTaggedProcessSampleRow, buildTimelineChartData } from '@tracehouse/core';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { useClickHouseServices } from '../../providers/ClickHouseProvider';
import { useCapabilityCheck } from '../shared/RequiresCapability';
import { formatBytes } from '../../stores/queryStore';
import { formatDurationMs, formatMicroseconds } from '../../utils/formatters';

/** Minimal query shape needed for comparison - works with both SimilarQuery and QueryHistoryItem */
export interface ComparableQuery {
  query_id: string;
  query_start_time: string;
  query_duration_ms: number;
  read_rows: number;
  result_rows: number;
  memory_usage: number;
  cpu_time_us?: number;
  read_bytes?: number;
  exception_code?: number;
  exception?: string | null;
  Settings?: Record<string, string>;
  query?: string;
  query_kind?: string;
  /** ClickHouse server hostname that executed this query */
  hostname?: string;
}

interface QueryComparisonPanelProps {
  queries: ComparableQuery[];
  onClose: () => void;
  /** 
   * Layout mode:
   * - 'sticky': sticks to bottom of scroll ancestor (default, for page-level tables)
   * - 'overlay': floats at bottom of nearest positioned ancestor (for modals/panels)
   */
  mode?: 'sticky' | 'overlay';
}

const fmtMs = formatDurationMs;
const fmtUs = formatMicroseconds;
const fmtTimeShort = (ts: string) => {
  const d = new Date(ts);
  return `${d.getMonth()+1}/${d.getDate()} ${d.getHours().toString().padStart(2,'0')}:${d.getMinutes().toString().padStart(2,'0')}`;
};

export const QueryComparisonPanel: React.FC<QueryComparisonPanelProps> = ({ queries, onClose, mode = 'sticky' }) => {
  const [compareView, setCompareView] = useState<'overview' | 'detailed' | 'timeline'>('overview');
  // Legacy 2-query comparison (kept for backward compat with SimilarQueriesTab)
  const [profileEventComparison, setProfileEventComparison] = useState<ProfileEventComparison[]>([]);
  // N-query comparison
  const [multiComparison, setMultiComparison] = useState<MultiProfileEventRow[]>([]);
  const [isLoadingDetailed, setIsLoadingDetailed] = useState(false);
  const [detailedError, setDetailedError] = useState<string | null>(null);
  const [detailedFilter, setDetailedFilter] = useState('');
  const [isCollapsed, setIsCollapsed] = useState(mode === 'overlay');
  // Timeline state
  const [timelineSamples, setTimelineSamples] = useState<TaggedProcessSample[]>([]);
  const [isLoadingTimeline, setIsLoadingTimeline] = useState(false);
  const [timelineError, setTimelineError] = useState<string | null>(null);
  const services = useClickHouseServices();
  const { available: hasProcessesHistory } = useCapabilityCheck(['tracehouse_processes_history']);

  const fetchDetailedComparison = async () => {
    if (!services) return;
    setIsLoadingDetailed(true);
    setDetailedError(null);
    try {
      const ids = queries.map(q => q.query_id);
      const dates = queries.map(q => q.query_start_time);
      if (ids.length === 2) {
        setProfileEventComparison(await services.queryAnalyzer.compareQueryProfileEvents(ids[0], ids[1], dates));
        setMultiComparison([]);
      } else {
        setMultiComparison(await services.queryAnalyzer.compareMultipleQueryProfileEvents(ids, dates));
        setProfileEventComparison([]);
      }
    } catch (e) {
      setDetailedError(e instanceof Error ? e.message : String(e));
    } finally {
      setIsLoadingDetailed(false);
    }
  };

  const fetchTimeline = async () => {
    if (!services) return;
    setIsLoadingTimeline(true);
    setTimelineError(null);
    try {
      const ids = queries.map(q => q.query_id);
      const sql = buildProcessSamplesSQL(ids);
      const rows = await services.adapter.executeQuery<Record<string, unknown>>(sql);
      setTimelineSamples(rows.map(mapTaggedProcessSampleRow));
    } catch (e) {
      setTimelineError(e instanceof Error ? e.message : String(e));
    } finally {
      setIsLoadingTimeline(false);
    }
  };

  // Check if queries have different SQL text (cross-hash comparison)
  const hasDifferentQueries = queries.length >= 2 && queries.some(q => q.query !== queries[0].query);

  return (
    <div style={{ 
      ...(mode === 'sticky' 
        ? { position: 'sticky', bottom: 0 } 
        : { position: 'absolute', bottom: 0, left: 0, right: 0 }),
      zIndex: 20,
      borderTop: '2px solid var(--border-primary)', 
      background: 'var(--bg-code)', 
      boxShadow: '0 -4px 12px rgba(0, 0, 0, 0.3)',
    }}>
      {/* Header — always visible, clickable to collapse/expand */}
      <div 
        style={{ 
          display: 'flex', 
          alignItems: 'center', 
          gap: 12, 
          padding: '8px 16px',
          cursor: 'pointer',
          userSelect: 'none',
        }}
        onClick={() => setIsCollapsed(c => !c)}
      >
        <span style={{ fontSize: 10, color: 'var(--text-muted)', transition: 'transform 0.15s', transform: isCollapsed ? 'rotate(-90deg)' : 'rotate(0deg)' }}>▼</span>
        <div style={{ fontSize: 11, fontWeight: 600, color: 'var(--text-primary)', textTransform: 'uppercase', letterSpacing: '0.5px' }}>
          Comparison ({queries.length} queries)
        </div>
        {hasDifferentQueries && (
          <span style={{ fontSize: 9, padding: '2px 6px', borderRadius: 3, background: 'rgba(88, 166, 255, 0.15)', color: '#58a6ff' }}>
            cross-query
          </span>
        )}
        <div className="tabs" style={{ marginLeft: 'auto' }}
          onClick={e => e.stopPropagation()}
        >
          {(['overview', 'detailed', ...(hasProcessesHistory ? ['timeline'] as const : [])] as const).map(view => (
            <button
              key={view}
              className={`tab ${compareView === view ? 'active' : ''}`}
              onClick={() => {
                setCompareView(view);
                if (view === 'detailed' && profileEventComparison.length === 0 && multiComparison.length === 0 && !isLoadingDetailed) {
                  fetchDetailedComparison();
                }
                if (view === 'timeline' && timelineSamples.length === 0 && !isLoadingTimeline) {
                  fetchTimeline();
                }
              }}
              style={{ textTransform: 'capitalize', position: 'relative' }}
            >
              {view}
              {view === 'timeline' && (
                <span style={{
                  position: 'absolute', top: -4, right: -2,
                  fontSize: 7, fontWeight: 700, color: '#f0883e',
                  background: 'var(--bg-tertiary)', border: '1px solid rgba(240,136,62,0.3)',
                  borderRadius: 3, padding: '0 3px', lineHeight: '12px',
                  textTransform: 'uppercase', letterSpacing: '0.3px',
                }}>exp</span>
              )}
            </button>
          ))}
        </div>
        <button
          onClick={(e) => { e.stopPropagation(); onClose(); }}
          style={{
            padding: '3px 10px',
            fontSize: 10,
            borderRadius: 3,
            border: '1px solid var(--border-primary)',
            background: 'transparent',
            color: 'var(--text-muted)',
            cursor: 'pointer',
          }}
        >
          Close
        </button>
      </div>

      {/* Collapsible body */}
      {!isCollapsed && (
        <div style={{ padding: '0 16px 16px', maxHeight: mode === 'overlay' ? (compareView === 'timeline' ? 600 : 450) : 400, overflow: 'auto' }}>
          {/* Query text preview for cross-hash comparisons */}
          {hasDifferentQueries && (
            <div style={{ marginBottom: 12, display: 'flex', flexDirection: 'column', gap: 4 }}>
              {queries.map((q, idx) => (
                <div key={q.query_id} style={{ display: 'flex', gap: 8, alignItems: 'baseline', fontSize: 10 }}>
                  <span style={{ fontFamily: 'monospace', color: '#58a6ff', fontWeight: 600, flexShrink: 0, width: 16, textAlign: 'center' }}>
                    {String.fromCharCode(65 + idx)}
                  </span>
                  <code style={{ color: 'var(--text-muted)', fontFamily: 'monospace', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: 600 }} title={q.query}>
                    {q.query_id.slice(0, 8)}… {q.query_kind ? `[${q.query_kind}]` : ''} {q.query?.slice(0, 80)}{(q.query?.length || 0) > 80 ? '…' : ''}
                  </code>
                </div>
              ))}
            </div>
          )}

          {compareView === 'overview' && <OverviewComparisonTable queries={queries} />}

          {compareView === 'timeline' && (
            <TimelineComparison
              queries={queries}
              samples={timelineSamples}
              isLoading={isLoadingTimeline}
              error={timelineError}
            />
          )}

          {compareView === 'detailed' && (
            queries.length === 2 ? (
              <DetailedComparison2
                queries={queries}
                profileEventComparison={profileEventComparison}
                isLoading={isLoadingDetailed}
                error={detailedError}
                filter={detailedFilter}
                onFilterChange={setDetailedFilter}
              />
            ) : (
              <DetailedComparisonN
                queries={queries}
                rows={multiComparison}
                isLoading={isLoadingDetailed}
                error={detailedError}
                filter={detailedFilter}
                onFilterChange={setDetailedFilter}
              />
            )
          )}
        </div>
      )}
    </div>
  );
};

/** Overview metrics comparison table - supports N queries */
const OverviewComparisonTable: React.FC<{ queries: ComparableQuery[] }> = ({ queries }) => {
  return (
    <div style={{ overflowX: 'auto' }}>
      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11, fontFamily: 'monospace' }}>
        <thead>
          <tr style={{ borderBottom: '1px solid var(--border-secondary)' }}>
            <th style={{ textAlign: 'left', padding: '4px 8px', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10 }}>Metric</th>
            {queries.map((q, idx) => (
              <th key={idx} style={{ textAlign: 'right', padding: '4px 8px', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10 }}>
                <span style={{ color: '#58a6ff', fontWeight: 600, marginRight: 4 }}>{String.fromCharCode(65 + idx)}</span>
                {fmtTimeShort(q.query_start_time)}
              </th>
            ))}
            {queries.length >= 2 && (
              <th style={{ textAlign: 'right', padding: '4px 8px', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10 }}>Δ</th>
            )}
          </tr>
        </thead>
        <tbody>
          {[
            { label: 'Duration', get: (q: ComparableQuery) => Number(q.query_duration_ms), fmt: fmtMs },
            { label: 'CPU Time', get: (q: ComparableQuery) => Number(q.cpu_time_us) || 0, fmt: fmtUs },
            { label: 'Memory', get: (q: ComparableQuery) => Number(q.memory_usage), fmt: (v: number) => formatBytes(v) },
            { label: 'Read Rows', get: (q: ComparableQuery) => Number(q.read_rows), fmt: (v: number) => v.toLocaleString() },
            { label: 'Read Bytes', get: (q: ComparableQuery) => Number(q.read_bytes || 0), fmt: (v: number) => formatBytes(v) },
            { label: 'Result Rows', get: (q: ComparableQuery) => Number(q.result_rows), fmt: (v: number) => v.toLocaleString() },
          ].map(({ label, get, fmt }) => {
            const values = queries.map(get);
            const min = Math.min(...values);
            const max = Math.max(...values);
            const delta = max - min;
            const deltaPct = min > 0 ? ((max - min) / min * 100).toFixed(0) : '—';
            return (
              <tr key={label} style={{ borderBottom: '1px solid var(--border-secondary)' }}>
                <td style={{ padding: '4px 8px', color: 'var(--text-tertiary)' }}>{label}</td>
                {values.map((v, idx) => (
                  <td key={idx} style={{ 
                    textAlign: 'right', 
                    padding: '4px 8px', 
                    color: v === max && values.length > 1 && delta > 0 ? 'var(--color-error)' : v === min && values.length > 1 && delta > 0 ? 'var(--color-success)' : 'var(--text-primary)',
                  }}>
                    {fmt(v)}
                  </td>
                ))}
                {queries.length >= 2 && (
                  <td style={{ textAlign: 'right', padding: '4px 8px', color: 'var(--text-muted)', fontSize: 10 }}>
                    {deltaPct !== '—' ? `${deltaPct}%` : '—'}
                  </td>
                )}
              </tr>
            );
          })}
          {/* Server hostname row - highlights when queries ran on different servers */}
          {queries.some(q => q.hostname) && (() => {
            const hostnames = queries.map(q => q.hostname || '—');
            const allSame = hostnames.every(h => h === hostnames[0]);
            return (
              <tr style={{ borderBottom: '1px solid var(--border-secondary)' }}>
                <td style={{ padding: '4px 8px', color: 'var(--text-tertiary)' }}>Server</td>
                {hostnames.map((h, idx) => (
                  <td key={idx} style={{ 
                    textAlign: 'right', 
                    padding: '4px 8px', 
                    color: allSame ? 'var(--text-primary)' : '#f59e0b',
                    fontWeight: allSame ? 400 : 500,
                  }}>
                    {h}
                  </td>
                ))}
                {queries.length >= 2 && (
                  <td style={{ textAlign: 'right', padding: '4px 8px', color: 'var(--text-muted)', fontSize: 10 }}>
                    {allSame ? '—' : '≠'}
                  </td>
                )}
              </tr>
            );
          })()}
          {/* Settings diff */}
          {(() => {
            const allKeys = new Set<string>();
            queries.forEach(q => {
              const s = q.Settings;
              if (s && typeof s === 'object') Object.keys(s).forEach(k => allKeys.add(k));
            });
            if (allKeys.size === 0) return (
              <tr><td colSpan={queries.length + 2} style={{ padding: '8px', color: 'var(--text-muted)', fontSize: 10, fontStyle: 'italic' }}>No custom settings on selected queries</td></tr>
            );
            return (
              <>
                <tr><td colSpan={queries.length + 2} style={{ padding: '8px 8px 4px', fontSize: 10, fontWeight: 600, color: '#f59e0b', textTransform: 'uppercase', letterSpacing: '0.5px', borderTop: '2px solid var(--border-secondary)' }}>Settings</td></tr>
                {Array.from(allKeys).sort().map(key => {
                  const values = queries.map(q => {
                    const s = q.Settings;
                    return (s && typeof s === 'object' && s[key]) ? s[key] : '(default)';
                  });
                  const allSame = values.every(v => v === values[0]);
                  return (
                    <tr key={`setting-${key}`} style={{ borderBottom: '1px solid var(--border-secondary)' }}>
                      <td style={{ padding: '4px 8px', color: '#f59e0b', fontSize: 10 }}>⚙ {key}</td>
                      {values.map((v, idx) => (
                        <td key={idx} style={{ 
                          textAlign: 'right', 
                          padding: '4px 8px', 
                          color: allSame ? 'var(--text-muted)' : '#f59e0b',
                          fontWeight: allSame ? 400 : 500,
                        }}>
                          {v}
                        </td>
                      ))}
                      {queries.length >= 2 && (
                        <td style={{ textAlign: 'right', padding: '4px 8px', color: allSame ? 'var(--text-muted)' : '#f59e0b', fontSize: 10 }}>
                          {allSame ? '=' : '≠'}
                        </td>
                      )}
                    </tr>
                  );
                })}
              </>
            );
          })()}
        </tbody>
      </table>
    </div>
  );
};

const fmtNum = (v: number) => {
  if (v === 0) return '0';
  if (v >= 1e9) return `${(v / 1e9).toFixed(2)}G`;
  if (v >= 1e6) return `${(v / 1e6).toFixed(2)}M`;
  if (v >= 1e3) return `${(v / 1e3).toFixed(1)}K`;
  return v.toLocaleString();
};

/** Shared loading/error/empty states for detailed views */
const DetailedState: React.FC<{ isLoading: boolean; error: string | null; isEmpty: boolean }> = ({ isLoading, error, isEmpty }) => {
  if (isLoading) return (
    <div style={{ padding: 20, textAlign: 'center' }}>
      <div style={{ width: 20, height: 20, borderWidth: 2, borderStyle: 'solid', borderColor: 'var(--border-primary)', borderTopColor: 'var(--text-tertiary)', borderRadius: '50%', animation: 'spin 1s linear infinite', margin: '0 auto 8px' }} />
      <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>Loading ProfileEvents comparison...</span>
    </div>
  );
  if (error) return (
    <div style={{ padding: 12, borderRadius: 6, background: 'rgba(var(--color-error-rgb), 0.1)', border: '1px solid rgba(var(--color-error-rgb), 0.2)', fontSize: 11, color: 'var(--color-error)' }}>
      {error}
    </div>
  );
  if (isEmpty) return (
    <div style={{ padding: 16, textAlign: 'center', fontSize: 11, color: 'var(--text-muted)' }}>
      No ProfileEvent differences found between these queries.
    </div>
  );
  return null;
};

/** Filter bar for detailed views */
const DetailedFilterBar: React.FC<{
  queries: ComparableQuery[];
  count: number;
  filter: string;
  onFilterChange: (f: string) => void;
}> = ({ queries, count, filter, onFilterChange }) => (
  <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
    <div style={{ fontSize: 10, color: 'var(--text-muted)', fontFamily: 'monospace' }}>
      {queries.map((q, i) => `${String.fromCharCode(65 + i)}: ${q.query_id.slice(0, 8)}…`).join(' vs ')}
    </div>
    <div style={{ fontSize: 10, color: 'var(--text-muted)', marginLeft: 'auto' }}>{count} metrics</div>
    <input
      type="text"
      placeholder="Filter metrics..."
      value={filter}
      onChange={e => onFilterChange(e.target.value)}
      style={{
        padding: '3px 8px', fontSize: 10, fontFamily: 'monospace', borderRadius: 4,
        border: '1px solid var(--border-primary)', background: 'var(--bg-tertiary)', color: 'var(--text-primary)', width: 160,
      }}
    />
  </div>
);

/** Detailed ProfileEvents comparison for exactly 2 queries (uses server-side dB/perc) */
const DetailedComparison2: React.FC<{
  queries: ComparableQuery[];
  profileEventComparison: ProfileEventComparison[];
  isLoading: boolean;
  error: string | null;
  filter: string;
  onFilterChange: (f: string) => void;
}> = ({ queries, profileEventComparison, isLoading, error, filter, onFilterChange }) => {
  const state = DetailedState({ isLoading, error, isEmpty: profileEventComparison.length === 0 });
  if (state) return state;

  const filtered = filter
    ? profileEventComparison.filter(r => r.metric.toLowerCase().includes(filter.toLowerCase()))
    : profileEventComparison;

  const barWidth = (perc: number) => Math.min(100, Math.abs(perc));

  return (
    <>
      <DetailedFilterBar queries={queries} count={filtered.length} filter={filter} onFilterChange={onFilterChange} />
      <div style={{ overflowX: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 10, fontFamily: 'monospace' }}>
          <thead>
            <tr style={{ borderBottom: '1px solid var(--border-secondary)' }}>
              <th style={{ textAlign: 'left', padding: '4px 8px', color: 'var(--text-muted)', fontWeight: 500 }}>ProfileEvent</th>
              <th style={{ textAlign: 'right', padding: '4px 8px', color: 'var(--text-muted)', fontWeight: 500 }} title={queries[0].query_id}>A: {queries[0].query_id.slice(0, 8)}…</th>
              <th style={{ textAlign: 'right', padding: '4px 8px', color: 'var(--text-muted)', fontWeight: 500 }} title={queries[1].query_id}>B: {queries[1].query_id.slice(0, 8)}…</th>
              <th style={{ textAlign: 'right', padding: '4px 8px', color: 'var(--text-muted)', fontWeight: 500 }}>dB</th>
              <th style={{ textAlign: 'right', padding: '4px 8px', color: 'var(--text-muted)', fontWeight: 500 }}>Δ%</th>
              <th style={{ textAlign: 'left', padding: '4px 8px', color: 'var(--text-muted)', fontWeight: 500, width: 120 }}></th>
            </tr>
          </thead>
          <tbody>
            {filtered.map(row => {
              const isPositive = row.perc > 0;
              const isInfinite = (row.v1 === 0 && row.v2 > 0) || (row.v2 === 0 && row.v1 > 0);
              const barColor = isPositive ? 'var(--color-error)' : 'var(--color-success)';
              const percColor = Math.abs(row.perc) < 5 ? 'var(--text-muted)' : isPositive ? 'var(--color-error)' : 'var(--color-success)';
              return (
                <tr key={row.metric} style={{ borderBottom: '1px solid var(--border-secondary)' }}>
                  <td style={{ padding: '3px 8px', color: 'var(--text-tertiary)', maxWidth: 220, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={row.metric}>{row.metric}</td>
                  <td style={{ textAlign: 'right', padding: '3px 8px', color: 'var(--text-primary)' }}>{fmtNum(row.v1)}</td>
                  <td style={{ textAlign: 'right', padding: '3px 8px', color: 'var(--text-primary)' }}>{fmtNum(row.v2)}</td>
                  <td style={{ textAlign: 'right', padding: '3px 8px', color: isInfinite ? 'var(--text-muted)' : percColor, fontSize: 9 }}>
                    {isInfinite ? (row.v2 > 0 ? '+∞' : '−∞') : row.dB > 0 ? `+${row.dB.toFixed(1)}` : row.dB.toFixed(1)}
                  </td>
                  <td style={{ textAlign: 'right', padding: '3px 8px', color: percColor, fontWeight: Math.abs(row.perc) >= 20 ? 600 : 400 }}>
                    {isInfinite ? (row.v2 > 0 ? '+100%' : '−100%') : `${row.perc > 0 ? '+' : ''}${row.perc.toFixed(1)}%`}
                  </td>
                  <td style={{ padding: '3px 8px' }}>
                    <div style={{ width: '100%', height: 8, background: 'var(--bg-tertiary)', borderRadius: 4, overflow: 'hidden', position: 'relative' }}>
                      <div style={{
                        position: 'absolute',
                        [isPositive ? 'left' : 'right']: '50%',
                        width: `${barWidth(row.perc) / 2}%`,
                        height: '100%',
                        background: barColor,
                        borderRadius: 4,
                        opacity: 0.7,
                      }} />
                      <div style={{ position: 'absolute', left: '50%', top: 0, bottom: 0, width: 1, background: 'var(--border-secondary)' }} />
                    </div>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      <div style={{ marginTop: 8, fontSize: 9, color: 'var(--text-muted)', fontStyle: 'italic' }}>
        Based on <a href="https://clickhouse.com/docs/knowledgebase/comparing-metrics-between-queries" target="_blank" rel="noopener noreferrer" style={{ color: '#58a6ff' }}>ClickHouse ProfileEvents comparison</a>. Positive dB/% = B used more resources.
      </div>
    </>
  );
};

/** Detailed ProfileEvents comparison for N queries (3+) */
const DetailedComparisonN: React.FC<{
  queries: ComparableQuery[];
  rows: MultiProfileEventRow[];
  isLoading: boolean;
  error: string | null;
  filter: string;
  onFilterChange: (f: string) => void;
}> = ({ queries, rows, isLoading, error, filter, onFilterChange }) => {
  const state = DetailedState({ isLoading, error, isEmpty: rows.length === 0 });
  if (state) return state;

  const filtered = filter
    ? rows.filter(r => r.metric.toLowerCase().includes(filter.toLowerCase()))
    : rows;

  // Sort by max spread (max - min) descending so the most interesting metrics are on top
  const sorted = [...filtered].sort((a, b) => {
    const spreadA = Math.max(...a.values) - Math.min(...a.values);
    const spreadB = Math.max(...b.values) - Math.min(...b.values);
    return spreadB - spreadA;
  });

  return (
    <>
      <DetailedFilterBar queries={queries} count={sorted.length} filter={filter} onFilterChange={onFilterChange} />
      <div style={{ overflowX: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 10, fontFamily: 'monospace' }}>
          <thead>
            <tr style={{ borderBottom: '1px solid var(--border-secondary)' }}>
              <th style={{ textAlign: 'left', padding: '4px 8px', color: 'var(--text-muted)', fontWeight: 500 }}>ProfileEvent</th>
              {queries.map((q, idx) => (
                <th key={idx} style={{ textAlign: 'right', padding: '4px 8px', color: 'var(--text-muted)', fontWeight: 500 }} title={q.query_id}>
                  {String.fromCharCode(65 + idx)}: {q.query_id.slice(0, 8)}…
                </th>
              ))}
              <th style={{ textAlign: 'right', padding: '4px 8px', color: 'var(--text-muted)', fontWeight: 500 }}>Spread</th>
            </tr>
          </thead>
          <tbody>
            {sorted.map(row => {
              const min = Math.min(...row.values);
              const max = Math.max(...row.values);
              const spread = max - min;
              const spreadPct = min > 0 ? ((spread / min) * 100).toFixed(0) : max > 0 ? '∞' : '0';
              return (
                <tr key={row.metric} style={{ borderBottom: '1px solid var(--border-secondary)' }}>
                  <td style={{ padding: '3px 8px', color: 'var(--text-tertiary)', maxWidth: 220, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={row.metric}>
                    {row.metric}
                  </td>
                  {row.values.map((v, idx) => (
                    <td key={idx} style={{ 
                      textAlign: 'right', 
                      padding: '3px 8px', 
                      color: v === max && spread > 0 ? 'var(--color-error)' : v === min && spread > 0 ? 'var(--color-success)' : 'var(--text-primary)',
                      fontWeight: (v === max || v === min) && spread > 0 ? 500 : 400,
                    }}>
                      {fmtNum(v)}
                    </td>
                  ))}
                  <td style={{ textAlign: 'right', padding: '3px 8px', color: 'var(--text-muted)', fontSize: 9 }}>
                    {spreadPct}%
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      <div style={{ marginTop: 8, fontSize: 9, color: 'var(--text-muted)', fontStyle: 'italic' }}>
        ProfileEvents from system.query_log. Values colored: <span style={{ color: 'var(--color-success)' }}>lowest</span> / <span style={{ color: 'var(--color-error)' }}>highest</span>. Sorted by spread (max − min).
      </div>
    </>
  );
};

// ── Colors for query lines (up to 8) ──
const QUERY_COLORS = ['#3B82F6', '#F59E0B', '#10B981', '#EF4444', '#8B5CF6', '#EC4899', '#06B6D4', '#84CC16'];

/** Timeline comparison charts — overlays process samples for N queries */
const TimelineComparison: React.FC<{
  queries: ComparableQuery[];
  samples: TaggedProcessSample[];
  isLoading: boolean;
  error: string | null;
}> = ({ queries, samples, isLoading, error }) => {
  const chartData = useMemo(
    () => buildTimelineChartData(samples, queries.map(q => q.query_id)),
    [samples, queries],
  );

  if (isLoading) return (
    <div style={{ padding: 20, textAlign: 'center' }}>
      <div style={{ width: 20, height: 20, borderWidth: 2, borderStyle: 'solid', borderColor: 'var(--border-primary)', borderTopColor: 'var(--text-tertiary)', borderRadius: '50%', animation: 'spin 1s linear infinite', margin: '0 auto 8px' }} />
      <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>Loading process samples...</span>
    </div>
  );
  if (error) return (
    <div style={{ padding: 12, borderRadius: 6, background: 'rgba(var(--color-error-rgb), 0.1)', border: '1px solid rgba(var(--color-error-rgb), 0.2)', fontSize: 11, color: 'var(--color-error)' }}>
      {error}
    </div>
  );
  if (samples.length === 0) return (
    <div style={{ padding: 16, textAlign: 'center', fontSize: 11, color: 'var(--text-muted)' }}>
      No process samples found. Queries may have been too short (&lt;1s) or processes_history may not be enabled.
    </div>
  );

  const { perQuery, points, activeMetrics } = chartData;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
      {/* Legend */}
      <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', fontSize: 10 }}>
        {queries.map((q, idx) => (
          <div key={q.query_id} style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
            <div style={{ width: 12, height: 3, borderRadius: 1, background: QUERY_COLORS[idx % QUERY_COLORS.length] }} />
            <span style={{ color: QUERY_COLORS[idx % QUERY_COLORS.length], fontWeight: 600 }}>{String.fromCharCode(65 + idx)}</span>
            <span style={{ color: 'var(--text-muted)', fontFamily: 'monospace' }}>{q.query_id.slice(0, 8)}...</span>
            <span style={{ color: 'var(--text-muted)' }}>({perQuery.get(q.query_id)?.length || 0} samples)</span>
          </div>
        ))}
      </div>

      {/* Charts */}
      {activeMetrics.map(metric => (
        <div key={metric.id}>
          <div style={{ fontSize: 10, fontWeight: 600, color: 'var(--text-tertiary)', marginBottom: 2, textTransform: 'uppercase', letterSpacing: '0.5px' }}>
            {metric.label} <span style={{ fontWeight: 400, color: 'var(--text-muted)' }}>({metric.unit})</span>
            {metric.lines.length > 1 && (
              <span style={{ fontWeight: 400, color: 'var(--text-muted)', marginLeft: 8, fontSize: 9, textTransform: 'none' }}>
                solid={metric.lines[0].suffix.trim() || 'primary'} dashed={metric.lines[1].suffix.trim() || 'secondary'}
              </span>
            )}
          </div>
          <ResponsiveContainer width="100%" height={120}>
            <LineChart data={points} margin={{ top: 4, right: 8, bottom: 4, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border-secondary)" />
              <XAxis
                dataKey="t"
                type="number"
                domain={['dataMin', 'dataMax']}
                tickFormatter={v => `${v}s`}
                tick={{ fontSize: 9, fill: 'var(--text-muted)' }}
                stroke="var(--border-secondary)"
              />
              <YAxis
                tick={{ fontSize: 9, fill: 'var(--text-muted)' }}
                stroke="var(--border-secondary)"
                width={50}
                tickFormatter={v => v >= 1000 ? `${(v/1000).toFixed(1)}K` : String(Math.round(v * 100) / 100)}
              />
              <Tooltip
                contentStyle={{ background: 'var(--bg-primary)', border: '1px solid var(--border-primary)', borderRadius: 6, fontSize: 10, fontFamily: 'monospace' }}
                labelFormatter={v => `t = ${v}s`}
                formatter={(value: number, name: string) => {
                  // name is like "d_net_send_kb_0" — extract query index and line key
                  const parts = name.split('_');
                  const idx = parseInt(parts.pop() || '0');
                  const lineKey = parts.join('_');
                  const line = metric.lines.find(l => l.key === lineKey);
                  const label = `${String.fromCharCode(65 + idx)}${line?.suffix || ''}`;
                  return [metric.formatter(value), label];
                }}
              />
              {metric.lines.flatMap(line =>
                queries.map((_, idx) => (
                  <Line
                    key={`${line.key}_${idx}`}
                    type="monotone"
                    dataKey={`${line.key}_${idx}`}
                    stroke={QUERY_COLORS[idx % QUERY_COLORS.length]}
                    strokeWidth={1.5}
                    strokeDasharray={line.strokeDasharray}
                    dot={false}
                    connectNulls={false}
                    isAnimationActive={false}
                  />
                ))
              )}
            </LineChart>
          </ResponsiveContainer>
        </div>
      ))}

      <div style={{ fontSize: 9, color: 'var(--text-muted)', fontStyle: 'italic' }}>
        Per-second samples from tracehouse.processes_history. X-axis = relative time from each query's start.
      </div>
    </div>
  );
};

export default QueryComparisonPanel;
