/**
 * QueryActivityTable - one chronological surface for live and completed queries.
 * Supports multi-select comparison of completed queries.
 */

import React, { useCallback, useMemo, useState } from 'react';
import type { 
  QueryHistoryItem, 
  QueryHistoryFilter, 
  QueryHistorySort, 
  SortField,
  SortDirection,
} from '../../stores/queryStore';
import { formatBytes, formatNumber } from '../../stores/queryStore';
import { formatDurationMs } from '../../utils/formatters';
import { QueryComparisonPanel } from './QueryComparisonPanel';
import type { ComparableQuery } from './QueryComparisonPanel';
import { QueryFilterBar } from './QueryFilterBar';
import { QueryFingerprintGlyph, QueryHoverPreview } from './QueryHoverPreview';
import { resourcePressureTooltip } from '../../utils/queryHoverMetrics';
import type { QueryAnalyzer } from '@tracehouse/core';
import { useQueryHoverTopology } from './hooks/useQueryHoverTopology';
import { sortQueryActivityRecords, type QueryActivityRecord } from './query-activity-model';
import { useUserPreferenceStore } from '../../stores/userPreferenceStore';
import { PreviewToggleButton } from '../common/PreviewToggleButton';
import {
  loadPreviewPreference,
  QUERY_ACTIVITY_PREVIEW_STORAGE_KEY,
  savePreviewPreference,
} from '../../utils/previewPreference';
import { resolveTrackerTimeRange } from '../../utils/trackerTimeRange';

interface QueryActivityTableProps {
  activity: QueryActivityRecord[];
  selectedQueryId: string | null;
  onSelectHistoryQuery: (query: QueryHistoryItem) => void;
  onSelectRunningQuery: (query: QueryActivityRecord['liveQuery']) => void;
  onKillQuery: (queryId: string) => void;
  isKillingQuery: boolean;
  filter: QueryHistoryFilter;
  sort: QueryHistorySort;
  onFilterChange: (filter: Partial<QueryHistoryFilter>) => void;
  onSortChange: (sort: QueryHistorySort) => void;
  isLoading: boolean;
  queryAnalyzer?: QueryAnalyzer;
  coordinatorIds?: Set<string>;
  showFilterBar?: boolean;
}

const thStyle: React.CSSProperties = {
  boxSizing: 'border-box',
  padding: '8px 8px',
  textAlign: 'left',
  fontSize: 10,
  fontWeight: 500,
  color: 'var(--text-muted)',
  borderBottom: '1px solid var(--border-primary)',
  textTransform: 'uppercase',
  letterSpacing: '0.5px',
  whiteSpace: 'nowrap',
};

const tdStyle: React.CSSProperties = {
  boxSizing: 'border-box',
  padding: '8px 8px',
  fontSize: 12,
  color: 'var(--text-secondary)',
  borderBottom: '1px solid var(--border-primary)',
};

interface SortThProps {
  field: SortField;
  label: string;
  sort: QueryHistorySort;
  onSort: (field: SortField) => void;
  align?: 'left' | 'right';
  width?: number;
}

const SortTh: React.FC<SortThProps> = ({
  field,
  label,
  sort,
  onSort,
  align = 'left',
  width,
}) => {
  const active = sort.field === field;
  return (
    <th style={{ ...thStyle, width, textAlign: align, cursor: 'pointer' }} onClick={() => onSort(field)}>
      {label}{' '}
      <span style={{ color: active ? '#58a6ff' : 'var(--text-muted)', fontSize: 9 }}>
        {active ? (sort.direction === 'asc' ? '▲' : '▼') : '⇅'}
      </span>
    </th>
  );
};

const fmtDuration = formatDurationMs;

const fmtTime = (ts: string): string => {
  const d = new Date(ts);
  return d.toLocaleString(undefined, { 
    month: 'short', day: 'numeric', 
    hour: '2-digit', minute: '2-digit', second: '2-digit' 
  });
};

const StatusBadge: React.FC<{ type: string; exception?: string }> = ({ type, exception }) => {
  const isRunning = type === 'running';
  const isError = type === 'ExceptionWhileProcessing' || !!exception;
  // Truncate exception message for display
  const displayText = isError && exception 
    ? (exception.length > 25 ? exception.slice(0, 25) + '...' : exception)
    : (isRunning ? 'Running' : isError ? 'Error' : 'Success');
  const color = isRunning ? '#58a6ff' : isError ? '#f85149' : '#3fb950';
  return (
    <span 
      style={{
        display: 'block',
        boxSizing: 'border-box',
        padding: '2px 8px',
        fontSize: 10,
        fontWeight: 500,
        borderRadius: 10,
        background: isRunning ? 'rgba(88,166,255,0.15)' : isError ? 'rgba(248,81,73,0.15)' : 'rgba(63,185,80,0.15)',
        color,
        maxWidth: '100%',
        overflow: 'hidden',
        textOverflow: 'ellipsis',
        whiteSpace: 'nowrap',
      }}
      title={exception || displayText}
    >
      {isRunning && (
        <span
          aria-hidden="true"
          style={{
            display: 'inline-block',
            width: 6,
            height: 6,
            marginRight: 5,
            borderRadius: '50%',
            background: 'currentColor',
            animation: 'activity-running-pulse 2.4s ease-in-out infinite',
          }}
        />
      )}
      {displayText}
    </span>
  );
};

const EfficiencyBadge: React.FC<{ score?: number | null }> = ({ score }) => {
  if (score === undefined || score === null) return <span style={{ color: 'var(--text-muted)' }}>—</span>;
  // Higher pruning % = better (more marks skipped via index)
  const color = score >= 90 ? '#3fb950' : score >= 50 ? '#d29922' : '#f85149';
  return (
    <span style={{
      display: 'inline-block',
      padding: '2px 8px',
      fontSize: 10,
      fontWeight: 500,
      borderRadius: 10,
      background: `${color}22`,
      color,
    }}>
      {score.toFixed(1)}%
    </span>
  );
}

const QueryKindBadge: React.FC<{ kind: string }> = ({ kind }) => {
  const getColor = (k: string): string => {
    switch (k.toUpperCase()) {
      case 'SELECT': return '#3b82f6';  // blue
      case 'INSERT': return '#f59e0b';  // amber
      case 'ALTER': return '#ef4444';   // red
      case 'CREATE': return '#22c55e';  // green
      case 'DROP': return '#f43f5e';    // rose
      case 'SYSTEM': return '#8b5cf6';  // purple
      default: return '#94a3b8';        // slate
    }
  };
  const color = getColor(kind);
  return (
    <span style={{
      display: 'inline-block',
      padding: '2px 6px',
      fontSize: 10,
      fontWeight: 500,
      borderRadius: 4,
      background: `${color}20`,
      color,
    }}>
      {kind || 'Unknown'}
    </span>
  );
};

/** Convert QueryHistoryItem to ComparableQuery for the comparison panel */
const toComparable = (q: QueryHistoryItem): ComparableQuery => ({
  query_id: q.query_id,
  query_start_time: q.query_start_time,
  query_duration_ms: q.query_duration_ms,
  read_rows: q.read_rows,
  read_bytes: q.read_bytes,
  result_rows: q.result_rows,
  memory_usage: q.memory_usage,
  cpu_time_us: q.cpu_time_us,
  exception_code: q.type === 'ExceptionWhileProcessing' ? 1 : 0,
  exception: q.exception,
  query: q.query,
  query_kind: q.query_kind,
  Settings: q.Settings,
  hostname: q.hostname,
});

export const QueryActivityTable: React.FC<QueryActivityTableProps> = ({
  activity, selectedQueryId, onSelectHistoryQuery, onSelectRunningQuery, onKillQuery, isKillingQuery,
  filter, sort, onFilterChange, onSortChange, isLoading, queryAnalyzer, coordinatorIds,
  showFilterBar = true,
}) => {
  const killQueriesEnabled = useUserPreferenceStore(state => state.killQueriesEnabled);
  const [compareMode, setCompareMode] = useState(false);
  const [selectedForCompare, setSelectedForCompare] = useState<Set<string>>(new Set());
  const [comparisonOpen, setComparisonOpen] = useState(false);
  const [hoveredQueryId, setHoveredQueryId] = useState<string | null>(null);
  const [showHoverPreview, setShowHoverPreview] = useState(
    () => loadPreviewPreference(QUERY_ACTIVITY_PREVIEW_STORAGE_KEY),
  );

  const handleSort = useCallback((field: SortField) => {
    const dir: SortDirection = sort.field === field && sort.direction === 'desc' ? 'asc' : 'desc';
    onSortChange({ field, direction: dir });
  }, [sort, onSortChange]);

  const sortedActivity = useMemo(
    () => sortQueryActivityRecords(activity, sort),
    [activity, sort],
  );
  const resolvedHistoryRange = useMemo(
    () => resolveTrackerTimeRange(filter.timeRange, filter.startTime, filter.endTime),
    [filter.endTime, filter.startTime, filter.timeRange],
  );

  const toggleCompareSelection = useCallback((queryId: string) => {
    setComparisonOpen(false);
    setSelectedForCompare(prev => {
      const next = new Set(prev);
      if (next.has(queryId)) next.delete(queryId); else next.add(queryId);
      return next;
    });
  }, []);

  const cancelCompare = useCallback(() => {
    setCompareMode(false);
    setComparisonOpen(false);
    setSelectedForCompare(new Set());
  }, []);

  const comparedQueries: ComparableQuery[] = compareMode && selectedForCompare.size >= 2
    ? sortedActivity.filter(q => q.activitySource === 'history' && selectedForCompare.has(q.query_id)).map(toComparable)
    : [];
  const previewQuery = sortedActivity.find(q => q.query_id === hoveredQueryId)
    ?? sortedActivity.find(q => q.query_id === selectedQueryId)
    ?? null;
  const hoverTopology = useQueryHoverTopology({
    enabled: showHoverPreview,
    queryAnalyzer,
    history: sortedActivity.filter(query => query.activitySource === 'history'),
    coordinatorIds,
    startTime: resolvedHistoryRange.startTime,
  });
  const previewChildQueries = hoverTopology.getChildQueriesForQuery(previewQuery);

  return (
    <div>
      {showFilterBar && (
        <QueryFilterBar filter={filter} onFilterChange={onFilterChange} queryAnalyzer={queryAnalyzer} />
      )}

      {/* Compare mode bar */}
      <div style={{ 
        display: 'flex', 
        alignItems: 'center', 
        gap: 12, 
        padding: '8px 0', 
        marginBottom: 8,
      }}>
        <button
          onClick={() => {
            if (compareMode) cancelCompare();
            else setCompareMode(true);
          }}
          style={{
            padding: '5px 14px',
            fontSize: 11,
            borderRadius: 5,
            border: compareMode ? '1px solid #58a6ff' : '1px solid var(--border-primary)',
            background: compareMode ? 'rgba(88, 166, 255, 0.15)' : 'transparent',
            color: compareMode ? '#58a6ff' : 'var(--text-muted)',
            cursor: 'pointer',
            fontWeight: compareMode ? 600 : 400,
            transition: 'all 0.15s',
          }}
        >
          {compareMode ? 'Cancel Compare' : '⇄ Compare Queries'}
        </button>
        {compareMode && (
          <button
            type="button"
            disabled={selectedForCompare.size < 2}
            onClick={() => setComparisonOpen(true)}
            style={{
              padding: '5px 14px',
              fontSize: 11,
              borderRadius: 5,
              border: selectedForCompare.size >= 2
                ? '1px solid rgba(88, 166, 255, 0.65)'
                : '1px solid var(--border-primary)',
              background: selectedForCompare.size >= 2
                ? 'rgba(88, 166, 255, 0.18)'
                : 'transparent',
              color: selectedForCompare.size >= 2 ? '#58a6ff' : 'var(--text-muted)',
              cursor: selectedForCompare.size >= 2 ? 'pointer' : 'not-allowed',
              fontWeight: 600,
              opacity: selectedForCompare.size >= 2 ? 1 : 0.55,
              transition: 'all 0.15s',
            }}
          >
            Compare {selectedForCompare.size >= 2 ? `${selectedForCompare.size} queries` : 'selected'}
          </button>
        )}
        {compareMode && (
          <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>
            {selectedForCompare.size === 0 
              ? 'Select 2 or more queries to compare' 
              : `${selectedForCompare.size} selected`}
          </span>
        )}
        <div style={{ marginLeft: 'auto' }}>
          <PreviewToggleButton
            label="Query Preview"
            visible={showHoverPreview}
            onToggle={() => setShowHoverPreview(v => {
              const next = !v;
              savePreviewPreference(QUERY_ACTIVITY_PREVIEW_STORAGE_KEY, next);
              return next;
            })}
          />
        </div>
      </div>

      {comparisonOpen && comparedQueries.length >= 2 ? (
        <QueryComparisonPanel
          queries={comparedQueries}
          onClose={() => setComparisonOpen(false)}
        />
      ) : sortedActivity.length === 0 ? (
        <div style={{ textAlign: 'center', padding: '40px 0', color: 'var(--text-muted)', fontSize: 13 }}>
          {isLoading ? 'Loading query activity...' : 'No queries found matching the current filters'}
        </div>
      ) : (
        <div style={{
          display: 'grid',
          gridTemplateColumns: showHoverPreview ? 'minmax(0, 1fr) 340px' : '1fr',
          gap: showHoverPreview ? 16 : 0,
          alignItems: 'start',
        }}
        onMouseLeave={() => setHoveredQueryId(null)}
        >
          <div style={{ minWidth: 0, overflowX: 'auto' }}>
            <table style={{ width: '100%', minWidth: 1500, borderCollapse: 'collapse', tableLayout: 'fixed' }}>
              <thead>
                <tr>
                  {compareMode && <th style={{ ...thStyle, width: 32, textAlign: 'center' }}></th>}
                  <th
                    style={{ ...thStyle, width: 48 }}
                    title="Resource pressure glyph: time, memory, CPU, I/O, and scan"
                  >
                    Shape
                  </th>
                  <th style={{ ...thStyle, width: 90 }}>ID</th>
                  <th style={{ ...thStyle, width: 120 }}>Type</th>
                  <th style={{ ...thStyle, width: 110 }}>Status</th>
                  <SortTh field="query_start_time" label="Started" width={140} sort={sort} onSort={handleSort} />
                  <th style={{ ...thStyle, width: 90 }}>User</th>
                  <th style={{ ...thStyle, width: 125 }}>Server</th>
                  <th style={{ ...thStyle, width: 320 }}>Query</th>
                  <SortTh field="query_duration_ms" label="Duration" align="right" width={95} sort={sort} onSort={handleSort} />
                  <SortTh field="read_rows" label="Rows Read" align="right" width={105} sort={sort} onSort={handleSort} />
                  <SortTh field="read_bytes" label="Bytes Read" align="right" width={105} sort={sort} onSort={handleSort} />
                  <SortTh field="result_rows" label="Result" align="right" width={90} sort={sort} onSort={handleSort} />
                  <SortTh field="memory_usage" label="Memory" align="right" width={100} sort={sort} onSort={handleSort} />
                  <SortTh field="efficiency_score" label="Pruning" align="right" width={90} sort={sort} onSort={handleSort} />
                  {killQueriesEnabled && <th style={{ ...thStyle, width: 70 }} aria-label="Actions"></th>}
                </tr>
              </thead>
              <tbody>
                {sortedActivity.map((q) => {
                  const isLive = q.activitySource !== 'history';
                  const sel = selectedQueryId === q.query_id;
                  const isChecked = selectedForCompare.has(q.query_id);
                  const isHovered = hoveredQueryId === q.query_id;
                  const trunc = q.query.length > 60 ? q.query.slice(0, 60) + '...' : q.query;
                  const shortId = q.query_id.slice(0, 8);
                  return (
                    <tr key={q.activityKey}
                      style={{ 
                        background: isChecked ? 'rgba(88, 166, 255, 0.08)' : sel ? 'rgba(88,166,255,0.1)' : isHovered ? 'var(--bg-tertiary)' : 'transparent', 
                        cursor: 'pointer', 
                        transition: 'background 0.1s',
                      }}
                      onMouseEnter={() => setHoveredQueryId(q.query_id)}
                      onClick={() => {
                        if (compareMode) {
                          if (!isLive) toggleCompareSelection(q.query_id);
                        } else if (q.liveQuery) {
                          onSelectRunningQuery(q.liveQuery);
                        } else {
                          onSelectHistoryQuery(q);
                        }
                      }}>
                      {compareMode && (
                        <td style={{ ...tdStyle, textAlign: 'center', width: 32 }}>
                          <span style={{ 
                            display: 'inline-flex', 
                            alignItems: 'center', 
                            justifyContent: 'center',
                            width: 16, 
                            height: 16, 
                            borderRadius: 3, 
                            border: isChecked ? '2px solid #58a6ff' : '1px solid var(--border-primary)',
                            background: isChecked ? '#58a6ff' : 'transparent', 
                            fontSize: 10, 
                            color: '#fff',
                            cursor: isLive ? 'not-allowed' : 'pointer',
                            opacity: isLive ? 0.3 : 1,
                            flexShrink: 0,
                          }}>
                            {isChecked ? '✓' : ''}
                          </span>
                        </td>
                      )}
                      <td style={{ ...tdStyle, width: 48 }} title={resourcePressureTooltip(q)}>
                        <QueryFingerprintGlyph query={q} coordinatorIds={coordinatorIds} size={30} />
                      </td>
                      <td style={{ ...tdStyle, whiteSpace: 'nowrap', fontFamily: 'monospace', fontSize: 10, color: '#58a6ff' }} title={q.query_id}>
                        {shortId}
                      </td>
                      <td style={{ ...tdStyle, whiteSpace: 'nowrap' }}>
                        <QueryKindBadge kind={q.query_kind} />
                        {q.is_initial_query === 0 && (
                          <span title={`Remote worker query (parent: ${q.initial_query_id || 'unknown'})`} style={{
                            display: 'inline-block',
                            marginLeft: 4,
                            padding: '2px 5px',
                            fontSize: 9,
                            fontWeight: 500,
                            borderRadius: 4,
                            background: 'rgba(210,169,34,0.15)',
                            color: '#d29922',
                          }}>
                            worker
                          </span>
                        )}
                        {coordinatorIds?.has(q.query_id) && (
                          <span title="Coordinator — dispatched child queries to remote workers or replicas" style={{
                            display: 'inline-block',
                            marginLeft: 4,
                            padding: '2px 5px',
                            fontSize: 9,
                            fontWeight: 500,
                            borderRadius: 4,
                            background: 'rgba(139,92,246,0.15)',
                            color: '#a78bfa',
                          }}>
                            coordinator
                          </span>
                        )}
                      </td>
                      <td style={{ ...tdStyle, width: 110, whiteSpace: 'nowrap', overflow: 'hidden' }}>
                        <StatusBadge type={q.type} exception={q.exception ?? undefined} />
                      </td>
                      <td style={{ ...tdStyle, whiteSpace: 'nowrap', fontSize: 11 }}>
                        {fmtTime(q.query_start_time)}
                      </td>
                      <td style={{ ...tdStyle, whiteSpace: 'nowrap' }}>{q.user}</td>
                      <td style={{ ...tdStyle, overflow: 'hidden' }} title={q.hostname || ''}>
                        <code style={{
                          display: 'block',
                          width: '100%',
                          fontSize: 11,
                          fontFamily: 'monospace',
                          color: 'var(--text-muted)',
                          overflow: 'hidden',
                          textOverflow: 'ellipsis',
                          whiteSpace: 'nowrap',
                        }}>
                          {q.hostname || '—'}
                        </code>
                      </td>
                      <td style={{ ...tdStyle, overflow: 'hidden' }} title={q.query}>
                        <code style={{
                          display: 'block',
                          width: '100%',
                          fontSize: 11,
                          fontFamily: 'monospace',
                          color: 'var(--text-muted)',
                          overflow: 'hidden',
                          textOverflow: 'ellipsis',
                          whiteSpace: 'nowrap',
                        }}>
                          {trunc}
                        </code>
                      </td>
                      <td style={{ ...tdStyle, textAlign: 'right', fontFamily: 'monospace', fontWeight: 500, color: 'var(--text-primary)', whiteSpace: 'nowrap' }}>
                        {fmtDuration(q.query_duration_ms)}
                      </td>
                      <td style={{ ...tdStyle, textAlign: 'right', fontFamily: 'monospace', whiteSpace: 'nowrap' }}>
                        {formatNumber(q.read_rows)}
                      </td>
                      <td style={{ ...tdStyle, textAlign: 'right', fontFamily: 'monospace', whiteSpace: 'nowrap' }}>
                        {formatBytes(q.read_bytes)}
                      </td>
                      <td style={{ ...tdStyle, textAlign: 'right', fontFamily: 'monospace', whiteSpace: 'nowrap' }}>
                        {formatNumber(q.result_rows)}
                      </td>
                      <td style={{ ...tdStyle, textAlign: 'right', fontFamily: 'monospace', whiteSpace: 'nowrap' }}>
                        {formatBytes(q.memory_usage)}
                      </td>
                      <td style={{ ...tdStyle, textAlign: 'right', whiteSpace: 'nowrap' }}>
                        <EfficiencyBadge score={isLive ? null : q.efficiency_score} />
                      </td>
                      {killQueriesEnabled && (
                      <td style={{ ...tdStyle, textAlign: 'center', whiteSpace: 'nowrap' }}>
                        {isLive && (
                          <button
                            onClick={(event) => {
                              event.stopPropagation();
                              onKillQuery(q.query_id);
                            }}
                            disabled={isKillingQuery}
                            style={{
                              padding: '3px 9px',
                              fontSize: 10,
                              borderRadius: 4,
                              border: '1px solid rgba(248,81,73,0.3)',
                              background: 'rgba(248,81,73,0.1)',
                              color: '#f85149',
                              cursor: isKillingQuery ? 'not-allowed' : 'pointer',
                            }}
                          >
                            Kill
                          </button>
                        )}
                      </td>
                      )}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
          {showHoverPreview && (
            <div
              style={{
                position: 'sticky',
                top: 12,
                zIndex: 2,
                background: 'var(--bg-primary)',
                cursor: previewQuery ? 'pointer' : 'default',
              }}
              onClick={() => {
                if (previewQuery?.liveQuery) onSelectRunningQuery(previewQuery.liveQuery);
                else if (previewQuery) onSelectHistoryQuery(previewQuery);
              }}
            >
              <QueryHoverPreview
                query={previewQuery}
                coordinatorIds={coordinatorIds}
                childQueries={previewChildQueries}
                isLoadingChildQueries={hoverTopology.isLoading}
                childQueryError={hoverTopology.error}
              />
            </div>
          )}
        </div>
      )}

    </div>
  );
};

export default QueryActivityTable;
