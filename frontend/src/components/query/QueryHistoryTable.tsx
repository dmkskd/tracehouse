/**
 * QueryHistoryTable - Query history with filtering and sorting
 * Supports multi-select comparison of queries (even across different query hashes)
 */

import React, { useCallback, useState } from 'react';
import type { 
  QueryHistoryItem, 
  QueryHistoryFilter, 
  QueryHistorySort, 
  SortField,
  SortDirection,
} from '../../stores/queryStore';
import { formatBytes, formatNumber, sortQueryHistory } from '../../stores/queryStore';
import { formatDurationMs } from '../../utils/formatters';
import { QueryComparisonPanel } from './QueryComparisonPanel';
import type { ComparableQuery } from './QueryComparisonPanel';
import { QueryFilterBar } from './QueryFilterBar';
import type { QueryAnalyzer } from '@tracehouse/core';

interface QueryHistoryTableProps {
  history: QueryHistoryItem[];
  selectedQueryId: string | null;
  onSelectQuery: (query: QueryHistoryItem) => void;
  filter: QueryHistoryFilter;
  sort: QueryHistorySort;
  onFilterChange: (filter: Partial<QueryHistoryFilter>) => void;
  onSortChange: (sort: QueryHistorySort) => void;
  isLoading: boolean;
  queryAnalyzer?: QueryAnalyzer;
  coordinatorIds?: Set<string>;
}

const thStyle: React.CSSProperties = {
  padding: '8px 12px',
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
  padding: '8px 12px',
  fontSize: 12,
  color: 'var(--text-secondary)',
  borderBottom: '1px solid var(--border-primary)',
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
  const isError = type === 'ExceptionWhileProcessing' || !!exception;
  // Truncate exception message for display
  const displayText = isError && exception 
    ? (exception.length > 25 ? exception.slice(0, 25) + '...' : exception)
    : (isError ? 'Error' : 'Success');
  return (
    <span 
      style={{
        display: 'inline-block',
        padding: '2px 8px',
        fontSize: 10,
        fontWeight: 500,
        borderRadius: 10,
        background: isError ? 'rgba(248,81,73,0.15)' : 'rgba(63,185,80,0.15)',
        color: isError ? '#f85149' : '#3fb950',
        maxWidth: 150,
        overflow: 'hidden',
        textOverflow: 'ellipsis',
        whiteSpace: 'nowrap',
      }}
      title={exception || (isError ? 'Error' : 'Success')}
    >
      {displayText}
    </span>
  );
};

const EfficiencyBadge: React.FC<{ score?: number | null }> = ({ score }) => {
  if (score === undefined || score === null) return <span style={{ color: 'var(--text-muted)' }}>—</span>;
  // Higher pruning % = better (more marks skipped via index)
  const color = score >= 80 ? '#3fb950' : score >= 50 ? '#d29922' : '#f85149';
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

export const QueryHistoryTable: React.FC<QueryHistoryTableProps> = ({
  history, selectedQueryId, onSelectQuery, filter, sort, onFilterChange, onSortChange, isLoading, queryAnalyzer, coordinatorIds,
}) => {
  const [compareMode, setCompareMode] = useState(false);
  const [selectedForCompare, setSelectedForCompare] = useState<Set<string>>(new Set());

  const handleSort = useCallback((field: SortField) => {
    const dir: SortDirection = sort.field === field && sort.direction === 'desc' ? 'asc' : 'desc';
    onSortChange({ field, direction: dir });
  }, [sort, onSortChange]);

  const sortedHistory = sortQueryHistory(
    filter.hostname
      ? history.filter(q => q.hostname?.toLowerCase().includes(filter.hostname!.toLowerCase()))
      : history,
    sort,
  );

  const toggleCompareSelection = useCallback((queryId: string) => {
    setSelectedForCompare(prev => {
      const next = new Set(prev);
      if (next.has(queryId)) next.delete(queryId); else next.add(queryId);
      return next;
    });
  }, []);

  const cancelCompare = useCallback(() => {
    setCompareMode(false);
    setSelectedForCompare(new Set());
  }, []);

  const comparedQueries: ComparableQuery[] = compareMode && selectedForCompare.size >= 2
    ? sortedHistory.filter(q => selectedForCompare.has(q.query_id)).map(toComparable)
    : [];

  const SortTh: React.FC<{ field: SortField; label: string; align?: 'left' | 'right' }> = ({ field, label, align = 'left' }) => {
    const active = sort.field === field;
    return (
      <th style={{ ...thStyle, textAlign: align, cursor: 'pointer' }} onClick={() => handleSort(field)}>
        {label}{' '}
        <span style={{ color: active ? '#58a6ff' : 'var(--text-muted)', fontSize: 9 }}>
          {active ? (sort.direction === 'asc' ? '▲' : '▼') : '⇅'}
        </span>
      </th>
    );
  };

  return (
    <div>
      <QueryFilterBar filter={filter} onFilterChange={onFilterChange} queryAnalyzer={queryAnalyzer} />

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
          <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>
            {selectedForCompare.size === 0 
              ? 'Select 2 or more queries to compare' 
              : `${selectedForCompare.size} selected`}
          </span>
        )}
      </div>

      {sortedHistory.length === 0 ? (
        <div style={{ textAlign: 'center', padding: '40px 0', color: 'var(--text-muted)', fontSize: 13 }}>
          {isLoading ? 'Loading query history...' : 'No queries found matching the current filters'}
        </div>
      ) : (
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse' }}>
            <thead>
              <tr>
                {compareMode && <th style={{ ...thStyle, width: 32, textAlign: 'center' }}></th>}
                <th style={thStyle}>ID</th>
                <th style={thStyle}>Type</th>
                <th style={thStyle}>Status</th>
                <SortTh field="query_start_time" label="Time" />
                <th style={thStyle}>User</th>
                <th style={thStyle}>Server</th>
                <th style={thStyle}>Query</th>
                <SortTh field="query_duration_ms" label="Duration" align="right" />
                <SortTh field="read_rows" label="Rows Read" align="right" />
                <SortTh field="read_bytes" label="Bytes Read" align="right" />
                <SortTh field="result_rows" label="Result" align="right" />
                <SortTh field="memory_usage" label="Memory" align="right" />
                <SortTh field="efficiency_score" label="Pruning" align="right" />
              </tr>
            </thead>
            <tbody>
              {sortedHistory.map((q) => {
                const sel = selectedQueryId === q.query_id;
                const isChecked = selectedForCompare.has(q.query_id);
                const trunc = q.query.length > 60 ? q.query.slice(0, 60) + '...' : q.query;
                const shortId = q.query_id.slice(0, 8);
                return (
                  <tr key={q.query_id}
                    style={{ 
                      background: isChecked ? 'rgba(88, 166, 255, 0.08)' : sel ? 'rgba(88,166,255,0.1)' : 'transparent', 
                      cursor: 'pointer', 
                      transition: 'background 0.1s',
                    }}
                    onMouseEnter={(e) => { if (!sel && !isChecked) e.currentTarget.style.background = 'var(--bg-tertiary)'; }}
                    onMouseLeave={(e) => { if (!sel && !isChecked) e.currentTarget.style.background = 'transparent'; }}
                    onClick={() => {
                      if (compareMode) toggleCompareSelection(q.query_id);
                      else onSelectQuery(q);
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
                          cursor: 'pointer',
                          flexShrink: 0,
                        }}>
                          {isChecked ? '✓' : ''}
                        </span>
                      </td>
                    )}
                    <td style={{ ...tdStyle, whiteSpace: 'nowrap', fontFamily: 'monospace', fontSize: 10, color: '#58a6ff' }} title={q.query_id}>
                      {shortId}
                    </td>
                    <td style={{ ...tdStyle, whiteSpace: 'nowrap' }}>
                      <QueryKindBadge kind={q.query_kind} />
                      {q.is_initial_query === 0 && (
                        <span title={`Shard sub-query (parent: ${q.initial_query_id || 'unknown'})`} style={{
                          display: 'inline-block',
                          marginLeft: 4,
                          padding: '2px 5px',
                          fontSize: 9,
                          fontWeight: 500,
                          borderRadius: 4,
                          background: 'rgba(210,169,34,0.15)',
                          color: '#d29922',
                        }}>
                          shard
                        </span>
                      )}
                      {coordinatorIds?.has(q.query_id) && (
                        <span title="Coordinator — dispatched sub-queries to shards" style={{
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
                    <td style={{ ...tdStyle, whiteSpace: 'nowrap' }}>
                      <StatusBadge type={q.type} exception={q.exception ?? undefined} />
                    </td>
                    <td style={{ ...tdStyle, whiteSpace: 'nowrap', fontSize: 11 }}>
                      {fmtTime(q.query_start_time)}
                    </td>
                    <td style={{ ...tdStyle, whiteSpace: 'nowrap' }}>{q.user}</td>
                    <td style={{ ...tdStyle, whiteSpace: 'nowrap', fontFamily: 'monospace', fontSize: 11, color: 'var(--text-muted)' }} title={q.hostname || ''}>
                      {q.hostname || '—'}
                    </td>
                    <td style={tdStyle}>
                      <code style={{ fontSize: 11, fontFamily: 'monospace', color: 'var(--text-muted)', wordBreak: 'break-all' }}>{trunc}</code>
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
                      <EfficiencyBadge score={q.efficiency_score} />
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      {/* Comparison panel - shown when 2+ queries selected */}
      {comparedQueries.length >= 2 && (
        <QueryComparisonPanel
          queries={comparedQueries}
          onClose={cancelCompare}
        />
      )}
    </div>
  );
};

export default QueryHistoryTable;
