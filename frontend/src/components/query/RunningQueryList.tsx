/**
 * RunningQueryList - Active/running queries table with progress bars and kill actions.
 */
import React, { useState, useMemo } from 'react';
import type { RunningQuery } from '../../stores/queryStore';
import { formatBytes, formatDuration, formatNumber } from '../../stores/queryStore';
import { QueryFilterBar } from './QueryFilterBar';
import type { QueryFilterState } from './QueryFilterBar';
import type { QueryAnalyzer } from '@tracehouse/core';
import { useUserPreferenceStore } from '../../stores/userPreferenceStore';

interface RunningQueryListProps {
  queries: RunningQuery[];
  selectedQueryId: string | null;
  onSelectQuery: (query: RunningQuery) => void;
  onKillQuery: (queryId: string) => void;
  isKillingQuery: boolean;
  coordinatorIds?: Set<string>;
  queryAnalyzer?: QueryAnalyzer;
}

type SortKey = 'query_kind' | 'user' | 'hostname' | 'elapsed_seconds' | 'progress' | 'memory_usage' | 'read_rows' | null;

const thStyle: React.CSSProperties = {
  padding: '8px 12px', textAlign: 'left', fontSize: 10, fontWeight: 500,
  color: 'var(--text-muted)', borderBottom: '1px solid var(--border-primary)',
  textTransform: 'uppercase', letterSpacing: '0.5px',
};

const tdStyle: React.CSSProperties = {
  padding: '8px 12px', fontSize: 12, color: 'var(--text-secondary)',
  borderBottom: '1px solid var(--border-primary)',
};

const ProgressBar: React.FC<{ progress: number }> = ({ progress }) => {
  const pct = Math.min(Math.max(progress * 100, 0), 100);
  const color = pct < 50 ? '#58a6ff' : '#3fb950';
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
      <div style={{ width: 60, height: 4, borderRadius: 2, background: 'var(--bg-tertiary)' }}>
        <div style={{ width: `${pct}%`, height: '100%', borderRadius: 2, background: color, transition: 'width 0.3s ease' }} />
      </div>
      <span style={{ fontSize: 10, color: 'var(--text-muted)', minWidth: 36 }}>{pct.toFixed(1)}%</span>
    </div>
  );
};

const QueryKindBadge: React.FC<{ kind: string }> = ({ kind }) => {
  const getColor = (k: string): string => {
    switch (k.toLowerCase()) {
      case 'select': return '#3b82f6';  // blue
      case 'insert': return '#f59e0b';  // amber
      case 'alter': return '#ef4444';   // red
      case 'create': return '#22c55e';  // green
      case 'drop': return '#f43f5e';    // rose
      case 'system': return '#8b5cf6';  // purple
      case 'optimize': return '#06b6d4'; // cyan
      default: return '#94a3b8';        // slate
    }
  };
  const color = getColor(kind);
  return (
    <span style={{
      display: 'inline-block', padding: '2px 6px', fontSize: 10, fontWeight: 500,
      borderRadius: 4, background: `${color}20`, color,
    }}>
      {kind || 'Unknown'}
    </span>
  );
};

export const RunningQueryList: React.FC<RunningQueryListProps> = ({
  queries, selectedQueryId, onSelectQuery, onKillQuery, isKillingQuery, coordinatorIds, queryAnalyzer,
}) => {
  const killQueriesEnabled = useUserPreferenceStore((s) => s.killQueriesEnabled);
  const [sortKey, setSortKey] = React.useState<SortKey>(null);
  const [sortDir, setSortDir] = React.useState<'asc' | 'desc'>('desc');
  const [filter, setFilter] = useState<QueryFilterState>({ limit: 100 });

  const handleFilterChange = (patch: Partial<QueryFilterState>) => {
    setFilter(prev => ({ ...prev, ...patch }));
  };

  const handleSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortDir(d => d === 'asc' ? 'desc' : 'asc');
    } else {
      setSortKey(key);
      setSortDir(key === 'user' || key === 'query_kind' || key === 'hostname' ? 'asc' : 'desc');
    }
  };

  // Client-side filtering of running queries
  const filtered = useMemo(() => {
    let result = queries;

    if (filter.queryId) {
      const qid = filter.queryId.toLowerCase();
      result = result.filter(q => q.query_id.toLowerCase().includes(qid));
    }
    if (filter.user) {
      const u = filter.user.toLowerCase();
      result = result.filter(q => q.user.toLowerCase().includes(u));
    }
    if (filter.hostname) {
      const h = filter.hostname.toLowerCase();
      result = result.filter(q => (q.hostname || '').toLowerCase().includes(h));
    }
    if (filter.queryText) {
      const t = filter.queryText.toLowerCase();
      result = result.filter(q => q.query.toLowerCase().includes(t));
    }
    if (filter.minDurationMs != null && filter.minDurationMs > 0) {
      const minSec = filter.minDurationMs / 1000;
      result = result.filter(q => q.elapsed_seconds >= minSec);
    }
    if (filter.minMemoryBytes != null && filter.minMemoryBytes > 0) {
      result = result.filter(q => q.memory_usage >= filter.minMemoryBytes!);
    }
    if (filter.startTime) {
      const start = new Date(filter.startTime).getTime();
      const now = Date.now();
      // Running queries don't have a start timestamp, but we can estimate from elapsed_seconds
      result = result.filter(q => {
        const queryStart = now - q.elapsed_seconds * 1000;
        return queryStart >= start;
      });
    }
    if (filter.endTime) {
      const end = new Date(filter.endTime).getTime();
      const now = Date.now();
      result = result.filter(q => {
        const queryStart = now - q.elapsed_seconds * 1000;
        return queryStart <= end;
      });
    }
    if (filter.excludeAppQueries) {
      result = result.filter(q => !q.query.includes('source:Monitor:'));
    }
    if (filter.queryKind) {
      const k = filter.queryKind.toLowerCase();
      result = result.filter(q => (q.query_kind || '').toLowerCase() === k);
    }
    // status and database/table filters don't apply to running queries (no data available)
    if (filter.limit && filter.limit > 0) {
      result = result.slice(0, filter.limit);
    }

    return result;
  }, [queries, filter]);

  const sorted = React.useMemo(() => {
    if (!sortKey) return filtered;
    return [...filtered].sort((a, b) => {
      let cmp = 0;
      switch (sortKey) {
        case 'query_kind': cmp = (a.query_kind || '').localeCompare(b.query_kind || ''); break;
        case 'user': cmp = a.user.localeCompare(b.user); break;
        case 'hostname': cmp = (a.hostname || '').localeCompare(b.hostname || ''); break;
        case 'elapsed_seconds': cmp = a.elapsed_seconds - b.elapsed_seconds; break;
        case 'progress': cmp = a.progress - b.progress; break;
        case 'memory_usage': cmp = a.memory_usage - b.memory_usage; break;
        case 'read_rows': cmp = a.read_rows - b.read_rows; break;
      }
      return sortDir === 'asc' ? cmp : -cmp;
    });
  }, [filtered, sortKey, sortDir]);

  const sortIndicator = (key: SortKey) =>
    sortKey === key ? (sortDir === 'asc' ? ' ▲' : ' ▼') : '';

  const sortableTh = (label: string, key: SortKey, align: 'left' | 'right' | 'center' = 'left') => (
    <th
      style={{ ...thStyle, textAlign: align, cursor: 'pointer', userSelect: 'none' }}
      onClick={() => handleSort(key)}
    >
      {label}{sortIndicator(key)}
    </th>
  );

  return (
    <div>
      <QueryFilterBar filter={filter} onFilterChange={handleFilterChange} count={sorted.length} queryAnalyzer={queryAnalyzer} />

      {sorted.length === 0 ? (
        <div style={{ textAlign: 'center', padding: '40px 0', color: 'var(--text-muted)', fontSize: 13 }}>
          {queries.length === 0 ? 'No running queries' : 'No running queries matching the current filters'}
        </div>
      ) : (
      <div style={{ overflowX: 'auto' }}>
      <table style={{ width: '100%', borderCollapse: 'collapse' }}>
        <thead>
          <tr>
            <th style={thStyle}>Query ID</th>
            {sortableTh('Type', 'query_kind')}
            {sortableTh('User', 'user')}
            {sortableTh('Server', 'hostname')}
            <th style={thStyle}>Query</th>
            {sortableTh('Elapsed', 'elapsed_seconds', 'right')}
            {sortableTh('Progress', 'progress')}
            {sortableTh('Memory', 'memory_usage', 'right')}
            {sortableTh('Rows Read', 'read_rows', 'right')}
            {killQueriesEnabled && <th style={{ ...thStyle, textAlign: 'center' }}></th>}
          </tr>
        </thead>
        <tbody>
          {sorted.map((q) => {
            const sel = selectedQueryId === q.query_id;
            const trunc = q.query.length > 80 ? q.query.slice(0, 80) + '...' : q.query;
            return (
              <tr key={q.query_id}
                style={{ background: sel ? 'rgba(88,166,255,0.1)' : 'transparent', cursor: 'pointer', transition: 'background 0.1s' }}
                onMouseEnter={(e) => { if (!sel) e.currentTarget.style.background = 'var(--bg-tertiary)'; }}
                onMouseLeave={(e) => { if (!sel) e.currentTarget.style.background = sel ? 'rgba(88,166,255,0.1)' : 'transparent'; }}
                onClick={() => onSelectQuery(q)}>
                <td style={{ ...tdStyle, fontFamily: 'monospace', color: '#58a6ff', fontSize: 11, whiteSpace: 'nowrap' }} title={q.query_id}>
                  {q.query_id.slice(0, 10)}...
                </td>
                <td style={{ ...tdStyle, whiteSpace: 'nowrap' }}>
                  <QueryKindBadge kind={q.query_kind} />
                  {q.is_initial_query === 0 && (
                    <span title={`Shard sub-query (parent: ${q.initial_query_id || 'unknown'})`} style={{
                      display: 'inline-block', marginLeft: 4, padding: '2px 5px', fontSize: 9,
                      fontWeight: 500, borderRadius: 4, background: 'rgba(210,169,34,0.15)', color: '#d29922',
                    }}>
                      shard
                    </span>
                  )}
                  {coordinatorIds?.has(q.query_id) && (
                    <span title="Coordinator — dispatched sub-queries to shards" style={{
                      display: 'inline-block', marginLeft: 4, padding: '2px 5px', fontSize: 9,
                      fontWeight: 500, borderRadius: 4, background: 'rgba(139,92,246,0.15)', color: '#a78bfa',
                    }}>
                      coordinator
                    </span>
                  )}
                </td>
                <td style={{ ...tdStyle, whiteSpace: 'nowrap' }}>{q.user}</td>
                <td style={{ ...tdStyle, whiteSpace: 'nowrap', fontFamily: 'monospace', fontSize: 11, color: 'var(--text-muted)' }} title={q.hostname || ''}>
                  {q.hostname || '—'}
                </td>
                <td style={tdStyle}>
                  <code style={{ fontSize: 11, color: 'var(--text-muted)', wordBreak: 'break-all' }}>{trunc}</code>
                </td>
                <td style={{ ...tdStyle, textAlign: 'right', fontFamily: 'monospace', fontWeight: 500, color: 'var(--text-primary)', whiteSpace: 'nowrap' }}>
                  {formatDuration(q.elapsed_seconds)}
                </td>
                <td style={tdStyle}><ProgressBar progress={q.progress} /></td>
                <td style={{ ...tdStyle, textAlign: 'right', fontFamily: 'monospace', whiteSpace: 'nowrap' }}>{formatBytes(q.memory_usage)}</td>
                <td style={{ ...tdStyle, textAlign: 'right', fontFamily: 'monospace', whiteSpace: 'nowrap' }}>{formatNumber(q.read_rows)}</td>
                {killQueriesEnabled && (
                <td style={{ ...tdStyle, textAlign: 'center', whiteSpace: 'nowrap' }}>
                  <button
                    onClick={(e) => { e.stopPropagation(); onKillQuery(q.query_id); }}
                    disabled={isKillingQuery}
                    style={{
                      padding: '3px 10px', fontSize: 11, fontWeight: 500, borderRadius: 4,
                      border: 'none', cursor: isKillingQuery ? 'not-allowed' : 'pointer',
                      background: isKillingQuery ? 'var(--bg-tertiary)' : 'rgba(248,81,73,0.12)',
                      color: isKillingQuery ? 'var(--text-muted)' : '#f85149',
                      transition: 'background 0.15s',
                    }}
                    onMouseEnter={(e) => { if (!isKillingQuery) e.currentTarget.style.background = 'rgba(248,81,73,0.25)'; }}
                    onMouseLeave={(e) => { if (!isKillingQuery) e.currentTarget.style.background = 'rgba(248,81,73,0.12)'; }}>
                    Kill
                  </button>
                </td>
                )}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
      )}
    </div>
  );
};

export default RunningQueryList;