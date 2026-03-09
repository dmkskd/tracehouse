/**
 * RunningQueriesTable - Table showing currently running queries
 */

import React, { useCallback } from 'react';
import { Link } from 'react-router-dom';
import { ProgressRing } from '../overview/ProgressRing';
import { SortableHeader } from '../overview/SortableHeader';
import { OVERVIEW_COLORS } from '../../styles/overviewColors';
import { useSortState, useSortedData } from '../../hooks/useSortState';
import { CopyTableButton } from '../common/CopyTableButton';
import type { RunningQueryInfo } from '@tracehouse/core';
import { formatBytes, formatBytesPerSec, formatElapsed, formatNumberCompact as formatNumber, truncateQuery } from '../../utils/formatters';

type QuerySortKey = 'query' | 'user' | 'elapsed' | 'cpu' | 'memory' | 'io' | 'rows' | 'progress';

interface RunningQueriesTableProps {
  queries: RunningQueryInfo[];
  expandedQueryId: string | null;
  onToggleExpand: (queryId: string) => void;
  maxRows?: number;
  className?: string;
}

function getQueryKindColor(kind: string): string {
  switch (kind.toUpperCase()) {
    case 'SELECT': return OVERVIEW_COLORS.queries;
    case 'INSERT': return OVERVIEW_COLORS.merges;
    case 'ALTER': return OVERVIEW_COLORS.mutations;
    default: return OVERVIEW_COLORS.other;
  }
}


export function RunningQueriesTable({
  queries,
  expandedQueryId,
  onToggleExpand,
  maxRows = 10,
  className = '',
}: RunningQueriesTableProps) {
  const { sort, toggleSort } = useSortState<QuerySortKey>('cpu');
  const getValue = useCallback((item: RunningQueryInfo, key: QuerySortKey): number | string => {
    switch (key) {
      case 'query': return item.queryKind;
      case 'user': return item.user;
      case 'elapsed': return item.elapsed;
      case 'cpu': return item.cpuCores;
      case 'memory': return item.memoryUsage;
      case 'io': return item.ioReadRate;
      case 'rows': return item.rowsRead;
      case 'progress': return item.progress;
    }
  }, []);
  const sortedQueries = useSortedData(queries, sort, getValue);

  // Calculate max height: header (~32px) + rows (~40px each)
  const maxHeight = 32 + (maxRows * 40);

  if (sortedQueries.length === 0) {
    return (
      <div className={`rounded-lg p-6 border ${className}`} style={{ background: 'var(--bg-card)', borderColor: 'var(--border-primary)', height: '100%' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 16 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
            <h3 style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-primary)' }}>Running Queries</h3>
            <Link to="/queries" state={{ from: { path: '/overview', label: 'Overview' } }} title="Queries" style={{ fontSize: 11, color: 'var(--text-muted)', textDecoration: 'none', display: 'flex', alignItems: 'center', gap: 3 }}>
              <span>→</span>
            </Link>
          </div>
        </div>
        <p style={{ fontSize: 11, textAlign: 'center', padding: '16px 0', color: 'var(--text-muted)' }}>No queries currently running</p>
      </div>
    );
  }

  return (
    <div className={`rounded-lg border overflow-hidden ${className}`} style={{ background: 'var(--bg-card)', borderColor: 'var(--border-primary)', height: '100%', display: 'flex', flexDirection: 'column' }}>
      <div className="px-4 py-3 border-b" style={{ borderColor: 'var(--border-secondary)', flexShrink: 0 }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '6px 4px' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
            <h3 style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-primary)' }}>
              Running Queries <span style={{ fontWeight: 400, color: 'var(--text-muted)' }}>({sortedQueries.length})</span>
            </h3>
            <Link to="/queries" state={{ from: { path: '/overview', label: 'Overview' } }} title="Queries" style={{ fontSize: 11, color: 'var(--text-muted)', textDecoration: 'none', display: 'flex', alignItems: 'center', gap: 3 }}>
              <span>→</span>
            </Link>
          </div>
          <CopyTableButton
            headers={['Query Kind', 'Query', 'User', 'Elapsed', 'CPU', 'Memory', 'IO', 'Rows', 'Progress']}
            rows={sortedQueries.map(q => [
              q.queryKind, q.query, q.user, formatElapsed(q.elapsed),
              q.cpuCores.toFixed(2), formatBytes(q.memoryUsage),
              formatBytesPerSec(q.ioReadRate), formatNumber(q.rowsRead),
              `${q.progress.toFixed(1)}%`,
            ])}
          />
        </div>
      </div>
      <div style={{ flex: 1, maxHeight: `${maxHeight}px`, overflowY: 'auto', overflowX: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11, tableLayout: 'fixed' }}>
          <thead className="sticky top-0 z-10" style={{ background: 'var(--bg-card)' }}>
            <tr style={{ borderBottom: '1px solid var(--border-secondary)' }}>
              <SortableHeader label="Query" sortKey="query" activeSortKey={sort.key} direction={sort.direction} onSort={toggleSort as (k: string) => void} />
              <SortableHeader label="User" sortKey="user" activeSortKey={sort.key} direction={sort.direction} onSort={toggleSort as (k: string) => void} width={60} />
              <SortableHeader label="Elapsed" sortKey="elapsed" activeSortKey={sort.key} direction={sort.direction} onSort={toggleSort as (k: string) => void} align="right" width={52} />
              <SortableHeader label="CPU" sortKey="cpu" activeSortKey={sort.key} direction={sort.direction} onSort={toggleSort as (k: string) => void} align="right" width={44} />
              <SortableHeader label="Memory" sortKey="memory" activeSortKey={sort.key} direction={sort.direction} onSort={toggleSort as (k: string) => void} align="right" width={68} />
              <SortableHeader label="IO" sortKey="io" activeSortKey={sort.key} direction={sort.direction} onSort={toggleSort as (k: string) => void} align="right" width={60} />
              <SortableHeader label="Rows" sortKey="rows" activeSortKey={sort.key} direction={sort.direction} onSort={toggleSort as (k: string) => void} align="right" width={52} />
              <SortableHeader label="" sortKey="progress" activeSortKey={sort.key} direction={sort.direction} onSort={toggleSort as (k: string) => void} align="center" width={36} />
            </tr>
          </thead>
          <tbody>
            {sortedQueries.map((query, idx) => {
              const isExpanded = expandedQueryId === query.queryId;
              return (
                <React.Fragment key={query.queryId}>
                  <tr
                    onClick={() => onToggleExpand(query.queryId)}
                    style={{ 
                      borderBottom: '1px solid var(--border-secondary)',
                      background: idx % 2 === 0 ? 'transparent' : 'var(--bg-tertiary)',
                      cursor: 'pointer',
                    }}
                  >
                    <td style={{ padding: '5px 8px' }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                        <span
                          style={{
                            padding: '2px 6px',
                            fontSize: 10,
                            fontWeight: 500,
                            borderRadius: 4,
                            backgroundColor: `${getQueryKindColor(query.queryKind)}20`,
                            color: getQueryKindColor(query.queryKind),
                          }}
                        >
                          {query.queryKind}
                        </span>
                        <span style={{ fontFamily: 'monospace', color: 'var(--text-primary)', maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                          {truncateQuery(query.query)}
                        </span>
                      </div>
                    </td>
                    <td style={{ padding: '5px 8px', color: 'var(--text-muted)' }}>{query.user}</td>
                    <td style={{ padding: '5px 8px', textAlign: 'right', fontFamily: 'monospace', color: 'var(--text-primary)' }}>
                      {formatElapsed(query.elapsed)}
                    </td>
                    <td style={{ padding: '5px 8px', textAlign: 'right', fontFamily: 'monospace', color: 'var(--text-primary)' }}>
                      {query.cpuCores.toFixed(2)}
                    </td>
                    <td style={{ padding: '5px 8px', textAlign: 'right', fontFamily: 'monospace', color: 'var(--text-muted)' }}>
                      {formatBytes(query.memoryUsage)}
                    </td>
                    <td style={{ padding: '5px 8px', textAlign: 'right', fontFamily: 'monospace', color: 'var(--text-muted)' }}>
                      {formatBytesPerSec(query.ioReadRate)}
                    </td>
                    <td style={{ padding: '5px 8px', textAlign: 'right', fontFamily: 'monospace', color: 'var(--text-muted)' }}>
                      {formatNumber(query.rowsRead)}
                    </td>
                    <td style={{ padding: '5px 4px', textAlign: 'center' }}>
                      <ProgressRing
                        pct={query.progress}
                        size={20}
                        stroke={2}
                        color={OVERVIEW_COLORS.queries}
                        showPercent={false}
                      />
                    </td>
                  </tr>
                  {isExpanded && (
                    <tr style={{ background: 'var(--bg-tertiary)' }}>
                      <td colSpan={8} style={{ padding: '8px 12px' }}>
                        <div style={{ fontFamily: 'monospace', fontSize: 10, whiteSpace: 'pre-wrap', wordBreak: 'break-all', padding: 12, borderRadius: 4, color: 'var(--text-secondary)', background: 'var(--bg-secondary)' }}>
                          {query.query}
                        </div>
                        <div style={{ marginTop: 8, fontSize: 10, color: 'var(--text-muted)' }}>
                          Query ID: <span style={{ fontFamily: 'monospace', color: 'var(--text-tertiary)' }}>{query.queryId}</span>
                        </div>
                      </td>
                    </tr>
                  )}
                </React.Fragment>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default RunningQueriesTable;
