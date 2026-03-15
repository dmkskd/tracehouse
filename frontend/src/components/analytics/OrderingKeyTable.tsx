/**
 * OrderingKeyTable — shows per-table ordering key efficiency analysis.
 * Expanding a row fetches the per-query-pattern breakdown (by normalized_query_hash)
 * with ordering key usage diagnostics explaining why pruning is good or poor.
 */

import React, { useState, useMemo, useEffect, useCallback } from 'react';
import type { TableOrderingKeyEfficiency, TableQueryPattern, ExplainIndexesResult, QuerySeries } from '@tracehouse/core';
import { diagnoseOrderingKeyUsage, type OrderingKeyDiagnostic } from '@tracehouse/core';
import { useClickHouseServices } from '../../providers/ClickHouseProvider';
import { QueryDetailModal } from '../query/QueryDetailModal';
import { CopyTableButton } from '../common/CopyTableButton';
import { SqlHighlight } from '../common/SqlHighlight';
import { formatBytes } from '../../utils/formatters';

interface Props {
  data: TableOrderingKeyEfficiency[];
  isLoading: boolean;
  lookbackDays: number;
}

type SortField = 'database' | 'table_name' | 'query_count' | 'avg_pruning_pct' | 'poor_pruning_queries' | 'avg_duration_ms' | 'avg_memory_bytes' | 'total_rows_read';
type SortDir = 'asc' | 'desc';

function formatNumber(n: number): string {
  if (n >= 1_000_000_000) return `${(n / 1_000_000_000).toFixed(1)}B`;
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return n.toFixed(0);
}

function formatDuration(ms: number): string {
  if (ms >= 1000) return `${(ms / 1000).toFixed(1)}s`;
  return `${ms.toFixed(0)}ms`;
}

function pruningColor(pct: number | null): string {
  if (pct == null) return 'var(--text-muted)';
  if (pct >= 90) return '#3fb950';
  if (pct >= 50) return '#d29922';
  return '#f85149';
}

function pruningLabel(pct: number | null): string {
  if (pct == null) return '—';
  if (pct >= 90) return 'Excellent';
  if (pct >= 50) return 'Fair';
  return 'Poor';
}

const DIAG_COLORS: Record<string, string> = {
  good: '#3fb950',
  warning: '#d29922',
  poor: '#f85149',
};

/** Stable color palette for user breakdown bars */
const USER_COLORS = [
  '#58a6ff', '#3fb950', '#d29922', '#f85149', '#bc8cff',
  '#f778ba', '#79c0ff', '#7ee787', '#e3b341', '#ffa198',
];

function getUserColor(index: number): string {
  return USER_COLORS[index % USER_COLORS.length];
}

const UserBreakdownBar: React.FC<{ breakdown: Record<string, number>; total: number }> = ({ breakdown, total }) => {
  const [hoveredUser, setHoveredUser] = useState<string | null>(null);

  const sorted = useMemo(() =>
    Object.entries(breakdown).sort((a, b) => b[1] - a[1]),
  [breakdown]);

  if (sorted.length === 0 || total === 0) return <span style={{ fontSize: 10, color: 'var(--text-muted)' }}>—</span>;

  return (
    <div style={{ position: 'relative', width: '100%', minWidth: 80 }}>
      <div style={{ display: 'flex', height: 16, borderRadius: 3, overflow: 'hidden', background: 'rgba(255,255,255,0.05)' }}>
        {sorted.map(([user, count], i) => {
          const pct = (count / total) * 100;
          if (pct < 1) return null;
          return (
            <div
              key={user}
              onMouseEnter={() => setHoveredUser(user)}
              onMouseLeave={() => setHoveredUser(null)}
              style={{
                width: `${pct}%`,
                background: getUserColor(i),
                opacity: hoveredUser === null || hoveredUser === user ? 1 : 0.3,
                transition: 'opacity 0.15s',
                cursor: 'default',
                minWidth: 2,
              }}
            />
          );
        })}
      </div>
      {hoveredUser && (
        <div style={{
          position: 'absolute', bottom: '100%', left: '50%', transform: 'translateX(-50%)',
          padding: '4px 8px', borderRadius: 4, fontSize: 10, whiteSpace: 'nowrap',
          background: 'var(--bg-card, #1e1e1e)', border: '1px solid var(--border-primary)',
          color: 'var(--text-primary)', zIndex: 10, marginBottom: 4,
          boxShadow: '0 2px 8px rgba(0,0,0,0.3)',
          fontFamily: "'Share Tech Mono', monospace",
        }}>
          {hoveredUser}: {breakdown[hoveredUser]} ({((breakdown[hoveredUser] / total) * 100).toFixed(0)}%)
        </div>
      )}
    </div>
  );
};

const PruningBadge: React.FC<{ pct: number | null }> = ({ pct }) => {
  const color = pruningColor(pct);
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
      <div style={{
        padding: '2px 8px', borderRadius: 4, fontSize: 11, fontWeight: 600,
        background: `${color}18`, color,
        fontFamily: "'Share Tech Mono', monospace",
      }}>
        {pct != null ? `${pct.toFixed(1)}%` : '—'}
      </div>
      <span style={{ fontSize: 10, color: 'var(--text-muted)' }}>{pruningLabel(pct)}</span>
    </div>
  );
};

const DiagBadge: React.FC<{ diag: OrderingKeyDiagnostic }> = ({ diag }) => {
  const color = DIAG_COLORS[diag.severity] || 'var(--text-muted)';
  return (
    <div style={{
      padding: '2px 8px', borderRadius: 4, fontSize: 10, fontWeight: 600,
      background: `${color}18`, color, whiteSpace: 'nowrap',
    }}>
      {diag.label}
    </div>
  );
};

export const OrderingKeyTable: React.FC<Props> = ({ data, isLoading, lookbackDays }) => {
  const [sortField, setSortField] = useState<SortField>('query_count');
  const [sortDir, setSortDir] = useState<SortDir>('desc');
  const [expandedRow, setExpandedRow] = useState<string | null>(null);

  const sorted = useMemo(() => {
    return [...data].sort((a, b) => {
      const aVal = a[sortField] ?? -1;
      const bVal = b[sortField] ?? -1;
      const cmp = aVal < bVal ? -1 : aVal > bVal ? 1 : 0;
      return sortDir === 'desc' ? -cmp : cmp;
    });
  }, [data, sortField, sortDir]);

  const handleSort = (field: SortField) => {
    if (sortField === field) setSortDir(d => d === 'asc' ? 'desc' : 'asc');
    else { setSortField(field); setSortDir('desc'); }
  };

  const hdr: React.CSSProperties = {
    padding: '8px 12px', fontSize: 10, fontWeight: 600,
    textTransform: 'uppercase', letterSpacing: '0.5px',
    color: 'var(--text-muted)', cursor: 'pointer', userSelect: 'none',
    whiteSpace: 'nowrap', borderBottom: '1px solid var(--border-primary)',
  };
  const cell: React.CSSProperties = {
    padding: '10px 12px', fontSize: 12,
    borderBottom: '1px solid var(--border-secondary, var(--border-primary))',
  };
  const mono: React.CSSProperties = { fontFamily: "'Share Tech Mono', monospace" };
  const si = (f: SortField) => sortField !== f ? '' : sortDir === 'asc' ? ' ↑' : ' ↓';

  if (isLoading) {
    return <div style={{ padding: 40, textAlign: 'center', color: 'var(--text-muted)', fontSize: 13 }}>Analyzing ordering key efficiency across query history…</div>;
  }
  if (data.length === 0) {
    return <div style={{ padding: 40, textAlign: 'center', color: 'var(--text-muted)', fontSize: 13 }}>No SELECT query data found in query_log. Run some queries first, or increase the lookback window.</div>;
  }

  return (
    <div style={{ overflowX: 'auto' }}>
      <div style={{ display: 'flex', justifyContent: 'flex-end', padding: '4px 12px' }}>
        <CopyTableButton
          headers={['Database', 'Table', 'Queries', 'Avg Pruning %', 'Poor Queries', 'Total Rows Read', 'Avg Duration', 'Avg Memory', 'ORDER BY']}
          rows={sorted.map(r => [
            r.database, r.table_name, formatNumber(r.query_count),
            r.avg_pruning_pct != null ? `${r.avg_pruning_pct.toFixed(1)}%` : '—',
            r.poor_pruning_queries, formatNumber(r.total_rows_read),
            formatDuration(r.avg_duration_ms), formatBytes(r.avg_memory_bytes),
            r.sorting_key || '—',
          ])}
        />
      </div>
      <table style={{ width: '100%', borderCollapse: 'collapse' }}>
        <thead>
          <tr>
            <th style={{ ...hdr, textAlign: 'left' }} onClick={() => handleSort('database')}>Database{si('database')}</th>
            <th style={{ ...hdr, textAlign: 'left' }} onClick={() => handleSort('table_name')}>Table{si('table_name')}</th>
            <th style={{ ...hdr, textAlign: 'right' }} onClick={() => handleSort('query_count')}>Queries{si('query_count')}</th>
            <th style={{ ...hdr, textAlign: 'left' }} onClick={() => handleSort('avg_pruning_pct')}>Avg Pruning{si('avg_pruning_pct')}</th>
            <th style={{ ...hdr, textAlign: 'right' }} onClick={() => handleSort('poor_pruning_queries')}>Poor Queries{si('poor_pruning_queries')}</th>
            <th style={{ ...hdr, textAlign: 'right' }} onClick={() => handleSort('total_rows_read')}>Total Rows Read{si('total_rows_read')}</th>
            <th style={{ ...hdr, textAlign: 'right' }} onClick={() => handleSort('avg_duration_ms')}>Avg Duration{si('avg_duration_ms')}</th>
            <th style={{ ...hdr, textAlign: 'right' }} onClick={() => handleSort('avg_memory_bytes')}>Avg Memory{si('avg_memory_bytes')}</th>
            <th style={{ ...hdr, textAlign: 'left' }}>ORDER BY</th>
          </tr>
        </thead>
        <tbody>
          {sorted.map((row) => {
            const rowKey = `${row.database}.${row.table_name}`;
            const isExpanded = expandedRow === rowKey;
            return (
              <React.Fragment key={rowKey}>
                <tr
                  onClick={() => setExpandedRow(isExpanded ? null : rowKey)}
                  style={{ cursor: 'pointer', background: isExpanded ? 'var(--bg-card-hover, rgba(255,255,255,0.03))' : 'transparent', transition: 'background 0.1s' }}
                  onMouseEnter={e => { if (!isExpanded) e.currentTarget.style.background = 'var(--bg-card-hover, rgba(255,255,255,0.02))'; }}
                  onMouseLeave={e => { if (!isExpanded) e.currentTarget.style.background = 'transparent'; }}
                >
                  <td style={{ ...cell, ...mono, color: 'var(--text-secondary)' }}>{row.database}</td>
                  <td style={{ ...cell, ...mono, color: 'var(--text-primary)' }}>{row.table_name}</td>
                  <td style={{ ...cell, ...mono, textAlign: 'right' }}>{formatNumber(row.query_count)}</td>
                  <td style={cell}><PruningBadge pct={row.avg_pruning_pct} /></td>
                  <td style={{ ...cell, ...mono, textAlign: 'right', color: row.poor_pruning_queries > 0 ? '#f85149' : 'var(--text-muted)' }}>{row.poor_pruning_queries}</td>
                  <td style={{ ...cell, ...mono, textAlign: 'right' }}>{formatNumber(row.total_rows_read)}</td>
                  <td style={{ ...cell, ...mono, textAlign: 'right' }}>{formatDuration(row.avg_duration_ms)}</td>
                  <td style={{ ...cell, ...mono, textAlign: 'right' }}>{formatBytes(row.avg_memory_bytes)}</td>
                  <td style={{ ...cell, fontSize: 11, color: 'var(--text-secondary)', maxWidth: 250, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{row.sorting_key || '—'}</td>
                </tr>
                {isExpanded && (
                  <tr><td colSpan={9} style={{ padding: 0 }}>
                    <ExpandedDetail row={row} lookbackDays={lookbackDays} />
                  </td></tr>
                )}
              </React.Fragment>
            );
          })}
        </tbody>
      </table>
    </div>
  );
};


/** Expanded detail: table metadata + query patterns drill-down */
const ExpandedDetail: React.FC<{ row: TableOrderingKeyEfficiency; lookbackDays: number }> = ({ row, lookbackDays }) => {
  const services = useClickHouseServices();
  const [patterns, setPatterns] = useState<TableQueryPattern[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchPatterns = useCallback(async () => {
    if (!services) return;
    setLoading(true);
    setError(null);
    try {
      const result = await services.analyticsService.getTableQueryPatterns(
        row.database, row.table_name, lookbackDays,
      );
      setPatterns(result);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load query patterns');
    } finally {
      setLoading(false);
    }
  }, [services, row.database, row.table_name, lookbackDays]);

  useEffect(() => { fetchPatterns(); }, [fetchPatterns]);

  const lbl: React.CSSProperties = {
    fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase',
    letterSpacing: '0.5px', marginBottom: 4,
  };

  return (
    <div style={{
      padding: '16px 24px 20px',
      background: 'var(--bg-secondary, rgba(0,0,0,0.15))',
      borderBottom: '1px solid var(--border-primary)',
    }}>
      <div style={{ display: 'flex', gap: 24, marginBottom: 12 }}>
        <div style={{ flex: 1 }}>
          <div style={lbl}>Primary Key</div>
          <div style={{ fontSize: 12, fontFamily: "'Share Tech Mono', monospace", color: 'var(--text-primary)' }}>
            {row.primary_key || '(none)'}
          </div>
        </div>
        <div style={{ flex: 1 }}>
          <div style={lbl}>Sorting Key (ORDER BY)</div>
          <div style={{ fontSize: 12, fontFamily: "'Share Tech Mono', monospace", color: 'var(--text-primary)' }}>
            {row.sorting_key || '(none)'}
          </div>
        </div>
      </div>

      <div style={lbl}>Query Patterns (by normalized hash)</div>
      {error && (
        <div style={{ padding: '8px 12px', borderRadius: 6, fontSize: 12, color: '#f85149', background: 'rgba(248,81,73,0.08)', marginTop: 8 }}>
          {error}
        </div>
      )}
      {loading ? (
        <div style={{ padding: 16, color: 'var(--text-muted)', fontSize: 12 }}>Loading query patterns…</div>
      ) : patterns.length === 0 ? (
        <div style={{ padding: 16, color: 'var(--text-muted)', fontSize: 12 }}>No query patterns found.</div>
      ) : (
        <div style={{ marginTop: 8 }}>
          <QueryPatternsTable patterns={patterns} sortingKey={row.sorting_key} />
        </div>
      )}
    </div>
  );
};


/** Nested table showing query patterns with ordering key diagnostics */
const QueryPatternsTable: React.FC<{ patterns: TableQueryPattern[]; sortingKey: string | null }> = ({ patterns, sortingKey }) => {
  const services = useClickHouseServices();
  const [expandedHash, setExpandedHash] = useState<string | null>(null);
  const [explainResults, setExplainResults] = useState<Map<string, ExplainIndexesResult>>(new Map());
  const [sortField, setSortField] = useState<'execution_count' | 'avg_pruning_pct' | 'avg_duration_ms' | 'p95_duration_ms' | 'p99_duration_ms' | 'avg_memory_bytes' | 'avg_rows_read'>('execution_count');
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc');
  const [detailQuery, setDetailQuery] = useState<QuerySeries | null>(null);
  const [loadingDetailHash, setLoadingDetailHash] = useState<string | null>(null);

  const handleViewLatestQuery = useCallback(async (queryHash: string, sampleQuery: string, e: React.MouseEvent) => {
    e.stopPropagation();
    if (!services) return;
    setLoadingDetailHash(queryHash);
    try {
      const similar = await services.queryAnalyzer.getSimilarQueries(queryHash, 1);
      console.debug('[OrderingKeyTable] getSimilarQueries returned', similar.length, 'rows for hash', queryHash);
      if (similar.length === 0) return;
      const q = similar[similar.length - 1]; // latest (sorted ASC, so last = most recent)
      // ClickHouse may return "2024-01-15 12:34:56" (space-separated, no T/Z).
      // Normalise to ISO-8601 so Date() parses reliably across browsers.
      const rawStart = String(q.query_start_time).replace(' ', 'T');
      const startDate = new Date(rawStart.includes('Z') || rawStart.includes('+') ? rawStart : rawStart + 'Z');
      const endDate = new Date(startDate.getTime() + (q.query_duration_ms || 0));
      const startIso = isNaN(startDate.getTime()) ? rawStart : startDate.toISOString();
      const endIso = isNaN(endDate.getTime()) ? rawStart : endDate.toISOString();
      setDetailQuery({
        query_id: q.query_id,
        label: q.query || sampleQuery || '',
        user: q.user || 'default',
        start_time: startIso,
        end_time: endIso,
        duration_ms: q.query_duration_ms || 0,
        peak_memory: q.memory_usage || 0,
        cpu_us: q.cpu_time_us || 0,
        net_send: 0,
        net_recv: 0,
        disk_read: q.read_bytes || 0,
        disk_write: 0,
        status: q.exception_code ? 'ExceptionWhileProcessing' : 'QueryFinish',
        exception_code: q.exception_code,
        exception: q.exception || undefined,
        points: [],
      });
    } catch (err) {
      console.warn('[OrderingKeyTable] Failed to load query detail:', err);
    } finally {
      setLoadingDetailHash(null);
    }
  }, [services]);

  // Run EXPLAIN indexes = 1 for each pattern on mount
  useEffect(() => {
    if (!services || patterns.length === 0) return;
    let cancelled = false;

    (async () => {
      const results = new Map<string, ExplainIndexesResult>();
      const batch = patterns.map(async (p) => {
        try {
          const res = await services.analyticsService.explainIndexes(p.sample_query);
          if (!cancelled) results.set(p.query_hash, res);
        } catch (err) {
          console.warn('[OrderingKeyTable] EXPLAIN indexes failed for pattern:', p.query_hash, err);
        }
      });
      await Promise.all(batch);
      if (!cancelled) setExplainResults(new Map(results));
    })();

    return () => { cancelled = true; };
  }, [services, patterns]);

  // Compute diagnostics using EXPLAIN keys when available, heuristic otherwise
  const diagnostics = useMemo(() => {
    const map = new Map<string, OrderingKeyDiagnostic>();
    for (const p of patterns) {
      const explain = explainResults.get(p.query_hash);
      const explainKeys = explain?.success && explain.primaryKey
        ? explain.primaryKey.keys
        : undefined;
      map.set(p.query_hash, diagnoseOrderingKeyUsage(sortingKey, p.sample_query, p.avg_pruning_pct, explainKeys));
    }
    return map;
  }, [patterns, sortingKey, explainResults]);

  const sorted = useMemo(() => {
    return [...patterns].sort((a, b) => {
      const aVal = a[sortField] ?? -1;
      const bVal = b[sortField] ?? -1;
      const cmp = aVal < bVal ? -1 : aVal > bVal ? 1 : 0;
      return sortDir === 'desc' ? -cmp : cmp;
    });
  }, [patterns, sortField, sortDir]);

  const handleSort = (field: typeof sortField) => {
    if (sortField === field) setSortDir(d => d === 'asc' ? 'desc' : 'asc');
    else { setSortField(field); setSortDir('desc'); }
  };

  const si = (f: typeof sortField) => sortField !== f ? '' : sortDir === 'asc' ? ' ↑' : ' ↓';

  const hdr: React.CSSProperties = {
    padding: '6px 10px', fontSize: 9, fontWeight: 600,
    textTransform: 'uppercase', letterSpacing: '0.5px',
    color: 'var(--text-muted)', whiteSpace: 'nowrap',
    borderBottom: '1px solid var(--border-primary)',
  };
  const sortableHdr: React.CSSProperties = { ...hdr, cursor: 'pointer', userSelect: 'none' };
  const cell: React.CSSProperties = {
    padding: '8px 10px', fontSize: 11,
    borderBottom: '1px solid var(--border-secondary, var(--border-primary))',
  };
  const mono: React.CSSProperties = { fontFamily: "'Share Tech Mono', monospace" };

  return (
    <>
    <table style={{ width: '100%', borderCollapse: 'collapse' }}>
      <thead>
        <tr>
          <th style={{ ...hdr, textAlign: 'left' }}>Sample Query</th>
          <th style={{ ...sortableHdr, textAlign: 'right', width: 80 }} onClick={() => handleSort('execution_count')}>Executions{si('execution_count')}</th>
          <th style={{ ...hdr, textAlign: 'left', width: 120 }}>Users</th>
          <th style={{ ...sortableHdr, textAlign: 'left', width: 100 }} onClick={() => handleSort('avg_pruning_pct')}>Pruning{si('avg_pruning_pct')}</th>
          <th style={{ ...hdr, textAlign: 'left', width: 140 }}>Diagnosis</th>
          <th style={{ ...sortableHdr, textAlign: 'right', width: 80 }} onClick={() => handleSort('avg_duration_ms')}>p50{si('avg_duration_ms')}</th>
          <th style={{ ...sortableHdr, textAlign: 'right', width: 80 }} onClick={() => handleSort('p95_duration_ms')}>p95{si('p95_duration_ms')}</th>
          <th style={{ ...sortableHdr, textAlign: 'right', width: 80 }} onClick={() => handleSort('p99_duration_ms')}>p99{si('p99_duration_ms')}</th>
          <th style={{ ...sortableHdr, textAlign: 'right', width: 90 }} onClick={() => handleSort('avg_memory_bytes')}>Avg Memory{si('avg_memory_bytes')}</th>
          <th style={{ ...sortableHdr, textAlign: 'right', width: 90 }} onClick={() => handleSort('avg_rows_read')}>Avg Rows Read{si('avg_rows_read')}</th>
        </tr>
      </thead>
      <tbody>
        {sorted.map((p) => {
          const isExpanded = expandedHash === p.query_hash;
          const diag = diagnostics.get(p.query_hash)!;
          return (
            <React.Fragment key={p.query_hash}>
              <tr
                onClick={() => setExpandedHash(isExpanded ? null : p.query_hash)}
                style={{ cursor: 'pointer', transition: 'background 0.1s' }}
                onMouseEnter={e => { e.currentTarget.style.background = 'var(--bg-card-hover, rgba(255,255,255,0.02))'; }}
                onMouseLeave={e => { e.currentTarget.style.background = 'transparent'; }}
              >
                <td style={{
                  ...cell, fontSize: 10, color: 'var(--text-secondary)',
                  maxWidth: 350, overflow: 'hidden', whiteSpace: 'nowrap',
                  ...mono,
                }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                    <span style={{ overflow: 'hidden', textOverflow: 'ellipsis' }}>
                      {p.sample_query.slice(0, 100)}{p.sample_query.length > 100 ? '…' : ''}
                    </span>
                    <button
                      onClick={(e) => handleViewLatestQuery(p.query_hash, p.sample_query, e)}
                      disabled={loadingDetailHash === p.query_hash}
                      style={{
                        flexShrink: 0,
                        background: 'rgba(88,166,255,0.12)',
                        border: '1px solid rgba(88,166,255,0.3)',
                        borderRadius: 3,
                        padding: '2px 6px',
                        fontSize: 9,
                        fontWeight: 600,
                        color: '#58a6ff',
                        cursor: loadingDetailHash === p.query_hash ? 'wait' : 'pointer',
                        opacity: loadingDetailHash === p.query_hash ? 0.6 : 1,
                        fontFamily: 'inherit',
                        transition: 'all 0.15s',
                        lineHeight: 1.2,
                      }}
                      onMouseEnter={e => { e.stopPropagation(); e.currentTarget.style.background = 'rgba(88,166,255,0.25)'; }}
                      onMouseLeave={e => { e.currentTarget.style.background = 'rgba(88,166,255,0.12)'; }}
                      title="Open query detail for the latest execution of this pattern"
                    >
                      {loadingDetailHash === p.query_hash ? '…' : '↗'}
                    </button>
                  </div>
                </td>
                <td style={{ ...cell, ...mono, textAlign: 'right' }}>{formatNumber(p.execution_count)}</td>
                <td style={cell}><UserBreakdownBar breakdown={p.user_breakdown} total={p.execution_count} /></td>
                <td style={cell}><PruningBadge pct={p.avg_pruning_pct} /></td>
                <td style={cell}><DiagBadge diag={diag} /></td>
                <td style={{ ...cell, ...mono, textAlign: 'right' }}>
                  <span title={`p50: ${formatDuration(p.p50_duration_ms)} (median)`}>{formatDuration(p.p50_duration_ms)}</span>
                </td>
                {(() => {
                  const r95 = p.p50_duration_ms > 0 ? p.p95_duration_ms / p.p50_duration_ms : 0;
                  const color95 = r95 > 3 ? '#d29922' : 'inherit';
                  const tip95 = `p95: ${formatDuration(p.p95_duration_ms)} (${r95.toFixed(1)}× p50)${r95 > 3 ? ' — high tail variance' : ''}`;
                  return <td style={{ ...cell, ...mono, textAlign: 'right', color: color95 }} title={tip95}>{formatDuration(p.p95_duration_ms)}</td>;
                })()}
                {(() => {
                  const r99 = p.p50_duration_ms > 0 ? p.p99_duration_ms / p.p50_duration_ms : 0;
                  const color99 = r99 > 10 ? '#f85149' : r99 > 5 ? '#d29922' : 'inherit';
                  const tip99 = `p99: ${formatDuration(p.p99_duration_ms)} (${r99.toFixed(1)}× p50)${r99 > 10 ? ' — extreme tail, check for resource contention' : r99 > 5 ? ' — heavy tail' : ''}`;
                  return <td style={{ ...cell, ...mono, textAlign: 'right', color: color99 }} title={tip99}>{formatDuration(p.p99_duration_ms)}</td>;
                })()}
                <td style={{ ...cell, ...mono, textAlign: 'right' }}>{formatBytes(p.avg_memory_bytes)}</td>
                <td style={{ ...cell, ...mono, textAlign: 'right' }}>{formatNumber(p.avg_rows_read)}</td>
              </tr>
              {isExpanded && (
                <tr>
                  <td colSpan={10} style={{ padding: '10px 16px', background: 'var(--bg-primary, rgba(0,0,0,0.1))' }}>
                    {/* SQL */}
                    <SqlHighlight style={{
                      fontSize: 11,
                      padding: 10,
                      borderRadius: 4,
                      overflow: 'auto',
                      maxHeight: 180,
                      border: '1px solid var(--border-primary)',
                      marginBottom: 10,
                      background: 'var(--bg-card, rgba(0,0,0,0.2))',
                    }}>
                      {p.sample_query}
                    </SqlHighlight>

                    {/* Two-column layout: Diagnosis + EXPLAIN */}
                    <div style={{ display: 'flex', gap: 12, marginBottom: 8 }}>
                      {/* Diagnosis */}
                      <div style={{ flex: 1, padding: '8px 12px', borderRadius: 4, background: 'var(--bg-card, rgba(255,255,255,0.03))', border: '1px solid var(--border-primary)' }}>
                        <div style={{ fontSize: 9, fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.5px', marginBottom: 6 }}>
                          Diagnosis
                        </div>
                        <div style={{ fontSize: 11, color: DIAG_COLORS[diag.severity], fontWeight: 600, marginBottom: 4, display: 'flex', alignItems: 'center', gap: 6 }}>
                          {diag.label}
                          {diag.indexAlgorithm !== 'none' && (
                            <span style={{ fontSize: 9, fontWeight: 500, color: 'var(--text-muted)', ...mono }}>
                              ({diag.indexAlgorithm === 'binary_search' ? 'binary search' : 'generic exclusion'})
                            </span>
                          )}
                        </div>
                        <div style={{ fontSize: 10, color: 'var(--text-secondary)', lineHeight: 1.5, marginBottom: 6 }}>
                          {diag.reason}
                        </div>
                        <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', fontSize: 10, color: 'var(--text-muted)' }}>
                          {diag.orderByColumns.length > 0 && (
                            <span>ORDER BY: <span style={mono}>{diag.orderByColumns.map((col, i) => (
                              <span key={col}>
                                {i > 0 && ', '}
                                <span style={{ color: diag.matchedColumns.includes(col) ? '#3fb950' : 'var(--text-muted)', textDecoration: i < diag.prefixLength ? 'underline' : 'none' }}>{col}</span>
                              </span>
                            ))}</span></span>
                          )}
                          {diag.whereColumns.length > 0 && (
                            <span>WHERE: <span style={mono}>{diag.whereColumns.join(', ')}</span></span>
                          )}
                          {diag.prefixLength > 0 && diag.prefixLength < diag.orderByColumns.length && (
                            <span>Prefix: <span style={mono}>{diag.prefixLength}/{diag.orderByColumns.length}</span></span>
                          )}
                        </div>
                      </div>

                      {/* EXPLAIN indexes */}
                      {(() => {
                        const explain = explainResults.get(p.query_hash);
                        if (!explain) return null;
                        if (!explain.success) {
                          return (
                            <div style={{ flex: 1, padding: '8px 12px', borderRadius: 4, background: 'var(--bg-card, rgba(255,255,255,0.03))', border: '1px solid var(--border-primary)' }}>
                              <div style={{ fontSize: 9, fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.5px', marginBottom: 6 }}>
                                Index Usage
                              </div>
                              <div style={{ fontSize: 10, color: 'var(--text-muted)' }}>Failed: {explain.error}</div>
                            </div>
                          );
                        }
                        if (explain.indexes.length === 0) return null;
                        // Check if this is a no-filter scan (Condition: true, all granules selected)
                        const isNoFilter = explain.primaryKey?.condition === 'true'
                          || explain.indexes.every(idx => idx.granules && idx.granules.selected === idx.granules.total);
                        return (
                          <div style={{ flex: 1, padding: '8px 12px', borderRadius: 4, background: 'var(--bg-card, rgba(255,255,255,0.03))', border: '1px solid var(--border-primary)' }}>
                            <div style={{ fontSize: 9, fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.5px', marginBottom: 6 }}>
                              Index Usage
                            </div>
                            {isNoFilter && (
                              <div style={{ fontSize: 10, color: 'var(--text-muted)', marginBottom: 6 }}>
                                No index filtering — full scan across all granules.
                              </div>
                            )}
                            {explain.indexes.map((idx, i) => {
                              const pruned = idx.granules && idx.granules.total > 0 && idx.granules.selected < idx.granules.total;
                              const hasDetail = idx.keys.length > 0 || idx.granules || idx.parts;
                              return (
                                <div key={i} style={{ display: 'flex', gap: 10, alignItems: 'baseline', marginBottom: 3, fontSize: 10 }}>
                                  <span style={{ ...mono, color: 'var(--text-secondary)', fontWeight: 600, minWidth: 70 }}>
                                    {idx.type}{idx.name ? ` (${idx.name})` : ''}
                                  </span>
                                  {!hasDetail && (
                                    <span style={{ color: 'var(--text-muted)', fontStyle: 'italic' }}>not specified</span>
                                  )}
                                  {idx.keys.length > 0 && (
                                    <span style={{ color: 'var(--text-muted)' }}>
                                      <span style={mono}>{idx.keys.join(', ')}</span>
                                    </span>
                                  )}
                                  {idx.granules && (
                                    <span style={{ ...mono, color: pruned ? '#3fb950' : 'var(--text-muted)' }}>
                                      {idx.granules.selected}/{idx.granules.total} granules
                                      {pruned && ` (${((1 - idx.granules.selected / idx.granules.total) * 100).toFixed(0)}% pruned)`}
                                    </span>
                                  )}
                                  {idx.parts && (
                                    <span style={{ ...mono, color: 'var(--text-muted)' }}>
                                      {idx.parts.selected}/{idx.parts.total} parts
                                    </span>
                                  )}
                                </div>
                              );
                            })}
                            {explain.primaryKey?.condition && explain.primaryKey.condition !== 'true' && (
                              <div style={{ marginTop: 3, fontSize: 10, color: 'var(--text-muted)' }}>
                                Condition: <span style={mono}>{explain.primaryKey.condition}</span>
                              </div>
                            )}
                          </div>
                        );
                      })()}
                    </div>
                  </td>
                </tr>
              )}
            </React.Fragment>
          );
        })}
      </tbody>
    </table>
    <QueryDetailModal query={detailQuery} onClose={() => setDetailQuery(null)} />
    </>
  );
};