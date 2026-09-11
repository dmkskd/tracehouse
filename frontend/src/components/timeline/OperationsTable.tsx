/**
 * OperationsTable — one full-width table for every Time Travel operation:
 * queries, merges and mutations together, filtered by kind, scoped to the
 * window or to the pinned instant, and sorted by the active resource metric.
 *
 * Row shaping lives in @tracehouse/core (buildOperationRows); this file renders.
 */
import React from 'react';
import type {
  OperationCounts,
  OperationKindFilter,
  OperationRow,
  OperationScope,
  OperationSortDir,
  OperationSortField,
} from '@tracehouse/core';
import { describeOperationContext, getMergeCategoryInfo, maxOperationMetric, operationRowHighlightKey, type MergeCategory } from '@tracehouse/core';
import { formatBytes, formatDurationMs as fmtMs } from '../../utils/formatters';
import { TruncatedHost } from '../common/TruncatedHost';
import {
  type MetricMode, type HighlightedItem,
  METRIC_CONFIG, Q_COLORS, M_COLORS, MUT_COLORS, QUERY_KIND_COLORS,
  operationBandColor,
} from './timeline-constants';

type SortField = OperationSortField;
type SortDir = OperationSortDir;

/** Rows live in their own scroll area so the chart stays put on long lists. */
const BODY_MAX_HEIGHT = 420;

/** Highlight color for hash-matched queries */
const HASH_MATCH_COLOR = '#58a6ff';

const KIND_ACCENT: Record<OperationRow['kind'], string> = {
  query: Q_COLORS[0],
  merge: M_COLORS[0],
  mutation: MUT_COLORS[0],
};

const KindBadge: React.FC<{ row: OperationRow }> = ({ row }) => {
  const color = row.kind === 'query'
    ? QUERY_KIND_COLORS[row.kindLabel] ?? '#94a3b8'
    : KIND_ACCENT[row.kind];
  return (
    <span style={{
      display: 'inline-block', padding: '1px 6px', fontSize: 9, fontWeight: 600,
      borderRadius: 4, background: `${color}22`, color, lineHeight: '16px',
      letterSpacing: '0.3px', whiteSpace: 'nowrap',
    }}>
      {row.kindLabel}
    </span>
  );
};

const MergeReasonBadge: React.FC<{ reason?: string }> = ({ reason }) => {
  const info = getMergeCategoryInfo((reason || 'Regular') as MergeCategory);
  if (!info) return null;
  return (
    <span style={{
      display: 'inline-block', padding: '1px 6px', fontSize: 9, fontWeight: 500,
      borderRadius: 4, background: `${info.color}22`, color: info.color, lineHeight: '16px',
      whiteSpace: 'nowrap',
    }}>
      {info.label}
    </span>
  );
};

/**
 * The one column whose meaning follows the row: a query's user, or a merge's
 * reason and how many bytes it read and wrote.
 */
const ContextCell: React.FC<{ row: OperationRow }> = ({ row }) => {
  const context = describeOperationContext(row);
  if (context.type === 'none') return <span style={{ color: 'var(--text-muted)' }}>—</span>;
  if (context.type === 'user') {
    return <span style={{ color: 'var(--text-secondary)' }}>{context.user}</span>;
  }
  const hasBytes = context.readBytes > 0 || context.writtenBytes > 0;
  return (
    <span style={{ display: 'flex', alignItems: 'center', gap: 6, overflow: 'hidden', whiteSpace: 'nowrap' }}>
      {context.reason && <MergeReasonBadge reason={context.reason} />}
      {hasBytes && (
        <span style={{ color: 'var(--text-muted)', fontFamily: 'monospace', fontSize: 10 }}>
          {formatBytes(context.readBytes)} → {formatBytes(context.writtenBytes)}
        </span>
      )}
      {context.progress !== undefined && (
        <span style={{ color: 'var(--text-secondary)', fontSize: 10 }}>
          {Math.round(context.progress * 100)}%
        </span>
      )}
    </span>
  );
};

const FILTERS: { key: OperationKindFilter; label: string }[] = [
  { key: 'all', label: 'All' },
  { key: 'query', label: 'Queries' },
  { key: 'merge', label: 'Merges' },
  { key: 'mutation', label: 'Mutations' },
];

const CHART_TOGGLES: { key: 'query' | 'merge' | 'mutation'; label: string; color: string }[] = [
  { key: 'query', label: 'Queries', color: Q_COLORS[0] },
  { key: 'merge', label: 'Merges', color: M_COLORS[0] },
  { key: 'mutation', label: 'Mutations', color: MUT_COLORS[0] },
];

export const OperationsTable: React.FC<{
  /** Rows already scoped, sorted and filtered by kind. */
  rows: OperationRow[];
  /** Counts per kind before the kind filter, so tab counts share one scope. */
  counts: OperationCounts;
  /** Total matching operations server-side, before the top-N activity limit. */
  totalCount: number;
  /** How many operations were loaded (the top-N cap). */
  loadedCount: number;
  kindFilter: OperationKindFilter;
  onKindFilterChange: (kind: OperationKindFilter) => void;
  scope: OperationScope;
  onScopeChange: (scope: OperationScope) => void;
  pinnedMs: number | null;
  metricMode: MetricMode;
  sortField: SortField;
  sortDir: SortDir;
  onSort: (field: SortField) => void;
  highlightedItem: HighlightedItem;
  onHighlightItem: (item: HighlightedItem) => void;
  onSelect: (row: OperationRow) => void;
  /** Free-text filter, already applied to `rows` and `counts` upstream. */
  search: string;
  onSearchChange: (value: string) => void;
  /** Currently selected operation, shown in the inspector. */
  selectedId?: string | null;
  showHost?: boolean;
  hiddenCategories: Set<'query' | 'merge' | 'mutation'>;
  onToggleChartVisibility: (kind: 'query' | 'merge' | 'mutation') => void;
  queryHashActive?: boolean;
}> = ({
  rows, counts, totalCount, loadedCount, kindFilter, onKindFilterChange, scope, onScopeChange,
  pinnedMs, metricMode, sortField, sortDir, onSort, highlightedItem, onHighlightItem, onSelect,
  search, onSearchChange, selectedId, showHost, hiddenCategories, onToggleChartVisibility, queryHashActive,
}) => {
  const metric = METRIC_CONFIG[metricMode];
  // Bars are scaled to the heaviest row in view, so they stay readable when the
  // table is sorted by something other than the metric.
  const metricMax = maxOperationMetric(rows);
  const sortIndicator = (field: SortField) => sortField === field ? (sortDir === 'desc' ? '▼' : '▲') : '⇅';
  // Second resource column: memory unless memory is already the sorted metric.
  const secondary = metricMode === 'memory'
    ? { label: 'CPU time', get: (r: OperationRow) => METRIC_CONFIG.cpu.fmtVal(r.cpuUs) }
    : { label: 'Peak memory', get: (r: OperationRow) => METRIC_CONFIG.memory.fmtVal(r.peakMemory) };

  const th = (extra: React.CSSProperties = {}): React.CSSProperties => ({
    padding: '6px 8px', textAlign: 'left', color: 'var(--text-muted)',
    fontWeight: 500, fontSize: 10,
    position: 'sticky', top: 0, zIndex: 1,
    background: 'var(--bg-secondary)',
    boxShadow: 'inset 0 -1px 0 var(--border-primary)',
    ...extra,
  });
  const sortableTh = (field: SortField, width: number): React.CSSProperties => ({
    ...th({ textAlign: 'right', width }),
    color: sortField === field ? metric.color : 'var(--text-muted)',
    cursor: 'pointer', userSelect: 'none',
  });

  return (
    <div style={{ borderRadius:10, background:'var(--bg-secondary)', border:'1px solid var(--border-primary)', overflow:'hidden' }}>
      {/* Header: title, kind filters, scope */}
      <div style={{ display:'flex', alignItems:'center', gap:12, padding:'10px 16px', borderBottom:'1px solid var(--border-primary)', flexWrap:'wrap' }}>
        <span style={{ color:'var(--text-primary)', fontSize:13, fontWeight:600 }}>Operations</span>
        <div role="group" aria-label="Operation kind" style={{ display:'inline-flex', gap:2, padding:2, borderRadius:7, background:'var(--bg-tertiary)', border:'1px solid var(--border-primary)' }}>
          {FILTERS.map(f => {
            const selected = kindFilter === f.key;
            return (
              <button key={f.key} type="button" aria-pressed={selected}
                onClick={() => onKindFilterChange(f.key)}
                disabled={f.key !== 'all' && counts[f.key] === 0}
                style={{
                  display:'flex', alignItems:'center', gap:6, border:'none', borderRadius:5,
                  padding:'4px 10px', fontSize:11, fontWeight:600,
                  cursor: f.key !== 'all' && counts[f.key] === 0 ? 'default' : 'pointer',
                  opacity: f.key !== 'all' && counts[f.key] === 0 ? 0.45 : 1,
                  background: selected ? 'var(--bg-primary)' : 'transparent',
                  color: selected ? 'var(--text-primary)' : 'var(--text-muted)',
                  boxShadow: selected ? '0 1px 3px rgba(0,0,0,0.15)' : 'none',
                  transition:'all 0.15s ease',
                }}>
                {f.label}
                <span style={{ fontFamily:"'Share Tech Mono',monospace", fontSize:10, color: selected ? metric.color : 'var(--text-muted)' }}>
                  {counts[f.key].toLocaleString()}
                </span>
              </button>
            );
          })}
        </div>

        <span style={{ color:'var(--text-muted)', fontSize:11 }}>
          Showing top {loadedCount.toLocaleString()} by {metric.label.toLowerCase()} of {totalCount.toLocaleString()}
          {pinnedMs !== null && ` · ${counts.all.toLocaleString()} active at pin`}
        </span>

        <div style={{ marginLeft:'auto', display:'flex', alignItems:'center', gap:10 }}>
          {/* Chart visibility per kind — kept here now that the three tables are one */}
          <div style={{ display:'flex', alignItems:'center', gap:6 }}>
            <span style={{ fontSize:10, color:'var(--text-muted)' }}>Chart</span>
            {CHART_TOGGLES.map(t => {
              const hidden = hiddenCategories.has(t.key);
              return (
                <button key={t.key} type="button" aria-pressed={!hidden}
                  onClick={() => onToggleChartVisibility(t.key)}
                  title={hidden ? `Show ${t.label.toLowerCase()} in chart` : `Hide ${t.label.toLowerCase()} from chart`}
                  style={{
                    display:'flex', alignItems:'center', gap:4, border:'none', background:'transparent',
                    padding:'2px 4px', fontSize:10, cursor:'pointer',
                    color: hidden ? 'var(--text-muted)' : 'var(--text-secondary)',
                    textDecoration: hidden ? 'line-through' : 'none',
                    opacity: hidden ? 0.55 : 1,
                  }}>
                  <span style={{ width:8, height:8, borderRadius:2, background:t.color, opacity: hidden ? 0.3 : 1 }} />
                  {t.label}
                </button>
              );
            })}
          </div>

          <div style={{ position:'relative', display:'flex', alignItems:'center' }}>
            <input
              type="search"
              value={search}
              onChange={e => onSearchChange(e.target.value)}
              placeholder="Filter by query, table, user, host"
              aria-label="Filter operations"
              style={{
                width: 220, padding:'5px 10px', fontSize:11,
                borderRadius:6, border:'1px solid var(--border-primary)',
                background:'var(--bg-tertiary)', color:'var(--text-primary)', outline:'none',
              }}
            />
          </div>

          <div className="tracehouse-compact-select-control" style={{ display:'flex', alignItems:'center', background:'var(--bg-tertiary)', borderRadius:6, border:'1px solid var(--border-primary)', overflow:'hidden' }}>
            <select className="tracehouse-compact-native-select"
              aria-label="Operation scope"
              value={scope}
              onChange={e => onScopeChange(e.target.value as OperationScope)}
              title={pinnedMs === null ? 'Pin a time on the chart to scope to that instant' : 'Which operations to list'}
              style={{ background:'transparent', color:'var(--text-primary)', border:'none', padding:'5px 10px', fontSize:11, outline:'none', cursor:'pointer' }}>
              <option value="window">In window</option>
              <option value="pin" disabled={pinnedMs === null}>Active at pin</option>
            </select>
          </div>
        </div>
      </div>

      {rows.length > 0 ? (
        <div style={{ maxHeight: BODY_MAX_HEIGHT, overflowY:'auto', overflowX:'hidden' }}>
        <table style={{ width:'100%', borderCollapse:'separate', borderSpacing:0, fontSize:11, tableLayout:'fixed' }}>
          <thead>
            <tr>
              <th style={th({ paddingLeft:12, width:18 })}></th>
              <th onClick={() => onSort('kind')} style={{ ...th({ width:86 }), cursor:'pointer', userSelect:'none', color: sortField === 'kind' ? metric.color : 'var(--text-muted)' }}>
                Kind {sortIndicator('kind')}
              </th>
              <th style={th()}>Operation</th>
              <th onClick={() => onSort('user')} title="User for queries, merge reason and bytes read → written for merges. Sorts by user."
                style={{ ...th({ width:220 }), cursor:'pointer', userSelect:'none', color: sortField === 'user' ? metric.color : 'var(--text-muted)' }}>
                Context {sortIndicator('user')}
              </th>
              {showHost && (
                <th onClick={() => onSort('server')} style={{ ...th({ width:100 }), cursor:'pointer', userSelect:'none', color: sortField === 'server' ? metric.color : 'var(--text-muted)' }}>
                  Server {sortIndicator('server')}
                </th>
              )}
              <th style={th({ textAlign:'center', width:30 })}></th>
              <th onClick={() => onSort('started')} style={sortableTh('started', 80)}>Started {sortIndicator('started')}</th>
              <th onClick={() => onSort('duration')} style={sortableTh('duration', 80)}>Duration {sortIndicator('duration')}</th>
              <th onClick={() => onSort('metric')} style={sortableTh('metric', 150)}
                title={`${metric.label} over each operation's lifetime. Bars are relative to the largest row shown, not to server capacity.`}>
                {metric.label} {sortIndicator('metric')}
              </th>
              <th style={th({ textAlign:'right', width:100, paddingRight:12 })}>{secondary.label}</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row, i) => {
              const id = operationRowHighlightKey(row);
              const isHighlighted = highlightedItem?.type === row.kind && highlightedItem.id === id;
              const isSelected = selectedId === id;
              const isDimmed = queryHashActive === true && !row.matchedHash;
              return (
                <tr key={`${row.kind}-${id}-${i}`}
                  onClick={() => onSelect(row)}
                  onMouseEnter={() => onHighlightItem({ type: row.kind, idx: row.idx, id })}
                  onMouseLeave={() => onHighlightItem(null)}
                  style={{
                    background: isHighlighted || isSelected
                      ? `color-mix(in srgb, ${KIND_ACCENT[row.kind]}, transparent 72%)`
                      : (i % 2 === 0 ? 'transparent' : 'var(--bg-tertiary)'),
                    boxShadow: isSelected ? `inset 2px 0 0 ${KIND_ACCENT[row.kind]}` : undefined,
                    cursor:'pointer', transition:'background 0.15s ease, opacity 0.15s ease',
                    opacity: isDimmed ? 0.35 : 1,
                  }}>
                  <td style={{ padding:'5px 4px 5px 12px', width:18 }}>
                    <div style={{
                      width:8, height:8, borderRadius:2,
                      background: queryHashActive && row.matchedHash ? HASH_MATCH_COLOR : operationBandColor(row.kind, row.idx),
                      boxShadow: queryHashActive && row.matchedHash ? `0 0 4px ${HASH_MATCH_COLOR}` : undefined,
                    }} />
                  </td>
                  <td style={{ padding:'5px 8px' }}><KindBadge row={row} /></td>
                  <td style={{ padding:'5px 8px', overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }} title={`${row.label}\n${row.id}`}>
                    {/* Queries lead with their id, parts with their table: in both
                        cases the identifier you would search for comes first.
                        The merge reason lives in Context, not here. */}
                    {row.kind === 'query' ? (
                      <>
                        <span style={{ fontFamily:'monospace', color:'#58a6ff', marginRight:8 }}>{row.id.slice(0, 8)}</span>
                        <span style={{ color:'var(--text-secondary)' }}>{row.label}</span>
                      </>
                    ) : (
                      <>
                        <span style={{ color:'var(--text-secondary)', marginRight:8 }}>{row.label}</span>
                        <span style={{ fontFamily:'monospace', color:'var(--text-muted)', fontSize:10 }}>{row.id}</span>
                      </>
                    )}
                  </td>
                  <td style={{ padding:'5px 8px', overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>
                    <ContextCell row={row} />
                  </td>
                  {showHost && (
                    <td style={{ padding:'5px 8px', color:'var(--text-muted)', overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap', fontSize:10 }} title={row.hostname}>
                      {row.hostname ? <TruncatedHost name={row.hostname} maxLen={12} /> : '—'}
                    </td>
                  )}
                  <td style={{ padding:'5px 8px', textAlign:'center' }}
                      title={row.isRunning ? 'Running' : row.failed ? 'Failed' : 'Completed'}>
                    {row.isRunning ? (
                      <span style={{ display:'inline-block', width:8, height:8, borderRadius:'50%', background: KIND_ACCENT[row.kind], animation:'pulse 1.5s ease-in-out infinite' }} />
                    ) : row.failed ? (
                      <span style={{ color:'var(--color-error)', fontSize:10 }}>✕</span>
                    ) : (
                      <span style={{ color:'var(--color-success)', fontSize:10 }}>✓</span>
                    )}
                  </td>
                  <td style={{ padding:'5px 8px', textAlign:'right', fontFamily:'monospace', color:'var(--text-muted)', fontSize:10 }}>
                    {new Date(row.startMs).toLocaleTimeString()}
                  </td>
                  <td style={{ padding:'5px 8px', textAlign:'right', color:'var(--text-muted)' }}>{fmtMs(row.durationMs)}</td>
                  <td style={{ padding:'5px 8px' }}
                    title={metricMax > 0
                      ? `${metric.fmtVal(row.metricValue)} of ${metric.label.toLowerCase()} over this operation's lifetime\n`
                        + `Bar: ${Math.round((row.metricValue / metricMax) * 100)}% of the largest row shown (${metric.fmtVal(metricMax)})`
                      : undefined}>
                    <span style={{ display:'flex', alignItems:'center', gap:6, justifyContent:'flex-end' }}>
                      <span style={{ flex:1, height:6, borderRadius:3, background:'var(--bg-tertiary)', overflow:'hidden', minWidth:24 }}>
                        <span style={{
                          display:'block', height:'100%',
                          width: `${metricMax > 0 ? Math.max(2, (row.metricValue / metricMax) * 100) : 0}%`,
                          background: operationBandColor(row.kind, row.idx), opacity: 0.75,
                        }} />
                      </span>
                      <span style={{ fontFamily:'monospace', color:'var(--text-primary)', fontWeight:500, whiteSpace:'nowrap' }}>
                        {row.metricEstimated ? '~' : ''}{metric.fmtVal(row.metricValue)}
                      </span>
                    </span>
                  </td>
                  <td style={{ padding:'5px 12px 5px 8px', textAlign:'right', fontFamily:'monospace', color:'var(--text-muted)' }}>
                    {secondary.get(row)}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
        </div>
      ) : (
        <div style={{ padding:'24px', textAlign:'center', color:'var(--text-muted)', fontSize:12 }}>
          {scope === 'pin' && pinnedMs !== null
            ? 'No operations active at the pinned time.'
            : 'No operations in this window.'}
        </div>
      )}
    </div>
  );
};
