/**
 * TimelineTable — Shared sortable table for queries, merges, and mutations
 * in the Time Travel page. Deduplicates the three nearly-identical table sections.
 */
import React from 'react';
import type { QuerySeries, MergeSeries, MutationSeries } from '@tracehouse/core';
import { getMergeCategoryInfo, type MergeCategory } from '@tracehouse/core';
import { parseTimestamp } from '../../utils/formatters';
import { type MetricMode, type HighlightedItem, METRIC_CONFIG, metricForItem } from './timeline-constants';
import { TruncatedHost } from '../common/TruncatedHost';

const fmtMs = (ms: number): string =>
  ms < 1000 ? `${ms}ms` : ms < 60000 ? `${(ms/1000).toFixed(1)}s` : `${(ms/60000).toFixed(1)}m`;

const getQueryKindColor = (kind: string): string => {
  switch (kind.toLowerCase()) {
    case 'select': return '#3b82f6';
    case 'insert': return '#f59e0b';
    case 'alter': return '#ef4444';
    case 'create': return '#22c55e';
    case 'drop': return '#f43f5e';
    case 'system': return '#8b5cf6';
    case 'optimize': return '#06b6d4';
    default: return '#94a3b8';
  }
};

const QueryKindBadge: React.FC<{ kind?: string }> = ({ kind }) => {
  if (!kind) return null;
  const color = getQueryKindColor(kind);
  return (
    <span style={{
      display: 'inline-block', padding: '1px 6px', fontSize: 9, fontWeight: 500,
      borderRadius: 4, background: `${color}22`, color, lineHeight: '16px',
    }}>
      {kind.charAt(0).toUpperCase() + kind.slice(1).toLowerCase()}
    </span>
  );
};

const MergeReasonBadge: React.FC<{ reason?: string }> = ({ reason }) => {
  const category = (reason || 'Regular') as MergeCategory;
  const info = getMergeCategoryInfo(category);
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

type SortField = 'metric' | 'duration' | 'started';
type SortDir = 'asc' | 'desc';

// ── Query table ────────────────────────────────────────────────────────────

export const QueryTable: React.FC<{
  queries: QuerySeries[];
  allQueries: QuerySeries[];
  totalCount: number;
  pinnedMs: number | null;
  metricMode: MetricMode;
  colors: string[];
  accentColor: string;
  highlightedItem: HighlightedItem;
  onHighlightItem: (item: HighlightedItem) => void;
  onSelect: (q: QuerySeries) => void;
  sortField: SortField;
  sortDir: SortDir;
  onSort: (field: SortField) => void;
  showHost?: boolean;
  isHiddenInChart?: boolean;
  onToggleChartVisibility?: () => void;
}> = ({ queries, allQueries, totalCount, pinnedMs, metricMode, colors, accentColor, highlightedItem, onHighlightItem, onSelect, sortField, sortDir, onSort, showHost, isHiddenInChart, onToggleChartVisibility }) => {
  const sortIndicator = (field: SortField) => sortField === field ? (sortDir === 'desc' ? '▼' : '▲') : '⇅';
  return (
    <div style={{ borderRadius:10, background:'var(--bg-secondary)', border:'1px solid var(--border-primary)', overflow:'hidden' }}>
      <div
        style={{ display:'flex', alignItems:'center', gap:8, padding:'12px 16px', borderBottom:'1px solid var(--border-primary)', cursor: onToggleChartVisibility ? 'pointer' : undefined, userSelect:'none' }}
        onClick={onToggleChartVisibility}
        title={isHiddenInChart ? 'Click to show queries in chart' : 'Click to hide queries from chart'}
      >
        <div style={{ width:3, height:16, borderRadius:2, background: accentColor, opacity: isHiddenInChart ? 0.3 : 1, transition:'opacity 0.15s ease' }} />
        <span style={{ color: isHiddenInChart ? 'var(--text-muted)' : 'var(--text-primary)', fontSize:13, fontWeight:600, transition:'color 0.15s ease', textDecoration: isHiddenInChart ? 'line-through' : 'none' }}>Queries</span>
        <span style={{ color:'var(--text-muted)', fontSize:11 }}>
          {queries.length}{pinnedMs !== null ? ' at pin' : ''} / {totalCount} total
          {totalCount > allQueries.length && ` (showing top ${allQueries.length} by ${METRIC_CONFIG[metricMode].label.toLowerCase()})`}
        </span>
        {isHiddenInChart && <span style={{ fontSize:9, color:'var(--text-muted)', opacity:0.6 }}>hidden</span>}
      </div>
      {queries.length > 0 ? (
      <table style={{ width:'100%', borderCollapse:'collapse', fontSize:11, tableLayout:'fixed' }}>
        <thead>
          <tr style={{ borderBottom:'1px solid var(--border-primary)' }}>
            <th style={{ padding:'6px 12px', textAlign:'left', color:'var(--text-muted)', fontWeight:500, fontSize:10, width:18 }}></th>
            <th style={{ padding:'6px 8px', textAlign:'left', color:'var(--text-muted)', fontWeight:500, fontSize:10 }}>ID</th>
            <th style={{ padding:'6px 8px', textAlign:'left', color:'var(--text-muted)', fontWeight:500, fontSize:10, width:54 }}>Type</th>
            <th style={{ padding:'6px 8px', textAlign:'left', color:'var(--text-muted)', fontWeight:500, fontSize:10 }}>User</th>
            {showHost && <th style={{ padding:'6px 8px', textAlign:'left', color:'var(--text-muted)', fontWeight:500, fontSize:10, width:80 }}>Server</th>}
            <th style={{ padding:'6px 8px', textAlign:'center', color:'var(--text-muted)', fontWeight:500, fontSize:10, width:24 }}></th>
            <th onClick={() => onSort('started')} style={{ padding:'6px 8px', textAlign:'right', color: sortField === 'started' ? accentColor : 'var(--text-muted)', fontWeight:500, fontSize:10, cursor:'pointer', userSelect:'none', width:70 }}>
              Started {sortIndicator('started')}
            </th>
            <th onClick={() => onSort('duration')} style={{ padding:'6px 8px', textAlign:'right', color: sortField === 'duration' ? accentColor : 'var(--text-muted)', fontWeight:500, fontSize:10, cursor:'pointer', userSelect:'none', width:70 }}>
              Duration {sortIndicator('duration')}
            </th>
            <th onClick={() => onSort('metric')} style={{ padding:'6px 12px', textAlign:'right', color: sortField === 'metric' ? accentColor : 'var(--text-muted)', fontWeight:500, fontSize:10, cursor:'pointer', userSelect:'none', width:80 }}>
              {METRIC_CONFIG[metricMode].label} {sortIndicator('metric')}
            </th>
          </tr>
        </thead>
        <tbody>
          {queries.map((q, i) => {
            const originalIdx = allQueries.indexOf(q);
            const isHighlighted = highlightedItem?.type === 'query' && highlightedItem.idx === originalIdx;
            return (
            <tr key={q.query_id}
              style={{
                background: isHighlighted ? 'rgba(88,166,255,0.35)' : (i % 2 === 0 ? 'transparent' : 'var(--bg-tertiary)'),
                cursor: 'pointer', transition: 'background 0.15s ease',
              }}
              title={q.label}
              onClick={() => onSelect(q)}
              onMouseEnter={() => originalIdx >= 0 && onHighlightItem({ type: 'query', idx: originalIdx })}
              onMouseLeave={() => onHighlightItem(null)}>
              <td style={{ padding:'5px 4px 5px 12px', width:18 }}>
                <div style={{ width:8, height:8, borderRadius:2, background: colors[originalIdx >= 0 ? originalIdx % colors.length : i % colors.length] }} />
              </td>
              <td style={{ padding:'5px 8px', fontFamily:'monospace', color:'#58a6ff', overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }} title={q.query_id}>{q.query_id.slice(0,8)}</td>
              <td style={{ padding:'5px 8px' }}><QueryKindBadge kind={q.query_kind} /></td>
              <td style={{ padding:'5px 8px', color:'var(--text-secondary)', overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>{q.user}</td>
              {showHost && <td style={{ padding:'5px 8px', color:'var(--text-muted)', overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap', fontSize:10 }} title={q.hostname}>{q.hostname ? <TruncatedHost name={q.hostname} maxLen={12} /> : '—'}</td>}
              <td style={{ padding:'5px 8px', textAlign:'center' }} title={q.is_running ? 'Running' : (q.exception || (q.status === 'ExceptionWhileProcessing' || (q.exception_code && q.exception_code !== 0) ? 'Failed' : 'Success'))}>
                {q.is_running ? (
                  <span style={{ display:'inline-block', width:8, height:8, borderRadius:'50%', background:'#58a6ff', animation:'pulse 1.5s ease-in-out infinite' }} />
                ) : (q.status === 'ExceptionWhileProcessing' || (q.exception_code && q.exception_code !== 0)) ? (
                  <span style={{ color:'var(--color-error)', fontSize:10 }}>✕</span>
                ) : (
                  <span style={{ color:'var(--color-success)', fontSize:10 }}>✓</span>
                )}
              </td>
              <td style={{ padding:'5px 8px', textAlign:'right', fontFamily:'monospace', color:'var(--text-muted)', fontSize:10 }}>{new Date(parseTimestamp(q.start_time)).toLocaleTimeString()}</td>
              <td style={{ padding:'5px 8px', textAlign:'right', color:'var(--text-muted)' }}>{fmtMs(q.duration_ms)}</td>
              <td style={{ padding:'5px 12px', textAlign:'right', fontFamily:'monospace', color:'var(--text-primary)', fontWeight:500 }}>{metricForItem(q, metricMode)}</td>
            </tr>
            );
          })}
        </tbody>
      </table>
      ) : (
        <div style={{ padding:'20px', textAlign:'center', color:'var(--text-muted)', fontSize:12 }}>
          {pinnedMs !== null ? 'No queries at pinned time' : 'No queries in window'}
        </div>
      )}
    </div>
  );
};

// ── Merge / Mutation table (shared) ────────────────────────────────────────

export const MergeTable: React.FC<{
  items: (MergeSeries | MutationSeries)[];
  allItems: (MergeSeries | MutationSeries)[];
  totalCount: number;
  pinnedMs: number | null;
  metricMode: MetricMode;
  colors: string[];
  accentColor: string;
  highlightColor: string;
  label: string;
  itemType: 'merge' | 'mutation';
  highlightedItem: HighlightedItem;
  onHighlightItem: (item: HighlightedItem) => void;
  onSelect: (item: MergeSeries | MutationSeries) => void;
  sortField: SortField;
  sortDir: SortDir;
  onSort: (field: SortField) => void;
  showHost?: boolean;
  isHiddenInChart?: boolean;
  onToggleChartVisibility?: () => void;
}> = ({ items, allItems, totalCount, pinnedMs, metricMode, colors, accentColor, highlightColor, label, itemType, highlightedItem, onHighlightItem, onSelect, sortField, sortDir, onSort, showHost, isHiddenInChart, onToggleChartVisibility }) => {
  const sortIndicator = (field: SortField) => sortField === field ? (sortDir === 'desc' ? '▼' : '▲') : '⇅';
  return (
    <div style={{ borderRadius:10, background:'var(--bg-secondary)', border:'1px solid var(--border-primary)', overflow:'hidden' }}>
      <div
        style={{ display:'flex', alignItems:'center', gap:8, padding:'12px 16px', borderBottom:'1px solid var(--border-primary)', cursor: onToggleChartVisibility ? 'pointer' : undefined, userSelect:'none' }}
        onClick={onToggleChartVisibility}
        title={isHiddenInChart ? `Click to show ${label.toLowerCase()} in chart` : `Click to hide ${label.toLowerCase()} from chart`}
      >
        <div style={{ width:3, height:16, borderRadius:2, background: accentColor, opacity: isHiddenInChart ? 0.3 : 1, transition:'opacity 0.15s ease' }} />
        <span style={{ color: isHiddenInChart ? 'var(--text-muted)' : 'var(--text-primary)', fontSize:13, fontWeight:600, transition:'color 0.15s ease', textDecoration: isHiddenInChart ? 'line-through' : 'none' }}>{label}</span>
        <span style={{ color:'var(--text-muted)', fontSize:11 }}>
          {items.length}{pinnedMs !== null ? ' at pin' : ''} / {totalCount} total
          {totalCount > allItems.length && ` (showing top ${allItems.length} by ${METRIC_CONFIG[metricMode].label.toLowerCase()})`}
        </span>
        {isHiddenInChart && <span style={{ fontSize:9, color:'var(--text-muted)', opacity:0.6 }}>hidden</span>}
      </div>
      {items.length > 0 ? (
      <table style={{ width:'100%', borderCollapse:'collapse', fontSize:11, tableLayout:'fixed' }}>
        <thead>
          <tr style={{ borderBottom:'1px solid var(--border-primary)' }}>
            <th style={{ padding:'6px 12px', textAlign:'left', color:'var(--text-muted)', fontWeight:500, fontSize:10, width:18 }}></th>
            <th style={{ padding:'6px 8px', textAlign:'left', color:'var(--text-muted)', fontWeight:500, fontSize:10 }}>Table</th>
            {itemType === 'merge' && <th style={{ padding:'6px 8px', textAlign:'left', color:'var(--text-muted)', fontWeight:500, fontSize:10, width:80 }}>Type</th>}
            <th style={{ padding:'6px 8px', textAlign:'left', color:'var(--text-muted)', fontWeight:500, fontSize:10 }}>Part</th>
            {showHost && <th style={{ padding:'6px 8px', textAlign:'left', color:'var(--text-muted)', fontWeight:500, fontSize:10, width:80 }}>Server</th>}
            <th style={{ padding:'6px 8px', textAlign:'center', color:'var(--text-muted)', fontWeight:500, fontSize:10, width:30 }}></th>
            <th onClick={() => onSort('started')} style={{ padding:'6px 8px', textAlign:'right', color: sortField === 'started' ? accentColor : 'var(--text-muted)', fontWeight:500, fontSize:10, cursor:'pointer', userSelect:'none', width:70 }}>
              Started {sortIndicator('started')}
            </th>
            <th onClick={() => onSort('duration')} style={{ padding:'6px 8px', textAlign:'right', color: sortField === 'duration' ? accentColor : 'var(--text-muted)', fontWeight:500, fontSize:10, cursor:'pointer', userSelect:'none', width:70 }}>
              Duration {sortIndicator('duration')}
            </th>
            <th onClick={() => onSort('metric')} style={{ padding:'6px 12px', textAlign:'right', color: sortField === 'metric' ? accentColor : 'var(--text-muted)', fontWeight:500, fontSize:10, cursor:'pointer', userSelect:'none', width:80 }}>
              {METRIC_CONFIG[metricMode].label} {sortIndicator('metric')}
            </th>
          </tr>
        </thead>
        <tbody>
          {items.map((m, i) => {
            const originalIdx = allItems.indexOf(m);
            const isHighlighted = highlightedItem?.type === itemType && highlightedItem.idx === originalIdx;
            return (
            <tr key={`${m.part_name}-${i}`}
              style={{
                background: isHighlighted ? highlightColor : (i % 2 === 0 ? 'transparent' : 'var(--bg-tertiary)'),
                cursor: 'pointer', transition: 'background 0.15s ease',
              }}
              onClick={() => onSelect(m)}
              onMouseEnter={() => originalIdx >= 0 && onHighlightItem({ type: itemType, idx: originalIdx })}
              onMouseLeave={() => onHighlightItem(null)}>
              <td style={{ padding:'5px 4px 5px 12px', width:18 }}>
                <div style={{ width:8, height:8, borderRadius:2, background: colors[originalIdx >= 0 ? originalIdx % colors.length : i % colors.length] }} />
              </td>
              <td style={{ padding:'5px 8px', fontFamily:'monospace', color:'var(--text-secondary)', overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>{m.table}</td>
              {itemType === 'merge' && <td style={{ padding:'5px 8px' }}><MergeReasonBadge reason={'merge_reason' in m ? m.merge_reason : undefined} /></td>}
              <td style={{ padding:'5px 8px', fontFamily:'monospace', color:'var(--text-muted)', overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }} title={m.part_name}>{m.part_name}</td>
              {showHost && <td style={{ padding:'5px 8px', color:'var(--text-muted)', overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap', fontSize:10 }} title={m.hostname}>{m.hostname ? <TruncatedHost name={m.hostname} maxLen={12} /> : '—'}</td>}
              <td style={{ padding:'5px 8px', textAlign:'center' }} title={m.is_running ? `Running (${Math.round((m.progress || 0) * 100)}%)` : 'Completed'}>
                {m.is_running ? (
                  <span style={{ display:'inline-block', width:8, height:8, borderRadius:'50%', background: accentColor, animation:'pulse 1.5s ease-in-out infinite' }} />
                ) : (
                  <span style={{ color:'var(--color-success)', fontSize:10 }}>✓</span>
                )}
              </td>
              <td style={{ padding:'5px 8px', textAlign:'right', fontFamily:'monospace', color:'var(--text-muted)', fontSize:10 }}>{new Date(parseTimestamp(m.start_time)).toLocaleTimeString()}</td>
              <td style={{ padding:'5px 8px', textAlign:'right', color:'var(--text-muted)' }}>{m.is_running && m.progress ? `${Math.round(m.progress * 100)}%` : fmtMs(m.duration_ms)}</td>
              <td style={{ padding:'5px 12px', textAlign:'right', fontFamily:'monospace', color:'var(--text-primary)', fontWeight:500 }}>{metricForItem(m, metricMode)}</td>
            </tr>
            );
          })}
        </tbody>
      </table>
      ) : (
        <div style={{ padding:'20px', textAlign:'center', color:'var(--text-muted)', fontSize:12 }}>
          {pinnedMs !== null ? `No ${label.toLowerCase()} at pinned time` : `No ${label.toLowerCase()} in window`}
        </div>
      )}
    </div>
  );
};
