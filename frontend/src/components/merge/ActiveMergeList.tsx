/**
 * ActiveMergeList - Table format for active merges
 * Styled to match Time Travel page tables
 */

import React, { useState, useCallback, useMemo } from 'react';
import type { MergeInfo } from '../../stores/mergeStore';
import { formatBytes, formatDuration, formatNumber, formatBytesPerSec } from '../../stores/mergeStore';
import { CopyTableButton } from '../common/CopyTableButton';
import { classifyActiveMerge, getMergeCategoryInfo } from '@tracehouse/core';

type ActiveMergeSortField = 'table' | 'size' | 'memory' | 'rows_read' | 'throughput' | 'progress' | 'elapsed';
type SortDirection = 'asc' | 'desc';
interface ActiveMergeSort {
  field: ActiveMergeSortField;
  direction: SortDirection;
}

const SortableHeader: React.FC<{
  field: ActiveMergeSortField;
  label: string;
  currentSort: ActiveMergeSort;
  onSort: (field: ActiveMergeSortField) => void;
  align?: 'left' | 'right';
}> = ({ field, label, currentSort, onSort, align = 'left' }) => {
  const isActive = currentSort.field === field;
  return (
    <th
      onClick={() => onSort(field)}
      style={{
        padding: '6px 8px',
        textAlign: align,
        color: isActive ? '#f0883e' : 'var(--text-muted)',
        fontWeight: 500,
        fontSize: 10,
        cursor: 'pointer',
        userSelect: 'none',
      }}
    >
      {label} {isActive ? (currentSort.direction === 'desc' ? '▼' : '▲') : '⇅'}
    </th>
  );
};

function getMergeThroughput(m: MergeInfo): number {
  const bytesProcessed = m.total_size_bytes_compressed * m.progress;
  return m.elapsed > 0 ? bytesProcessed / m.elapsed : 0;
}

function sortActiveMerges(merges: MergeInfo[], sort: ActiveMergeSort): MergeInfo[] {
  const sorted = [...merges];
  const dir = sort.direction === 'desc' ? -1 : 1;
  sorted.sort((a, b) => {
    let cmp = 0;
    switch (sort.field) {
      case 'table': cmp = `${a.database}.${a.table}`.localeCompare(`${b.database}.${b.table}`); break;
      case 'size': cmp = a.total_size_bytes_compressed - b.total_size_bytes_compressed; break;
      case 'memory': cmp = (a.memory_usage || 0) - (b.memory_usage || 0); break;
      case 'rows_read': cmp = a.rows_read - b.rows_read; break;
      case 'throughput': cmp = getMergeThroughput(a) - getMergeThroughput(b); break;
      case 'progress': cmp = a.progress - b.progress; break;
      case 'elapsed': cmp = a.elapsed - b.elapsed; break;
    }
    return cmp * dir;
  });
  return sorted;
}

interface ActiveMergeListProps {
  merges: MergeInfo[];
  selectedMerge: MergeInfo | null;
  onSelectMerge: (merge: MergeInfo) => void;
  isLoading: boolean;
}

const ProgressBar: React.FC<{ progress: number; stuck?: boolean }> = ({ progress, stuck }) => {
  const percentage = Math.min(Math.max(progress * 100, 0), 100);

  return (
    <div
      style={{ background: 'var(--bg-tertiary)', minWidth: '60px', height: '4px', borderRadius: 2 }}
    >
      <div
        style={{
          width: `${percentage}%`,
          height: '4px',
          borderRadius: 2,
          background: stuck ? '#f85149' : percentage < 50 ? '#58a6ff' : '#3fb950',
          transition: 'width 0.3s ease',
        }}
      />
    </div>
  );
};

const EmptyState: React.FC<{ isLoading: boolean }> = ({ isLoading }) => (
  <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: 80, color: 'var(--text-muted)' }}>
    {isLoading ? (
      <div style={{ textAlign: 'center' }}>
        <div style={{ 
          width: 16, height: 16, border: '2px solid var(--border-primary)', 
          borderTopColor: 'var(--accent-primary)', borderRadius: '50%',
          animation: 'spin 1s linear infinite', marginBottom: 4 
        }} />
        <p style={{ fontSize: 11 }}>Loading...</p>
      </div>
    ) : (
      <div style={{ textAlign: 'center' }}>
        <div style={{ fontSize: 13, marginBottom: 2, fontWeight: 300 }}>OK</div>
        <p style={{ fontSize: 10 }}>No active merges</p>
      </div>
    )}
  </div>
);

// Color palette for merge indicators (matching Time Travel style)
// ── Stuck / slow merge detection ──

/** Thresholds for elapsed time color-coding (in seconds) */
const ELAPSED_WARN_SEC = 10 * 60;   // 10 min → yellow
const ELAPSED_DANGER_SEC = 30 * 60; // 30 min → red

/**
 * A merge is considered "potentially stuck" when it has been running for a long
 * time relative to its progress — i.e. it won't finish any time soon at its
 * current rate.
 *
 * Heuristic: if elapsed > 10 min AND estimated remaining time > 30 min, flag it.
 *
 * Note: ClickHouse can report progress = 1.0 while a merge is still listed in
 * system.merges (finalization phase: writing metadata, renaming parts). If a
 * merge is at 100% but has been sitting there for a long time, it's also stuck.
 */
export function isMergeStuck(m: MergeInfo): boolean {
  if (m.elapsed < ELAPSED_WARN_SEC) return false;             // too early to tell
  // progress >= 99.95% but still in system.merges — stuck in finalization
  // (ClickHouse may report 0.999… which displays as "100.0%" after rounding)
  if (m.progress >= 0.9995) return m.elapsed > ELAPSED_DANGER_SEC;
  if (m.progress <= 0.001) return true;                       // no progress at all after 10min
  const estimatedTotal = m.elapsed / m.progress;
  const remaining = estimatedTotal - m.elapsed;
  return remaining > ELAPSED_DANGER_SEC;
}

/** Color for elapsed time based on duration */
function elapsedColor(m: MergeInfo): string {
  if (isMergeStuck(m)) return '#f85149';                      // red — stuck
  if (m.elapsed >= ELAPSED_DANGER_SEC) return '#d29922';      // yellow — long but progressing
  if (m.elapsed >= ELAPSED_WARN_SEC) return '#d29922';        // yellow — warning
  return 'var(--text-muted)';                                 // normal
}

const MERGE_COLORS = ['#f0883e', '#ffa657', '#d29922', '#e3b341', '#f78166', '#db6d28'];

export const ActiveMergeList: React.FC<ActiveMergeListProps> = ({
  merges,
  selectedMerge,
  onSelectMerge,
  isLoading,
}) => {
  const [sort, setSort] = useState<ActiveMergeSort>({ field: 'elapsed', direction: 'desc' });

  const handleSort = useCallback((field: ActiveMergeSortField) => {
    setSort(prev => ({
      field,
      direction: prev.field === field && prev.direction === 'desc' ? 'asc' : 'desc',
    }));
  }, []);

  const sortedMerges = useMemo(() => sortActiveMerges(merges, sort), [merges, sort]);

  if (merges.length === 0 && isLoading) {
    return <EmptyState isLoading={true} />;
  }

  if (merges.length === 0) {
    return <EmptyState isLoading={false} />;
  }

  const getCategory = (merge: MergeInfo) => {
    const cat = classifyActiveMerge(merge.merge_type, merge.is_mutation, merge.result_part_name);
    return getMergeCategoryInfo(cat);
  };

  return (
    <div style={{ overflow: 'auto', contain: 'content' }}>
      <style>{`
        .active-merge-row:hover { background: var(--bg-hover) !important; }
      `}</style>
      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11 }}>
        <thead>
          <tr style={{ borderBottom: '1px solid var(--border-primary)' }}>
            <th style={{ padding: '6px 12px', textAlign: 'left', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10, width: 18 }}>
              <CopyTableButton
                headers={['Table', 'Host', 'Part', 'Type', 'Size', 'Memory', 'Rows Read', 'Rows Written', 'Throughput', 'Progress', 'Elapsed']}
                rows={sortedMerges.map(m => {
                  const tp = getMergeThroughput(m);
                  const cat = getCategory(m);
                  return [
                    `${m.database}.${m.table}`, m.hostname || '', `${m.num_parts} → ${m.result_part_name}`,
                    cat.label,
                    formatBytes(m.total_size_bytes_compressed), formatBytes(m.memory_usage || 0),
                    formatNumber(m.rows_read), formatNumber(m.rows_written),
                    formatBytesPerSec(tp), `${(m.progress * 100).toFixed(1)}%`, formatDuration(m.elapsed),
                  ];
                })}
                size={12}
              />
            </th>
            <SortableHeader field="table" label="Table" currentSort={sort} onSort={handleSort} />
            <th style={{ padding: '6px 8px', textAlign: 'left', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10 }}>Host</th>
            <th style={{ padding: '6px 8px', textAlign: 'left', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10 }}>Part</th>
            <th style={{ padding: '6px 8px', textAlign: 'left', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10 }}>Type</th>
            <SortableHeader field="size" label="Size" currentSort={sort} onSort={handleSort} align="right" />
            <SortableHeader field="memory" label="Memory" currentSort={sort} onSort={handleSort} align="right" />
            <SortableHeader field="rows_read" label="Rows R/W" currentSort={sort} onSort={handleSort} align="right" />
            <SortableHeader field="throughput" label="Throughput" currentSort={sort} onSort={handleSort} align="right" />
            <SortableHeader field="progress" label="Progress" currentSort={sort} onSort={handleSort} align="right" />
            <SortableHeader field="elapsed" label="Elapsed" currentSort={sort} onSort={handleSort} align="right" />
          </tr>
        </thead>
        <tbody>
          {sortedMerges.map((merge, i) => {
            const mergeKey = `${merge.database}.${merge.table}.${merge.result_part_name}`;
            const isSelected = selectedMerge !== null &&
              selectedMerge.database === merge.database &&
              selectedMerge.table === merge.table &&
              selectedMerge.result_part_name === merge.result_part_name;
            const catInfo = getCategory(merge);
            const typeLabel = catInfo.label;
            const typeColor = catInfo.color;
            const throughput = getMergeThroughput(merge);
            
            return (
              <tr 
                key={mergeKey}
                className="active-merge-row"
                onClick={() => onSelectMerge(merge)}
                style={{ 
                  borderBottom: '1px solid var(--border-primary)',
                  background: isSelected ? 'rgba(240,136,62,0.2)' : (i % 2 === 0 ? 'transparent' : 'var(--bg-tertiary)'),
                  cursor: 'pointer',
                  transition: 'background 0.15s ease',
                }}
              >
                <td style={{ padding: '5px 4px 5px 12px', width: 18 }}>
                  <div style={{ width: 8, height: 8, borderRadius: 2, background: isMergeStuck(merge) ? '#f85149' : MERGE_COLORS[i % MERGE_COLORS.length] }} title={isMergeStuck(merge) ? 'Potentially stuck — low progress relative to elapsed time' : undefined} />
                </td>
                <td style={{ padding: '5px 8px', fontFamily: 'monospace', color: 'var(--text-secondary)' }}>
                  {merge.database}.{merge.table}
                </td>
                <td style={{ padding: '5px 8px', fontFamily: 'monospace', color: 'var(--text-muted)', fontSize: 10 }} title={merge.hostname || 'local'}>
                  {merge.hostname || '—'}
                </td>
                <td style={{ padding: '5px 8px', fontFamily: 'monospace', color: 'var(--text-muted)', maxWidth: 140, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={`${merge.num_parts} → ${merge.result_part_name}`}>
                  {merge.num_parts} → {merge.result_part_name}
                </td>
                <td style={{ padding: '5px 8px' }}>
                  <span style={{ 
                    padding: '1px 6px', 
                    fontSize: 9, 
                    borderRadius: 3,
                    background: `${typeColor}20`,
                    color: typeColor,
                    border: `1px solid ${typeColor}33`,
                  }}>
                    {typeLabel}
                  </span>
                </td>
                <td style={{ padding: '5px 8px', textAlign: 'right', fontFamily: 'monospace', color: 'var(--text-muted)' }}>
                  {formatBytes(merge.total_size_bytes_compressed)}
                </td>
                <td style={{ padding: '5px 8px', textAlign: 'right', fontFamily: 'monospace', color: 'var(--text-muted)' }}>
                  {formatBytes(merge.memory_usage || 0)}
                </td>
                <td style={{ padding: '5px 8px', textAlign: 'right', fontFamily: 'monospace', color: 'var(--text-muted)' }}>
                  {formatNumber(merge.rows_read)}/{formatNumber(merge.rows_written)}
                </td>
                <td style={{ padding: '5px 8px', textAlign: 'right', fontFamily: 'monospace', color: 'var(--text-muted)' }}>
                  {formatBytesPerSec(throughput)}
                </td>
                <td style={{ padding: '5px 8px', textAlign: 'right' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 6, justifyContent: 'flex-end' }}>
                    <ProgressBar progress={merge.progress} stuck={isMergeStuck(merge)} />
                    <span style={{ fontFamily: 'monospace', fontSize: 10, color: 'var(--text-primary)', fontWeight: 500, minWidth: 36 }}>
                      {(merge.progress * 100).toFixed(1)}%
                    </span>
                  </div>
                </td>
                <td style={{ padding: '5px 12px', textAlign: 'right', fontFamily: 'monospace', color: elapsedColor(merge), fontSize: 10, fontWeight: isMergeStuck(merge) ? 600 : 400 }} title={isMergeStuck(merge) ? 'Potentially stuck — low progress relative to elapsed time' : undefined}>
                  {formatDuration(merge.elapsed)}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
};

export default ActiveMergeList;
