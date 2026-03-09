/**
 * MergeHistoryTable - Component for displaying merge history with filtering and sorting
 * Styled to match Time Travel page tables
 */

import React, { useCallback } from 'react';
import type { 
  MergeHistoryRecord, 
  MergeHistorySort, 
  MergeHistorySortField,
  SortDirection,
} from '../../stores/mergeStore';
import { formatBytes, formatNumber, formatDurationMs, formatBytesPerSec, sortMergeHistory } from '../../stores/mergeStore';
import { CopyTableButton } from '../common/CopyTableButton';
import { getMergeCategoryInfo, type MergeCategory } from '@tracehouse/core';

interface MergeHistoryTableProps {
  history: MergeHistoryRecord[];
  sort: MergeHistorySort;
  onSortChange: (sort: MergeHistorySort) => void;
  isLoading: boolean;
  selectedRecord?: MergeHistoryRecord | null;
  onSelectRecord?: (record: MergeHistoryRecord) => void;
}

// Color palette for merge history indicators
const HISTORY_COLORS = ['#f0883e', '#ffa657', '#d29922', '#e3b341', '#f78166', '#db6d28'];

const SortableHeader: React.FC<{
  field: MergeHistorySortField;
  label: string;
  currentSort: MergeHistorySort;
  onSort: (field: MergeHistorySortField) => void;
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

const EmptyStatus: React.FC<{ isLoading: boolean }> = ({ isLoading }) => (
  <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: 80, color: 'var(--text-muted)' }}>
    {isLoading ? (
      <div style={{ textAlign: 'center' }}>
        <div style={{ width: 16, height: 16, border: '2px solid var(--border-primary)', borderTopColor: 'var(--accent-primary)', borderRadius: '50%', animation: 'spin 1s linear infinite', marginBottom: 4 }} />
        <p style={{ fontSize: 11 }}>Loading...</p>
      </div>
    ) : (
      <div style={{ textAlign: 'center' }}>
        <div style={{ fontSize: 13, marginBottom: 2, fontWeight: 300 }}>No History</div>
        <p style={{ fontSize: 10 }}>No merge operations found</p>
      </div>
    )}
  </div>
);

export const MergeHistoryTable: React.FC<MergeHistoryTableProps> = ({
  history,
  sort,
  onSortChange,
  isLoading,
  selectedRecord,
  onSelectRecord,
}) => {
  const handleSort = useCallback((field: MergeHistorySortField) => {
    const newDirection: SortDirection = sort.field === field && sort.direction === 'desc' ? 'asc' : 'desc';
    onSortChange({ field, direction: newDirection });
  }, [sort, onSortChange]);

  const sortedHistory = sortMergeHistory(history, sort);

  return (
    <div>

      {sortedHistory.length === 0 ? (
        <EmptyStatus isLoading={isLoading} />
      ) : (
        <div style={{ overflow: 'auto' }}>
          <style>{`
            .merge-history-row:hover { background: var(--bg-hover) !important; }
          `}</style>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11 }}>
            <thead>
              <tr style={{ borderBottom: '1px solid var(--border-primary)' }}>
                <th style={{ padding: '6px 12px', textAlign: 'left', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10, width: 18 }}>
                  <CopyTableButton
                    headers={['Table', 'Host', 'Part', 'Reason', 'Status', 'Rows', 'Rows Diff', 'Size', 'Duration', 'Throughput', 'Time']}
                    rows={sortedHistory.map(r => [
                      `${r.database}.${r.table}`, r.hostname || '', r.part_name,
                      r.merge_reason || '-', r.error ? `Error ${r.error}` : 'OK', formatNumber(r.rows), (r.rows_diff ?? 0) !== 0 ? formatNumber(r.rows_diff) : '', formatBytes(r.size_in_bytes),
                      formatDurationMs(r.duration_ms),
                      r.duration_ms > 0 ? formatBytesPerSec(r.size_in_bytes / (r.duration_ms / 1000)) : '-',
                      new Date(r.event_time).toLocaleString(),
                    ])}
                    size={12}
                  />
                </th>
                <th style={{ padding: '6px 8px', textAlign: 'left', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10 }}>Table</th>
                <th style={{ padding: '6px 8px', textAlign: 'left', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10 }}>Host</th>
                <th style={{ padding: '6px 8px', textAlign: 'left', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10 }}>Part</th>
                <th style={{ padding: '6px 8px', textAlign: 'left', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10 }}>Reason</th>
                <th style={{ padding: '6px 8px', textAlign: 'left', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10 }}>Status</th>
                <SortableHeader field="rows" label="Rows" currentSort={sort} onSort={handleSort} align="right" />
                <SortableHeader field="size_in_bytes" label="Size" currentSort={sort} onSort={handleSort} align="right" />
                <SortableHeader field="duration_ms" label="Duration" currentSort={sort} onSort={handleSort} align="right" />
                <SortableHeader field="throughput" label="Throughput" currentSort={sort} onSort={handleSort} align="right" />
                <SortableHeader field="event_time" label="Time" currentSort={sort} onSort={handleSort} align="right" />
              </tr>
            </thead>
            <tbody>
              {sortedHistory.map((record, idx) => {
                const isSelected = selectedRecord && 
                  selectedRecord.event_time === record.event_time && 
                  selectedRecord.part_name === record.part_name;
                return (
                <tr 
                  key={`${record.event_time}-${record.part_name}-${idx}`}
                  className="merge-history-row"
                  onClick={() => onSelectRecord?.(record)}
                  style={{ 
                    borderBottom: '1px solid var(--border-primary)',
                    background: isSelected ? 'rgba(240,136,62,0.2)' : (idx % 2 === 0 ? 'transparent' : 'var(--bg-tertiary)'),
                    cursor: onSelectRecord ? 'pointer' : 'default',
                    transition: 'background 0.15s ease',
                  }}
                >
                  <td style={{ padding: '5px 4px 5px 12px', width: 18 }}>
                    <div style={{ width: 8, height: 8, borderRadius: 2, background: HISTORY_COLORS[idx % HISTORY_COLORS.length] }} />
                  </td>
                  <td style={{ padding: '5px 8px', fontFamily: 'monospace', color: 'var(--text-secondary)' }}>
                    {record.database}.{record.table}
                  </td>
                  <td style={{ padding: '5px 8px', fontFamily: 'monospace', color: 'var(--text-muted)', fontSize: 10 }} title={record.hostname || 'local'}>
                    {record.hostname || '—'}
                  </td>
                  <td style={{ padding: '5px 8px', fontFamily: 'monospace', color: 'var(--text-muted)', maxWidth: 140, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={record.part_name}>
                    {record.part_name}
                  </td>
                  <td style={{ padding: '5px 8px' }}>
                    {(() => {
                      const reason = record.merge_reason || 'Regular';
                      const info = getMergeCategoryInfo(reason as MergeCategory);
                      return (
                        <span
                          title={info.description}
                          style={{
                            padding: '1px 6px', fontSize: 9, borderRadius: 3,
                            background: `${info.color}20`, color: info.color,
                            border: `1px solid ${info.color}33`,
                          }}
                        >
                          {info.label}
                        </span>
                      );
                    })()}
                  </td>
                  <td style={{ padding: '5px 8px' }}>
                    {record.error ? (
                      <span
                        title={record.exception || `Error code ${record.error}`}
                        style={{
                          padding: '1px 6px', fontSize: 9, borderRadius: 3,
                          background: 'rgba(229,83,75,0.12)', color: '#e5534b',
                          border: '1px solid rgba(229,83,75,0.25)',
                        }}
                      >
                        Error {record.error}
                      </span>
                    ) : (
                      <span style={{
                        padding: '1px 6px', fontSize: 9, borderRadius: 3,
                        background: 'rgba(63,185,80,0.12)', color: '#3fb950',
                        border: '1px solid rgba(63,185,80,0.25)',
                      }}>
                        OK
                      </span>
                    )}
                  </td>
                  <td style={{ padding: '5px 8px', textAlign: 'right', fontFamily: 'monospace', color: 'var(--text-muted)' }}>
                    {formatNumber(record.rows)}
                  </td>
                  <td style={{ padding: '5px 8px', textAlign: 'right', fontFamily: 'monospace', color: 'var(--text-muted)' }}>
                    {formatBytes(record.size_in_bytes)}
                  </td>
                  <td style={{ padding: '5px 8px', textAlign: 'right', fontFamily: 'monospace', color: 'var(--text-primary)', fontWeight: 500 }}>
                    {formatDurationMs(record.duration_ms)}
                  </td>
                  <td style={{ padding: '5px 8px', textAlign: 'right', fontFamily: 'monospace', color: 'var(--text-muted)' }}>
                    {record.duration_ms > 0 ? formatBytesPerSec(record.size_in_bytes / (record.duration_ms / 1000)) : '-'}
                  </td>
                  <td style={{ padding: '5px 12px', textAlign: 'right', fontFamily: 'monospace', color: 'var(--text-muted)', fontSize: 10 }}>
                    {new Date(record.event_time).toLocaleString()}
                  </td>
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

export default MergeHistoryTable;
