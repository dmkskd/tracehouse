/**
 * MergeActivityTable - one lifecycle table for live and completed merges.
 */

import React, { useCallback, useMemo } from 'react';
import { getMergeCategoryInfo } from '@tracehouse/core';
import type {
  MergeHistoryRecord,
  MergeHistorySort,
  MergeHistorySortField,
  MergeInfo,
  SortDirection,
} from '../../stores/mergeStore';
import {
  formatBytes,
  formatBytesPerSec,
  formatDurationMs,
  formatNumber,
} from '../../stores/mergeStore';
import { CopyTableButton } from '../common/CopyTableButton';
import {
  isMergeActivityRecordSelected,
  sortMergeActivityRecords,
  type MergeActivityRecord,
} from './merge-activity-model';

interface MergeActivityTableProps {
  activity: MergeActivityRecord[];
  sort: MergeHistorySort;
  onSortChange: (sort: MergeHistorySort) => void;
  isLoading: boolean;
  selectedLiveMerge?: MergeInfo | null;
  selectedHistoryRecord?: MergeHistoryRecord | null;
  onSelectLive: (merge: MergeInfo) => void;
  onPreviewLive?: (merge: MergeInfo) => void;
  onSelectHistory: (record: MergeHistoryRecord) => void;
  onPreviewHistory?: (record: MergeHistoryRecord) => void;
}

const thStyle: React.CSSProperties = {
  padding: '7px 8px',
  textAlign: 'left',
  color: 'var(--text-muted)',
  fontWeight: 500,
  fontSize: 10,
  whiteSpace: 'nowrap',
};

const tdStyle: React.CSSProperties = {
  padding: '6px 8px',
  color: 'var(--text-muted)',
  whiteSpace: 'nowrap',
};

const formatStarted = (value: string): string =>
  new Date(value).toLocaleString(undefined, {
    day: 'numeric',
    month: 'short',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });

const SortableHeader: React.FC<{
  field: MergeHistorySortField;
  label: string;
  sort: MergeHistorySort;
  onSort: (field: MergeHistorySortField) => void;
  align?: 'left' | 'right';
}> = ({ field, label, sort, onSort, align = 'left' }) => {
  const active = sort.field === field;
  return (
    <th
      onClick={() => onSort(field)}
      style={{ ...thStyle, textAlign: align, cursor: 'pointer', userSelect: 'none' }}
    >
      {label}{' '}
      <span style={{ color: active ? '#f0883e' : 'var(--text-muted)', fontSize: 9 }}>
        {active ? (sort.direction === 'desc' ? '▼' : '▲') : '⇅'}
      </span>
    </th>
  );
};

const StatusBadge: React.FC<{ record: MergeActivityRecord }> = ({ record }) => {
  const running = record.status === 'running';
  const finalizing = record.status === 'finalizing';
  const error = record.status === 'error';
  const label = running ? 'Running' : finalizing ? 'Finalizing…' : error ? 'Error' : 'OK';
  const color = running ? '#58a6ff' : finalizing ? '#d29922' : error ? '#f85149' : '#3fb950';
  const background = running
    ? 'rgba(88,166,255,0.15)'
    : finalizing
      ? 'rgba(210,169,34,0.15)'
      : error
        ? 'rgba(248,81,73,0.15)'
        : 'rgba(63,185,80,0.12)';

  return (
    <span
      title={record.exception || (record.error ? `Error code ${record.error}` : label)}
      style={{
        display: 'inline-flex',
        alignItems: 'center',
        gap: 5,
        padding: '2px 8px',
        fontSize: 10,
        fontWeight: 500,
        borderRadius: 10,
        background,
        color,
      }}
    >
      {(running || finalizing) && (
        <span
          aria-hidden="true"
          style={{
            width: 6,
            height: 6,
            borderRadius: '50%',
            background: 'currentColor',
            animation: 'activity-running-pulse 2.4s ease-in-out infinite',
          }}
        />
      )}
      {label}
    </span>
  );
};

const Progress: React.FC<{ value: number | null; stuck: boolean }> = ({ value, stuck }) => {
  if (value == null) return <span style={{ color: 'var(--text-muted)' }}>—</span>;
  const percentage = Math.min(100, Math.max(0, value * 100));
  return (
    <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'flex-end', gap: 6 }}>
      <div style={{ width: 54, height: 4, borderRadius: 2, background: 'var(--bg-tertiary)' }}>
        <div
          style={{
            width: `${percentage}%`,
            height: 4,
            borderRadius: 2,
            background: stuck ? '#f85149' : percentage < 50 ? '#58a6ff' : '#3fb950',
            transition: 'width 0.3s ease',
          }}
        />
      </div>
      <span style={{ minWidth: 36, fontFamily: 'monospace', color: 'var(--text-primary)' }}>
        {percentage.toFixed(1)}%
      </span>
    </div>
  );
};

export const MergeActivityTable: React.FC<MergeActivityTableProps> = ({
  activity,
  sort,
  onSortChange,
  isLoading,
  selectedLiveMerge,
  selectedHistoryRecord,
  onSelectLive,
  onPreviewLive,
  onSelectHistory,
  onPreviewHistory,
}) => {
  const sortedActivity = useMemo(
    () => sortMergeActivityRecords(activity, sort),
    [activity, sort],
  );
  const handleSort = useCallback((field: MergeHistorySortField) => {
    const direction: SortDirection =
      sort.field === field && sort.direction === 'desc' ? 'asc' : 'desc';
    onSortChange({ field, direction });
  }, [onSortChange, sort]);

  if (sortedActivity.length === 0) {
    return (
      <div style={{ padding: '40px 0', textAlign: 'center', color: 'var(--text-muted)', fontSize: 12 }}>
        {isLoading ? 'Loading merge activity...' : 'No merges found matching the current filters'}
      </div>
    );
  }

  return (
    <div style={{ overflow: 'auto', contain: 'content' }}>
      <style>{`
        .merge-activity-row:hover { background: var(--bg-hover) !important; }
        .merge-activity-row.selected,
        .merge-activity-row.selected:hover { background: rgba(240,136,62,0.2) !important; }
      `}</style>
      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11 }}>
        <thead>
          <tr style={{ borderBottom: '1px solid var(--border-primary)' }}>
            <th style={{ ...thStyle, width: 18, paddingLeft: 12 }}>
              <CopyTableButton
                headers={[
                  'Status', 'Table', 'Host', 'Part', 'Category', 'Started', 'Duration',
                  'Rows Read', 'Rows Written', 'Size', 'Memory', 'Throughput', 'Progress',
                ]}
                rows={sortedActivity.map(record => {
                  const category = getMergeCategoryInfo(record.category);
                  return [
                    record.status === 'ok'
                      ? 'OK'
                      : record.status === 'error'
                        ? 'Error'
                        : record.status === 'finalizing'
                          ? 'Finalizing'
                          : 'Running',
                    `${record.database}.${record.table}`,
                    record.hostname || '',
                    record.partLabel,
                    category.label,
                    formatStarted(record.startedAt),
                    formatDurationMs(record.durationMs),
                    formatNumber(record.rowsRead),
                    formatNumber(record.rowsWritten),
                    formatBytes(record.sizeBytes),
                    formatBytes(record.memoryBytes),
                    formatBytesPerSec(record.throughputBytesPerSec),
                    record.progress == null ? '' : `${(record.progress * 100).toFixed(1)}%`,
                  ];
                })}
                size={12}
              />
            </th>
            <th style={thStyle}>Status</th>
            <th style={thStyle}>Table</th>
            <th style={thStyle}>Host</th>
            <th style={thStyle}>Part</th>
            <th style={thStyle}>Category</th>
            <SortableHeader field="event_time" label="Started" sort={sort} onSort={handleSort} />
            <SortableHeader field="duration_ms" label="Duration" sort={sort} onSort={handleSort} align="right" />
            <SortableHeader field="rows" label="Rows R/W" sort={sort} onSort={handleSort} align="right" />
            <SortableHeader field="size_in_bytes" label="Size" sort={sort} onSort={handleSort} align="right" />
            <th style={{ ...thStyle, textAlign: 'right' }}>Memory</th>
            <SortableHeader field="throughput" label="Throughput" sort={sort} onSort={handleSort} align="right" />
            <th style={{ ...thStyle, textAlign: 'right', paddingRight: 12 }}>Progress</th>
          </tr>
        </thead>
        <tbody>
          {sortedActivity.map(record => {
            const category = getMergeCategoryInfo(record.category);
            const selected = isMergeActivityRecordSelected(
              record,
              selectedLiveMerge,
              selectedHistoryRecord,
            );
            const handleClick = () => {
              if (record.liveMerge) onSelectLive(record.liveMerge);
              else if (record.historyRecord) onSelectHistory(record.historyRecord);
            };
            const handlePreview = () => {
              if (record.liveMerge) onPreviewLive?.(record.liveMerge);
              else if (record.historyRecord) onPreviewHistory?.(record.historyRecord);
            };

            return (
              <tr
                key={record.activityKey}
                className={`merge-activity-row${selected ? ' selected' : ''}`}
                onClick={handleClick}
                onMouseEnter={handlePreview}
                style={{
                  borderBottom: '1px solid var(--border-primary)',
                  background: 'transparent',
                  cursor: 'pointer',
                  transition: 'background 0.15s ease',
                }}
              >
                <td style={{ ...tdStyle, width: 18, paddingLeft: 12, paddingRight: 4 }}>
                  <span
                    title={record.isStuck ? 'Potentially stuck — low progress relative to elapsed time' : undefined}
                    style={{
                      display: 'block',
                      width: 8,
                      height: 8,
                      borderRadius: 2,
                      background: record.isStuck ? '#f85149' : category.color,
                    }}
                  />
                </td>
                <td style={tdStyle}><StatusBadge record={record} /></td>
                <td style={{ ...tdStyle, fontFamily: 'monospace', color: 'var(--text-secondary)' }}>
                  {record.database}.{record.table}
                </td>
                <td
                  title={record.hostname || 'local'}
                  style={{ ...tdStyle, fontFamily: 'monospace', fontSize: 10 }}
                >
                  {record.hostname || '—'}
                </td>
                <td
                  title={record.partLabel}
                  style={{
                    ...tdStyle,
                    maxWidth: 170,
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                    fontFamily: 'monospace',
                  }}
                >
                  {record.partLabel}
                </td>
                <td style={tdStyle}>
                  <span
                    title={category.description}
                    style={{
                      padding: '1px 6px',
                      fontSize: 9,
                      borderRadius: 3,
                      background: `${category.color}20`,
                      color: category.color,
                      border: `1px solid ${category.color}33`,
                    }}
                  >
                    {category.label}
                  </span>
                  {record.isReplicaMerge && (
                    <span
                      title="Same merge running on another replica"
                      style={{
                        padding: '1px 6px',
                        marginLeft: 4,
                        fontSize: 9,
                        borderRadius: 3,
                        background: 'rgba(136,136,136,0.15)',
                        color: '#888',
                        border: '1px solid rgba(136,136,136,0.25)',
                      }}
                    >
                      Replica
                    </span>
                  )}
                </td>
                <td style={{ ...tdStyle, fontFamily: 'monospace', fontSize: 10 }}>
                  {formatStarted(record.startedAt)}
                </td>
                <td
                  title={record.isStuck ? 'Potentially stuck — low progress relative to elapsed time' : undefined}
                  style={{
                    ...tdStyle,
                    textAlign: 'right',
                    fontFamily: 'monospace',
                    color: record.isStuck ? '#f85149' : 'var(--text-primary)',
                    fontWeight: record.isStuck ? 600 : 500,
                  }}
                >
                  {formatDurationMs(record.durationMs)}
                </td>
                <td style={{ ...tdStyle, textAlign: 'right', fontFamily: 'monospace' }}>
                  {formatNumber(record.rowsRead)}/{formatNumber(record.rowsWritten)}
                </td>
                <td style={{ ...tdStyle, textAlign: 'right', fontFamily: 'monospace' }}>
                  {formatBytes(record.sizeBytes)}
                </td>
                <td style={{ ...tdStyle, textAlign: 'right', fontFamily: 'monospace' }}>
                  {formatBytes(record.memoryBytes)}
                </td>
                <td style={{ ...tdStyle, textAlign: 'right', fontFamily: 'monospace' }}>
                  {formatBytesPerSec(record.throughputBytesPerSec)}
                </td>
                <td style={{ ...tdStyle, textAlign: 'right', paddingRight: 12 }}>
                  <Progress value={record.progress} stuck={record.isStuck} />
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
};

export default MergeActivityTable;
