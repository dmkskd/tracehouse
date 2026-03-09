/**
 * ActiveMergesTable - Table showing active merge operations
 */

import { useCallback } from 'react';
import { Link } from 'react-router-dom';
import { ProgressRing } from './ProgressRing';
import { SortableHeader } from './SortableHeader';
import { OVERVIEW_COLORS } from '../../styles/overviewColors';
import { useSortState, useSortedData } from '../../hooks/useSortState';
import type { ActiveMergeInfo } from '@tracehouse/core';
import { CopyTableButton } from '../common/CopyTableButton';
import { formatBytes, formatBytesPerSec, formatElapsed, formatNumberCompact as formatNumber } from '../../utils/formatters';

type MergeSortKey = 'table' | 'elapsed' | 'memory' | 'read' | 'write' | 'rows' | 'parts' | 'progress';

interface ActiveMergesTableProps {
  merges: ActiveMergeInfo[];
  maxRows?: number;
  className?: string;
}

function getMergeSortValue(item: ActiveMergeInfo, key: MergeSortKey): number | string {
  switch (key) {
    case 'table': return `${item.database}.${item.table}`;
    case 'elapsed': return item.elapsed;
    case 'memory': return item.memoryUsage;
    case 'read': return item.readBytesPerSec;
    case 'write': return item.writeBytesPerSec;
    case 'rows': return item.rowsRead;
    case 'parts': return item.numParts;
    case 'progress': return item.progress;
  }
}

export function ActiveMergesTable({ merges, maxRows = 10, className = '' }: ActiveMergesTableProps) {
  const { sort, toggleSort } = useSortState<MergeSortKey>('elapsed');
  const getValue = useCallback(getMergeSortValue, []);
  const sortedMerges = useSortedData(merges, sort, getValue);

  // Calculate max height: header (~32px) + rows (~40px each)
  const maxHeight = 32 + (maxRows * 40);

  if (sortedMerges.length === 0) {
    return (
      <div className={`rounded-lg p-6 border ${className}`} style={{ background: 'var(--bg-card)', borderColor: 'var(--border-primary)', height: '100%' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 16 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
            <h3 style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-primary)' }}>Active Merges</h3>
            <Link to="/merges" state={{ from: { path: '/overview', label: 'Overview' } }} title="Merges" style={{ fontSize: 11, color: 'var(--text-muted)', textDecoration: 'none', display: 'flex', alignItems: 'center', gap: 3 }}>
              <span>→</span>
            </Link>
          </div>
        </div>
        <p style={{ fontSize: 11, textAlign: 'center', padding: '16px 0', color: 'var(--text-muted)' }}>No merges currently running</p>
      </div>
    );
  }

  return (
    <div className={`rounded-lg border overflow-hidden ${className}`} style={{ background: 'var(--bg-card)', borderColor: 'var(--border-primary)', height: '100%', display: 'flex', flexDirection: 'column' }}>
      <div className="px-4 py-3 border-b" style={{ borderColor: 'var(--border-secondary)', flexShrink: 0 }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '6px 4px' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
            <h3 style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-primary)' }}>
              Active Merges <span style={{ fontWeight: 400, color: 'var(--text-muted)' }}>({sortedMerges.length})</span>
            </h3>
            <Link to="/merges" state={{ from: { path: '/overview', label: 'Overview' } }} title="Merges" style={{ fontSize: 11, color: 'var(--text-muted)', textDecoration: 'none', display: 'flex', alignItems: 'center', gap: 3 }}>
              <span>→</span>
            </Link>
          </div>
          <CopyTableButton
            headers={['Table', 'Part', 'Elapsed', 'Memory', 'Read', 'Write', 'Rows', 'Parts', 'Progress']}
            rows={sortedMerges.map(m => [
              `${m.database}.${m.table}`, m.partName, formatElapsed(m.elapsed),
              formatBytes(m.memoryUsage), formatBytesPerSec(m.readBytesPerSec),
              formatBytesPerSec(m.writeBytesPerSec), formatNumber(m.rowsRead),
              `${m.numParts}→1`, `${(m.progress * 100).toFixed(1)}%`,
            ])}
          />
        </div>
      </div>
      <div style={{ flex: 1, maxHeight: `${maxHeight}px`, overflowY: 'auto', overflowX: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11, tableLayout: 'fixed' }}>
          <thead className="sticky top-0 z-10" style={{ background: 'var(--bg-card)' }}>
            <tr style={{ borderBottom: '1px solid var(--border-secondary)' }}>
              <SortableHeader label="Table" sortKey="table" activeSortKey={sort.key} direction={sort.direction} onSort={toggleSort as (k: string) => void} />
              <th style={{ padding: '6px 8px', textAlign: 'left', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10, width: 90 }}>Part</th>
              <SortableHeader label="Elapsed" sortKey="elapsed" activeSortKey={sort.key} direction={sort.direction} onSort={toggleSort as (k: string) => void} align="right" width={52} />
              <SortableHeader label="Memory" sortKey="memory" activeSortKey={sort.key} direction={sort.direction} onSort={toggleSort as (k: string) => void} align="right" width={62} />
              <SortableHeader label="Read" sortKey="read" activeSortKey={sort.key} direction={sort.direction} onSort={toggleSort as (k: string) => void} align="right" width={68} />
              <SortableHeader label="Write" sortKey="write" activeSortKey={sort.key} direction={sort.direction} onSort={toggleSort as (k: string) => void} align="right" width={68} />
              <SortableHeader label="Rows" sortKey="rows" activeSortKey={sort.key} direction={sort.direction} onSort={toggleSort as (k: string) => void} align="right" width={52} />
              <SortableHeader label="Parts" sortKey="parts" activeSortKey={sort.key} direction={sort.direction} onSort={toggleSort as (k: string) => void} align="center" width={44} />
              <SortableHeader label="" sortKey="progress" activeSortKey={sort.key} direction={sort.direction} onSort={toggleSort as (k: string) => void} align="center" width={36} />
            </tr>
          </thead>
          <tbody>
            {sortedMerges.map((merge, index) => (
              <tr
                key={`${merge.database}.${merge.table}.${merge.partName}-${index}`}
                style={{ 
                  borderBottom: '1px solid var(--border-secondary)',
                  background: index % 2 === 0 ? 'transparent' : 'var(--bg-tertiary)',
                }}
              >
                <td style={{ padding: '5px 8px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={`${merge.database}.${merge.table}`}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 4, minWidth: 0 }}>
                    {merge.isMutation && (
                      <span style={{ padding: '1px 4px', fontSize: 9, fontWeight: 500, borderRadius: 3, backgroundColor: `${OVERVIEW_COLORS.mutations}20`, color: OVERVIEW_COLORS.mutations, flexShrink: 0 }}>
                        MUT
                      </span>
                    )}
                    {!merge.isMutation && merge.mergeType.startsWith('TTL') && (
                      <span style={{ padding: '1px 4px', fontSize: 9, fontWeight: 500, borderRadius: 3, backgroundColor: '#f9731620', color: '#f97316', flexShrink: 0 }}>
                        TTL
                      </span>
                    )}
                    <span style={{ fontFamily: 'monospace', color: 'var(--text-secondary)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                      {merge.database}.<span style={{ color: 'var(--text-primary)' }}>{merge.table}</span>
                    </span>
                  </div>
                </td>
                <td style={{ padding: '5px 8px', fontFamily: 'monospace', color: 'var(--text-muted)', maxWidth: 140, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={merge.partName}>
                  {merge.partName}
                </td>
                <td style={{ padding: '5px 8px', textAlign: 'right', fontFamily: 'monospace', color: 'var(--text-primary)' }}>
                  {formatElapsed(merge.elapsed)}
                </td>
                <td style={{ padding: '5px 8px', textAlign: 'right', fontFamily: 'monospace', color: 'var(--text-muted)' }}>
                  {formatBytes(merge.memoryUsage)}
                </td>
                <td style={{ padding: '5px 8px', textAlign: 'right', fontFamily: 'monospace', color: 'var(--text-muted)' }}>
                  {formatBytesPerSec(merge.readBytesPerSec)}
                </td>
                <td style={{ padding: '5px 8px', textAlign: 'right', fontFamily: 'monospace', color: 'var(--text-muted)' }}>
                  {formatBytesPerSec(merge.writeBytesPerSec)}
                </td>
                <td style={{ padding: '5px 8px', textAlign: 'right', fontFamily: 'monospace', color: 'var(--text-muted)' }}>
                  {formatNumber(merge.rowsRead)}
                </td>
                <td style={{ padding: '5px 8px', textAlign: 'center' }}>
                  <span
                    style={{
                      padding: '2px 6px',
                      fontSize: 10,
                      fontWeight: 500,
                      borderRadius: 4,
                      backgroundColor: `${OVERVIEW_COLORS.merges}20`,
                      color: OVERVIEW_COLORS.merges,
                    }}
                  >
                    {merge.numParts}→1
                  </span>
                </td>
                <td style={{ padding: '5px 4px', textAlign: 'center' }}>
                  <ProgressRing
                    pct={merge.progress * 100}
                    size={20}
                    stroke={2}
                    color={OVERVIEW_COLORS.queries}
                    showPercent={false}
                  />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default ActiveMergesTable;
