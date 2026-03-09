/**
 * DictionariesTable - Dictionaries in memory
 */

import type { DictionaryInfo } from '@tracehouse/core';
import { CopyTableButton } from '../common/CopyTableButton';
import { formatBytes } from '../../utils/formatters';

interface DictionariesTableProps {
  dictionaries: DictionaryInfo[];
  className?: string;
}

function formatNumber(num: number): string {
  if (num < 1000) return num.toString();
  if (num < 1_000_000) return `${(num / 1000).toFixed(1)}K`;
  if (num < 1_000_000_000) return `${(num / 1_000_000).toFixed(1)}M`;
  return `${(num / 1_000_000_000).toFixed(1)}B`;
}

function getStatusColor(status: string): string {
  switch (status.toLowerCase()) {
    case 'loaded': return '#22c55e';
    case 'loading': return '#f59e0b';
    case 'failed': return '#ef4444';
    default: return '#94a3b8';
  }
}

export function DictionariesTable({ dictionaries, className = '' }: DictionariesTableProps) {
  // Already sorted by bytesAllocated descending from service
  const maxHeight = 10 * 48; // 10 rows max (slightly taller rows due to update timestamp)

  return (
    <div 
      className={`rounded-lg border overflow-hidden ${className}`}
      style={{ background: 'var(--bg-card)', borderColor: 'var(--border-primary)' }}
    >
      <div style={{ padding: '12px 16px', borderBottom: '1px solid var(--border-secondary)', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
        <h3 style={{ fontSize: 11, fontWeight: 500, color: 'var(--text-secondary)' }}>
          Dictionaries in Memory
          {dictionaries.length > 0 && (
            <span style={{ marginLeft: 8, color: 'var(--text-muted)' }}>({dictionaries.length})</span>
          )}
        </h3>
        {dictionaries.length > 0 && (
          <CopyTableButton
            headers={['Name', 'Type', 'Memory', 'Elements', 'Load Factor', 'Status']}
            rows={dictionaries.map(d => [
              d.name, d.type, formatBytes(d.bytesAllocated),
              formatNumber(d.elementCount), `${(d.loadFactor * 100).toFixed(1)}%`, d.loadingStatus,
            ])}
          />
        )}
      </div>

      {dictionaries.length === 0 ? (
        <div style={{ padding: 16, fontSize: 11, textAlign: 'center', color: 'var(--text-muted)' }}>
          No dictionaries loaded
        </div>
      ) : (
        <div style={{ maxHeight: `${maxHeight}px`, overflowY: 'auto', overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11 }}>
            <thead className="sticky top-0 z-10" style={{ background: 'var(--bg-card)' }}>
              <tr style={{ borderBottom: '1px solid var(--border-secondary)' }}>
                <th style={{ padding: '6px 8px', textAlign: 'left', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10 }}>Name</th>
                <th style={{ padding: '6px 8px', textAlign: 'left', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10 }}>Type</th>
                <th style={{ padding: '6px 8px', textAlign: 'right', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10 }}>Memory</th>
                <th style={{ padding: '6px 8px', textAlign: 'right', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10 }}>Elements</th>
                <th style={{ padding: '6px 8px', textAlign: 'right', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10 }}>Load Factor</th>
                <th style={{ padding: '6px 8px', textAlign: 'center', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10 }}>Status</th>
              </tr>
            </thead>
            <tbody>
              {dictionaries.map((dict, index) => (
                <tr
                  key={`${dict.name}-${index}`}
                  style={{ 
                    borderBottom: '1px solid var(--border-secondary)',
                    background: index % 2 === 0 ? 'transparent' : 'var(--bg-tertiary)',
                  }}
                >
                  <td style={{ padding: '5px 8px' }}>
                    <span style={{ fontFamily: 'monospace', color: 'var(--text-primary)' }}>{dict.name}</span>
                    {dict.lastSuccessfulUpdate && (
                      <div style={{ fontSize: 10, marginTop: 2, color: 'var(--text-muted)' }}>
                        Updated: {new Date(dict.lastSuccessfulUpdate).toLocaleString()}
                      </div>
                    )}
                  </td>
                  <td style={{ padding: '5px 8px', color: 'var(--text-muted)' }}>
                    {dict.type}
                  </td>
                  <td style={{ padding: '5px 8px', textAlign: 'right', fontFamily: 'monospace', color: 'var(--text-primary)' }}>
                    {formatBytes(dict.bytesAllocated)}
                  </td>
                  <td style={{ padding: '5px 8px', textAlign: 'right', fontFamily: 'monospace', color: 'var(--text-muted)' }}>
                    {formatNumber(dict.elementCount)}
                  </td>
                  <td style={{ padding: '5px 8px', textAlign: 'right', fontFamily: 'monospace', color: 'var(--text-muted)' }}>
                    {(dict.loadFactor * 100).toFixed(1)}%
                  </td>
                  <td style={{ padding: '5px 8px', textAlign: 'center' }}>
                    <span
                      style={{
                        padding: '2px 6px',
                        fontSize: 10,
                        fontWeight: 500,
                        borderRadius: 4,
                        backgroundColor: `${getStatusColor(dict.loadingStatus)}20`,
                        color: getStatusColor(dict.loadingStatus),
                      }}
                    >
                      {dict.loadingStatus}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

export default DictionariesTable;
