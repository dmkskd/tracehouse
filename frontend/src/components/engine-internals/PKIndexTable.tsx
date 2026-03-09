/**
 * PKIndexTable - Primary key index memory by table
 */

import type { PKIndexEntry } from '@tracehouse/core';
import { CopyTableButton } from '../common/CopyTableButton';
import { formatBytes, formatNumberCompact as formatNumber } from '../../utils/formatters';

interface PKIndexTableProps {
  entries: PKIndexEntry[];
  className?: string;
}

const BAR_COLORS = [
  '#6366f1', '#f59e0b', '#10b981', '#ef4444',
  '#3b82f6', '#ec4899', '#14b8a6', '#f97316',
  '#8b5cf6', '#22c55e', '#e11d48', '#06b6d4',
  '#a855f7', '#eab308', '#0ea5e9', '#d946ef',
  '#84cc16', '#f43f5e', '#2dd4bf', '#fb923c',
];

export function PKIndexTable({ entries, className = '' }: PKIndexTableProps) {
  const topEntries = entries.slice(0, 20);
  const totalPkMemory = topEntries.reduce((sum, e) => sum + e.pkMemory, 0);
  const visibleRowCount = 5;
  const rowHeight = 32;
  const maxHeight = visibleRowCount * rowHeight;

  return (
    <div className={`rounded-lg border overflow-hidden ${className}`} style={{ background: 'var(--bg-card)', borderColor: 'var(--border-primary)' }}>
      <div style={{ padding: '12px 16px', borderBottom: '1px solid var(--border-secondary)' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
          <h3 style={{ fontSize: 11, fontWeight: 500, color: 'var(--text-secondary)' }}>Primary Key Index by Table</h3>
          <div style={{ display: 'flex', alignItems: 'center', gap: 12, fontSize: 10 }}>
            <span style={{ color: 'var(--text-muted)' }}>
              Total: <span style={{ fontFamily: 'monospace', color: 'var(--text-primary)' }}>{formatBytes(totalPkMemory)}</span>
            </span>
            <span style={{ color: 'var(--text-muted)' }}>Top {topEntries.length}</span>
            <CopyTableButton
              headers={['Table', 'PK Memory', 'Parts', 'Rows', 'Granules']}
              rows={topEntries.map(e => [
                `${e.database}.${e.table}`, formatBytes(e.pkMemory),
                formatNumber(e.parts), formatNumber(e.rows), formatNumber(e.granules),
              ])}
            />
          </div>
        </div>
      </div>

      {topEntries.length === 0 ? (
        <div style={{ padding: 16, fontSize: 11, textAlign: 'center', color: 'var(--text-muted)' }}>
          No primary key data available
        </div>
      ) : topEntries.every(e => e.pkMemory === 0) ? (
        <div style={{ padding: 16, fontSize: 11, textAlign: 'center', color: 'var(--text-muted)' }}>
          <p>Primary key memory shows 0 for all tables.</p>
          <p style={{ marginTop: 4, fontSize: 10 }}>This is expected when <code style={{ background: 'var(--bg-tertiary)', padding: '0 4px', borderRadius: 3 }}>primary_key_lazy_load=1</code> is enabled (default in newer ClickHouse versions).</p>
        </div>
      ) : (
        <>
          {/* Stacked memory bar */}
          <div style={{ padding: '12px 16px' }}>
            <div style={{ width: '100%', height: 16, borderRadius: 4, overflow: 'hidden', display: 'flex', backgroundColor: 'var(--bg-tertiary)' }}>
              {topEntries.map((entry, idx) => {
                const widthPct = totalPkMemory > 0 ? (entry.pkMemory / totalPkMemory) * 100 : 0;
                if (widthPct < 0.5) return null;
                const color = BAR_COLORS[idx % BAR_COLORS.length];
                return (
                  <div
                    key={`${entry.database}.${entry.table}`}
                    className="group"
                    style={{ height: '100%', position: 'relative', width: `${widthPct}%`, backgroundColor: color }}
                  >
                    <div
                      className="opacity-0 group-hover:opacity-100"
                      style={{
                        position: 'absolute',
                        bottom: '100%',
                        left: '50%',
                        transform: 'translateX(-50%)',
                        marginBottom: 4,
                        padding: '4px 8px',
                        borderRadius: 4,
                        fontSize: 10,
                        whiteSpace: 'nowrap',
                        transition: 'opacity 0.15s',
                        pointerEvents: 'none',
                        zIndex: 10,
                        background: 'var(--bg-secondary)',
                        color: 'var(--text-primary)',
                        border: '1px solid var(--border-primary)',
                      }}
                    >
                      {entry.database}.{entry.table}: {formatBytes(entry.pkMemory)} ({widthPct.toFixed(1)}%)
                    </div>
                  </div>
                );
              })}
            </div>

            {/* Legend */}
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: '4px 12px', marginTop: 8 }}>
              {topEntries.filter(e => totalPkMemory > 0 && (e.pkMemory / totalPkMemory) * 100 >= 0.5).map((entry, idx) => (
                <div key={`legend-${entry.database}.${entry.table}`} style={{ display: 'flex', alignItems: 'center', gap: 4, fontSize: 10 }}>
                  <div style={{ width: 8, height: 8, borderRadius: 2, flexShrink: 0, backgroundColor: BAR_COLORS[idx % BAR_COLORS.length] }} />
                  <span style={{ color: 'var(--text-muted)' }}>{entry.table}</span>
                  <span style={{ fontFamily: 'monospace', color: 'var(--text-primary)' }}>{formatBytes(entry.pkMemory)}</span>
                </div>
              ))}
            </div>
          </div>

          {/* Table */}
          <div style={{ maxHeight: `${maxHeight}px`, overflowY: 'auto', overflowX: 'auto', borderTop: '1px solid var(--border-secondary)' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11 }}>
              <thead className="sticky top-0 z-10" style={{ background: 'var(--bg-card)' }}>
                <tr style={{ borderBottom: '1px solid var(--border-secondary)' }}>
                  <th style={{ padding: '6px 8px', textAlign: 'left', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10 }}>Table</th>
                  <th style={{ padding: '6px 8px', textAlign: 'right', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10 }}>PK Memory</th>
                  <th style={{ padding: '6px 8px', textAlign: 'right', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10 }}>Parts</th>
                  <th style={{ padding: '6px 8px', textAlign: 'right', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10 }}>Rows</th>
                  <th style={{ padding: '6px 8px', textAlign: 'right', color: 'var(--text-muted)', fontWeight: 500, fontSize: 10 }}>Granules</th>
                </tr>
              </thead>
              <tbody>
                {topEntries.map((entry, index) => (
                  <tr
                    key={`${entry.database}.${entry.table}-${index}`}
                    style={{
                      borderBottom: '1px solid var(--border-secondary)',
                      background: index % 2 === 0 ? 'transparent' : 'var(--bg-tertiary)',
                      height: rowHeight,
                    }}
                  >
                    <td style={{ padding: '5px 8px' }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                        <div style={{ width: 8, height: 8, borderRadius: 2, flexShrink: 0, backgroundColor: BAR_COLORS[index % BAR_COLORS.length] }} />
                        <span>
                          <span style={{ fontFamily: 'monospace', color: 'var(--text-muted)' }}>{entry.database}.</span>
                          <span style={{ fontFamily: 'monospace', color: 'var(--text-primary)' }}>{entry.table}</span>
                        </span>
                      </div>
                    </td>
                    <td style={{ padding: '5px 8px', textAlign: 'right', fontFamily: 'monospace', color: 'var(--text-primary)' }}>
                      {formatBytes(entry.pkMemory)}
                    </td>
                    <td style={{ padding: '5px 8px', textAlign: 'right', fontFamily: 'monospace', color: 'var(--text-muted)' }}>
                      {formatNumber(entry.parts)}
                    </td>
                    <td style={{ padding: '5px 8px', textAlign: 'right', fontFamily: 'monospace', color: 'var(--text-muted)' }}>
                      {formatNumber(entry.rows)}
                    </td>
                    <td style={{ padding: '5px 8px', textAlign: 'right', fontFamily: 'monospace', color: 'var(--text-muted)' }}>
                      {formatNumber(entry.granules)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  );
}

export default PKIndexTable;
