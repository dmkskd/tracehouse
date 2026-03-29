/**
 * ResultsTable — shared sortable table for query results.
 *
 * Used by both DashboardViewer and QueryExplorer to render clickable,
 * sortable tables with cell decorations (RAG coloring, gauge bars, sparklines)
 * and link-column support.
 */

import React from 'react';
import { formatCell } from './charts';
import { getRagColor, type CellStyleRule, type GaugeCellStyle, type SparklineCellStyle } from './metaLanguage';
import { GaugeBar } from './GaugeBar';
import { Sparkline } from './Sparkline';

export interface ResultsTableProps {
  columns: string[];
  rows: Record<string, unknown>[];
  /** Currently sorted column (null = unsorted) */
  sortColumn: string | null;
  /** Current sort direction */
  sortDirection: 'asc' | 'desc';
  onSort: (column: string) => void;
  /** Column that renders as a clickable link */
  linkOnColumn?: string;
  /** Cell style rules (rag, gauge, sparkline) from @cell: directives */
  cellStyles?: CellStyleRule[];
  /** Called when a link cell is clicked */
  onLinkClick?: (column: string, value: string) => void;
  /** Column whose cells are clickable for drill-down */
  drillOnColumn?: string;
  /** Called when a drill cell is clicked */
  onDrillClick?: (column: string, value: string) => void;
  /** Target query name shown as tooltip on drillable cells */
  drillIntoQuery?: string;
  /** Column whose cells open a part inspector */
  partLinkOnColumn?: string;
  /** Called when a part-link cell is clicked */
  onPartLinkClick?: (column: string, value: string, row: Record<string, unknown>) => void;
  /** Visual density variant */
  compact?: boolean;
}

/** Detect whether a column holds numeric data by sampling first rows. */
function isNumericColumn(rows: Record<string, unknown>[], col: string): boolean {
  for (let i = 0; i < Math.min(rows.length, 5); i++) {
    const v = rows[i][col];
    if (v != null && v !== '') return typeof v === 'number';
  }
  return false;
}

export const ResultsTable: React.FC<ResultsTableProps> = ({
  columns, rows, sortColumn, sortDirection, onSort,
  linkOnColumn, cellStyles, onLinkClick,
  drillOnColumn, onDrillClick, drillIntoQuery,
  partLinkOnColumn, onPartLinkClick,
  compact = false,
}) => {
  const [hoveredRow, setHoveredRow] = React.useState<number | null>(null);
  const gaugeMap = React.useMemo(
    () => new Map(
      cellStyles?.filter((r): r is GaugeCellStyle => r.type === 'gauge').map(g => [g.column, g]),
    ),
    [cellStyles],
  );
  const sparklineMap = React.useMemo(
    () => new Map(
      cellStyles?.filter((r): r is SparklineCellStyle => r.type === 'sparkline').map(s => [s.column, s]),
    ),
    [cellStyles],
  );
  const fontSize = compact ? 11 : 13;
  const headerFontSize = compact ? 9 : 10;
  const headerPadding = compact ? '6px 10px' : '10px 14px';
  const cellPadding = compact ? '6px 10px' : '8px 14px';
  const maxCellWidth = compact ? 200 : 400;
  const hasLinks = !!linkOnColumn && !!onLinkClick;
  const hasDrill = !!drillOnColumn && !!onDrillClick;
  const hasPartLink = !!partLinkOnColumn && !!onPartLinkClick;
  const numericCols = React.useMemo(
    () => new Set(columns.filter(c => isNumericColumn(rows, c))),
    [columns, rows],
  );

  return (
    <table style={{ width: 'max-content', minWidth: '100%', borderCollapse: 'collapse', fontSize }}>
      <thead>
        <tr>
          {columns.map(col => (
            <th key={col} onClick={() => onSort(col)} style={{
              position: 'sticky', top: 0, zIndex: 1, cursor: 'pointer',
              background: compact ? 'var(--bg-tertiary, var(--bg-secondary))' : 'var(--bg-tertiary)',
              padding: headerPadding, textAlign: numericCols.has(col) ? 'right' : 'left', fontWeight: 600, fontSize: headerFontSize,
              color: sortColumn === col ? 'var(--text-primary)' : 'var(--text-muted)',
              borderBottom: '2px solid var(--border-primary)',
              whiteSpace: 'nowrap', userSelect: 'none',
              letterSpacing: '0.06em', textTransform: 'uppercase',
            }}>
              {col}{sortColumn === col ? (sortDirection === 'asc' ? ' ↑' : ' ↓') : ''}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.map((row, i) => (
          <tr key={i}
            onMouseEnter={() => setHoveredRow(i)}
            onMouseLeave={() => setHoveredRow(null)}
            style={{
              background: hoveredRow === i
                ? 'var(--bg-card-hover, rgba(88,166,255,0.07))'
                : i % 2 === 1 ? 'rgba(255,255,255,0.04)' : 'transparent',
              transition: 'background 0.1s ease',
            }}>
            {columns.map(col => {
              const isLink = hasLinks && col === linkOnColumn;
              const isDrill = hasDrill && col === drillOnColumn;
              const isPartLink = hasPartLink && col === partLinkOnColumn;
              const isClickable = isLink || isDrill || isPartLink;
              const cellValue = formatCell(row[col], col);
              const ragColor = getRagColor(col, row[col], cellStyles);
              const numeric = numericCols.has(col);
              const gauge = gaugeMap.get(col);
              const sparkline = sparklineMap.get(col);

              // Gauge bar rendering
              if (gauge) {
                const val = typeof row[col] === 'number' ? row[col] as number : Number(row[col]);
                const maxVal = typeof gauge.max === 'number'
                  ? gauge.max
                  : (typeof row[gauge.max] === 'number' ? row[gauge.max] as number : Number(row[gauge.max]));
                return (
                  <td key={col} style={{ padding: cellPadding, borderBottom: '1px solid var(--border-secondary)', minWidth: 140 }}>
                    <GaugeBar value={val} max={maxVal} ragColor={ragColor} unit={gauge.unit} />
                  </td>
                );
              }

              // Sparkline rendering
              if (sparkline) {
                const raw = row[col];
                const data = Array.isArray(raw) ? raw.map(Number).filter(n => !isNaN(n)) : [];
                return (
                  <td key={col} style={{ padding: cellPadding, borderBottom: '1px solid var(--border-secondary)' }}>
                    <Sparkline
                      data={data}
                      referenceValue={sparkline.ref}
                      color={sparkline.color}
                      fill={sparkline.fill}
                    />
                  </td>
                );
              }

              return (
                <td key={col} style={{
                  padding: cellPadding, borderBottom: '1px solid var(--border-secondary)',
                  color: isClickable
                    ? 'var(--accent-primary, #6366f1)'
                    : ragColor ?? 'var(--text-secondary)',
                  fontWeight: ragColor ? 600 : undefined,
                  whiteSpace: 'nowrap', maxWidth: maxCellWidth, overflow: 'hidden', textOverflow: 'ellipsis',
                  cursor: isClickable ? 'pointer' : undefined,
                  textDecoration: isClickable ? 'underline' : undefined,
                  textAlign: numeric ? 'right' : 'left',
                  fontVariantNumeric: numeric ? 'tabular-nums' : undefined,
                }}
                title={isDrill && drillIntoQuery ? `Drill into: ${drillIntoQuery}` : isPartLink ? 'Open part details' : undefined}
                onClick={isLink ? () => onLinkClick!(col, cellValue) : isDrill ? () => onDrillClick!(col, cellValue) : isPartLink ? () => onPartLinkClick!(col, cellValue, row) : undefined}
                >{cellValue}</td>
              );
            })}
          </tr>
        ))}
      </tbody>
    </table>
  );
};
