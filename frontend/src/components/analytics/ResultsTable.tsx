/**
 * ResultsTable - shared sortable table for query results.
 *
 * Used by both DashboardViewer and QueryExplorer to render clickable,
 * sortable tables with cell decorations (RAG coloring, gauge bars, sparklines)
 * and link-column support.
 */

import React from 'react';
import { formatCell } from './charts';
import { getRagColor, type CellStyleRule, type GaugeCellStyle, type SparklineCellStyle, type RadarCellStyle } from './metaLanguage';
import { GaugeBar } from './GaugeBar';
import { Sparkline } from './Sparkline';
import { RadarShape } from './RadarCell';
import { buildRadar, radarDisplayColumn } from './radarModel';
import { ModalWrapper } from '../shared/ModalWrapper';

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
  /** Allow clicking a table row to expand all row values */
  enableRowDetails?: boolean;
}

/** Detect whether a column holds numeric data by sampling first rows. */
function isNumericColumn(rows: Record<string, unknown>[], col: string): boolean {
  for (let i = 0; i < Math.min(rows.length, 5); i++) {
    const v = rows[i][col];
    if (v != null && v !== '') return typeof v === 'number';
  }
  return false;
}

function formatComplexValue(value: unknown): string {
  if (Array.isArray(value)) return value.map(v => formatCell(v)).join(', ');
  if (value == null || typeof value !== 'object') return formatCell(value);
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

export const ResultsTable: React.FC<ResultsTableProps> = ({
  columns, rows, sortColumn, sortDirection, onSort,
  linkOnColumn, cellStyles, onLinkClick,
  drillOnColumn, onDrillClick, drillIntoQuery,
  partLinkOnColumn, onPartLinkClick,
  compact = false,
  enableRowDetails = false,
}) => {
  const [hoveredRow, setHoveredRow] = React.useState<number | null>(null);
  const [selectedRow, setSelectedRow] = React.useState<number | null>(null);
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
  const radarMap = React.useMemo(
    () => new Map(
      cellStyles?.filter((r): r is RadarCellStyle => r.type === 'radar')
        .map(r => [radarDisplayColumn(r), r] as const)
        .filter((entry): entry is [string, RadarCellStyle] => Boolean(entry[0])),
    ),
    [cellStyles],
  );
  const displayColumns = React.useMemo(() => {
    const syntheticRadarColumns = [...radarMap.keys()].filter(column => !columns.includes(column));
    return [...syntheticRadarColumns, ...columns];
  }, [columns, radarMap]);
  const fontSize = compact ? 11 : 13;
  const headerFontSize = compact ? 9 : 10;
  const headerPadding = compact ? '6px 10px' : '10px 14px';
  const cellPadding = compact ? '6px 10px' : '8px 14px';
  const maxCellWidth = compact ? 200 : 400;
  const hasLinks = !!linkOnColumn && !!onLinkClick;
  const hasDrill = !!drillOnColumn && !!onDrillClick;
  const hasPartLink = !!partLinkOnColumn && !!onPartLinkClick;
  const numericCols = React.useMemo(
    () => new Set(displayColumns.filter(c => columns.includes(c) && isNumericColumn(rows, c))),
    [columns, displayColumns, rows],
  );

  React.useEffect(() => {
    setSelectedRow(null);
  }, [rows, columns]);

  const selectedRowNumber = selectedRow == null ? 0 : selectedRow + 1;
  const canGoPreviousRow = selectedRow != null && selectedRow > 0;
  const canGoNextRow = selectedRow != null && selectedRow < rows.length - 1;

  React.useEffect(() => {
    if (selectedRow == null) return undefined;
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.target instanceof HTMLInputElement || event.target instanceof HTMLTextAreaElement || event.target instanceof HTMLSelectElement) return;
      if (event.key === 'ArrowUp' || event.key === 'ArrowLeft') {
        event.preventDefault();
        setSelectedRow(rowIndex => rowIndex == null ? rowIndex : Math.max(0, rowIndex - 1));
      }
      if (event.key === 'ArrowDown' || event.key === 'ArrowRight') {
        event.preventDefault();
        setSelectedRow(rowIndex => rowIndex == null ? rowIndex : Math.min(rows.length - 1, rowIndex + 1));
      }
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [rows.length, selectedRow]);

  const renderDetailValue = React.useCallback((col: string, row: Record<string, unknown>) => {
    const ragColor = getRagColor(col, row[col], cellStyles);
    const gauge = gaugeMap.get(col);
    const sparkline = sparklineMap.get(col);
    const radar = radarMap.get(col);

    if (radar) {
      const radarData = buildRadar(radar, row);
      return (
        <div style={{ display: 'flex', alignItems: 'center', gap: 12, minHeight: compact ? 46 : 58 }}>
          <RadarShape values={radarData.values} labels={radarData.labels} color={radarData.color} size={compact ? 54 : 68} title={radarData.tooltip} />
          <span style={{ color: 'var(--text-muted)', fontSize: compact ? 10 : 11 }}>{radarData.tooltip}</span>
        </div>
      );
    }

    if (gauge) {
      const val = typeof row[col] === 'number' ? row[col] as number : Number(row[col]);
      const maxVal = typeof gauge.max === 'number'
        ? gauge.max
        : (typeof row[gauge.max] === 'number' ? row[gauge.max] as number : Number(row[gauge.max]));
      return (
        <div style={{ maxWidth: 520 }}>
          <GaugeBar value={val} max={maxVal} ragColor={ragColor} unit={gauge.unit} />
        </div>
      );
    }

    if (sparkline) {
      const raw = row[col];
      const data = Array.isArray(raw) ? raw.map(Number).filter(n => !isNaN(n)) : [];
      return (
        <div style={{ display: 'flex', alignItems: 'center', gap: 12, minHeight: 34 }}>
          <Sparkline
            data={data}
            referenceValue={sparkline.ref}
            color={sparkline.color}
            fill={sparkline.fill}
          />
          <span style={{ color: 'var(--text-muted)', fontSize: compact ? 10 : 11 }}>
            {data.length} point{data.length === 1 ? '' : 's'}
          </span>
        </div>
      );
    }

    return (
      <pre style={{
        margin: 0,
        color: ragColor ?? 'var(--text-secondary)',
        fontFamily: "'Share Tech Mono','Fira Code',monospace",
        fontSize: compact ? 11 : 12,
        fontWeight: ragColor ? 600 : undefined,
        lineHeight: 1.45,
        whiteSpace: 'pre-wrap',
        overflowWrap: 'anywhere',
      }}>
        {formatComplexValue(row[col])}
      </pre>
    );
  }, [cellStyles, compact, gaugeMap, radarMap, sparklineMap]);

  return (
    <>
      <table style={{ width: 'max-content', minWidth: '100%', borderCollapse: 'collapse', fontSize }}>
        <thead>
          <tr>
            {displayColumns.map(col => {
              const sortable = columns.includes(col);
              return (
              <th key={col} onClick={sortable ? () => onSort(col) : undefined} style={{
                position: 'sticky', top: 0, zIndex: 1, cursor: sortable ? 'pointer' : undefined,
                background: compact ? 'var(--bg-tertiary, var(--bg-secondary))' : 'var(--bg-tertiary)',
                padding: headerPadding, textAlign: numericCols.has(col) ? 'right' : 'left', fontWeight: 600, fontSize: headerFontSize,
                color: sortColumn === col ? 'var(--text-primary)' : 'var(--text-muted)',
                borderBottom: '2px solid var(--border-primary)',
                whiteSpace: 'nowrap', userSelect: 'none',
                letterSpacing: '0.06em', textTransform: 'uppercase',
              }}>
                {col}{sortable && sortColumn === col ? (sortDirection === 'asc' ? ' ↑' : ' ↓') : ''}
              </th>
              );
            })}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => {
            const isSelected = selectedRow === i;
            return (
              <tr
                key={i}
                onMouseEnter={() => setHoveredRow(i)}
                onMouseLeave={() => setHoveredRow(null)}
                onClick={enableRowDetails ? () => setSelectedRow(i) : undefined}
                onKeyDown={enableRowDetails ? (e) => {
                  if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault();
                    setSelectedRow(i);
                  }
                } : undefined}
                tabIndex={enableRowDetails ? 0 : undefined}
                aria-haspopup={enableRowDetails ? 'dialog' : undefined}
                style={{
                  background: isSelected
                    ? 'var(--bg-card-hover, rgba(88,166,255,0.09))'
                    : hoveredRow === i
                      ? 'var(--bg-card-hover, rgba(88,166,255,0.07))'
                      : i % 2 === 1 ? 'rgba(255,255,255,0.04)' : 'transparent',
                  transition: 'background 0.1s ease',
                  cursor: enableRowDetails ? 'pointer' : undefined,
                  outline: 'none',
                }}>
                {displayColumns.map(col => {
                  const isLink = hasLinks && col === linkOnColumn;
                  const isDrill = hasDrill && col === drillOnColumn;
                  const isPartLink = hasPartLink && col === partLinkOnColumn;
                  const isClickable = isLink || isDrill || isPartLink;
                  const cellValue = formatCell(row[col], col);
                  const ragColor = getRagColor(col, row[col], cellStyles);
                  const numeric = numericCols.has(col);
                  const gauge = gaugeMap.get(col);
                  const sparkline = sparklineMap.get(col);
                  const radar = radarMap.get(col);

                  if (radar) {
                    const radarData = buildRadar(radar, row);
                    return (
                      <td key={col} style={{ padding: cellPadding, borderBottom: '1px solid var(--border-secondary)', textAlign: 'center', minWidth: compact ? 64 : 80 }}>
                        <RadarShape values={radarData.values} labels={radarData.labels} color={radarData.color} size={compact ? 54 : 68} title={radarData.tooltip} />
                      </td>
                    );
                  }

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
                    title={isDrill && drillIntoQuery ? `Drill into: ${drillIntoQuery}` : isPartLink ? 'Open part details' : enableRowDetails ? 'Open row details' : undefined}
                    onClick={isLink ? (e) => { e.stopPropagation(); onLinkClick!(col, cellValue); } : isDrill ? (e) => { e.stopPropagation(); onDrillClick!(col, cellValue); } : isPartLink ? (e) => { e.stopPropagation(); onPartLinkClick!(col, cellValue, row); } : undefined}
                    >{cellValue}</td>
                  );
                })}
              </tr>
            );
          })}
        </tbody>
      </table>

      {enableRowDetails && selectedRow !== null && rows[selectedRow] && (
        <ModalWrapper
          isOpen
          onClose={() => setSelectedRow(null)}
          maxWidth={920}
          height="auto"
          maxHeight="min(78vh, 760px)"
          backdropBackground="rgba(15, 23, 42, 0.28)"
          backdropBlur={2}
        >
          <div style={{ display: 'flex', flexDirection: 'column', minHeight: 0 }}>
            <div style={{
              padding: '14px 18px',
              borderBottom: '1px solid var(--border-primary)',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              gap: 16,
              flexShrink: 0,
            }}>
              <div>
                <div style={{ fontSize: 15, fontWeight: 600, color: 'var(--text-primary)' }}>Row Details</div>
                <div style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 3 }}>
                  Row {selectedRowNumber} of {rows.length} · {displayColumns.length} column{displayColumns.length === 1 ? '' : 's'}
                </div>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                <button
                  type="button"
                  onClick={() => setSelectedRow(rowIndex => rowIndex == null ? rowIndex : Math.max(0, rowIndex - 1))}
                  disabled={!canGoPreviousRow}
                  style={{
                    background: 'transparent',
                    border: '1px solid var(--border-primary)',
                    borderRadius: 4,
                    color: canGoPreviousRow ? 'var(--text-secondary)' : 'var(--text-muted)',
                    cursor: canGoPreviousRow ? 'pointer' : 'not-allowed',
                    fontSize: 12,
                    lineHeight: 1,
                    padding: '7px 9px',
                    opacity: canGoPreviousRow ? 1 : 0.45,
                  }}
                  title="Previous row"
                >
                  ↑
                </button>
                <button
                  type="button"
                  onClick={() => setSelectedRow(rowIndex => rowIndex == null ? rowIndex : Math.min(rows.length - 1, rowIndex + 1))}
                  disabled={!canGoNextRow}
                  style={{
                    background: 'transparent',
                    border: '1px solid var(--border-primary)',
                    borderRadius: 4,
                    color: canGoNextRow ? 'var(--text-secondary)' : 'var(--text-muted)',
                    cursor: canGoNextRow ? 'pointer' : 'not-allowed',
                    fontSize: 12,
                    lineHeight: 1,
                    padding: '7px 9px',
                    opacity: canGoNextRow ? 1 : 0.45,
                  }}
                  title="Next row"
                >
                  ↓
                </button>
                <button
                  type="button"
                  onClick={() => setSelectedRow(null)}
                  style={{ background: 'transparent', border: '1px solid var(--border-primary)', borderRadius: 4, color: 'var(--text-muted)', cursor: 'pointer', fontSize: 16, lineHeight: 1, padding: '6px 9px' }}
                  title="Close row details"
                >
                  ×
                </button>
              </div>
            </div>
            <div style={{ padding: 18, overflow: 'auto', minHeight: 0, maxHeight: 'calc(min(78vh, 760px) - 74px)' }}>
              <div style={{ display: 'grid', gridTemplateColumns: 'minmax(160px, 240px) minmax(360px, 1fr)', border: '1px solid var(--border-secondary)', borderBottom: 'none', borderRadius: 6, overflow: 'hidden' }}>
                {displayColumns.map((col) => (
                  <React.Fragment key={col}>
                    <div style={{
                      padding: '10px 12px',
                      borderBottom: '1px solid var(--border-secondary)',
                      background: 'var(--bg-tertiary)',
                      color: 'var(--text-muted)',
                      fontSize: 11,
                      fontWeight: 600,
                      letterSpacing: '0.04em',
                      textTransform: 'uppercase',
                      overflowWrap: 'anywhere',
                    }}>
                      {col}
                    </div>
                    <div style={{
                      padding: '10px 12px',
                      borderBottom: '1px solid var(--border-secondary)',
                      background: 'var(--bg-card)',
                      maxHeight: sparklineMap.has(col) || gaugeMap.has(col) || radarMap.has(col) ? undefined : 260,
                      overflow: 'auto',
                    }}>
                      {renderDetailValue(col, rows[selectedRow])}
                    </div>
                  </React.Fragment>
                ))}
              </div>
            </div>
          </div>
        </ModalWrapper>
      )}
    </>
  );
};
