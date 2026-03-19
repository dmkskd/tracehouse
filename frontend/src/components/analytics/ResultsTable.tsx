/**
 * ResultsTable — shared sortable table for query results.
 *
 * Used by both DashboardViewer and QueryExplorer to render clickable,
 * sortable tables with RAG coloring and link-column support.
 */

import React from 'react';
import { formatCell } from './charts';
import { getRagColor, type RagRule } from './metaLanguage';

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
  /** RAG color rules for cells */
  ragRules?: RagRule[];
  /** Called when a link cell is clicked */
  onLinkClick?: (column: string, value: string) => void;
  /** Column whose cells are clickable for drill-down */
  drillOnColumn?: string;
  /** Called when a drill cell is clicked */
  onDrillClick?: (column: string, value: string) => void;
  /** Target query name shown as tooltip on drillable cells */
  drillIntoQuery?: string;
  /** Visual density variant */
  compact?: boolean;
}

export const ResultsTable: React.FC<ResultsTableProps> = ({
  columns, rows, sortColumn, sortDirection, onSort,
  linkOnColumn, ragRules, onLinkClick,
  drillOnColumn, onDrillClick, drillIntoQuery,
  compact = false,
}) => {
  const fontSize = compact ? 11 : 12;
  const headerFontSize = compact ? 10 : 11;
  const headerPadding = compact ? '5px 8px' : '8px 12px';
  const cellPadding = compact ? '4px 8px' : '6px 12px';
  const maxCellWidth = compact ? 200 : 400;
  const hasLinks = !!linkOnColumn && !!onLinkClick;
  const hasDrill = !!drillOnColumn && !!onDrillClick;

  return (
    <table style={{ width: 'max-content', minWidth: '100%', borderCollapse: 'collapse', fontSize }}>
      <thead>
        <tr>
          {columns.map(col => (
            <th key={col} onClick={() => onSort(col)} style={{
              position: 'sticky', top: 0, zIndex: 1, cursor: 'pointer',
              background: compact ? 'var(--bg-tertiary, var(--bg-secondary))' : 'var(--bg-tertiary)',
              padding: headerPadding, textAlign: 'left', fontWeight: 600, fontSize: headerFontSize,
              color: sortColumn === col ? 'var(--text-primary)' : 'var(--text-secondary)',
              borderBottom: '1px solid var(--border-primary)',
              whiteSpace: 'nowrap', userSelect: 'none',
            }}>
              {col}{sortColumn === col ? (sortDirection === 'asc' ? ' ↑' : ' ↓') : ''}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.map((row, i) => (
          <tr key={i}>
            {columns.map(col => {
              const isLink = hasLinks && col === linkOnColumn;
              const isDrill = hasDrill && col === drillOnColumn;
              const isClickable = isLink || isDrill;
              const cellValue = formatCell(row[col]);
              const ragColor = getRagColor(col, row[col], ragRules);
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
                }}
                title={isDrill && drillIntoQuery ? `Drill into: ${drillIntoQuery}` : undefined}
                onClick={isLink ? () => onLinkClick!(col, cellValue) : isDrill ? () => onDrillClick!(col, cellValue) : undefined}
                >{cellValue}</td>
              );
            })}
          </tr>
        ))}
      </tbody>
    </table>
  );
};
