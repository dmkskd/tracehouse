import { useState } from 'react';
import { Link } from 'react-router-dom';
import { ResultsTable } from '../../components/analytics/ResultsTable';
import { formatCell, sortRows } from '../../components/analytics/charts';
import { formatBytes } from '../../utils/formatters';
import { rowMatchesKey, type NotebookEvidence, type NotebookCell } from './model';

export function NotebookTable({ cell, evidence }: { cell: NotebookCell; evidence: NotebookEvidence }) {
  const [sortColumn, setSortColumn] = useState(cell.encoding.rankBy ?? null);
  const [direction, setDirection] = useState<'asc' | 'desc'>('desc');
  const columns = cell.columns ?? evidence.columns.map(field => ({ field, label: field.replaceAll('_', ' ') }));
  const rows = sortColumn ? sortRows(evidence.rows, sortColumn, direction) : evidence.rows;
  return <div style={{ overflowX: 'auto' }}>
    <ResultsTable
      columns={columns.map(column => column.field)}
      columnLabels={Object.fromEntries(columns.map(column => [column.field, column.label]))}
      detailColumns={Array.from(new Set([...evidence.columns, ...evidence.rows.flatMap(row => Object.keys(row))]))}
      rows={rows}
      sortColumn={sortColumn}
      sortDirection={direction}
      onSort={column => {
        if (sortColumn === column) setDirection(value => value === 'asc' ? 'desc' : 'asc');
        else { setSortColumn(column); setDirection('desc'); }
      }}
      isRowHighlighted={row => Boolean(cell.highlight?.rowKey && rowMatchesKey(row, cell.highlight.rowKey))}
      renderCell={(field, row) => {
        const column = cell.columns?.find(column => column.field === field);
        const value = row[field];
        if (column?.type === 'query' && typeof value === 'string' && value) {
          return <Link to={`/queries?${new URLSearchParams({ qd_id: value })}`}
            target="_blank" rel="noopener noreferrer" title={`Open query details: ${value}`}
            onClick={event => event.stopPropagation()} onKeyDown={event => event.stopPropagation()}
            style={{ color: 'var(--accent-primary)', fontFamily: 'monospace' }}>{value.slice(0, 8)}… ↗</Link>;
        }
        if (column?.type === 'bytes' && value !== null && value !== '' && Number.isFinite(Number(value))) return formatBytes(Number(value));
        if (column?.type === 'timestamp' && typeof value === 'string' && Number.isFinite(Date.parse(value))) {
          return <span title={value}>{new Date(value).toISOString().slice(11, 23)}</span>;
        }
        if (column?.type === 'text' && typeof value === 'string' && value.length <= 100) return <span style={{ whiteSpace: 'normal' }}>{value}</span>;
        if (column?.type === 'sql') return <code title={String(value ?? '')}>{String(value ?? '').replace(/\/\*[\s\S]*?\*\//g, '').replace(/\s+/g, ' ').trim()}</code>;
        return formatCell(value, field);
      }}
      compact enableRowDetails
    />
  </div>;
}
