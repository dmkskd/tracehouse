/**
 * LinkQueryModal — bridges the @link directive to QueryDetailModal.
 *
 * When a @link column is clicked, this component runs the target preset query
 * to fetch matching rows (which must include a `query_id` column), then opens
 * the full QueryDetailModal so the user gets the same deep-dive experience as
 * the Query tab (history, flamegraph, logs, threads, etc.).
 *
 * If the target query returns multiple rows the user picks one; if it returns
 * exactly one we open the detail modal immediately.
 */

import React, { useEffect, useState, useMemo, useCallback } from 'react';
import { createPortal } from 'react-dom';
import { useClickHouseServices } from '../../providers/ClickHouseProvider';
import { type Query } from './types';
import { resolveTimeRange, resolveDrillParams } from './templateResolution';
import { getRagColor } from './metaLanguage';
import { QueryDetailModal } from '../query/QueryDetailModal';
import { type QuerySeries, parseTimeValue } from '@tracehouse/core';
import { formatCell, isNumericValue, extractNumeric } from './charts';

export interface LinkQueryModalProps {
  /** The target preset query to run */
  targetQuery: Query;
  /** Column → value pairs to inject as drill params */
  params: Record<string, string>;
  /** Parent query's existing drill params to carry forward */
  parentDrillParams?: Record<string, string>;
  /** Called when the modal should close */
  onClose: () => void;
}

interface QueryRow {
  query_id?: string;
  event_time?: string;
  query_duration_ms?: number;
  memory_usage?: number;
  read_rows?: number;
  read_bytes?: number;
  query?: string;
  query_text?: string;
  user?: string;
  type?: string;
  [key: string]: unknown;
}

/** Build a QuerySeries from a query_log row so QueryDetailModal can take over. */
function rowToQuerySeries(row: QueryRow): QuerySeries {
  const durationMs = Number(row.query_duration_ms ?? 0);
  const { timeMs, timeStr: startTime } = parseTimeValue(row.event_time ?? new Date().toISOString());
  const endTime = new Date(timeMs + durationMs).toISOString();

  return {
    query_id: String(row.query_id ?? ''),
    label: String(row.query ?? row.query_text ?? ''),
    user: String(row.user ?? 'default'),
    start_time: startTime,
    end_time: endTime,
    duration_ms: durationMs,
    peak_memory: Number(row.memory_usage ?? 0),
    cpu_us: 0,
    net_send: 0,
    net_recv: 0,
    disk_read: Number(row.read_bytes ?? 0),
    disk_write: 0,
    status: String(row.type ?? 'QueryFinish'),
    points: [],
  };
}

export const LinkQueryModal: React.FC<LinkQueryModalProps> = ({
  targetQuery,
  params,
  parentDrillParams,
  onClose,
}) => {
  const services = useClickHouseServices();
  const [rows, setRows] = useState<QueryRow[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [selectedQuery, setSelectedQuery] = useState<QuerySeries | null>(null);
  const [sortCol, setSortCol] = useState<string | null>(null);
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc');

  // Run the target query to get matching rows
  useEffect(() => {
    if (!services) return;
    let cancelled = false;

    const run = async () => {
      setLoading(true);
      setError(null);
      try {
        let sql = resolveTimeRange(targetQuery.sql, targetQuery.directives.meta?.interval);
        const mergedParams = { ...(parentDrillParams ?? {}), ...params };
        sql = resolveDrillParams(sql, mergedParams);
        const result = await services.adapter.executeQuery<QueryRow>(sql);
        if (cancelled) return;
        setRows(result);
        // If exactly one row with a query_id, open detail immediately
        if (result.length === 1 && result[0].query_id) {
          setSelectedQuery(rowToQuerySeries(result[0]));
        }
      } catch (err) {
        if (!cancelled) setError(err instanceof Error ? err.message : String(err));
      } finally {
        if (!cancelled) setLoading(false);
      }
    };
    run();
    return () => { cancelled = true; };
  }, [services, targetQuery, params, parentDrillParams]);

  const handleSort = useCallback((col: string) => {
    if (sortCol === col) {
      setSortDir(d => d === 'asc' ? 'desc' : 'asc');
    } else {
      setSortCol(col);
      setSortDir('desc');
    }
  }, [sortCol]);

  const columns = useMemo(() => rows.length > 0 ? Object.keys(rows[0]) : [], [rows]);

  const sortedRows = useMemo(() => {
    if (!sortCol) return rows;
    const sorted = [...rows];
    sorted.sort((a, b) => {
      const av = a[sortCol], bv = b[sortCol];
      if (isNumericValue(av) && isNumericValue(bv)) {
        return sortDir === 'asc' ? extractNumeric(av) - extractNumeric(bv) : extractNumeric(bv) - extractNumeric(av);
      }
      const as = String(av ?? ''), bs = String(bv ?? '');
      return sortDir === 'asc' ? as.localeCompare(bs) : bs.localeCompare(as);
    });
    return sorted;
  }, [rows, sortCol, sortDir]);

  // Close on Escape (only when detail modal is not open)
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && !selectedQuery) onClose();
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [onClose, selectedQuery]);

  // If the QueryDetailModal is open, render only that
  if (selectedQuery) {
    return createPortal(
      <QueryDetailModal
        query={selectedQuery}
        onClose={() => {
          // If we auto-opened (single result), close everything
          if (rows.length <= 1) {
            onClose();
          } else {
            setSelectedQuery(null);
          }
        }}
      />,
      document.body,
    );
  }

  return createPortal(
    <div
      style={{
        position: 'fixed', inset: 0, zIndex: 10000,
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        background: 'rgba(0,0,0,0.55)', backdropFilter: 'blur(4px)',
      }}
      onClick={onClose}
    >
      <div
        style={{
          background: 'var(--bg-secondary)', border: '1px solid var(--border-primary)',
          borderRadius: 12, boxShadow: '0 16px 48px rgba(0,0,0,0.35)',
          width: '90vw', maxWidth: 1200, maxHeight: '85vh',
          display: 'flex', flexDirection: 'column', overflow: 'hidden',
        }}
        onClick={e => e.stopPropagation()}
      >
        {/* Header */}
        <div style={{
          padding: '16px 20px', borderBottom: '1px solid var(--border-primary)',
          display: 'flex', alignItems: 'center', justifyContent: 'space-between',
          flexShrink: 0,
        }}>
          <div>
            <div style={{ fontSize: 14, fontWeight: 600, color: 'var(--text-primary)' }}>
              {targetQuery.name}
            </div>
            <div style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 2 }}>
              {Object.entries(params).map(([k, v]) => `${k} = ${v}`).join(', ')}
              {targetQuery.description && ` — ${targetQuery.description}`}
              {!loading && ` · ${rows.length} executions — click a row to inspect`}
            </div>
          </div>
          <button
            onClick={onClose}
            style={{
              background: 'none', border: 'none', cursor: 'pointer',
              color: 'var(--text-muted)', fontSize: 18, padding: '0 4px',
              lineHeight: 1,
            }}
          >×</button>
        </div>

        {/* Body */}
        <div style={{ flex: 1, overflow: 'auto', padding: 0 }}>
          {loading && (
            <div style={{ padding: 40, textAlign: 'center', color: 'var(--text-muted)', fontSize: 13 }}>
              Finding executions…
            </div>
          )}
          {error && (
            <div style={{ padding: 20, color: 'var(--accent-red, #f85149)', fontSize: 12, fontFamily: "'Share Tech Mono',monospace" }}>
              {error}
            </div>
          )}
          {!loading && !error && rows.length === 0 && (
            <div style={{ padding: 40, textAlign: 'center', color: 'var(--text-muted)', fontSize: 13 }}>
              No executions found in the selected time range.
            </div>
          )}
          {!loading && rows.length > 0 && (
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11, fontFamily: "'Share Tech Mono',monospace" }}>
              <thead>
                <tr>
                  {columns.map(col => (
                    <th key={col} onClick={() => handleSort(col)}
                      style={{
                        position: 'sticky', top: 0, zIndex: 1,
                        background: 'var(--bg-tertiary)', padding: '8px 12px',
                        textAlign: 'left', fontWeight: 600, fontSize: 11,
                        color: 'var(--text-secondary)', borderBottom: '1px solid var(--border-primary)',
                        cursor: 'pointer', userSelect: 'none', whiteSpace: 'nowrap',
                      }}>
                      {col}{sortCol === col ? (sortDir === 'asc' ? ' ↑' : ' ↓') : ''}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {sortedRows.map((row, i) => {
                  const hasQueryId = !!row.query_id;
                  return (
                    <tr
                      key={i}
                      style={{
                        cursor: hasQueryId ? 'pointer' : undefined,
                        transition: 'background 0.1s',
                      }}
                      onClick={hasQueryId ? () => setSelectedQuery(rowToQuerySeries(row)) : undefined}
                      onMouseEnter={e => { if (hasQueryId) (e.currentTarget as HTMLElement).style.background = 'var(--bg-hover, rgba(255,255,255,0.03))'; }}
                      onMouseLeave={e => { (e.currentTarget as HTMLElement).style.background = ''; }}
                    >
                      {columns.map(col => (
                        <td key={col} style={{
                          padding: '6px 12px', borderBottom: '1px solid var(--border-secondary)',
                          color: getRagColor(col, row[col], targetQuery.directives.cellStyles) ?? 'var(--text-secondary)',
                          fontWeight: getRagColor(col, row[col], targetQuery.directives.cellStyles) ? 600 : undefined,
                          whiteSpace: 'nowrap', maxWidth: 400, overflow: 'hidden', textOverflow: 'ellipsis',
                        }}>
                          {formatCell(row[col], col)}
                        </td>
                      ))}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          )}
        </div>
      </div>
    </div>,
    document.body,
  );
};
