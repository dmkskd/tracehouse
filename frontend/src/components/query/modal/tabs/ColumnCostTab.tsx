/**
 * ColumnCostTab — Per-column cost breakdown for a query.
 *
 * Two analyses:
 * 1. Client-side (returned data): Re-runs the query wrapped with byteSize() per column
 * 2. Server-side (processed data): Queries system.parts_columns for each column's on-disk size
 *
 * Both are expensive — the client-side analysis re-executes the original query,
 * and the server-side runs one query per column. The user must explicitly trigger them.
 */

import React, { useState, useCallback, useMemo, useEffect } from 'react';
import type { QueryDetail, ColumnCost, ServerColumnCost, ServerProgress } from '@tracehouse/core';
import { useClickHouseServices } from '../../../../providers/ClickHouseProvider';
import { useClusterStore } from '../../../../stores/clusterStore';

/* ------------------------------------------------------------------ */
/*  Helpers                                                            */
/* ------------------------------------------------------------------ */

const fmtBytes = (b: number): string => {
  if (b === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.min(Math.floor(Math.log2(b) / 10), units.length - 1);
  return `${(b / Math.pow(1024, i)).toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
};

/* ------------------------------------------------------------------ */
/*  Styles                                                             */
/* ------------------------------------------------------------------ */

const CARD: React.CSSProperties = {
  background: 'var(--bg-card)',
  border: '1px solid var(--border-secondary)',
  borderRadius: 10,
  padding: '18px 22px',
};

const WARNING_BOX: React.CSSProperties = {
  background: 'rgba(210, 153, 34, 0.08)',
  border: '1px solid rgba(210, 153, 34, 0.25)',
  borderRadius: 8,
  padding: '12px 16px',
  marginBottom: 20,
  display: 'flex',
  gap: 10,
  alignItems: 'flex-start',
  fontSize: 12,
  color: 'var(--text-secondary)',
  lineHeight: 1.5,
};

const RUN_BTN: React.CSSProperties = {
  fontFamily: 'monospace',
  fontSize: 11,
  padding: '8px 18px',
  borderRadius: 6,
  border: '1px solid rgba(88, 166, 255, 0.3)',
  background: 'rgba(88, 166, 255, 0.1)',
  color: '#58a6ff',
  cursor: 'pointer',
  letterSpacing: '0.5px',
  transition: 'all 0.15s ease',
};

const RUN_BTN_DISABLED: React.CSSProperties = {
  ...RUN_BTN,
  opacity: 0.5,
  cursor: 'not-allowed',
};

const BAR_CONTAINER: React.CSSProperties = {
  width: '100%',
  height: 16,
  background: 'var(--bg-tertiary)',
  borderRadius: 4,
  overflow: 'hidden',
};

/* ------------------------------------------------------------------ */
/*  Bar chart row                                                      */
/* ------------------------------------------------------------------ */

const CostRow: React.FC<{
  column: string;
  primary: string;
  secondary?: string;
  pct: number;
  color: string;
}> = ({ column, primary, secondary, pct, color }) => (
  <div style={{ display: 'grid', gridTemplateColumns: '200px 1fr 100px', gap: 12, alignItems: 'center', padding: '4px 0' }}>
    <div style={{ fontFamily: 'monospace', fontSize: 12, color: 'var(--text-secondary)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={column}>
      {column}
    </div>
    <div style={BAR_CONTAINER}>
      <div style={{
        width: `${Math.max(pct, 0.5)}%`,
        height: '100%',
        background: color,
        borderRadius: 4,
        transition: 'width 0.3s ease',
      }} />
    </div>
    <div style={{ fontFamily: 'monospace', fontSize: 11, color: 'var(--text-muted)', textAlign: 'right', whiteSpace: 'nowrap' }}>
      {primary} {secondary ? <span style={{ opacity: 0.6 }}>({secondary})</span> : null}
    </div>
  </div>
);

/* ------------------------------------------------------------------ */
/*  Section header                                                     */
/* ------------------------------------------------------------------ */

const SectionHeader: React.FC<{
  title: string;
  subtitle: string;
  isRunning: boolean;
  hasResults: boolean;
  error: string | null;
  onRun: () => void;
}> = ({ title, subtitle, isRunning, hasResults, error, onRun }) => (
  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 14 }}>
    <div>
      <div style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-primary)', marginBottom: 3 }}>{title}</div>
      <div style={{ fontSize: 11, color: 'var(--text-muted)', lineHeight: 1.4 }}>{subtitle}</div>
      {error && <div style={{ fontSize: 11, color: '#f85149', marginTop: 6 }}>{error}</div>}
    </div>
    <button
      onClick={onRun}
      disabled={isRunning}
      style={isRunning ? RUN_BTN_DISABLED : RUN_BTN}
      onMouseEnter={(e) => { if (!isRunning) { e.currentTarget.style.background = 'rgba(88, 166, 255, 0.2)'; } }}
      onMouseLeave={(e) => { if (!isRunning) { e.currentTarget.style.background = 'rgba(88, 166, 255, 0.1)'; } }}
    >
      {isRunning ? 'Running...' : hasResults ? 'Re-run' : 'Run Analysis'}
    </button>
  </div>
);

/* ------------------------------------------------------------------ */
/*  Color scale                                                        */
/* ------------------------------------------------------------------ */

const costColor = (pct: number): string =>
  pct >= 50 ? '#f85149' : pct >= 25 ? '#f0883e' : pct >= 10 ? '#d29922' : pct >= 5 ? '#7ee787' : '#3fb950';

/* ------------------------------------------------------------------ */
/*  Main component                                                     */
/* ------------------------------------------------------------------ */

interface ColumnCostTabProps {
  queryDetail: QueryDetail | null;
  isLoading: boolean;
}

export const ColumnCostTab: React.FC<ColumnCostTabProps> = ({ queryDetail, isLoading }) => {
  const services = useClickHouseServices();
  const { clusterName } = useClusterStore();

  const target = useMemo(() => ({
    clusterName,
    hostname: queryDetail?.hostname,
  }), [clusterName, queryDetail?.hostname]);

  // Client-side analysis state
  const [clientCosts, setClientCosts] = useState<ColumnCost[] | null>(null);
  const [clientTotal, setClientTotal] = useState(0);
  const [isRunningClient, setIsRunningClient] = useState(false);
  const [clientError, setClientError] = useState<string | null>(null);
  const [clientDurationMs, setClientDurationMs] = useState<number | null>(null);

  // Server-side analysis state
  const [serverCosts, setServerCosts] = useState<ServerColumnCost[] | null>(null);
  const [serverTotal, setServerTotal] = useState(0);
  const [isRunningServer, setIsRunningServer] = useState(false);
  const [serverError, setServerError] = useState<string | null>(null);
  const [serverDurationMs, setServerDurationMs] = useState<number | null>(null);

  // Flush interval from server (fetched once)
  const [flushIntervalMs, setFlushIntervalMs] = useState<number | null>(null);
  useEffect(() => {
    if (!services) return;
    services.queryAnalyzer.getQueryLogFlushIntervalMs().then(setFlushIntervalMs);
  }, [services]);

  const queryText = queryDetail?.query || '';
  const isSelect = queryDetail?.query_kind?.toUpperCase() === 'SELECT';

  /* ---------------------------------------------------------------- */
  /*  Client-side: re-run query with byteSize() per column             */
  /* ---------------------------------------------------------------- */

  const runClientAnalysis = useCallback(async () => {
    if (!services || !queryText || !isSelect) return;

    setIsRunningClient(true);
    setClientError(null);
    const start = performance.now();

    try {
      const result = await services.columnCostService.runClientAnalysis(queryText, target);
      setClientCosts(result.costs);
      setClientTotal(result.total);
    } catch (err) {
      setClientError(err instanceof Error ? err.message : 'Analysis failed');
    } finally {
      setClientDurationMs(Math.round(performance.now() - start));
      setIsRunningClient(false);
    }
  }, [services, queryText, isSelect, target]);

  /* ---------------------------------------------------------------- */
  /*  Server-side: re-run query per column, read read_bytes from       */
  /*  system.query_log                                                  */
  /*                                                                    */
  /*  BUG: read_bytes is a query-level metric. For queries with GROUP   */
  /*  BY / JOIN / aggregation, wrapping the original query as a sub-    */
  /*  query forces a full execution regardless of which output column   */
  /*  is selected — so all columns report the same read_bytes.          */
  /*  Only accurate for simple SELECT … FROM table scans.               */
  /* ---------------------------------------------------------------- */

  const [serverProgress, setServerProgress] = useState<ServerProgress | null>(null);

  const runServerAnalysis = useCallback(async () => {
    if (!services || !queryText || !isSelect) return;

    setIsRunningServer(true);
    setServerError(null);
    setServerProgress(null);
    const start = performance.now();

    try {
      const result = await services.columnCostService.runServerAnalysis(queryText, {
        ...target,
        flushIntervalMs,
        onProgress: setServerProgress,
      });
      setServerCosts(result.costs);
      setServerTotal(result.total);
      setServerProgress(null);
    } catch (err) {
      setServerError(err instanceof Error ? err.message : 'Analysis failed');
    } finally {
      setServerDurationMs(Math.round(performance.now() - start));
      setIsRunningServer(false);
    }
  }, [services, queryText, isSelect, target, flushIntervalMs]);

  /* ---------------------------------------------------------------- */
  /*  Render                                                           */
  /* ---------------------------------------------------------------- */

  if (isLoading) {
    return (
      <div style={{ padding: 24, textAlign: 'center', color: 'var(--text-muted)' }}>
        Loading query details...
      </div>
    );
  }

  if (!queryDetail) {
    return (
      <div style={{ padding: 24, textAlign: 'center', color: 'var(--text-muted)' }}>
        No query detail available.
      </div>
    );
  }

  if (!isSelect) {
    return (
      <div style={{ padding: 24, textAlign: 'center', color: 'var(--text-muted)' }}>
        Column cost analysis is only available for SELECT queries.
      </div>
    );
  }

  return (
    <div style={{ padding: 22, display: 'flex', flexDirection: 'column', gap: 20 }}>
      {/* Warning banner */}
      <div style={WARNING_BOX}>
        <span style={{ fontSize: 16, flexShrink: 0, marginTop: 1 }}>&#9888;</span>
        <div>
          <span style={{
            display: 'inline-block',
            fontSize: 9,
            fontWeight: 700,
            textTransform: 'uppercase',
            letterSpacing: '1.5px',
            padding: '2px 7px',
            borderRadius: 4,
            background: 'rgba(210, 153, 34, 0.2)',
            color: '#d29922',
            marginBottom: 6,
          }}>Beta</span>
          <br />
          <strong style={{ color: '#d29922' }}>Expensive to compute — results may not always be available.</strong>
          <br /><br />
          <strong>Client-side</strong> analysis <strong>re-executes the original query</strong> to measure per-column byte sizes in the result.
          <br /><br />
          <strong>Server-side</strong> analysis <strong>re-runs the query once per output column</strong> and reads{' '}
          <code style={{ fontSize: 11, background: 'rgba(255,255,255,0.06)', padding: '1px 5px', borderRadius: 3 }}>read_bytes</code> from{' '}
          <code style={{ fontSize: 11, background: 'rgba(255,255,255,0.06)', padding: '1px 5px', borderRadius: 3 }}>system.query_log</code> to measure
          how much data the server processed for each column.
          Automatically waits for the query_log flush interval ({flushIntervalMs != null ? `${(flushIntervalMs / 1000).toFixed(1)}s` : '...'}) before reading results.
          <br /><br />
          Both are on-demand — click "Run Analysis" when ready.
        </div>
      </div>

      {/* Client-side analysis */}
      <div style={CARD}>
        <SectionHeader
          title="Returned Data (Client-side)"
          subtitle="Re-runs the query with byteSize() per output column to measure how much data each column contributes to the result set."
          isRunning={isRunningClient}
          hasResults={clientCosts !== null}
          error={clientError}
          onRun={runClientAnalysis}
        />

        {clientCosts && (
          <div>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 10, fontSize: 11, color: 'var(--text-muted)' }}>
              <span>Total returned: <strong style={{ color: 'var(--text-secondary)' }}>{fmtBytes(clientTotal)}</strong></span>
              {clientDurationMs !== null && <span>Completed in {clientDurationMs < 1000 ? `${clientDurationMs}ms` : `${(clientDurationMs / 1000).toFixed(1)}s`}</span>}
            </div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
              {clientCosts.map(c => (
                <CostRow
                  key={c.column}
                  column={c.column}
                  primary={fmtBytes(c.bytes)}
                  secondary={`${c.pct.toFixed(1)}%`}
                  pct={c.pct}
                  color={costColor(c.pct)}
                />
              ))}
            </div>
          </div>
        )}
      </div>

      {/* Server-side analysis */}
      <div style={CARD}>
        <SectionHeader
          title="Processed Data (Server-side)"
          subtitle="Re-runs the query once per output column and reads read_bytes from system.query_log. Results may be inaccurate for queries with GROUP BY, JOIN, or aggregations (all columns may show equal cost)."
          isRunning={isRunningServer}
          hasResults={serverCosts !== null}
          error={serverError}
          onRun={runServerAnalysis}
        />

        {serverProgress && (
          <div style={{ fontSize: 11, color: 'var(--text-muted)', marginBottom: 8 }}>
            {serverProgress.flushCountdown != null ? (
              <>
                Waiting for query_log flush — <span style={{ color: '#58a6ff', fontFamily: 'monospace' }}>{serverProgress.flushCountdown}s</span> remaining
                {serverProgress.flushIntervalMs != null && (
                  <span style={{ opacity: 0.6 }}> (server flush interval: {(serverProgress.flushIntervalMs / 1000).toFixed(1)}s)</span>
                )}
                <div style={{ ...BAR_CONTAINER, height: 4, marginTop: 6 }}>
                  <div style={{
                    width: `${100 - (serverProgress.flushCountdown / Math.ceil((serverProgress.flushIntervalMs ?? 7500) / 1000)) * 100}%`,
                    height: '100%',
                    background: '#58a6ff',
                    borderRadius: 4,
                    transition: 'width 1s linear',
                  }} />
                </div>
              </>
            ) : serverProgress.completed < serverProgress.total ? (
              <>
                Running column {serverProgress.completed + 1}/{serverProgress.total}: <span style={{ color: 'var(--text-secondary)', fontFamily: 'monospace' }}>{serverProgress.currentColumn}</span>
                <div style={{ ...BAR_CONTAINER, height: 4, marginTop: 6 }}>
                  <div style={{
                    width: `${(serverProgress.completed / serverProgress.total) * 100}%`,
                    height: '100%',
                    background: '#58a6ff',
                    borderRadius: 4,
                    transition: 'width 0.3s ease',
                  }} />
                </div>
              </>
            ) : (
              <>
                <span style={{ color: 'var(--text-secondary)', fontFamily: 'monospace' }}>{serverProgress.currentColumn}</span>
              </>
            )}
          </div>
        )}

        {serverCosts && (
          <div>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 10, fontSize: 11, color: 'var(--text-muted)' }}>
              <span>Total read: <strong style={{ color: 'var(--text-secondary)' }}>{fmtBytes(serverTotal)}</strong></span>
              {serverDurationMs !== null && <span>Completed in {serverDurationMs < 1000 ? `${serverDurationMs}ms` : `${(serverDurationMs / 1000).toFixed(1)}s`}</span>}
            </div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
              {serverCosts.map(c => (
                <CostRow
                  key={c.column}
                  column={c.column}
                  primary={fmtBytes(c.readBytes)}
                  secondary={`${c.pct.toFixed(1)}%`}
                  pct={c.pct}
                  color={costColor(c.pct)}
                />
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default ColumnCostTab;
