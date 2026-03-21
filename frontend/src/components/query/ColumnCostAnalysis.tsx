/**
 * ColumnCostAnalysis — Per-column cost breakdown for a query.
 *
 * Two analyses:
 * 1. Client-side (returned data): Re-runs the query wrapped with byteSize() per column
 * 2. Server-side (processed data): Queries system.parts_columns for each column's on-disk size
 *
 * Both are expensive — the client-side analysis re-executes the original query,
 * and the server-side runs one query per column. The user must explicitly trigger them.
 */

import React, { useState, useCallback, useMemo, useEffect } from 'react';
import type { QueryDetail } from '@tracehouse/core';
import { HostTargetedAdapter } from '@tracehouse/core';
import { useClickHouseServices } from '../../providers/ClickHouseProvider';
import { useClusterStore } from '../../stores/clusterStore';

/* ------------------------------------------------------------------ */
/*  Helpers                                                            */
/* ------------------------------------------------------------------ */

const fmtBytes = (b: number): string => {
  if (b === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.min(Math.floor(Math.log2(b) / 10), units.length - 1);
  return `${(b / Math.pow(1024, i)).toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
};

interface ColumnCost {
  column: string;
  bytes: number;
  pct: number;
}

interface ServerColumnCost {
  column: string;
  readBytes: number;
  pct: number;
}

/** Progress tracker for per-column server analysis */
interface ServerProgress {
  total: number;
  completed: number;
  currentColumn: string;
  /** Seconds remaining while waiting for query_log flush */
  flushCountdown?: number;
  /** The flush interval read from the server, in ms */
  flushIntervalMs?: number;
}

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

interface ColumnCostAnalysisProps {
  queryDetail: QueryDetail | null;
  isLoading: boolean;
}

export const ColumnCostAnalysis: React.FC<ColumnCostAnalysisProps> = ({ queryDetail, isLoading }) => {
  const services = useClickHouseServices();
  const { clusterName } = useClusterStore();

  // When in a cluster, route client-side queries to the host that ran the original query
  const queryAdapter = useMemo(() => {
    if (!services) return null;
    const targetHost = queryDetail?.hostname;
    if (targetHost && clusterName) {
      return new HostTargetedAdapter(services.adapter, clusterName, targetHost);
    }
    return services.adapter;
  }, [services, clusterName, queryDetail?.hostname]);

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
  /*  Shared: discover output column names of the original query        */
  /* ---------------------------------------------------------------- */

  const discoverOutputColumns = useCallback(async (): Promise<string[] | null> => {
    if (!queryAdapter || !queryText) return null;
    const stripped = queryText.replace(/;\s*$/, '');

    // Strategy 1: DESCRIBE (subquery) — returns name/type pairs
    try {
      const describeSql = `DESCRIBE (${stripped})`;
      const describeResult = await queryAdapter.executeQuery<{ name: string; type: string }>(describeSql);
      if (describeResult.length > 0) return describeResult.map(r => r.name);
    } catch { /* fall through */ }

    // Strategy 2: Run with LIMIT 1 and read result keys
    try {
      const probeSql = `SELECT * FROM (${stripped}) LIMIT 1`;
      const probeResult = await queryAdapter.executeQuery<Record<string, unknown>>(probeSql);
      if (probeResult.length > 0) {
        return Object.keys(probeResult[0]);
      }
    } catch { /* fall through */ }

    return null;
  }, [queryAdapter, queryText]);

  /* ---------------------------------------------------------------- */
  /*  Client-side: re-run query with byteSize() per column             */
  /* ---------------------------------------------------------------- */

  const runClientAnalysis = useCallback(async () => {
    if (!queryAdapter || !queryText || !isSelect) return;

    setIsRunningClient(true);
    setClientError(null);
    const start = performance.now();

    try {
      const outputColumns = await discoverOutputColumns();

      if (!outputColumns || outputColumns.length === 0) {
        setClientError('Could not determine output columns for this query.');
        return;
      }

      // Build the analysis query
      const byteSizeExprs = outputColumns.map(col => {
        const escaped = col.replace(/`/g, '\\`');
        return `sum(byteSize(\`${escaped}\`)) AS \`__bytes_${escaped}\``;
      });

      const analysisSql = `SELECT ${byteSizeExprs.join(', ')} FROM (${queryText.replace(/;\s*$/, '')})`;
      const result = await queryAdapter.executeQuery<Record<string, number>>(analysisSql);

      if (result.length > 0) {
        const row = result[0];
        let total = 0;
        const costs: ColumnCost[] = [];

        for (const col of outputColumns) {
          const bytes = Number(row[`__bytes_${col}`] || 0);
          total += bytes;
          costs.push({ column: col, bytes, pct: 0 });
        }

        // Calculate percentages
        for (const c of costs) {
          c.pct = total > 0 ? (c.bytes / total) * 100 : 0;
        }

        // Sort by bytes descending
        costs.sort((a, b) => b.bytes - a.bytes);

        setClientCosts(costs);
        setClientTotal(total);
      }
    } catch (err) {
      setClientError(err instanceof Error ? err.message : 'Analysis failed');
    } finally {
      setClientDurationMs(Math.round(performance.now() - start));
      setIsRunningClient(false);
    }
  }, [queryAdapter, queryText, isSelect, discoverOutputColumns]);

  /* ---------------------------------------------------------------- */
  /*  Server-side: re-run query per column, read read_bytes from       */
  /*  system.query_log                                                  */
  /* ---------------------------------------------------------------- */

  const [serverProgress, setServerProgress] = useState<ServerProgress | null>(null);

  const runServerAnalysis = useCallback(async () => {
    if (!queryAdapter || !services || !queryText || !isSelect) return;

    setIsRunningServer(true);
    setServerError(null);
    setServerProgress(null);
    const start = performance.now();

    try {
      const outputColumns = await discoverOutputColumns();

      if (!outputColumns || outputColumns.length === 0) {
        setServerError('Could not determine output columns for this query.');
        return;
      }

      const strippedQuery = queryText.replace(/;\s*$/, '');
      const runTag = `__ccost_${Date.now()}`;
      const columnTags: { col: string; tag: string; failed: boolean }[] = [];

      // Phase 1: Run all column queries, collecting tags for later lookup.
      // Each query selects only one column from the original query as a subquery.
      // The tag is embedded as a column alias so it's preserved in query_log.query.
      for (let i = 0; i < outputColumns.length; i++) {
        const col = outputColumns[i];
        const escaped = col.replace(/`/g, '\\`');
        setServerProgress({ total: outputColumns.length, completed: i, currentColumn: col });

        const tag = `${runTag}_${i}`;
        const analysisSql = `SELECT count() AS \`${tag}\` FROM (SELECT \`${escaped}\` FROM (${strippedQuery}))`;

        let failed = false;
        try {
          await queryAdapter.executeQuery<Record<string, number>>(analysisSql);
        } catch {
          failed = true;
        }
        columnTags.push({ col, tag, failed });
      }

      // Phase 2: Wait for query_log to flush.
      // Use the flush interval already fetched on mount (fall back to 7500ms).
      const flushMs = flushIntervalMs ?? 7500;

      const lastSuccessTag = [...columnTags].reverse().find(t => !t.failed)?.tag;
      if (lastSuccessTag) {
        // Countdown: wait the full flush interval before we start polling
        const waitSec = Math.ceil(flushMs / 1000);
        for (let remaining = waitSec; remaining > 0; remaining--) {
          setServerProgress({
            total: outputColumns.length,
            completed: outputColumns.length,
            currentColumn: `waiting for query_log flush (${remaining}s)`,
            flushCountdown: remaining,
            flushIntervalMs: flushMs,
          });
          await new Promise(r => setTimeout(r, 1000));
        }

        // Now poll until the tag appears (it should be there already in most cases)
        setServerProgress({
          total: outputColumns.length,
          completed: outputColumns.length,
          currentColumn: 'checking query_log...',
          flushIntervalMs: flushMs,
        });

        const pollStart = Date.now();
        const POLL_TIMEOUT_MS = 15_000;
        const POLL_INTERVAL_MS = 1_000;
        while (Date.now() - pollStart < POLL_TIMEOUT_MS) {
          try {
            const checkSql = `
              SELECT count() AS c
              FROM {{cluster_aware:system.query_log}}
              WHERE query LIKE '%${lastSuccessTag}%'
                AND query NOT LIKE '%system.query_log%'
                AND type = 'QueryFinish'
            `;
            const checkResult = await services.adapter.executeQuery<{ c: number }>(checkSql);
            if (checkResult.length > 0 && Number(checkResult[0].c) > 0) break;
          } catch { /* ignore */ }
          await new Promise(r => setTimeout(r, POLL_INTERVAL_MS));
        }
      }

      // Phase 3: Batch-lookup all results from query_log in one query.
      const costs: ServerColumnCost[] = [];
      const successTags = columnTags.filter(t => !t.failed);

      if (successTags.length > 0) {
        const likeConditions = successTags
          .map(t => `query LIKE '%${t.tag}%'`)
          .join(' OR ');

        const logSql = `
          SELECT query, read_bytes
          FROM {{cluster_aware:system.query_log}}
          WHERE (${likeConditions})
            AND query NOT LIKE '%system.query_log%'
            AND type = 'QueryFinish'
          ORDER BY event_time_microseconds DESC
        `;

        try {
          const logResults = await services.adapter.executeQuery<{ query: string; read_bytes: number }>(logSql);

          // Map tags back to columns
          const tagToBytes = new Map<string, number>();
          for (const row of logResults) {
            for (const t of successTags) {
              if (row.query.includes(t.tag) && !tagToBytes.has(t.tag)) {
                tagToBytes.set(t.tag, Number(row.read_bytes));
              }
            }
          }

          for (const t of columnTags) {
            costs.push({
              column: t.col,
              readBytes: t.failed ? 0 : (tagToBytes.get(t.tag) ?? 0),
              pct: 0,
            });
          }
        } catch {
          // If batch lookup fails, record all as 0
          for (const t of columnTags) {
            costs.push({ column: t.col, readBytes: 0, pct: 0 });
          }
        }
      } else {
        for (const t of columnTags) {
          costs.push({ column: t.col, readBytes: 0, pct: 0 });
        }
      }

      // Calculate percentages and sort
      let total = 0;
      for (const c of costs) total += c.readBytes;
      for (const c of costs) {
        c.pct = total > 0 ? (c.readBytes / total) * 100 : 0;
      }
      costs.sort((a, b) => b.readBytes - a.readBytes);

      setServerCosts(costs);
      setServerTotal(total);
      setServerProgress(null);
    } catch (err) {
      setServerError(err instanceof Error ? err.message : 'Analysis failed');
    } finally {
      setServerDurationMs(Math.round(performance.now() - start));
      setIsRunningServer(false);
    }
  }, [queryAdapter, services, queryText, isSelect, discoverOutputColumns, flushIntervalMs]);

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
          subtitle="Re-runs the query once per output column and reads read_bytes from system.query_log. Shows how much data the server had to process for each column."
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

export default ColumnCostAnalysis;
