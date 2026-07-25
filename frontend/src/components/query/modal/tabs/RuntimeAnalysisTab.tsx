import React, { useCallback, useMemo, useState } from 'react';
import {
  EXPLAIN_ANALYZE_MIN_CLICKHOUSE_VERSION_LABEL,
  sourceTag,
  TAB_QUERIES,
  type QueryDetail,
} from '@tracehouse/core';
import { useMonitoringCapabilitiesStore } from '../../../../stores/monitoringCapabilitiesStore';
import { useConnectionStore } from '../../../../stores/connectionStore';
import { useQueryExecutionAnalysis } from '../../../../hooks/useQueryExecutionAnalysis';
import {
  AnalysisRunButton,
  ExecutionAnalysisDialog,
  ExecutionAnalysisPanel,
  PreviousExecutionSummary,
  ProcessorTimingOption,
  ResolvedQueryPreview,
} from '../../ExecutionAnalysis';
import {
  EXPLAIN_ANALYZE_DOCS_URL,
  executionAnalysisSessionKey,
  executionAnalysisSql,
  getPreviousExecutionMetrics,
} from '../../executionAnalysisModel';

interface RuntimeAnalysisTabProps {
  queryDetail: QueryDetail | null;
  isLoading: boolean;
}

const CARD: React.CSSProperties = {
  width: 'min(820px, calc(100% - 48px))',
  margin: '28px auto',
  padding: 24,
  border: '1px solid var(--border-secondary)',
  borderRadius: 10,
  background: 'var(--bg-card)',
};

/**
 * Replays a historical SELECT through EXPLAIN ANALYZE.
 *
 * Historical metrics remain untouched: this view always labels the result as
 * a new execution against current data and current connection settings.
 */
export const RuntimeAnalysisTab: React.FC<RuntimeAnalysisTabProps> = ({
  queryDetail,
  isLoading,
}) => {
  const hasExplainAnalyze = useMonitoringCapabilitiesStore(state => state.flags.hasExplainAnalyze);
  const probeStatus = useMonitoringCapabilitiesStore(state => state.probeStatus);
  const serverVersion = useMonitoringCapabilitiesStore(state => state.capabilities?.serverVersion);
  const explainAnalyzeCapability = useMonitoringCapabilitiesStore(
    state => state.capabilities?.capabilities.find(capability => capability.id === 'explain_analyze'),
  );
  const probeError = useMonitoringCapabilitiesStore(state => state.probeError);
  const activeProfileId = useConnectionStore(state => state.activeProfileId);
  const activeConnectionTimeout = useConnectionStore(
    state => state.profiles.find(profile => profile.id === state.activeProfileId)
      ?.config.send_receive_timeout,
  );
  const setConnectionFormOpen = useConnectionStore(state => state.setConnectionFormOpen);

  const [showConfirmation, setShowConfirmation] = useState(false);
  const [includeProcessorTimings, setIncludeProcessorTimings] = useState(false);
  const sql = executionAnalysisSql(queryDetail);
  const analysisSessionKey = executionAnalysisSessionKey(queryDetail);
  const {
    analyze,
    reset: resetAnalysis,
    result,
    requestDurationMs,
    isAnalyzing,
    error,
    errorCategory,
    isConnected,
  } = useQueryExecutionAnalysis(analysisSessionKey);
  const isSelect = queryDetail?.query_kind?.toUpperCase() === 'SELECT';
  const capabilityUnavailable =
    !hasExplainAnalyze && (probeStatus === 'done' || probeStatus === 'error');
  const previousExecution = getPreviousExecutionMetrics(queryDetail);

  const unavailableReason = useMemo(() => {
    if (isLoading) return 'Loading the historical query…';
    if (!queryDetail) return 'Historical query details are unavailable.';
    if (!isSelect) return 'EXPLAIN ANALYZE currently supports SELECT queries.';
    if (!sql.trim()) return 'The historical SQL text is unavailable.';
    if (probeStatus === 'probing' || probeStatus === 'idle') {
      return 'Detecting EXPLAIN ANALYZE support…';
    }
    if (probeStatus === 'error') {
      return `Could not verify the EXPLAIN ANALYZE capability${probeError ? `: ${probeError}` : '.'}`;
    }
    if (!hasExplainAnalyze) {
      return `EXPLAIN ANALYZE requires ClickHouse ${EXPLAIN_ANALYZE_MIN_CLICKHOUSE_VERSION_LABEL}+ (connected: ${serverVersion ?? 'unknown'}).`;
    }
    if (!isConnected) return 'No active ClickHouse connection.';
    return null;
  }, [
    hasExplainAnalyze,
    isLoading,
    isSelect,
    probeError,
    probeStatus,
    queryDetail,
    serverVersion,
    isConnected,
    sql,
  ]);

  const runAnalysis = useCallback(async () => {
    if (!queryDetail || unavailableReason) return;

    setShowConfirmation(false);
    await analyze({
      query: sql,
      source: sourceTag(TAB_QUERIES, 'historicalQueryExecutionAnalysis'),
      database: queryDetail.current_database,
      processors: includeProcessorTimings,
    });
  }, [
    analyze,
    includeProcessorTimings,
    queryDetail,
    sql,
    unavailableReason,
  ]);

  const confirmationDialog = showConfirmation ? (
    <ExecutionAnalysisDialog
      title="Confirm query execution?"
      message="ClickHouse will execute this historical SELECT again."
      onConfirm={runAnalysis}
      onCancel={() => setShowConfirmation(false)}
    />
  ) : null;

  if (result) {
    return (
      <>
        <ExecutionAnalysisPanel
          result={result}
          requestDurationMs={requestDurationMs}
          onAnalyzeAgain={() => setShowConfirmation(true)}
          onClose={resetAnalysis}
        />
        {confirmationDialog}
      </>
    );
  }

  return (
    <>
      <div style={{ height: '100%', overflow: 'auto' }}>
        <div style={CARD}>
          <div>
            <div style={{
              marginBottom: 7,
              color: 'var(--text-primary)',
              fontSize: 15,
              fontWeight: 650,
            }}>
              Runtime execution analysis
            </div>
            <div style={{ color: 'var(--text-secondary)', fontSize: 12, lineHeight: 1.6 }}>
              Re-run this historical SELECT with ClickHouse EXPLAIN ANALYZE to measure
              execution time, rows, bytes, memory, selectivity, and parallelism per plan stage.
            </div>
          </div>

          <div style={{
            marginTop: 18,
            padding: '12px 14px',
            border: '1px solid rgba(var(--color-warning-rgb), 0.25)',
            borderRadius: 7,
            background: 'rgba(var(--color-warning-rgb), 0.07)',
            color: 'var(--text-secondary)',
            fontSize: 11,
            lineHeight: 1.55,
          }}>
            This starts a new execution against current data using the current connection settings.
            It consumes normal query resources, quotas, and limits; streaming reads and distributed
            mode are not currently supported.
            {' '}
            <a
              href={EXPLAIN_ANALYZE_DOCS_URL}
              target="_blank"
              rel="noreferrer"
              style={{ color: 'var(--color-info)', textDecoration: 'none' }}
            >
              EXPLAIN ANALYZE documentation ↗
            </a>
          </div>

          {capabilityUnavailable && (
            <div
              role="alert"
              style={{
                marginTop: 16,
                padding: '12px 14px',
                border: '1px solid rgba(var(--color-error-rgb), 0.35)',
                borderRadius: 7,
                background: 'rgba(var(--color-error-rgb), 0.08)',
                fontSize: 11,
                lineHeight: 1.5,
              }}
            >
              <div style={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between',
                gap: 12,
                color: 'var(--color-error)',
                fontWeight: 650,
              }}>
                <span>Runtime Analysis is unavailable</span>
                <span style={{
                  padding: '2px 7px',
                  border: '1px solid currentColor',
                  borderRadius: 4,
                  fontFamily: 'monospace',
                  fontSize: 9,
                  fontWeight: 500,
                }}>
                  MISSING CAPABILITY
                </span>
              </div>
              <div style={{
                display: 'grid',
                gridTemplateColumns: '90px minmax(0, 1fr)',
                gap: '5px 12px',
                marginTop: 10,
                color: 'var(--text-secondary)',
                fontFamily: 'monospace',
                fontSize: 10,
              }}>
                <span style={{ color: 'var(--text-muted)' }}>Capability</span>
                <span>EXPLAIN ANALYZE</span>
                <span style={{ color: 'var(--text-muted)' }}>Requirement</span>
                <span>ClickHouse {EXPLAIN_ANALYZE_MIN_CLICKHOUSE_VERSION_LABEL} or later</span>
                <span style={{ color: 'var(--text-muted)' }}>Detected</span>
                <span>ClickHouse {serverVersion ?? 'version unknown'}</span>
                <span style={{ color: 'var(--text-muted)' }}>Reason</span>
                <span>
                  {probeStatus === 'error'
                    ? probeError ?? 'Capability detection failed.'
                    : explainAnalyzeCapability?.detail ?? 'The connected server version does not provide EXPLAIN ANALYZE.'}
                </span>
              </div>
            </div>
          )}

          {queryDetail && (
            <div style={{
              display: 'grid',
              gridTemplateColumns: '120px minmax(0, 1fr)',
              gap: '7px 14px',
              marginTop: 18,
              padding: '13px 14px',
              border: '1px solid var(--border-secondary)',
              borderRadius: 7,
              background: 'var(--bg-code)',
              fontFamily: 'monospace',
              fontSize: 10,
            }}>
              <span style={{ color: 'var(--text-muted)' }}>Original query</span>
              <span style={{ color: 'var(--text-secondary)', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                {queryDetail.query_id}
              </span>
              <span style={{ color: 'var(--text-muted)' }}>Database</span>
              <span style={{ color: 'var(--text-secondary)' }}>
                {queryDetail.current_database || 'default'}
              </span>
            </div>
          )}

          {previousExecution && (
            <div style={{ marginTop: 18 }}>
              <PreviousExecutionSummary metrics={previousExecution} />
            </div>
          )}

          {sql.trim() && (
            <div style={{ marginTop: 18 }}>
              <ResolvedQueryPreview query={sql} />
            </div>
          )}

          {!capabilityUnavailable && hasExplainAnalyze && (
            <div style={{ marginTop: 18 }}>
              <ProcessorTimingOption
                checked={includeProcessorTimings}
                onChange={setIncludeProcessorTimings}
              />
            </div>
          )}

          {error && (
            <div
              role="alert"
              style={{
                marginTop: 16,
                padding: '10px 12px',
                border: '1px solid rgba(var(--color-error-rgb), 0.3)',
                borderRadius: 7,
                background: 'rgba(var(--color-error-rgb), 0.08)',
                color: 'var(--color-error)',
                fontSize: 11,
                lineHeight: 1.5,
              }}
            >
              <div>{error}</div>
              {errorCategory === 'timeout' && (
                <div style={{
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'space-between',
                  gap: 16,
                  marginTop: 9,
                  paddingTop: 9,
                  borderTop: '1px solid rgba(var(--color-error-rgb), 0.2)',
                  color: 'var(--text-secondary)',
                }}>
                  <span>
                    EXPLAIN ANALYZE waits for the query to finish.
                    {activeConnectionTimeout !== undefined
                      ? ` The current Send/Recv Timeout is ${activeConnectionTimeout}s.`
                      : ''}
                    {' '}Set it above the expected query runtime under Advanced Settings.
                  </span>
                  {activeProfileId && (
                    <button
                      type="button"
                      onClick={() => setConnectionFormOpen(true, activeProfileId)}
                      style={{
                        flexShrink: 0,
                        padding: '5px 9px',
                        border: '1px solid var(--border-secondary)',
                        borderRadius: 5,
                        background: 'var(--bg-card)',
                        color: 'var(--text-primary)',
                        fontSize: 10,
                        cursor: 'pointer',
                      }}
                    >
                      Edit connection
                    </button>
                  )}
                </div>
              )}
            </div>
          )}

          {!capabilityUnavailable && (
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 16, marginTop: 20 }}>
              <div style={{ color: 'var(--text-muted)', fontSize: 10, lineHeight: 1.45 }}>
                {unavailableReason ?? 'ClickHouse will execute the query and return its measured execution plan.'}
              </div>
              <AnalysisRunButton
                onClick={() => setShowConfirmation(true)}
                isRunning={isAnalyzing}
                label="Run execution analysis"
                runningLabel="Analyzing…"
                disabled={Boolean(unavailableReason) || isAnalyzing}
                title={unavailableReason ?? 'Re-run this historical SELECT with EXPLAIN ANALYZE'}
              />
            </div>
          )}
        </div>

        {isAnalyzing && (
          <div style={{ textAlign: 'center', color: 'var(--text-muted)', fontSize: 12 }}>
            Executing the query and collecting its runtime plan…
          </div>
        )}
      </div>

      {confirmationDialog}
    </>
  );
};
