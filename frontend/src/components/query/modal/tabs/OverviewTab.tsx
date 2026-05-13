import React from 'react';
import type { QuerySeries, QueryDetail as QueryDetailType, SubQueryInfo } from '@tracehouse/core';
import { formatBytes } from '../../../../stores/databaseStore';
import { formatDurationMs, formatMicroseconds } from '../../../../utils/formatters';
import { MetricItem } from '../../../shared/ModalWrapper';
import { SqlHighlight } from '../../../common/SqlHighlight';
import { DistributedQueryTopology } from '../shared/DistributedQueryTopology';
import type { TopologyCoordinator } from '../shared/DistributedQueryTopology';

interface OverviewTabProps {
  q: QuerySeries;
  activeQuery: QuerySeries;
  queryDetail: QueryDetailType | null;
  isSelectQuery: boolean;
  topologyCoordinator: TopologyCoordinator | null;
  subQueries: SubQueryInfo[];
  isLoadingSubQueries: boolean;
  onNavigateToQuery: (queryId: string) => void;
}

const fmtMs = formatDurationMs;
const fmtUs = formatMicroseconds;
const fmtTime = (ts: string) => new Date(ts).toLocaleString();

export const OverviewTab: React.FC<OverviewTabProps> = ({
  q, activeQuery, queryDetail, isSelectQuery,
  topologyCoordinator, subQueries, isLoadingSubQueries,
  onNavigateToQuery,
}) => {
  return (
            <>
              {/* Query ID + Status row */}
              <div style={{ display: 'grid', gridTemplateColumns: '1fr auto', gap: 16, marginBottom: 16 }}>
                <div>
                  <div style={{ fontSize: 9, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '1px', marginBottom: 6 }}>
                    Query ID
                  </div>
                  <div style={{
                    fontFamily: 'monospace',
                    fontSize: 13,
                    color: 'var(--text-secondary)',
                    background: 'var(--bg-tertiary)',
                    border: '1px solid var(--border-primary)',
                    padding: '10px 14px',
                    borderRadius: 6,
                    wordBreak: 'break-all',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'space-between',
                    gap: 12,
                  }}>
                    <span>{q.query_id}</span>
                    <button
                      onClick={() => {
                        navigator.clipboard.writeText(q.query_id);
                      }}
                      title="Copy Query ID"
                      style={{
                        background: 'transparent',
                        border: 'none',
                        cursor: 'pointer',
                        padding: 4,
                        borderRadius: 4,
                        color: 'var(--text-muted)',
                        display: 'flex',
                        alignItems: 'center',
                        flexShrink: 0,
                      }}
                      onMouseOver={(e) => e.currentTarget.style.color = 'var(--text-primary)'}
                      onMouseOut={(e) => e.currentTarget.style.color = 'var(--text-muted)'}
                    >
                      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                        <rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect>
                        <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path>
                      </svg>
                    </button>
                  </div>
                </div>
                <div>
                  <div style={{ fontSize: 9, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '1px', marginBottom: 6 }}>
                    Status
                  </div>
                  {(() => {
                    const status = (q as QuerySeries & { status?: string }).status;
                    const exCode = (q as QuerySeries & { exception_code?: number }).exception_code;
                    const exMsg = (q as QuerySeries & { exception?: string }).exception;
                    const isRunningFlag = (q as QuerySeries & { is_running?: boolean }).is_running;

                    const isFailed = status === 'ExceptionWhileProcessing' || status === 'ExceptionBeforeStart' || (exCode !== undefined && exCode !== 0) || (exMsg !== undefined && exMsg !== null && exMsg !== '');
                    const isRunning = isRunningFlag === true;

                    let displayStatus = 'Success';
                    let statusColor = 'var(--color-success)';
                    let statusBg = 'rgba(var(--color-success-rgb), 0.1)';

                    if (isRunning) {
                      displayStatus = 'Running';
                      statusColor = 'var(--color-warning)';
                      statusBg = 'rgba(var(--color-warning-rgb), 0.1)';
                    } else if (isFailed) {
                      displayStatus = 'Failed';
                      statusColor = 'var(--color-error)';
                      statusBg = 'rgba(var(--color-error-rgb), 0.1)';
                    }

                    return (
                      <div style={{
                        fontFamily: 'monospace',
                        fontSize: 13,
                        color: statusColor,
                        background: statusBg,
                        border: `1px solid ${statusColor}33`,
                        padding: '10px 14px',
                        borderRadius: 6,
                        display: 'flex',
                        alignItems: 'center',
                        gap: 8,
                      }}>
                        <span style={{
                          width: 8,
                          height: 8,
                          borderRadius: '50%',
                          background: statusColor,
                          animation: isRunning ? 'pulse 1.5s ease-in-out infinite' : 'none',
                        }} />
                        {displayStatus}
                        {exCode ? ` (${exCode})` : ''}
                      </div>
                    );
                  })()}
                </div>
              </div>

              {/* Exception message if query failed */}
              {(() => {
                const status = (q as QuerySeries & { status?: string }).status;
                const exCode = (q as QuerySeries & { exception_code?: number }).exception_code;
                const exceptionMsg = (q as QuerySeries & { exception?: string }).exception;
                const isFailed = status === 'ExceptionWhileProcessing' || status === 'ExceptionBeforeStart' || (exCode !== undefined && exCode !== 0) || (exceptionMsg !== undefined && exceptionMsg !== null && exceptionMsg !== '');
                if (!isFailed || !exceptionMsg) return null;
                return (
                  <div style={{ marginBottom: 20 }}>
                    <div style={{ fontSize: 9, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '1px', marginBottom: 6 }}>
                      Error
                    </div>
                    <div style={{
                      padding: '12px 14px',
                      borderRadius: 6,
                      background: 'rgba(var(--color-error-rgb), 0.08)',
                      border: '1px solid rgba(var(--color-error-rgb), 0.2)',
                    }}>
                      <pre style={{
                        margin: 0,
                        fontFamily: 'monospace',
                        fontSize: 12,
                        color: 'var(--color-error)',
                        whiteSpace: 'pre-wrap',
                        wordBreak: 'break-word',
                      }}>
                        {exceptionMsg}
                      </pre>
                    </div>
                  </div>
                );
              })()}

              {/* SQL */}
              <div style={{ marginBottom: 20 }}>
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 6 }}>
                  <div style={{ fontSize: 9, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '1px' }}>
                    SQL
                  </div>
                  <div style={{ display: 'flex', gap: 4 }}>
                    <button
                      onClick={() => {
                        const sqlText = queryDetail?.query || queryDetail?.formatted_query || q.label || '';
                        navigator.clipboard.writeText(sqlText);
                      }}
                      title="Copy SQL"
                      style={{
                        background: 'transparent',
                        border: 'none',
                        cursor: 'pointer',
                        padding: 4,
                        borderRadius: 4,
                        color: 'var(--text-muted)',
                        display: 'flex',
                        alignItems: 'center',
                      }}
                      onMouseOver={(e) => e.currentTarget.style.color = 'var(--text-primary)'}
                      onMouseOut={(e) => e.currentTarget.style.color = 'var(--text-muted)'}
                    >
                      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                        <rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect>
                        <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path>
                      </svg>
                    </button>
                    <button
                      onClick={isSelectQuery ? () => {
                        const sqlText = queryDetail?.query || queryDetail?.formatted_query || q.label || '';
                        const encoded = btoa(unescape(encodeURIComponent(sqlText)));
                        window.location.hash = `#/analytics?tab=misc&sql=b64:${encoded}&noAutoExecute=1&from=queries`;
                      } : undefined}
                      disabled={!isSelectQuery}
                      title={isSelectQuery ? "Open in Query Explorer (without executing)" : "Only SELECT queries can be opened in Query Explorer (non-SELECT queries are not safe to re-execute)"}
                      style={{
                        background: 'transparent',
                        border: 'none',
                        cursor: isSelectQuery ? 'pointer' : 'not-allowed',
                        padding: 4,
                        borderRadius: 4,
                        color: 'var(--text-muted)',
                        opacity: isSelectQuery ? 1 : 0.4,
                        display: 'flex',
                        alignItems: 'center',
                      }}
                      onMouseOver={(e) => { if (isSelectQuery) e.currentTarget.style.color = 'var(--text-primary)'; }}
                      onMouseOut={(e) => { if (isSelectQuery) e.currentTarget.style.color = 'var(--text-muted)'; }}
                    >
                      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                        <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"></path>
                        <polyline points="15 3 21 3 21 9"></polyline>
                        <line x1="10" y1="14" x2="21" y2="3"></line>
                      </svg>
                    </button>
                  </div>
                </div>
                <div style={{
                  borderRadius: 8,
                  background: 'var(--bg-code)',
                  border: '1px solid var(--border-primary)',
                }}>
                  <SqlHighlight maxHeight={200} style={{
                    padding: 14,
                    fontSize: 12,
                    lineHeight: 1.5,
                    color: 'var(--text-secondary)',
                  }}>
                    {queryDetail?.query || queryDetail?.formatted_query || q.label || '-- no query text available'}
                  </SqlHighlight>
                </div>
              </div>

              {/* Metrics Grid - all 3 columns */}
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 12, marginBottom: 12 }}>
                <MetricItem label="User" value={q.user} />
                <MetricItem label="Duration" value={fmtMs(q.duration_ms)} />
                <MetricItem label="Peak Memory" value={formatBytes(q.peak_memory)} />
              </div>

              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 12, marginBottom: 12 }}>
                <MetricItem label="CPU Time" value={fmtUs(q.cpu_us)} />
                <MetricItem label="Network I/O" value={formatBytes(q.net_send + q.net_recv)} />
                <MetricItem label="Disk I/O" value={formatBytes(q.disk_read + q.disk_write)} />
              </div>

              {/* Time Range */}
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 12 }}>
                <MetricItem label="Started" value={fmtTime(q.start_time)} />
                <MetricItem label="Ended" value={fmtTime(q.end_time)} />
              </div>

              {/* Distributed query / server info */}
              {queryDetail && (
                <div style={{ marginTop: 16 }}>
                  <div style={{ fontSize: 9, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '1px', marginBottom: 8 }}>
                    Origin
                  </div>
                  <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 12 }}>
                    {queryDetail.client_hostname && (
                      <MetricItem label="Client Host" value={queryDetail.client_hostname} />
                    )}
                    <MetricItem label="Role" value={
                      <span style={{ color: queryDetail.is_initial_query === 1 ? 'var(--color-info, #58a6ff)' : 'var(--color-warning, #d29922)' }}>
                        {queryDetail.is_initial_query === 1 ? 'Coordinator' : 'Shard sub-query'}
                      </span>
                    } />
                    {queryDetail.is_initial_query === 0 && queryDetail.initial_query_id && (
                      <MetricItem label="Parent Query" value={
                        <button
                          onClick={() => onNavigateToQuery(queryDetail.initial_query_id)}
                          title={`Go to parent: ${queryDetail.initial_query_id}`}
                          style={{
                            fontFamily: 'monospace', fontSize: 11, color: '#58a6ff',
                            background: 'none', border: 'none', cursor: 'pointer', padding: 0,
                            textDecoration: 'underline', textDecorationStyle: 'dotted',
                          }}
                        >
                          {queryDetail.initial_query_id.slice(0, 16)}… ↗
                        </button>
                      } />
                    )}
                    {queryDetail.is_initial_query === 0 && queryDetail.initial_address && (
                      <MetricItem label="Coordinator Address" value={queryDetail.initial_address} />
                    )}
                    {queryDetail.current_database && (
                      <MetricItem label="Database" value={queryDetail.current_database} />
                    )}
                  </div>
                </div>
              )}

              {/* Distributed query topology (Gantt view) */}
              {topologyCoordinator && subQueries.length > 0 && (
                <div style={{ marginTop: 16 }}>
                  <DistributedQueryTopology
                    coordinator={topologyCoordinator}
                    subQueries={subQueries}
                    activeQueryId={activeQuery!.query_id}
                    onNavigate={onNavigateToQuery}
                  />
                </div>
              )}
              {isLoadingSubQueries && (
                <div style={{ marginTop: 16, fontSize: 11, color: 'var(--text-muted)' }}>Loading topology…</div>
              )}

              {/* Index Selectivity - shows how well the primary key pruned data */}
              {queryDetail?.ProfileEvents && (() => {
                const pe = queryDetail.ProfileEvents;
                const selectedParts = pe['SelectedParts'] || 0;
                const selectedPartsTotal = pe['SelectedPartsTotal'] || 0;
                const selectedMarks = pe['SelectedMarks'] || 0;
                const selectedMarksTotal = pe['SelectedMarksTotal'] || 0;

                const partsSelectivity = selectedPartsTotal > 0 ? (selectedParts / selectedPartsTotal) * 100 : null;
                const marksSelectivity = selectedMarksTotal > 0 ? (selectedMarks / selectedMarksTotal) * 100 : null;

                // Color: lower is better (more pruning)
                const getSelectivityColor = (pct: number) =>
                  pct <= 10 ? 'var(--color-success)' : pct <= 50 ? 'var(--color-warning)' : 'var(--color-error)';

                if (partsSelectivity === null && marksSelectivity === null) return null;

                return (
                  <div style={{ marginTop: 16 }}>
                    <div style={{ fontSize: 9, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '1px', marginBottom: 8 }}>
                      Index Selectivity
                    </div>
                    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: 12 }}>
                      {partsSelectivity !== null && (
                        <div style={{
                          background: 'var(--bg-card)',
                          border: '1px solid var(--border-secondary)',
                          borderRadius: 6,
                          padding: '8px 12px',
                        }}>
                          <div style={{ fontSize: 9, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '1px', marginBottom: 4 }}>Parts Scanned</div>
                          <div style={{ display: 'flex', alignItems: 'baseline', gap: 8 }}>
                            <div style={{ fontSize: 14, fontWeight: 500, color: getSelectivityColor(partsSelectivity), fontFamily: 'monospace' }}>
                              {partsSelectivity.toFixed(1)}%
                            </div>
                            <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>
                              {selectedParts.toLocaleString()} / {selectedPartsTotal.toLocaleString()}
                            </div>
                          </div>
                        </div>
                      )}
                      {marksSelectivity !== null && (
                        <div style={{
                          background: 'var(--bg-card)',
                          border: '1px solid var(--border-secondary)',
                          borderRadius: 6,
                          padding: '8px 12px',
                        }}>
                          <div style={{ fontSize: 9, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '1px', marginBottom: 4 }}>Marks Scanned</div>
                          <div style={{ display: 'flex', alignItems: 'baseline', gap: 8 }}>
                            <div style={{ fontSize: 14, fontWeight: 500, color: getSelectivityColor(marksSelectivity), fontFamily: 'monospace' }}>
                              {marksSelectivity.toFixed(1)}%
                            </div>
                            <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>
                              {selectedMarks.toLocaleString()} / {selectedMarksTotal.toLocaleString()}
                            </div>
                          </div>
                        </div>
                      )}
                    </div>
                  </div>
                );
              })()}
            </>
  );
};
