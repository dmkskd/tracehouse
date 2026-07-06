import React, { useMemo } from 'react';
import type { DistributedTopology, ObjectStorageProfileSummary, QueryDetail as QueryDetailType, QuerySeries, SimilarQuery, SubQueryInfo } from '@tracehouse/core';
import { formatBytes } from '../../../../stores/databaseStore';
import { formatDurationMs, formatMicroseconds, formatNumberCompact } from '../../../../utils/formatters';
import { querySqlLineCount, querySqlText } from '../../../../utils/querySqlText';
import { SqlHighlight } from '../../../common/SqlHighlight';
import { percentile } from '../shared/chartConstants';

type OverviewTargetTab = 'sql' | 'details' | 'analytics' | 'object-storage' | 'distributed' | 'logs' | 'history' | 'pipeline' | 'xray' | 'threads' | 'flamegraph';

interface OverviewTabProps {
  q: QuerySeries;
  queryDetail: QueryDetailType | null;
  isSelectQuery: boolean;
  subQueries: SubQueryInfo[];
  distributedTopology: DistributedTopology | null;
  isLoadingSubQueries: boolean;
  similarQueries: SimilarQuery[];
  isLoadingSimilarQueries: boolean;
  objectStorageSummary: ObjectStorageProfileSummary;
  showLogsCard: boolean;
  showHistoryCard: boolean;
  showXRayCard: boolean;
  showThreadsCard: boolean;
  showFlamegraphCard: boolean;
  onOpenTab: (tab: OverviewTargetTab) => void;
  onNavigateToQuery: (queryId: string) => void;
}

const LABEL: React.CSSProperties = {
  fontSize: 9,
  color: 'var(--text-muted)',
  textTransform: 'uppercase',
  letterSpacing: '1px',
};

const PANEL: React.CSSProperties = {
  background: 'var(--bg-card)',
  border: '1px solid var(--border-secondary)',
  borderRadius: 8,
};

const fmtMs = formatDurationMs;
const fmtUs = formatMicroseconds;

function statusInfo(q: QuerySeries): { label: string; color: string; bg: string; code?: number } {
  const isFailed = q.status === 'ExceptionWhileProcessing'
    || q.status === 'ExceptionBeforeStart'
    || (q.exception_code !== undefined && q.exception_code !== 0)
    || Boolean(q.exception);
  if (q.is_running) {
    return { label: 'Running', color: 'var(--color-warning)', bg: 'rgba(var(--color-warning-rgb), 0.1)' };
  }
  if (isFailed) {
    return { label: 'Failed', color: 'var(--color-error)', bg: 'rgba(var(--color-error-rgb), 0.1)', code: q.exception_code };
  }
  return { label: 'Success', color: 'var(--color-success)', bg: 'rgba(var(--color-success-rgb), 0.1)' };
}

function shortId(id: string): string {
  return id.length > 12 ? id.slice(0, 8) : id;
}

function ratioLabel(numerator: number, denominator: number): string {
  if (denominator <= 0) return numerator > 0 ? 'no result rows' : '-';
  const ratio = numerator / denominator;
  if (ratio >= 100) return `${Math.round(ratio).toLocaleString()}:1`;
  if (ratio >= 10) return `${ratio.toFixed(1)}:1`;
  return `${ratio.toFixed(2)}:1`;
}

function percentLabel(value: number): string {
  if (!Number.isFinite(value)) return '-';
  if (value < 0.01 && value > 0) return '<0.01%';
  if (value < 10) return `${value.toFixed(2)}%`;
  return `${value.toFixed(1)}%`;
}

export const OverviewTab: React.FC<OverviewTabProps> = ({
  q,
  queryDetail,
  isSelectQuery,
  subQueries,
  distributedTopology,
  isLoadingSubQueries,
  similarQueries,
  isLoadingSimilarQueries,
  objectStorageSummary,
  showLogsCard,
  showHistoryCard,
  showXRayCard,
  showThreadsCard,
  showFlamegraphCard,
  onOpenTab,
  onNavigateToQuery,
}) => {
  const status = statusInfo(q);
  const readRows = Number(queryDetail?.read_rows ?? 0);
  const readBytes = Number(queryDetail?.read_bytes ?? q.disk_read ?? 0);
  const resultRows = Number(queryDetail?.result_rows ?? 0);
  const netBytes = Number(q.net_recv ?? 0) + Number(q.net_send ?? 0);
  const diskBytes = Number(q.disk_read ?? 0) + Number(q.disk_write ?? 0);
  const tables = queryDetail?.tables ?? [];
  const columns = queryDetail?.columns ?? [];
  const host = queryDetail?.hostname || q.hostname || 'unknown host';
  const childCount = subQueries.length || distributedTopology?.nodes.filter(node => node.role !== 'coordinator' && node.role !== 'insert_client').length || 0;
  const nodeCount = distributedTopology
    ? new Set(distributedTopology.nodes.map(node => node.hostname).filter(Boolean)).size
    : new Set(subQueries.map(sq => sq.hostname).filter(Boolean)).size;
  const displayNodeCount = nodeCount || 1;
  const hasDistributedExecution = childCount > 0;
  const queryKind = queryDetail?.query_kind || q.query_kind || 'Query';
  const db = queryDetail?.current_database || 'default';
  const overviewSql = querySqlText(q, queryDetail, 'formatted', '');
  const parentQueryId = queryDetail?.is_initial_query === 0 ? queryDetail.initial_query_id : '';
  const queryRole = queryDetail ? (queryDetail.is_initial_query === 0 ? 'worker' : 'coordinator') : undefined;

  const history = useMemo(() => {
    const durations = similarQueries
      .map(item => Number(item.query_duration_ms))
      .filter(value => Number.isFinite(value) && value >= 0)
      .sort((a, b) => a - b);
    if (durations.length === 0) return null;
    const current = Number(queryDetail?.query_duration_ms ?? q.duration_ms);
    const rank = Math.round((durations.filter(value => value <= current).length / durations.length) * 100);
    return {
      count: durations.length,
      p50: percentile(durations, 50),
      p95: percentile(durations, 95),
      rank,
    };
  }, [q.duration_ms, queryDetail?.query_duration_ms, similarQueries]);

  const executionTab: OverviewTargetTab = hasDistributedExecution ? 'distributed' : (isSelectQuery ? 'pipeline' : 'details');
  const pressureScores = {
    time: history?.p95 ? Math.min(1, q.duration_ms / Math.max(history.p95, 1)) : Math.min(1, q.duration_ms / 60_000),
    memory: Math.min(1, q.peak_memory / Math.max(q.peak_memory, 512 * 1024 * 1024)),
    cpu: q.duration_ms > 0 ? Math.min(1, (q.cpu_us / 1000) / q.duration_ms) : 0,
    io: Math.min(1, (readBytes + diskBytes + netBytes) / Math.max(readBytes + diskBytes + netBytes, 1024 * 1024 * 1024)),
    scan: readRows > 0 ? Math.min(1, readRows / Math.max(readRows, resultRows || 1)) : 0,
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, minmax(0, 1fr))', gap: 14 }}>
        <QuerySummaryPreview
          q={q}
          status={status}
          queryKind={queryKind}
          host={host}
          queryRole={queryRole}
          parentQueryId={parentQueryId}
          onOpen={() => onOpenTab('sql')}
          onNavigateToQuery={onNavigateToQuery}
        />
        <ResourcePressurePreview
          scores={pressureScores}
          durationMs={q.duration_ms}
          cpuUs={q.cpu_us}
          memoryBytes={q.peak_memory}
          ioBytes={readBytes + diskBytes + netBytes}
          onOpen={() => onOpenTab('details')}
        />
        <HistoryPreview
          history={history}
          durationMs={q.duration_ms}
          similarQueries={similarQueries}
          isLoading={isLoadingSimilarQueries}
          onOpen={() => onOpenTab('history')}
        />
        <ParallelExecutionPreview
          subQueries={subQueries}
          nodeCount={displayNodeCount}
          childCount={childCount}
          rootDurationMs={q.duration_ms}
          onOpen={() => onOpenTab(executionTab)}
        />
      </div>

      {overviewSql && (
        <SqlOverviewStrip
          sql={overviewSql}
          onOpen={() => onOpenTab('sql')}
        />
      )}

      {q.exception && (
        <div style={{
          padding: '10px 12px',
          borderRadius: 6,
          background: 'rgba(var(--color-error-rgb), 0.08)',
          border: '1px solid rgba(var(--color-error-rgb), 0.2)',
        }}>
          <div style={{ ...LABEL, marginBottom: 6, color: 'var(--color-error)' }}>Error</div>
          <pre style={{
            margin: 0,
            fontFamily: 'monospace',
            fontSize: 12,
            color: 'var(--color-error)',
            whiteSpace: 'pre-wrap',
            wordBreak: 'break-word',
          }}>
            {q.exception}
          </pre>
        </div>
      )}

      <div style={{ ...LABEL, color: 'var(--text-tertiary)', fontWeight: 700, marginTop: 4 }}>Explore</div>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: 12 }}>
        <ExploreDestinationCard
          title="SQL"
          accent="#58a6ff"
          icon={<Icon><path d="M4 7h16" /><path d="M4 12h10" /><path d="M4 17h16" /></Icon>}
          primary={queryKind.toUpperCase()}
          secondary={`${db} / ${tables.length || 0} tables / ${columns.length || 0} columns`}
          onAction={() => onOpenTab('sql')}
        >
          <ChipPreview values={[db, ...tables].filter(Boolean)} empty="query text and shape" color="#58a6ff" />
        </ExploreDestinationCard>

        <ExploreDestinationCard
          title="Details"
          accent="#a371f7"
          icon={<Icon><circle cx="12" cy="12" r="9" /><path d="M12 7v5l3 2" /></Icon>}
          primary={fmtMs(q.duration_ms)}
          secondary={`CPU ${fmtUs(q.cpu_us)} / memory ${formatBytes(q.peak_memory)}`}
          onAction={() => onOpenTab('details')}
        >
          <ProgressBar value={q.duration_ms > 0 ? ((q.cpu_us / 1000) / q.duration_ms) * 100 : 0} color="#a371f7" />
        </ExploreDestinationCard>

        <ExploreDestinationCard
          title="Analytics"
          accent="#3fb950"
          icon={<Icon><path d="M4 19V9" /><path d="M10 19V5" /><path d="M16 19v-8" /><path d="M3 19h18" /></Icon>}
          primary={`${formatNumberCompact(readRows)} -> ${formatNumberCompact(resultRows)}`}
          secondary={`scan ratio ${ratioLabel(readRows, resultRows)}`}
          onAction={() => onOpenTab('analytics')}
        >
          <ReadResultBars readRows={readRows} resultRows={resultRows} />
        </ExploreDestinationCard>

        {objectStorageSummary.hasObjectStorageIO && (
          <ExploreDestinationCard
            title="Object Storage"
            accent="#f0883e"
            icon={<Icon><path d="M5 17.5h13a4 4 0 0 0 .7-7.94 6 6 0 0 0-11.54-1.7A4.5 4.5 0 0 0 5 17.5Z" /></Icon>}
            primary={objectStoragePrimary(objectStorageSummary)}
            secondary={objectStorageSecondary(objectStorageSummary)}
            onAction={() => onOpenTab('object-storage')}
          >
            <ObjectStorageBars summary={objectStorageSummary} />
          </ExploreDestinationCard>
        )}

        {hasDistributedExecution && (
          <ExploreDestinationCard
            title="Distributed"
            accent="#d29922"
            icon={<Icon><path d="M4 7h6" /><path d="M4 17h6" /><path d="M10 7l4 5-4 5" /><path d="M14 12h6" /></Icon>}
            primary={`${displayNodeCount} ${displayNodeCount === 1 ? 'node' : 'nodes'}`}
            secondary={`${childCount} child ${childCount === 1 ? 'query' : 'queries'}${isLoadingSubQueries ? ' / loading' : ''}`}
            onAction={() => onOpenTab('distributed')}
          >
            <DotRow count={displayNodeCount} active={displayNodeCount} color="#d29922" />
          </ExploreDestinationCard>
        )}

        {showLogsCard && (
          <ExploreDestinationCard
            title="Logs"
            accent="#58a6ff"
            icon={<Icon><path d="M4 6h16" /><path d="M4 12h12" /><path d="M4 18h8" /></Icon>}
            primary={q.exception ? 'Exception' : 'Log context'}
            secondary={status.code ? `code ${status.code} / text_log` : 'text_log entries'}
            onAction={() => onOpenTab('logs')}
          >
            <ChipPreview values={['system.text_log']} empty="system.text_log" color="#58a6ff" />
          </ExploreDestinationCard>
        )}

        {showHistoryCard && (
          <ExploreDestinationCard
            title="History"
            accent="#58a6ff"
            icon={<Icon><path d="M3 12a9 9 0 1 0 3-6.7" /><path d="M3 4v5h5" /><path d="M12 7v5l3 2" /></Icon>}
            primary={history ? `p${history.rank}` : (isLoadingSimilarQueries ? 'loading' : 'no data')}
            secondary={history ? `p50 ${fmtMs(history.p50)} / p95 ${fmtMs(history.p95)}` : 'similar executions'}
            onAction={() => onOpenTab('history')}
          >
            <MiniSparkline values={similarQueries.map(item => Number(item.query_duration_ms)).filter(Number.isFinite)} color="#58a6ff" />
          </ExploreDestinationCard>
        )}

        {isSelectQuery && (
          <ExploreDestinationCard
            title="Pipeline"
            accent="#d29922"
            icon={<Icon><path d="M6 5h12" /><path d="M8 12h8" /><path d="M10 19h4" /><path d="M12 5v14" /></Icon>}
            primary="Processor plan"
            secondary="DAG / waits / throughput"
            onAction={() => onOpenTab('pipeline')}
          >
            <ChipPreview values={['DAG', 'Bars', 'Table']} empty="pipeline views" color="#d29922" />
          </ExploreDestinationCard>
        )}

        {showXRayCard && (
          <ExploreDestinationCard
            title="X-Ray"
            accent="#f0883e"
            icon={<Icon><path d="M12 3v18" /><path d="M3 12h18" /><circle cx="12" cy="12" r="4" /><circle cx="12" cy="12" r="8" /></Icon>}
            primary="Process timeline"
            secondary="CPU / memory / query threads"
            onAction={() => onOpenTab('xray')}
          >
            <ChipPreview values={['experimental']} empty="tracehouse.processes_history" color="#f0883e" />
          </ExploreDestinationCard>
        )}

        {showThreadsCard && (
          <ExploreDestinationCard
            title="Threads"
            accent="#a371f7"
            icon={<Icon><path d="M7 4v16" /><path d="M12 4v16" /><path d="M17 4v16" /><path d="M4 8h16" /><path d="M4 16h16" /></Icon>}
            primary={threadCardPrimary(queryDetail)}
            secondary="CPU / memory by thread"
            onAction={() => onOpenTab('threads')}
          >
            <ChipPreview values={['system.query_thread_log']} empty="system.query_thread_log" color="#a371f7" />
          </ExploreDestinationCard>
        )}

        {showFlamegraphCard && (
          <ExploreDestinationCard
            title="Flamegraph"
            accent="#f0883e"
            icon={<Icon><path d="M12 3c3 3 5 5.5 5 9a5 5 0 0 1-10 0c0-2 1-3.5 2.4-5.2" /><path d="M12 13c1.3 1.2 2 2.2 2 3.4a2 2 0 0 1-4 0c0-1 .5-1.9 1.2-2.8" /></Icon>}
            primary="CPU profile"
            secondary="stack samples / hotspots"
            onAction={() => onOpenTab('flamegraph')}
          >
            <ChipPreview values={['system.trace_log']} empty="system.trace_log" color="#f0883e" />
          </ExploreDestinationCard>
        )}
      </div>
    </div>
  );
};

const SqlOverviewStrip: React.FC<{
  sql: string;
  onOpen: () => void;
}> = ({ sql, onOpen }) => {
  const lineCount = querySqlLineCount(sql);
  return (
    <div
      style={{
        ...PANEL,
        border: '1px solid var(--border-primary)',
        padding: '12px 14px',
        boxShadow: 'var(--shadow-sm)',
      }}
    >
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', gap: 12, marginBottom: 8 }}>
        <div style={{ ...LABEL, color: 'var(--text-tertiary)', fontWeight: 700, fontSize: 11 }}>
          SQL preview
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <span style={{ fontFamily: 'monospace', fontSize: 11, color: 'var(--text-muted)', whiteSpace: 'nowrap' }}>
            {lineCount} {lineCount === 1 ? 'line' : 'lines'}
          </span>
          <button
            type="button"
            onClick={onOpen}
            style={{
              border: 'none',
              background: 'transparent',
              color: '#58a6ff',
              cursor: 'pointer',
              padding: 0,
              fontFamily: 'monospace',
              fontSize: 11,
            }}
          >
            SQL {'->'}
          </button>
        </div>
      </div>
      <SqlHighlight
        maxHeight={132}
        style={{
          width: '100%',
          height: 132,
          padding: '10px 12px',
          borderRadius: 6,
          border: '1px solid var(--border-secondary)',
          background: 'var(--bg-tertiary)',
          color: 'var(--text-secondary)',
          fontSize: 12,
          lineHeight: 1.55,
          boxSizing: 'border-box',
          overflow: 'auto',
        }}
      >
        {sql}
      </SqlHighlight>
    </div>
  );
};

const PreviewCard: React.FC<{
  title: string;
  action: string;
  onOpen: () => void;
  children: React.ReactNode;
}> = ({ title, action, onOpen, children }) => {
  const accent = previewAccent(title);
  return (
  <div
    role="button"
    tabIndex={0}
    onClick={onOpen}
    onKeyDown={event => {
      if (event.currentTarget === event.target && (event.key === 'Enter' || event.key === ' ')) {
        event.preventDefault();
        onOpen();
      }
    }}
    style={{
      textAlign: 'left',
      padding: 0,
      borderRadius: 8,
      border: '1px solid var(--border-primary)',
      background: 'var(--bg-card)',
      cursor: 'pointer',
      minWidth: 0,
      overflow: 'hidden',
      display: 'flex',
      flexDirection: 'column',
      boxShadow: 'var(--shadow-sm)',
      transition: 'border-color 0.15s ease, transform 0.15s ease, background 0.15s ease',
      outline: 'none',
    }}
    onMouseEnter={event => {
      event.currentTarget.style.borderColor = accent;
      event.currentTarget.style.background = 'var(--bg-card-hover)';
      event.currentTarget.style.transform = 'translateY(-1px)';
    }}
    onMouseLeave={event => {
      event.currentTarget.style.borderColor = 'var(--border-primary)';
      event.currentTarget.style.background = 'var(--bg-card)';
      event.currentTarget.style.transform = 'none';
    }}
  >
    <div style={{ padding: '12px 14px', display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', gap: 10 }}>
      <div style={{ ...LABEL, color: 'var(--text-tertiary)', fontWeight: 700, fontSize: 11 }}>{title}</div>
      <div style={{ fontFamily: 'monospace', fontSize: 10, color: '#58a6ff', whiteSpace: 'nowrap' }}>{action} {'->'}</div>
    </div>
    <div style={{ padding: '0 14px 14px', flex: 1 }}>
      {children}
    </div>
  </div>
  );
};

function previewAccent(title: string): string {
  switch (title) {
    case 'Resource Pressure':
      return '#d29922';
    case 'Runtime':
      return '#d29922';
    case 'Parallel Execution':
      return '#d29922';
    default:
      return '#58a6ff';
  }
}

const QuerySummaryPreview: React.FC<{
  q: QuerySeries;
  status: ReturnType<typeof statusInfo>;
  queryKind: string;
  host: string;
  queryRole?: string;
  parentQueryId?: string;
  onOpen: () => void;
  onNavigateToQuery: (queryId: string) => void;
}> = ({ q, status, queryKind, host, queryRole, parentQueryId, onOpen, onNavigateToQuery }) => (
  <PreviewCard title="Query Summary" action="SQL" onOpen={onOpen}>
    <div style={{ display: 'flex', flexDirection: 'column', gap: 9, minWidth: 0 }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, minWidth: 0, flexWrap: 'wrap' }}>
        <span style={{ fontFamily: 'monospace', fontSize: 15, color: '#58a6ff', fontWeight: 700 }}>{shortId(q.query_id)}</span>
        <Badge color={status.color}>{status.label.toLowerCase()}</Badge>
      </div>

      <div style={{ display: 'flex', alignItems: 'center', gap: 7, flexWrap: 'wrap' }}>
        <Badge color="#58a6ff">{queryKind.toLowerCase()}</Badge>
        {queryRole && <Badge color={queryRole === 'worker' ? '#d29922' : '#a371f7'}>{queryRole}</Badge>}
      </div>

      <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
        <SummaryFact label="user" value={q.user || '-'} />
        <SummaryFact label="host" value={host} />
      </div>

      {parentQueryId && (
        <button
          type="button"
          onClick={(event) => {
            event.stopPropagation();
            onNavigateToQuery(parentQueryId);
          }}
          style={{
            width: 'fit-content',
            padding: 0,
            border: 'none',
            background: 'transparent',
            color: '#58a6ff',
            cursor: 'pointer',
            fontFamily: 'monospace',
            fontSize: 11,
          }}
        >
          parent {shortId(parentQueryId)} {'->'}
        </button>
      )}
    </div>
  </PreviewCard>
);

const SummaryFact: React.FC<{ label: string; value: string }> = ({ label, value }) => (
  <div style={{ display: 'grid', gridTemplateColumns: '34px minmax(0, 1fr)', gap: 8, alignItems: 'baseline', minWidth: 0 }}>
    <span style={{ fontFamily: 'monospace', fontSize: 10, color: 'var(--text-muted)' }}>{label}</span>
    <span
      title={value}
      style={{
        fontFamily: 'monospace',
        fontSize: 12,
        color: 'var(--text-secondary)',
        overflow: 'hidden',
        textOverflow: 'ellipsis',
        whiteSpace: 'nowrap',
      }}
    >
      {value}
    </span>
  </div>
);

const HistoryPreview: React.FC<{
  history: { count: number; p50: number; p95: number; rank: number } | null;
  durationMs: number;
  similarQueries: SimilarQuery[];
  isLoading: boolean;
  onOpen: () => void;
}> = ({ history, durationMs, similarQueries, isLoading, onOpen }) => (
  <PreviewCard title="History" action="History" onOpen={onOpen}>
    <div style={{ fontFamily: 'monospace', fontSize: 18, color: 'var(--text-primary)', lineHeight: 1.15, marginBottom: 6 }}>
      {history ? `p${history.rank}` : (isLoading ? 'loading' : 'no data')}
    </div>
    <div style={{ fontFamily: 'monospace', fontSize: 11, color: 'var(--text-secondary)', marginBottom: 18 }}>
      {history ? `current ${fmtMs(durationMs)} / p50 ${fmtMs(history.p50)}` : 'similar executions'}
    </div>
    <div style={{ minHeight: 32 }}>
      <MiniSparkline values={similarQueries.map(item => Number(item.query_duration_ms)).filter(Number.isFinite)} color="#58a6ff" />
    </div>
    {history && (
      <div style={{ marginTop: 8, fontFamily: 'monospace', fontSize: 10, color: 'var(--text-muted)' }}>
        p95 {fmtMs(history.p95)} / {history.count} runs
      </div>
    )}
  </PreviewCard>
);

const ResourcePressurePreview: React.FC<{
  scores: { time: number; memory: number; cpu: number; io: number; scan: number };
  durationMs: number;
  cpuUs: number;
  memoryBytes: number;
  ioBytes: number;
  onOpen: () => void;
}> = ({ scores, durationMs, cpuUs, memoryBytes, ioBytes, onOpen }) => (
  <PreviewCard title="Resource Pressure" action="Details" onOpen={onOpen}>
    <div style={{ display: 'grid', gridTemplateColumns: '144px minmax(0, 1fr)', gap: 20, alignItems: 'center' }}>
      <PressureGlyphPanel scores={scores} />
      <div>
        <MetricBar label="Time" value={fmtMs(durationMs)} ratio={scores.time} color="#58a6ff" />
        <MetricBar label="Memory" value={formatBytes(memoryBytes)} ratio={scores.memory} color="#a371f7" />
        <MetricBar label="CPU" value={fmtUs(cpuUs)} ratio={scores.cpu} color="#d29922" />
        <MetricBar label="I/O" value={formatBytes(ioBytes)} ratio={scores.io} color="#3fb950" />
        <MetricBar label="Scan" value={scores.scan > 0 ? percentLabel(scores.scan * 100) : 'n/a'} ratio={scores.scan} color="#8b949e" />
      </div>
    </div>
  </PreviewCard>
);

const PressureGlyphPanel: React.FC<{ scores: { time: number; memory: number; cpu: number; io: number; scan: number } }> = ({ scores }) => (
  <div style={{ position: 'relative', display: 'grid', placeItems: 'center', height: 128, width: 144, minWidth: 0 }}>
    <PressureGlyph scores={scores} size={94} />
    <AxisLabel label="Time" style={{ top: 0, left: '50%', transform: 'translateX(-50%)' }} />
    <AxisLabel label="Mem" style={{ top: 38, right: 0 }} />
    <AxisLabel label="CPU" style={{ bottom: 4, right: 18 }} />
    <AxisLabel label="I/O" style={{ bottom: 4, left: 22 }} />
    <AxisLabel label="Scan" style={{ top: 38, left: 0 }} />
  </div>
);

const AxisLabel: React.FC<{ label: string; style: React.CSSProperties }> = ({ label, style }) => (
  <div style={{
    position: 'absolute',
    color: 'var(--text-muted)',
    fontSize: 8,
    fontWeight: 700,
    letterSpacing: '0.05em',
    textTransform: 'uppercase',
    ...style,
  }}>
    {label}
  </div>
);

const PressureGlyph: React.FC<{ scores: { time: number; memory: number; cpu: number; io: number; scan: number }; size?: number }> = ({ scores, size = 76 }) => {
  const values = [scores.time, scores.memory, scores.cpu, scores.io, scores.scan];
  const center = 48;
  const radius = 35;
  const points = values.map((score, i) => {
    const angle = (-90 + i * 72) * Math.PI / 180;
    const r = 8 + Math.max(0, Math.min(1, score)) * radius;
    return `${center + Math.cos(angle) * r},${center + Math.sin(angle) * r}`;
  }).join(' ');
  return (
    <svg width={size} height={size} viewBox="0 0 96 96" aria-hidden="true" style={{ flexShrink: 0 }}>
      <circle cx={center} cy={center} r={radius} fill="transparent" stroke="var(--border-primary)" strokeWidth="2" />
      {[0, 1, 2, 3, 4].map((i) => {
        const angle = (-90 + i * 72) * Math.PI / 180;
        return <line key={i} x1={center} y1={center} x2={center + Math.cos(angle) * radius} y2={center + Math.sin(angle) * radius} stroke="var(--border-primary)" />;
      })}
      <polygon points={points} fill="rgba(210,153,34,0.2)" stroke="#d29922" strokeWidth="3" />
    </svg>
  );
};

const ParallelExecutionPreview: React.FC<{
  subQueries: SubQueryInfo[];
  nodeCount: number;
  childCount: number;
  rootDurationMs: number;
  onOpen: () => void;
}> = ({ subQueries, nodeCount, childCount, rootDurationMs, onOpen }) => (
  <PreviewCard title="Parallel Execution" action={childCount > 0 ? 'Distributed' : 'Pipeline'} onOpen={onOpen}>
    <div style={{
      display: 'flex',
      justifyContent: 'space-between',
      alignItems: 'baseline',
      gap: 10,
      marginBottom: 10,
    }}>
      <div style={{ fontFamily: 'monospace', fontSize: 18, color: 'var(--text-primary)' }}>{nodeCount} {nodeCount === 1 ? 'node' : 'nodes'}</div>
      <div style={{ fontFamily: 'monospace', fontSize: 12, color: '#a371f7' }}>{childCount} child {childCount === 1 ? 'query' : 'queries'}</div>
    </div>
    <ChildQueryBars subQueries={subQueries} rootDurationMs={rootDurationMs} />
  </PreviewCard>
);

const Badge: React.FC<{ color: string; children: React.ReactNode }> = ({ color, children }) => (
  <span style={{
    display: 'inline-flex',
    padding: '3px 7px',
    borderRadius: 5,
    border: `1px solid ${color}55`,
    background: `${color}18`,
    color,
    fontFamily: 'monospace',
    fontSize: 11,
    fontWeight: 700,
    lineHeight: 1.2,
  }}>
    {children}
  </span>
);

const MetricBar: React.FC<{ label: string; value: string; ratio: number; color: string; title?: string; labelWidth?: number }> = ({ label, value, ratio, color, title, labelWidth = 42 }) => (
  <div
    title={title}
    style={{
      display: 'grid',
      gridTemplateColumns: `${labelWidth}px minmax(0, 1fr) 72px`,
      columnGap: 6,
      alignItems: 'center',
      marginBottom: 7,
      cursor: title ? 'help' : undefined,
    }}
  >
    <div style={{ fontFamily: 'monospace', fontSize: 10, color: 'var(--text-muted)' }}>{label}</div>
    <div style={{ height: 8, borderRadius: 999, overflow: 'hidden', background: 'var(--bg-tertiary)' }}>
      <div style={{ width: `${Math.max(0, Math.min(100, ratio * 100))}%`, height: '100%', borderRadius: 999, background: color }} />
    </div>
    <div style={{ fontFamily: 'monospace', fontSize: 10, color: 'var(--text-secondary)', textAlign: 'right', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', paddingRight: 4 }}>
      {value}
    </div>
  </div>
);

function Icon({ children }: { children: React.ReactNode }) {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      {children}
    </svg>
  );
}

const ExploreDestinationCard: React.FC<{
  title: string;
  accent: string;
  icon: React.ReactNode;
  primary: string;
  secondary: string;
  onAction: () => void;
  children: React.ReactNode;
}> = ({ title, accent, icon, primary, secondary, onAction, children }) => (
  <button
    type="button"
    onClick={onAction}
    style={{
      ...PANEL,
      border: '1px solid var(--border-primary)',
      minHeight: 126,
      padding: '13px 14px',
      color: 'inherit',
      font: 'inherit',
      textAlign: 'left',
      cursor: 'pointer',
      display: 'flex',
      flexDirection: 'column',
      gap: 8,
      minWidth: 0,
      boxShadow: 'var(--shadow-sm)',
      transition: 'border-color 0.15s ease, transform 0.15s ease, background 0.15s ease',
    }}
    onMouseEnter={event => {
      event.currentTarget.style.borderColor = accent;
      event.currentTarget.style.background = 'var(--bg-card-hover)';
      event.currentTarget.style.transform = 'translateY(-1px)';
    }}
    onMouseLeave={event => {
      event.currentTarget.style.borderColor = 'var(--border-primary)';
      event.currentTarget.style.background = 'var(--bg-card)';
      event.currentTarget.style.transform = 'none';
    }}
  >
    <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 10, color: 'var(--text-secondary)' }}>
      <span style={{ display: 'flex', alignItems: 'center', gap: 9, minWidth: 0 }}>
        <span style={{ color: accent, display: 'flex', alignItems: 'center', flexShrink: 0 }}>{icon}</span>
        <span style={{ fontSize: 14, fontWeight: 700, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{title}</span>
      </span>
      <span
        style={{
          color: '#58a6ff',
          fontSize: 15,
          fontWeight: 700,
          whiteSpace: 'nowrap',
          flexShrink: 0,
        }}
      >
        {'->'}
      </span>
    </div>

    <div>
      <div style={{ color: 'var(--text-primary)', fontSize: 23, lineHeight: 1.08, fontWeight: 750, letterSpacing: 0, fontVariantNumeric: 'tabular-nums' }}>
        {primary}
      </div>
      <div style={{ marginTop: 4, color: 'var(--text-secondary)', fontSize: 12, lineHeight: 1.25, minHeight: 15, maxHeight: 32, overflow: 'hidden' }}>
        {secondary}
      </div>
    </div>

    <div style={{ flex: 1, minHeight: 24, display: 'flex', flexDirection: 'column', justifyContent: 'center' }}>
      {children}
    </div>
  </button>
);

const ChipPreview: React.FC<{ values: string[]; empty: string; color: string }> = ({ values, empty, color }) => {
  const shown = values.slice(0, 5);
  if (shown.length === 0) {
    return <div style={{ fontFamily: 'monospace', fontSize: 12, color: 'var(--text-muted)' }}>{empty}</div>;
  }
  return (
    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6, marginTop: 4 }}>
      {shown.map(value => (
        <span
          key={value}
          title={value}
          style={{
            maxWidth: 125,
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            whiteSpace: 'nowrap',
            padding: '3px 6px',
            borderRadius: 5,
            border: `1px solid ${color}55`,
            background: `${color}14`,
            color,
            fontFamily: 'monospace',
            fontSize: 10,
          }}
        >
          {value}
        </span>
      ))}
      {values.length > shown.length && (
        <span style={{ fontFamily: 'monospace', fontSize: 11, color: 'var(--text-muted)', padding: '4px 0' }}>
          +{values.length - shown.length}
        </span>
      )}
    </div>
  );
};

function ReadResultBars({ readRows, resultRows }: { readRows: number; resultRows: number }) {
  const resultPct = readRows > 0 ? (resultRows / readRows) * 100 : 0;
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
      <div style={{ height: 9, borderRadius: 999, overflow: 'hidden', background: 'rgba(63, 185, 80, 0.14)' }}>
        <div style={{ width: readRows > 0 ? '100%' : 0, height: '100%', borderRadius: 999, background: '#3fb950' }} />
      </div>
      <div style={{ height: 9, borderRadius: 999, overflow: 'hidden', background: 'rgba(163, 113, 247, 0.14)' }}>
        <div style={{
          width: resultRows > 0 ? `${Math.max(2, Math.min(100, resultPct))}%` : 0,
          height: '100%',
          borderRadius: 999,
          background: '#a371f7',
        }} />
      </div>
    </div>
  );
}

function objectStorageRequestCount(summary: ObjectStorageProfileSummary): number {
  const readRequests = Math.max(summary.readRequests, summary.getRequests + summary.headRequests);
  const writeRequests = Math.max(summary.writeRequests, summary.putRequests + summary.postRequests);
  return readRequests + writeRequests + summary.listRequests;
}

function objectStoragePrimary(summary: ObjectStorageProfileSummary): string {
  if (summary.bytesRead > 0) return `${formatBytes(summary.bytesRead)} read`;
  if (summary.bytesWritten > 0) return `${formatBytes(summary.bytesWritten)} write`;
  return `${formatNumberCompact(objectStorageRequestCount(summary))} requests`;
}

function objectStorageSecondary(summary: ObjectStorageProfileSummary): string {
  const requests = objectStorageRequestCount(summary);
  const elapsedUs = Math.max(
    summary.bufferReadMicroseconds,
    summary.bufferWriteMicroseconds,
    summary.s3ReadMicroseconds,
    summary.s3WriteMicroseconds,
  );
  const requestPart = requests > 0 ? `${formatNumberCompact(requests)} requests` : 'object storage I/O';
  return elapsedUs > 0 ? `${requestPart} / ${fmtUs(elapsedUs)}` : requestPart;
}

function threadCardPrimary(queryDetail: QueryDetailType | null): string {
  const threadCount = queryDetail?.thread_ids?.length ?? 0;
  if (threadCount > 0) return `${threadCount} ${threadCount === 1 ? 'thread' : 'threads'}`;
  return 'Threads';
}

function ObjectStorageBars({ summary }: { summary: ObjectStorageProfileSummary }) {
  const maxBytes = Math.max(summary.bytesRead, summary.bytesWritten, 1);
  const rows = [
    { label: 'read', value: summary.bytesRead, color: '#3fb950' },
    ...(summary.bytesWritten > 0 ? [{ label: 'write', value: summary.bytesWritten, color: '#f0883e' }] : []),
  ];
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
      {rows.map(row => (
        <div key={row.label} style={{ display: 'grid', gridTemplateColumns: '46px 1fr', gap: 8, alignItems: 'center' }}>
          <div style={{ fontFamily: 'monospace', fontSize: 10, color: 'var(--text-muted)' }}>{row.label}</div>
          <div style={{ height: 9, borderRadius: 999, overflow: 'hidden', background: `${row.color}16` }}>
            <div
              style={{
                width: row.value > 0 ? `${Math.max(2, Math.min(100, (row.value / maxBytes) * 100))}%` : 0,
                height: '100%',
                borderRadius: 999,
                background: row.color,
              }}
            />
          </div>
        </div>
      ))}
      {rows.length === 1 && (
        <div style={{ fontFamily: 'monospace', fontSize: 10, color: 'var(--text-muted)' }}>
          {summary.patterns.slice(0, 2).join(' / ') || 'remote storage'}
        </div>
      )}
    </div>
  );
}

function ProgressBar({ value, color }: { value: number; color: string }) {
  const pct = Math.max(0, Math.min(100, value));
  return (
    <div style={{ height: 9, borderRadius: 999, overflow: 'hidden', background: 'var(--bg-tertiary)' }}>
      <div style={{ width: `${pct}%`, height: '100%', borderRadius: 999, background: color, transition: 'width 0.2s ease' }} />
    </div>
  );
}

function DotRow({ count, active, color }: { count: number; active: number; color: string }) {
  const visible = Math.max(1, Math.min(count, 8));
  return (
    <div style={{ display: 'flex', gap: 6, alignItems: 'center', height: 22 }}>
      {Array.from({ length: visible }, (_, index) => (
        <span
          key={index}
          style={{
            width: 11,
            height: 11,
            borderRadius: '50%',
            background: index < active ? color : 'transparent',
            border: `1px solid ${index < active ? color : 'var(--border-primary)'}`,
            boxShadow: index < active ? `0 0 0 3px ${color}18` : 'none',
          }}
        />
      ))}
    </div>
  );
}

function MiniSparkline({ values, color }: { values: number[]; color: string }) {
  const points = values.slice(-18);
  if (points.length < 2) {
    return <div style={{ height: 24 }} />;
  }

  const min = Math.min(...points);
  const max = Math.max(...points);
  const span = Math.max(max - min, 1);
  const d = points
    .map((value, index) => {
      const x = (index / (points.length - 1)) * 100;
      const y = 28 - ((value - min) / span) * 24 - 2;
      return `${index === 0 ? 'M' : 'L'} ${x.toFixed(2)} ${y.toFixed(2)}`;
    })
    .join(' ');

  return (
    <svg viewBox="0 0 100 32" preserveAspectRatio="none" style={{ width: '100%', height: 24, display: 'block' }}>
      <path d={d} fill="none" stroke={color} strokeWidth="2.4" strokeLinecap="round" strokeLinejoin="round" vectorEffect="non-scaling-stroke" />
    </svg>
  );
}

const ChildQueryBars: React.FC<{ subQueries: SubQueryInfo[]; rootDurationMs: number }> = ({ subQueries, rootDurationMs }) => {
  if (subQueries.length === 0) {
    return (
      <div style={{ height: 46, display: 'flex', alignItems: 'center', color: 'var(--text-muted)', fontFamily: 'monospace', fontSize: 11 }}>
        local or not detected
      </div>
    );
  }
  const rows = subQueries.slice(0, 4);
  const maxDuration = Math.max(rootDurationMs, ...rows.map(row => Number(row.query_duration_ms) || 0), 1);
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 6, marginTop: 2 }}>
      {rows.map(row => {
        const duration = Number(row.query_duration_ms) || 0;
        return (
          <div key={row.query_id} style={{ display: 'grid', gridTemplateColumns: '58px 1fr 44px', gap: 8, alignItems: 'center' }}>
            <div style={{ fontFamily: 'monospace', fontSize: 10, color: 'var(--text-muted)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
              {row.hostname.split('.')[0]}
            </div>
            <div style={{ height: 8, borderRadius: 3, background: 'var(--bg-tertiary)', overflow: 'hidden' }}>
              <div style={{ width: `${Math.max(2, (duration / maxDuration) * 100)}%`, height: '100%', borderRadius: 3, background: '#d29922' }} />
            </div>
            <div style={{ fontFamily: 'monospace', fontSize: 10, color: 'var(--text-secondary)', textAlign: 'right' }}>
              {fmtMs(duration)}
            </div>
          </div>
        );
      })}
    </div>
  );
};
