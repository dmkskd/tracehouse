import React from 'react';
import type { SubQueryInfo } from '@tracehouse/core';
import type { QueryHistoryItem } from '../../stores/queryStore';
import { formatBytes, formatNumber } from '../../stores/queryStore';
import { formatDurationMs } from '../../utils/formatters';
import {
  buildResourcePressureMetrics,
  buildScanEfficiencyMetrics,
  type ResourcePressureLevel,
} from '../../utils/queryHoverMetrics';

const mono = 'ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace';

const colors = {
  blue: '#58a6ff',
  green: '#3fb950',
  amber: '#d29922',
  red: '#f85149',
  violet: '#a78bfa',
  gray: '#94a3b8',
  muted: 'var(--text-muted)',
};

const cardStyle: React.CSSProperties = {
  border: '1px solid var(--border-primary)',
  borderRadius: 7,
  background: 'var(--bg-card)',
  overflow: 'hidden',
};

const cardBodyStyle: React.CSSProperties = {
  padding: 12,
};

const cardTitleStyle: React.CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'space-between',
  gap: 8,
  marginBottom: 9,
  color: 'var(--text-muted)',
  fontSize: 10,
  fontWeight: 700,
  letterSpacing: '0.08em',
  textTransform: 'uppercase',
};

const chipStyle = (color: string, bgAlpha = '22'): React.CSSProperties => {
  const isCssVar = color.startsWith('var(');
  return {
    display: 'inline-flex',
    alignItems: 'center',
    minWidth: 0,
    maxWidth: '100%',
    padding: '3px 7px',
    borderRadius: 5,
    border: isCssVar ? '1px solid var(--border-primary)' : `1px solid ${color}55`,
    background: isCssVar ? 'var(--bg-tertiary)' : `${color}${bgAlpha}`,
    color,
    fontFamily: mono,
    fontSize: 10,
    fontWeight: 700,
    lineHeight: 1.2,
  };
};

const normalizeKind = (kind: string): string => kind ? kind.toUpperCase() : 'QUERY';

const kindColor = (kind: string): string => {
  switch (normalizeKind(kind)) {
    case 'SELECT': return colors.blue;
    case 'INSERT': return colors.amber;
    case 'ALTER': return colors.red;
    case 'CREATE': return colors.green;
    case 'SYSTEM': return colors.violet;
    default: return colors.muted;
  }
};

const isSuccess = (q: QueryHistoryItem): boolean => !q.exception && q.type !== 'ExceptionWhileProcessing' && q.type !== 'error';

const isDistributed = (q: QueryHistoryItem, coordinatorIds?: Set<string>): boolean =>
  q.is_initial_query === 0 || Boolean(coordinatorIds?.has(q.query_id));

const isInternalTraceHouse = (q: QueryHistoryItem): boolean => {
  const databases = q.databases ?? [];
  const tables = q.tables ?? [];
  return databases.some(db => db.toLowerCase() === 'tracehouse')
    || tables.some(table => table.toLowerCase().startsWith('tracehouse.'));
};

const pressureColor = (level: ResourcePressureLevel): string => {
  switch (level) {
    case 'high': return colors.red;
    case 'moderate': return colors.amber;
    default: return colors.green;
  }
};

const pruningColor = (level: ResourcePressureLevel | 'none'): string => {
  switch (level) {
    case 'low': return colors.green;
    case 'moderate': return colors.amber;
    case 'high': return colors.red;
    default: return colors.gray;
  }
};

export const QueryFingerprintGlyph: React.FC<{
  query: QueryHistoryItem;
  coordinatorIds?: Set<string>;
  size?: number;
}> = ({ query, size = 34 }) => {
  const metrics = buildResourcePressureMetrics(query);
  const scores = [metrics.scores.time, metrics.scores.memory, metrics.scores.cpu, metrics.scores.io, metrics.scores.scan];
  const center = 21;
  const radius = 17;
  const points = scores.map((score, i) => {
    const angle = (-90 + i * 72) * Math.PI / 180;
    const r = 6 + score * radius;
    return `${center + Math.cos(angle) * r},${center + Math.sin(angle) * r}`;
  }).join(' ');
  const color = pressureColor(metrics.level);

  return (
    <svg width={size} height={size} viewBox="0 0 42 42" aria-label="Query shape glyph" role="img">
      <circle cx="21" cy="21" r="18" fill="var(--bg-secondary)" stroke="var(--border-primary)" />
      {[0, 1, 2, 3, 4].map((i) => {
        const angle = (-90 + i * 72) * Math.PI / 180;
        return (
          <line
            key={i}
            x1="21"
            y1="21"
            x2={21 + Math.cos(angle) * 17}
            y2={21 + Math.sin(angle) * 17}
            stroke="var(--border-primary)"
            strokeWidth="1"
          />
        );
      })}
      <polygon points={points} fill={`${color}30`} stroke={color} strokeWidth="2" />
    </svg>
  );
};

const MetricCell: React.FC<{ value: string; label: string }> = ({ value, label }) => (
  <div style={{ minWidth: 0, padding: '9px 8px', borderRight: '1px solid var(--border-primary)' }}>
    <div style={{ color: 'var(--text-primary)', fontFamily: mono, fontSize: 13, fontWeight: 700, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
      {value}
    </div>
    <div style={{ marginTop: 2, color: 'var(--text-muted)', fontSize: 9, fontWeight: 700, letterSpacing: '0.08em', textTransform: 'uppercase' }}>
      {label}
    </div>
  </div>
);

const QuerySummaryCard: React.FC<{ query: QueryHistoryItem; coordinatorIds?: Set<string> }> = ({ query, coordinatorIds }) => {
  const kColor = kindColor(query.query_kind);
  const tables = query.tables ?? [];
  const databases = query.databases ?? [];
  return (
    <section style={cardStyle}>
      <div style={cardBodyStyle}>
        <div style={cardTitleStyle}>
          Query Summary
          <span style={chipStyle(kColor)}>{normalizeKind(query.query_kind)}</span>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 9, minWidth: 0 }}>
          <QueryFingerprintGlyph query={query} coordinatorIds={coordinatorIds} />
          <div style={{ minWidth: 0 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap', marginBottom: 5 }}>
              <span style={{ color: colors.blue, fontFamily: mono, fontSize: 13, fontWeight: 700 }}>{query.query_id.slice(0, 8)}</span>
              {isDistributed(query, coordinatorIds) && <span style={chipStyle(colors.violet)}>parallel</span>}
              {isInternalTraceHouse(query) && <span style={chipStyle(colors.amber)}>internal</span>}
              <span style={chipStyle(isSuccess(query) ? colors.green : colors.red)}>{isSuccess(query) ? 'success' : 'error'}</span>
            </div>
            <div style={{ color: 'var(--text-secondary)', fontSize: 12, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
              {query.user || 'unknown user'} on {query.hostname || 'unknown host'}
            </div>
          </div>
        </div>
        {(tables.length > 0 || databases.length > 0) && (
          <div style={{ marginTop: 12, paddingTop: 10, borderTop: '1px solid var(--border-primary)' }}>
            <div style={{ ...cardTitleStyle, marginBottom: 7 }}>
              Resources
              <span>{tables.length || databases.length}</span>
            </div>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
              {tables.slice(0, 6).map((table) => (
                <span key={table} title={table} style={chipStyle(colors.green)}>{table}</span>
              ))}
              {tables.length === 0 && databases.slice(0, 4).map((database) => (
                <span key={database} title={database} style={chipStyle(colors.blue)}>{database}.*</span>
              ))}
              {tables.length > 6 && <span style={chipStyle(colors.muted)}>+{tables.length - 6}</span>}
            </div>
          </div>
        )}
      </div>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, minmax(0, 1fr))', borderTop: '1px solid var(--border-primary)' }}>
        <MetricCell value={formatDurationMs(query.query_duration_ms)} label="Duration" />
        <MetricCell value={formatNumber(query.read_rows)} label="Rows" />
        <MetricCell value={formatBytes(query.read_bytes)} label="Bytes" />
      </div>
    </section>
  );
};

const ShapeLegendCard: React.FC<{ query: QueryHistoryItem; coordinatorIds?: Set<string> }> = ({ query, coordinatorIds }) => {
  const metrics = buildResourcePressureMetrics(query);
  const tone = pressureColor(metrics.level);
  const toneLabel = metrics.level;

  return (
    <section style={cardStyle}>
      <div style={cardBodyStyle}>
        <div style={cardTitleStyle}>
          Resource Pressure
          <span style={{ color: tone }}>{toneLabel}</span>
        </div>
        <div style={{ position: 'relative', display: 'grid', placeItems: 'center', minHeight: 118, marginTop: 2 }}>
          <QueryFingerprintGlyph query={query} coordinatorIds={coordinatorIds} size={76} />
          <AxisLabel label="time" style={{ top: 0, left: '50%', transform: 'translateX(-50%)' }} />
          <AxisLabel label="memory" style={{ top: 31, right: 4 }} />
          <AxisLabel label="CPU" style={{ bottom: 5, right: 35 }} />
          <AxisLabel label="I/O" style={{ bottom: 5, left: 35 }} />
          <AxisLabel label="scan" style={{ top: 31, left: 0 }} />
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 6, marginTop: 10 }}>
          <DimensionMeter label="Time" value={metrics.scores.time} displayValue={formatDurationMs(query.query_duration_ms)} color={colors.blue} />
          <DimensionMeter label="Memory" value={metrics.scores.memory} displayValue={formatBytes(query.memory_usage)} color={colors.violet} />
          <DimensionMeter label="CPU" value={metrics.scores.cpu} displayValue={formatDurationMs(metrics.cpuMs)} color={colors.amber} />
          <DimensionMeter label="I/O" value={metrics.scores.io} displayValue={formatBytes(metrics.ioBytes)} color={colors.green} />
          <DimensionMeter
            label="Scan"
            value={metrics.scores.scan}
            displayValue={metrics.scanDisplay}
            color={query.efficiency_score == null ? colors.gray : colors.red}
          />
        </div>
      </div>
    </section>
  );
};

const AxisLabel: React.FC<{ label: string; style: React.CSSProperties }> = ({ label, style }) => (
  <div style={{
    position: 'absolute',
    color: 'var(--text-muted)',
    fontSize: 9,
    fontWeight: 700,
    letterSpacing: '0.06em',
    textTransform: 'uppercase',
    ...style,
  }}>
    {label}
  </div>
);

const DimensionMeter: React.FC<{ label: string; value: number; displayValue: string; color: string }> = ({ label, value, displayValue, color }) => (
  <div>
    <div style={{ display: 'flex', justifyContent: 'space-between', gap: 6, marginBottom: 3, color: 'var(--text-muted)', fontSize: 10, fontWeight: 700 }}>
      <span>{label}</span>
      <span style={{ fontFamily: mono }}>{displayValue}</span>
    </div>
    <div style={{ height: 5, borderRadius: 999, background: 'var(--bg-tertiary)', overflow: 'hidden' }}>
      <div style={{ width: `${Math.max(4, value * 100)}%`, height: '100%', borderRadius: 999, background: color, opacity: 0.78 }} />
    </div>
  </div>
);
const ScanEfficiencyCard: React.FC<{ query: QueryHistoryItem }> = ({ query }) => {
  const metrics = buildScanEfficiencyMetrics(query);
  const tone = pruningColor(metrics.pruningLevel);

  return (
    <section style={cardStyle}>
      <div style={cardBodyStyle}>
        <div style={cardTitleStyle}>
          Scan Efficiency
          <span style={{ color: tone }}>{metrics.pruningDisplay}</span>
        </div>
        <div style={{ display: 'grid', gap: 8 }}>
          <FunnelBar label="read" value={`${formatNumber(query.read_rows)} rows / ${formatBytes(query.read_bytes)}`} width={metrics.readWidth} color={colors.green} />
          <FunnelBar label="result" value={`${formatNumber(query.result_rows)} / ${formatBytes(query.result_bytes)}`} width={metrics.resultWidth} color={colors.violet} />
          {(metrics.partsPct != null || metrics.marksPct != null) && (
            <div style={{ display: 'grid', gap: 8, paddingTop: 8, marginTop: 2, borderTop: '1px solid var(--border-primary)' }}>
              {metrics.partsPct != null && (
                <FunnelBar
                  label="parts"
                  value={`${query.selected_parts}/${query.selected_parts_total}`}
                  width={metrics.partsPct}
                  color={colors.green}
                />
              )}
              {metrics.marksPct != null && (
                <FunnelBar
                  label="marks"
                  value={`${query.selected_marks}/${query.selected_marks_total}`}
                  width={metrics.marksPct}
                  color={colors.blue}
                />
              )}
            </div>
          )}
        </div>
      </div>
    </section>
  );
};

const FunnelBar: React.FC<{ label: string; value: string; width: number; color: string }> = ({ label, value, width, color }) => (
  <div style={{ display: 'grid', gridTemplateColumns: '48px 1fr', alignItems: 'center', gap: 8 }}>
    <span style={{ color: 'var(--text-muted)', fontSize: 11, fontWeight: 700 }}>{label}</span>
    <div style={{ position: 'relative', height: 25, borderRadius: 5, background: 'var(--bg-tertiary)', overflow: 'hidden' }}>
      <div style={{ position: 'absolute', inset: `0 auto 0 0`, width: `${width}%`, minWidth: 8, borderRadius: 5, background: color, opacity: 0.78 }} />
      <span style={{ position: 'relative', zIndex: 1, display: 'flex', alignItems: 'center', height: '100%', paddingLeft: 9, color: 'var(--text-primary)', fontFamily: mono, fontSize: 11, fontWeight: 700 }}>
        {value}
      </span>
    </div>
  </div>
);

const ColumnsCard: React.FC<{ query: QueryHistoryItem }> = ({ query }) => {
  const columns = query.columns ?? [];
  if (columns.length === 0) return null;
  return (
    <section style={cardStyle}>
      <div style={cardBodyStyle}>
        <div style={cardTitleStyle}>Columns <span>{columns.length}</span></div>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
          {columns.slice(0, 12).map((column) => (
            <span key={column} title={column} style={chipStyle(colors.amber)}>{column}</span>
          ))}
          {columns.length > 12 && <span style={chipStyle(colors.muted)}>+{columns.length - 12}</span>}
        </div>
      </div>
    </section>
  );
};

const DistributedCard: React.FC<{
  query: QueryHistoryItem;
  coordinatorIds?: Set<string>;
  childQueries?: SubQueryInfo[];
  isLoading?: boolean;
}> = ({ query, coordinatorIds, childQueries, isLoading = false }) => {
  const nodeQueries = childQueries ?? [];
  const distinctNodeCount = new Set(nodeQueries.map(q => q.hostname || q.query_id)).size;
  const hasParallelShape = nodeQueries.length > 0 || (isLoading && isDistributed(query, coordinatorIds));
  if (!hasParallelShape) return null;

  const maxDuration = Math.max(
    1,
    ...nodeQueries.map(nodeQuery => nodeQuery.query_duration_ms),
  );

  return (
    <section style={cardStyle}>
      <div style={cardBodyStyle}>
        <div style={cardTitleStyle}>
          Parallel Execution
          <span style={{ color: colors.violet }}>
            {isLoading
              ? 'loading'
              : nodeQueries.length > 0
              ? `${nodeQueries.length} child ${nodeQueries.length === 1 ? 'query' : 'queries'} · ${distinctNodeCount} node${distinctNodeCount === 1 ? '' : 's'}`
              : 'parallel'}
          </span>
        </div>
        <div style={{ display: 'grid', gap: 8 }}>
          {isLoading ? (
            <div style={{ color: 'var(--text-muted)', fontSize: 11, fontFamily: mono }}>
              Loading child query timings…
            </div>
          ) : nodeQueries.slice(0, 4).map(nodeQuery => (
            <TopologyRow
              key={`${nodeQuery.query_id}:${nodeQuery.hostname}:${nodeQuery.query_start_time_microseconds}`}
              label={nodeQuery.hostname || nodeQuery.query_id.slice(0, 8)}
              active={nodeQuery.query_id === query.query_id}
              width={Math.max(18, (nodeQuery.query_duration_ms / maxDuration) * 100)}
              color={colors.amber}
              value={formatDurationMs(nodeQuery.query_duration_ms)}
              title={[
                `query_id: ${nodeQuery.query_id}`,
                `host: ${nodeQuery.hostname || 'unknown host'}`,
                `duration: ${formatDurationMs(nodeQuery.query_duration_ms)}`,
                `memory: ${formatBytes(nodeQuery.memory_usage)}`,
                `rows: ${formatNumber(nodeQuery.read_rows)}`,
              ].join('\n')}
            />
          ))}
          {nodeQueries.length > 4 && (
            <div style={{ color: 'var(--text-muted)', fontSize: 11, fontFamily: mono }}>
              +{nodeQueries.length - 4} more child queries
            </div>
          )}
        </div>
        {query.is_initial_query === 0 && query.initial_query_id && (
          <div style={{ marginTop: 9, color: 'var(--text-muted)', fontSize: 11, fontFamily: mono, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
            parent {query.initial_query_id}
          </div>
        )}
      </div>
    </section>
  );
};

const TopologyRow: React.FC<{ label: string; active: boolean; width: number; color: string; value: string; title?: string }> = ({ label, active, width, color, value, title }) => (
  <div title={title} style={{ display: 'grid', gridTemplateColumns: '96px 1fr 58px', alignItems: 'center', gap: 8, color: active ? 'var(--text-primary)' : 'var(--text-muted)', fontSize: 11, fontFamily: mono, fontWeight: 700 }}>
    <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{label}</span>
    <div style={{ height: 20, borderRadius: 5, background: 'var(--bg-tertiary)', overflow: 'hidden' }}>
      <div style={{ height: '100%', width: `${width}%`, background: color, opacity: active ? 0.85 : 0.25 }} />
    </div>
    <span>{value}</span>
  </div>
);

const InsertPipelineCard: React.FC<{ query: QueryHistoryItem }> = ({ query }) => {
  if (normalizeKind(query.query_kind) !== 'INSERT') return null;
  const internal = isInternalTraceHouse(query);
  return (
    <section style={cardStyle}>
      <div style={cardBodyStyle}>
        <div style={cardTitleStyle}>Insert Pipeline <span style={{ color: internal ? colors.amber : colors.green }}>{internal ? 'internal' : 'write'}</span></div>
        <div style={{ display: 'grid', gap: 8 }}>
          <TopologyRow label="source" active width={38} color={colors.blue} value={query.read_rows > 0 ? formatNumber(query.read_rows) : 'n/a'} />
          <TopologyRow label="write" active width={52} color={colors.green} value={formatBytes(query.memory_usage)} />
        </div>
      </div>
    </section>
  );
};

export const QueryHoverPreview: React.FC<{
  query: QueryHistoryItem | null;
  coordinatorIds?: Set<string>;
  childQueries?: SubQueryInfo[];
  isLoadingChildQueries?: boolean;
}> = ({ query, coordinatorIds, childQueries, isLoadingChildQueries }) => {
  if (!query) {
    return (
      <aside style={{ ...cardStyle, padding: 14, color: 'var(--text-muted)', fontSize: 12 }}>
        <div style={cardTitleStyle}>Query Preview</div>
        Hover a history row to inspect query shape.
      </aside>
    );
  }

  return (
    <aside style={{
      border: '1px solid var(--border-primary)',
      borderRadius: 9,
      background: 'var(--bg-secondary)',
      boxShadow: '0 18px 50px rgba(0,0,0,0.12)',
      overflow: 'hidden',
    }}>
      <div style={{
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        gap: 10,
        padding: '12px 14px',
        borderBottom: '1px solid var(--border-primary)',
      }}>
        <div style={{ color: 'var(--text-muted)', fontSize: 10, fontWeight: 700, letterSpacing: '0.1em', textTransform: 'uppercase' }}>
          Query Preview
        </div>
        <div style={{ color: 'var(--text-muted)', fontFamily: mono, fontSize: 10 }}>
          {query.query_id.slice(0, 8)}
        </div>
      </div>
      <div style={{ display: 'grid', gap: 9, padding: 10 }}>
        <QuerySummaryCard query={query} coordinatorIds={coordinatorIds} />
        <ShapeLegendCard query={query} coordinatorIds={coordinatorIds} />
        <ScanEfficiencyCard query={query} />
        <ColumnsCard query={query} />
        <DistributedCard query={query} coordinatorIds={coordinatorIds} childQueries={childQueries} isLoading={isLoadingChildQueries} />
        <InsertPipelineCard query={query} />
      </div>
    </aside>
  );
};

export default QueryHoverPreview;
