import React, { useEffect, useMemo, useState } from 'react';
import type {
  EventContextMetricPoint,
  EventContextResult,
  EventContextSource,
  EventContextSourceStatus,
  OperationalEvent,
} from '@tracehouse/core';
import { useClickHouseServices } from '../../providers/ClickHouseProvider';
import { formatBytes } from '../../utils/formatters';
import { EVENT_SEVERITY_COLORS } from './event-model';
import {
  buildMetricChartGeometry,
  closestMetricPointIndex,
  formatContextOffset,
  metricSeriesForHost,
  selectNearbyEvents,
} from './event-context-model';
import { eventKindLabel, formatEventDateTime } from './events-dashboard-model';

interface EventContextViewProps {
  event: OperationalEvent;
  events: readonly OperationalEvent[];
  availableCapabilities: readonly string[];
}

const WINDOW_OPTIONS = [
  { seconds: 60, label: '±1m' },
  { seconds: 300, label: '±5m' },
  { seconds: 900, label: '±15m' },
] as const;

export const EventContextView: React.FC<EventContextViewProps> = ({
  event,
  events,
  availableCapabilities,
}) => {
  const services = useClickHouseServices();
  const [windowSeconds, setWindowSeconds] = useState(300);
  const [requestState, setRequestState] = useState<{
    key: string;
    context: EventContextResult | null;
    error: string | null;
  } | null>(null);
  const requestKey = [
    event.occurred_at,
    event.hostname ?? '',
    event.query_id ?? '',
    event.initial_query_id ?? '',
    windowSeconds,
    availableCapabilities.join(','),
  ].join(':');
  const currentRequest = requestState?.key === requestKey ? requestState : null;
  const context = currentRequest?.context ?? null;
  const error = currentRequest?.error ?? null;
  const loading = services !== null && currentRequest === null;

  useEffect(() => {
    if (!services) return;
    let cancelled = false;
    void services.eventContextService.getContext({
      eventTime: event.occurred_at,
      hostname: event.hostname,
      queryId: event.query_id,
      initialQueryId: event.initial_query_id,
      availableCapabilities,
      windowSeconds,
      limit: 100,
    }).then(result => {
      if (!cancelled) {
        setRequestState({ key: requestKey, context: result, error: null });
      }
    }).catch(cause => {
      if (!cancelled) {
        setRequestState({
          key: requestKey,
          context: null,
          error: cause instanceof Error ? cause.message : 'Failed to load event context',
        });
      }
    });
    return () => {
      cancelled = true;
    };
  }, [
    availableCapabilities,
    event.hostname,
    event.initial_query_id,
    event.occurred_at,
    event.query_id,
    requestKey,
    services,
    windowSeconds,
  ]);

  const nearbyEvents = useMemo(
    () => selectNearbyEvents(event, events, windowSeconds),
    [event, events, windowSeconds],
  );

  return (
    <div style={{ overflow: 'auto', padding: 14 }}>
      <div style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        flexWrap: 'wrap',
        gap: 10,
        marginBottom: 12,
      }}>
        <div>
          <strong style={{ color: 'var(--text-primary)', fontSize: 13 }}>
            Context at {formatEventDateTime(event.occurred_at)}
          </strong>
          <div style={{ color: 'var(--text-muted)', fontSize: 9, marginTop: 3 }}>
            {event.hostname ?? 'All hosts'} · historical data around the selected event
          </div>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
          <span style={{ color: 'var(--text-muted)', fontSize: 9 }}>Window</span>
          {WINDOW_OPTIONS.map(option => (
            <button
              key={option.seconds}
              onClick={() => setWindowSeconds(option.seconds)}
              style={{
                ...smallButtonStyle,
                color: windowSeconds === option.seconds ? '#58a6ff' : 'var(--text-muted)',
                borderColor: windowSeconds === option.seconds
                  ? 'rgba(88,166,255,0.45)'
                  : 'var(--border-primary)',
                background: windowSeconds === option.seconds
                  ? 'rgba(88,166,255,0.08)'
                  : 'var(--bg-card)',
              }}
            >
              {option.label}
            </button>
          ))}
        </div>
      </div>

      {error && (
        <div style={errorStyle}>{error}</div>
      )}

      <div style={{
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))',
        gap: 10,
        alignItems: 'start',
        opacity: loading && context ? 0.72 : 1,
      }}>
        <ContextPanel
          title="Workload at event"
          source={context?.workload}
          loading={loading && !context}
          empty="No completed query interval overlaps the event."
          count={context?.workload.data.length}
        >
          {context?.workload.data.slice(0, 12).map(query => (
            <div key={`${query.hostname}:${query.query_id}`} style={rowStyle}>
              <div style={{ minWidth: 0 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                  <strong style={{ color: 'var(--text-primary)', fontSize: 10 }}>
                    {query.query_kind ?? 'Query'}
                  </strong>
                  {query.is_event_query && <Tag label="event query" color="#58a6ff" />}
                  {query.exception_code != null && (
                    <Tag label={`error ${query.exception_code}`} color="#f0883e" />
                  )}
                </div>
                <div style={monoSecondaryStyle}>{query.query_id}</div>
                <div style={{
                  color: 'var(--text-secondary)',
                  fontSize: 9,
                  marginTop: 4,
                  overflow: 'hidden',
                  textOverflow: 'ellipsis',
                  whiteSpace: 'nowrap',
                }}>
                  {query.query || 'Query text unavailable'}
                </div>
              </div>
              <div style={{ textAlign: 'right', whiteSpace: 'nowrap' }}>
                <div style={{ color: 'var(--text-secondary)', fontSize: 9 }}>
                  {formatBytes(query.memory_usage)}
                </div>
                <div style={{ color: 'var(--text-muted)', fontSize: 8, marginTop: 2 }}>
                  {(query.duration_ms / 1000).toLocaleString(undefined, {
                    maximumFractionDigits: 2,
                  })}s · {(query.cpu_us / 1_000_000).toLocaleString(undefined, {
                    maximumFractionDigits: 2,
                  })} CPU-s
                </div>
              </div>
            </div>
          ))}
        </ContextPanel>

        <ContextPanel
          title="Host metrics"
          source={context?.metrics}
          loading={loading && !context}
          empty="No metric sample was recorded in this window."
          count={context?.metrics.snapshots.length}
        >
          {context?.metrics.snapshots.map(snapshot => {
            const series = metricSeriesForHost(context.metrics.data, snapshot.hostname);
            return (
              <div key={snapshot.hostname ?? 'cluster'} style={{
                ...rowStyle,
                display: 'block',
              }}>
                <div style={{
                  display: 'flex',
                  alignItems: 'baseline',
                  justifyContent: 'space-between',
                  gap: 8,
                }}>
                  <strong style={monoPrimaryStyle}>{snapshot.hostname ?? 'cluster'}</strong>
                  <span style={{ color: 'var(--text-muted)', fontSize: 8 }}>
                    preceding sample · {Math.round(snapshot.sample_age_ms / 1000)}s old
                  </span>
                </div>
                <div style={{
                  display: 'grid',
                  gridTemplateColumns: 'repeat(4, minmax(0, 1fr))',
                  gap: 8,
                  marginTop: 8,
                }}>
                  <MetricValue label="Memory" value={formatBytes(snapshot.memory_usage)} />
                  <MetricValue label="CPU" value={`${formatNumber(snapshot.cpu_cores)} cores`} />
                  <MetricValue label="Queries" value={formatNumber(snapshot.active_queries)} />
                  <MetricValue label="Merges" value={formatNumber(snapshot.active_merges)} />
                </div>
                <HostMetricsChart
                  points={series}
                  eventTime={event.occurred_at}
                />
              </div>
            );
          })}
        </ContextPanel>

        <ContextPanel
          title="Server logs"
          source={context?.logs}
          loading={loading && !context}
          empty="No query-correlated or warning-and-higher log entries were recorded."
          count={context?.logs.data.length}
        >
          {context?.logs.data.slice(0, 20).map((entry, index) => (
            <div key={`${entry.hostname}:${entry.time}:${index}`} style={rowStyle}>
              <div style={{ minWidth: 0 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                  <Tag label={entry.level || 'Log'} color={logLevelColor(entry.level)} />
                  {entry.is_event_query && <Tag label="event query" color="#58a6ff" />}
                  <span style={{ color: 'var(--text-muted)', fontSize: 8 }}>
                    {entry.logger}
                  </span>
                </div>
                <div style={{
                  color: 'var(--text-secondary)',
                  fontSize: 9,
                  lineHeight: 1.4,
                  marginTop: 4,
                  overflowWrap: 'anywhere',
                }}>
                  {entry.message}
                </div>
              </div>
              <span style={{ color: 'var(--text-muted)', fontSize: 8, whiteSpace: 'nowrap' }}>
                {new Date(entry.time).toLocaleTimeString()}
              </span>
            </div>
          ))}
        </ContextPanel>

        <ContextPanel
          title="Nearby events"
          source={nearbySource(nearbyEvents.length)}
          loading={false}
          empty="No other events were recorded in this window."
          count={nearbyEvents.length}
        >
          {nearbyEvents.map(item => (
            <div key={item.event.id} style={rowStyle}>
              <span style={{
                width: 7,
                height: 7,
                marginTop: 4,
                flexShrink: 0,
                borderRadius: '50%',
                background: EVENT_SEVERITY_COLORS[item.event.severity],
              }} />
              <div style={{ minWidth: 0, flex: 1 }}>
                <strong style={{
                  display: 'block',
                  color: 'var(--text-primary)',
                  fontSize: 10,
                }}>
                  {item.event.title}
                </strong>
                <span style={{ color: 'var(--text-muted)', fontSize: 8 }}>
                  {formatContextOffset(item.distanceMs)} · {item.relation}
                  {' · '}{eventKindLabel(item.event)}
                </span>
              </div>
            </div>
          ))}
        </ContextPanel>
      </div>
    </div>
  );
};

const ContextPanel: React.FC<{
  title: string;
  source?: EventContextSource<unknown>;
  loading: boolean;
  empty: string;
  count?: number;
  children: React.ReactNode;
}> = ({ title, source, loading, empty, count, children }) => (
  <section style={{
    minWidth: 0,
    overflow: 'hidden',
    border: '1px solid var(--border-primary)',
    borderRadius: 7,
    background: 'var(--bg-card)',
  }}>
    <div style={{
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'space-between',
      gap: 8,
      padding: '8px 10px',
      borderBottom: '1px solid var(--border-primary)',
    }}>
      <strong style={{ color: 'var(--text-primary)', fontSize: 10 }}>
        {title}{count != null ? ` · ${count}` : ''}
      </strong>
      {source && <SourceStatus source={source} />}
    </div>
    <div style={{ padding: '2px 8px', maxHeight: 250, overflow: 'auto' }}>
      {loading ? (
        <div style={panelEmptyStyle}>Loading…</div>
      ) : source?.status === 'unavailable' ? (
        <div style={panelEmptyStyle}>{source.source} is not available.</div>
      ) : source?.status === 'failed' ? (
        <div style={panelEmptyStyle}>{source.detail ?? 'Source query failed.'}</div>
      ) : count === 0 ? (
        <div style={panelEmptyStyle}>{empty}</div>
      ) : children}
    </div>
  </section>
);

const SourceStatus: React.FC<{
  source: Pick<EventContextSource<unknown>, 'source' | 'status'>;
}> = ({ source }) => (
  <span
    title={`${source.source}: ${source.status}`}
    style={{
      display: 'inline-flex',
      alignItems: 'center',
      gap: 5,
      minWidth: 0,
      color: sourceStatusColor(source.status),
      fontSize: 8,
      fontFamily: "'Share Tech Mono', monospace",
    }}
  >
    <span style={{
      width: 5,
      height: 5,
      flexShrink: 0,
      borderRadius: '50%',
      background: sourceStatusColor(source.status),
    }} />
    {source.source}
  </span>
);

const MetricValue: React.FC<{ label: string; value: string }> = ({ label, value }) => (
  <div>
    <div style={{ color: 'var(--text-muted)', fontSize: 8 }}>{label}</div>
    <div style={{ color: 'var(--text-primary)', fontSize: 12, marginTop: 2 }}>{value}</div>
  </div>
);

const HostMetricsChart: React.FC<{
  points: readonly EventContextMetricPoint[];
  eventTime: string;
}> = ({ points, eventTime }) => {
  const width = 300;
  const height = 62;
  const [hoverIndex, setHoverIndex] = useState<number | null>(null);
  const geometry = useMemo(
    () => buildMetricChartGeometry(points, eventTime, width, height),
    [eventTime, points],
  );
  const hoveredPoint = hoverIndex == null ? null : points[hoverIndex];
  const hoverX = hoverIndex == null ? null : geometry.pointXs[hoverIndex];
  const startTime = points[0]?.time;
  const endTime = points[points.length - 1]?.time;

  return (
    <div style={{ marginTop: 8 }}>
      <div style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        gap: 8,
        marginBottom: 4,
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <MetricLegend color="#58a6ff" label="Memory · left axis" />
          <MetricLegend color="#3fb950" label="CPU · right axis" />
        </div>
        <MetricLegend color="#d29922" label="Event" dashed />
      </div>
      <div style={{
        display: 'grid',
        gridTemplateColumns: '47px minmax(0, 1fr) 47px',
        alignItems: 'stretch',
      }}>
        <MetricAxis
          values={[
            formatBytes(geometry.memoryAxis.maximum),
            formatBytes(geometry.memoryAxis.midpoint),
            formatBytes(geometry.memoryAxis.minimum),
          ]}
          align="left"
          color="#58a6ff"
        />
        <div style={{ minWidth: 0 }}>
          <div style={{ position: 'relative', height }}>
            <svg
              viewBox={`0 0 ${width} ${height}`}
              preserveAspectRatio="none"
              style={{ display: 'block', width: '100%', height }}
              aria-label="Memory and CPU around event"
              onMouseMove={mouseEvent => {
                const bounds = mouseEvent.currentTarget.getBoundingClientRect();
                const localX = (mouseEvent.clientX - bounds.left) / bounds.width * width;
                setHoverIndex(closestMetricPointIndex(points, localX, width));
              }}
              onMouseLeave={() => setHoverIndex(null)}
            >
              {[0, height / 2, height].map(y => (
                <line
                  key={y}
                  x1={0}
                  x2={width}
                  y1={y}
                  y2={y}
                  stroke="var(--border-primary)"
                  strokeWidth={0.7}
                  vectorEffect="non-scaling-stroke"
                />
              ))}
              <line
                x1={geometry.eventX}
                x2={geometry.eventX}
                y1={0}
                y2={height}
                stroke="#d29922"
                strokeWidth={1}
                strokeDasharray="3 2"
                vectorEffect="non-scaling-stroke"
              />
              {geometry.memoryPoints && (
                <polyline
                  points={geometry.memoryPoints}
                  fill="none"
                  stroke="#58a6ff"
                  strokeWidth={1.7}
                  vectorEffect="non-scaling-stroke"
                />
              )}
              {geometry.cpuPoints && (
                <polyline
                  points={geometry.cpuPoints}
                  fill="none"
                  stroke="#3fb950"
                  strokeWidth={1.5}
                  vectorEffect="non-scaling-stroke"
                />
              )}
              {hoverX != null && (
                <line
                  x1={hoverX}
                  x2={hoverX}
                  y1={0}
                  y2={height}
                  stroke="var(--text-secondary)"
                  strokeWidth={0.8}
                  vectorEffect="non-scaling-stroke"
                />
              )}
            </svg>
            {hoveredPoint && hoverX != null && (
              <div style={{
                position: 'absolute',
                zIndex: 2,
                top: 4,
                left: `${hoverX / width * 100}%`,
                transform: hoverX > width * 0.68
                  ? 'translateX(-100%)'
                  : hoverX < width * 0.32
                    ? 'translateX(0)'
                    : 'translateX(-50%)',
                minWidth: 128,
                padding: '6px 7px',
                border: '1px solid var(--border-primary)',
                borderRadius: 5,
                background: 'var(--bg-secondary)',
                boxShadow: '0 4px 14px rgba(0,0,0,0.18)',
                pointerEvents: 'none',
              }}>
                <div style={{ color: 'var(--text-primary)', fontSize: 8.5, marginBottom: 4 }}>
                  {formatMetricTime(hoveredPoint.time)}
                </div>
                <div style={{ color: '#58a6ff', fontSize: 8.5 }}>
                  Memory&nbsp; {formatBytes(hoveredPoint.memory_usage)}
                </div>
                <div style={{ color: '#3fb950', fontSize: 8.5, marginTop: 2 }}>
                  CPU&nbsp; {formatNumber(hoveredPoint.cpu_cores)} cores
                </div>
              </div>
            )}
          </div>
          <div style={{
            display: 'grid',
            gridTemplateColumns: '1fr auto 1fr',
            gap: 6,
            marginTop: 3,
            color: 'var(--text-muted)',
            fontSize: 7.5,
          }}>
            <span>{formatMetricTime(startTime)}</span>
            <span style={{ color: '#d29922' }}>event</span>
            <span style={{ textAlign: 'right' }}>{formatMetricTime(endTime)}</span>
          </div>
        </div>
        <MetricAxis
          values={[
            `${formatNumber(geometry.cpuAxis.maximum)}c`,
            `${formatNumber(geometry.cpuAxis.midpoint)}c`,
            `${formatNumber(geometry.cpuAxis.minimum)}c`,
          ]}
          align="right"
          color="#3fb950"
        />
      </div>
    </div>
  );
};

const MetricLegend: React.FC<{
  color: string;
  label: string;
  dashed?: boolean;
}> = ({ color, label, dashed }) => (
  <span style={{
    display: 'inline-flex',
    alignItems: 'center',
    gap: 4,
    color: 'var(--text-muted)',
    fontSize: 7.5,
    whiteSpace: 'nowrap',
  }}>
    <span style={{
      width: 12,
      borderTop: `2px ${dashed ? 'dashed' : 'solid'} ${color}`,
    }} />
    {label}
  </span>
);

const MetricAxis: React.FC<{
  values: readonly string[];
  align: 'left' | 'right';
  color: string;
}> = ({ values, align, color }) => (
  <div style={{
    height: 62,
    display: 'flex',
    flexDirection: 'column',
    justifyContent: 'space-between',
    color,
    fontSize: 7,
    textAlign: align === 'left' ? 'right' : 'left',
    paddingRight: align === 'left' ? 5 : 0,
    paddingLeft: align === 'right' ? 5 : 0,
    whiteSpace: 'nowrap',
  }}>
    {values.map(value => <span key={value}>{value}</span>)}
  </div>
);

const Tag: React.FC<{ label: string; color: string }> = ({ label, color }) => (
  <span style={{
    color,
    fontSize: 7.5,
    fontWeight: 700,
    textTransform: 'uppercase',
    letterSpacing: '0.35px',
  }}>
    {label}
  </span>
);

function nearbySource(count: number): EventContextSource<unknown> {
  return {
    source: 'loaded event set',
    capability: 'events',
    status: 'loaded',
    data: Array.from({ length: count }, () => ({})),
  };
}

function sourceStatusColor(status: EventContextSourceStatus): string {
  switch (status) {
    case 'loaded': return '#3fb950';
    case 'failed': return '#f85149';
    default: return '#8b949e';
  }
}

function logLevelColor(level: string): string {
  if (level === 'Fatal' || level === 'Critical') return '#f85149';
  if (level === 'Error') return '#f0883e';
  if (level === 'Warning') return '#d29922';
  return '#58a6ff';
}

function formatNumber(value: number): string {
  return value.toLocaleString(undefined, { maximumFractionDigits: 1 });
}

function formatMetricTime(value?: string): string {
  if (!value) return '';
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleTimeString([], {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
}

const smallButtonStyle: React.CSSProperties = {
  padding: '3px 7px',
  borderRadius: 5,
  border: '1px solid var(--border-primary)',
  background: 'var(--bg-card)',
  color: 'var(--text-muted)',
  fontSize: 9,
  cursor: 'pointer',
};

const rowStyle: React.CSSProperties = {
  display: 'flex',
  alignItems: 'flex-start',
  justifyContent: 'space-between',
  gap: 8,
  padding: '8px 2px',
  borderBottom: '1px solid var(--border-primary)',
};

const monoPrimaryStyle: React.CSSProperties = {
  color: 'var(--text-primary)',
  fontSize: 10,
  fontFamily: "'Share Tech Mono', monospace",
};

const monoSecondaryStyle: React.CSSProperties = {
  marginTop: 2,
  color: 'var(--text-muted)',
  fontSize: 8,
  fontFamily: "'Share Tech Mono', monospace",
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
};

const panelEmptyStyle: React.CSSProperties = {
  minHeight: 76,
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  padding: 10,
  color: 'var(--text-muted)',
  fontSize: 9,
  textAlign: 'center',
  overflowWrap: 'anywhere',
};

const errorStyle: React.CSSProperties = {
  marginBottom: 10,
  padding: '7px 9px',
  borderRadius: 5,
  border: '1px solid rgba(248,81,73,0.35)',
  background: 'rgba(248,81,73,0.08)',
  color: '#f85149',
  fontSize: 9,
};
