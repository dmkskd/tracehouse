/**
 * DistributedQueryTopology — Gantt-style timeline showing coordinator + shard sub-queries.
 * Renders inline metrics on each bar and supports click-to-navigate between queries.
 */

import React, { useMemo } from 'react';
import type { SubQueryInfo } from '@tracehouse/core';
import { formatDurationMs } from '../../../../utils/formatters';
import { formatBytes } from '../../../../stores/databaseStore';

export interface TopologyCoordinator {
  query_id: string;
  hostname: string;
  query_duration_ms: number;
  query_start_time_microseconds: string;
  memory_usage: number;
  read_rows: number;
  exception?: string;
}

interface DistributedQueryTopologyProps {
  coordinator: TopologyCoordinator;
  subQueries: SubQueryInfo[];
  /** The query_id currently being viewed (to highlight "you are here") */
  activeQueryId: string;
  /** Navigate to a query by ID */
  onNavigate: (queryId: string) => void;
  isLoading?: boolean;
}

const COORD_COLOR = '#58a6ff';
const SHARD_COLOR = '#d29922';
const ERROR_COLOR = '#f85149';


/** Parse ClickHouse microsecond timestamp to epoch microseconds */
function parseUs(ts: string): number {
  if (!ts) return 0;
  const dotIdx = ts.lastIndexOf('.');
  const baseDateStr = dotIdx >= 0 ? ts.substring(0, dotIdx) : ts;
  const usFrac = dotIdx >= 0 ? ts.substring(dotIdx + 1) : '0';
  const baseMs = new Date(baseDateStr.replace(' ', 'T') + 'Z').getTime();
  return baseMs * 1000 + parseInt(usFrac.padEnd(6, '0').substring(0, 6), 10);
}

export const DistributedQueryTopology: React.FC<DistributedQueryTopologyProps> = ({
  coordinator,
  subQueries,
  activeQueryId,
  onNavigate,
  isLoading,
}) => {
  const timeline = useMemo(() => {
    const coordStartUs = parseUs(coordinator.query_start_time_microseconds);
    const coordDurationUs = coordinator.query_duration_ms * 1000;
    const totalDurationUs = Math.max(1, coordDurationUs);

    const shards = subQueries.map(sq => {
      const shardStartUs = parseUs(sq.query_start_time_microseconds);
      const shardDurationUs = sq.query_duration_ms * 1000;
      const offsetUs = Math.max(0, shardStartUs - coordStartUs);
      return {
        ...sq,
        offsetUs,
        durationUs: shardDurationUs,
      };
    });

    // Sort by start offset, then longest first
    shards.sort((a, b) => a.offsetUs - b.offsetUs || b.durationUs - a.durationUs);

    return { coordDurationUs, totalDurationUs, shards };
  }, [coordinator, subQueries]);

  if (isLoading) {
    return (
      <div style={{ fontSize: 11, color: 'var(--text-muted)', padding: '8px 0' }}>
        Loading topology…
      </div>
    );
  }

  if (subQueries.length === 0) return null;

  const fmtMs = formatDurationMs;
  const LABEL_W = 120;
  const METRIC_W = 70;

  const maxShardDuration = Math.max(...subQueries.map(s => s.query_duration_ms));
  const overhead = coordinator.query_duration_ms - maxShardDuration;

  return (
    <div>
      {/* Header */}
      <div style={{
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        marginBottom: 8,
      }}>
        <div style={{ fontSize: 9, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '1px' }}>
          Distributed Query Topology ({subQueries.length} shard{subQueries.length !== 1 ? 's' : ''})
        </div>
        {overhead > 0 && (
          <div style={{ fontSize: 10, color: 'var(--text-muted)' }}>
            Coordinator overhead: <span style={{ color: COORD_COLOR, fontFamily: 'var(--font-mono, monospace)' }}>{fmtMs(overhead)}</span>
          </div>
        )}
      </div>

      {/* Time axis */}
      <div style={{
        display: 'flex', justifyContent: 'space-between',
        marginLeft: LABEL_W, marginRight: METRIC_W,
        marginBottom: 3,
        fontSize: 9, color: 'var(--text-muted)', fontFamily: 'var(--font-mono, monospace)',
      }}>
        <span>0</span>
        <span>{fmtMs(coordinator.query_duration_ms / 4)}</span>
        <span>{fmtMs(coordinator.query_duration_ms / 2)}</span>
        <span>{fmtMs(coordinator.query_duration_ms * 3 / 4)}</span>
        <span>{fmtMs(coordinator.query_duration_ms)}</span>
      </div>
      <div style={{ marginLeft: LABEL_W, marginRight: METRIC_W, height: 1, background: 'var(--border-primary)', marginBottom: 6 }} />

      {/* Coordinator bar */}
      <TopologyBar
        label="Coordinator"
        hostname={coordinator.hostname}
        leftPct={0}
        widthPct={100}
        color={COORD_COLOR}
        durationMs={coordinator.query_duration_ms}
        memoryUsage={coordinator.memory_usage}
        readRows={coordinator.read_rows}
        hasError={!!coordinator.exception}
        isActive={activeQueryId === coordinator.query_id}
        onClick={() => onNavigate(coordinator.query_id)}
        isCoordinator
        labelWidth={LABEL_W}
        metricWidth={METRIC_W}
      />

      {/* Separator */}
      <div style={{ marginLeft: LABEL_W, marginRight: METRIC_W, height: 1, background: 'var(--border-primary)', margin: '4px 0', opacity: 0.5 }} />

      {/* Shard bars */}
      {timeline.shards.map((sq, i) => {
        const leftPct = (sq.offsetUs / timeline.totalDurationUs) * 100;
        const widthPct = Math.max(0.5, (sq.durationUs / timeline.totalDurationUs) * 100);
        return (
          <TopologyBar
            key={sq.query_id || i}
            label={sq.hostname}
            leftPct={leftPct}
            widthPct={widthPct}
            color={sq.exception_code ? ERROR_COLOR : SHARD_COLOR}
            durationMs={sq.query_duration_ms}
            memoryUsage={sq.memory_usage}
            readRows={sq.read_rows}
            hasError={!!sq.exception_code}
            isActive={activeQueryId === sq.query_id}
            onClick={() => onNavigate(sq.query_id)}
            labelWidth={LABEL_W}
            metricWidth={METRIC_W}
          />
        );
      })}

      {/* Legend */}
      <div style={{ marginTop: 10, marginLeft: LABEL_W, display: 'flex', gap: 16, flexWrap: 'wrap', fontSize: 10, color: 'var(--text-muted)' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
          <div style={{ width: 8, height: 8, borderRadius: 2, background: COORD_COLOR }} />
          Coordinator
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
          <div style={{ width: 8, height: 8, borderRadius: 2, background: SHARD_COLOR }} />
          Shard
        </div>
        {subQueries.some(sq => sq.exception_code) && (
          <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
            <div style={{ width: 8, height: 8, borderRadius: 2, background: ERROR_COLOR }} />
            Error
          </div>
        )}
      </div>
    </div>
  );
};

/** Single bar in the topology Gantt */
const TopologyBar: React.FC<{
  label: string;
  hostname?: string;
  leftPct: number;
  widthPct: number;
  color: string;
  durationMs: number;
  memoryUsage: number;
  readRows: number;
  hasError: boolean;
  isActive: boolean;
  isCoordinator?: boolean;
  onClick: () => void;
  labelWidth: number;
  metricWidth: number;
}> = ({
  label, hostname, leftPct, widthPct, color, durationMs, memoryUsage, readRows,
  hasError, isActive, isCoordinator, onClick, labelWidth, metricWidth,
}) => {
  const fmtMs = formatDurationMs;
  const fmtCompact = (n: number) => {
    if (n < 1000) return n.toString();
    if (n < 1_000_000) return `${(n / 1000).toFixed(1)}K`;
    if (n < 1_000_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
    return `${(n / 1_000_000_000).toFixed(1)}B`;
  };

  return (
    <div
      onClick={onClick}
      style={{
        display: 'flex', alignItems: 'center', height: 24, marginBottom: 2,
        cursor: 'pointer',
        borderRadius: 3,
        transition: 'background 0.1s',
      }}
      onMouseEnter={e => { e.currentTarget.style.background = 'rgba(255,255,255,0.03)'; }}
      onMouseLeave={e => { e.currentTarget.style.background = 'transparent'; }}
    >
      {/* Label */}
      <div style={{
        width: labelWidth, flexShrink: 0,
        fontSize: 10, fontFamily: 'var(--font-mono, monospace)',
        color: isActive ? (isCoordinator ? COORD_COLOR : SHARD_COLOR) : 'var(--text-muted)',
        overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
        paddingRight: 6,
        fontWeight: isActive || isCoordinator ? 600 : 400,
      }} title={hostname || label}>
        {label}
        {isActive && !isCoordinator && (
          <span style={{ fontSize: 8, color: SHARD_COLOR, marginLeft: 4 }}>◂</span>
        )}
      </div>

      {/* Bar track */}
      <div style={{
        flex: 1, position: 'relative', height: 16,
        background: 'var(--bg-tertiary)', borderRadius: 3, overflow: 'hidden',
      }}>
        <div
          title={`${fmtMs(durationMs)} · ${formatBytes(memoryUsage)} · ${fmtCompact(readRows)} rows`}
          style={{
            position: 'absolute',
            left: `${leftPct}%`,
            width: `${widthPct}%`,
            height: '100%',
            background: color,
            borderRadius: 2,
            opacity: isActive ? 1 : 0.5,
            border: 'none',
            boxShadow: 'none',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'flex-start',
            paddingLeft: 4,
            paddingRight: 4,
            boxSizing: 'border-box',
            minWidth: 0,
          }}
        >
          {/* Inline metrics on the bar */}
          {widthPct > 12 && (
            <span style={{
              fontSize: 9, color: '#fff', fontFamily: 'var(--font-mono, monospace)',
              whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis',
              textShadow: '0 1px 2px rgba(0,0,0,0.5)',
              fontWeight: 500,
            }}>
              {fmtMs(durationMs)}
              {widthPct > 25 && ` · ${formatBytes(memoryUsage)}`}
              {widthPct > 40 && ` · ${fmtCompact(readRows)} rows`}
            </span>
          )}
        </div>
      </div>

      {/* Right-side duration (always visible, even for tiny bars) */}
      <div style={{
        width: metricWidth, flexShrink: 0, textAlign: 'right',
        fontSize: 10, fontFamily: 'var(--font-mono, monospace)',
        color: hasError ? ERROR_COLOR : 'var(--text-muted)',
        paddingLeft: 6,
      }}>
        {hasError ? '✗ ' : ''}{fmtMs(durationMs)}
      </div>
    </div>
  );
};

export default DistributedQueryTopology;
