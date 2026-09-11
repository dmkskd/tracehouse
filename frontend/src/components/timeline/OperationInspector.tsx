/**
 * OperationInspector — right-hand panel of the Time Travel page.
 *
 * Nothing selected: what the load is made of at the hovered or pinned instant,
 * split by operation type, next to the measured server value.
 *
 * An operation selected: which one it is, where it sits in the window, how much
 * of the load it accounts for, and what else was running at the same time.
 *
 * A base ClickHouse install records only lifetime totals per operation, so the
 * type stack is composed from average rates. It is labelled as such and never
 * presented as the measured value.
 *
 * All shaping lives in @tracehouse/core (buildInstantBreakdown,
 * buildOperationContext, buildOperationSpan, summarizeOperations).
 */
import React from 'react';
import type {
  InstantBreakdown,
  OperationContext,
  OperationRow,
  OperationSpan,
  OperationSummary,
} from '@tracehouse/core';
import { formatBytes, formatMicroseconds } from '../../utils/formatters';
import { formatDurationMs as fmtMs } from '../../utils/formatters';
import { TruncatedHost } from '../common/TruncatedHost';
import {
  type MetricMode, METRIC_CONFIG,
  Q_COLORS, M_COLORS, MUT_COLORS, operationBandColor, segmentColor,
} from './timeline-constants';

const KIND_ACCENT: Record<OperationRow['kind'], string> = {
  query: Q_COLORS[0],
  merge: M_COLORS[0],
  mutation: MUT_COLORS[0],
};

const KIND_NOUN: Record<OperationRow['kind'], string> = {
  query: 'Selected query',
  merge: 'Selected merge',
  mutation: 'Selected mutation',
};

const pct = (fraction: number): string => `${(fraction * 100).toFixed(fraction < 0.1 ? 1 : 0)}%`;

const Card: React.FC<{ children: React.ReactNode }> = ({ children }) => (
  <div style={{
    borderRadius: 10, background: 'var(--bg-secondary)',
    border: '1px solid var(--border-primary)', padding: '12px 14px',
  }}>{children}</div>
);

const Field: React.FC<{ label: string; children: React.ReactNode; mono?: boolean }> = ({ label, children, mono }) => (
  <div style={{ display: 'flex', alignItems: 'baseline', gap: 8, minWidth: 0 }}>
    <span style={{ fontSize: 10, color: 'var(--text-muted)', width: 62, flexShrink: 0 }}>{label}</span>
    <span style={{
      fontSize: 11, color: 'var(--text-primary)', minWidth: 0,
      fontFamily: mono ? 'monospace' : undefined,
      overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
    }}>{children}</span>
  </div>
);

/** Stacked bar of the type breakdown, scaled to capacity when there is one. */
const SegmentBar: React.FC<{ breakdown: InstantBreakdown; metricMode: MetricMode }> = ({ breakdown, metricMode }) => {
  const { segments, capacity, shownTotal, shownShareOfCapacity } = breakdown;
  // With a capacity the bar reads as utilisation; without one it fills the width.
  const scale = capacity && capacity > 0 ? capacity : shownTotal;
  if (scale <= 0) return null;
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 10 }}>
      <div style={{
        flex: 1, display: 'flex', height: 22, borderRadius: 5, overflow: 'hidden',
        background: 'var(--bg-tertiary)',
      }}>
        {segments.map(segment => {
          const width = (segment.value / scale) * 100;
          if (width <= 0) return null;
          return (
            <div key={segment.key}
              title={`${segment.key} · ${segment.count} active · ${METRIC_CONFIG[metricMode].fmtVal(segment.value)}`}
              style={{
                width: `${width}%`, background: segmentColor(segment.key, segment.kind),
                display: 'flex', alignItems: 'center', justifyContent: 'center',
                fontSize: 9, fontWeight: 600, color: '#fff', overflow: 'hidden', whiteSpace: 'nowrap',
              }}>
              {width > 7 ? segment.count : ''}
            </div>
          );
        })}
      </div>
      <span style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-primary)', fontFamily: "'Share Tech Mono',monospace" }}>
        {shownShareOfCapacity !== null ? pct(shownShareOfCapacity) : METRIC_CONFIG[metricMode].fmtVal(shownTotal)}
      </span>
    </div>
  );
};

/** Where the operation sits inside the window, with the pin marked. */
const SpanBar: React.FC<{ span: OperationSpan; color: string }> = ({ span, color }) => (
  <div style={{ position: 'relative', height: 8, borderRadius: 4, background: 'var(--bg-tertiary)', margin: '2px 0 8px' }}>
    <div style={{
      position: 'absolute', top: 0, bottom: 0,
      left: `${span.startFraction * 100}%`,
      width: `${Math.max(0.8, (span.endFraction - span.startFraction) * 100)}%`,
      background: color,
      borderTopLeftRadius: span.clippedStart ? 0 : 4, borderBottomLeftRadius: span.clippedStart ? 0 : 4,
      borderTopRightRadius: span.clippedEnd ? 0 : 4, borderBottomRightRadius: span.clippedEnd ? 0 : 4,
    }} />
    {span.pinFraction !== null && (
      <div style={{
        position: 'absolute', top: -2, bottom: -2, left: `${span.pinFraction * 100}%`,
        width: 2, marginLeft: -1, background: '#3fb950',
      }} />
    )}
  </div>
);

const InstantPanel: React.FC<{
  breakdown: InstantBreakdown;
  summary: OperationSummary;
  metricMode: MetricMode;
  isPinned: boolean;
  hasInstant: boolean;
}> = ({ breakdown, summary, metricMode, isPinned, hasInstant }) => {
  const metric = METRIC_CONFIG[metricMode];

  if (!hasInstant) {
    return (
      <Card>
        <div style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-primary)', marginBottom: 8 }}>Window</div>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
          <Field label="Operations" mono>{summary.counts.all.toLocaleString()}</Field>
          <Field label={`Total ${metric.label.toLowerCase()}`} mono>{metric.fmtVal(summary.metricTotal)}</Field>
        </div>
        <div style={{ marginTop: 10, fontSize: 10, color: 'var(--text-muted)', lineHeight: 1.5 }}>
          Hover the chart to see what the load is made of. Click a band or a row to inspect one operation.
        </div>
      </Card>
    );
  }

  return (
    <Card>
      <div style={{ display: 'flex', alignItems: 'baseline', justifyContent: 'space-between', marginBottom: 2 }}>
        <span style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-primary)' }}>
          At {new Date(breakdown.atMs).toLocaleTimeString()}
        </span>
        <span style={{ fontSize: 9, color: isPinned ? '#3fb950' : 'var(--text-muted)' }}>
          {isPinned ? 'pinned' : 'hovering'}
        </span>
      </div>
      <div style={{ fontSize: 10, color: 'var(--text-muted)', marginBottom: 6 }}>
        {breakdown.hasSamples ? `Sampled ${metric.label.toLowerCase()}` : `Estimated ${metric.label.toLowerCase()}`} · shown operations
      </div>

      <SegmentBar breakdown={breakdown} metricMode={metricMode} />

      {breakdown.segments.length > 0 ? (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
          {breakdown.segments.map(segment => (
            <div key={segment.key} style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 11 }}>
              <span style={{ width: 9, height: 9, borderRadius: 2, background: segmentColor(segment.key, segment.kind), flexShrink: 0 }} />
              <span style={{ color: 'var(--text-secondary)', flex: 1 }}>{segment.key}</span>
              <span style={{ color: 'var(--text-muted)', fontSize: 9 }}>{segment.count}</span>
              <span style={{ color: 'var(--text-primary)', fontFamily: "'Share Tech Mono',monospace", width: 52, textAlign: 'right' }}>
                {segment.shareOfCapacity !== null ? pct(segment.shareOfCapacity) : metric.fmtVal(segment.value)}
              </span>
            </div>
          ))}
        </div>
      ) : (
        <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>No operations active at this instant.</div>
      )}

      <div style={{ marginTop: 10, paddingTop: 8, borderTop: '1px solid var(--border-primary)', display: 'flex', alignItems: 'center', gap: 8, fontSize: 11 }}>
        <span style={{ width: 14, height: 2, background: metric.color, flexShrink: 0 }} />
        <span style={{ color: 'var(--text-secondary)', flex: 1 }}>Measured {metric.label.toLowerCase()}</span>
        <span style={{ color: 'var(--text-primary)', fontFamily: "'Share Tech Mono',monospace" }}>
          {breakdown.measured === null
            ? 'no sample'
            : breakdown.measuredShareOfCapacity !== null
              ? pct(breakdown.measuredShareOfCapacity)
              : metric.fmtVal(breakdown.measured)}
        </span>
      </div>

      <div style={{ marginTop: 8, fontSize: 9, color: 'var(--text-muted)', lineHeight: 1.5 }}>
        {breakdown.hasSamples
          ? 'Per-second samples where available; other bands use operation-average rates.'
          : 'Bands use each operation’s average rate over its lifetime, not a per-second measurement.'}
      </div>
    </Card>
  );
};

export const OperationInspector: React.FC<{
  /** The operation being inspected, or null for the instant breakdown. */
  selected: OperationRow | null;
  /** Type breakdown at the hovered or pinned instant. */
  breakdown: InstantBreakdown;
  /** True when the instant comes from a pin rather than the cursor. */
  isPinned: boolean;
  /** False when neither a pin nor a hover gives us an instant. */
  hasInstant: boolean;
  /** Concurrency context for the selected operation. */
  context: OperationContext;
  /** Where the selected operation sits in the window. */
  span: OperationSpan | null;
  summary: OperationSummary;
  metricMode: MetricMode;
  showHost: boolean;
  onClear: () => void;
  onSelect: (row: OperationRow) => void;
  onOpenDetails: (row: OperationRow) => void;
}> = ({
  selected, breakdown, isPinned, hasInstant, context, span, summary,
  metricMode, showHost, onClear, onSelect, onOpenDetails,
}) => {
  if (!selected || !span) {
    return (
      <InstantPanel breakdown={breakdown} summary={summary} metricMode={metricMode}
        isPinned={isPinned} hasInstant={hasInstant} />
    );
  }

  const accent = operationBandColor(selected.kind, selected.idx);
  const metric = METRIC_CONFIG[metricMode];

  return (
    <Card>
      {/* Identity */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 10 }}>
        <span style={{ width: 8, height: 8, borderRadius: 2, background: accent, flexShrink: 0 }} />
        <span style={{ fontSize: 13, fontWeight: 600, color: 'var(--text-primary)' }}>{KIND_NOUN[selected.kind]}</span>
        <span style={{
          padding: '1px 6px', fontSize: 9, fontWeight: 600, borderRadius: 4, letterSpacing: '0.3px',
          background: `${KIND_ACCENT[selected.kind]}22`, color: KIND_ACCENT[selected.kind],
        }}>{selected.kindLabel}</span>
        <button type="button" onClick={onClear} title="Clear selection" aria-label="Clear selection"
          style={{ marginLeft: 'auto', border: 'none', background: 'transparent', color: 'var(--text-muted)', cursor: 'pointer', fontSize: 12, padding: 2 }}>
          ✕
        </button>
      </div>

      <div style={{ display: 'flex', flexDirection: 'column', gap: 4, marginBottom: 10 }}>
        <Field label={selected.kind === 'query' ? 'ID' : 'Part'} mono>{selected.id}</Field>
        {showHost && <Field label="Host" mono>{selected.hostname ? <TruncatedHost name={selected.hostname} maxLen={18} /> : '—'}</Field>}
        {selected.kind === 'query'
          ? <Field label="User">{selected.user ?? '—'}</Field>
          : <Field label="Table" mono>{selected.label}</Field>}
        {selected.kind === 'query' && <Field label="Query" mono>{selected.label}</Field>}
        {selected.kind === 'merge' && selected.mergeReason && <Field label="Reason">{selected.mergeReason}</Field>}
      </div>

      {/* Where it sits in the window */}
      <div style={{ fontSize: 9, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.4px', marginBottom: 2 }}>
        In this window
      </div>
      <SpanBar span={span} color={accent} />
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '4px 10px', marginBottom: 12 }}>
        <Field label="Started" mono>{new Date(selected.startMs).toLocaleTimeString()}</Field>
        <Field label="Duration" mono>{selected.isRunning ? `${fmtMs(selected.durationMs)} (running)` : fmtMs(selected.durationMs)}</Field>
        <Field label="CPU time" mono>{formatMicroseconds(selected.cpuUs)}</Field>
        <Field label="Peak mem" mono>{formatBytes(selected.peakMemory)}</Field>
        <Field label="Disk I/O" mono>{formatBytes(selected.diskBytes)}</Field>
        <Field label="Network" mono>{formatBytes(selected.netBytes)}</Field>
      </div>

      {/* Share of the concurrent load */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
        <span style={{ fontSize: 11, color: 'var(--text-secondary)', flex: 1 }}>
          Share of {metric.label.toLowerCase()} while it ran
        </span>
        <span style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-primary)', fontFamily: "'Share Tech Mono',monospace" }}>
          {pct(context.concurrentShare)}
        </span>
      </div>
      <div style={{ height: 6, borderRadius: 3, background: 'var(--bg-tertiary)', overflow: 'hidden', marginBottom: 4 }}>
        <div style={{ width: `${Math.min(100, context.concurrentShare * 100)}%`, height: '100%', background: accent }} />
      </div>
      <div style={{ fontSize: 9, color: 'var(--text-muted)', marginBottom: 12 }}>
        #{context.rank} of {context.neighborCount + 1} operations running at the same time · {pct(context.metricShare)} of the whole window
      </div>

      {/* What else was running */}
      <div style={{ fontSize: 9, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.4px', marginBottom: 4 }}>
        Ran at the same time{context.neighborCount > context.neighbors.length ? ` · top ${context.neighbors.length} of ${context.neighborCount}` : ''}
      </div>
      {context.neighbors.length > 0 ? (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 2, marginBottom: 12 }}>
          {context.neighbors.map(neighbor => (
            <button key={`${neighbor.kind}-${neighbor.id}-${neighbor.hostname ?? ''}`}
              type="button" onClick={() => onSelect(neighbor)}
              title={`${neighbor.kindLabel} · ${neighbor.label}`}
              style={{
                display: 'flex', alignItems: 'center', gap: 6, width: '100%', textAlign: 'left',
                border: 'none', background: 'transparent', padding: '3px 4px', borderRadius: 5,
                cursor: 'pointer', fontSize: 11, color: 'var(--text-secondary)',
              }}>
              <span style={{ width: 8, height: 8, borderRadius: 2, flexShrink: 0, background: operationBandColor(neighbor.kind, neighbor.idx) }} />
              <span style={{ flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                {neighbor.kind === 'query' ? neighbor.label : `${neighbor.kindLabel} ${neighbor.label}`}
              </span>
              <span style={{ fontFamily: "'Share Tech Mono',monospace", color: 'var(--text-muted)', fontSize: 10 }}>
                {metric.fmtVal(neighbor.metricValue)}
              </span>
            </button>
          ))}
        </div>
      ) : (
        <div style={{ fontSize: 11, color: 'var(--text-muted)', marginBottom: 12 }}>
          Nothing else was running at the same time.
        </div>
      )}

      <button type="button" onClick={() => onOpenDetails(selected)}
        style={{
          width: '100%', padding: '7px 12px', borderRadius: 7, fontSize: 11, fontWeight: 600, cursor: 'pointer',
          background: 'rgba(88,166,255,0.12)', border: '1px solid rgba(88,166,255,0.3)', color: '#58a6ff',
        }}>
        Open {selected.kind} details ↗
      </button>
    </Card>
  );
};
