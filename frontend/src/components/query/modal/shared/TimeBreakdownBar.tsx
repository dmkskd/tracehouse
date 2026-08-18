/**
 * TimeBreakdownBar — what a duration was actually made of.
 *
 * The bar's *length* is wall clock (set by the caller's layout); its *fill* is a
 * thread-time composition. Those are two different clocks and mixing them is
 * what would make the bar lie: on a parallel query the thread-summed counters
 * routinely add up to several times the elapsed time, so they can only ever be
 * rendered as shares, never as widths on a wall-clock axis.
 *
 * See packages/core/src/utils/time-breakdown.ts for the composition itself.
 * Shared so the Distributed timeline can render per-node bars from the same
 * component (proposal phase 4).
 */

import React from 'react';
import type { TimeBreakdown, TimeBreakdownKey } from '@tracehouse/core';
import { TIME_BREAKDOWN_EVENTS, TIME_BREAKDOWN_DENOMINATOR } from '@tracehouse/core';

/** Work reads warm, waits read cool, the unknown reads grey. */
const SEGMENT_COLORS: Record<TimeBreakdownKey, string> = {
  cpu: '#d29922',
  disk_wait: '#7B83FF',
  cpu_wait: '#B682FF',
  network_wait: '#FF6692',
  unaccounted: '#6e7681',
};

// Kept to a few words each: this renders in a native title tooltip, which
// cannot be styled and turns anything longer into a wall of wrapped text.
const SEGMENT_HINTS: Record<TimeBreakdownKey, string> = {
  cpu: 'working',
  disk_wait: 'blocked on disk',
  cpu_wait: 'waiting for a free CPU',
  network_wait: 'blocked on a socket',
  // RealTimeMicroseconds is thread *lifetime*, not busy time, so an idle-but-
  // alive thread lands here. Three very different things look identical:
  // over-parallelised short queries (threads waiting for work), a distributed
  // coordinator waiting on shards (async epoll, which Network*Elapsed does not
  // time — measured at 10ms of a 5.63s wait), and genuine lock contention.
  // Hence naming the state rather than diagnosing a cause.
  unaccounted: 'thread alive but idle — waiting on shards, pipeline, or locks',
};

const pct = (share: number) => `${(share * 100).toFixed(share < 0.1 ? 1 : 0)}%`;

/**
 * 'full' lists every segment — needs real width, so it is for wide layouts.
 * 'dominant' is a single line naming the biggest cost, which is what fits in a
 * narrow card and is usually the only thing a reader wants at a glance.
 * 'none' leaves the bar to speak for itself with the tooltip behind it.
 */
export type TimeBreakdownLegend = 'full' | 'dominant' | 'none';

export const TimeBreakdownBar: React.FC<{
  breakdown: TimeBreakdown;
  legend?: TimeBreakdownLegend;
  height?: number;
}> = ({ breakdown, legend = 'full', height = 8 }) => {
  if (!breakdown.available) return null;

  const caveats = [
    breakdown.normalized ? '! counters overlapped past total; scaled to fit' : '',
    breakdown.diskWaitReported ? '' : '! no disk wait recorded (needs procfs)',
  ].filter(Boolean);

  const title = [
    `Time spent — share of ${TIME_BREAKDOWN_DENOMINATOR}, not elapsed time`,
    '',
    ...breakdown.segments.flatMap(s => [
      `${s.label} ${pct(s.share)} · ${SEGMENT_HINTS[s.key]}`,
      `  ${TIME_BREAKDOWN_EVENTS[s.key]}`,
    ]),
    ...(caveats.length ? ['', ...caveats] : []),
  ].join('\n');

  // Segments are already sorted widest-first with the residual last, so the
  // dominant cost is simply the head of the list.
  const legendItems = legend === 'dominant' ? breakdown.segments.slice(0, 1) : breakdown.segments;

  return (
    <div title={title} style={{ cursor: 'help' }}>
      <div style={{ display: 'flex', height, borderRadius: 999, overflow: 'hidden', background: 'var(--bg-tertiary)' }}>
        {breakdown.segments.map(s => (
          <div
            key={s.key}
            style={{
              width: `${s.share * 100}%`,
              height: '100%',
              background: SEGMENT_COLORS[s.key],
              // Hairline separators so adjacent segments stay distinguishable
              // for viewers who cannot rely on the hue difference.
              boxShadow: 'inset -1px 0 0 var(--bg-card)',
            }}
          />
        ))}
      </div>

      {legend !== 'none' && (
        // One line, clipped rather than wrapped. Wrapping turned this row into a
        // stack in narrow cards and collapsed the bar it was meant to explain.
        <div style={{
          display: 'flex', gap: 6, marginTop: 4,
          fontFamily: 'monospace', fontSize: 9, color: 'var(--text-muted)',
          whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis',
        }}>
          {legendItems.map(s => (
            <span key={s.key} style={{ display: 'inline-flex', alignItems: 'center', gap: 3, flexShrink: 0 }}>
              <span style={{ width: 6, height: 6, borderRadius: 2, background: SEGMENT_COLORS[s.key], flexShrink: 0 }} />
              {s.label.toLowerCase()} {pct(s.share)}
            </span>
          ))}
          {breakdown.normalized && <span title={caveats[0]} style={{ color: '#FFA15A', flexShrink: 0 }}>⚠ scaled</span>}
        </div>
      )}
    </div>
  );
};
