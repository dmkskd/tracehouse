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

import React, { useCallback, useEffect, useRef, useState } from 'react';
import type { TimeBreakdown } from '@tracehouse/core';
import { TIME_BREAKDOWN_EVENTS, TIME_BREAKDOWN_DENOMINATOR, pipelineStallHint, totalBlockedSamples, MIN_BLOCKED_SAMPLES } from '@tracehouse/core';
import type { ParkedTimeExplanation } from '../hooks/useParkedTimeExplanation';
import { TimeBreakdownPopover, type PopoverLayer } from './TimeBreakdownPopover';
import { SEGMENT_COLORS, SEGMENT_HINTS, pct, PARKED_EXPLANATION_THRESHOLD, fmtWait } from './timeBreakdownDisplay';

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
  /**
   * Optional deeper explanation of the parked segment (layers 2 and 3). The bar
   * renders fine without it — this only enriches the tooltip.
   */
  parked?: ParkedTimeExplanation;
}> = ({ breakdown, legend = 'full', height = 8, parked }) => {
  // The anchor's rect is captured on hover rather than held as an element ref:
  // the popover positions from it directly, so there is no second render to
  // measure and place, and nothing to strand the panel off-screen.
  const [anchorRect, setAnchorRect] = useState<DOMRect | null>(null);

  // Closing is deferred so the pointer can cross the gap into the popover to
  // read or select from it. Without this the panel vanishes the moment you move
  // towards it, which reads as the hover being broken.
  const closeTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const cancelClose = useCallback(() => {
    if (closeTimer.current) clearTimeout(closeTimer.current);
    closeTimer.current = null;
  }, []);
  const scheduleClose = useCallback(() => {
    cancelClose();
    closeTimer.current = setTimeout(() => setAnchorRect(null), 160);
  }, [cancelClose]);

  useEffect(() => cancelClose, [cancelClose]);

  if (!breakdown.available) return null;

  const caveats = [
    breakdown.normalized ? '! counters overlapped past total; scaled to fit' : '',
    breakdown.diskWaitReported ? '' : '! no disk wait recorded (needs procfs)',
    breakdown.handlerThreadExcluded ? 'connection handler thread discounted' : '',
  ].filter(Boolean);

  // Layers 2 and 3 explain the parked segment. They answer different questions
  // — which stage stalled, versus what it was blocked in — so each gets its own
  // section named after the table it rests on. A reader should never have to
  // guess which of the three sources a claim comes from, especially since layers
  // 2 and 3 are absent on many deployments.
  //
  // Only shown when there is parked time to explain. A query that is fully
  // accounted for (all CPU, queue and network) has nothing for these layers to
  // say, and rendering "no data" there implies a failed lookup rather than a
  // question that was never worth asking.
  const parkedShare = breakdown.segments.find(s => s.key === 'unaccounted')?.share ?? 0;
  const layers: PopoverLayer[] = [];
  if (parked && parkedShare >= PARKED_EXPLANATION_THRESHOLD) {
    layers.push({
      source: 'processors_profile_log',
      question: 'which stage stalled',
      // Per-processor waits overlap across concurrently blocked processors —
      // measured at 3x-100x a query's thread-time residual — so this ranks
      // stages and cannot be read as a share of the segment above. Say so,
      // rather than letting a bare duration imply it adds up.
      note: parked.stall ? 'this waiting happens inside the parked time, but stages overlap so it cannot be sized against it' : undefined,
      unavailable: !parked.layers.pipeline
        ? 'unavailable on this deployment'
        : (!parked.stall ? 'no stalled stage recorded' : undefined),
      rows: parked.stall
        ? [{
            label: parked.stall.processor,
            pct: fmtWait(parked.stall.waitUs),
            hint: pipelineStallHint(parked.stall.kind),
          }]
        : [],
    });

    // Real traces fire on a fixed period, so short queries yield few or no
    // samples — measured: only 49% of sub-100ms queries had any. Below the floor
    // the shares are noise, so report the sample count instead of percentages.
    const samples = totalBlockedSamples(parked.blocked);
    const tooFewSamples = samples > 0 && samples < MIN_BLOCKED_SAMPLES;
    const blocked = tooFewSamples ? [] : parked.blocked.filter(category => category.share >= 0.05);
    layers.push({
      source: 'trace_log · Real',
      question: samples > 0 ? `what it blocked in · ${samples} samples` : 'what it blocked in',
      note: blocked.length > 0 ? 'share of sampled blocked stacks' : undefined,
      // The capability existing but yielding nothing is indistinguishable from
      // denied introspection, stripped symbols, or a zero profiler period — so
      // say what is needed rather than guessing which one failed.
      unavailable: !parked.layers.stacks
        ? 'unavailable on this deployment'
        : tooFewSamples
          ? `only ${samples} samples — too few to apportion (sampled every 10ms)`
          : parked.introspectionDenied
          ? 'introspection functions not permitted for this user'
          : (blocked.length === 0 ? 'no blocked samples recorded for this query' : undefined),
      rows: blocked.map(category => ({
        label: category.label,
        pct: pct(category.share),
        hint: category.hint,
      })),
    });
  }

  // Segments are already sorted widest-first with the residual last, so the
  // dominant cost is simply the head of the list.
  const legendItems = legend === 'dominant' ? breakdown.segments.slice(0, 1) : breakdown.segments;

  return (
    <div
      onMouseEnter={event => {
        cancelClose();
        setAnchorRect(event.currentTarget.getBoundingClientRect());
      }}
      onMouseLeave={scheduleClose}
      style={{ cursor: 'help' }}
    >
      {anchorRect && (
        <TimeBreakdownPopover
          anchor={anchorRect}
          title={`Time spent — share of worker thread time (${TIME_BREAKDOWN_DENOMINATOR})`}
          segments={breakdown.segments.map(s => ({
            label: s.label,
            color: SEGMENT_COLORS[s.key],
            pct: pct(s.share),
            hint: SEGMENT_HINTS[s.key],
            source: TIME_BREAKDOWN_EVENTS[s.key],
          }))}
          layers={layers}
          layersHeading={`explaining parked ${pct(parkedShare)}`}
          caveats={caveats}
          onPointerEnter={cancelClose}
          onPointerLeave={scheduleClose}
        />
      )}
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
