import { describe, it, expect } from 'vitest';
import { computeTimeBreakdown, dominantSegment, waitShare } from '../time-breakdown.js';

const S = 1_000_000; // one thread-second in microseconds

describe('computeTimeBreakdown', { tags: ['observability'] }, () => {
  it('composes named segments plus a residual against RealTimeMicroseconds', () => {
    const b = computeTimeBreakdown({
      RealTimeMicroseconds: 100 * S,
      OSCPUVirtualTimeMicroseconds: 20 * S,
      OSIOWaitMicroseconds: 10 * S,
      OSCPUWaitMicroseconds: 5 * S,
      NetworkReceiveElapsedMicroseconds: 15 * S,
    });

    expect(b.available).toBe(true);
    expect(b.normalized).toBe(false);
    expect(b.totalUs).toBe(100 * S);

    const byKey = Object.fromEntries(b.segments.map(s => [s.key, s.share]));
    expect(byKey.cpu).toBeCloseTo(0.2, 6);
    expect(byKey.disk_wait).toBeCloseTo(0.1, 6);
    expect(byKey.cpu_wait).toBeCloseTo(0.05, 6);
    expect(byKey.network_wait).toBeCloseTo(0.15, 6);
    // 100 - 50 named = 50 unaccounted
    expect(byKey.unaccounted).toBeCloseTo(0.5, 6);

    const total = b.segments.reduce((sum, s) => sum + s.share, 0);
    expect(total).toBeCloseTo(1, 6);
  });

  it('sums both directions of network wait', () => {
    const b = computeTimeBreakdown({
      RealTimeMicroseconds: 10 * S,
      NetworkReceiveElapsedMicroseconds: 3 * S,
      NetworkSendElapsedMicroseconds: 2 * S,
    });
    expect(b.segments.find(s => s.key === 'network_wait')?.share).toBeCloseTo(0.5, 6);
  });

  it('falls back to User+System when OSCPUVirtualTime is absent', () => {
    const b = computeTimeBreakdown({
      RealTimeMicroseconds: 10 * S,
      UserTimeMicroseconds: 6 * S,
      SystemTimeMicroseconds: 2 * S,
    });
    expect(b.segments.find(s => s.key === 'cpu')?.share).toBeCloseTo(0.8, 6);
  });

  it('normalizes and drops the residual when counters overlap past real time', () => {
    // ClickHouse accounting paths overlap, so named segments can exceed
    // RealTimeMicroseconds. Scale to fit rather than overflowing the bar.
    const b = computeTimeBreakdown({
      RealTimeMicroseconds: 10 * S,
      OSCPUVirtualTimeMicroseconds: 8 * S,
      NetworkReceiveElapsedMicroseconds: 12 * S,
    });

    expect(b.normalized).toBe(true);
    expect(b.totalUs).toBe(20 * S);
    expect(b.segments.some(s => s.key === 'unaccounted')).toBe(false);
    expect(b.segments.reduce((sum, s) => sum + s.share, 0)).toBeCloseTo(1, 6);
    expect(b.segments.find(s => s.key === 'network_wait')?.share).toBeCloseTo(0.6, 6);
  });

  it('orders widest first but always puts the residual last', () => {
    const b = computeTimeBreakdown({
      RealTimeMicroseconds: 100 * S,
      OSCPUVirtualTimeMicroseconds: 5 * S,
      NetworkReceiveElapsedMicroseconds: 30 * S,
      OSCPUWaitMicroseconds: 10 * S,
    });
    expect(b.segments.map(s => s.key)).toEqual([
      'network_wait', 'cpu_wait', 'cpu', 'unaccounted',
    ]);
  });

  it('distinguishes an unreported disk counter from a zero one', () => {
    // ClickHouse omits zero-valued events and OSIOWaitMicroseconds also needs
    // procfs/taskstats, so absence must not be rendered as "no disk wait".
    const absent = computeTimeBreakdown({
      RealTimeMicroseconds: 10 * S,
      OSCPUVirtualTimeMicroseconds: 5 * S,
    });
    expect(absent.diskWaitReported).toBe(false);

    const reported = computeTimeBreakdown({
      RealTimeMicroseconds: 10 * S,
      OSCPUVirtualTimeMicroseconds: 5 * S,
      OSIOWaitMicroseconds: 0,
    });
    expect(reported.diskWaitReported).toBe(true);
  });

  it('returns unavailable rather than guessing when there is nothing to compose', () => {
    expect(computeTimeBreakdown(undefined).available).toBe(false);
    expect(computeTimeBreakdown(null).available).toBe(false);
    expect(computeTimeBreakdown({}).available).toBe(false);
    expect(computeTimeBreakdown({ RealTimeMicroseconds: 0 }).available).toBe(false);
    // Real time with no named counters is still unavailable: a bar that is
    // 100% unaccounted would imply we measured something.
    expect(computeTimeBreakdown({ SomeOtherEvent: 5 }).available).toBe(false);
  });

  it('composes from named counters when RealTimeMicroseconds is missing', () => {
    const b = computeTimeBreakdown({
      OSCPUVirtualTimeMicroseconds: 3 * S,
      NetworkReceiveElapsedMicroseconds: 1 * S,
    });
    expect(b.available).toBe(true);
    expect(b.totalUs).toBe(4 * S);
    expect(b.segments.find(s => s.key === 'cpu')?.share).toBeCloseTo(0.75, 6);
    expect(b.segments.some(s => s.key === 'unaccounted')).toBe(false);
  });

  it('ignores negative and non-numeric counter values', () => {
    const b = computeTimeBreakdown({
      RealTimeMicroseconds: 10 * S,
      OSCPUVirtualTimeMicroseconds: -5 * S,
      OSIOWaitMicroseconds: 'nonsense',
      OSCPUWaitMicroseconds: 2 * S,
    });
    expect(b.segments.some(s => s.key === 'cpu')).toBe(false);
    expect(b.segments.some(s => s.key === 'disk_wait')).toBe(false);
    expect(b.segments.find(s => s.key === 'cpu_wait')?.share).toBeCloseTo(0.2, 6);
  });

  it('accepts string counter values, as the HTTP interface returns them', () => {
    const b = computeTimeBreakdown({
      RealTimeMicroseconds: String(10 * S),
      OSCPUVirtualTimeMicroseconds: String(4 * S),
    });
    expect(b.segments.find(s => s.key === 'cpu')?.share).toBeCloseTo(0.4, 6);
  });
});

describe('connection handler correction', { tags: ['observability'] }, () => {
  it('discounts one wall-clock of handler thread from the denominator', () => {
    // Real coordinator shape: 2 threads, both alive the full 11.75s, but one is
    // TCPHandler doing 0.03s of work. Leaving it in halves every share.
    const pe = {
      RealTimeMicroseconds: 23.5 * S,
      OSCPUVirtualTimeMicroseconds: 2.01 * S,
    };

    const raw = computeTimeBreakdown(pe);
    expect(raw.segments.find(s => s.key === 'cpu')?.share).toBeCloseTo(0.0855, 3);
    expect(raw.handlerThreadExcluded).toBe(false);

    const corrected = computeTimeBreakdown(pe, { wallClockMs: 11_750 });
    expect(corrected.handlerThreadExcluded).toBe(true);
    expect(corrected.totalUs).toBeCloseTo(11.75 * S, 0);
    expect(corrected.segments.find(s => s.key === 'cpu')?.share).toBeCloseTo(0.171, 3);
  });

  it('skips the correction rather than fabricating a full accounting', () => {
    // Regression: clamping the denominator up to `named` reported a residual of
    // exactly zero with no overlap warning — a confident claim the query was
    // fully explained. Measured on a live cluster this fired on most queries.
    // When the subtraction does not fit, leave the denominator alone.
    const b = computeTimeBreakdown({
      RealTimeMicroseconds: 10 * S,
      OSCPUVirtualTimeMicroseconds: 9 * S,
    }, { wallClockMs: 10_000 });

    expect(b.handlerThreadExcluded).toBe(false);
    expect(b.totalUs).toBe(10 * S);
    // The residual survives instead of being swallowed by the clamp.
    expect(b.segments.find(s => s.key === 'unaccounted')?.share).toBeCloseTo(0.1, 6);
  });

  it('still reports overlap when counters exceed real time', () => {
    // Live example: 102s of named time against 55s of RealTime. The correction
    // must not turn that into a clean 100% with no warning.
    const b = computeTimeBreakdown({
      RealTimeMicroseconds: 55 * S,
      OSCPUVirtualTimeMicroseconds: 30 * S,
      OSCPUWaitMicroseconds: 72 * S,
    }, { wallClockMs: 21_600 });

    expect(b.handlerThreadExcluded).toBe(false);
    expect(b.normalized).toBe(true);
    expect(b.segments.some(s => s.key === 'unaccounted')).toBe(false);
  });

  it('ignores a missing or nonsensical wall clock', () => {
    const pe = { RealTimeMicroseconds: 20 * S, OSCPUVirtualTimeMicroseconds: 5 * S };
    expect(computeTimeBreakdown(pe).handlerThreadExcluded).toBe(false);
    expect(computeTimeBreakdown(pe, { wallClockMs: 0 }).handlerThreadExcluded).toBe(false);
    expect(computeTimeBreakdown(pe, { wallClockMs: -5 }).handlerThreadExcluded).toBe(false);
  });
});

describe('dominantSegment / waitShare', { tags: ['observability'] }, () => {
  it('reports the widest non-residual segment as dominant', () => {
    const b = computeTimeBreakdown({
      RealTimeMicroseconds: 100 * S,
      OSCPUVirtualTimeMicroseconds: 5 * S,
      NetworkReceiveElapsedMicroseconds: 30 * S,
    });
    expect(dominantSegment(b)?.key).toBe('network_wait');
  });

  it('sums waiting but excludes CPU and the residual', () => {
    const b = computeTimeBreakdown({
      RealTimeMicroseconds: 100 * S,
      OSCPUVirtualTimeMicroseconds: 20 * S,
      OSIOWaitMicroseconds: 10 * S,
      OSCPUWaitMicroseconds: 5 * S,
      NetworkReceiveElapsedMicroseconds: 15 * S,
    });
    expect(waitShare(b)).toBeCloseTo(0.3, 6);
  });

  it('is safe on an unavailable breakdown', () => {
    const b = computeTimeBreakdown({});
    expect(dominantSegment(b)).toBeUndefined();
    expect(waitShare(b)).toBe(0);
  });
});
