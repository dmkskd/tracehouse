import { describe, expect, it } from 'vitest';
import {
  clampNavigatorDragX,
  navigatorByteScaleCeiling,
  isRangeCovered,
  mergeCoverage,
  mergeTimelinePoints,
  navigatorBucketSeconds,
  navigatorChangePoints,
  navigatorChunkMs,
  navigatorEdgeScrollVelocity,
  navigatorPercentScaleCeiling,
  panRangeToIncludeViewport,
  uncoveredTimelineRanges,
} from '../timeline-navigator-buffer';

describe('timeline navigator buffer', () => {
  it('keeps a stable point budget across overview durations', () => {
    expect(navigatorBucketSeconds(60 * 60_000)).toBe(3);
    expect(navigatorBucketSeconds(24 * 60 * 60_000)).toBe(58);
    expect(navigatorChunkMs(60 * 60_000)).toBe(15 * 60_000);
    expect(navigatorChunkMs(24 * 60 * 60_000)).toBe(30 * 60_000);
  });

  it('uses readable percentage ceilings without clipping peaks', () => {
    expect(navigatorPercentScaleCeiling(0.6)).toBe(1);
    expect(navigatorPercentScaleCeiling(3)).toBe(5);
    expect(navigatorPercentScaleCeiling(73)).toBe(75);
    expect(navigatorPercentScaleCeiling(97)).toBe(100);
    expect(navigatorPercentScaleCeiling(158)).toBe(175);
    expect(navigatorPercentScaleCeiling(221)).toBe(250);
  });

  it('uses readable binary-byte ceilings for disk and network scales', () => {
    expect(navigatorByteScaleCeiling(900 * 1024)).toBe(1024 ** 2);
    expect(navigatorByteScaleCeiling(13 * 1024 ** 2)).toBe(25 * 1024 ** 2);
    expect(navigatorByteScaleCeiling(90 * 1024 ** 2)).toBe(100 * 1024 ** 2);
    expect(navigatorByteScaleCeiling(1.2 * 1024 ** 3)).toBe(2 * 1024 ** 3);
  });

  it('deduplicates and orders overlapping metric chunks', () => {
    expect(mergeTimelinePoints(
      [
        { t: '2026-07-31 08:00:03', v: 3 },
        { t: '2026-07-31 08:00:06', v: 6 },
      ],
      [
        { t: '2026-07-31 08:00:00', v: 0 },
        { t: '2026-07-31 08:00:03', v: 4 },
      ],
    )).toEqual([
      { t: '2026-07-31 08:00:00', v: 0 },
      { t: '2026-07-31 08:00:03', v: 4 },
      { t: '2026-07-31 08:00:06', v: 6 },
    ]);
  });

  it('builds signed changes without creating spikes across cache gaps', () => {
    expect(navigatorChangePoints([
      { t: '2026-07-31 08:00:06', v: 9 },
      { t: '2026-07-31 08:00:00', v: 4 },
      { t: '2026-07-31 08:00:03', v: 7 },
      { t: '2026-07-31 08:01:00', v: 20 },
    ], 3_000)).toEqual([
      { t: '2026-07-31 08:00:00', v: 0 },
      { t: '2026-07-31 08:00:03', v: 3 },
      { t: '2026-07-31 08:00:06', v: 2 },
      { t: '2026-07-31 08:01:00', v: 0 },
    ]);
  });

  it('merges coverage and detects already-loaded ranges', () => {
    const coverage = mergeCoverage(
      [{ startMs: 100, endMs: 200 }],
      { startMs: 190, endMs: 300 },
    );
    expect(coverage).toEqual([{ startMs: 100, endMs: 300 }]);
    expect(isRangeCovered(coverage, { startMs: 150, endMs: 250 })).toBe(true);
    expect(isRangeCovered(coverage, { startMs: 50, endMs: 150 })).toBe(false);
  });

  it('requests only gaps between cached and in-flight ranges', () => {
    expect(uncoveredTimelineRanges(
      [
        { startMs: 100, endMs: 200 },
        { startMs: 250, endMs: 300 },
      ],
      { startMs: 50, endMs: 350 },
    )).toEqual([
      { startMs: 50, endMs: 100 },
      { startMs: 200, endMs: 250 },
      { startMs: 300, endMs: 350 },
    ]);
  });

  it('pans the overview without changing its duration', () => {
    const left = panRangeToIncludeViewport(
      { startMs: 1_000, endMs: 4_000 },
      { startMs: 500, endMs: 1_000 },
      10_000,
    );
    expect(left).toEqual({ startMs: 500, endMs: 3_500 });

    const right = panRangeToIncludeViewport(
      left,
      { startMs: 3_500, endMs: 4_500 },
      4_000,
    );
    expect(right).toEqual({ startMs: 1_000, endMs: 4_000 });
  });

  it('auto-scrolls only while the pointer is held at an edge', () => {
    const hour = 60 * 60_000;
    expect(navigatorEdgeScrollVelocity(500, 1000, hour)).toBe(0);
    expect(navigatorEdgeScrollVelocity(0, 1000, hour)).toBe(-hour / 9);
    expect(navigatorEdgeScrollVelocity(1000, 1000, hour)).toBe(hour / 9);
    expect(navigatorEdgeScrollVelocity(-100, 1000, hour)).toBe(-hour * 2 / 9);
  });

  it('does not accumulate off-screen distance in direct drag deltas', () => {
    expect(clampNavigatorDragX(-500, 1000)).toBe(0);
    expect(clampNavigatorDragX(200, 1000)).toBe(200);
    expect(clampNavigatorDragX(1_500, 1000)).toBe(1000);
  });
});
