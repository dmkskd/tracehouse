import { describe, expect, it } from 'vitest';
import { placeTooltip, type TooltipPlacementInput } from '../timeline-tooltip-placement';

// 1000-wide chart, plot from y=10 to y=310, one sample per px
const xs = Array.from({ length: 1001 }, (_, i) => i);
const bandOver = (from: number, to: number) => ({
  top: xs.map(x => (x >= from && x <= to ? 10 : 310)),
  bottom: xs.map(() => 310),
});
const input = (over: Partial<TooltipPlacementInput>): TooltipPlacementInput => ({
  cursorX: 500, cursorY: 160, width: 200, height: 100,
  chartWidth: 1000, plotTop: 10, plotBottom: 310,
  xs, bands: [], previousSide: 'right', ...over,
});

describe('placeTooltip', () => {
  it('goes to the side that hides less of the highlighted band', () => {
    const p = placeTooltip(input({ bands: [bandOver(520, 800)] }));
    expect(p).toEqual({ side: 'left', left: 500 - 200 - 24, top: 110 });
  });

  it('keeps the previous side while the other side saves under 15% of its own area', () => {
    // Right side hides a 1px sliver (100) against a 3000 threshold
    const sliver = bandOver(530, 530);
    expect(placeTooltip(input({ bands: [sliver], previousSide: 'right' })).side).toBe('right');
    expect(placeTooltip(input({ bands: [bandOver(520, 800)], previousSide: 'right' })).side).toBe('left');
  });

  it('never covers the crosshair, even when clamped at the chart edge', () => {
    // Near the right edge the right-hand box would clamp back over the cursor
    const p = placeTooltip(input({ cursorX: 950, previousSide: 'right' }));
    expect(p.side).toBe('left');
    expect(p.left + 200).toBeLessThan(950);
  });

  it('centres on the pointer and stays inside the plot', () => {
    expect(placeTooltip(input({ cursorY: 15 })).top).toBe(14);
    expect(placeTooltip(input({ cursorY: 305 })).top).toBe(310 - 100 - 4);
  });
});
