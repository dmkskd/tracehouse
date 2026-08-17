import { describe, expect, it } from 'vitest';

import { countVisibleNavigationItems } from '../navigation.js';

const fit = (
  itemWidths: number[],
  availableWidth: number,
  { gap = 2, overflowTriggerWidth = 50 } = {},
) => countVisibleNavigationItems({ itemWidths, gap, availableWidth, overflowTriggerWidth });

describe('countVisibleNavigationItems', () => {
  it('shows every item when there is room to spare', () => {
    expect(fit([100, 100, 100], 1000)).toBe(3);
  });

  it('folds away the trailing items that do not fit', () => {
    // budget 350 after the trigger; items cost 102 each, so three fit (306), the fourth does not
    expect(fit([100, 100, 100, 100], 400)).toBe(3);
  });

  it('reserves room for the trigger even when the items alone would fit', () => {
    // 3 x 102 = 306 fits in 320, but the 50px trigger has to go somewhere
    expect(fit([100, 100, 100], 320)).toBe(2);
  });

  it('counts the gap that follows each item', () => {
    expect(fit([100, 100, 100], 356)).toBe(3);
    expect(fit([100, 100, 100], 355)).toBe(2);
  });

  it('folds everything when the trigger alone fills the row', () => {
    expect(fit([100], 50)).toBe(0);
    expect(fit([100], 40)).toBe(0);
  });

  it('handles a zero or negative width row', () => {
    expect(fit([100, 100], 0)).toBe(0);
    expect(fit([100, 100], -20)).toBe(0);
  });

  it('returns zero for an empty item list', () => {
    expect(fit([], 1000)).toBe(0);
  });

  it('stops at the first item that does not fit rather than packing later ones', () => {
    // keeping tab order matters more than filling the row: the 10px item stays folded
    expect(fit([100, 400, 10], 400)).toBe(1);
  });

  it('never returns more items than it was given', () => {
    expect(fit([10, 10], 100_000)).toBe(2);
  });
});
