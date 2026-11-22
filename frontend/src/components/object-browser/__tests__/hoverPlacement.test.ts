import { describe, expect, it } from 'vitest';
import { placeHover } from '../hoverPlacement';

const viewport = { width: 1000, height: 600 };
const card = { width: 260, height: 120 };
const anchor = (left: number, top: number) => ({ left, top, right: left + 176, bottom: top + 52 });

describe('hover placement', () => {
  it('sits right of the card, then left of it, then against the right edge', () => {
    expect(placeHover(anchor(100, 50), card, viewport)).toEqual({ left: 286, top: 50 });
    expect(placeHover(anchor(700, 50), card, viewport)).toEqual({ left: 430, top: 50 });
    expect(placeHover(anchor(100, 50), card, { width: 450, height: 600 })).toEqual({ left: 182, top: 50 });
  });
  it('stays inside the viewport vertically', () => {
    expect(placeHover(anchor(100, 560), card, viewport).top).toBe(472);
    expect(placeHover(anchor(100, -30), card, viewport).top).toBe(8);
  });
});
