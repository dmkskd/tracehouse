export interface Box { left: number; top: number; right: number; bottom: number }
export interface Size { width: number; height: number }

const GAP = 10, MARGIN = 8;

/** Hover card position in viewport coordinates: right of the anchor, else left of it,
 * else against the viewport's right edge; top aligned with the anchor and kept on screen. */
export function placeHover(anchor: Box, card: Size, viewport: Size): { left: number; top: number } {
  const right = anchor.right + GAP, left = anchor.left - GAP - card.width;
  const x = right + card.width <= viewport.width - MARGIN ? right
    : left >= MARGIN ? left
    : Math.max(MARGIN, viewport.width - MARGIN - card.width);
  const y = Math.max(MARGIN, Math.min(anchor.top, viewport.height - MARGIN - card.height));
  return { left: x, top: y };
}
