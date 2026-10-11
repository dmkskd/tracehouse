/**
 * Overview Resource Arena height, derived from the space available below the
 * arena's top edge so the whole arena fits on short screens.
 */

const MAX_ARENA_HEIGHT = 520;
const MIN_ARENA_HEIGHT = 320;
/** Bottom margin kept below the arena so the destination cards start peeking in. */
const RESERVED_BELOW = 24;

/** `availableHeight` is the distance from the arena's top to the bottom of its scroll viewport (null = not measured yet). */
export function getArenaHeight(availableHeight: number | null): number {
  if (availableHeight == null) return MAX_ARENA_HEIGHT;
  return Math.min(MAX_ARENA_HEIGHT, Math.max(MIN_ARENA_HEIGHT, availableHeight - RESERVED_BELOW));
}
