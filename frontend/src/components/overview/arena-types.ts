/**
 * Shared types and constants for ResourceArena3D and ArenaCheatMode.
 */

/* ── Scale factors ──────────────────────────────────── */

export const Ts = 0.12;
export const Cs = 0.6;
export const MIN_DIM = 0.06;
export const LANE_GAP = 0.55;
export const DECK_GAP = 1.2;

/* ── Deck assignment ────────────────────────────────── */

export type Deck = 'select' | 'insert' | 'merge';
export const DECK_ORDER: Deck[] = ['select', 'insert', 'merge'];

export function deckOf(kind: string, isMerge: boolean): Deck {
  if (isMerge) return 'merge';
  const k = kind.toUpperCase();
  if (k === 'INSERT') return 'insert';
  return 'select';
}

export function deckBaseY(deck: Deck): number {
  return DECK_ORDER.indexOf(deck) * DECK_GAP;
}

/* ── Block entry ────────────────────────────────────── */

export interface BlockEntry {
  id: string;
  kind: string;
  color: string;
  label: string;
  tableHint: string;
  isMerge: boolean;
  deck: Deck;
  lane: number;
  startTime: number;
  endTime: number | null;
  cpu: number;
  mem: number;
  elapsed: number;
  queryId?: string;
  user?: string;
  progress: number;
  ioReadRate: number;
  rowsRead: number;
  bytesRead: number;
  profileEvents?: {
    userTimeMicroseconds: number;
    systemTimeMicroseconds: number;
    osReadBytes: number;
    osWriteBytes: number;
    selectedParts: number;
    selectedMarks: number;
    markCacheHits: number;
    markCacheMisses: number;
  };
  readBytesPerSec?: number;
  writeBytesPerSec?: number;
  numParts?: number;
  mergeType?: string;
  database?: string;
  table?: string;
  partName?: string;
  hostname?: string;
}

/* ── History backfill ───────────────────────────────── */

/** Lookback fetched at page load; each arena keeps only what fits its own horizon. */
export const ARENA_BACKFILL_WINDOW_SEC = 300;

/**
 * History from the logs ignores operations shorter than this: the live 5s poll of
 * system.processes cannot show them either, and hundreds of them are unreadable.
 */
export const ARENA_BACKFILL_MIN_DURATION_MS = 1000;

/** Legend note explaining that short activity is not drawn */
export const ARENA_SAMPLED_NOTE = `sampled: operations under ${ARENA_BACKFILL_MIN_DURATION_MS / 1000}s are not shown`;

/** Lanes reserved per host block in split-by-host view */
export const HOST_LANE_BLOCK = 6;
