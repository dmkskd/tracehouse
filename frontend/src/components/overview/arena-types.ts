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
