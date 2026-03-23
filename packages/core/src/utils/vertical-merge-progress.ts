/**
 * Parse text_log entries from a vertical merge to extract per-column
 * Gantt-style timeline data.
 *
 * Vertical merges proceed in two phases:
 *   1. Horizontal stage — PK columns are merge-sorted together.
 *      Log: "Merged sorted, N blocks, M rows, B bytes in T sec., R rows/sec."
 *   2. Vertical stage — each non-PK column is gathered **sequentially**,
 *      one at a time. The VerticalMergeStage is a state machine that
 *      iterates through gathering_columns with a single iterator.
 *      Different thread IDs appear because the background pool picks up
 *      each execute() call from whichever thread is free, but execution
 *      is never concurrent.
 *      End:   "Gathered column X, N blocks, M rows, B bytes in T sec., R rows/sec."
 *
 * We extract microsecond timestamps from event_time_microseconds to build
 * a timeline showing the sequential column processing and relative durations.
 *
 * Timing: the self-reported duration in "Gathered column" only measures
 * the final write/gather phase. The bulk of the work (reading, decompressing)
 * happens between the previous column's "Gathered" and this one's. We use
 * sequential chaining: each column starts where the previous one ended.
 */

import type { MergeTextLog } from '../types/merge.js';

export interface VerticalMergeSegment {
  name: string;
  /** Milliseconds relative to merge start */
  start_ms: number;
  /** Milliseconds relative to merge start */
  end_ms: number;
  duration_sec: number;
  rows: number;
  bytes: number;
  throughput_rows: number;
  kind: 'horizontal' | 'gathered';
}

export interface VerticalMergeProgress {
  /** All segments (horizontal + gathered) in timeline order */
  segments: VerticalMergeSegment[];
  /** Total wall-clock duration in ms */
  total_ms: number;
}

// "Merged sorted, 2325 blocks, 19000000 rows, 152000000 bytes in 0.575 sec., 33043478.26 rows/sec."
const MERGED_SORTED_RE = /Merged sorted,\s*\d+ blocks,\s*(\d+) rows,\s*(\d+) bytes in ([\d.]+) sec\.,\s*([\d.]+) rows\/sec/;

// "Gathered column trip_id, 2320 blocks, 19000000 rows, 244169656 bytes in 0.077 sec., 246753246.75 rows/sec."
const GATHERED_COLUMN_RE = /Gathered column (\S+),\s*\d+ blocks,\s*(\d+) rows,\s*(\d+) bytes in ([\d.]+) sec\.,\s*([\d.]+) rows\/sec/;

// "Reading 13402 marks from part 202602_0_216_3, total 108500000 rows starting from the beginning of the part, column _block_number"
const READING_COLUMN_RE = /Reading \d+ marks from part \S+, total (\d+) rows .*, column (\S+)$/;

/** Parse event_time_microseconds string to epoch-ms (float). */
function parseMicroseconds(ts: string): number {
  // Format: "2025-03-12 12:16:55.892123" → parse to ms
  const d = new Date(ts.replace(' ', 'T') + 'Z');
  if (!isNaN(d.getTime())) return d.getTime();
  // Fallback: try direct parse
  return new Date(ts).getTime();
}

/**
 * Parse vertical merge column progress from text_log entries.
 * Returns null if the logs don't contain vertical merge patterns.
 *
 * Timing model: columns are processed sequentially. The "Reading" messages
 * for column X fire at essentially the same instant as the "Gathered" message
 * (they log at write time, not read-start). The actual work (reading from
 * disk, decompressing, re-compressing) happens between the previous column's
 * "Gathered" and this column's "Gathered". So we set each column's start_ms
 * to the previous segment's end_ms.
 */
export function parseVerticalMergeProgress(logs: MergeTextLog[]): VerticalMergeProgress | null {
  if (logs.length === 0) return null;

  // Find the earliest timestamp as T0
  const t0 = parseMicroseconds(logs[0].event_time_microseconds);
  if (isNaN(t0)) return null;

  let segments: VerticalMergeSegment[] = [];
  let prevEndMs = 0;

  // Track columns we see "Reading" for, keyed by column name.
  // Value: { firstReadMs, rows } from the first Reading log for that column.
  let readColumns = new Map<string, { firstReadMs: number; rows: number }>();
  let gatheredColumns = new Set<string>();

  for (const log of logs) {
    const ms = parseMicroseconds(log.event_time_microseconds) - t0;

    // Horizontal stage end
    const sortedMatch = log.message.match(MERGED_SORTED_RE);
    if (sortedMatch) {
      // If we already have a PK merge segment, this is a retry of the same
      // merge (same query_id / result part). Reset and keep only the latest
      // attempt — the earlier one was abandoned or failed.
      if (segments.length > 0) {
        segments = [];
        prevEndMs = 0;
        readColumns = new Map();
        gatheredColumns = new Set();
      }

      const duration_sec = parseFloat(sortedMatch[3]);
      segments.push({
        name: 'PK merge',
        start_ms: Math.max(0, ms - duration_sec * 1000),
        end_ms: ms,
        duration_sec,
        rows: parseInt(sortedMatch[1], 10),
        bytes: parseInt(sortedMatch[2], 10),
        throughput_rows: parseFloat(sortedMatch[4]),
        kind: 'horizontal',
      });
      prevEndMs = ms;
      continue;
    }

    // Track "Reading ... column X" messages
    const readingMatch = log.message.match(READING_COLUMN_RE);
    if (readingMatch) {
      const colName = readingMatch[2];
      if (!readColumns.has(colName)) {
        readColumns.set(colName, { firstReadMs: ms, rows: parseInt(readingMatch[1], 10) });
      }
      continue;
    }

    // Column gathered (end) — start is previous segment's end
    const gatheredMatch = log.message.match(GATHERED_COLUMN_RE);
    if (gatheredMatch) {
      const colName = gatheredMatch[1];
      gatheredColumns.add(colName);

      // Check if any previously-read columns were never gathered —
      // they must have been processed between prevEndMs and now.
      for (const [readCol, info] of readColumns) {
        if (readCol === colName || gatheredColumns.has(readCol)) continue;
        // This column was read but we're now seeing a different column gathered,
        // so the ungathered column's work happened in between.
        const startMs = prevEndMs;
        const endMs = info.firstReadMs > prevEndMs ? info.firstReadMs : ms;
        // We'll set endMs properly below when we know the gathered column's timing.
      }

      const startMs = prevEndMs;
      const wallClockSec = (ms - startMs) / 1000;

      // If there are ungathered read columns whose reads started before this
      // gathered column, split the time: give the ungathered column(s) the
      // time from prevEndMs to their first read, then this gathered column
      // gets from that point to now.
      const ungathered: string[] = [];
      for (const [readCol] of readColumns) {
        if (readCol !== colName && !gatheredColumns.has(readCol)) {
          ungathered.push(readCol);
        }
      }

      if (ungathered.length > 0) {
        // Find the earliest read timestamp among ungathered columns
        let earliestReadMs = ms;
        for (const uc of ungathered) {
          const info = readColumns.get(uc)!;
          if (info.firstReadMs < earliestReadMs) {
            earliestReadMs = info.firstReadMs;
          }
        }

        // Insert segments for ungathered columns, splitting time from prevEndMs to ms
        // The ungathered column(s) get prevEndMs → (ms - gathered_self_time)
        // The gathered column gets the remainder
        const gatheredSelfSec = parseFloat(gatheredMatch[4]);
        const gatheredSelfMs = gatheredSelfSec * 1000;
        const splitPoint = Math.max(prevEndMs, ms - gatheredSelfMs);

        for (const uc of ungathered) {
          const info = readColumns.get(uc)!;
          const ucWallSec = (splitPoint - prevEndMs) / 1000;
          segments.push({
            name: uc,
            start_ms: prevEndMs,
            end_ms: splitPoint,
            duration_sec: ucWallSec > 0 ? ucWallSec : 0,
            rows: info.rows,
            bytes: 0,
            throughput_rows: 0,
            kind: 'gathered',
          });
          gatheredColumns.add(uc);
          prevEndMs = splitPoint;
        }
      }

      const adjustedStartMs = prevEndMs;
      const adjustedWallSec = (ms - adjustedStartMs) / 1000;
      segments.push({
        name: colName,
        start_ms: adjustedStartMs,
        end_ms: ms,
        duration_sec: adjustedWallSec > 0 ? adjustedWallSec : parseFloat(gatheredMatch[4]),
        rows: parseInt(gatheredMatch[2], 10),
        bytes: parseInt(gatheredMatch[3], 10),
        throughput_rows: parseFloat(gatheredMatch[5]),
        kind: 'gathered',
      });
      prevEndMs = ms;
    }
  }

  // Handle any columns that were read but never gathered (at the end of the log)
  for (const [readCol, info] of readColumns) {
    if (!gatheredColumns.has(readCol)) {
      segments.push({
        name: readCol,
        start_ms: prevEndMs,
        end_ms: prevEndMs, // unknown end — best we can do
        duration_sec: 0,
        rows: info.rows,
        bytes: 0,
        throughput_rows: 0,
        kind: 'gathered',
      });
    }
  }

  if (segments.length === 0) return null;

  const total_ms = Math.max(...segments.map(s => s.end_ms));
  return { segments, total_ms };
}
