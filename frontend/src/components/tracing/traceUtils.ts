/**
 * Trace log utility functions: color generation, log level colors,
 * and log duration processing.
 *
 * Extracted from TraceLogViewer.tsx to keep presentation separate from logic.
 */

import type { TraceLog } from '../../stores/traceStore';

/* ─── types ─── */

export interface ProcessedLog extends TraceLog {
  durationMs: number;
  isCoordinator: boolean;
}

/* ─── log level colors ─── */

const LEVEL_COLORS: Record<string, string> = {
  Error: '#e57373',
  Warning: '#ffb74d',
  Information: '#81c784',
  Debug: '#64b5f6',
  Trace: '#90a4ae',
  Fatal: '#ef5350',
  Critical: '#ef5350',
  Notice: '#4dd0e1',
};

export function getLevelColor(level: string): string {
  return LEVEL_COLORS[level] || '#90a4ae';
}

/* ─── deterministic color generation ─── */

export function stringToHslColor(str: string, isLight: boolean): string {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    hash = str.charCodeAt(i) + ((hash << 5) - hash);
  }
  const lightness = isLight ? 35 : 65;
  const saturation = isLight ? 70 : 60;
  return `hsl(${hash % 360}, ${saturation}%, ${lightness}%)`;
}

export function getThreadIdColor(threadId: number, isLight: boolean): string {
  const hash = ((threadId * 2654435761) >>> 0) % 360;
  const lightness = isLight ? 35 : 65;
  const saturation = isLight ? 65 : 55;
  return `hsl(${hash}, ${saturation}%, ${lightness}%)`;
}

/* ─── log duration processing ─── */

export function processLogsWithDuration(logs: TraceLog[]): ProcessedLog[] {
  if (logs.length === 0) return [];

  // Find coordinator thread (first TCPHandler thread)
  const firstLog = logs[0];
  const coordinatorThreadId = firstLog?.thread_name?.includes('TCPHandler')
    ? firstLog.thread_id
    : logs.find(l => l.thread_name?.includes('TCPHandler'))?.thread_id ?? firstLog?.thread_id;

  // Pre-parse all timestamps
  const timestamps = logs.map(log =>
    new Date(log.event_time_microseconds?.replace(' ', 'T') + 'Z').getTime()
  );

  return logs.map((log, i) => {
    const currentTime = timestamps[i];
    const isCoordinator = log.thread_id === coordinatorThreadId;

    let endTime: number | null = null;

    // Look ahead for when this log "ends"
    for (let j = i + 1; j < logs.length; j++) {
      // First priority: next log from same thread
      if (logs[j].thread_id === log.thread_id) {
        endTime = timestamps[j];
        break;
      }
      // Second priority: if we're a worker, next coordinator log means we're done
      if (!isCoordinator && logs[j].thread_id === coordinatorThreadId) {
        endTime = timestamps[j];
        break;
      }
    }

    const durationMs = endTime !== null ? endTime - currentTime : 0;

    return { ...log, durationMs, isCoordinator };
  });
}
