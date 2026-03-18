/**
 * Shared formatting utilities.
 *
 * Centralises the byte / number / duration helpers that were previously
 * duplicated across 20+ component and store files.
 */

// ── Bytes ──────────────────────────────────────────────────────────────────

export function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`;
}

export function formatBytesPerSec(bytesPerSec: number): string {
  if (bytesPerSec === 0) return '0 B/s';
  return `${formatBytes(bytesPerSec)}/s`;
}

export function formatBytesToGB(bytes: number, precision: number = 2): string {
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(precision)} GB`;
}

// ── Numbers ────────────────────────────────────────────────────────────────

/** Locale-aware thousands separator (e.g. 1,234,567) */
export function formatNumber(num: number): string {
  return num.toLocaleString();
}

/** Compact number (e.g. 1.2K, 3.4M, 5.6B) */
export function formatNumberCompact(num: number): string {
  if (num < 1000) return num.toString();
  if (num < 1_000_000) return `${(num / 1000).toFixed(1)}K`;
  if (num < 1_000_000_000) return `${(num / 1_000_000).toFixed(1)}M`;
  return `${(num / 1_000_000_000).toFixed(1)}B`;
}

// ── Durations ──────────────────────────────────────────────────────────────

/** Format seconds → human-readable (e.g. 250ms, 3.14s, 2m 30s, 1h 5m, 2d 3h) */
export function formatDuration(seconds: number): string {
  if (seconds < 1) {
    return `${Math.round(seconds * 1000)}ms`;
  }
  if (seconds < 60) {
    return `${seconds.toFixed(2)}s`;
  }
  const minutes = Math.floor(seconds / 60);
  const remainingSeconds = seconds % 60;
  if (minutes < 60) {
    return `${minutes}m ${Math.round(remainingSeconds)}s`;
  }
  const hours = Math.floor(minutes / 60);
  const remainingMinutes = minutes % 60;
  if (hours < 24) {
    return `${hours}h ${remainingMinutes}m`;
  }
  const days = Math.floor(hours / 24);
  return `${days}d ${hours % 24}h`;
}

/** Format milliseconds → human-readable (e.g. 3.14ms, 1.25s, 22:30m, 1:05:03h) */
export function formatDurationMs(ms: number): string {
  if (ms < 1000) return `${Number(ms.toFixed(2))}ms`;
  const s = ms / 1000;
  if (s < 60) return `${s.toFixed(2)}s`;
  const totalSeconds = Math.floor(s);
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  if (minutes < 60) return `${minutes}:${String(seconds).padStart(2, '0')}m`;
  const hours = Math.floor(minutes / 60);
  const mins = minutes % 60;
  return `${hours}:${String(mins).padStart(2, '0')}:${String(seconds).padStart(2, '0')}h`;
}

/** Format elapsed seconds with one decimal (e.g. 3.2s, 2m 15s, 1h 5m) */
export function formatElapsed(seconds: number): string {
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${Math.floor(seconds % 60)}s`;
  return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
}

// ── Microseconds ───────────────────────────────────────────────────────────

/** Format microseconds → human-readable (e.g. 500µs, 3.1ms, 1.25s, 22:30m, 1:05:03h) */
export function formatMicroseconds(us: number): string {
  if (us < 1000) return `${us}µs`;
  if (us < 1_000_000) return `${(us / 1000).toFixed(1)}ms`;
  const s = us / 1_000_000;
  if (s < 60) return `${s.toFixed(2)}s`;
  const totalSeconds = Math.floor(s);
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  if (minutes < 60) return `${minutes}:${String(seconds).padStart(2, '0')}m`;
  const hours = Math.floor(minutes / 60);
  const mins = minutes % 60;
  return `${hours}:${String(mins).padStart(2, '0')}:${String(seconds).padStart(2, '0')}h`;
}

// ── Timestamps ─────────────────────────────────────────────────────────────

/** Parse a ClickHouse timestamp string (with or without timezone) to epoch ms */
export function parseTimestamp(s: string): number {
  const n = s.replace(' ', 'T') + (s.includes('+') || s.includes('Z') || s.includes('T') ? '' : 'Z');
  return new Date(n).getTime();
}

// ── Query helpers ──────────────────────────────────────────────────────────

export function truncateQuery(query: string, maxLength: number = 60): string {
  const cleaned = query.replace(/\s+/g, ' ').trim();
  if (cleaned.length <= maxLength) return cleaned;
  return cleaned.substring(0, maxLength) + '...';
}
