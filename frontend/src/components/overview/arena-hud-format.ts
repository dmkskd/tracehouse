/**
 * Formatters and constants shared by the ResourceArena3D overlays
 * (hover card, HUD, inspector).
 */

export function fmtRate(bps: number): string {
  if (bps < 1024) return `${bps.toFixed(0)} B/s`;
  if (bps < 1048576) return `${(bps / 1024).toFixed(1)} KB/s`;
  return `${(bps / 1048576).toFixed(1)} MB/s`;
}

export function fmtMicro(us: number): string {
  if (us < 1000) return `${us.toFixed(0)}µs`;
  if (us < 1_000_000) return `${(us / 1000).toFixed(1)}ms`;
  return `${(us / 1_000_000).toFixed(2)}s`;
}

export function fmtNum(n: number): string {
  if (n < 1000) return n.toFixed(0);
  if (n < 1_000_000) return `${(n / 1000).toFixed(1)}K`;
  if (n < 1_000_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  return `${(n / 1_000_000_000).toFixed(2)}B`;
}

export interface MetricSnapshot {
  t: number; // timestamp ms
  cpu: number;
  mem: number;
  io: number;
  rows: number;
  write: number;
  progress: number;
  // Profile event cumulative totals (for delta computation)
  userTimeMicro: number;
  sysTimeMicro: number;
  diskRead: number;
  diskWrite: number;
}

export const LOG_LEVEL_COLORS: Record<string, string> = {
  Fatal: '#ef4444', Critical: '#ef4444', Error: '#f87171',
  Warning: '#fbbf24', Notice: '#60a5fa', Information: '#818cf8',
  Debug: 'rgba(255,255,255,0.35)', Trace: 'rgba(255,255,255,0.2)',
};

/** Parse a ClickHouse event_time_microseconds string into epoch ms */
export function parseLogTime(raw: string): number {
  if (!raw) return 0;
  try {
    // "2025-03-07 01:00:33.860123" or "01:00:33.860123"
    if (raw.includes('-')) return new Date(raw.replace(' ', 'T') + 'Z').getTime();
    const [h, m, rest] = raw.split(':');
    const secs = parseFloat(rest || '0');
    const d = new Date();
    d.setUTCHours(+h, +m, Math.floor(secs), Math.round((secs % 1) * 1000));
    return d.getTime();
  } catch { return 0; }
}

/** Compact duration string */
export function fmtDelta(ms: number): string {
  if (ms <= 0) return '';
  const s = ms / 1000;
  if (s < 0.01) return '<10ms';
  if (s < 1) return `${(s * 1000).toFixed(0)}ms`;
  if (s < 60) return `${s.toFixed(1)}s`;
  const m = Math.floor(s / 60);
  return `${m}m${Math.floor(s % 60)}s`;
}

/** CPU cores for display; a merge without recorded CPU shows a dash, not 0. */
export function fmtCpuCores(cpu: number, isMerge: boolean, unit = ''): string {
  return isMerge && cpu <= 0 ? '–' : `${cpu.toFixed(2)}${unit}`;
}

export const panelFont = "'JetBrains Mono', 'Fira Code', 'SF Mono', monospace";

/**
 * `color` at the given opacity (0-1). Block colors are hsl() strings, so
 * appending hex alpha digits (`${color}40`) is invalid CSS; color-mix works
 * for any CSS color.
 */
export function withAlpha(color: string, alpha: number): string {
  return `color-mix(in srgb, ${color} ${Math.round(alpha * 100)}%, transparent)`;
}
