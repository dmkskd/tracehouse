/**
 * Smart cell formatting for query results.
 *
 * Detects Unix timestamps (seconds or milliseconds) and formats them as
 * human-readable dates. Falls back to locale-aware number/string formatting.
 */

// Reasonable epoch range: 2000-01-01 to 2100-01-01
const EPOCH_SEC_MIN = 946_684_800;
const EPOCH_SEC_MAX = 4_102_444_800;
const EPOCH_MS_MIN = EPOCH_SEC_MIN * 1000;
const EPOCH_MS_MAX = EPOCH_SEC_MAX * 1000;

export type TimestampUnit = 'seconds' | 'milliseconds' | null;

/** Detect whether an integer looks like a Unix timestamp (seconds or ms). */
export function detectTimestamp(v: number): TimestampUnit {
  if (!Number.isInteger(v)) return null;
  if (v > EPOCH_SEC_MIN && v < EPOCH_SEC_MAX) return 'seconds';
  if (v > EPOCH_MS_MIN && v < EPOCH_MS_MAX) return 'milliseconds';
  return null;
}

/** Convert a detected timestamp to a Date. Returns null if not a timestamp. */
export function timestampToDate(v: number): Date | null {
  const unit = detectTimestamp(v);
  if (unit === 'seconds') return new Date(v * 1000);
  if (unit === 'milliseconds') return new Date(v);
  return null;
}

/**
 * Format a Date as "YYYY-MM-DD HH:MM:SS" (ISO-ish, no timezone).
 * This format is consistent with how ClickHouse returns DateTime strings,
 * so chart axis formatters (formatXTick) can extract the time portion.
 */
function formatDateISO(d: Date): string {
  const Y = d.getFullYear();
  const M = String(d.getMonth() + 1).padStart(2, '0');
  const D = String(d.getDate()).padStart(2, '0');
  const h = String(d.getHours()).padStart(2, '0');
  const m = String(d.getMinutes()).padStart(2, '0');
  const s = String(d.getSeconds()).padStart(2, '0');
  return `${Y}-${M}-${D} ${h}:${m}:${s}`;
}

/**
 * Returns true if the column name suggests it holds a timestamp value.
 * Only these columns get automatic epoch→date formatting.
 */
function isTimeColumn(col: string): boolean {
  const lower = col.toLowerCase();
  return /(time|date|timestamp|_at$|_ts$)/.test(lower);
}

/** Format a value for display in tables and chart axes.
 *  Pass the column name so timestamp detection only fires for time-like columns. */
export function formatCell(v: unknown, columnName?: string): string {
  if (v == null) return '—';
  if (typeof v === 'number') {
    if (!columnName || isTimeColumn(columnName)) {
      const d = timestampToDate(v);
      if (d) return formatDateISO(d);
    }
    return Number.isInteger(v)
      ? v.toLocaleString()
      : v.toLocaleString(undefined, { maximumFractionDigits: 2 });
  }
  if (Array.isArray(v)) return v.join(', ');
  return String(v);
}
