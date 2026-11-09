import { parseAbsoluteTimestamp } from '../utils/absolute-timestamp.js';

/** Normalize an absolute timestamp to ISO. Source SQL must emit UTC for
 * unmarked datetime strings. Numeric seconds/ms and Date inputs keep their
 * existing units; invalid inputs remain empty. Localize only for UI display. */
export function normalizeTimestamp(val: unknown): string {
  if (val == null) return '';
  if (val instanceof Date) return Number.isFinite(val.getTime()) ? val.toISOString() : '';
  if (typeof val === 'number') {
    const date = new Date(val < 1e12 ? val * 1000 : val);
    return Number.isFinite(date.getTime()) ? date.toISOString() : '';
  }
  if (typeof val !== 'string' || !val.trim()) return '';
  const input = val.trim();
  const ms = parseAbsoluteTimestamp(input);
  if (Number.isFinite(ms)) return new Date(ms).toISOString();
  const number = Number(input);
  return Number.isFinite(number) ? normalizeTimestamp(number) : '';
}

/** Only for optional ClickHouse system clocks where epoch zero means absent.
 * General timestamp parsing must continue to accept the Unix epoch. */
export function normalizeOptionalSystemTimestamp(value: unknown): string {
  const normalized = normalizeTimestamp(value);
  return normalized === '1970-01-01T00:00:00.000Z' ? '' : normalized;
}
