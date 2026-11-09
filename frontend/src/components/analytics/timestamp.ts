import { parseAbsoluteTimestamp, hasExplicitOffset } from '@tracehouse/core';

/** Only explicitly zoned values declare an absolute instant. Unmarked strings
 * in authored SQL can be labels or local calendar values and retain their text. */
function zonedTimestamp(value: string): number {
  return hasExplicitOffset(value) ? parseAbsoluteTimestamp(value) : NaN;
}

/** Display only: never use the localized label as a chart key or saved identity. */
export function formatAnalyticsTimestamp(value: string): string {
  const ms = zonedTimestamp(value);
  if (!Number.isFinite(ms)) return value;
  const d = new Date(ms);
  const pad = (n: number) => String(n).padStart(2, '0');
  // Keep the source fraction: Date only carries milliseconds, while identities
  // and direct DateTime64 results can contain six or nine significant digits.
  const fraction = value.trim().match(/\.(\d+)(?:[zZ]|[+-]\d{2}:?\d{2})$/)?.[1];
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}${fraction ? `.${fraction}` : ''}`;
}

/** The Merge Attempts preset has a declared composite label. Localize only
 * when its fields prove that shape; arbitrary timestamp-containing text stays text. */
export function formatAnalyticsCell(value: string, column: string, row: Record<string, unknown>): string {
  if (column === 'attempt' && typeof row.event_time === 'string' && typeof row.host === 'string'
    && value === `${row.event_time} ${row.host}`) {
    return `${formatAnalyticsTimestamp(row.event_time)} ${row.host}`;
  }
  return formatAnalyticsTimestamp(value);
}

/** Explicit timestamp parsing is engine-independent; preserve existing label
 * interpolation behavior for other, user-authored chart domains. */
export function parseAnalyticsTimestamp(value: string): number {
  return hasExplicitOffset(value) ? parseAbsoluteTimestamp(value) : Date.parse(value);
}
