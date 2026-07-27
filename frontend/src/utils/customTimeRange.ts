export interface AbsoluteCustomTimeRange {
  startTime: string;
  endTime: string;
}

export interface LocalCustomTimeRange {
  start: string;
  end: string;
}

const CUSTOM_PREFIX = 'CUSTOM:';

function parseBrowserDateTime(value: string): Date {
  // datetime-local uses `T`; normalize older ClickHouse-shaped persisted values
  // so parsing does not depend on browser support for a space separator.
  return new Date(value.replace(
    /^(\d{4}-\d{2}-\d{2}) /,
    '$1T',
  ));
}

/** Format an instant for a datetime-local input in the browser's timezone. */
export function toLocalDateTimeInput(value: string | number | Date): string {
  const date = value instanceof Date ? value : new Date(value);
  if (!Number.isFinite(date.getTime())) return '';
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

/**
 * Parse a custom range into absolute UTC instants.
 *
 * Canonical values include `Z` (or an explicit offset). Legacy values from a
 * datetime-local input have no offset, so Date intentionally interprets them
 * in the browser's local timezone before they are normalized to UTC.
 */
export function resolveCustomTimeRange(value?: string | null): AbsoluteCustomTimeRange | null {
  if (!value?.startsWith(CUSTOM_PREFIX)) return null;
  const [rawStart, rawEnd] = value.slice(CUSTOM_PREFIX.length).split(',');
  if (!rawStart || !rawEnd) return null;

  const start = parseBrowserDateTime(rawStart);
  const end = parseBrowserDateTime(rawEnd);
  if (!Number.isFinite(start.getTime()) || !Number.isFinite(end.getTime()) || end <= start) {
    return null;
  }

  return {
    startTime: start.toISOString(),
    endTime: end.toISOString(),
  };
}

/** Convert datetime-local input values into the canonical CUSTOM UTC format. */
export function createCustomTimeRange(start: string, end: string): string | null {
  const resolved = resolveCustomTimeRange(`${CUSTOM_PREFIX}${start},${end}`);
  return resolved
    ? `${CUSTOM_PREFIX}${resolved.startTime},${resolved.endTime}`
    : null;
}

/** Normalize an existing canonical or legacy CUSTOM value to UTC. */
export function canonicalizeCustomTimeRange(value?: string | null): string | null {
  const resolved = resolveCustomTimeRange(value);
  return resolved
    ? `${CUSTOM_PREFIX}${resolved.startTime},${resolved.endTime}`
    : null;
}

/** Restore canonical or legacy custom ranges into browser-local input values. */
export function customTimeRangeToLocalInputs(value?: string | null): LocalCustomTimeRange | null {
  const resolved = resolveCustomTimeRange(value);
  if (!resolved) return null;
  return {
    start: toLocalDateTimeInput(resolved.startTime),
    end: toLocalDateTimeInput(resolved.endTime),
  };
}
