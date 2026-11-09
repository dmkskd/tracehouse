/** A lexical timezone marker check, not validation of the datetime or offset. */
export function hasExplicitOffset(value: string | null | undefined): boolean {
  return typeof value === 'string' && /(?:[zZ]|[+-]\d{2}:?\d{2})$/.test(value.trim());
}

/** Range/start-hint inputs also accept dates and minute precision. Keep that
 * boundary compatibility separate from the strict source-timestamp contract. */
export function parseUtcDateTimeInput(value: string): number {
  const input = value.trim();
  const normalized = /^\d{4}-\d{2}-\d{2}$/.test(input) ? `${input}T00:00:00Z`
    : input.replace(/^(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2})([zZ]|[+-]\d{2}:?\d{2})?$/, '$1:00$2');
  return parseAbsoluteTimestamp(normalized);
}

/**
 * Parse a SQL-guaranteed UTC datetime string or an explicitly zoned ISO datetime.
 * Never pass an unnormalized server-local string or a browser-local form value.
 *
 * Returns epoch milliseconds, or NaN for an unsupported/invalid string. Fractions
 * beyond milliseconds are truncated: preserve the original string for identities.
 * Numeric epochs, Date objects, and legacy fallback formats belong in wrappers.
 */
export function parseAbsoluteTimestamp(value: string): number {
  const match = /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(?:([zZ])|([+-])(\d{2}):?(\d{2}))?$/.exec(value.trim());
  if (!match) return NaN;

  const [, yearText, monthText, dayText, hourText, minuteText, secondText,
    fraction = '', , sign, offsetHourText = '00', offsetMinuteText = '00'] = match;
  const year = Number(yearText);
  const month = Number(monthText);
  const day = Number(dayText);
  const hour = Number(hourText);
  const minute = Number(minuteText);
  const second = Number(secondText);
  const offsetHour = Number(offsetHourText);
  const offsetMinute = Number(offsetMinuteText);

  const leapYear = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const daysInMonth = [31, leapYear ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  if (month < 1 || month > 12 || day < 1 || day > daysInMonth[month - 1]
    || hour > 23 || minute > 59 || second > 59
    || offsetHour > 23 || offsetMinute > 59) return NaN;

  // UTC setters avoid both engine-dependent string parsing and Date.UTC's
  // special handling of years 00–99. No step consults the host timezone.
  const date = new Date(0);
  date.setUTCFullYear(year, month - 1, day);
  date.setUTCHours(hour, minute, second, Number(fraction.slice(0, 3).padEnd(3, '0')));
  const offsetMs = (offsetHour * 60 + offsetMinute) * 60_000;
  return date.getTime() - (sign === '-' ? -offsetMs : offsetMs);
}

/** Numeric microseconds for relative visualization/sorting, preserving the
 * submillisecond remainder. Keep raw strings for persisted exact identities. */
export function parseAbsoluteTimestampMicros(value: string): number {
  const ms = parseAbsoluteTimestamp(value);
  if (!Number.isFinite(ms)) return NaN;
  const fraction = /\.(\d{1,9})(?:[zZ]|[+-]\d{2}:?\d{2})?$/.exec(value.trim())?.[1] ?? '';
  return ms * 1000 + Number(fraction.padEnd(6, '0').slice(3, 6));
}

/** Optional trace clocks use zero for absence, while malformed clocks stay NaN. */
export function parseOptionalTimestampMicros(value: string | null | undefined): number {
  return value ? parseAbsoluteTimestampMicros(value) : 0;
}

/** Event services retain invalid source text for diagnostics, unlike mappers
 * whose public contract represents invalid values as an empty string. */
export function normalizeAbsoluteTimestamp(value: unknown): string {
  const raw = String(value ?? '').trim();
  const ms = parseAbsoluteTimestamp(raw);
  return Number.isNaN(ms) ? raw : new Date(ms).toISOString();
}
