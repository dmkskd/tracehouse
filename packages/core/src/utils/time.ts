/**
 * Safely parse a time slot from different ClickHouse adapters and Grafana DataFrame layers.
 * 
* Grafana's internal DataFrame parsers can return epoch numbers or convert RFC2822 date strings
 * (e.g. `"Fri, 27 Feb 2026 20:24:42"`).
 * Alternatively, the ClickHouse HTTP adapter might return standard "YYYY-MM-DD HH:MM:SS" strings
 * without timezone information (e.g. `"2026-03-04 14:02:28.000"`).
 * This utility robustly handles Date object, number, and strings 
 * (both ISO and ClickHouse-specific format) to compute consistent normalized UTC time attributes. 
 */
export function parseTimeValue(timeVal: unknown): { timeMs: number, timeStr: string } {
  let ms = parseToMilliseconds(timeVal);

  if (isNaN(ms) || ms <= 0) {
    console.warn('[TimeParser] Data point time parsed as NaN or invalid. Original payload:', { type: typeof timeVal, value: timeVal });
    ms = 0;
  }

  return { timeMs: ms, timeStr: new Date(ms).toISOString() };
}

function parseToMilliseconds(val: unknown): number {
  if (val instanceof Date) {
    return val.getTime();
  }

  if (typeof val === 'number') {
    return val;
  }

  if (typeof val === 'string') {
    // Check if it's purely a stringified epoch number (e.g. "1700000000000")
    // Needs to be strict so it doesn't accidentally parse strings like "" or " "
    if (val.trim() !== '' && !isNaN(Number(val))) {
      return Number(val);
    }

    // Check for standard ClickHouse datetime format like "2023-01-01 12:00:00"
    // and format it into a spec-compliant ISO string so Chrome Date doesn't barf.
    if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/.test(val)) {
      const isUTC = val.includes('Z') || val.includes('+');
      const isoFormat = val.trim().replace(' ', 'T') + (isUTC ? '' : 'Z');
      return new Date(isoFormat).getTime();
    }

    // Fallback to native JS Date.parse (will handle RFC2822, ISO strings, etc.)
    // Note: Grafana sometimes gives un-offset strings like "Fri, 27 Feb 2026 20:24:42"
    return Date.parse(val);
  }

  return NaN;
}
