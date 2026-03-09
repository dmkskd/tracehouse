/**
 * Normalize a timestamp value from ClickHouse into an ISO 8601 string.
 * Handles: epoch seconds, epoch milliseconds, Date objects,
 * space-separated datetime strings ("2024-01-15 10:30:00"), and ISO strings.
 */
export function normalizeTimestamp(val: unknown): string {
  if (val == null) return '';
  if (val instanceof Date) {
    return isNaN(val.getTime()) ? '' : val.toISOString();
  }
  if (typeof val === 'number') {
    const ms = val < 1e12 ? val * 1000 : val;
    const d = new Date(ms);
    return isNaN(d.getTime()) ? '' : d.toISOString();
  }
  if (typeof val === 'string') {
    const trimmed = val.trim();
    if (!trimmed) return '';
    // ClickHouse "2024-01-15 10:30:00" → "2024-01-15T10:30:00"
    const normalized = trimmed.replace(' ', 'T');
    let d = new Date(normalized);
    if (!isNaN(d.getTime())) return d.toISOString();
    d = new Date(normalized + 'Z');
    if (!isNaN(d.getTime())) return d.toISOString();
    const n = Number(trimmed);
    if (!isNaN(n)) {
      const ms = n < 1e12 ? n * 1000 : n;
      const nd = new Date(ms);
      return isNaN(nd.getTime()) ? '' : nd.toISOString();
    }
  }
  return '';
}
