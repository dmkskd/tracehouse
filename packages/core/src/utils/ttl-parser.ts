/**
 * TTL parser for ClickHouse CREATE TABLE DDL.
 *
 * Extracts the table-level TTL expression from a CREATE TABLE statement
 * and converts it to a human-readable duration string.
 *
 * ClickHouse keeps `create_table_query` in `system.tables` in sync with
 * ALTER TABLE ... MODIFY TTL, so this always reflects the current TTL.
 *
 * Supported forms:
 *   TTL event_date + toIntervalDay(30)
 *   TTL event_date + INTERVAL 30 DAY
 *   TTL event_date + toIntervalMonth(3)
 *   TTL event_date + toIntervalHour(24)
 */

/**
 * Extract a human-readable TTL duration from a CREATE TABLE DDL.
 * Returns null if no TTL clause is found.
 */
export function parseTTL(ddl: string): string | null {
  if (!ddl) return null;

  // Match TTL clause — capture everything after TTL until DELETE, TO DISK, TO VOLUME, SETTINGS, or end
  const ttlMatch = ddl.match(/\bTTL\s+(.*?)(?:\s+DELETE|\s+TO\s+(?:DISK|VOLUME)|\s+SETTINGS\b|$)/i);
  if (!ttlMatch) return null;

  const expr = ttlMatch[1].trim();

  // Try toIntervalDay(N), toIntervalMonth(N), toIntervalHour(N), etc.
  const funcMatch = expr.match(/toInterval(\w+)\((\d+)\)/i);
  if (funcMatch) {
    const unit = funcMatch[1].toLowerCase();
    const value = parseInt(funcMatch[2], 10);
    return formatTTLDuration(value, unit);
  }

  // Try INTERVAL N UNIT syntax
  const intervalMatch = expr.match(/INTERVAL\s+(\d+)\s+(\w+)/i);
  if (intervalMatch) {
    const value = parseInt(intervalMatch[1], 10);
    const unit = intervalMatch[2].toLowerCase();
    return formatTTLDuration(value, unit);
  }

  // Fallback: return the raw expression (trimmed)
  return expr;
}

export function formatTTLDuration(value: number, unit: string): string {
  // Normalize unit to singular
  const u = unit.replace(/s$/, '');
  switch (u) {
    case 'day': return value === 1 ? '1 day' : `${value} days`;
    case 'month': return value === 1 ? '1 month' : `${value} months`;
    case 'hour': return value === 1 ? '1 hour' : `${value} hours`;
    case 'week': return value === 1 ? '1 week' : `${value} weeks`;
    case 'year': return value === 1 ? '1 year' : `${value} years`;
    case 'minute': return value === 1 ? '1 minute' : `${value} minutes`;
    case 'second': return value === 1 ? '1 second' : `${value} seconds`;
    default: return `${value} ${unit}`;
  }
}
