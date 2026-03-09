/**
 * Shared helper functions for row mappers.
 * Provide null-safe type coercion from raw ClickHouse query result values.
 */

export type RawRow = Record<string, unknown>;

export function toInt(val: unknown, fallback = 0): number {
  if (val == null) return fallback;
  const n = Number(val);
  return isNaN(n) ? fallback : Math.floor(n);
}

export function toStr(val: unknown, fallback = ''): string {
  if (val == null) return fallback;
  return String(val);
}

export function toFloat(val: unknown, fallback = 0): number {
  if (val == null) return fallback;
  const n = Number(val);
  return isNaN(n) ? fallback : n;
}

export function toBool(val: unknown, fallback = false): boolean {
  if (val == null) return fallback;
  if (typeof val === 'boolean') return val;
  if (typeof val === 'number') return val !== 0;
  if (typeof val === 'string') return val === '1' || val.toLowerCase() === 'true';
  return fallback;
}

export function toStrArray(val: unknown): string[] {
  if (Array.isArray(val)) return val.map(v => String(v));
  if (typeof val === 'string') {
    const trimmed = val.trim();
    if (!trimmed || trimmed === '[]') return [];
    // Handle ClickHouse array format: ['a','b','c']
    return trimmed.replace(/^\[|\]$/g, '').split(',').map(s => s.trim().replace(/^'|'$/g, ''));
  }
  return [];
}

/**
 * Extract the short hostname from a potentially fully-qualified domain name.
 * e.g. "dev-cluster-clickhouse-0-0-0.dev-cluster-clickhouse-headless.svc.cluster.local"
 *   → "dev-cluster-clickhouse-0-0-0"
 */
export function shortenHostname(fqdn: unknown): string {
  const s = toStr(fqdn);
  if (!s) return '';
  const dot = s.indexOf('.');
  return dot > 0 ? s.substring(0, dot) : s;
}

/**
 * Truncate a hostname for compact display.
 * Shows the first `headLen` and last `tailLen` characters with "…" in between.
 * e.g. "dev-cluster-clickhouse-0-0-0" → "dev-cl…0-0-0" (head=6, tail=5)
 * Returns the original string if it's short enough.
 */
export function truncateHostname(name: string, maxLen = 12): string {
  if (!name || name.length <= maxLen) return name;
  const headLen = Math.ceil((maxLen - 1) / 2);
  const tailLen = maxLen - 1 - headLen;
  return `${name.slice(0, headLen)}…${name.slice(-tailLen)}`;
}

