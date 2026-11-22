/** Only call after normalizing known set-valued fields; arbitrary arrays remain ordered. */
export function canonical(value: unknown): string {
  if (value === undefined) return '[absent]';
  if (value === null) return 'null';
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
  if (typeof value === 'object') return `{${Object.entries(value as Record<string, unknown>)
    .sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0).map(([k, v]) => `${JSON.stringify(k)}:${canonical(v)}`).join(',')}}`;
  return JSON.stringify(value);
}
export const sortedSet = (value: unknown): string[] => Array.isArray(value)
  ? [...new Set(value.map(String))].sort() : [];
