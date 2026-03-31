/**
 * Template resolution for SQL query placeholders.
 *
 * Handles {{time_range}}, {{drill:col | fallback}}, {{drill_value:col | fallback}}.
 */

/** Predefined time range options for the picker. */
export const TIME_RANGE_OPTIONS: { label: string; interval: string | null }[] = [
  { label: '15m',     interval: '15 MINUTE' },
  { label: '1h',      interval: '1 HOUR' },
  { label: '6h',      interval: '6 HOUR' },
  { label: '1d',      interval: '1 DAY' },
  { label: '2d',      interval: '2 DAY' },
  { label: '7d',      interval: '7 DAY' },
  { label: '30d',     interval: '30 DAY' },
];

/**
 * Replace {{time_range}} placeholders with a ClickHouse time expression.
 */
export function resolveTimeRange(sql: string, defaultInterval?: string, userInterval?: string | null): string {
  if (!sql.includes('{{time_range}}')) return sql;
  const interval = userInterval ?? defaultInterval;
  if (!interval) return sql;
  if (interval.startsWith('CUSTOM:')) {
    const [rawStart, rawEnd] = interval.slice(7).split(',');
    const normaliseDT = (v: string) => {
      let s = v.replace('T', ' ');
      if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$/.test(s)) s += ':00';
      return s;
    };
    const start = normaliseDT(rawStart);
    const end = normaliseDT(rawEnd);
    let resolved = sql;
    resolved = resolved.replace(
      /event_date\s*>=\s*toDate\(\{\{time_range\}\}\)/g,
      `event_date >= toDate('${start}') AND event_date <= toDate('${end}')`
    );
    resolved = resolved.replace(
      /event_time\s*>\s*\{\{time_range\}\}/g,
      `event_time > toDateTime('${start}') AND event_time < toDateTime('${end}')`
    );
    resolved = resolved.replaceAll('{{time_range}}', `toDateTime('${start}')`);
    return resolved;
  }
  return sql.replaceAll('{{time_range}}', `now() - INTERVAL ${interval}`);
}

/**
 * Replace {{drill:column | fallback}} and {{drill_value:column | fallback}} placeholders.
 */
export function resolveDrillParams(sql: string, drillParams: Record<string, string>): string {
  let result = sql.replace(
    /\{\{drill:(\w+)\s*\|\s*([^}]+)\}\}/g,
    (_match, column: string, fallback: string) => {
      const value = drillParams[column];
      if (value !== undefined) {
        const escaped = value.replace(/'/g, "''");
        return `${column} = '${escaped}'`;
      }
      return fallback.trim();
    }
  );
  result = result.replace(
    /\{\{drill_value:(\w+)\s*\|\s*([^}]+)\}\}/g,
    (_match, column: string, fallback: string) => {
      const value = drillParams[column];
      if (value !== undefined) {
        const escaped = value.replace(/'/g, "''");
        return `'${escaped}'`;
      }
      return fallback.trim();
    }
  );
  return result;
}

/** True if the query SQL accepts drill parameters (either {{drill:…}} or {{drill_value:…}}). */
export function isDrillTarget(sql: string): boolean {
  return sql.includes('{{drill_value:') || sql.includes('{{drill:');
}

/** Human-readable description of a time range interval (for tooltips). */
export function describeTimeRange(defaultInterval?: string, userInterval?: string | null): string {
  const interval = userInterval ?? defaultInterval;
  if (!interval) return '(no time filter)';
  if (interval.startsWith('CUSTOM:')) {
    const [start, end] = interval.slice(7).split(',');
    return `${start} → ${end || 'now'}`;
  }
  return `now() - INTERVAL ${interval}`;
}
