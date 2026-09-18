/**
 * Template resolution for SQL query placeholders.
 *
 * Handles {{time_range}}, {{drill:col | fallback}}, {{drill_value:col | fallback}}.
 */

import { escapeValue, buildSelectedXRayOverlaySQL, type SelectQueryXRaySourceInput, type XRayOverlayMetric } from '@tracehouse/core';
import { resolveCustomTimeRange } from '../../utils/customTimeRange';

export { TIME_RANGE_OPTIONS } from '../common/time-range-options';

/** Expand only explicit source-aware templates; custom SQL remains untouched. */
export function resolveQueryXRaySQL(sql: string, selection: SelectQueryXRaySourceInput): string {
  return sql.replace(/\{\{query_xray_overlay:(cpu_cores|mem_mb|read_mb_s|cpu_wait_s|io_wait_s|net_wait_s)\}\}/g,
    (_match, metric: XRayOverlayMetric) => buildSelectedXRayOverlaySQL(selection, metric, {
      start: '{{time_range}}', end: 'now()',
      queryIdsSQL: 'SELECT query_id FROM top_q',
      identityStart: 'if((SELECT count() FROM top_q) = 0, {{time_range}}, (SELECT min(query_start) FROM top_q))',
    }));
}

/**
 * Resolution result for the Grafana export path.
 *
 * The export keeps time and drill templates unresolved, but the X-Ray source
 * must be concrete, and that resolution can legitimately fail: the active
 * source may be unable to serve the panel at all. The reason is carried rather
 * than discarded, because it is the only explanation the disabled export
 * button can give.
 */
export type ExportTemplate = { sql: string; error?: undefined } | { sql?: undefined; error: string };

export function resolveExportTemplate(sql: string, selection: SelectQueryXRaySourceInput): ExportTemplate {
  try {
    return { sql: resolveQueryXRaySQL(sql, selection) };
  } catch (error) {
    return { error: error instanceof Error ? error.message : String(error) };
  }
}

export class InvalidCustomTimeRangeError extends Error {
  constructor(value: string) {
    super(`Invalid custom time range: ${value}`);
    this.name = 'InvalidCustomTimeRangeError';
  }
}

function utcDateTimeLiteral(iso: string): string {
  const value = iso.slice(0, 19).replace('T', ' ');
  return `toDateTime('${value}', 'UTC')`;
}

/**
 * Replace {{time_range}} placeholders with a ClickHouse time expression.
 */
export function resolveTimeRange(sql: string, defaultInterval?: string, userInterval?: string | null): string {
  if (!sql.includes('{{time_range}}')) return sql;
  const interval = userInterval ?? defaultInterval;
  if (!interval) return sql;
  if (interval.startsWith('CUSTOM:')) {
    const range = resolveCustomTimeRange(interval);
    if (!range) throw new InvalidCustomTimeRangeError(interval);
    const startDateTime = utcDateTimeLiteral(range.startTime);
    const endDateTime = utcDateTimeLiteral(range.endTime);
    let resolved = sql;
    resolved = resolved.replace(
      /event_date\s*>=\s*toDate\(\{\{time_range\}\}\)/g,
      `event_date >= toDate(${startDateTime}, 'UTC') AND event_date <= toDate(${endDateTime}, 'UTC')`
    );
    resolved = resolved.replace(
      /event_time\s*>\s*\{\{time_range\}\}/g,
      `event_time > ${startDateTime} AND event_time < ${endDateTime}`
    );
    resolved = resolved.replaceAll('{{time_range}}', startDateTime);
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
        const escaped = escapeValue(value);
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
        const escaped = escapeValue(value);
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
    const range = resolveCustomTimeRange(interval);
    if (!range) return '(invalid custom time range)';
    return `${range.startTime} → ${range.endTime} (UTC)`;
  }
  return `now() - INTERVAL ${interval}`;
}
