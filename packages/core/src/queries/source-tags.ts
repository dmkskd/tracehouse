/**
 * Source tag constants for SQL query traceability.
 *
 * Every query we send to ClickHouse is tagged with a comment like:
 *   /* source:TimeTravel:timeline *\/
 *
 * The first segment is the UI tab (screen), the second is the service.
 * Keeping tab names as constants makes them easy to rename globally.
 */

// ── App identifier (change this to rename across all queries) ───────────
export const APP_NAME = 'TraceHouse';

// ── UI Tab / Screen names ──────────────────────────────────────────────
export const TAB_OVERVIEW        = 'Overview';
export const TAB_ENGINE          = 'Engine';
export const TAB_CLUSTER         = 'Cluster';
export const TAB_DATABASES       = 'Explorer';
export const TAB_TIME_TRAVEL     = 'TimeTravel';
export const TAB_QUERIES         = 'Queries';
export const TAB_MERGES          = 'Merges';
export const TAB_ANALYTICS       = 'Analytics';
export const TAB_INTERNAL        = 'Internal';   // capability checks, not user-facing

// ── SQL filter fragments (use these instead of hardcoding the app name) ─
const SOURCE_KEY = 'source';
/** `'%source:TraceHouse:%'` — for LIKE / NOT LIKE clauses */
export const APP_SOURCE_LIKE = `'%${SOURCE_KEY}:${APP_NAME}:%'`;
/** `'source:TraceHouse:'` — for includes() / regex / extractAllGroups */
export const APP_SOURCE_PREFIX = `${SOURCE_KEY}:${APP_NAME}:`;
/** Regex capturing 1 group (component): `source:TraceHouse:(\\w+):` */
export const APP_RE_COMPONENT = `${SOURCE_KEY}:${APP_NAME}:(\\\\w+):`;
/** Regex capturing 2 groups (component, service): `source:TraceHouse:(\\w+):(\\w+)` */
export const APP_RE_COMPONENT_SERVICE = `${SOURCE_KEY}:${APP_NAME}:(\\\\w+):(\\\\w+)`;

// ── Helper to build "App:Tab:service" source strings ───────────────────
export function sourceTag(tab: string, service: string): string {
  return `${APP_NAME}:${tab}:${service}`;
}
