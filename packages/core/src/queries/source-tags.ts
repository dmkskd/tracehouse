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
export const APP_NAME = 'Monitor';

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

// ── Helper to build "App:Tab:service" source strings ───────────────────
export function sourceTag(tab: string, service: string): string {
  return `${APP_NAME}:${tab}:${service}`;
}
