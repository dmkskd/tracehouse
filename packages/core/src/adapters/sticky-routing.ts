/**
 * Sticky routing helper for ClickHouse Cloud.
 *
 * ClickHouse Cloud uses Envoy's ring-hash LB with a wildcard subdomain
 * pattern: `<tag>.sticky.<service-host>`.  Any requests to that hostname
 * are consistently routed to the same replica (barring topology changes).
 *
 * @see https://clickhouse.com/docs/en/manage/replica-aware-routing
 */

/** Matches ClickHouse Cloud service hostnames. */
const CH_CLOUD_RE = /^[a-z0-9]+\.[a-z0-9-]+\.[a-z]+\.clickhouse\.cloud$/i;

import { randomUUID } from '../utils/uuid.js';

/**
 * Generate a stable per-session sticky tag.
 */
function generateStickyTag(): string {
  return `chm-${randomUUID().slice(0, 8)}`;
}

/**
 * Rewrite a ClickHouse Cloud hostname to use sticky routing.
 *
 * Example:
 *   `abc123.us-west-2.aws.clickhouse.cloud`
 *   → `chm-a1b2c3d4.sticky.abc123.us-west-2.aws.clickhouse.cloud`
 *
 * Returns the original host unchanged if:
 * - `enabled` is false/undefined
 * - The host doesn't look like a ClickHouse Cloud endpoint
 */
export function applyStickyRouting(host: string, enabled?: boolean): string {
  if (!enabled) return host;
  if (!CH_CLOUD_RE.test(host)) return host;
  return `${generateStickyTag()}.sticky.${host}`;
}
