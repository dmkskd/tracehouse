/**
 * ShadowAdapter — wraps a real ClickHouseClient but rewrites queries
 * so that `system.metric_log`, `system.asynchronous_metric_log`, etc.
 * are redirected to `test_shadow.*` shadow tables.
 *
 * This lets us run the *exact same production SQL* against controlled
 * test data without modifying the query templates.
 */

import type { ClickHouseClient } from '@clickhouse/client';
import type { IClickHouseAdapter } from '../../../adapters/types.js';
import { AdapterError } from '../../../adapters/types.js';
import { ClusterService } from '../../../services/cluster-service.js';

/**
 * System tables that we shadow. Order matters — longer prefixes first
 * to avoid partial replacements (e.g. `system.asynchronous_metric_log`
 * must be matched before `system.metric_log`).
 */
const SHADOW_REWRITES: [RegExp, string][] = [
  [/\bsystem\.asynchronous_metric_log\b/g, 'test_shadow.asynchronous_metric_log'],
  [/\bsystem\.asynchronous_metrics\b/g, 'test_shadow.asynchronous_metrics'],
  [/\bsystem\.metric_log\b/g, 'test_shadow.metric_log'],
  [/\bsystem\.query_log\b/g, 'test_shadow.query_log'],
  [/\bsystem\.part_log\b/g, 'test_shadow.part_log'],
];

function rewriteQuery(sql: string): string {
  // Resolve cluster placeholders first (single-node: strip to plain system.X)
  let rewritten = ClusterService.resolveTableRefs(sql, null);
  for (const [pattern, replacement] of SHADOW_REWRITES) {
    rewritten = rewritten.replace(pattern, replacement);
  }
  return rewritten;
}

/**
 * Adapter that transparently rewrites system table references to shadow tables.
 * Use this to inject into MetricsCollector, TimelineService, etc. during
 * integration tests so the production query templates run against test data.
 */
export class ShadowAdapter implements IClickHouseAdapter {
  constructor(private client: ClickHouseClient) {}

  async executeQuery<T extends Record<string, unknown>>(sql: string): Promise<T[]> {
    const rewritten = rewriteQuery(sql);
    try {
      const result = await this.client.query({ query: rewritten, format: 'JSONEachRow' });
      return await result.json<T>();
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error);
      throw new AdapterError(msg, 'query', error instanceof Error ? error : undefined);
    }
  }

  async executeCommand(sql: string): Promise<void> {
    const rewritten = rewriteQuery(sql);
    await this.client.command({ query: rewritten });
  }
}
