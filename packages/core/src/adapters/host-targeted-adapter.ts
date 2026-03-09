/**
 * HostTargetedAdapter — decorator that rewrites engine-internals queries
 * to target a specific host in a cluster via clusterAllReplicas().
 *
 * Every `FROM system.<table>` reference (including inside scalar subqueries)
 * is rewritten to:
 *   FROM (SELECT * FROM clusterAllReplicas('<cluster>', system.<table>)
 *         WHERE hostname() = '<host>')
 *
 * This inline subquery approach ensures each table access is filtered to
 * the target host, without needing to parse or modify the outer query's
 * WHERE/GROUP BY/ORDER BY structure.
 */

import type { IClickHouseAdapter } from './types.js';

/** Match every `FROM system.<table>` reference. */
const FROM_SYSTEM_RE = /\bFROM\s+(system\.\w+)/gi;

/** Detect if a query already contains clusterAllReplicas. */
const HAS_CLUSTER_ALL_RE = /clusterAllReplicas\s*\(/i;

export class HostTargetedAdapter implements IClickHouseAdapter {
  constructor(
    private inner: IClickHouseAdapter,
    private clusterName: string,
    private targetHost: string,
  ) {}

  async executeQuery<T extends Record<string, unknown>>(
    sql: string,
    params?: Record<string, unknown>,
  ): Promise<T[]> {
    return this.inner.executeQuery<T>(this.rewrite(sql), params);
  }

  async executeCommand(sql: string): Promise<void> {
    return this.inner.executeCommand?.(sql);
  }

  async executeRawQuery(sql: string, database?: string): Promise<string[]> {
    if (!this.inner.executeRawQuery) {
      throw new Error('executeRawQuery not supported by inner adapter');
    }
    return this.inner.executeRawQuery(this.rewrite(sql), database);
  }

  private rewrite(sql: string): string {
    // Queries already using clusterAllReplicas (from ClusterAwareAdapter
    // resolving {{cluster_aware:...}} placeholders) — inject hostname filter
    // INSIDE the clusterAllReplicas call so it runs on each replica.
    if (HAS_CLUSTER_ALL_RE.test(sql)) {
      // Replace clusterAllReplicas('cluster', system.table) with a subquery
      // that filters by hostname on each replica
      return sql.replace(
        /clusterAllReplicas\(\s*'([^']+)'\s*,\s*(system\.\w+)\s*\)/gi,
        (_match, _cluster: string, table: string) =>
          `(SELECT * FROM clusterAllReplicas('${this.clusterName}', ${table}) WHERE hostname() = '${this.targetHost}')`
      );
    }

    // Rewrite every `FROM system.X` → inline filtered subquery
    const replaced = sql.replace(FROM_SYSTEM_RE, (_match, table: string) => {
      return `FROM (SELECT * FROM clusterAllReplicas('${this.clusterName}', ${table}) WHERE hostname() = '${this.targetHost}')`;
    });

    if (replaced !== sql) return replaced;

    // Queries with no FROM (e.g. `SELECT hostName(), version()`) —
    // route through clusterAllReplicas on system.one
    if (/\bhostName\(\)/i.test(sql) && !/\bFROM\b/i.test(sql)) {
      return `${sql.trimEnd().replace(/;?\s*$/, '')} FROM (SELECT * FROM clusterAllReplicas('${this.clusterName}', system.one) WHERE hostname() = '${this.targetHost}')`;
    }

    return sql;
  }
}
