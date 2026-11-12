/**
 * ClusterAwareAdapter — decorator that wraps an IClickHouseAdapter and
 * resolves {{cluster_aware:system.X}} placeholders when a cluster is detected.
 *
 * The underlying adapter stays a dumb SQL transport. This layer handles
 * the cluster topology concern.
 */

import type { IClickHouseAdapter, QueryExecutionOptions, TaggedQuery } from './types.js';
import { ClusterService } from '../services/cluster-service.js';

export class ClusterAwareAdapter implements IClickHouseAdapter {
  private clusterName: string | null = null;
  readonly executeRawQuery?: NonNullable<IClickHouseAdapter['executeRawQuery']>;

  constructor(private inner: IClickHouseAdapter) {
    const rawQuery = inner.executeRawQuery?.bind(inner);
    if (rawQuery) {
      this.executeRawQuery = (sql, database, options) => rawQuery(
        ClusterService.resolveTableRefs(sql, this.clusterName) as TaggedQuery,
        database,
        options,
      );
    }
  }

  get supportsExplicitQueryId(): boolean {
    return this.inner.supportsExplicitQueryId === true;
  }

  setClusterName(name: string | null): void {
    this.clusterName = name;
  }

  getClusterName(): string | null {
    return this.clusterName;
  }

  async executeQuery<T extends Record<string, unknown>>(sql: TaggedQuery, options?: QueryExecutionOptions): Promise<T[]> {
    return this.inner.executeQuery<T>(ClusterService.resolveTableRefs(sql, this.clusterName) as TaggedQuery, options);
  }

  async executeCommand(sql: TaggedQuery): Promise<void> {
    return this.inner.executeCommand?.(sql);
  }
}
