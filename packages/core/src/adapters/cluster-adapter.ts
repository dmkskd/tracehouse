/**
 * ClusterAwareAdapter — decorator that wraps an IClickHouseAdapter and
 * resolves {{cluster_aware:system.X}} placeholders when a cluster is detected.
 *
 * The underlying adapter stays a dumb SQL transport. This layer handles
 * the cluster topology concern.
 */

import type { IClickHouseAdapter } from './types.js';
import { ClusterService } from '../services/cluster-service.js';

export class ClusterAwareAdapter implements IClickHouseAdapter {
  private clusterName: string | null = null;

  constructor(private inner: IClickHouseAdapter) {}

  setClusterName(name: string | null): void {
    this.clusterName = name;
  }

  getClusterName(): string | null {
    return this.clusterName;
  }

  async executeQuery<T extends Record<string, unknown>>(sql: string): Promise<T[]> {
    return this.inner.executeQuery<T>(ClusterService.resolveTableRefs(sql, this.clusterName));
  }

  async executeCommand(sql: string): Promise<void> {
    return this.inner.executeCommand?.(sql);
  }

  async executeRawQuery(sql: string, database?: string): Promise<string[]> {
    if (!this.inner.executeRawQuery) {
      throw new Error('executeRawQuery not supported by inner adapter');
    }
    return this.inner.executeRawQuery(ClusterService.resolveTableRefs(sql, this.clusterName), database);
  }
}
