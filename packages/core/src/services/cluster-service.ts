/**
 * ClusterService — detects ClickHouse cluster topology and provides
 * a helper to resolve {{cluster_aware:db.table}} placeholders in SQL.
 *
 * Usage:
 *   const cluster = await ClusterService.detect(adapter);
 *   // In SQL templates, use {{cluster_aware:system.query_log}} for log tables
 *   // or {{cluster_aware:tracehouse.processes_history}} for sampler tables
 *   // that should be queried across all replicas when a cluster is detected.
 *   const sql = ClusterService.resolveTableRefs(template, clusterName);
 *
 * On a single-node server (no cluster), resolveTableRefs replaces
 * {{cluster_aware:db.table}} with plain db.table.
 * On a cluster, it wraps with clusterAllReplicas('cluster_name', db.table).
 */

import type { IClickHouseAdapter } from '../adapters/types.js';
import { tagQuery } from '../queries/builder.js';
import { TAB_CLUSTER, sourceTag } from '../queries/source-tags.js';

export interface ClusterInfo {
  /** Cluster name, or null if no cluster detected */
  clusterName: string | null;
  /** Number of replicas in the cluster */
  replicaCount: number;
  /** Number of shards in the cluster */
  shardCount: number;
}

/**
 * Validate that a cluster name is safe for SQL interpolation.
 * Only allows alphanumeric, underscore, hyphen, and dot characters.
 */
const SAFE_CLUSTER_NAME_RE = /^[a-zA-Z0-9_.\-]+$/;

function sanitizeClusterName(name: string): string | null {
  if (!name || !SAFE_CLUSTER_NAME_RE.test(name)) return null;
  return name;
}

/**
 * Regex to match {{cluster_aware:db.table}} placeholders.
 * Accepts any database-qualified table (system.query_log, tracehouse.processes_history, etc.)
 * Used for tables where each node has unique data that should be fanned out to all replicas.
 */
const CLUSTER_AWARE_RE = /\{\{cluster_aware:(\w+\.\w+)\}\}/g;

/**
 * Regex to match {{cluster_name}} placeholder.
 * Resolves to the detected cluster name (quoted string), or empty string on single-node.
 */
const CLUSTER_NAME_RE = /\{\{cluster_name\}\}/g;

export class ClusterService {
  /**
   * Detect cluster topology by querying system.clusters.
   * Returns the first cluster that has > 1 replica, or the 'default' cluster
   * if it exists, or null if no cluster is found.
   */
  static async detect(adapter: IClickHouseAdapter): Promise<ClusterInfo> {
    try {
      // Query all clusters. We don't filter out Replicated database virtual clusters
      // here because the 'default' database may use ENGINE=Replicated, and the real
      // infrastructure cluster is also named 'default'. Instead, we rely on the
      // preference logic below (multi-replica clusters first) to pick the right one.
      const rows = await adapter.executeQuery<{
        cluster: string;
        replica_count: number;
        shard_count: number;
      }>(tagQuery(
        `SELECT cluster, count() AS replica_count, uniq(shard_num) AS shard_count
         FROM system.clusters
         GROUP BY cluster
         ORDER BY replica_count DESC, cluster ASC`,
        sourceTag(TAB_CLUSTER, 'clusterDetect')
      ));

      if (rows.length === 0) {
        return { clusterName: null, replicaCount: 1, shardCount: 1 };
      }

      // Prefer a cluster with multiple replicas; if tied, prefer 'default'
      const multiReplica = rows.filter(r => Number(r.replica_count) > 1);
      if (multiReplica.length > 0) {
        const preferred = multiReplica.find(r => String(r.cluster) === 'default') ?? multiReplica[0];
        const name = sanitizeClusterName(String(preferred.cluster));
        if (name) {
          return {
            clusterName: name,
            replicaCount: Number(preferred.replica_count),
            shardCount: Number(preferred.shard_count),
          };
        }
      }

      // Fall back to 'default' cluster if it exists (Docker Compose single-node)
      const defaultCluster = rows.find(r => String(r.cluster) === 'default');
      if (defaultCluster) {
        return {
          clusterName: 'default',
          replicaCount: Number(defaultCluster.replica_count),
          shardCount: Number(defaultCluster.shard_count),
        };
      }

      // Use whatever cluster exists — find the first with a valid name
      for (const row of rows) {
        const name = sanitizeClusterName(String(row.cluster));
        if (name) {
          return {
            clusterName: name,
            replicaCount: Number(row.replica_count),
            shardCount: Number(row.shard_count),
          };
        }
      }

      return { clusterName: null, replicaCount: 1, shardCount: 1 };
    } catch {
      // system.clusters not available or query failed — try fallback detection
      return this.detectFallback(adapter);
    }
  }

  /**
   * Fallback cluster detection when system.clusters query fails.
   * Checks system.replicas for replicated tables, which implies a cluster
   * is configured even if system.clusters is inaccessible.
   */
  private static async detectFallback(adapter: IClickHouseAdapter): Promise<ClusterInfo> {
    try {
      // Check if there are any replicated tables — if so, a cluster exists
      const replicaRows = await adapter.executeQuery<{
        total_replicas: number;
        active_replicas: number;
        zookeeper_path: string;
      }>(tagQuery(
        `SELECT
           max(total_replicas) AS total_replicas,
           max(active_replicas) AS active_replicas,
           any(zookeeper_path) AS zookeeper_path
         FROM system.replicas
         WHERE is_readonly = 0`,
        sourceTag(TAB_CLUSTER, 'clusterDetectFallback')
      ));

      if (replicaRows.length > 0 && Number(replicaRows[0].total_replicas) > 1) {
        // There are replicated tables with multiple replicas — a cluster exists.
        // We don't know the cluster name, so use 'default' as a best guess.
        // The replica count from system.replicas reflects the actual topology.
        return {
          clusterName: 'default',
          replicaCount: Number(replicaRows[0].total_replicas),
          shardCount: 1,
        };
      }
    } catch (err) {
      console.warn('[ClusterService] Cluster detection failed:', err);
    }
    return { clusterName: null, replicaCount: 1, shardCount: 1 };
  }

  /**
   * Resolve {{cluster_aware:db.table}}
   * placeholders in SQL.
   *
   * - If clusterName is null (single node): both resolve to plain db.table
   * - If clusterName is set: both resolve to clusterAllReplicas('name', db.table)
   *
   * Resolves {{cluster_aware:db.table}} to clusterAllReplicas('cluster', db.table)
   * on clusters, or strips the placeholder to bare db.table on single-node.
   * Metadata queries must include their own GROUP BY dedup to handle
   * duplicate rows from clusterAllReplicas.
   */
  static resolveTableRefs(sql: string, clusterName: string | null): string {
    if (!clusterName) {
      let result = sql.replace(CLUSTER_AWARE_RE, '$1');
      result = result.replace(CLUSTER_NAME_RE, "''");
      return result;
    }

    if (!SAFE_CLUSTER_NAME_RE.test(clusterName)) {
      let result = sql.replace(CLUSTER_AWARE_RE, '$1');
      result = result.replace(CLUSTER_NAME_RE, "''");
      return result;
    }

    let result = sql.replace(CLUSTER_AWARE_RE, (_match, tableRef: string) => {
      return `clusterAllReplicas('${clusterName}', ${tableRef})`;
    });
    result = result.replace(CLUSTER_NAME_RE, `'${clusterName}'`);

    return result;
  }
}
