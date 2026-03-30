/**
 * ClusterOverview — shows cluster topology, node list, and replication health.
 * Gracefully handles single-node (no cluster) setups.
 */

import { useEffect, useState, useCallback } from 'react';
import { useClickHouseServices } from '../providers/ClickHouseProvider';
import { useConnectionStore } from '../stores/connectionStore';
import { useClusterStore } from '../stores/clusterStore';
import { useGlobalLastUpdatedStore } from '../stores/refreshSettingsStore';
import { tagQuery, buildQuery, sourceTag, TAB_CLUSTER } from '@tracehouse/core';
import {
  GET_CLUSTERS,
  GET_CLUSTER_NODES,
  GET_REPLICATION_DETAIL,
  GET_DATABASE_ENGINES,
  GET_KEEPER_CONNECTIONS,
  GET_CLUSTER_HOST_METRICS,
} from '@tracehouse/core';
import { ClusterTopology } from '../components/cluster/ClusterTopology';
import type { HostMetrics } from '../components/cluster/ClusterTopology';
import { PermissionGate } from '../components/shared/PermissionGate';
import { extractErrorMessage } from '../utils/errorFormatters';
import { useCapabilityCheck } from '../components/shared/RequiresCapability';
import { DocsLink } from '../components/common/DocsLink';
import { Link } from 'react-router-dom';

// ── Types ──

interface ClusterRow {
  cluster: string;
  node_count: number;
  shard_count: number;
  max_replicas_per_shard: number;
  [key: string]: unknown;
}

interface NodeRow {
  cluster: string;
  shard_num: number;
  shard_weight: number;
  replica_num: number;
  host_name: string;
  host_address: string;
  port: number;
  is_local: number;
  errors_count: number;
  slowdowns_count: number;
  estimated_recovery_time: number;
  [key: string]: unknown;
}

interface ReplicaRow {
  database: string;
  table: string;
  engine: string;
  is_leader: number;
  is_readonly: number;
  is_session_expired: number;
  absolute_delay: number;
  queue_size: number;
  inserts_in_queue: number;
  merges_in_queue: number;
  total_replicas: number;
  active_replicas: number;
  replica_name: string;
  zookeeper_path: string;
  shard_count: number;
  [key: string]: unknown;
}

interface DatabaseEngineRow {
  name: string;
  engine: string;
  data_path: string;
  uuid: string;
  [key: string]: unknown;
}

interface KeeperNode {
  host: string;
  port: number;
  index: number;
  connected_time: string;
  is_expired: number;
  keeper_api_version: number;
  [key: string]: unknown;
}

// ── Shared table styling ──

const TH_STYLE: React.CSSProperties = {
  padding: '6px 8px',
  textAlign: 'left',
  color: 'var(--text-muted)',
  fontWeight: 500,
  fontSize: 10,
};

const TH_CENTER: React.CSSProperties = { ...TH_STYLE, textAlign: 'center' };
const TH_RIGHT: React.CSSProperties = { ...TH_STYLE, textAlign: 'right' };

const TD_STYLE: React.CSSProperties = { padding: '5px 8px' };
const TD_MONO: React.CSSProperties = { ...TD_STYLE, fontFamily: 'monospace', color: 'var(--text-primary)' };
const TD_MUTED: React.CSSProperties = { ...TD_STYLE, color: 'var(--text-muted)' };
const TD_CENTER: React.CSSProperties = { ...TD_STYLE, textAlign: 'center' };

function rowBg(i: number): string | undefined {
  return i % 2 === 0 ? 'transparent' : 'var(--bg-tertiary)';
}

// ── No Connection ──

const NoConnection: React.FC<{ onConnect: () => void }> = ({ onConnect }) => (
  <div className="flex flex-col items-center justify-center py-16">
    <div
      className="w-16 h-16 rounded-full flex items-center justify-center text-xl font-bold mb-4"
      style={{ background: 'var(--bg-tertiary)', color: 'var(--text-muted)' }}
    >
      --
    </div>
    <h3 className="text-lg font-semibold mb-2" style={{ color: 'var(--text-primary)' }}>
      No Connection
    </h3>
    <p className="text-sm mb-4 text-center max-w-md" style={{ color: 'var(--text-secondary)' }}>
      Connect to a ClickHouse server to view cluster information
    </p>
    <button className="btn btn-primary" onClick={onConnect}>Add Connection</button>
  </div>
);

// ── Single Node Banner ──

const SingleNodeBanner: React.FC = () => (
  <div
    className="rounded-lg border"
    style={{ background: 'var(--bg-card)', borderColor: 'var(--border-primary)', padding: '32px 24px', textAlign: 'center' }}
  >
    <div style={{ fontSize: 14, fontWeight: 600, letterSpacing: '0.5px', color: 'var(--text-muted)', marginBottom: 12, opacity: 0.4 }}>—</div>
    <div style={{ fontSize: 14, fontWeight: 600, color: 'var(--text-primary)', marginBottom: 6 }}>
      Single-Node Server
    </div>
    <div style={{ fontSize: 12, color: 'var(--text-muted)', maxWidth: 420, margin: '0 auto' }}>
      No cluster detected. This server is running as a standalone instance.
      Cluster features like replication and sharding are not configured.
    </div>
  </div>
);

// ── Node Table ──

const NodeTable: React.FC<{ nodes: NodeRow[] }> = ({ nodes }) => (
  <div
    className="rounded-lg border overflow-hidden"
    style={{ background: 'var(--bg-card)', borderColor: 'var(--border-primary)' }}
  >
    <div style={{ padding: '12px 16px', borderBottom: '1px solid var(--border-secondary)' }}>
      <h3 style={{ fontSize: 11, fontWeight: 500, color: 'var(--text-secondary)' }}>
        Cluster Nodes
        <span style={{ marginLeft: 8, color: 'var(--text-muted)' }}>({nodes.length})</span>
      </h3>
    </div>
    <div style={{ overflowX: 'auto' }}>
      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11 }}>
        <thead>
          <tr style={{ borderBottom: '1px solid var(--border-secondary)' }}>
            <th style={TH_STYLE}>Host</th>
            <th style={TH_STYLE}>Address</th>
            <th style={TH_CENTER}>Shard</th>
            <th style={TH_CENTER}>Replica</th>
            <th style={TH_CENTER}>Port</th>
            <th style={TH_CENTER}>Local</th>
            <th style={TH_RIGHT}>Errors</th>
            <th style={TH_RIGHT}>Slowdowns</th>
          </tr>
        </thead>
        <tbody>
          {nodes.map((n, i) => (
            <tr key={i} style={{ borderBottom: '1px solid var(--border-secondary)', background: rowBg(i) }}>
              <td style={TD_MONO}>{n.host_name}</td>
              <td style={{ ...TD_STYLE, fontFamily: 'monospace', color: 'var(--text-muted)' }}>{n.host_address}</td>
              <td style={{ ...TD_CENTER, color: 'var(--text-primary)' }}>{n.shard_num}</td>
              <td style={{ ...TD_CENTER, color: 'var(--text-primary)' }}>{n.replica_num}</td>
              <td style={{ ...TD_CENTER, fontFamily: 'monospace', color: 'var(--text-muted)' }}>{n.port}</td>
              <td style={TD_CENTER}>
                {Number(n.is_local) ? (
                  <span style={{ color: '#22c55e' }}>●</span>
                ) : (
                  <span style={{ color: 'var(--text-muted)' }}>○</span>
                )}
              </td>
              <td style={{ ...TD_STYLE, textAlign: 'right', fontFamily: 'monospace', color: Number(n.errors_count) > 0 ? '#ef4444' : 'var(--text-muted)' }}>
                {n.errors_count}
              </td>
              <td style={{ ...TD_STYLE, textAlign: 'right', fontFamily: 'monospace', color: Number(n.slowdowns_count) > 0 ? '#f59e0b' : 'var(--text-muted)' }}>
                {n.slowdowns_count}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  </div>
);

// ── Unified Databases & Replication Table ──

interface DatabaseReplicationTableProps {
  databases: DatabaseEngineRow[];
  replicas: ReplicaRow[];
}

const DatabaseReplicationTable: React.FC<DatabaseReplicationTableProps> = ({ databases, replicas }) => {
  const filtered = databases.filter(d => !['INFORMATION_SCHEMA', 'information_schema'].includes(d.name));

  // Group replicas by database
  const replicasByDb = new Map<string, ReplicaRow[]>();
  for (const r of replicas) {
    const db = String(r.database);
    if (!replicasByDb.has(db)) replicasByDb.set(db, []);
    replicasByDb.get(db)!.push(r);
  }

  // Build rows: one per database, with aggregated replication info
  const rows = filtered.map(d => {
    const dbReplicas = replicasByDb.get(d.name) || [];
    const replicatedTables = dbReplicas.length;
    const healthyTables = dbReplicas.filter(r => !Number(r.is_readonly) && Number(r.absolute_delay) < 300).length;
    const maxDelay = dbReplicas.reduce((max, r) => Math.max(max, Number(r.absolute_delay)), 0);
    const totalQueue = dbReplicas.reduce((sum, r) => sum + Number(r.queue_size), 0);
    const hasLeader = dbReplicas.some(r => Number(r.is_leader));
    const hasReadonly = dbReplicas.some(r => Number(r.is_readonly));
    const maxShards = dbReplicas.reduce((max, r) => Math.max(max, Number(r.shard_count)), 0);
    // Use first replica's total/active counts (same across tables in same db)
    const totalReplicas = dbReplicas.length > 0 ? Number(dbReplicas[0].total_replicas) : 0;
    const activeReplicas = dbReplicas.length > 0 ? Number(dbReplicas[0].active_replicas) : 0;
    return { ...d, replicatedTables, healthyTables, maxDelay, totalQueue, hasLeader, hasReadonly, totalReplicas, activeReplicas, maxShards, dbReplicas };
  });

  return (
    <div
      className="rounded-lg border overflow-hidden"
      style={{ background: 'var(--bg-card)', borderColor: 'var(--border-primary)' }}
    >
      <div style={{ padding: '12px 16px', borderBottom: '1px solid var(--border-secondary)', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <h3 style={{ fontSize: 11, fontWeight: 500, color: 'var(--text-secondary)' }}>
          Databases & Replication
          <span style={{ marginLeft: 8, color: 'var(--text-muted)' }}>({filtered.length})</span>
        </h3>
        <Link
          to="/replication"
          state={{ from: { path: '/cluster', label: 'Cluster' } }}
          style={{ fontSize: 10, color: 'var(--text-muted)', textDecoration: 'none', display: 'flex', alignItems: 'center', gap: 4 }}
        >
          Per-table replication <span style={{ fontSize: 9 }}>→</span>
        </Link>
      </div>
      <div style={{ overflowX: 'auto', maxHeight: 520, overflowY: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11 }}>
          <thead className="sticky top-0 z-10" style={{ background: 'var(--bg-card)' }}>
            <tr style={{ borderBottom: '1px solid var(--border-secondary)' }}>
              <th style={TH_STYLE}>Database</th>
              <th style={TH_STYLE}>Engine</th>
              <th style={TH_CENTER}>DDL Replicated</th>
              <th style={TH_CENTER}>Tables</th>
              <th style={TH_CENTER}>Shards</th>
              <th style={TH_CENTER}>Health</th>
              <th style={TH_CENTER}>Replicas</th>
              <th style={TH_CENTER}>Leader</th>
              <th style={TH_RIGHT}>Max Delay</th>
              <th style={TH_RIGHT}>Queue</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((d, i) => (
              <tr key={i} style={{ borderBottom: '1px solid var(--border-secondary)', background: rowBg(i) }}>
                <td style={TD_MONO}>{d.name}</td>
                <td style={TD_MUTED}>{d.engine}</td>
                <td style={TD_CENTER}>
                  {d.engine === 'Replicated' ? (
                    <span style={{ padding: '1px 5px', fontSize: 10, fontWeight: 500, borderRadius: 4, backgroundColor: 'rgba(34,197,94,0.15)', color: '#22c55e' }}>Yes</span>
                  ) : (
                    <span style={{ color: 'var(--text-muted)' }}>—</span>
                  )}
                </td>
                <td style={{ ...TD_CENTER, fontFamily: 'monospace', color: d.replicatedTables > 0 ? 'var(--text-primary)' : 'var(--text-muted)' }}>
                  {d.replicatedTables > 0 ? d.replicatedTables : '—'}
                </td>
                <td style={{ ...TD_CENTER, fontFamily: 'monospace', color: d.maxShards > 0 ? 'var(--text-primary)' : 'var(--text-muted)' }}>
                  {d.maxShards > 0 ? d.maxShards : '—'}
                </td>
                <td style={TD_CENTER}>
                  {d.replicatedTables > 0 ? (
                    d.healthyTables === d.replicatedTables ? (
                      <span style={{ padding: '1px 5px', fontSize: 10, fontWeight: 500, borderRadius: 4, backgroundColor: 'rgba(34,197,94,0.15)', color: '#22c55e' }}>
                        {d.healthyTables}/{d.replicatedTables}
                      </span>
                    ) : (
                      <span style={{ padding: '1px 5px', fontSize: 10, fontWeight: 500, borderRadius: 4, backgroundColor: 'rgba(239,68,68,0.15)', color: '#ef4444' }}>
                        {d.healthyTables}/{d.replicatedTables}
                      </span>
                    )
                  ) : (
                    <span style={{ color: 'var(--text-muted)' }}>—</span>
                  )}
                </td>
                <td style={{ ...TD_CENTER, fontFamily: 'monospace', color: d.totalReplicas > 0 ? 'var(--text-primary)' : 'var(--text-muted)' }}>
                  {d.totalReplicas > 0 ? `${d.activeReplicas}/${d.totalReplicas}` : '—'}
                </td>
                <td style={TD_CENTER}>
                  {d.hasLeader ? (
                    <span title="Leader" style={{ fontSize: 10, fontWeight: 600, color: '#3fb950', padding: '1px 5px', borderRadius: 4, backgroundColor: 'rgba(63,185,80,0.12)' }}>L</span>
                  ) : d.replicatedTables > 0 ? (
                    <span style={{ color: 'var(--text-muted)' }}>—</span>
                  ) : (
                    <span style={{ color: 'var(--text-muted)' }}>—</span>
                  )}
                </td>
                <td style={{
                  ...TD_STYLE, textAlign: 'right', fontFamily: 'monospace',
                  color: d.maxDelay > 300 ? '#ef4444' : d.maxDelay > 60 ? '#f59e0b' : 'var(--text-muted)',
                }}>
                  {d.replicatedTables > 0 ? `${d.maxDelay}s` : '—'}
                </td>
                <td style={{
                  ...TD_STYLE, textAlign: 'right', fontFamily: 'monospace',
                  color: d.totalQueue > 0 ? '#f59e0b' : 'var(--text-muted)',
                }}>
                  {d.replicatedTables > 0 ? d.totalQueue : '—'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

// ── Main Page Component ──

export const ClusterOverview: React.FC = () => {
  const services = useClickHouseServices();
  const { activeProfileId, profiles, setConnectionFormOpen } = useConnectionStore();
  const { clusterName, replicaCount, detected } = useClusterStore();

  const activeProfile = profiles.find(p => p.id === activeProfileId);
  const isConnected = activeProfile?.is_connected ?? false;

  const [clusters, setClusters] = useState<ClusterRow[]>([]);
  const [nodes, setNodes] = useState<NodeRow[]>([]);
  const [replicas, setReplicas] = useState<ReplicaRow[]>([]);
  const [databases, setDatabases] = useState<DatabaseEngineRow[]>([]);
  const [keeperNodes, setKeeperNodes] = useState<KeeperNode[]>([]);
  const [hostMetrics, setHostMetrics] = useState<HostMetrics[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const { available: hasClusters, probing: isCapProbing } = useCapabilityCheck(['system_clusters']);

  const handleOpenConnectionForm = useCallback(() => {
    setConnectionFormOpen(true);
  }, [setConnectionFormOpen]);

  // Fetch all cluster data
  useEffect(() => {
    if (!services || !isConnected || !detected) return;
    // Wait for capability probe to finish before querying
    if (isCapProbing) return;
    if (!hasClusters) {
      setError('Insufficient privileges to access system.clusters. Ask your administrator to grant SELECT on this table.');
      return;
    }

    let cancelled = false;
    setLoading(true);
    setError(null);

    // Timeout wrapper — prevents a single slow query from blocking the page
    const withTimeout = <T,>(promise: Promise<T>, ms = 15_000): Promise<T> =>
      Promise.race([
        promise,
        new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error('Query timed out')), ms)
        ),
      ]);

    (async () => {
      try {
        // Critical queries — these must succeed for the page to render
        const clusterRows = await withTimeout(services.adapter.executeQuery<ClusterRow>(
          tagQuery(GET_CLUSTERS, sourceTag(TAB_CLUSTER, 'clusters'))
        ));
        if (cancelled) return;
        setClusters(clusterRows);

        if (clusterName) {
          const nodeRows = await withTimeout(services.adapter.executeQuery<NodeRow>(
            tagQuery(buildQuery(GET_CLUSTER_NODES, { cluster_name: clusterName }), sourceTag(TAB_CLUSTER, 'nodes'))
          ));
          if (cancelled) return;
          setNodes(nodeRows);
        }

        const dbRows = await withTimeout(services.adapter.executeQuery<DatabaseEngineRow>(
          tagQuery(GET_DATABASE_ENGINES, sourceTag(TAB_CLUSTER, 'dbEngines'))
        ));
        if (!cancelled) setDatabases(dbRows);
        if (!cancelled) useGlobalLastUpdatedStore.getState().touch();
      } catch (err) {
        if (!cancelled) {
          setError(extractErrorMessage(err, 'Failed to fetch cluster data'));
        }
      } finally {
        if (!cancelled) setLoading(false);
      }

      // Non-critical queries — fire in parallel, don't block loading state
      if (cancelled) return;

      const replicaPromise = withTimeout(services.adapter.executeQuery<ReplicaRow>(
        tagQuery(GET_REPLICATION_DETAIL, sourceTag(TAB_CLUSTER, 'replication'))
      )).then(rows => { if (!cancelled) setReplicas(rows); })
        .catch(() => { if (!cancelled) setReplicas([]); });

      const keeperPromise = withTimeout(services.adapter.executeQuery<KeeperNode>(
        tagQuery(GET_KEEPER_CONNECTIONS, sourceTag(TAB_CLUSTER, 'keeperConnections'))
      )).then(rows => { if (!cancelled) setKeeperNodes(rows); })
        .catch(() => { if (!cancelled) setKeeperNodes([]); });

      const metricsPromise = withTimeout(services.adapter.executeQuery<HostMetrics>(
        tagQuery(GET_CLUSTER_HOST_METRICS, sourceTag(TAB_CLUSTER, 'hostMetrics'))
      )).then(rows => { if (!cancelled) setHostMetrics(rows); })
        .catch(() => { if (!cancelled) setHostMetrics([]); });

      await Promise.allSettled([replicaPromise, keeperPromise, metricsPromise]);
    })();

    return () => { cancelled = true; };
  }, [services, isConnected, detected, clusterName, hasClusters, isCapProbing]);

  // No connection
  if (!activeProfileId || !isConnected) {
    return (
      <div className="page-layout">
        <div className="flex items-center justify-between">
          <h1 className="text-xl font-semibold" style={{ color: 'var(--text-primary)' }}>Cluster</h1>
        </div>
        <div className="card">
          <NoConnection onConnect={handleOpenConnectionForm} />
        </div>
      </div>
    );
  }

  // Loading
  if (loading || !detected) {
    return (
      <div className="page-layout">
        <div className="flex items-center justify-between">
          <h1 className="text-xl font-semibold" style={{ color: 'var(--text-primary)' }}>Cluster</h1>
        </div>
        <div
          className="rounded-lg border"
          style={{ background: 'var(--bg-card)', borderColor: 'var(--border-primary)', padding: 24, textAlign: 'center', fontSize: 12, color: 'var(--text-muted)' }}
        >
          Detecting cluster topology...
        </div>
      </div>
    );
  }

  // Error
  if (error) {
    return (
      <div className="page-layout">
        <div className="flex items-center justify-between">
          <h1 className="text-xl font-semibold" style={{ color: 'var(--text-primary)' }}>Cluster</h1>
        </div>
        <PermissionGate error={error} title="Cluster" variant="page" />
      </div>
    );
  }

  // No cluster detected
  if (!clusterName) {
    return (
      <div className="page-layout">
        <div className="flex items-center justify-between">
          <h1 className="text-xl font-semibold" style={{ color: 'var(--text-primary)' }}>Cluster</h1>
        </div>
        <SingleNodeBanner />
        {databases.length > 0 && <DatabaseReplicationTable databases={databases} replicas={replicas} />}
      </div>
    );
  }

  // Cluster detected — full view
  const activeCluster = clusters.find(c => String(c.cluster) === clusterName);
  const healthyReplicas = replicas.filter(r => !Number(r.is_readonly) && Number(r.absolute_delay) < 300);
  const unhealthyCount = replicas.length - healthyReplicas.length;

  return (
    <div className="page-layout">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-4">
          <h1 className="text-xl font-semibold" style={{ color: 'var(--text-primary)' }}>Cluster</h1>
          <DocsLink path="/features/cluster" />
          <span className="text-sm" style={{ color: 'var(--text-muted)' }}>{clusterName}</span>
        </div>
      </div>

      {/* Summary Cards */}
      <div className="grid grid-cols-4 gap-4">
        <div className="stat-card">
          <div className="stat-value">{clusterName}</div>
          <div className="stat-label">Cluster Name</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">{replicaCount}</div>
          <div className="stat-label">Nodes</div>
          {activeCluster && (
            <div className="text-xs mt-1" style={{ color: 'var(--text-muted)' }}>
              {activeCluster.shard_count} shard{Number(activeCluster.shard_count) !== 1 ? 's' : ''}
            </div>
          )}
        </div>
        <div className="stat-card">
          <div className="stat-value">{replicas.length}</div>
          <div className="stat-label">Replicated Tables</div>
          <div className="text-xs mt-1" style={{ color: unhealthyCount > 0 ? '#f59e0b' : 'var(--text-muted)' }}>
            {unhealthyCount > 0 ? `${unhealthyCount} unhealthy` : 'all healthy'}
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-value">
            {replicas.length > 0 ? `${Math.round((healthyReplicas.length / replicas.length) * 100)}%` : 'N/A'}
          </div>
          <div className="stat-label">Replication Health</div>
        </div>
      </div>

      {/* Cluster Topology Diagram — first, it's the hero */}
      {nodes.length > 0 && (
        <ClusterTopology
          clusterName={clusterName}
          nodes={nodes}
          keeperNodes={keeperNodes}
          hostMetrics={hostMetrics}
        />
      )}

      {/* Node list — cluster infra first */}
      {nodes.length > 0 && <NodeTable nodes={nodes} />}

      {/* Databases & Replication — unified table */}
      <DatabaseReplicationTable databases={databases} replicas={replicas} />
    </div>
  );
};

export default ClusterOverview;
