/**
 * Replication — dedicated tab for replication health, queue inspection, and per-table detail.
 * Split layout: table list (left) + always-visible 3D topology (right).
 */

import React, { useEffect, useState, useMemo } from 'react';
import { useClickHouseServices } from '../providers/ClickHouseProvider';
import { useConnectionStore } from '../stores/connectionStore';
import { useClusterStore } from '../stores/clusterStore';
import { useGlobalLastUpdatedStore } from '../stores/refreshSettingsStore';
import { tagQuery, sourceTag, buildQuery } from '@tracehouse/core';
import {
  GET_REPLICATION_DETAIL,
  GET_REPLICATION_QUEUE,
} from '@tracehouse/core';
import { PermissionGate } from '../components/shared/PermissionGate';
import { extractErrorMessage } from '../utils/errorFormatters';
import { useCapabilityCheck } from '../components/shared/RequiresCapability';
import { DocsLink } from '../components/common/DocsLink';
import { ReplicationTopology } from '../components/replication/ReplicationTopology';

const TAB_REPLICATION = 'replication';

// ── Types ──

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

// ── Shared styles ──

const TH: React.CSSProperties = {
  padding: '6px 8px', textAlign: 'left', color: 'var(--text-muted)',
  fontWeight: 500, fontSize: 10, whiteSpace: 'nowrap',
};
const TH_C: React.CSSProperties = { ...TH, textAlign: 'center' };
const TH_R: React.CSSProperties = { ...TH, textAlign: 'right' };
const TD: React.CSSProperties = { padding: '5px 8px', fontSize: 11 };
const TD_MONO: React.CSSProperties = { ...TD, fontFamily: 'monospace', color: 'var(--text-primary)' };
const TD_C: React.CSSProperties = { ...TD, textAlign: 'center' };
const TD_R: React.CSSProperties = { ...TD, textAlign: 'right', fontFamily: 'monospace' };

function formatDelay(seconds: number): string {
  if (seconds === 0) return '0s';
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
  return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
}

// ── Badge helpers ──

const HealthBadge: React.FC<{ healthy: boolean; label: string }> = ({ healthy, label }) => (
  <span style={{
    padding: '1px 5px', fontSize: 10, fontWeight: 500, borderRadius: 4,
    backgroundColor: healthy ? 'rgba(34,197,94,0.15)' : 'rgba(239,68,68,0.15)',
    color: healthy ? '#22c55e' : '#ef4444',
  }}>
    {label}
  </span>
);

// ── Stat Card ──

const StatCard: React.FC<{ label: string; value: string | number; warn?: boolean }> = ({ label, value, warn }) => (
  <div className="stat-card">
    <div className="stat-value" style={warn ? { color: '#f59e0b' } : undefined}>{value}</div>
    <div className="stat-label">{label}</div>
  </div>
);

// ── Main Component ──

export const Replication: React.FC = () => {
  const services = useClickHouseServices();
  const { activeProfileId, profiles, setConnectionFormOpen } = useConnectionStore();
  const { detected } = useClusterStore();

  const activeProfile = profiles.find(p => p.id === activeProfileId);
  const isConnected = activeProfile?.is_connected ?? false;

  const [replicas, setReplicas] = useState<ReplicaRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const { available: hasReplicas, probing: isCapProbing } = useCapabilityCheck(['system_replicas']);

  // Selected table for topology view
  const [selectedTable, setSelectedTable] = useState<string | null>(null);

  // Queue expansion: key is "db.table", value is fetched queue rows
  const [expandedQueue, setExpandedQueue] = useState<string | null>(null);
  const [queueRows, setQueueRows] = useState<Record<string, unknown>[]>([]);
  const [queueLoading, setQueueLoading] = useState(false);

  // Fetch replica data
  useEffect(() => {
    if (!services || !isConnected || !detected) return;
    if (isCapProbing) return;
    if (!hasReplicas) {
      setError('Insufficient privileges to access system.replicas. Ask your administrator to grant SELECT on this table.');
      setLoading(false);
      return;
    }
    let cancelled = false;
    setLoading(true);
    setError(null);

    (async () => {
      try {
        const rows = await services.adapter.executeQuery<ReplicaRow>(
          tagQuery(GET_REPLICATION_DETAIL, sourceTag(TAB_REPLICATION, 'replicas'))
        );
        if (!cancelled) setReplicas(rows);
        if (!cancelled) useGlobalLastUpdatedStore.getState().touch();
      } catch (err) {
        if (!cancelled) setError(extractErrorMessage(err, 'Failed to fetch replication data'));
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();

    return () => { cancelled = true; };
  }, [services, isConnected, detected, hasReplicas, isCapProbing]);

  // Auto-select first table when data loads
  useEffect(() => {
    if (replicas.length > 0 && selectedTable === null) {
      setSelectedTable(`${replicas[0].database}.${replicas[0].table}`);
    }
  }, [replicas, selectedTable]);

  // Aggregate stats
  const stats = useMemo(() => {
    const total = replicas.length;
    const healthy = replicas.filter(r => !Number(r.is_readonly) && Number(r.absolute_delay) < 300).length;
    const totalQueue = replicas.reduce((s, r) => s + Number(r.queue_size), 0);
    const maxDelay = replicas.reduce((m, r) => Math.max(m, Number(r.absolute_delay)), 0);
    const readonly = replicas.filter(r => Number(r.is_readonly)).length;
    return { total, healthy, totalQueue, maxDelay, readonly };
  }, [replicas]);

  const toggleQueue = async (fullName: string, database: string) => {
    if (expandedQueue === fullName) {
      setExpandedQueue(null);
      return;
    }
    if (!services) return;
    setExpandedQueue(fullName);
    setQueueLoading(true);
    try {
      const rows = await services.adapter.executeQuery<Record<string, unknown>>(
        tagQuery(buildQuery(GET_REPLICATION_QUEUE, { database }), sourceTag(TAB_REPLICATION, 'queue'))
      );
      // Filter to this specific table
      const tableName = fullName.split('.').slice(1).join('.');
      setQueueRows(rows.filter(r => String(r.table) === tableName));
    } catch {
      setQueueRows([]);
    } finally {
      setQueueLoading(false);
    }
  };

  // Parse selected table
  const selectedDb = selectedTable ? selectedTable.split('.')[0] : null;
  const selectedTbl = selectedTable ? selectedTable.split('.').slice(1).join('.') : null;

  // No connection
  if (!activeProfileId || !isConnected) {
    return (
      <div className="page-layout">
        <h1 className="text-xl font-semibold" style={{ color: 'var(--text-primary)' }}>Replication</h1>
        <div className="card">
          <div className="card-body text-center py-8" style={{ color: 'var(--text-muted)', fontSize: 13 }}>
            <p>Connect to a ClickHouse instance to view replication status.</p>
            <button className="btn btn-primary mt-4" onClick={() => setConnectionFormOpen(true)}>Connect</button>
          </div>
        </div>
      </div>
    );
  }

  if (loading || !detected) {
    return (
      <div className="page-layout">
        <h1 className="text-xl font-semibold" style={{ color: 'var(--text-primary)' }}>Replication</h1>
        <div className="card" style={{ padding: 24, textAlign: 'center', fontSize: 12, color: 'var(--text-muted)' }}>
          Loading replication data...
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="page-layout">
        <h1 className="text-xl font-semibold" style={{ color: 'var(--text-primary)' }}>Replication</h1>
        <PermissionGate error={error} title="Replication" variant="page" />
      </div>
    );
  }

  if (replicas.length === 0) {
    return (
      <div className="page-layout">
        <h1 className="text-xl font-semibold" style={{ color: 'var(--text-primary)' }}>Replication</h1>
        <div className="card" style={{ padding: 24, textAlign: 'center', fontSize: 12, color: 'var(--text-muted)' }}>
          No replicated tables found. Replication requires ReplicatedMergeTree tables.
        </div>
      </div>
    );
  }

  return (
    <div className="page-layout">
      {/* Header */}
      <div className="flex items-center gap-4">
        <h1 className="text-xl font-semibold" style={{ color: 'var(--text-primary)' }}>Replication</h1>
        <DocsLink path="/features/replication" />
      </div>

      {/* Summary Cards */}
      <div className="grid grid-cols-5 gap-4">
        <StatCard label="Replicated Tables" value={stats.total} />
        <StatCard label="Healthy" value={`${stats.healthy}/${stats.total}`} />
        <StatCard label="Queue Size" value={stats.totalQueue} warn={stats.totalQueue > 0} />
        <StatCard label="Max Delay" value={formatDelay(stats.maxDelay)} warn={stats.maxDelay > 60} />
        <StatCard label="Read-Only" value={stats.readonly} warn={stats.readonly > 0} />
      </div>

      {/* Topology — always visible, full width */}
      {selectedDb && selectedTbl ? (
        <ReplicationTopology
          database={selectedDb}
          table={selectedTbl}
        />
      ) : (
        <div className="card" style={{
          height: 400, display: 'flex', alignItems: 'center', justifyContent: 'center',
          color: 'var(--text-muted)', fontSize: 12,
        }}>
          Select a table below to view its replication topology
        </div>
      )}

      {/* Table list — below topology */}
      <div className="card overflow-hidden">
        <div style={{ padding: '10px 14px', borderBottom: '1px solid var(--border-secondary)' }}>
          <h3 style={{ fontSize: 11, fontWeight: 500, color: 'var(--text-secondary)' }}>
            Replicated Tables
            <span style={{ marginLeft: 8, color: 'var(--text-muted)' }}>({replicas.length})</span>
          </h3>
        </div>
        <div style={{ overflowX: 'auto', maxHeight: 400, overflowY: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11 }}>
            <thead className="sticky top-0 z-10" style={{ background: 'var(--bg-card)' }}>
              <tr style={{ borderBottom: '1px solid var(--border-secondary)' }}>
                <th style={TH}>Database</th>
                <th style={TH}>Table</th>
                <th style={TH_C}>Health</th>
                <th style={TH_C}>Leader</th>
                <th style={TH_C}>Replicas</th>
                <th style={TH_C}>Shards</th>
                <th style={TH_R}>Delay</th>
                <th style={TH_R}>Queue</th>
                <th style={TH_R}>Inserts</th>
                <th style={TH_R}>Merges</th>
              </tr>
            </thead>
            <tbody>
              {replicas.map((r, i) => {
                const healthy = !Number(r.is_readonly) && Number(r.absolute_delay) < 300;
                const queueSize = Number(r.queue_size);
                const fullName = `${r.database}.${r.table}`;
                const isSelected = selectedTable === fullName;
                return (
                  <React.Fragment key={fullName}>
                  <tr
                    style={{
                      borderBottom: '1px solid var(--border-secondary)',
                      background: isSelected ? 'var(--bg-hover)' : (i % 2 === 1 ? 'var(--bg-row-alt)' : undefined),
                      cursor: 'pointer',
                      borderLeft: isSelected ? '3px solid #60a5fa' : '3px solid transparent',
                    }}
                    onClick={() => setSelectedTable(fullName)}
                  >
                    <td style={TD_MONO}>{r.database}</td>
                    <td style={TD_MONO}>{r.table}</td>
                    <td style={TD_C}>
                      <HealthBadge healthy={healthy} label={healthy ? 'OK' : Number(r.is_readonly) ? 'RO' : 'LAG'} />
                    </td>
                    <td style={TD_C}>
                      {Number(r.is_leader) ? (
                        <span style={{ fontSize: 10, fontWeight: 600, color: '#3fb950', padding: '1px 5px', borderRadius: 4, backgroundColor: 'rgba(63,185,80,0.12)' }}>L</span>
                      ) : (
                        <span style={{ color: 'var(--text-muted)' }}>—</span>
                      )}
                    </td>
                    <td style={{ ...TD_C, fontFamily: 'monospace' }}>
                      {Number(r.active_replicas)}/{Number(r.total_replicas)}
                    </td>
                    <td style={{ ...TD_C, fontFamily: 'monospace' }}>
                      {Number(r.shard_count)}
                    </td>
                    <td style={{
                      ...TD_R,
                      color: Number(r.absolute_delay) > 300 ? '#ef4444' : Number(r.absolute_delay) > 60 ? '#f59e0b' : 'var(--text-muted)',
                    }}>
                      {formatDelay(Number(r.absolute_delay))}
                    </td>
                    <td
                      style={{
                        ...TD_R,
                        color: queueSize > 0 ? '#f59e0b' : 'var(--text-muted)',
                        cursor: queueSize > 0 ? 'pointer' : undefined,
                        textDecoration: queueSize > 0 ? 'underline' : undefined,
                      }}
                      onClick={queueSize > 0 ? (e) => { e.stopPropagation(); toggleQueue(fullName, r.database); } : undefined}
                      title={queueSize > 0 ? 'Click to expand queue details' : undefined}
                    >
                      {queueSize}
                    </td>
                    <td style={{ ...TD_R, color: Number(r.inserts_in_queue) > 0 ? 'var(--text-primary)' : 'var(--text-muted)' }}>
                      {Number(r.inserts_in_queue)}
                    </td>
                    <td style={{ ...TD_R, color: Number(r.merges_in_queue) > 0 ? 'var(--text-primary)' : 'var(--text-muted)' }}>
                      {Number(r.merges_in_queue)}
                    </td>
                  </tr>
                  {expandedQueue === fullName && (
                    <tr>
                      <td colSpan={10} style={{ padding: 0 }}>
                        <div style={{ padding: '8px 12px', backgroundColor: 'var(--bg-secondary)', borderBottom: '1px solid var(--border-secondary)' }}>
                          {queueLoading ? (
                            <span style={{ fontSize: 10, color: 'var(--text-muted)' }}>Loading queue...</span>
                          ) : queueRows.length === 0 ? (
                            <span style={{ fontSize: 10, color: 'var(--text-muted)' }}>Queue is empty</span>
                          ) : (
                            <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                              <thead>
                                <tr>
                                  <th style={{ ...TH, fontSize: 9 }}>Replica</th>
                                  <th style={{ ...TH, fontSize: 9 }}>Type</th>
                                  <th style={{ ...TH, fontSize: 9 }}>Part</th>
                                  <th style={{ ...TH_C, fontSize: 9 }}>Tries</th>
                                  <th style={{ ...TH_C, fontSize: 9 }}>Running</th>
                                  <th style={{ ...TH, fontSize: 9 }}>Exception</th>
                                  <th style={{ ...TH, fontSize: 9 }}>Created</th>
                                </tr>
                              </thead>
                              <tbody>
                                {queueRows.map((q, qi) => {
                                  const tries = Number(q.num_tries);
                                  const hasErr = String(q.last_exception || '') !== '';
                                  const stuck = tries > 5 && hasErr;
                                  return (
                                    <tr key={qi} style={{ borderBottom: '1px solid var(--border-secondary)' }}>
                                      <td style={{ ...TD_MONO, fontSize: 10 }}>{String(q.replica_name)}</td>
                                      <td style={{ ...TD_MONO, fontSize: 10, color: stuck ? '#ef4444' : '#f59e0b' }}>{String(q.type)}</td>
                                      <td style={{ ...TD_MONO, fontSize: 10 }}>{String(q.new_part_name)}</td>
                                      <td style={{ ...TD_C, fontSize: 10, fontFamily: 'monospace', color: stuck ? '#ef4444' : tries > 0 ? '#f59e0b' : 'var(--text-muted)' }}>{tries}</td>
                                      <td style={{ ...TD_C, fontSize: 10 }}>{Number(q.is_currently_executing) ? '▶' : '—'}</td>
                                      <td style={{ ...TD, fontSize: 9, color: '#ef4444', maxWidth: 300, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={String(q.last_exception || '')}>
                                        {hasErr ? String(q.last_exception).slice(0, 80) : '—'}
                                      </td>
                                      <td style={{ ...TD, fontSize: 10, color: 'var(--text-muted)', fontFamily: 'monospace' }}>{String(q.create_time)}</td>
                                    </tr>
                                  );
                                })}
                              </tbody>
                            </table>
                          )}
                        </div>
                      </td>
                    </tr>
                  )}
                  </React.Fragment>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};

export default Replication;
