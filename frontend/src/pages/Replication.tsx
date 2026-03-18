/**
 * Replication — dedicated tab for replication health, queue inspection, and per-table detail.
 */

import React, { useEffect, useState, useCallback, useMemo } from 'react';
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

interface QueueEntry {
  database: string;
  table: string;
  replica_name: string;
  type: string;
  create_time: string;
  source_replica: string;
  new_part_name: string;
  parts_to_merge: string;
  is_currently_executing: number;
  num_tries: number;
  last_attempt_time: string;
  last_exception: string;
  num_postponed: number;
  postpone_reason: string;
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
const TD_MUTED: React.CSSProperties = { ...TD, color: 'var(--text-muted)' };

function rowBg(i: number): string | undefined {
  return i % 2 === 1 ? 'var(--bg-row-alt)' : undefined;
}

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

const TypeBadge: React.FC<{ type: string }> = ({ type }) => {
  const colors: Record<string, { bg: string; fg: string }> = {
    MERGE_PARTS: { bg: 'rgba(99,102,241,0.15)', fg: '#818cf8' },
    GET_PART: { bg: 'rgba(34,197,94,0.15)', fg: '#22c55e' },
    MUTATE_PART: { bg: 'rgba(245,158,11,0.15)', fg: '#f59e0b' },
    ALTER_METADATA: { bg: 'rgba(168,85,247,0.15)', fg: '#a855f7' },
    DROP_RANGE: { bg: 'rgba(239,68,68,0.15)', fg: '#ef4444' },
  };
  const c = colors[type] || { bg: 'rgba(148,163,184,0.15)', fg: '#94a3b8' };
  return (
    <span style={{
      padding: '1px 5px', fontSize: 10, fontWeight: 500, borderRadius: 4,
      backgroundColor: c.bg, color: c.fg,
    }}>
      {type}
    </span>
  );
};

// ── Stat Card ──

const StatCard: React.FC<{ label: string; value: string | number; warn?: boolean }> = ({ label, value, warn }) => (
  <div className="stat-card">
    <div className="stat-value" style={warn ? { color: '#f59e0b' } : undefined}>{value}</div>
    <div className="stat-label">{label}</div>
  </div>
);

// ── Queue Detail Panel ──

const QueueDetailPanel: React.FC<{
  entries: QueueEntry[];
  loading: boolean;
  tableName: string;
}> = ({ entries, loading, tableName }) => {
  if (loading) {
    return (
      <div style={{ padding: 12, fontSize: 11, color: 'var(--text-muted)', textAlign: 'center' }}>
        Loading queue for {tableName}...
      </div>
    );
  }
  if (entries.length === 0) {
    return (
      <div style={{ padding: 12, fontSize: 11, color: 'var(--text-muted)', textAlign: 'center' }}>
        Queue is empty for {tableName}
      </div>
    );
  }
  return (
    <div style={{ overflowX: 'auto' }}>
      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11 }}>
        <thead>
          <tr style={{ borderBottom: '1px solid var(--border-secondary)' }}>
            <th style={TH}>Type</th>
            <th style={TH}>Part</th>
            <th style={TH}>Source</th>
            <th style={TH_C}>Executing</th>
            <th style={TH_R}>Tries</th>
            <th style={TH_R}>Postponed</th>
            <th style={TH}>Created</th>
            <th style={TH}>Error</th>
          </tr>
        </thead>
        <tbody>
          {entries.map((e, i) => (
            <tr key={i} style={{ borderBottom: '1px solid var(--border-secondary)', background: rowBg(i) }}>
              <td style={TD}><TypeBadge type={e.type} /></td>
              <td style={TD_MONO}>{e.new_part_name}</td>
              <td style={TD_MUTED}>{e.source_replica || '—'}</td>
              <td style={TD_C}>
                {Number(e.is_currently_executing) ? (
                  <span style={{ color: '#22c55e', fontSize: 10, fontWeight: 600 }}>●</span>
                ) : (
                  <span style={{ color: 'var(--text-muted)' }}>—</span>
                )}
              </td>
              <td style={{ ...TD_R, color: Number(e.num_tries) > 3 ? '#f59e0b' : 'var(--text-muted)' }}>
                {e.num_tries}
              </td>
              <td style={{ ...TD_R, color: Number(e.num_postponed) > 0 ? '#f59e0b' : 'var(--text-muted)' }}>
                {e.num_postponed}
              </td>
              <td style={TD_MUTED}>{e.create_time}</td>
              <td style={{ ...TD, color: e.last_exception ? '#ef4444' : 'var(--text-muted)', maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}
                  title={e.last_exception || undefined}>
                {e.last_exception || '—'}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

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

  // Queue drill-down state
  const [expandedDb, setExpandedDb] = useState<string | null>(null);
  const [queueEntries, setQueueEntries] = useState<QueueEntry[]>([]);
  const [queueLoading, setQueueLoading] = useState(false);
  const [expandedTable, setExpandedTable] = useState<string | null>(null);

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

  // Fetch queue for a database
  const fetchQueue = useCallback(async (database: string) => {
    if (!services || !isConnected) return;
    setQueueLoading(true);
    try {
      const rows = await services.adapter.executeQuery<QueueEntry>(
        tagQuery(
          buildQuery(GET_REPLICATION_QUEUE, { database }),
          sourceTag(TAB_REPLICATION, 'queue')
        )
      );
      setQueueEntries(rows);
    } catch {
      setQueueEntries([]);
    } finally {
      setQueueLoading(false);
    }
  }, [services, isConnected]);

  const handleToggleDb = useCallback((db: string) => {
    if (expandedDb === db) {
      setExpandedDb(null);
      setQueueEntries([]);
      setExpandedTable(null);
    } else {
      setExpandedDb(db);
      setExpandedTable(null);
      fetchQueue(db);
    }
  }, [expandedDb, fetchQueue]);

  // Aggregate stats
  const stats = useMemo(() => {
    const total = replicas.length;
    const healthy = replicas.filter(r => !Number(r.is_readonly) && Number(r.absolute_delay) < 300).length;
    const totalQueue = replicas.reduce((s, r) => s + Number(r.queue_size), 0);
    const maxDelay = replicas.reduce((m, r) => Math.max(m, Number(r.absolute_delay)), 0);
    const readonly = replicas.filter(r => Number(r.is_readonly)).length;
    return { total, healthy, totalQueue, maxDelay, readonly };
  }, [replicas]);

  // Group by database
  const byDatabase = useMemo(() => {
    const map = new Map<string, ReplicaRow[]>();
    for (const r of replicas) {
      const db = String(r.database);
      if (!map.has(db)) map.set(db, []);
      map.get(db)!.push(r);
    }
    return map;
  }, [replicas]);

  // Queue entries for expanded table
  const tableQueueEntries = useMemo(() => {
    if (!expandedTable) return queueEntries;
    return queueEntries.filter(e => `${e.database}.${e.table}` === expandedTable);
  }, [queueEntries, expandedTable]);

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
      <h1 className="text-xl font-semibold" style={{ color: 'var(--text-primary)' }}>Replication</h1>

      {/* Summary Cards */}
      <div className="grid grid-cols-5 gap-4">
        <StatCard label="Replicated Tables" value={stats.total} />
        <StatCard label="Healthy" value={`${stats.healthy}/${stats.total}`} />
        <StatCard label="Queue Size" value={stats.totalQueue} warn={stats.totalQueue > 0} />
        <StatCard label="Max Delay" value={formatDelay(stats.maxDelay)} warn={stats.maxDelay > 60} />
        <StatCard label="Read-Only" value={stats.readonly} warn={stats.readonly > 0} />
      </div>

      {/* Per-table replication table */}
      <div className="card overflow-hidden">
        <div style={{ padding: '12px 16px', borderBottom: '1px solid var(--border-secondary)' }}>
          <h3 style={{ fontSize: 11, fontWeight: 500, color: 'var(--text-secondary)' }}>
            Replica Status
            <span style={{ marginLeft: 8, color: 'var(--text-muted)' }}>({replicas.length} tables)</span>
          </h3>
        </div>
        <div style={{ overflowX: 'auto', maxHeight: 600, overflowY: 'auto' }}>
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
              {Array.from(byDatabase.entries()).map(([db, tables]) =>
                tables.map((r, i) => {
                  const healthy = !Number(r.is_readonly) && Number(r.absolute_delay) < 300;
                  const queueSize = Number(r.queue_size);
                  const fullName = `${r.database}.${r.table}`;
                  const isExpanded = expandedTable === fullName;
                  return (
                    <React.Fragment key={fullName}>
                      <tr
                        style={{
                          borderBottom: '1px solid var(--border-secondary)',
                          background: isExpanded ? 'var(--bg-hover)' : rowBg(i),
                          cursor: queueSize > 0 ? 'pointer' : 'default',
                        }}
                        onClick={() => {
                          if (queueSize > 0) {
                            if (expandedDb !== db) {
                              handleToggleDb(db);
                            }
                            setExpandedTable(isExpanded ? null : fullName);
                          }
                        }}
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
                        <td style={{
                          ...TD_R,
                          color: queueSize > 0 ? '#f59e0b' : 'var(--text-muted)',
                          textDecoration: queueSize > 0 ? 'underline' : 'none',
                        }}>
                          {queueSize}
                        </td>
                        <td style={{ ...TD_R, color: Number(r.inserts_in_queue) > 0 ? 'var(--text-primary)' : 'var(--text-muted)' }}>
                          {Number(r.inserts_in_queue)}
                        </td>
                        <td style={{ ...TD_R, color: Number(r.merges_in_queue) > 0 ? 'var(--text-primary)' : 'var(--text-muted)' }}>
                          {Number(r.merges_in_queue)}
                        </td>
                      </tr>
                      {isExpanded && (
                        <tr>
                          <td colSpan={10} style={{ padding: 0, background: 'var(--bg-secondary)' }}>
                            <div style={{ padding: '8px 16px', borderBottom: '2px solid var(--border-primary)' }}>
                              <div style={{ fontSize: 10, fontWeight: 500, color: 'var(--text-muted)', marginBottom: 6 }}>
                                Queue entries for {fullName}
                              </div>
                              <QueueDetailPanel
                                entries={tableQueueEntries}
                                loading={queueLoading && expandedDb === db}
                                tableName={fullName}
                              />
                            </div>
                          </td>
                        </tr>
                      )}
                    </React.Fragment>
                  );
                })
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};

export default Replication;
