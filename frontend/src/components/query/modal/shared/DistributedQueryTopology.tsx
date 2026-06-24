/**
 * DistributedQueryTopology — Gantt-style timeline showing coordinator + node sub-queries.
 * Renders inline metrics on each bar and supports click-to-navigate between queries.
 */

import React, { useMemo } from 'react';
import {
  inferDistributedTopology,
  type DistributedQueryExecutionInput,
  type DistributedTopology,
  type DistributedTopologyNode,
  type SubQueryInfo,
} from '@tracehouse/core';
import { formatDurationMs } from '../../../../utils/formatters';
import { formatBytes } from '../../../../stores/databaseStore';

export interface TopologyCoordinator {
  query_id: string;
  hostname: string;
  query_duration_ms: number;
  query_start_time_microseconds: string;
  memory_usage: number;
  read_rows: number;
  exception?: string;
}

interface DistributedQueryTopologyProps {
  coordinator: TopologyCoordinator;
  subQueries: SubQueryInfo[];
  inferredTopology?: DistributedTopology | null;
  /** The query_id currently being viewed (to highlight "you are here") */
  activeQueryId: string;
  /** Navigate to a query by ID */
  onNavigate: (queryId: string) => void;
  isLoading?: boolean;
}

const COORD_COLOR = '#58a6ff';
const NODE_COLOR = '#d29922';
const SHARD_LEADER_COLOR = '#a371f7';
const REPLICA_READER_COLOR = '#d29922';
const OBJECT_WORKER_COLOR = '#3fb950';
const INSERT_COLOR = '#db6d28';
const ERROR_COLOR = '#f85149';
const MUTED_COLOR = 'var(--text-muted)';
function stableHostColor(host: string, index = 0): string {
  let hash = 0;
  for (let i = 0; i < host.length; i += 1) {
    hash = ((hash << 5) - hash + host.charCodeAt(i)) | 0;
  }
  const hue = Math.abs(hash + index * 47) % 360;
  return `hsl(${hue} 72% 48%)`;
}

/** Parse ClickHouse microsecond timestamp to epoch microseconds */
function parseUs(ts: string): number {
  if (!ts) return 0;
  const dotIdx = ts.lastIndexOf('.');
  const baseDateStr = dotIdx >= 0 ? ts.substring(0, dotIdx) : ts;
  const usFrac = dotIdx >= 0 ? ts.substring(dotIdx + 1) : '0';
  const baseMs = new Date(baseDateStr.replace(' ', 'T') + 'Z').getTime();
  return baseMs * 1000 + parseInt(usFrac.padEnd(6, '0').substring(0, 6), 10);
}

function shortHost(hostname: string): string {
  const short = hostname.split('.')[0] || hostname;
  const tail = short.match(/-(\d+)-(\d+)-\d+$/);
  if (short.startsWith('chi-') && tail) return `s${Number(tail[1]) + 1}r${Number(tail[2]) + 1}`;
  const cloudServerMatch = short.match(/-server-([a-z0-9]+)-\d+$/);
  if (cloudServerMatch) return cloudServerMatch[1];
  const demoMatch = short.match(/ch-s(\d+)r(\d+)/);
  if (demoMatch) return `s${demoMatch[1]}r${demoMatch[2]}`;
  return short;
}

function fmtCompact(n: number): string {
  if (n < 1000) return n.toString();
  if (n < 1_000_000) return `${(n / 1000).toFixed(1)}K`;
  if (n < 1_000_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  return `${(n / 1_000_000_000).toFixed(1)}B`;
}

function roleLabel(node?: DistributedTopologyNode): string {
  if (!node) return 'Child';
  switch (node.role) {
    case 'coordinator': return 'Coordinator';
    case 'shard_leader': return node.shardNum ? `Shard ${node.shardNum} coordinator` : 'Shard coordinator';
    case 'replica_reader': return node.shardNum ? `Shard ${node.shardNum} reader` : 'Replica reader';
    case 'remote_child': return node.shardNum ? `Shard ${node.shardNum} child` : 'Remote child';
    case 'independent_child': return 'Independent child';
    case 'object_storage_worker': return 'Object worker';
    case 'hybrid_segment': return 'Hybrid segment';
    case 'insert_client': return 'Insert client';
    case 'insert_forwarder': return 'Remote table INSERT';
    case 'async_insert_flush': return 'Async insert flush';
    default: return 'Child';
  }
}

function coordinatorRoleLabel(topology: DistributedTopology): string {
  if (topology.localRead) {
    const shard = topology.localRead.shardNum != null && topology.localRead.replicaNum != null
      ? `s${topology.localRead.shardNum}r${topology.localRead.replicaNum}`
      : 'local replica';
    return `Coordinator + local read (${shard})`;
  }
  return topology.clusterAllReplicas ? 'Coordinator · all-replicas fan-out' : 'Coordinator';
}

function roleColor(node: DistributedTopologyNode | undefined, hasError: boolean): string {
  if (hasError) return ERROR_COLOR;
  if (!node) return NODE_COLOR;
  if (node.role === 'shard_leader') return SHARD_LEADER_COLOR;
  if (node.role === 'replica_reader') return REPLICA_READER_COLOR;
  if (node.role === 'object_storage_worker' || node.role === 'hybrid_segment') return OBJECT_WORKER_COLOR;
  if (node.role === 'insert_forwarder' || node.role === 'async_insert_flush') return INSERT_COLOR;
  return NODE_COLOR;
}

function hostIdentity(hostname: string): string {
  return hostname.split('.')[0] || hostname;
}

function roleOrder(role?: DistributedTopologyNode['role']): number {
  if (role === 'insert_forwarder') return 0;
  if (role === 'async_insert_flush') return 1;
  if (role === 'shard_leader') return 0;
  if (role === 'replica_reader') return 1;
  return 2;
}

export const DistributedQueryTopology: React.FC<DistributedQueryTopologyProps> = ({
  coordinator,
  subQueries,
  inferredTopology,
  activeQueryId,
  onNavigate,
  isLoading,
}) => {
  const topology = useMemo(() => {
    const executions: DistributedQueryExecutionInput[] = [
      {
        queryId: coordinator.query_id,
        initialQueryId: coordinator.query_id,
        isInitialQuery: true,
        hostname: coordinator.hostname,
        queryKind: 'Select',
        queryStartTimeMicroseconds: coordinator.query_start_time_microseconds,
        queryDurationMs: coordinator.query_duration_ms,
        readRows: coordinator.read_rows,
        memoryUsage: coordinator.memory_usage,
      },
      ...subQueries.map((sq): DistributedQueryExecutionInput => ({
        queryId: sq.query_id,
        initialQueryId: coordinator.query_id,
        isInitialQuery: false,
        hostname: sq.hostname,
        queryKind: 'Select',
        queryStartTimeMicroseconds: sq.query_start_time_microseconds,
        queryDurationMs: sq.query_duration_ms,
        readRows: sq.read_rows,
        readBytes: sq.read_bytes,
        memoryUsage: sq.memory_usage,
        queryPreview: sq.query_preview,
      })),
    ];

    if (inferredTopology) return inferredTopology;

    return inferDistributedTopology({
      rootQueryId: coordinator.query_id,
      executions,
      capabilities: {
        profileEvents: false,
        processorsProfileLog: false,
        systemClusters: false,
        textLog: false,
      },
    });
  }, [coordinator, subQueries, inferredTopology]);

  const nodeByQueryId = useMemo(() => {
    const byQueryId = new Map<string, DistributedTopologyNode[]>();
    for (const node of topology.nodes) {
      const existing = byQueryId.get(node.queryId);
      if (existing) existing.push(node);
      else byQueryId.set(node.queryId, [node]);
    }
    return byQueryId;
  }, [topology.nodes]);

  const asyncLinkByInsertQueryId = useMemo(() => {
    const links = new Map<string, typeof topology.asyncInsertLinks[number]>();
    for (const link of topology.asyncInsertLinks) links.set(link.queryId, link);
    return links;
  }, [topology.asyncInsertLinks]);

  const asyncLinkByFlushQueryId = useMemo(() => {
    const links = new Map<string, typeof topology.asyncInsertLinks[number]>();
    for (const link of topology.asyncInsertLinks) links.set(link.flushQueryId, link);
    return links;
  }, [topology.asyncInsertLinks]);

  const hostColorByIdentity = useMemo(() => {
    const hosts = [...new Set(
      topology.nodes
        .filter(node => node.role !== 'coordinator' && node.role !== 'insert_client')
        .map(node => hostIdentity(node.hostname)),
    )].sort();
    const colors = new Map<string, string>();
    hosts.forEach((host, index) => colors.set(host, stableHostColor(host, index)));
    return colors;
  }, [topology.nodes]);

  const coordinatorReaderIds = useMemo(() => {
    const leaderKeys = new Set(
      topology.nodes
        .filter(node => node.role === 'shard_leader')
        .map(node => `${node.shardNum ?? 'unknown'}:${hostIdentity(node.hostname)}:${node.queryId}`),
    );
    return new Set(
      topology.nodes
        .filter(node => node.role === 'replica_reader')
        .filter(node => leaderKeys.has(`${node.shardNum ?? 'unknown'}:${hostIdentity(node.hostname)}:${node.queryId}`))
        .map(node => node.id),
    );
  }, [topology.nodes]);

  const timeline = useMemo(() => {
    const coordStartUs = parseUs(coordinator.query_start_time_microseconds);
    const coordDurationUs = coordinator.query_duration_ms * 1000;
    const totalDurationUs = Math.max(1, coordDurationUs);
    const richChildNodes = inferredTopology
      ? topology.nodes.filter(node => node.role !== 'coordinator' && node.role !== 'insert_client')
      : [];

    const nodeQueries = richChildNodes.length > 0
      ? richChildNodes.map((node, i) => {
        const matchingSubQuery = subQueries.find(sq => sq.query_id === node.queryId && shortHost(sq.hostname) === shortHost(node.hostname));
        const nodeStartUs = parseUs(matchingSubQuery?.query_start_time_microseconds || node.queryStartTimeMicroseconds || '');
        const durationUs = node.queryDurationMs * 1000;
        const insertLink = node.role === 'insert_forwarder' ? asyncLinkByInsertQueryId.get(node.queryId) : undefined;
        const flushLink = node.role === 'async_insert_flush' ? asyncLinkByFlushQueryId.get(node.queryId) : undefined;
        const baseRoleLabel = coordinatorReaderIds.has(node.id)
          ? (node.shardNum ? `Shard ${node.shardNum} local reader` : 'Local reader')
          : roleLabel(node);
        return {
          queryId: node.queryId,
          hostname: node.hostname,
          label: shortHost(node.hostname),
          roleLabel: insertLink
            ? `${baseRoleLabel} -> ${insertLink.flushQueryId.slice(0, 8)}`
            : flushLink
              ? `${baseRoleLabel} <- ${flushLink.queryId.slice(0, 8)}`
              : baseRoleLabel,
          role: node.role,
          shardNum: node.shardNum,
          replicaNum: node.replicaNum,
          linkedQueryId: insertLink?.flushQueryId ?? flushLink?.queryId,
          color: roleColor(node, false),
          hostColor: hostColorByIdentity.get(hostIdentity(node.hostname)) ?? NODE_COLOR,
          durationMs: node.queryDurationMs,
          memoryUsage: matchingSubQuery?.memory_usage ?? 0,
          readRows: node.readRows,
          hasError: false,
          offsetUs: Math.max(0, nodeStartUs > 0 ? nodeStartUs - coordStartUs : 0),
          durationUs,
          sortIndex: i,
        };
      })
      : subQueries.map((sq, i) => {
        const nodeStartUs = parseUs(sq.query_start_time_microseconds);
        const nodeDurationUs = sq.query_duration_ms * 1000;
        const node = nodeByQueryId.get(sq.query_id)?.find(candidate => shortHost(candidate.hostname) === shortHost(sq.hostname));
        const insertLink = node?.role === 'insert_forwarder' ? asyncLinkByInsertQueryId.get(sq.query_id) : undefined;
        const flushLink = node?.role === 'async_insert_flush' ? asyncLinkByFlushQueryId.get(sq.query_id) : undefined;
        const baseRoleLabel = (nodeByQueryId.get(sq.query_id)?.length ?? 0) > 1 ? 'multi-role?' : roleLabel(node);
        return {
          queryId: sq.query_id,
          hostname: sq.hostname,
          label: shortHost(sq.hostname),
          roleLabel: insertLink
            ? `${baseRoleLabel} -> ${insertLink.flushQueryId.slice(0, 8)}`
            : flushLink
              ? `${baseRoleLabel} <- ${flushLink.queryId.slice(0, 8)}`
              : baseRoleLabel,
          role: node?.role,
          shardNum: node?.shardNum,
          replicaNum: node?.replicaNum,
          linkedQueryId: insertLink?.flushQueryId ?? flushLink?.queryId,
          color: roleColor(node, !!sq.exception_code),
          hostColor: hostColorByIdentity.get(hostIdentity(sq.hostname)) ?? NODE_COLOR,
          durationMs: sq.query_duration_ms,
          memoryUsage: sq.memory_usage,
          readRows: sq.read_rows,
          hasError: !!sq.exception_code,
          offsetUs: Math.max(0, nodeStartUs - coordStartUs),
          durationUs: nodeDurationUs,
          sortIndex: i,
        };
      });

    nodeQueries.sort((a, b) => {
      if (topology.kind === 'parallel_replicas_select') {
        return (a.shardNum ?? 9999) - (b.shardNum ?? 9999) ||
          roleOrder(a.role) - roleOrder(b.role) ||
          a.offsetUs - b.offsetUs ||
          b.durationUs - a.durationUs ||
          a.sortIndex - b.sortIndex;
      }
      if (topology.kind === 'distributed_insert') {
        return (a.shardNum ?? 9999) - (b.shardNum ?? 9999) ||
          (a.replicaNum ?? 9999) - (b.replicaNum ?? 9999) ||
          roleOrder(a.role) - roleOrder(b.role) ||
          a.offsetUs - b.offsetUs ||
          b.durationUs - a.durationUs ||
          a.sortIndex - b.sortIndex;
      }
      return a.offsetUs - b.offsetUs || b.durationUs - a.durationUs || a.sortIndex - b.sortIndex;
    });

    return { coordDurationUs, totalDurationUs, nodeQueries };
  }, [asyncLinkByFlushQueryId, asyncLinkByInsertQueryId, coordinator, coordinatorReaderIds, hostColorByIdentity, inferredTopology, nodeByQueryId, subQueries, topology.kind, topology.nodes]);

  if (isLoading) {
    return (
      <div style={{ fontSize: 11, color: 'var(--text-muted)', padding: '8px 0' }}>
        Loading topology…
      </div>
    );
  }

  if (subQueries.length === 0) return null;

  const fmtMs = formatDurationMs;
  const LABEL_W = 150;
  const METRIC_W = 70;

  const renderedChildCount = timeline.nodeQueries.length;
  const distinctNodeCount = new Set(timeline.nodeQueries.map(row => row.hostname || row.queryId)).size;
  const expectedParticipants = topology.clusterAllReplicas?.expectedParticipants;
  const localParticipants = topology.clusterAllReplicas?.localParticipantsOnInitiator ?? 0;
  const participantLabel = topology.clusterAllReplicas
    ? expectedParticipants != null
      ? ` · ${expectedParticipants} expected participant${expectedParticipants === 1 ? '' : 's'}${localParticipants > 0 ? ` (${localParticipants} local folded)` : ''}`
      : ` · all replicas targeted${localParticipants > 0 ? ` (${localParticipants} local folded)` : ''}`
    : '';
  const maxNodeQueryDuration = Math.max(...timeline.nodeQueries.map(row => row.durationMs));
  const overhead = coordinator.query_duration_ms - maxNodeQueryDuration;
  const visibleRoles = new Set(timeline.nodeQueries.map(row => row.role).filter(Boolean));
  const insertPairs = topology.kind === 'distributed_insert' && topology.asyncInsertLinks.length > 0
    ? topology.asyncInsertLinks.map((link, index) => {
      const insertNode = topology.nodes.find(node => node.queryId === link.queryId);
      const flushNode = topology.nodes.find(node => node.queryId === link.flushQueryId);
      const node = insertNode ?? flushNode;
      return {
        link,
        insertNode,
        flushNode,
        shardNum: node?.shardNum,
        replicaNum: node?.replicaNum,
        hostname: node?.hostname ?? link.hostname ?? '',
        color: node ? (hostColorByIdentity.get(hostIdentity(node.hostname)) ?? INSERT_COLOR) : INSERT_COLOR,
        sortIndex: index,
      };
    }).sort((a, b) =>
      (a.shardNum ?? 9999) - (b.shardNum ?? 9999) ||
      (a.replicaNum ?? 9999) - (b.replicaNum ?? 9999) ||
      a.sortIndex - b.sortIndex,
    )
    : [];

  return (
    <div>
      {/* Header */}
      <div style={{
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        marginBottom: 8,
      }}>
        <div style={{ fontSize: 9, color: MUTED_COLOR, textTransform: 'uppercase', letterSpacing: '1px' }}>
          Distributed Query ({renderedChildCount} remote {renderedChildCount === 1 ? 'query' : 'queries'} · {distinctNodeCount} remote node{distinctNodeCount !== 1 ? 's' : ''}{participantLabel})
        </div>
        {overhead > 0 && (
          <div style={{ fontSize: 10, color: 'var(--text-muted)' }}>
            Coordinator overhead: <span style={{ color: COORD_COLOR, fontFamily: 'var(--font-mono, monospace)' }}>{fmtMs(overhead)}</span>
          </div>
        )}
      </div>

      {/* Time axis */}
      <div style={{
        display: 'flex', justifyContent: 'space-between',
        marginLeft: LABEL_W, marginRight: METRIC_W,
        marginBottom: 3,
        fontSize: 9, color: 'var(--text-muted)', fontFamily: 'var(--font-mono, monospace)',
      }}>
        <span>0</span>
        <span>{fmtMs(coordinator.query_duration_ms / 4)}</span>
        <span>{fmtMs(coordinator.query_duration_ms / 2)}</span>
        <span>{fmtMs(coordinator.query_duration_ms * 3 / 4)}</span>
        <span>{fmtMs(coordinator.query_duration_ms)}</span>
      </div>
      <div style={{ marginLeft: LABEL_W, marginRight: METRIC_W, height: 1, background: 'var(--border-primary)', marginBottom: 6 }} />

      {insertPairs.length > 0 && (
        <div style={{
          marginLeft: LABEL_W,
          marginRight: METRIC_W,
          marginBottom: 8,
          color: MUTED_COLOR,
          fontSize: 10,
          lineHeight: 1.45,
        }}>
          Each lane pairs the remote table INSERT with the async insert flush recorded in <code style={{ fontFamily: 'var(--font-mono, monospace)' }}>system.asynchronous_insert_log</code>.
          The coordinator span before the lanes is the top-level INSERT work before those remote insert/flush query-log rows appear.
        </div>
      )}

      {/* Coordinator bar */}
      <TopologyBar
        queryId={coordinator.query_id}
        label="Coordinator"
        hostname={coordinator.hostname}
        leftPct={0}
        widthPct={100}
        color={COORD_COLOR}
        hostColor={COORD_COLOR}
        durationMs={coordinator.query_duration_ms}
        memoryUsage={coordinator.memory_usage}
        readRows={coordinator.read_rows}
        hasError={!!coordinator.exception}
        isActive={activeQueryId === coordinator.query_id}
        onClick={() => onNavigate(coordinator.query_id)}
        isCoordinator
        roleLabel={coordinatorRoleLabel(topology)}
        labelWidth={LABEL_W}
        metricWidth={METRIC_W}
      />

      {/* Separator */}
      <div style={{ marginLeft: LABEL_W, marginRight: METRIC_W, height: 1, background: 'var(--border-primary)', margin: '4px 0', opacity: 0.5 }} />

      {/* Node sub-query bars */}
      {insertPairs.length > 0 ? insertPairs.map((pair, i) => (
        <InsertPairBar
          key={`${pair.link.queryId}:${pair.link.flushQueryId}:${pair.hostname}:${i}`}
          pair={pair}
          coordinatorStartUs={parseUs(coordinator.query_start_time_microseconds)}
          totalDurationUs={timeline.totalDurationUs}
          labelWidth={LABEL_W}
          metricWidth={METRIC_W}
          activeQueryId={activeQueryId}
          onNavigate={onNavigate}
        />
      )) : timeline.nodeQueries.map((row, i) => {
        const leftPct = (row.offsetUs / timeline.totalDurationUs) * 100;
        const widthPct = Math.max(0.5, (row.durationUs / timeline.totalDurationUs) * 100);
        return (
          <TopologyBar
            key={`${row.queryId}:${row.hostname}:${i}`}
            queryId={row.queryId}
            label={row.label}
            hostname={row.hostname}
            roleLabel={row.roleLabel}
            indentLevel={row.role === 'replica_reader' ? 1 : 0}
            leftPct={leftPct}
            widthPct={widthPct}
            color={row.color}
            hostColor={row.hostColor}
            durationMs={row.durationMs}
            memoryUsage={row.memoryUsage}
            readRows={row.readRows}
            hasError={row.hasError}
            isActive={activeQueryId === row.queryId}
            onClick={() => onNavigate(row.queryId)}
            labelWidth={LABEL_W}
            metricWidth={METRIC_W}
          />
        );
      })}

      {/* Legend */}
      <div style={{ marginTop: 10, marginLeft: LABEL_W, display: 'flex', gap: 16, flexWrap: 'wrap', fontSize: 10, color: 'var(--text-muted)' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
          <div style={{ width: 8, height: 8, borderRadius: 2, background: COORD_COLOR }} />
          Coordinator
        </div>
        {visibleRoles.has('shard_leader') && (
          <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
            <div style={{ width: 8, height: 8, borderRadius: 2, background: SHARD_LEADER_COLOR }} />
            Shard coordinator
          </div>
        )}
        {visibleRoles.has('replica_reader') && (
          <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
            <div style={{ width: 8, height: 8, borderRadius: 2, background: REPLICA_READER_COLOR }} />
            Reader
          </div>
        )}
        {!visibleRoles.has('shard_leader') && !visibleRoles.has('replica_reader') && (
          <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
            <div style={{ width: 8, height: 8, borderRadius: 2, background: NODE_COLOR }} />
            Child execution
          </div>
        )}
        {visibleRoles.has('object_storage_worker') || visibleRoles.has('hybrid_segment') ? (
          <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
            <div style={{ width: 8, height: 8, borderRadius: 2, background: OBJECT_WORKER_COLOR }} />
            Storage worker
          </div>
        ) : null}
        {visibleRoles.has('insert_forwarder') || visibleRoles.has('async_insert_flush') ? (
          <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
            <div style={{ width: 8, height: 8, borderRadius: 2, background: INSERT_COLOR }} />
            Distributed table INSERT
          </div>
        ) : null}
        {subQueries.some(sq => sq.exception_code) && (
          <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
            <div style={{ width: 8, height: 8, borderRadius: 2, background: ERROR_COLOR }} />
            Error
          </div>
        )}
      </div>
    </div>
  );
};

const InsertPairBar: React.FC<{
  pair: {
    link: NonNullable<DistributedTopology['asyncInsertLinks']>[number];
    insertNode?: DistributedTopologyNode;
    flushNode?: DistributedTopologyNode;
    shardNum?: number;
    replicaNum?: number;
    hostname: string;
    color: string;
  };
  coordinatorStartUs: number;
  totalDurationUs: number;
  labelWidth: number;
  metricWidth: number;
  activeQueryId: string;
  onNavigate: (queryId: string) => void;
}> = ({ pair, coordinatorStartUs, totalDurationUs, labelWidth, metricWidth, activeQueryId, onNavigate }) => {
  const fmtMs = formatDurationMs;
  const laneLabel = pair.shardNum != null && pair.replicaNum != null
    ? `s${pair.shardNum}r${pair.replicaNum}`
    : shortHost(pair.hostname);
  const insertStartUs = parseUs(pair.insertNode?.queryStartTimeMicroseconds ?? '');
  const flushStartUs = parseUs(pair.flushNode?.queryStartTimeMicroseconds ?? '');
  const insertLeftPct = Math.max(0, ((insertStartUs - coordinatorStartUs) / totalDurationUs) * 100);
  const flushLeftPct = Math.max(0, ((flushStartUs - coordinatorStartUs) / totalDurationUs) * 100);
  const insertWidthPct = Math.max(0.5, ((pair.insertNode?.queryDurationMs ?? 0) * 1000 / totalDurationUs) * 100);
  const flushWidthPct = Math.max(0.5, ((pair.flushNode?.queryDurationMs ?? 0) * 1000 / totalDurationUs) * 100);
  const insertActive = activeQueryId === pair.link.queryId;
  const flushActive = activeQueryId === pair.link.flushQueryId;
  const tableName = [pair.link.database, pair.link.table].filter(Boolean).join('.') || pair.flushNode?.tables?.[0] || '';
  const tooltip = [
    `insert_query_id: ${pair.link.queryId}`,
    `flush_query_id: ${pair.link.flushQueryId}`,
    tableName ? `table: ${tableName}` : '',
    `host: ${pair.hostname || '-'}`,
    pair.link.rows ? `rows: ${pair.link.rows.toLocaleString()}` : '',
    pair.link.bytes ? `bytes: ${formatBytes(pair.link.bytes)}` : '',
    pair.link.status ? `status: ${pair.link.status}` : '',
  ].filter(Boolean).join('\n');

  return (
    <div
      title={tooltip}
      style={{
        display: 'flex',
        alignItems: 'center',
        minHeight: 42,
        marginBottom: 3,
        borderRadius: 3,
        background: insertActive || flushActive ? 'var(--bg-hover)' : 'transparent',
      }}
    >
      <div style={{
        width: labelWidth,
        flexShrink: 0,
        position: 'relative',
        paddingLeft: 8,
        paddingRight: 6,
        fontSize: 10,
        fontFamily: 'var(--font-mono, monospace)',
        color: pair.color,
        overflow: 'hidden',
        textOverflow: 'ellipsis',
        whiteSpace: 'nowrap',
      }}>
        <span style={{
          position: 'absolute',
          left: 0,
          top: 5,
          bottom: 5,
          width: 3,
          borderRadius: 1,
          background: pair.color,
        }} />
        <span>{laneLabel}</span>
        <span style={{ display: 'block', marginTop: 2, color: MUTED_COLOR, fontSize: 8, lineHeight: 1 }}>
          Remote table INSERT {'->'} async insert flush
        </span>
      </div>

      <div style={{ flex: 1, position: 'relative', height: 32, background: 'var(--bg-tertiary)', borderRadius: 3, overflow: 'hidden' }}>
        <button
          type="button"
          onClick={() => onNavigate(pair.link.queryId)}
          title={pair.insertNode?.queryPreview || tooltip}
          style={{
            position: 'absolute',
            left: `${insertLeftPct}%`,
            top: 3,
            width: `${Math.min(100 - insertLeftPct, insertWidthPct)}%`,
            height: 12,
            minWidth: 8,
            border: insertActive ? '1px solid #58a6ff' : 'none',
            borderRadius: 2,
            background: INSERT_COLOR,
            opacity: insertActive ? 1 : 0.62,
            cursor: 'pointer',
            padding: 0,
            color: '#fff',
            fontSize: 8,
            fontFamily: 'var(--font-mono, monospace)',
            textAlign: 'left',
            paddingLeft: 4,
            overflow: 'hidden',
            whiteSpace: 'nowrap',
          }}
        >
          insert
        </button>
        <button
          type="button"
          onClick={() => onNavigate(pair.link.flushQueryId)}
          title={pair.flushNode?.queryPreview || tooltip}
          style={{
            position: 'absolute',
            left: `${flushLeftPct}%`,
            bottom: 3,
            width: `${Math.min(100 - flushLeftPct, flushWidthPct)}%`,
            height: 12,
            minWidth: 8,
            border: flushActive ? '1px solid #58a6ff' : 'none',
            borderRadius: 2,
            background: '#f59e0b',
            opacity: flushActive ? 1 : 0.42,
            cursor: 'pointer',
            padding: 0,
            color: '#1f2937',
            fontSize: 8,
            fontFamily: 'var(--font-mono, monospace)',
            textAlign: 'left',
            paddingLeft: 4,
            overflow: 'hidden',
            whiteSpace: 'nowrap',
          }}
        >
          flush
        </button>
        <span style={{
          position: 'absolute',
          left: `${Math.min(99, Math.max(0, flushLeftPct))}%`,
          top: 2,
          bottom: 2,
          width: 1,
          background: 'rgba(245, 158, 11, 0.65)',
          opacity: 0.7,
        }} />
      </div>

      <div style={{
        width: metricWidth,
        flexShrink: 0,
        textAlign: 'right',
        fontSize: 9,
        fontFamily: 'var(--font-mono, monospace)',
        color: 'var(--text-muted)',
        paddingLeft: 6,
        lineHeight: 1.35,
      }}>
        <div>{pair.insertNode ? fmtMs(pair.insertNode.queryDurationMs) : '-'}</div>
        <div>{pair.flushNode ? fmtMs(pair.flushNode.queryDurationMs) : '-'}</div>
      </div>
    </div>
  );
};

/** Single bar in the topology Gantt */
const TopologyBar: React.FC<{
  queryId: string;
  label: string;
  hostname?: string;
  leftPct: number;
  widthPct: number;
  color: string;
  hostColor: string;
  durationMs: number;
  memoryUsage: number;
  readRows: number;
  hasError: boolean;
  isActive: boolean;
  isCoordinator?: boolean;
  roleLabel: string;
  indentLevel?: number;
  onClick: () => void;
  labelWidth: number;
  metricWidth: number;
}> = ({
  queryId, label, hostname, leftPct, widthPct, color, durationMs, memoryUsage, readRows,
  hasError, isActive, isCoordinator, roleLabel, indentLevel = 0, onClick, labelWidth, metricWidth, hostColor,
}) => {
  const fmtMs = formatDurationMs;
  const tooltip = [
    `query_id: ${queryId}`,
    `role: ${roleLabel}`,
    `host: ${hostname || label}`,
    `duration: ${fmtMs(durationMs)}`,
    `memory: ${formatBytes(memoryUsage)}`,
    `rows: ${fmtCompact(readRows)}`,
  ].join('\n');

  return (
    <div
      onClick={onClick}
      title={tooltip}
      style={{
        display: 'flex', alignItems: 'center', minHeight: 30, marginBottom: 2,
        cursor: 'pointer',
        borderRadius: 3,
        transition: 'background 0.1s',
        background: isActive ? 'var(--bg-hover)' : 'transparent',
      }}
      onMouseEnter={e => { e.currentTarget.style.background = 'var(--bg-hover)'; }}
      onMouseLeave={e => { e.currentTarget.style.background = isActive ? 'var(--bg-hover)' : 'transparent'; }}
    >
      {/* Label */}
      <div style={{
        width: labelWidth, flexShrink: 0,
        position: 'relative',
        fontSize: 10, fontFamily: 'var(--font-mono, monospace)',
        color: isActive ? (isCoordinator ? COORD_COLOR : hostColor) : 'var(--text-muted)',
        overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
        paddingRight: 6,
        fontWeight: isActive || isCoordinator ? 600 : 400,
        paddingLeft: isCoordinator ? 0 : 8 + indentLevel * 14,
      }}>
        {!isCoordinator && indentLevel > 0 && (
          <>
            <span style={{
              position: 'absolute',
              left: 1,
              top: 0,
              bottom: 0,
              width: 1,
              background: 'var(--border-primary)',
            }} />
            <span style={{
              position: 'absolute',
              left: 1,
              top: 11,
              width: indentLevel * 14 - 2,
              height: 1,
              background: 'var(--border-primary)',
            }} />
          </>
        )}
        {!isCoordinator && (
          <span style={{
            position: 'absolute',
            left: indentLevel * 14,
            top: 3,
            bottom: 3,
            width: isActive ? 4 : 3,
            borderRadius: 1,
            background: hostColor,
          }} />
        )}
        <span title={hostname || label} style={{ color: isCoordinator ? COORD_COLOR : hostColor }}>{label}</span>
        <span style={{
          display: 'block',
          marginTop: 1,
          fontSize: 8,
          color: isCoordinator ? COORD_COLOR : MUTED_COLOR,
          fontFamily: 'var(--font-mono, monospace)',
          lineHeight: 1,
        }}>
          {roleLabel}
        </span>
      </div>

      {/* Bar track */}
      <div style={{
        flex: 1, position: 'relative', height: 16,
        background: 'var(--bg-tertiary)', borderRadius: 3, overflow: 'hidden',
      }}>
        <div
          title={tooltip}
          style={{
            position: 'absolute',
            left: `${leftPct}%`,
            width: `${widthPct}%`,
            height: '100%',
            background: color,
            borderRadius: 2,
            opacity: isActive ? 1 : 0.38,
            border: 'none',
            boxShadow: isActive
              ? `0 0 0 2px ${isCoordinator ? 'rgba(88, 166, 255, 0.18)' : 'rgba(88, 166, 255, 0.22)'}, 0 0 12px rgba(88, 166, 255, 0.18)`
              : 'none',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'flex-start',
            paddingLeft: 4,
            paddingRight: 4,
            boxSizing: 'border-box',
            minWidth: 0,
          }}
        >
          {/* Inline metrics on the bar */}
          {widthPct > 12 && (
            <span style={{
              fontSize: 9, color: '#fff', fontFamily: 'var(--font-mono, monospace)',
              whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis',
              textShadow: '0 1px 2px rgba(0,0,0,0.5)',
              fontWeight: 500,
            }}>
              {fmtMs(durationMs)}
              {widthPct > 25 && ` · ${formatBytes(memoryUsage)}`}
              {widthPct > 40 && ` · ${fmtCompact(readRows)} rows`}
            </span>
          )}
        </div>
      </div>

      {/* Right-side duration (always visible, even for tiny bars) */}
      <div style={{
        width: metricWidth, flexShrink: 0, textAlign: 'right',
        fontSize: 10, fontFamily: 'var(--font-mono, monospace)',
        color: hasError ? ERROR_COLOR : 'var(--text-muted)',
        paddingLeft: 6,
      }}>
        {hasError ? '✗ ' : ''}{fmtMs(durationMs)}
      </div>
    </div>
  );
};

export default DistributedQueryTopology;
