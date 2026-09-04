import type {
  DistributedExecutionFlowEventKind,
  DistributedTopologyNode,
  SubQueryInfo,
  TopologyNodeRole,
} from '@tracehouse/core';

/**
 * Hover motion shared by the two topology views, so a cube rising under the
 * pointer and a bar row lighting up feel like the same gesture.
 */
export const HOVER_TRANSITION = '180ms cubic-bezier(0.22, 0.61, 0.36, 1)';

export const COORD_COLOR = '#58a6ff';
export const NODE_COLOR = '#d29922';
export const SHARD_LEADER_COLOR = '#a371f7';
export const NESTED_COORDINATOR_COLOR = '#8b5cf6';
export const REPLICA_READER_COLOR = '#d29922';
export const OBJECT_WORKER_COLOR = '#3fb950';
export const INSERT_COLOR = '#db6d28';
export const ERROR_COLOR = '#f85149';

/** Role palette shared by the timeline and flow renderings, so both agree on a color. */
export function topologyRoleColor(
  role: TopologyNodeRole | 'local_reader' | undefined,
  hasError = false,
): string {
  if (hasError) return ERROR_COLOR;
  if (role === 'coordinator') return COORD_COLOR;
  if (role === 'shard_leader') return SHARD_LEADER_COLOR;
  if (role === 'nested_coordinator') return NESTED_COORDINATOR_COLOR;
  if (role === 'replica_reader' || role === 'local_reader') return REPLICA_READER_COLOR;
  if (role === 'object_storage_worker' || role === 'hybrid_segment') return OBJECT_WORKER_COLOR;
  if (role === 'insert_forwarder' || role === 'async_insert_flush') return INSERT_COLOR;
  return NODE_COLOR;
}

/**
 * Hue per shard. Replicas of one shard are shades of its hue, so "same colour"
 * reads as "same data" and a skewed shard is visible without reading a label.
 * The coordinator keeps COORD_COLOR: it is a role, not a shard.
 */
const SHARD_COLORS = ['#d29922', '#3fb950', '#a371f7', '#2bb5b5', '#db61a2', '#db6d28'];

function clampChannel(value: number): number {
  return Math.max(0, Math.min(255, Math.round(value)));
}

/** Lighten (amount > 0) or darken (amount < 0) a #rrggbb colour, returning #rrggbb. */
export function shadeColor(hex: string, amount: number): string {
  const value = parseInt(hex.slice(1), 16);
  const channels = [(value >> 16) & 0xff, (value >> 8) & 0xff, value & 0xff];
  const mixed = channels.map(channel => clampChannel(
    amount >= 0 ? channel + (255 - channel) * amount : channel * (1 + amount),
  ));
  return `#${mixed.map(channel => channel.toString(16).padStart(2, '0')).join('')}`;
}

/**
 * How far each replica's colour is lightened from its shard's hue, indexed by
 * replica number. The steps open up quickly and then tighten: telling r1 from
 * r2 is the case that comes up, and a ramp that keeps its early pace ends up
 * washed out against the canvas. Replicas past the table reuse the last step.
 */
const REPLICA_SHADES = [0, 0.3, 0.48, 0.6, 0.7];

/**
 * Colour for a participant: its shard's hue, lightened per replica. Falls back
 * to the role palette when the topology could not attribute a shard.
 */
export function participantColor(
  role: TopologyNodeRole | 'local_reader' | undefined,
  shardNum: number | undefined,
  replicaNum: number | undefined,
  hasError = false,
): string {
  if (hasError) return ERROR_COLOR;
  if (role === 'coordinator') return COORD_COLOR;
  if (shardNum == null) return topologyRoleColor(role, false);
  const base = SHARD_COLORS[(shardNum - 1) % SHARD_COLORS.length];
  const shade = replicaNum != null && replicaNum > 1
    ? REPLICA_SHADES[Math.min(replicaNum - 1, REPLICA_SHADES.length - 1)]
    : 0;
  // Always #rrggbb, including for shaded replicas: callers derive cube faces
  // from this and should not have to handle two colour syntaxes.
  return shade !== 0 ? shadeColor(base, shade) : base;
}

/** The hue a shard is drawn in, for legends. */
export function shardColor(shardNum: number): string {
  return SHARD_COLORS[(shardNum - 1) % SHARD_COLORS.length];
}

const REMOTE_EXECUTION_NOUN: Partial<Record<TopologyNodeRole, string>> = {
  insert_forwarder: 'Remote table INSERT',
  async_insert_flush: 'Async insert flush',
  shard_leader: 'Shard coordinator',
  nested_coordinator: 'Nested coordinator',
  replica_reader: 'Reader query',
};

const EVENT_TITLE: Partial<Record<DistributedExecutionFlowEventKind, string>> = {
  coordinator_started: 'Coordinator accepted query',
  async_insert_buffered: 'Remote INSERT buffered for async flush',
  local_read_started: 'Local read started',
  local_read_completed: 'Local read folded into coordinator',
  coordinator_merge: 'Coordinator merged remote results',
  coordinator_output: 'Coordinator produced output',
  coordinator_read_completed: 'Coordinator completed query',
};

function remoteExecutionNoun(role?: TopologyNodeRole): string {
  return role ? REMOTE_EXECUTION_NOUN[role] ?? 'Remote query' : 'Remote query';
}

export function isWritePathRole(role?: TopologyNodeRole): boolean {
  return role === 'insert_forwarder' || role === 'async_insert_flush';
}

export function topologyNodeWorkRows(node?: DistributedTopologyNode, subQuery?: SubQueryInfo): number {
  if (!node) return subQuery?.read_rows ?? 0;
  return isWritePathRole(node.role) ? Math.max(node.writtenRows, node.readRows) : node.readRows;
}

export function topologyNodeWorkBytes(node?: DistributedTopologyNode, subQuery?: SubQueryInfo): number {
  if (!node) return subQuery?.read_bytes ?? 0;
  return isWritePathRole(node.role) ? Math.max(node.writtenBytes, node.readBytes) : node.readBytes;
}

export function distributedFlowEventTitle(
  eventKind: DistributedExecutionFlowEventKind,
  role?: TopologyNodeRole,
  hostname?: string,
): string {
  if (eventKind === 'coordinator_started' && hostname) return `${EVENT_TITLE.coordinator_started} on ${hostname}`;
  if (eventKind === 'remote_started') return `${remoteExecutionNoun(role)} started`;
  if (eventKind === 'remote_read_completed') {
    return `${remoteExecutionNoun(role)} completed${hostname ? ` on ${hostname}` : ''}`;
  }
  return EVENT_TITLE[eventKind] ?? 'Remote query';
}

export function distributedRemoteEventPrefix(
  eventKind: 'remote_started' | 'remote_read_completed',
  role?: TopologyNodeRole,
): string {
  return `${remoteExecutionNoun(role)} ${eventKind === 'remote_started' ? 'started' : 'completed'} on `;
}
