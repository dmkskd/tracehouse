import type {
  DistributedExecutionFlowEventKind,
  DistributedTopologyNode,
  SubQueryInfo,
  TopologyNodeRole,
} from '@tracehouse/core';

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
