/**
 * distributedFlowLabels — what each participant and each edge is called, and
 * which of its numbers are shown where.
 *
 * This is the vocabulary of the distributed views, kept out of the components
 * that paint them. Every decision here is about meaning rather than pixels:
 * whether a cube leads with its job or its coordinate, whether a figure is
 * what a node read or what it returned, and in which order the two views name
 * the same three metrics. Both the flow diagram and the timeline read from it,
 * so the two panels cannot drift apart by being edited separately.
 */

import { distributedNodeRoleLabel, participantCoordinate } from '@tracehouse/core';
import type { DistributedFlowEdge, DistributedFlowNode } from './distributedFlowLayout';
import { formatBytes } from '../../../../stores/databaseStore';
import { formatDurationMs, formatRowCount } from '../../../../utils/formatters';

/**
 * A participant that hands work out rather than doing the reading itself.
 *
 * These lead with their job and carry their coordinate underneath; everyone
 * else leads with the coordinate. The two kinds are told apart by different
 * facts, which is the whole reason the lines swap. There is one initiator per
 * query and one per shard, so naming the job identifies the cube: "Shard 1
 * initiator" is unique in the diagram. There are many replicas and remote
 * nodes per shard, so their job is the one thing they all share and only the
 * coordinate separates them. Leading each with the line that distinguishes it
 * keeps every heading unique down the column.
 */
export function isDispatcher(node: DistributedFlowNode): boolean {
  return node.isCoordinator || node.role === 'shard_leader' || node.role === 'nested_coordinator';
}

/**
 * What to call this participant on its second line, and whether that name
 * places it in the cluster or merely identifies the machine.
 *
 * The coordinate comes from system.clusters by way of the topology, not from
 * parsing the hostname. Hostnames are container ids and cloud hashes as often
 * as they are names, so a regex over them answers "s1r2" for some deployments
 * and a twelve-character hash for others, from data that was equally available
 * in both cases. The read-distribution table below the diagram has always used
 * the coordinate; this is the same rule, so the two panels agree.
 *
 * `placed` is false when the topology could not attribute a shard and replica.
 * The name shown then is the machine's, and the caller renders it differently:
 * "we could not place this host" and "this host is called s1r2" should not look
 * like the same statement.
 */
export function nodeIdentity(node: DistributedFlowNode): { label: string; placed: boolean } {
  const coordinate = participantCoordinate(node.shardNum, node.replicaNum);
  return coordinate ? { label: coordinate, placed: true } : { label: node.hostLabel, placed: false };
}

export function nodeRoleLabel(node: DistributedFlowNode): string {
  if (node.isFolded) {
    return node.shardNum != null && node.replicaNum != null
      ? `Local read · s${node.shardNum}r${node.replicaNum}`
      : 'Local read';
  }
  return distributedNodeRoleLabel(node.role, node.shardNum);
}

/**
 * The order the three metrics are named in, everywhere they appear: beside a
 * cube, on the stripes painted across it, and in either view's hover panel.
 *
 * One order, stated once. The stripes cannot carry their own captions at rest,
 * so the nth line of text is what tells a reader which bar is which; that only
 * works while every surface agrees, and agreement by convention across three
 * files is agreement until someone edits one of them.
 */
export const FLOW_METRIC_ORDER = ['duration', 'memory', 'rows'] as const;

/**
 * Metrics beside a cube, one per line, in FLOW_METRIC_ORDER.
 *
 * Metrics a participant never reported are dropped rather than shown as zero:
 * a folded local read has no duration of its own, and "0ms" would be a claim
 * about its speed rather than an absence of data.
 */
export function nodeDetails(node: DistributedFlowNode): string[] {
  const details: string[] = [];
  if (node.metrics.durationMs > 0) details.push(formatDurationMs(node.metrics.durationMs));
  if (node.metrics.memoryUsage > 0) details.push(formatBytes(node.metrics.memoryUsage));
  details.push(node.metrics.readRows > 0 ? formatRowCount(node.metrics.readRows) : 'no rows read');
  return details;
}

/**
 * What travelled back up this link: the child's result bytes and result rows,
 * bytes first so the edge reads in the same order as the cube labels.
 *
 * This is not the child's read volume, which is what the cube beside it shows.
 * The two differ by whatever the child aggregated away, and on a GROUP BY they
 * differ by orders of magnitude — which is the fact this view exists to show.
 */
export function edgeLabel(edge: DistributedFlowEdge): string {
  if (edge.returnedRows == null && edge.returnedBytes == null) return '';
  const parts: string[] = [];
  if (edge.returnedBytes != null) parts.push(formatBytes(edge.returnedBytes));
  if (edge.returnedRows != null) parts.push(formatRowCount(edge.returnedRows));
  return parts.join(' · ');
}

export interface MetricFact {
  label: string;
  value: string;
}

/**
 * The metric rows of a hover panel: one value per row, each named, in
 * FLOW_METRIC_ORDER, with whatever else the caller's view knows about the
 * participant appended.
 *
 * One row per value rather than "437ms · 1.41 MB peak" on a line called
 * "cost": a pair sharing a row saves height and spends a label, leaving the
 * reader to work out which half is which.
 *
 * The timeline knows only the three basics, the flow view also has the read
 * and result byte counters, so everything past the three is optional and
 * omitted when absent — an unknown counter is left out, never printed as 0.
 */
export function nodeMetricFacts(metrics: {
  durationMs: number;
  memoryUsage: number;
  readRows: number;
  readBytes?: number;
  resultRows?: number;
  resultBytes?: number;
  selectedParts?: number;
}, options: { formatRows?: (rows: number) => string } = {}): MetricFact[] {
  const rows = options.formatRows ?? ((value: number) => value.toLocaleString());
  return [
    { label: 'duration', value: formatDurationMs(metrics.durationMs) },
    { label: 'memory', value: `${formatBytes(metrics.memoryUsage)} peak` },
    { label: 'rows read', value: rows(metrics.readRows) },
    ...(metrics.readBytes != null ? [{ label: 'bytes read', value: formatBytes(metrics.readBytes) }] : []),
    // What went back up the edge, kept next to the read figures so the drop
    // from one to the other is read as a pair.
    ...(metrics.resultRows ? [{ label: 'rows returned', value: rows(metrics.resultRows) }] : []),
    ...(metrics.resultBytes ? [{ label: 'bytes returned', value: formatBytes(metrics.resultBytes) }] : []),
    ...(metrics.selectedParts ? [{ label: 'parts', value: metrics.selectedParts.toLocaleString() }] : []),
  ];
}
