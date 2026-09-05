/**
 * Every user-facing label for a distributed-topology concept, in one table.
 *
 * These labels were written in five places: three label functions in
 * distributed-query-topology.ts, a second copy of the role nouns in the
 * frontend's presentation module, event titles inline in the flow builder, a
 * second copy of those in the frontend, and literals typed into components.
 * The copies drifted - one role was "Reader" in core and "Reader query" in the
 * frontend - and renaming anything meant finding all five and missing some.
 *
 * They all read from here now, and topology-labels.guard.test.ts fails the
 * build if a sixth place appears. The keys are the stable API; the strings are
 * the only place a label is written, so renaming is an edit to this file.
 *
 * The wording follows ClickHouse's own, which several of these did not:
 *
 *   initiator      the node that received a query and hands out work. The
 *                  architecture overview calls the top one "the node that
 *                  received the query (initiator node)", and ProfileEvents.cpp
 *                  uses the same word one level down, for the node a shard's
 *                  replicas send to: ParallelReplicasNumRequests is "Number of
 *                  requests to the initiator". So a shard's leader is a shard
 *                  initiator, not a coordinator - ClickHouse has exactly one
 *                  coordinator per query and it is the initiator itself.
 *   replica        a node reading part of a shard. ParallelReplicasReadMarks
 *                  is "How many marks were read by the given replica". Not
 *                  "reader": in ClickHouse a reader is IMergeTreeReader, a
 *                  layer below, reading columns off parts.
 *   remote node    a node running a fragment for the initiator, per the
 *                  architecture overview. Used where we know a participant ran
 *                  elsewhere but not what it did, which is the classifier's
 *                  fallback.
 *   child query    one non-initial query_log row. system.query_log defines
 *                  is_initial_query = 0 as "a child query initiated by another
 *                  query, including queries for distributed execution". A row,
 *                  not a node; the node it ran on is a remote node.
 *   master/worker  threads inside one server, a separate vocabulary from
 *                  nodes. query_thread_log has master_thread_id, "OS initial
 *                  ID of initial thread", and workload scheduling defines
 *                  MASTER THREAD and WORKER THREAD.
 */

import type {
  DistributedExecutionFlowEventKind,
  DistributedQueryKind,
  TopologyNodeRole,
} from './distributed-query-topology.js';

/** A folded local read has no query_log row of its own, so it is not a role in
 * the detector's enum, but it is a participant the UI has to name. */
export type LabelledTopologyRole = TopologyNodeRole | 'local_reader';

export interface TopologyRoleLabels {
  /** Sentence-initial form, e.g. beside a cube or at the head of a row. */
  title: string;
  /** Mid-sentence form, e.g. "folded into the shard coordinator". */
  noun: string;
  /**
   * Qualified by the shard the participant ran for. Only the roles that belong
   * to one shard have it; a coordinator serves the whole query.
   */
  perShard?: (shardNum: number) => string;
  /**
   * How the flow view names a remote execution of this role, which reads as an
   * event ("Reader query started") rather than as a participant. Absent for
   * roles the flow view never draws an event for.
   */
  remoteExecution?: string;
}

/**
 * Labels per participant role.
 *
 * `title` and `noun` are the two grammatical positions these appear in, and
 * both are spelled out rather than derived by lower-casing: "Remote table
 * INSERT" does not lower-case to "remote table insert".
 */
export const TOPOLOGY_ROLE_LABELS: Record<LabelledTopologyRole, TopologyRoleLabels> = {
  coordinator: {
    title: 'Initiator',
    noun: 'initiator',
  },
  shard_leader: {
    title: 'Shard initiator',
    noun: 'shard initiator',
    perShard: shardNum => `Shard ${shardNum} initiator`,
    remoteExecution: 'Shard initiator',
  },
  nested_coordinator: {
    title: 'Nested initiator',
    noun: 'nested initiator',
    remoteExecution: 'Nested initiator',
  },
  replica_reader: {
    title: 'Replica',
    noun: 'replica',
    perShard: shardNum => `Shard ${shardNum} replica`,
    remoteExecution: 'Replica query',
  },
  remote_child: {
    title: 'Remote node',
    noun: 'remote node',
    perShard: shardNum => `Shard ${shardNum} remote node`,
  },
  independent_child: {
    title: 'Independent child',
    noun: 'independent child',
  },
  object_storage_worker: {
    title: 'Object worker',
    noun: 'object worker',
  },
  hybrid_segment: {
    title: 'Hybrid segment',
    noun: 'hybrid segment',
  },
  insert_client: {
    title: 'Insert client',
    noun: 'insert client',
  },
  insert_forwarder: {
    title: 'Remote table INSERT',
    noun: 'remote table INSERT',
    remoteExecution: 'Remote table INSERT',
  },
  async_insert_flush: {
    title: 'Async insert flush',
    noun: 'async insert flush',
    remoteExecution: 'Async insert flush',
  },
  local_reader: {
    title: 'Local replica',
    noun: 'local replica',
  },
  unknown: {
    title: 'Unknown',
    noun: 'unknown',
  },
};

/** What the flow view calls a remote execution whose role it could not name. */
export const UNNAMED_REMOTE_EXECUTION = 'Remote query';

export interface TopologyEventLabels {
  /**
   * The timeline's wording, written when the event is built.
   */
  title: string;
  /**
   * The flow view's wording for the same event, where it differs from the
   * timeline's. The two views phrase several events differently and always
   * have; the difference is preserved here rather than resolved, because
   * choosing one is a wording decision and not a naming one.
   */
  headline?: string;
  /** Fixed explanatory line, where the event has one that is not computed. */
  detail?: string;
}

export const TOPOLOGY_EVENT_LABELS = {
  coordinator_started: {
    title: 'Initiator started',
    headline: 'Initiator accepted query',
    detail: 'Top-level query accepted by the initiator.',
  },
  async_insert_buffered: {
    title: 'Async insert buffered',
    headline: 'Remote INSERT buffered for async flush',
  },
  local_read_started: {
    title: 'Local read folded into initiator',
    headline: 'Local read started',
  },
  local_read_completed: {
    title: 'Local read accounted in initiator row',
    headline: 'Local read folded into initiator',
    detail: 'Local participant work is folded into initiator metrics.',
  },
  remote_started: {
    title: 'Remote read started',
  },
  remote_setup: {
    title: 'Remote setup',
  },
  remote_read_completed: {
    title: 'Remote read completed',
    detail: 'Remote node completed.',
  },
  coordinator_merge: {
    title: 'Initiator merge',
    headline: 'Initiator merged remote results',
  },
  coordinator_output: {
    title: 'Result output',
    headline: 'Initiator produced output',
  },
  coordinator_read_completed: {
    title: 'Initiator read completed',
    headline: 'Initiator completed query',
  },
  // `satisfies` rather than an annotation: every key is still checked against
  // TopologyEventLabels, but each entry keeps its literal type, so a caller
  // reading a `detail` that is present here gets a string rather than
  // `string | undefined` and does not need a fallback for a case that cannot
  // happen.
} as const satisfies Record<DistributedExecutionFlowEventKind, TopologyEventLabels>;

/**
 * Names in the actor column of the timeline. A participant's actor name is its
 * shard and replica coordinate, so only the actors that are not participants
 * are spelled here.
 */
export const TOPOLOGY_ACTOR_LABELS = {
  coordinator: 'Initiator',
  localReader: 'Local replica',
  asyncInsert: 'Async insert',
  /** Stands in when a phase names a host we could not resolve. */
  unknownRemote: 'remote',
} as const;

/**
 * What a single query_log row is called, from `is_initial_query`.
 *
 * This is a property of the row, not of the node it ran on: the same host can
 * be an initial query in one row and a child in another, which is why the
 * child form is "child query" and not "remote node".
 */
export const QUERY_ROW_LABELS = {
  initial: { title: 'Initiator', noun: 'initiator' },
  child: { title: 'Child query', noun: 'child query' },
} as const;

/**
 * Threads inside one server, which are a different vocabulary from nodes: the
 * trace views talk about the thread that accepted the connection and the
 * threads it forked, not about hosts.
 */
export const THREAD_LABELS = {
  master: { title: 'Master', thread: 'Master thread', start: 'Master thread start' },
  worker: { title: 'Worker', thread: 'Worker thread', start: 'Worker thread start' },
} as const;

/** How a whole distributed query is described, by the shape we detected. */
export const QUERY_KIND_LABELS: Record<DistributedQueryKind, string> = {
  local: 'Local',
  plain_distributed_select: 'Distributed SELECT',
  parallel_replicas_select: 'Parallel replicas',
  cluster_all_replicas: 'All replicas fan-out',
  object_storage_swarm_select: 'Object storage swarm',
  hybrid_storage_select: 'Hybrid storage',
  distributed_insert: 'Distributed INSERT',
  unknown_distributed: 'Distributed',
};

export const UNKNOWN_QUERY_KIND = 'Unknown';

/** Title-case name for a role, e.g. beside a cube. */
export function topologyRoleTitle(role: LabelledTopologyRole): string {
  return TOPOLOGY_ROLE_LABELS[role]?.title ?? TOPOLOGY_ROLE_LABELS.unknown.title;
}

/** Mid-sentence name for a role. */
export function topologyRoleNoun(role: LabelledTopologyRole): string {
  return TOPOLOGY_ROLE_LABELS[role]?.noun ?? role.replace(/_/g, ' ');
}

/**
 * Role name qualified by the shard it ran for, e.g. "Shard 2 reader". Falls
 * back to the unqualified title for roles that do not belong to one shard, and
 * when the topology could not attribute a shard.
 */
export function topologyRoleTitleForShard(role: LabelledTopologyRole, shardNum?: number): string {
  const words = TOPOLOGY_ROLE_LABELS[role];
  if (shardNum && words?.perShard) return words.perShard(shardNum);
  return topologyRoleTitle(role);
}

/** What the flow view calls a remote execution of this role. */
export function remoteExecutionNoun(role?: LabelledTopologyRole): string {
  return (role && TOPOLOGY_ROLE_LABELS[role]?.remoteExecution) ?? UNNAMED_REMOTE_EXECUTION;
}

/**
 * Widens one entry back to the interface. The table keeps literal types so
 * callers reading a specific `detail` do not have to handle an absent one, but
 * a lookup by a variable kind needs the common shape, where `headline` and
 * `detail` are optional.
 */
function eventLabels(kind: DistributedExecutionFlowEventKind): TopologyEventLabels | undefined {
  return TOPOLOGY_EVENT_LABELS[kind];
}

/** The timeline's title for a flow event. */
export function topologyEventTitle(kind: DistributedExecutionFlowEventKind): string {
  return eventLabels(kind)?.title ?? UNNAMED_REMOTE_EXECUTION;
}

/** The flow view's title for a flow event, which is the timeline's when they agree. */
export function topologyEventHeadline(kind: DistributedExecutionFlowEventKind): string {
  const labels = eventLabels(kind);
  return labels?.headline ?? labels?.title ?? UNNAMED_REMOTE_EXECUTION;
}

/**
 * A participant's place in the cluster, `s2r1`, from the shard and replica
 * numbers system.clusters attributed to it.
 *
 * This is the name a participant goes by across the whole Query Detail. A
 * hostname cannot do the job: it is a container id or a cloud hash as often as
 * it is a name, it does not say which replica did the work, and one host can
 * be several participants in one query under different roles.
 *
 * Undefined when the topology could not place the host, which is the caller's
 * cue to fall back to a hostname and to show that it is a fallback. Both
 * numbers come from one system.clusters lookup, so they are either both known
 * or both absent; there is no half-placed case to handle.
 */
export function participantCoordinate(shardNum?: number, replicaNum?: number): string | undefined {
  if (shardNum == null || replicaNum == null) return undefined;
  return `s${shardNum}r${replicaNum}`;
}

/**
 * How the read-distribution table names the group holding the initiator's own
 * local read, which has no query_log row of its own to take a shape from.
 */
export const FOLDED_LOCAL_READ_GROUP_LABEL = 'Initiator local read';

/**
 * The initiator's own local read, as the timeline names it.
 *
 * Qualified by shard where we know which one it read for. Kept apart from
 * `perShard` because no core label function produces this form; it is the
 * timeline's own phrasing, and folding it into the role table's shard
 * qualifier would change what `distributedNodeRoleLabel` returns.
 */
export function localReadTitle(shardNum?: number): string {
  return shardNum
    ? `Shard ${shardNum} local replica`
    : TOPOLOGY_ROLE_LABELS.local_reader.title;
}

/** Name for one query_log row from its `is_initial_query` flag. */
export function queryRowRoleTitle(isInitialQuery: boolean): string {
  return isInitialQuery ? QUERY_ROW_LABELS.initial.title : QUERY_ROW_LABELS.child.title;
}

/** Lower-case name for one query_log row, for fields that render in lower case. */
export function queryRowRoleNoun(isInitialQuery: boolean): string {
  return isInitialQuery ? QUERY_ROW_LABELS.initial.noun : QUERY_ROW_LABELS.child.noun;
}

/** How a whole distributed query is described. */
export function queryKindTitle(kind?: DistributedQueryKind): string {
  return kind ? QUERY_KIND_LABELS[kind] ?? UNKNOWN_QUERY_KIND : UNKNOWN_QUERY_KIND;
}
