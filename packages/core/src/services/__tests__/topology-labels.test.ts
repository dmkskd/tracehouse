import { describe, expect, it } from 'vitest';
import {
  distributedNodeRoleLabel,
  distributedQueryKindLabel,
  topologyNodeRoleLabel,
  topologyNodeRoleText,
} from '../distributed-query-topology.js';
import {
  QUERY_ROW_LABELS,
  THREAD_LABELS,
  TOPOLOGY_ACTOR_LABELS,
  TOPOLOGY_ROLE_LABELS,
  localReadTitle,
  queryRowRoleNoun,
  queryRowRoleTitle,
  remoteExecutionNoun,
  topologyRoleNoun,
  topologyRoleTitle,
  topologyRoleTitleForShard,
  type LabelledTopologyRole,
} from '../topology-labels.js';

/**
 * These spell out the labels rather than deriving them, on purpose. Every
 * caller now reads from the table, so a test that compared the table against
 * the label functions would be comparing the table against itself and would
 * pass however the words changed.
 *
 * Written out, they are the record of what the UI says. Changing a label here
 * is a line of test to update, which is the point at which someone notices
 * they are changing what a user reads.
 */

describe('topology role labels', () => {
  it('names the node that received the query the initiator', () => {
    // ClickHouse's own word for it, in the architecture overview and in the
    // ParallelReplicas ProfileEvent descriptions.
    expect(topologyRoleTitle('coordinator')).toBe('Initiator');
    expect(topologyRoleNoun('coordinator')).toBe('initiator');
  });

  it('names a shard leader a shard initiator, not a coordinator', () => {
    // ClickHouse has one coordinator per query and it is the initiator itself,
    // so "shard coordinator" would be a second meaning for a taken word.
    // ParallelReplicasNumRequests, "Number of requests to the initiator", is
    // counted against exactly this node.
    expect(topologyRoleTitle('shard_leader')).toBe('Shard initiator');
    expect(topologyRoleTitleForShard('shard_leader', 2)).toBe('Shard 2 initiator');
    expect(topologyRoleTitle('nested_coordinator')).toBe('Nested initiator');
  });

  it('names a node reading part of a shard a replica, not a reader', () => {
    // ParallelReplicasReadMarks: "How many marks were read by the given
    // replica". A "reader" in ClickHouse is IMergeTreeReader, one layer down.
    expect(topologyRoleTitle('replica_reader')).toBe('Replica');
    expect(topologyRoleTitleForShard('replica_reader', 3)).toBe('Shard 3 replica');
    expect(topologyRoleTitle('local_reader')).toBe('Local replica');
    expect(localReadTitle(1)).toBe('Shard 1 local replica');
    expect(localReadTitle()).toBe('Local replica');
  });

  it('names an unclassified participant a remote node', () => {
    // The classifier's fallback: we know it ran elsewhere for the initiator,
    // and nothing more. "Remote node" is the architecture overview's word.
    expect(topologyRoleTitle('remote_child')).toBe('Remote node');
    expect(topologyRoleTitleForShard('remote_child', 1)).toBe('Shard 1 remote node');
  });

  it('leaves the write path and storage roles alone', () => {
    // Out of scope for the rename: no ClickHouse wording was established for
    // these, so inventing one would be the mistake this change is undoing.
    expect(topologyRoleTitle('insert_forwarder')).toBe('Remote table INSERT');
    expect(topologyRoleTitle('async_insert_flush')).toBe('Async insert flush');
    expect(topologyRoleTitle('object_storage_worker')).toBe('Object worker');
    expect(topologyRoleTitle('hybrid_segment')).toBe('Hybrid segment');
    expect(topologyRoleTitle('insert_client')).toBe('Insert client');
  });

  it('falls back for a role it cannot name', () => {
    expect(topologyRoleTitle('unknown')).toBe('Unknown');
    expect(remoteExecutionNoun(undefined)).toBe('Remote query');
    expect(remoteExecutionNoun('object_storage_worker')).toBe('Remote query');
  });

  it('drops the shard qualifier for roles that do not belong to one shard', () => {
    expect(topologyRoleTitleForShard('coordinator', 2)).toBe('Initiator');
    expect(topologyRoleTitleForShard('replica_reader', 0)).toBe('Replica');
    expect(topologyRoleTitleForShard('replica_reader')).toBe('Replica');
  });

  it('covers every role the detector can assign', () => {
    // A missing role would fall back to "Unknown" rather than failing.
    const roles = Object.keys(TOPOLOGY_ROLE_LABELS) as LabelledTopologyRole[];
    expect(roles).toHaveLength(13);
    expect(roles).toContain('local_reader');
  });
});

describe('query_log row labels', () => {
  it('distinguishes a row from the node that ran it', () => {
    // is_initial_query describes the row. The node is named by its role, which
    // is why the child form is "child query" and not "remote node".
    expect(queryRowRoleTitle(true)).toBe('Initiator');
    expect(queryRowRoleTitle(false)).toBe('Child query');
    expect(queryRowRoleNoun(false)).toBe('child query');
  });

  it('no longer says "worker" or "node sub-query" for a child row', () => {
    // Both were ours; system.query_log calls it a child query.
    expect(Object.values(QUERY_ROW_LABELS).flatMap(Object.values)).not.toContain('worker');
    expect(Object.values(QUERY_ROW_LABELS).flatMap(Object.values)).not.toContain('Node sub-query');
  });
});

describe('thread labels', () => {
  it('keeps threads in their own vocabulary', () => {
    // query_thread_log has master_thread_id, "OS initial ID of initial
    // thread"; workload scheduling defines MASTER THREAD and WORKER THREAD.
    // Threads are not nodes, so "initiator" does not belong here.
    expect(THREAD_LABELS.master.thread).toBe('Master thread');
    expect(THREAD_LABELS.worker.thread).toBe('Worker thread');
    expect(THREAD_LABELS.master.thread).not.toContain('Initiator');
  });
});

describe('the label functions render from the table', () => {
  // The four exported functions are the API the rest of the codebase calls.
  // These check the wiring, not the wording.
  it('routes every label function through the table', () => {
    expect(topologyNodeRoleLabel('shard_leader')).toBe(TOPOLOGY_ROLE_LABELS.shard_leader.title);
    expect(topologyNodeRoleText('shard_leader')).toBe(TOPOLOGY_ROLE_LABELS.shard_leader.noun);
    expect(distributedNodeRoleLabel('replica_reader', 4)).toBe('Shard 4 replica');
    expect(distributedQueryKindLabel('parallel_replicas_select')).toBe('Parallel replicas');
    expect(TOPOLOGY_ACTOR_LABELS.coordinator).toBe(topologyRoleTitle('coordinator'));
  });
});
