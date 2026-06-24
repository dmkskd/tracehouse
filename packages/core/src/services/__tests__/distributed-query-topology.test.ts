import { describe, expect, it } from 'vitest';
import {
  buildDistributedExecutionFlowSteps,
  formatDistributedTopologyReport,
  inferDistributedTopology,
  parseDistributedTextLogPhases,
  type ClusterHostInput,
  type DistributedQueryExecutionInput,
  type DistributedTextLogInput,
  type ProcessorProfileInput,
} from '../distributed-query-topology.js';

const clusterHosts: ClusterHostInput[] = [
  { hostName: 'chi-dev-cluster-dev-0-0', shardNum: 1, replicaNum: 1 },
  { hostName: 'chi-dev-cluster-dev-0-1', shardNum: 1, replicaNum: 2 },
  { hostName: 'chi-dev-cluster-dev-1-0', shardNum: 2, replicaNum: 1 },
  { hostName: 'chi-dev-cluster-dev-1-1', shardNum: 2, replicaNum: 2 },
];

function row(overrides: Partial<DistributedQueryExecutionInput>): DistributedQueryExecutionInput {
  return {
    queryId: 'q',
    initialQueryId: 'q',
    isInitialQuery: true,
    hostname: 'chi-dev-cluster-dev-0-0.clickhouse.svc.cluster.local',
    queryKind: 'Select',
    queryStartTimeMicroseconds: '2026-06-21 12:00:00.000000',
    queryDurationMs: 10,
    readRows: 0,
    readBytes: 0,
    writtenRows: 0,
    writtenBytes: 0,
    profileEvents: {},
    tables: ['synthetic_data.events'],
    ...overrides,
  };
}

function proc(overrides: Partial<ProcessorProfileInput>): ProcessorProfileInput {
  return {
    queryId: 'q',
    initialQueryId: 'q',
    hostname: 'chi-dev-cluster-dev-0-0.clickhouse.svc.cluster.local',
    processorName: 'ReadFromMergeTree',
    planStepName: 'ReadFromMergeTree',
    planStepDescription: 'synthetic_data.events_local',
    ...overrides,
  };
}

function textLog(overrides: Partial<DistributedTextLogInput>): DistributedTextLogInput {
  return {
    queryId: 'root',
    eventTimeMicroseconds: '2026-06-21 16:01:59.257000',
    level: 'Debug',
    source: 'executeQuery',
    message: '',
    threadName: 'TCPHandler',
    ...overrides,
  };
}

describe('inferDistributedTopology', { tags: ['query-analysis'] }, () => {
  it('classifies a single execution as local', () => {
    const topology = inferDistributedTopology({
      rootQueryId: 'local',
      executions: [row({ queryId: 'local', initialQueryId: 'local' })],
      clusterHosts,
    });

    expect(topology.kind).toBe('local');
    expect(topology.confidence).toBe('high');
    expect(topology.coordinator?.role).toBe('coordinator');
    expect(topology.nodes).toHaveLength(1);
    expect(topology.shards).toHaveLength(1);
    expect(topology.shards[0].children).toHaveLength(0);
  });

  it('classifies a plain distributed SELECT as coordinator plus one remote child per shard', () => {
    const topology = inferDistributedTopology({
      rootQueryId: 'plain',
      clusterHosts,
      processorProfiles: [
        proc({
          queryId: 'child-s1',
          initialQueryId: 'plain',
          hostname: 'chi-dev-cluster-dev-0-0.clickhouse.svc.cluster.local',
          planStepDescription: 'MergeTreeSelect(pool: ReadPool, algorithm: Thread)',
        }),
        proc({
          queryId: 'child-s2',
          initialQueryId: 'plain',
          hostname: 'chi-dev-cluster-dev-1-0.clickhouse.svc.cluster.local',
          planStepDescription: 'MergeTreeSelect(pool: ReadPool, algorithm: Thread)',
        }),
      ],
      executions: [
        row({
          queryId: 'plain',
          initialQueryId: 'plain',
          isInitialQuery: true,
          profileEvents: { DistributedConnectionTries: 2, SuspendSendingQueryToShard: 1, Shards: 2 },
        }),
        row({
          queryId: 'child-s1',
          initialQueryId: 'plain',
          isInitialQuery: false,
          hostname: 'chi-dev-cluster-dev-0-0.clickhouse.svc.cluster.local',
          queryDurationMs: 27,
          readRows: 59_536_603,
          profileEvents: { SelectedMarks: 7378, SelectedParts: 5 },
        }),
        row({
          queryId: 'child-s2',
          initialQueryId: 'plain',
          isInitialQuery: false,
          hostname: 'chi-dev-cluster-dev-1-0.clickhouse.svc.cluster.local',
          queryDurationMs: 29,
          readRows: 59_463_397,
          profileEvents: { SelectedMarks: 7378, SelectedParts: 6 },
        }),
      ],
    });

    expect(topology.kind).toBe('plain_distributed_select');
    expect(topology.confidence).toBe('medium');
    expect(topology.coordinator?.role).toBe('coordinator');
    expect(topology.shards).toHaveLength(2);
    expect(topology.shards.map((shard) => shard.children.map((node) => node.role))).toEqual([
      ['remote_child'],
      ['remote_child'],
    ]);
    expect(topology.shardCoverage).toEqual({
      observedShards: 2,
      expectedShardSends: 2,
      allExpectedShardSendsObserved: true,
    });
    expect(topology.readDistribution.entries.map(entry => entry.queryId)).toEqual(['child-s1', 'child-s2']);
    expect(topology.readDistribution.groups).toHaveLength(1);
  });

  it('groups read distribution by normalized query hash for UNION-style fan-out branches', () => {
    const topology = inferDistributedTopology({
      rootQueryId: 'union-root',
      clusterHosts,
      executions: [
        row({
          queryId: 'union-root',
          initialQueryId: 'union-root',
          isInitialQuery: true,
          queryPreview: 'SELECT ... UNION ALL SELECT ...',
        }),
        row({
          queryId: 'metric-s1',
          initialQueryId: 'union-root',
          isInitialQuery: false,
          normalizedQueryHash: '111',
          hostname: 'chi-dev-cluster-dev-0-0.clickhouse.svc.cluster.local',
          queryPreview: 'SELECT avg(ProfileEvent_Query) FROM clusterAllReplicas(\'all\', system.metric_log)',
          queryDurationMs: 12,
          readRows: 100,
          readBytes: 1000,
        }),
        row({
          queryId: 'metric-s2',
          initialQueryId: 'union-root',
          isInitialQuery: false,
          normalizedQueryHash: '111',
          hostname: 'chi-dev-cluster-dev-1-0.clickhouse.svc.cluster.local',
          queryPreview: 'SELECT avg(ProfileEvent_Query) FROM clusterAllReplicas(\'all\', system.metric_log)',
          queryDurationMs: 10,
          readRows: 120,
          readBytes: 1200,
        }),
        row({
          queryId: 'query-log-s1',
          initialQueryId: 'union-root',
          isInitialQuery: false,
          normalizedQueryHash: '222',
          hostname: 'chi-dev-cluster-dev-0-1.clickhouse.svc.cluster.local',
          queryPreview: 'SELECT count() FROM clusterAllReplicas(\'all\', system.query_log)',
          queryDurationMs: 8,
          readRows: 20,
          readBytes: 500,
        }),
        row({
          queryId: 'query-log-s2',
          initialQueryId: 'union-root',
          isInitialQuery: false,
          normalizedQueryHash: '222',
          hostname: 'chi-dev-cluster-dev-1-1.clickhouse.svc.cluster.local',
          queryPreview: 'SELECT count() FROM clusterAllReplicas(\'all\', system.query_log)',
          queryDurationMs: 9,
          readRows: 25,
          readBytes: 550,
        }),
      ],
    });

    expect(topology.readDistribution.entries).toHaveLength(4);
    expect(topology.readDistribution.groups).toHaveLength(2);
    expect(topology.readDistribution.groups.map(group => group.label).sort()).toEqual(['system.metric_log', 'system.query_log']);
    expect(topology.readDistribution.groups.map(group => group.entries.length)).toEqual([2, 2]);
  });

  it('maps query-log pod hostnames to system.clusters service hostnames', () => {
    const topology = inferDistributedTopology({
      rootQueryId: 'pod-hosts',
      clusterHosts: [
        { hostName: 'chi-dev-cluster-dev-0-0', shardNum: 1, replicaNum: 1 },
        { hostName: 'chi-dev-cluster-dev-1-0', shardNum: 2, replicaNum: 1 },
        { hostName: 'chi-dev-cluster-dev-2-0', shardNum: 3, replicaNum: 1 },
        { hostName: 'chi-dev-cluster-dev-3-0', shardNum: 4, replicaNum: 1 },
      ],
      executions: [
        row({
          queryId: 'pod-hosts',
          initialQueryId: 'pod-hosts',
          isInitialQuery: true,
          hostname: 'chi-dev-cluster-dev-0-0-0',
        }),
        ...[0, 1, 2, 3].map((shardIndex) => row({
          queryId: `child-s${shardIndex + 1}`,
          initialQueryId: 'pod-hosts',
          isInitialQuery: false,
          hostname: `chi-dev-cluster-dev-${shardIndex}-0-0`,
          queryDurationMs: 20 + shardIndex,
        })),
      ],
    });

    expect(topology.shards.map((shard) => shard.shardNum)).toEqual([1, 2, 3, 4]);
    expect(topology.nodes.filter((node) => node.role === 'remote_child').map((node) => node.shardNum)).toEqual([1, 2, 3, 4]);
    expect(topology.confidence).toBe('medium');
  });

  it('classifies parallel-replica SELECT leaders and readers from ProfileEvents', () => {
    const topology = inferDistributedTopology({
      rootQueryId: 'parallel',
      clusterHosts,
      processorProfiles: [
        proc({
          queryId: 'reader-s1r1',
          initialQueryId: 'parallel',
          hostname: 'chi-dev-cluster-dev-0-0.clickhouse.svc.cluster.local',
          planStepDescription: 'MergeTreeSelect(pool: ReadPoolParallelReplicas, algorithm: Thread)',
        }),
        proc({
          queryId: 'reader-s1r2',
          initialQueryId: 'parallel',
          hostname: 'chi-dev-cluster-dev-0-1.clickhouse.svc.cluster.local',
          planStepDescription: 'MergeTreeSelect(pool: ReadPoolParallelReplicas, algorithm: Thread)',
        }),
      ],
      executions: [
        row({
          queryId: 'parallel',
          initialQueryId: 'parallel',
          isInitialQuery: true,
          readRows: 119_000_000,
          profileEvents: { DistributedConnectionTries: 2 },
        }),
        row({
          queryId: 'leader-s1',
          initialQueryId: 'parallel',
          isInitialQuery: false,
          hostname: 'chi-dev-cluster-dev-0-0.clickhouse.svc.cluster.local',
          readRows: 59_536_603,
          profileEvents: {
            ParallelReplicasHandleRequestMicroseconds: 100,
            ParallelReplicasReadAssignedMarks: 1200,
            ParallelReplicasUsedCount: 2,
          },
        }),
        row({
          queryId: 'reader-s1r1',
          initialQueryId: 'parallel',
          isInitialQuery: false,
          hostname: 'chi-dev-cluster-dev-0-0.clickhouse.svc.cluster.local',
          readRows: 32_000_990,
          profileEvents: {
            SelectedMarks: 7378,
            SelectedParts: 5,
            ParallelReplicasReadRequestMicroseconds: 3997,
            ParallelReplicasReadMarks: 17,
          },
        }),
        row({
          queryId: 'reader-s1r2',
          initialQueryId: 'parallel',
          isInitialQuery: false,
          hostname: 'chi-dev-cluster-dev-0-1.clickhouse.svc.cluster.local',
          readRows: 27_535_613,
          profileEvents: {
            SelectedMarks: 7378,
            SelectedParts: 5,
            ParallelReplicasReadRequestMicroseconds: 3381,
            ParallelReplicasReadMarks: 46,
          },
        }),
      ],
    });

    expect(topology.kind).toBe('parallel_replicas_select');
    expect(topology.confidence).toBe('high');
    expect(topology.detectorPlugins).toContain('profile-events-parallel-replicas');
    expect(topology.detectorPlugins).toContain('cluster-host-mapper');
    expect(topology.shards).toHaveLength(1);
    expect(topology.shards[0].leader?.queryId).toBe('leader-s1');
    expect(topology.shards[0].readers.map((node) => node.queryId)).toEqual(['reader-s1r1', 'reader-s1r2']);
    expect(topology.nodes.find((node) => node.queryId === 'reader-s1r2')?.replicaNum).toBe(2);
  });

  it('groups parallel-replica reader flow steps under their shard coordinator', () => {
    const topology = inferDistributedTopology({
      rootQueryId: 'parallel-flow',
      clusterHosts,
      processorProfiles: [
        proc({
          queryId: 'reader-s1r1',
          initialQueryId: 'parallel-flow',
          hostname: 'chi-dev-cluster-dev-0-0.clickhouse.svc.cluster.local',
          planStepDescription: 'MergeTreeSelect(pool: ReadPoolParallelReplicas, algorithm: Thread)',
        }),
        proc({
          queryId: 'reader-s1r2',
          initialQueryId: 'parallel-flow',
          hostname: 'chi-dev-cluster-dev-0-1.clickhouse.svc.cluster.local',
          planStepDescription: 'MergeTreeSelect(pool: ReadPoolParallelReplicas, algorithm: Thread)',
        }),
      ],
      executions: [
        row({
          queryId: 'parallel-flow',
          initialQueryId: 'parallel-flow',
          isInitialQuery: true,
          readRows: 1000,
          profileEvents: { DistributedConnectionTries: 2 },
        }),
        row({
          queryId: 'leader-s1',
          initialQueryId: 'parallel-flow',
          isInitialQuery: false,
          hostname: 'chi-dev-cluster-dev-0-0.clickhouse.svc.cluster.local',
          queryStartTimeMicroseconds: '2026-06-21 12:00:00.003000',
          queryDurationMs: 24,
          readRows: 900,
          profileEvents: {
            ParallelReplicasHandleRequestMicroseconds: 100,
            ParallelReplicasReadAssignedMarks: 1200,
            ParallelReplicasUsedCount: 2,
          },
        }),
        row({
          queryId: 'reader-s1r1',
          initialQueryId: 'parallel-flow',
          isInitialQuery: false,
          hostname: 'chi-dev-cluster-dev-0-0.clickhouse.svc.cluster.local',
          queryStartTimeMicroseconds: '2026-06-21 12:00:00.006000',
          queryDurationMs: 20,
          readRows: 600,
          profileEvents: {
            SelectedMarks: 7378,
            SelectedParts: 5,
            ParallelReplicasReadRequestMicroseconds: 3997,
            ParallelReplicasReadMarks: 17,
          },
        }),
        row({
          queryId: 'reader-s1r2',
          initialQueryId: 'parallel-flow',
          isInitialQuery: false,
          hostname: 'chi-dev-cluster-dev-0-1.clickhouse.svc.cluster.local',
          queryStartTimeMicroseconds: '2026-06-21 12:00:00.006000',
          queryDurationMs: 13,
          readRows: 400,
          profileEvents: {
            SelectedMarks: 7378,
            SelectedParts: 5,
            ParallelReplicasReadRequestMicroseconds: 3381,
            ParallelReplicasReadMarks: 46,
          },
        }),
      ],
    });

    const steps = buildDistributedExecutionFlowSteps(topology);
    const leader = topology.nodes.find((node) => node.queryId === 'leader-s1');
    const coordinatorStarted = steps.find((step) => step.event.kind === 'coordinator_started');
    const leaderStarted = steps.find((step) => step.event.kind === 'remote_started' && step.node?.queryId === 'leader-s1');
    const readerStarted = steps.filter((step) => step.event.kind === 'remote_started' && step.node?.role === 'replica_reader');

    expect(coordinatorStarted).toMatchObject({ depth: 0 });
    expect(leaderStarted).toMatchObject({ depth: 1, parentNodeId: undefined, groupId: 'shard:1' });
    expect(readerStarted).toHaveLength(2);
    expect(readerStarted.map((step) => step.node?.queryId)).toEqual(['reader-s1r1', 'reader-s1r2']);
    expect(readerStarted.map((step) => step.parentNodeId)).toEqual([leader?.id, leader?.id]);
    expect(readerStarted.map((step) => step.depth)).toEqual([2, 2]);
    expect(readerStarted.map((step) => step.groupId)).toEqual(['shard:1', 'shard:1']);
  });

  it('distinguishes clusterAllReplicas-style fan-out from shard-leader topology', () => {
    const topology = inferDistributedTopology({
      rootQueryId: 'all-replicas',
      clusterHosts,
      executions: [
        row({
          queryId: 'all-replicas',
          initialQueryId: 'all-replicas',
          isInitialQuery: true,
          profileEvents: { DistributedConnectionTries: 4 },
        }),
        row({
          queryId: 'r1',
          initialQueryId: 'all-replicas',
          isInitialQuery: false,
          hostname: 'chi-dev-cluster-dev-0-0.clickhouse.svc.cluster.local',
          profileEvents: { SelectedRows: 10 },
        }),
        row({
          queryId: 'r2',
          initialQueryId: 'all-replicas',
          isInitialQuery: false,
          hostname: 'chi-dev-cluster-dev-0-1.clickhouse.svc.cluster.local',
          profileEvents: { SelectedRows: 10 },
        }),
        row({
          queryId: 'r3',
          initialQueryId: 'all-replicas',
          isInitialQuery: false,
          hostname: 'chi-dev-cluster-dev-1-0.clickhouse.svc.cluster.local',
          profileEvents: { SelectedRows: 10 },
        }),
        row({
          queryId: 'r4',
          initialQueryId: 'all-replicas',
          isInitialQuery: false,
          hostname: 'chi-dev-cluster-dev-1-1.clickhouse.svc.cluster.local',
          profileEvents: { SelectedRows: 10 },
        }),
      ],
    });

    expect(topology.kind).toBe('cluster_all_replicas');
    expect(topology.nodes.filter((node) => node.role === 'remote_child')).toHaveLength(4);
    expect(topology.shards).toHaveLength(2);
    expect(topology.shards.map((shard) => shard.children)).toHaveLength(2);
    expect(topology.shards.every((shard) => shard.leader == null && shard.readers.length === 0)).toBe(true);
  });

  it('marks clusterAllReplicas forced all-node fan-out with a folded local initiator participant', () => {
    const topology = inferDistributedTopology({
      rootQueryId: 'all-replicas-folded-local',
      clusterHosts,
      executions: [
        row({
          queryId: 'all-replicas-folded-local',
          initialQueryId: 'all-replicas-folded-local',
          isInitialQuery: true,
          hostname: 'chi-dev-cluster-dev-0-0-0',
          readRows: 5,
          readBytes: 500,
          queryPreview: "SELECT count() FROM clusterAllReplicas('tracehouse', system.processes)",
        }),
        row({
          queryId: 'r-s1r2',
          initialQueryId: 'all-replicas-folded-local',
          isInitialQuery: false,
          hostname: 'chi-dev-cluster-dev-0-1.clickhouse.svc.cluster.local',
          readRows: 4,
          readBytes: 400,
        }),
        row({
          queryId: 'r-s2r1',
          initialQueryId: 'all-replicas-folded-local',
          isInitialQuery: false,
          hostname: 'chi-dev-cluster-dev-1-0.clickhouse.svc.cluster.local',
          readRows: 5,
          readBytes: 500,
        }),
        row({
          queryId: 'r-s2r2',
          initialQueryId: 'all-replicas-folded-local',
          isInitialQuery: false,
          hostname: 'chi-dev-cluster-dev-1-1.clickhouse.svc.cluster.local',
          readRows: 5,
          readBytes: 500,
        }),
      ],
    });

    expect(topology.kind).toBe('cluster_all_replicas');
    expect(topology.fanoutMode).toBe('all_replicas');
    expect(topology.clusterAllReplicas).toMatchObject({
      fanoutMode: 'all_replicas',
      expectedParticipants: 4,
      observedRemoteChildren: 3,
      localParticipantsOnInitiator: 1,
      allExpectedParticipantsAccounted: true,
    });
    expect(topology.localRead).toMatchObject({
      kind: 'folded_into_coordinator',
      shardNum: 1,
      replicaNum: 1,
      readRows: 5,
      readBytes: 500,
    });
    expect(topology.executionFlow.map(event => event.kind)).toContain('local_read_started');
    expect(topology.readDistribution.entries).toHaveLength(4);
    expect(topology.readDistribution.entries.find(entry => entry.foldedIntoCoordinator)).toMatchObject({
      role: 'local_reader',
      shardNum: 1,
      replicaNum: 1,
    });
    expect(topology.readDistribution.groups).toContainEqual(expect.objectContaining({
      key: 'local_reader',
      label: 'Coordinator local read',
      entries: [expect.objectContaining({ foldedIntoCoordinator: true })],
    }));
  });

  it('rolls read distribution up by shard and flags per-replica reading', () => {
    const topology = inferDistributedTopology({
      rootQueryId: 'parallel-distribution',
      clusterHosts,
      processorProfiles: [
        proc({
          queryId: 'reader-s1r1',
          initialQueryId: 'parallel-distribution',
          hostname: 'chi-dev-cluster-dev-0-0.clickhouse.svc.cluster.local',
          planStepDescription: 'MergeTreeSelect(pool: ReadPoolParallelReplicas, algorithm: Thread)',
        }),
        proc({
          queryId: 'reader-s1r2',
          initialQueryId: 'parallel-distribution',
          hostname: 'chi-dev-cluster-dev-0-1.clickhouse.svc.cluster.local',
          planStepDescription: 'MergeTreeSelect(pool: ReadPoolParallelReplicas, algorithm: Thread)',
        }),
        proc({
          queryId: 'reader-s2r1',
          initialQueryId: 'parallel-distribution',
          hostname: 'chi-dev-cluster-dev-1-0.clickhouse.svc.cluster.local',
          planStepDescription: 'MergeTreeSelect(pool: ReadPoolParallelReplicas, algorithm: Thread)',
        }),
        proc({
          queryId: 'reader-s2r2',
          initialQueryId: 'parallel-distribution',
          hostname: 'chi-dev-cluster-dev-1-1.clickhouse.svc.cluster.local',
          planStepDescription: 'MergeTreeSelect(pool: ReadPoolParallelReplicas, algorithm: Thread)',
        }),
      ],
      executions: [
        row({
          queryId: 'parallel-distribution',
          initialQueryId: 'parallel-distribution',
          isInitialQuery: true,
          readRows: 60_000_000,
          profileEvents: { DistributedConnectionTries: 2 },
        }),
        row({
          queryId: 'leader-s1',
          initialQueryId: 'parallel-distribution',
          isInitialQuery: false,
          hostname: 'chi-dev-cluster-dev-0-0.clickhouse.svc.cluster.local',
          readRows: 30_500_000,
          profileEvents: {
            ParallelReplicasHandleRequestMicroseconds: 100,
            ParallelReplicasReadAssignedMarks: 1200,
            ParallelReplicasUsedCount: 2,
          },
        }),
        row({
          queryId: 'reader-s1r1',
          initialQueryId: 'parallel-distribution',
          isInitialQuery: false,
          hostname: 'chi-dev-cluster-dev-0-0.clickhouse.svc.cluster.local',
          readRows: 12_900_000,
          readBytes: 13_470_000,
          profileEvents: { ParallelReplicasReadMarks: 700 },
        }),
        row({
          queryId: 'reader-s1r2',
          initialQueryId: 'parallel-distribution',
          isInitialQuery: false,
          hostname: 'chi-dev-cluster-dev-0-1.clickhouse.svc.cluster.local',
          readRows: 17_600_000,
          readBytes: 13_440_000,
          profileEvents: { ParallelReplicasReadMarks: 900 },
        }),
        row({
          queryId: 'leader-s2',
          initialQueryId: 'parallel-distribution',
          isInitialQuery: false,
          hostname: 'chi-dev-cluster-dev-1-0.clickhouse.svc.cluster.local',
          readRows: 29_500_000,
          profileEvents: {
            ParallelReplicasHandleRequestMicroseconds: 100,
            ParallelReplicasReadAssignedMarks: 1200,
            ParallelReplicasUsedCount: 2,
          },
        }),
        row({
          queryId: 'reader-s2r1',
          initialQueryId: 'parallel-distribution',
          isInitialQuery: false,
          hostname: 'chi-dev-cluster-dev-1-0.clickhouse.svc.cluster.local',
          readRows: 11_400_000,
          readBytes: 5_250_000,
          profileEvents: { ParallelReplicasReadMarks: 600 },
        }),
        row({
          queryId: 'reader-s2r2',
          initialQueryId: 'parallel-distribution',
          isInitialQuery: false,
          hostname: 'chi-dev-cluster-dev-1-1.clickhouse.svc.cluster.local',
          readRows: 18_000_000,
          readBytes: 14_790_000,
          profileEvents: { ParallelReplicasReadMarks: 800 },
        }),
      ],
    });

    expect(topology.kind).toBe('parallel_replicas_select');
    expect(topology.fanoutMode).toBe('parallel_replicas');
    expect(topology.readDistribution.hasPerReplicaReading).toBe(true);
    expect(topology.readDistribution.entries.map(entry => entry.queryId)).not.toContain('leader-s1');
    expect(topology.readDistribution.shards).toHaveLength(2);
    expect(topology.readDistribution.shards.map(shard => ({
      shardNum: shard.shardNum,
      readRows: shard.readRows,
      replicas: shard.replicas.length,
      hasPerReplicaReading: shard.hasPerReplicaReading,
    }))).toEqual([
      { shardNum: 1, readRows: 30_500_000, replicas: 2, hasPerReplicaReading: true },
      { shardNum: 2, readRows: 29_400_000, replicas: 2, hasPerReplicaReading: true },
    ]);
    const rowSkew = topology.readDistribution.skew.metrics.find(metric => metric.metric === 'read_rows');
    expect(rowSkew).toMatchObject({
      metric: 'read_rows',
      max: 18_000_000,
      maxShardNum: 2,
      maxReplicaNum: 2,
      severity: 'none',
    });
    expect(rowSkew?.maxShare).toBeCloseTo(18_000_000 / 59_900_000, 5);
  });

  it('models distributed INSERT cascades without relying on a shared initial_query_id', () => {
    const topology = inferDistributedTopology({
      rootQueryId: 'cf11be63-d75f-4d56-a224-98fe346c36db',
      clusterHosts,
      executions: [
        row({
          queryId: 'cf11be63-d75f-4d56-a224-98fe346c36db',
          initialQueryId: 'cf11be63-d75f-4d56-a224-98fe346c36db',
          isInitialQuery: true,
          queryKind: 'Insert',
          tables: ['_table_function.numbers', 'replacing_test.product_prices'],
          writtenRows: 500_000,
          writtenBytes: 19_501_288,
          profileEvents: { InsertedRows: 500_000, InsertedBytes: 19_501_288 },
        }),
        row({
          queryId: 'bc9688ee-315b-4a1a-a2b6-f8f8ea00066a',
          initialQueryId: 'afc4fc32-f289-41d5-8d3b-fe9fb2fdf5a6',
          isInitialQuery: false,
          queryKind: 'Insert',
          tables: ['replacing_test.product_prices_local'],
          writtenRows: 249_283,
          writtenBytes: 9_722_198,
          profileEvents: { AsyncInsertQuery: 1, AsyncInsertBytes: 9_722_198 },
        }),
        row({
          queryId: '72ff1dee-1384-4d72-b574-52a0eb2b0bd8',
          initialQueryId: '72ff1dee-1384-4d72-b574-52a0eb2b0bd8',
          isInitialQuery: true,
          queryKind: 'AsyncInsertFlush',
          tables: ['replacing_test.product_prices_local'],
          writtenRows: 249_283,
          writtenBytes: 9_722_198,
          profileEvents: { AsyncInsertRows: 249_283, InsertedRows: 249_283, InsertedBytes: 9_722_198 },
        }),
      ],
      asyncInsertLogs: [{
        queryId: 'cf11be63-d75f-4d56-a224-98fe346c36db',
        flushQueryId: '72ff1dee-1384-4d72-b574-52a0eb2b0bd8',
        hostname: 'chi-dev-cluster-dev-0-0.clickhouse.svc.cluster.local',
        database: 'replacing_test',
        table: 'product_prices_local',
        status: 'Ok',
        rows: 249_283,
        bytes: 9_722_198,
        eventTimeMicroseconds: '2026-06-21 12:00:00.005000',
      }],
    });

    expect(topology.kind).toBe('distributed_insert');
    expect(topology.nodes.map((node) => node.role)).toEqual([
      'insert_client',
      'insert_forwarder',
      'async_insert_flush',
    ]);
    expect(topology.evidence).toContainEqual({ source: 'query_log', message: 'write-side query kinds detected' });
    expect(topology.asyncInsertLinks).toHaveLength(1);
    expect(topology.asyncInsertLinks[0]).toMatchObject({
      source: 'asynchronous_insert_log',
      queryId: 'cf11be63-d75f-4d56-a224-98fe346c36db',
      flushQueryId: '72ff1dee-1384-4d72-b574-52a0eb2b0bd8',
      rows: 249_283,
      bytes: 9_722_198,
      confidence: 'high',
    });
    expect(topology.evidence).toContainEqual({ source: 'asynchronous_insert_log', message: '1 async insert flush link(s) detected' });
    expect(topology.executionFlow.map(event => event.kind)).toContain('async_insert_buffered');
    expect(topology.detectorPlugins).toContain('query-log-write-cascade');
    expect(topology.detectorPlugins).toContain('async-insert-log-linker');
  });

  it('models object-storage swarm execution separately from shard replicas', () => {
    const topology = inferDistributedTopology({
      rootQueryId: 'iceberg-swarm',
      capabilities: {
        objectStorageClusterMetadata: true,
        icebergMetadata: true,
        parquetMetadata: true,
        processorsProfileLog: true,
      },
      processorProfiles: [
        proc({
          queryId: 'swarm-worker-a',
          initialQueryId: 'iceberg-swarm',
          hostname: 'swarm-a',
          processorName: 'ReadFromObjectStorage',
          planStepName: 'ReadFromStorage',
          planStepDescription: 'Read Iceberg Parquet files from S3',
        }),
      ],
      executions: [
        row({
          queryId: 'iceberg-swarm',
          initialQueryId: 'iceberg-swarm',
          isInitialQuery: true,
          hostname: 'initiator',
          settings: { object_storage_cluster: 'swarm-dev' },
          tableEngines: ['IcebergS3'],
          usedStorages: ['Iceberg', 'Parquet', 'S3'],
          tables: ['iceberg_nyc_taxi.trips'],
        }),
        row({
          queryId: 'swarm-worker-a',
          initialQueryId: 'iceberg-swarm',
          isInitialQuery: false,
          hostname: 'swarm-a',
          tableEngines: ['IcebergS3'],
          usedStorages: ['Iceberg', 'Parquet', 'S3'],
          readRows: 1_000_000,
        }),
      ],
    });

    expect(topology.kind).toBe('object_storage_swarm_select');
    expect(topology.detectorPlugins).toContain('object-storage-swarm');
    expect(topology.detectorPlugins).toContain('iceberg-parquet-storage');
    expect(topology.nodes.find((node) => node.queryId === 'swarm-worker-a')?.role).toBe('object_storage_worker');
    expect(topology.shards).toHaveLength(0);
    expect(topology.decisions).toContainEqual({
      level: 'info',
      code: 'object-storage-execution-detected',
      source: 'query_log',
      message: 'Object-storage execution was detected from table engines, storages, settings, or processor phases.',
    });
  });

  it('models Hybrid hot/cold storage segments as storage topology', () => {
    const topology = inferDistributedTopology({
      rootQueryId: 'hybrid',
      capabilities: {
        hybridTableMetadata: true,
        icebergMetadata: true,
        parquetMetadata: true,
      },
      executions: [
        row({
          queryId: 'hybrid',
          initialQueryId: 'hybrid',
          isInitialQuery: true,
          hostname: 'initiator',
          tableEngines: ['Hybrid'],
          tables: ['analytics.events_hybrid'],
          queryPreview: 'SELECT count() FROM analytics.events_hybrid WHERE event_date >= today() - 30',
        }),
        row({
          queryId: 'hybrid-hot',
          initialQueryId: 'hybrid',
          isInitialQuery: false,
          hostname: 'hot-node',
          tableEngines: ['MergeTree'],
          usedStorages: ['Hybrid', 'MergeTree'],
          tables: ['analytics.events_local'],
        }),
        row({
          queryId: 'hybrid-cold',
          initialQueryId: 'hybrid',
          isInitialQuery: false,
          hostname: 'cold-worker',
          tableEngines: ['IcebergS3'],
          usedStorages: ['Hybrid', 'Iceberg', 'Parquet', 'S3'],
          tables: ['analytics.events_iceberg'],
        }),
      ],
    });

    expect(topology.kind).toBe('hybrid_storage_select');
    expect(topology.detectorPlugins).toContain('hybrid-storage');
    expect(topology.nodes.filter((node) => node.role === 'hybrid_segment')).toHaveLength(2);
    expect(topology.decisions).toContainEqual({
      level: 'info',
      code: 'hybrid-storage-detected',
      source: 'query_log',
      message: 'Hybrid storage segments were detected from table engines, storages, or processor phases.',
    });
  });

  it('falls back to low-confidence unknown topology when evidence is missing', () => {
    const topology = inferDistributedTopology({
      rootQueryId: 'mystery',
      executions: [
        row({ queryId: 'child-a', initialQueryId: 'mystery', isInitialQuery: false, hostname: 'node-a' }),
        row({ queryId: 'child-b', initialQueryId: 'mystery', isInitialQuery: false, hostname: 'node-b' }),
      ],
    });

    expect(topology.kind).toBe('plain_distributed_select');
    expect(topology.confidence).toBe('low');
    expect(topology.warnings).toContain('No coordinator/client root row found');
  });

  it('respects explicit capability degradation', () => {
    const topology = inferDistributedTopology({
      rootQueryId: 'parallel',
      clusterHosts,
      capabilities: {
        profileEvents: false,
        systemClusters: false,
        processorsProfileLog: false,
        textLog: false,
      },
      executions: [
        row({
          queryId: 'parallel',
          initialQueryId: 'parallel',
          isInitialQuery: true,
          profileEvents: { DistributedConnectionTries: 2 },
        }),
        row({
          queryId: 'leader-s1',
          initialQueryId: 'parallel',
          isInitialQuery: false,
          hostname: 'chi-dev-cluster-dev-0-0.clickhouse.svc.cluster.local',
          profileEvents: {
            ParallelReplicasHandleRequestMicroseconds: 100,
            ParallelReplicasUsedCount: 2,
          },
        }),
      ],
    });

    expect(topology.kind).toBe('plain_distributed_select');
    expect(topology.confidence).toBe('low');
    expect(topology.shards).toHaveLength(0);
    expect(topology.nodes.find((node) => node.queryId === 'leader-s1')?.role).toBe('remote_child');
    expect(topology.decisions).toContainEqual({
      level: 'degraded',
      code: 'missing-profile-events',
      source: 'capability',
      message: 'ProfileEvents are unavailable; role detection falls back to query shape and processor data.',
    });
    expect(topology.decisions).toContainEqual({
      level: 'degraded',
      code: 'missing-system-clusters',
      source: 'capability',
      message: 'system.clusters mapping is unavailable; shard and replica labels may be unknown.',
    });
  });

  it('parses text_log execution events without treating them as primary topology evidence', () => {
    const topology = inferDistributedTopology({
      rootQueryId: 'root',
      clusterHosts,
      executions: [
        row({
          queryId: 'root',
          initialQueryId: 'root',
          isInitialQuery: true,
          queryStartTimeMicroseconds: '2026-06-21 16:01:59.204000',
          profileEvents: { DistributedConnectionTries: 2 },
        }),
        row({
          queryId: 'child-s1',
          initialQueryId: 'root',
          isInitialQuery: false,
          hostname: 'chi-dev-cluster-dev-0-0.clickhouse.svc.cluster.local',
          queryStartTimeMicroseconds: '2026-06-21 16:01:59.257000',
          queryDurationMs: 33_950,
          readRows: 2_344_901,
          readBytes: 58_140_000,
          profileEvents: { SelectedMarks: 10 },
        }),
      ],
      textLogs: [
        textLog({
          source: 'Connection (chi-dev-cluster-dev-2-0:9000)',
          message: 'Sent data for 2 scalars, total 2 rows in 6.2708e-05 sec., 31537 rows/sec., 72.00 B (1.08 MiB/sec.), compressed 0.47368421052631576 times to 152.00 B (2.28 MiB/sec.)',
        }),
        textLog({
          eventTimeMicroseconds: '2026-06-21 16:03:06.284000',
          source: 'MergingAggregatedTransform',
          message: 'Read 1024 blocks of partially aggregated data, total 3000000 rows.',
        }),
        textLog({
          eventTimeMicroseconds: '2026-06-21 16:03:29.519000',
          source: 'executeQuery',
          message: 'Read 12501918 rows, 310.00 MiB in 90.321524 sec., 138415.7114089439 rows/sec., 3.43 MiB/sec.',
        }),
      ],
      capabilities: {
        textLog: true,
      },
    });

    expect(topology.kind).toBe('plain_distributed_select');
    expect(topology.executionPhases.map(phase => phase.kind)).toEqual([
      'remote_scalar_exchange',
      'merge_partial_aggregation',
      'final_read',
    ]);
    expect(topology.executionPhases[0]).toMatchObject({
      source: 'text_log',
      hostname: 'chi-dev-cluster-dev-2-0',
      offsetMs: 53,
      rows: 2,
      scalars: 2,
    });
    expect(topology.executionPhases[1].offsetMs).toBe(67_080);
    expect(topology.executionPhases[2].offsetMs).toBe(90_315);
    expect(topology.evidence).toContainEqual({ source: 'text_log', message: '3 execution event(s) parsed' });
    expect(topology.decisions).toContainEqual({
      level: 'info',
      code: 'text-log-phases-detected',
      source: 'text_log',
      message: '3 execution event(s) were parsed from system.text_log.',
    });

    expect(topology.executionFlow.map(event => event.kind)).toEqual([
      'coordinator_started',
      'remote_setup',
      'remote_started',
      'remote_read_completed',
      'coordinator_merge',
      'coordinator_read_completed',
    ]);
    expect(topology.executionFlow[1]).toMatchObject({
      actor: 'chi-dev-cluster-dev-2-0',
      source: 'text_log',
      offsetMs: 53,
    });
    expect(topology.executionFlow[2]).toMatchObject({
      actor: 's1r1',
      source: 'query_log',
      offsetMs: 53,
    });
    expect(topology.executionFlow[3]).toMatchObject({
      actor: 's1r1',
      source: 'query_log',
      offsetMs: 34_003,
      rows: 2_344_901,
    });
    expect(topology.executionFlow[4]).toMatchObject({
      actor: 'Coordinator',
      source: 'text_log',
      offsetMs: 67_080,
    });
  });

  it('binds execution-flow events by query id and host when child query ids repeat', () => {
    const topology = inferDistributedTopology({
      rootQueryId: 'union-root',
      executions: [
        row({
          queryId: 'union-root',
          initialQueryId: 'union-root',
          isInitialQuery: true,
          hostname: 'chi-dev-cluster-dev-0-0-0',
          queryStartTimeMicroseconds: '2026-06-21 18:00:00.000000',
          queryDurationMs: 40,
        }),
        row({
          queryId: 'same-child-id',
          initialQueryId: 'union-root',
          isInitialQuery: false,
          hostname: 'chi-dev-cluster-dev-0-0-0',
          queryStartTimeMicroseconds: '2026-06-21 18:00:00.010000',
          queryDurationMs: 8,
          queryPreview: 'SELECT branch_one FROM shard_one',
        }),
        row({
          queryId: 'same-child-id',
          initialQueryId: 'union-root',
          isInitialQuery: false,
          hostname: 'chi-dev-cluster-dev-1-0-0',
          queryStartTimeMicroseconds: '2026-06-21 18:00:00.012000',
          queryDurationMs: 9,
          queryPreview: 'SELECT branch_two FROM shard_two',
        }),
      ],
    });

    const steps = buildDistributedExecutionFlowSteps(topology);
    const started = steps.filter((step) => step.event.kind === 'remote_started');

    expect(started).toHaveLength(2);
    expect(started.map((step) => step.node?.hostname)).toEqual([
      'chi-dev-cluster-dev-0-0-0',
      'chi-dev-cluster-dev-1-0-0',
    ]);
    expect(started.map((step) => step.node?.queryPreview)).toEqual([
      'SELECT branch_one FROM shard_one',
      'SELECT branch_two FROM shard_two',
    ]);
    expect(started.map((step) => step.showQueryPreview)).toEqual([true, true]);
  });

  it('marks the coordinator start step for top-level query preview', () => {
    const topology = inferDistributedTopology({
      rootQueryId: 'root-with-preview',
      executions: [
        row({
          queryId: 'root-with-preview',
          initialQueryId: 'root-with-preview',
          isInitialQuery: true,
          hostname: 'coordinator-host',
          queryPreview: 'SELECT count() FROM distributed_table',
        }),
        row({
          queryId: 'child-with-preview',
          initialQueryId: 'root-with-preview',
          isInitialQuery: false,
          hostname: 'remote-host',
          queryPreview: 'SELECT count() FROM local_table',
        }),
      ],
    });

    const steps = buildDistributedExecutionFlowSteps(topology);
    const coordinatorStarted = steps.find((step) => step.event.kind === 'coordinator_started');
    const remoteStarted = steps.find((step) => step.event.kind === 'remote_started');

    expect(coordinatorStarted?.node?.queryPreview).toBe('SELECT count() FROM distributed_table');
    expect(coordinatorStarted?.showQueryPreview).toBe(true);
    expect(remoteStarted?.node?.queryPreview).toBe('SELECT count() FROM local_table');
    expect(remoteStarted?.showQueryPreview).toBe(true);
  });

  it('formats a human-readable decision report', () => {
    const topology = inferDistributedTopology({
      rootQueryId: 'plain',
      clusterHosts,
      executions: [
        row({
          queryId: 'plain',
          initialQueryId: 'plain',
          isInitialQuery: true,
          profileEvents: { DistributedConnectionTries: 2 },
        }),
        row({
          queryId: 'child-s1',
          initialQueryId: 'plain',
          isInitialQuery: false,
          hostname: 'chi-dev-cluster-dev-0-0.clickhouse.svc.cluster.local',
          profileEvents: { SelectedMarks: 10 },
        }),
      ],
    });

    const report = formatDistributedTopologyReport(topology);

    expect(report).toContain('Distributed topology report for plain');
    expect(report).toContain('Kind: plain_distributed_select');
    expect(report).toContain('Capabilities used: queryLog, profileEvents, systemClusters');
    expect(report).toContain('Detectors: query-log-read-fanout, cluster-host-mapper');
    expect(report).toContain('Decision trace:');
  });
});

describe('parseDistributedTextLogPhases', { tags: ['query-analysis'] }, () => {
  it('extracts remote exchange, merge, output, memory, and profile-event phases', () => {
    const phases = parseDistributedTextLogPhases([
      textLog({
        source: 'Connection (chi-dev-cluster-dev-2-0:9000)',
        message: 'Sent data for 2 scalars, total 2 rows in 6.2708e-05 sec., 31537 rows/sec., 72.00 B (1.08 MiB/sec.), compressed 0.47368421052631576 times to 152.00 B (2.28 MiB/sec.)',
      }),
      textLog({
        source: 'MergingAggregatedTransform',
        message: 'Read 1024 blocks of partially aggregated data, total 3000000 rows.',
      }),
      textLog({
        source: 'Aggregator',
        message: 'Converted aggregated data to chunks. 3000000 rows, 51.51 MiB in 8.956715462 sec. (334944.212 rows/sec., 5.75 MiB/sec.)',
      }),
      textLog({
        source: 'executeQuery',
        message: 'Read 12501918 rows, 310.00 MiB in 90.321524 sec., 138415.7114089439 rows/sec., 3.43 MiB/sec.',
      }),
      textLog({
        source: 'MemoryTracker',
        message: 'Query peak memory usage: 1.27 GiB.',
      }),
      textLog({
        source: 'TCPHandler',
        message: 'Sending profile events block with 146 rows, 20193 bytes took 156 milliseconds',
      }),
    ]);

    expect(phases.map(phase => phase.kind)).toEqual([
      'remote_scalar_exchange',
      'merge_partial_aggregation',
      'aggregation_output',
      'final_read',
      'memory_peak',
      'profile_events',
    ]);
    expect(phases[0].hostname).toBe('chi-dev-cluster-dev-2-0');
    expect(phases[1].blocks).toBe(1024);
    expect(phases[2].durationMs).toBeCloseTo(8956.715462);
    expect(phases[3].rows).toBe(12_501_918);
    expect(phases[4].bytesText).toBe('1.27 GiB');
    expect(phases[5].durationMs).toBe(156);
  });

  it('does not classify partial aggregate reads as final read summaries', () => {
    const phases = parseDistributedTextLogPhases([
      textLog({
        source: 'MergingAggregatedTransform',
        message: 'Read 1024 blocks of partially aggregated data, total 3000000 rows.',
      }),
    ]);

    expect(phases).toHaveLength(1);
    expect(phases[0].kind).toBe('merge_partial_aggregation');
  });

  it('returns no phases for unrelated log lines', () => {
    expect(parseDistributedTextLogPhases([
      textLog({ source: 'Planner', message: 'Query to stage Complete' }),
      textLog({ source: 'SelectExecutor', message: 'Key condition: unknown' }),
    ])).toEqual([]);
  });
});
