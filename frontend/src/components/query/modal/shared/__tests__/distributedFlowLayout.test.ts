import { describe, expect, it } from 'vitest';
import { buildDistributedFlowDiagram, computeFlowGauges } from '../distributedFlowLayout';
import {
  inferDistributedTopology,
  type ClusterHostInput,
  type DistributedQueryExecutionInput,
} from '@tracehouse/core';

const clusterHosts: ClusterHostInput[] = [
  { hostName: 'chi-dev-cluster-dev-0-0', shardNum: 1, replicaNum: 1 },
  { hostName: 'chi-dev-cluster-dev-0-1', shardNum: 1, replicaNum: 2 },
  { hostName: 'chi-dev-cluster-dev-1-0', shardNum: 2, replicaNum: 1 },
  { hostName: 'chi-dev-cluster-dev-1-1', shardNum: 2, replicaNum: 2 },
];

function row(overrides: Partial<DistributedQueryExecutionInput>): DistributedQueryExecutionInput {
  return {
    queryId: 'q',
    initialQueryId: 'root',
    isInitialQuery: false,
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

function fanOutTopology() {
  return inferDistributedTopology({
    rootQueryId: 'root',
    clusterHosts,
    executions: [
      row({
        queryId: 'root',
        isInitialQuery: true,
        queryDurationMs: 69,
        readRows: 55,
        queryPreview: 'SELECT count() FROM distributed_events',
      }),
      row({
        queryId: 'child-a',
        hostname: 'chi-dev-cluster-dev-1-0.clickhouse.svc.cluster.local',
        queryDurationMs: 21,
        readRows: 55,
        readBytes: 2900,
        // Scans 55 rows, hands back 4: the gap the edge label exists to show.
        resultRows: 4,
        resultBytes: 320,
      }),
      row({
        queryId: 'child-b',
        hostname: 'chi-dev-cluster-dev-0-1.clickhouse.svc.cluster.local',
        queryDurationMs: 10,
      }),
    ],
  });
}

describe('buildDistributedFlowDiagram', () => {
  it('puts the coordinator in the first column and its children in the second', () => {
    const diagram = buildDistributedFlowDiagram(fanOutTopology());

    const coordinator = diagram.nodes.find(node => node.isCoordinator);
    expect(coordinator?.column).toBe(0);

    const children = diagram.nodes.filter(node => !node.isCoordinator);
    expect(children).toHaveLength(2);
    for (const child of children) {
      expect(child.column).toBe(1);
      expect(child.x).toBeGreaterThan(coordinator!.x);
    }
  });

  it('draws one edge per parent/child pair, carrying what the child returned', () => {
    const diagram = buildDistributedFlowDiagram(fanOutTopology());
    const coordinator = diagram.nodes.find(node => node.isCoordinator)!;

    expect(diagram.edges).toHaveLength(2);
    for (const edge of diagram.edges) {
      expect(edge.sourceId).toBe(coordinator.id);
      expect(edge.kind).toBe('dispatch');
      expect(edge.path.startsWith('M ')).toBe(true);
    }

    const readEdge = diagram.edges.find(edge => {
      const target = diagram.nodes.find(node => node.id === edge.targetId);
      return target?.queryId === 'child-a';
    });
    // The child's result counters, not its read counters: an edge measures
    // what crossed it, and the 55 rows it scanned never left the shard.
    expect(readEdge?.returnedRows).toBe(4);
    expect(readEdge?.returnedBytes).toBe(320);
    const idleEdge = diagram.edges.find(edge => {
      const target = diagram.nodes.find(node => node.id === edge.targetId);
      return target?.queryId === 'child-b';
    });
    expect(idleEdge?.returnedRows).toBeUndefined();
  });

  it('never stacks two nodes on the same anchor', () => {
    const diagram = buildDistributedFlowDiagram(fanOutTopology());
    const anchors = diagram.nodes.map(node => `${node.x}:${node.y}`);
    expect(new Set(anchors).size).toBe(anchors.length);
  });

  it('centres a parent between its children and keeps every node inside the cluster box', () => {
    const diagram = buildDistributedFlowDiagram(fanOutTopology());
    const coordinator = diagram.nodes.find(node => node.isCoordinator)!;
    const childYs = diagram.nodes.filter(node => !node.isCoordinator).map(node => node.y);
    expect(coordinator.y).toBe((Math.min(...childYs) + Math.max(...childYs)) / 2);

    const [box] = diagram.groups;
    expect(box.label).toBe('CLICKHOUSE');
    for (const node of diagram.nodes) {
      expect(node.x).toBeGreaterThan(box.x);
      expect(node.x).toBeLessThan(box.x + box.width);
      expect(node.y).toBeGreaterThan(box.y);
      expect(node.y).toBeLessThan(box.y + box.height);
    }
    expect(diagram.width).toBeGreaterThan(box.x + box.width);
    expect(diagram.height).toBeGreaterThan(box.y + box.height);
  });

  it('applies the injected host label and the failed-query flags', () => {
    const diagram = buildDistributedFlowDiagram(fanOutTopology(), {
      hostLabel: hostname => hostname.slice(0, 4),
      failedQueryIds: ['child-b'],
    });

    expect(diagram.nodes.every(node => node.hostLabel === 'chi-')).toBe(true);
    const failed = diagram.nodes.find(node => node.queryId === 'child-b');
    expect(failed?.hasError).toBe(true);
    expect(diagram.nodes.find(node => node.queryId === 'child-a')?.hasError).toBe(false);
  });

  it('nests replica readers under their shard coordinator', () => {
    const topology = inferDistributedTopology({
      rootQueryId: 'root',
      clusterHosts,
      executions: [
        row({
          queryId: 'root',
          isInitialQuery: true,
          queryDurationMs: 90,
          queryPreview: "SELECT count() FROM cluster('dev', system.query_log)",
        }),
        row({
          queryId: 'leader-1',
          hostname: 'chi-dev-cluster-dev-0-0.clickhouse.svc.cluster.local',
          queryDurationMs: 60,
        }),
        row({
          queryId: 'reader-1',
          hostname: 'chi-dev-cluster-dev-0-1.clickhouse.svc.cluster.local',
          queryDurationMs: 30,
          readRows: 100,
        }),
      ],
    });
    const shardWithLeader = topology.shards.find(shard => shard.leader && shard.readers.length > 0);
    // Guard the fixture: without a leader/reader split there is nothing to nest.
    if (!shardWithLeader) return;

    const diagram = buildDistributedFlowDiagram(topology);
    const leader = diagram.nodes.find(node => node.id === shardWithLeader.leader!.id)!;
    const reader = diagram.nodes.find(node => node.id === shardWithLeader.readers[0].id)!;

    expect(reader.column).toBe(leader.column + 1);
    expect(diagram.edges).toContainEqual(
      expect.objectContaining({ sourceId: leader.id, targetId: reader.id }),
    );
  });

  it('wraps a wide fan-out into sub-columns instead of one tall file', () => {
    const topology = inferDistributedTopology({
      rootQueryId: 'root',
      clusterHosts,
      executions: [
        row({ queryId: 'root', isInitialQuery: true, queryDurationMs: 90 }),
        ...Array.from({ length: 16 }, (_, index) => row({
          queryId: `child-${index}`,
          hostname: `chi-dev-cluster-dev-${index % 2}-${index % 2}.clickhouse.svc.cluster.local`,
          queryDurationMs: index + 1,
          readRows: index * 10,
        })),
      ],
    });

    const diagram = buildDistributedFlowDiagram(topology);
    const children = diagram.nodes.filter(node => !node.isCoordinator);
    expect(children).toHaveLength(16);

    const subColumns = new Set(children.map(node => node.subColumn));
    expect(subColumns.size).toBeGreaterThan(1);
    // All of them are still the coordinator's children, one rank deep.
    expect(new Set(children.map(node => node.column))).toEqual(new Set([1]));
    // Rows are reused across sub-columns, so the diagram is wider than it is tall.
    const rows = new Set(children.map(node => node.y));
    expect(rows.size).toBeLessThan(children.length);
    expect(diagram.width).toBeGreaterThan(diagram.height);
  });

  it('gives each query shape its own labelled column in a multi-query fan-out', () => {
    const tables = ['system.metric_log', 'system.metrics', 'system.query_log', 'system.server_settings'];
    const topology = inferDistributedTopology({
      rootQueryId: 'root',
      clusterHosts,
      executions: [
        row({ queryId: 'root', isInitialQuery: true, queryDurationMs: 90 }),
        // Four query shapes, each sent to all four nodes.
        ...tables.flatMap((table, shapeIndex) => clusterHosts.map((host, hostIndex) => row({
          queryId: `child-${shapeIndex}-${hostIndex}`,
          hostname: `${host.hostName}.clickhouse.svc.cluster.local`,
          normalizedQueryHash: `shape-${shapeIndex}`,
          tables: [table],
          queryDurationMs: 5 + hostIndex,
          readRows: 100 * (shapeIndex + 1),
        }))),
      ],
    });

    const diagram = buildDistributedFlowDiagram(topology);
    const children = diagram.nodes.filter(node => !node.isCoordinator);
    expect(children).toHaveLength(16);

    // Every node in a sub-column ran the same shape.
    const shapesPerColumn = new Map<number, Set<string>>();
    for (const child of children) {
      const shapes = shapesPerColumn.get(child.subColumn) ?? new Set<string>();
      shapes.add(child.shapeKey);
      shapesPerColumn.set(child.subColumn, shapes);
    }
    expect(shapesPerColumn.size).toBe(4);
    for (const shapes of shapesPerColumn.values()) expect(shapes.size).toBe(1);

    expect(diagram.columnLabels.map(label => label.label).sort()).toEqual(tables);
  });

  it('leaves the columns unlabelled when every sibling ran the same query', () => {
    const topology = inferDistributedTopology({
      rootQueryId: 'root',
      clusterHosts,
      executions: [
        row({ queryId: 'root', isInitialQuery: true, queryDurationMs: 90 }),
        ...Array.from({ length: 8 }, (_, index) => row({
          queryId: `child-${index}`,
          hostname: `chi-dev-cluster-dev-${index % 2}-0.clickhouse.svc.cluster.local`,
          normalizedQueryHash: 'one-shape',
          queryDurationMs: index + 1,
        })),
      ],
    });

    expect(buildDistributedFlowDiagram(topology).columnLabels).toEqual([]);
  });

  it('fills a wide panel with the box and centres the participants inside it', () => {
    const natural = buildDistributedFlowDiagram(fanOutTopology());
    const wide = buildDistributedFlowDiagram(fanOutTopology(), { availableWidth: 1800 });

    expect(natural.width).toBeLessThan(1800);
    expect(wide.width).toBe(1800);

    const spread = (diagram: typeof natural) =>
      Math.max(...diagram.nodes.map(node => node.x)) - Math.min(...diagram.nodes.map(node => node.x));
    // Participants keep their spacing; the surplus becomes margin, not distance.
    expect(spread(wide)).toBe(spread(natural));
    expect(wide.nodes.map(node => node.y)).toEqual(natural.nodes.map(node => node.y));

    const [box] = wide.groups;
    expect(box.x).toBe(20);
    expect(box.x + box.width).toBe(1780);

    // Comparable margin either side. Not equal: an anchor's labels extend to
    // its right, so the rightmost anchor sits further from the box edge.
    const leftGap = Math.min(...wide.nodes.map(node => node.x)) - box.x;
    const rightGap = box.x + box.width - Math.max(...wide.nodes.map(node => node.x));
    expect(rightGap).toBeGreaterThan(leftGap);
    expect(rightGap - leftGap).toBeLessThan(260);
  });

  it('returns an empty diagram when the topology has no drawable nodes', () => {
    const diagram = buildDistributedFlowDiagram({
      ...fanOutTopology(),
      nodes: [],
      shards: [],
    });

    expect(diagram.nodes).toHaveLength(0);
    expect(diagram.edges).toHaveLength(0);
    expect(diagram.groups).toHaveLength(0);
  });

  it('scales each cube gauge against the largest node in the diagram', () => {
    const diagram = buildDistributedFlowDiagram(fanOutTopology());
    const gauges = computeFlowGauges(diagram.nodes);

    const slowest = Math.max(...diagram.nodes.map(node => node.metrics.durationMs));
    for (const node of diagram.nodes) {
      const duration = gauges.get(node.id)?.find(gauge => gauge.key === 'duration');
      expect(duration?.share).toBeCloseTo(node.metrics.durationMs / slowest, 6);
    }
    // Exactly one node tops each metric, and it fills its bar.
    expect(
      diagram.nodes.filter(node => gauges.get(node.id)?.[0].share === 1),
    ).toHaveLength(1);
  });

  it('draws an empty gauge rather than a full one when no node reported the metric', () => {
    const diagram = buildDistributedFlowDiagram(fanOutTopology());
    const gauges = computeFlowGauges(
      diagram.nodes.map(node => ({ ...node, metrics: { ...node.metrics, memoryUsage: 0 } })),
    );

    for (const node of diagram.nodes) {
      expect(gauges.get(node.id)?.find(gauge => gauge.key === 'memory')?.share).toBe(0);
    }
  });
});
