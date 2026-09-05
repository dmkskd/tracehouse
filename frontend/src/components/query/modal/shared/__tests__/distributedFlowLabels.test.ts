import { describe, expect, it } from 'vitest';
import {
  edgeLabel,
  isDispatcher,
  nodeDetails,
  nodeIdentity,
  nodeMetricFacts,
  nodeRoleLabel,
} from '../distributedFlowLabels';
import type { DistributedFlowEdge, DistributedFlowNode } from '../distributedFlowLayout';

function node(overrides: Partial<DistributedFlowNode> = {}): DistributedFlowNode {
  return {
    id: 'n1',
    queryId: 'q1',
    hostname: 'chi-dev-cluster-dev-0-1.clickhouse.svc.cluster.local',
    hostLabel: 'chi-dev-cluster-dev-0-1',
    role: 'replica_reader',
    shardNum: 1,
    replicaNum: 2,
    shapeKey: 'shape',
    shapeLabel: 'synthetic_data.events',
    column: 1,
    subColumn: 0,
    x: 0,
    y: 0,
    isCoordinator: false,
    isFolded: false,
    hasError: false,
    ...overrides,
    metrics: {
      durationMs: 458,
      readRows: 129_282_941,
      readBytes: 433_790_000,
      resultRows: 28,
      resultBytes: 44_600,
      memoryUsage: 1_290_000,
      ...overrides.metrics,
    },
  };
}

function edge(overrides: Partial<DistributedFlowEdge> = {}): DistributedFlowEdge {
  return {
    id: 'e1',
    sourceId: 'n0',
    targetId: 'n1',
    kind: 'dispatch',
    path: 'M 0,0 C 1,1 2,2 3,3',
    labelX: 0,
    labelY: 0,
    ...overrides,
  };
}

describe('nodeDetails', () => {
  it('lists one metric per line in stripe order: duration, memory, rows', () => {
    const [duration, memory, rows] = nodeDetails(node());

    expect(duration).toBe('458ms');
    expect(memory).toContain('MB');
    expect(rows).toBe('129,282,941 rows');
  });

  it('drops a metric the participant never reported rather than printing zero', () => {
    // A folded local read has no duration or memory of its own. "0ms" would be
    // a claim about how fast it was, not an absence of data.
    const details = nodeDetails(node({ metrics: { ...node().metrics, durationMs: 0, memoryUsage: 0 } }));

    expect(details).toEqual(['129,282,941 rows']);
  });

  it('says so when a participant read nothing at all', () => {
    const details = nodeDetails(node({ metrics: { ...node().metrics, readRows: 0 } }));

    expect(details[details.length - 1]).toBe('no rows read');
  });

  it('writes a single row in the singular', () => {
    const details = nodeDetails(node({ metrics: { ...node().metrics, readRows: 1 } }));

    expect(details[details.length - 1]).toBe('1 row');
  });
});

describe('edgeLabel', () => {
  it('carries what the target returned, not what it read', () => {
    // The edge measures its own link. The 129M rows the target scanned never
    // left the shard, and putting them here would say that they did.
    expect(edgeLabel(edge({ returnedBytes: 44_600, returnedRows: 28 }))).toBe('43.55 KB · 28 rows');
  });

  it('draws bare when query_log carried no result counters', () => {
    expect(edgeLabel(edge())).toBe('');
  });

  it('keeps the surviving half when only one counter is known', () => {
    expect(edgeLabel(edge({ returnedRows: 1 }))).toBe('1 row');
  });
});

describe('nodeMetricFacts', () => {
  it('names one value per row, in the same order as the cube labels', () => {
    const facts = nodeMetricFacts(node().metrics);

    expect(facts.map(fact => fact.label)).toEqual([
      'duration', 'memory', 'rows read', 'bytes read', 'rows returned', 'bytes returned',
    ]);
    expect(facts[0].value).toBe('458ms');
    expect(facts[2].value).toBe('129,282,941');
  });

  it('omits counters the caller does not have', () => {
    // The timeline knows only the three basics, so its panel must not sprout
    // empty "bytes read" and "rows returned" rows.
    const facts = nodeMetricFacts({ durationMs: 10, memoryUsage: 2048, readRows: 55 });

    expect(facts.map(fact => fact.label)).toEqual(['duration', 'memory', 'rows read']);
  });

  it('lets a caller format row counts its own way', () => {
    const facts = nodeMetricFacts(
      { durationMs: 10, memoryUsage: 2048, readRows: 64_980_417 },
      { formatRows: () => '65.0M' },
    );

    expect(facts[2].value).toBe('65.0M');
  });

  it('appends parts when the read distribution reported them', () => {
    const facts = nodeMetricFacts({ ...node().metrics, selectedParts: 12 });

    expect(facts[facts.length - 1]).toEqual({ label: 'parts', value: '12' });
  });
});

describe('node naming', () => {
  it('names a placed participant by its cluster coordinate', () => {
    expect(nodeIdentity(node())).toEqual({ label: 's1r2', placed: true });
  });

  it('falls back to the hostname, marked unplaced, when there is no coordinate', () => {
    // The caller renders an unplaced name differently: "we could not place this
    // host" must not look like "this host is called s1r2".
    expect(nodeIdentity(node({ shardNum: undefined, replicaNum: undefined }))).toEqual({
      label: 'chi-dev-cluster-dev-0-1',
      placed: false,
    });
  });

  it('treats coordinators and shard leaders as dispatchers', () => {
    expect(isDispatcher(node({ isCoordinator: true }))).toBe(true);
    expect(isDispatcher(node({ role: 'shard_leader' }))).toBe(true);
    expect(isDispatcher(node())).toBe(false);
  });

  it('names a folded read by the replica it happened on', () => {
    expect(nodeRoleLabel(node({ isFolded: true }))).toBe('Local read · s1r2');
    expect(nodeRoleLabel(node({ isFolded: true, shardNum: undefined }))).toBe('Local read');
  });
});
