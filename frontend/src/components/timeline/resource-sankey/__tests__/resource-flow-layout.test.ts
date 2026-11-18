import { describe, expect, it } from 'vitest';
import { groupResourceFlow, type ResourceConsumer } from '../resource-exploration-model';
import { buildResourceFlowLayout } from '../resource-flow-layout';

function consumer(id: string, value: number, failureCode = 0, table = 'db.a'): ResourceConsumer {
  return {
    id, value, failureCode, group: 'SELECT', kind: 'query', tables: [table], tableKey: JSON.stringify([table]), tableLabel: table,
    label: id, basis: 'sampled', source: {
      query_id: id, query_kind: 'Select', label: id, tables: [table], user: 'default',
      start_time: '2026-11-18T09:00:00Z', end_time: '2026-11-18T09:01:00Z', duration_ms: 60000,
      cpu_us: value, peak_memory: 0, disk_read: 0, disk_write: 0, net_send: 0, net_recv: 0,
    },
  };
}

describe('resource flow layout', () => {
  it('conserves total and failed usage when limiting mixed-outcome leaves', () => {
    const rows = [consumer('failed-large', 20, 241), consumer('ok', 10), consumer('failed-small', 6, 241), consumer('ok-small', 4)];
    const groups = groupResourceFlow(rows, 'activity');
    const layout = buildResourceFlowLayout(groups, { groupBy: 'activity', selection: { primaryId: 'SELECT', groupId: groups[0].id }, scaleReference: 100, operationLimit: 1 });
    const branch = layout.primaries[0].tables[0];
    expect(layout.total).toBe(40);
    expect(layout.sourceHeight).toBeCloseTo(230 * 40 / 100);
    expect(branch.leaves.map(l => [l.id, l.value, l.failureValue])).toEqual([
      ['remainder', 20, 6], ['failed-large', 20, 20],
    ]);
    expect(branch.leaves.reduce((sum, leaf) => sum + leaf.height, 0)).toBeCloseTo(branch.height);
    expect(branch.leaves.reduce((sum, leaf) => sum + leaf.failureValue, 0)).toBe(branch.failureValue);
    expect(groups[0].rows.map(row => row.id)).toEqual(rows.map(row => row.id));
  });
  it('keeps table colours stable when usage ranking changes', () => {
    const options = { groupBy: 'table' as const, selection: null, scaleReference: 10, operationLimit: 5 };
    const before = buildResourceFlowLayout(groupResourceFlow([consumer('a', 2), consumer('b', 8, 0, 'db.b')], 'table'), options);
    const after = buildResourceFlowLayout(groupResourceFlow([consumer('a', 8), consumer('b', 2, 0, 'db.b')], 'table'), options);
    expect(before.primaries.map(node => node.primaryId)).toEqual(after.primaries.map(node => node.primaryId).reverse());
    for (const node of before.primaries) expect(after.primaries.find(next => next.primaryId === node.primaryId)?.color).toBe(node.color);
    expect(before.primaries[0].color).not.toBe(before.primaries[1].color);
    expect(before.primaries.every(node => node.tables.length === 0)).toBe(true);
  });
  it('uses the same total and scale in both pivots and emits no hidden leaves', () => {
    const rows = [consumer('a', 2), consumer('b', 8, 0, 'db.b')];
    const activity = buildResourceFlowLayout(groupResourceFlow(rows, 'activity'), { groupBy: 'activity', selection: null, scaleReference: 20, operationLimit: Infinity });
    const table = buildResourceFlowLayout(groupResourceFlow(rows, 'table'), { groupBy: 'table', selection: null, scaleReference: 20, operationLimit: Infinity });
    expect(activity.total).toBe(table.total);
    expect(activity.sourceHeight).toBe(table.sourceHeight);
    expect(activity.primaries.flatMap(node => node.tables).every(node => node.leaves.length === 0)).toBe(true);
  });
  it('uses operation-type colours after a table is expanded', () => {
    const query = consumer('query', 2);
    const merge: ResourceConsumer = { ...consumer('merge', 3), group: 'MERGE', kind: 'merge',
      source: { ...query.source, table: 'db.a', part_name: 'part-1' } };
    const groups = groupResourceFlow([query, merge], 'table');
    const collapsed = buildResourceFlowLayout(groups, { groupBy: 'table', selection: null, scaleReference: 10, operationLimit: 5 });
    const expanded = buildResourceFlowLayout(groups, { groupBy: 'table', selection: { primaryId: groups[0].primaryId }, scaleReference: 10, operationLimit: 5 });
    expect(expanded.primaries[0].color).toBe(collapsed.primaries[0].color);
    const branches = expanded.primaries[0].tables;
    expect(branches.find(b => b.secondaryLabel === 'SELECT')?.color).toBe('#3b82f6');
    expect(branches.find(b => b.secondaryLabel === 'MERGE')?.color).toBe('#f0883e');
  });
});
