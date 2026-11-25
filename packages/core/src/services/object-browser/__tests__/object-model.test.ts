import { describe, expect, it } from 'vitest';
import type { IClickHouseAdapter } from '../../../adapters/types.js';
import { accessFixture } from '../fixtures/access.js';
import { collectObjects } from '../collectors/collect.js';
import { projectGraph, projectCatalogGraph, provenancePaths } from '../projections/graph.js';
import { objectKey } from '../model/types.js';
import type { Source } from '../model/types.js';
import { ACCESS_TABLE_DEPENDENCY_FIELDS } from '../../../queries/access-queries.js';

const input = { connection: 'test', principal: 'alice', operation: 'SELECT' as const, database: 'demo', table: 'trips' };
function setup() {
  const fixture = accessFixture();const denied = new Set<string>();const sql: string[] = [];
  const adapter = { executeQuery: async (query: string) => {
    sql.push(query);
    const source = /TraceHouse:Objects:(\w+)/.exec(query)?.[1];
    if (!source || denied.has(source)) throw Error('Observer lacks source privilege');
    if (source === 'hosts') return fixture.hosts.map(host => ({ host, version: '26.10.7.24' }));
    if (source === 'schema') return fixture.schema;
    if (source === 'privileges') return [{ privilege: 'SELECT', level: 'COLUMN', parent_group: 'ALL' }];
    return fixture.rows[source as Source];
  } } as IClickHouseAdapter;
  return { fixture, denied, sql, collect: (limit?: number) => collectObjects(adapter, input, { limit }), adapter };
}

describe('host-aware access provenance', () => {
  it('coalesces four identical entities while retaining every record and host', async () => {
    const s = setup();const capture = await s.collect(),graph = projectGraph(capture);
    const user = graph.nodes.find(n => n.kind === 'user')!;
    expect(user.records).toHaveLength(4);expect(user.differs).toBe(false);expect(user.partial).toBe(false);
    expect(user.observedHosts).toEqual(s.fixture.hosts);
    expect(graph.nodes.filter(n => n.kind === 'role')).toHaveLength(1);
    expect(capture.verdict.kind).toBe('not-checked');
    expect(provenancePaths(graph, objectKey('test','user','alice'),objectKey('test','table','demo','trips'))[0].hosts).toHaveLength(4);
  });
  it('marks both attributes and owned grant changes without merging host evidence', async () => {
    const s = setup();s.fixture.rows.users[3].default_roles_list = [];
    s.fixture.rows.grants[3].grant_option = 1;
    const graph = projectGraph(await s.collect());
    expect(graph.nodes.find(n => n.kind === 'user')!.differs).toBe(true);
    expect(graph.nodes.find(n => n.kind === 'role')!.differs).toBe(true);
    expect(graph.edges.filter(e => e.kind === 'grant')).toHaveLength(2);
  });
  it('does not call three observed hosts identical across four expected hosts', async () => {
    const s = setup();s.fixture.rows.users.pop();const graph = projectGraph(await s.collect());
    expect(graph.nodes.find(n => n.kind === 'user')!.partial).toBe(true);
    expect(graph.nodes.find(n => n.kind === 'user')!.differs).toBe(false);
    expect(graph.nodes.find(n => n.kind === 'user')!.observedHosts).toHaveLength(3);
    expect(graph.nodes.find(n => n.kind === 'user')!.expectedHosts).toHaveLength(4);
  });
  it('does not build a path out of assignments and grants on disjoint hosts', async () => {
    const s = setup();s.fixture.rows.role_grants = s.fixture.rows.role_grants.filter(r => r.host === 'ch-1');
    s.fixture.rows.grants = s.fixture.rows.grants.filter(r => r.host === 'ch-2');
    const graph = projectGraph(await s.collect());
    expect(provenancePaths(graph,objectKey('test','user','alice'),objectKey('test','table','demo','trips'))).toEqual([]);
  });
  it('keeps revokes as evidence and never derives an authorization verdict', async () => {
    const s = setup();s.fixture.rows.grants[0].is_partial_revoke = 1;
    const capture = await s.collect();expect(capture.verdict.kind).toBe('not-checked');
    expect(capture.relationships.some(e => e.label.includes('partial revoke'))).toBe(true);
  });
  it('keeps restricted observer failures and unsupported schemas distinct from empty data', async () => {
    const s = setup();s.denied.add('grants');s.fixture.schema = s.fixture.schema.filter(r => !(r.table === 'users' && r.name === 'default_roles_except'));
    const capture = await s.collect();
    expect(capture.coverage.find(c => c.source === 'grants')!.state).toBe('failed');
    expect(capture.coverage.find(c => c.source === 'users')!.state).toBe('schema-unavailable');
    expect(s.sql.filter(q => q.includes('TraceHouse:Objects:users'))).toHaveLength(0);
    expect(capture.verdict.kind).toBe('not-checked');
  });
  it('marks source bounds and stops issuing queries after disposal', async () => {
    const s = setup();const capture = await s.collect(1);
    expect(capture.coverage.find(c => c.source === 'users')!.state).toBe('truncated');
    const cancellation = { aborted: true };
    const before = s.sql.length;
    await expect(collectObjects(s.adapter,input,{ cancellation })).rejects.toThrow('disposed');
    expect(s.sql).toHaveLength(before);
  });
});

describe('native materialized-view dependencies', () => {
  function cascade() {
    const s = setup();
    for (const host of s.fixture.hosts) {
      for (const name of ACCESS_TABLE_DEPENDENCY_FIELDS) s.fixture.schema.push({ host, table: 'tables', name });
      const row = s.fixture.rows.tables.find(r => r.host === host)!;
      Object.assign(row, { dependencies_database: ['demo'], dependencies_table: ['first_mv'], target_database: '', target_table: '', as_select: '' });
      for (const [name, engine, dependent, target] of [
        ['first_mv','MaterializedView','','minute'], ['minute','SummingMergeTree','second_mv',''],
        ['second_mv','MaterializedView','','hourly'], ['hourly','SummingMergeTree','',''],
      ]) s.fixture.rows.tables.push({ ...row, name, engine, uuid: name,
        dependencies_database: dependent ? ['demo'] : [], dependencies_table: dependent ? [dependent] : [],
        target_database: target ? 'demo' : '', target_table: target });
    }
    return s;
  }
  it('collects both native trigger and write stages with host-preserved evidence', async () => {
    const s = cascade();const capture = await collectObjects(s.adapter, input, { mode: 'catalog' });
    const edges = projectGraph(capture).edges.filter(edge => edge.kind === 'insert-trigger' || edge.kind === 'writes-to');
    expect(edges).toHaveLength(4);
    expect(edges.every(edge => edge.hosts.length === 4)).toBe(true);
    expect(edges.map(edge => [JSON.parse(edge.from).at(-1), edge.kind, JSON.parse(edge.to).at(-1)])).toEqual(expect.arrayContaining([
      ['trips','insert-trigger','first_mv'], ['first_mv','writes-to','minute'],
      ['minute','insert-trigger','second_mv'], ['second_mv','writes-to','hourly'],
    ]));
    expect(capture.verdict.kind).toBe('not-checked');
  });
  it('keeps source triggers but explicitly reports unavailable destination fields', async () => {
    const s = cascade();s.fixture.schema = s.fixture.schema.filter(row => row.name !== 'target_table' && row.name !== 'target_database');
    const capture = await collectObjects(s.adapter, input, { mode: 'catalog' });
    expect(capture.relationships.some(edge => edge.kind === 'insert-trigger')).toBe(true);
    expect(capture.relationships.some(edge => edge.kind === 'writes-to')).toBe(false);
    expect(capture.coverage.find(source => source.source === 'tables')!.reason).toContain('target_table');
  });
  it('does not join malformed pairs or materialized views observed only on another host', async () => {
    const s = cascade();s.fixture.rows.tables = s.fixture.rows.tables.filter(row => !(row.host === 'ch-1' && row.name === 'first_mv'));
    Object.assign(s.fixture.rows.tables.find(row => row.host === 'ch-2' && row.name === 'trips')!, { dependencies_database: [] });
    const capture = await collectObjects(s.adapter, input, { mode: 'catalog' });
    const trigger = projectGraph(capture).edges.find(edge => edge.kind === 'insert-trigger' && JSON.parse(edge.from).at(-1) === 'trips')!;
    expect(trigger.hosts).toEqual(['ch-3','ch-4']);
    expect(capture.coverage.find(source => source.source === 'tables')!.reason).toContain('Malformed');
  });
});

it('retains unresolved references when the role inventory cannot be read', async () => {
  const s = setup();s.denied.add('roles');const capture = await s.collect();
  const graph = projectGraph(capture), role = graph.nodes.find(n => n.kind === 'role')!;
  expect(role.records.every(r => r.attributes.unresolvedReference)).toBe(true);
  expect(role.partial).toBe(true);
  expect(graph.edges.some(e => e.kind === 'has-role')).toBe(true);
});

it('does not normalize malformed selector arrays into empty selectors', async () => {
  const s = setup();s.fixture.rows.users[0].default_roles_list = 'unexpected';
  const capture = await s.collect();
  expect(capture.coverage.find(c => c.source === 'users')!.state).toBe('failed');
});

it('rejects stale in-flight results and does not schedule the next probe', async () => {
  const cancellation = { aborted: false };let calls = 0;
  const adapter = { executeQuery: async () => { calls++;cancellation.aborted = true;return []; } } as IClickHouseAdapter;
  await expect(collectObjects(adapter,input,{ cancellation })).rejects.toThrow('disposed');
  expect(calls).toBe(1);
});

it('keeps per-host native IDs in the records without flagging them, and matches supported prefix scopes', async () => {
  const s = setup();s.fixture.rows.roles[3].id = 'different-native-id';
  s.fixture.rows.grants.forEach(r => { r.table = 'trip';r.is_wildcard = 1; });
  const graph = projectGraph(await s.collect());
  expect(graph.nodes.filter(n => n.kind === 'role')).toHaveLength(1);
  const role = graph.nodes.find(n => n.kind === 'role')!;
  expect(role.records.some(r => r.attributes.id === 'different-native-id')).toBe(true);
  expect(role.differs).toBe(false);
  expect(graph.edges.some(e => e.kind === 'grant')).toBe(true);
  expect(graph.edges.some(e => e.kind === 'candidate-scope')).toBe(true);
});

it('normalizes known selector sets but does not rewrite expression text', async () => {
  const s = setup();s.fixture.rows.users[0].host_names = ['b','a'];
  for (const r of s.fixture.rows.users.slice(1)) r.host_names = ['a','b'];
  s.fixture.rows.row_policies[3].select_filter = 'tenant_id  = 42';
  const graph = projectGraph(await s.collect());
  expect(graph.nodes.find(n => n.kind === 'user')!.differs).toBe(false);
  expect(graph.nodes.find(n => n.kind === 'row-policy')!.differs).toBe(true);
});


describe('catalog browsing', () => {
  it('retains users outside the scoped principal and keys each table independently', async () => {
    const s = setup();
    s.fixture.rows.users.push({ ...s.fixture.rows.users[0], name: 'bob', id: 'bob-id' });
    s.fixture.rows.tables.push({ ...s.fixture.rows.tables[0], database: 'other', name: 'trips', uuid: 'other-id' });
    const capture = await collectObjects(s.adapter, input, { mode: 'catalog' });
    const graph = projectCatalogGraph(capture);
    expect(capture.mode).toBe('catalog');
    expect(graph.nodes.some(n => n.kind === 'user' && n.name === 'bob')).toBe(true);
    expect(graph.nodes.filter(n => n.kind === 'table').map(n => n.name)).toEqual(['demo.trips','other.trips']);
    expect(s.sql.some(q => q.includes('1 = 1 OR name'))).toBe(true);
  });
  it('expands broad scopes only for a selected table and preserves host intersections', async () => {
    const s = setup();
    const capture = await collectObjects(s.adapter, input, { mode: 'catalog' });
    expect(projectCatalogGraph(capture).edges.some(e => e.kind === 'candidate-scope')).toBe(false);
    const target = objectKey('test', 'table', 'demo', 'trips');
    const graph = projectCatalogGraph(capture, target);
    expect(graph.edges.find(e => e.kind === 'candidate-scope')!.hosts).toHaveLength(4);
    expect(provenancePaths(graph, objectKey('test','user','alice'), target)[0].hosts).toHaveLength(4);
    capture.observations = capture.observations.filter(o => o.kind !== 'table' || o.host === 'ch-1');
    expect(projectCatalogGraph(capture, target).edges.find(e => e.kind === 'candidate-scope')!.hosts).toEqual(['ch-1']);
    expect(capture.relationships.some(e => e.kind === 'candidate-scope')).toBe(false);
  });
});
