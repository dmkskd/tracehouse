import { describe, expect, it } from 'vitest';
import { projectCatalogGraph, projectGraph } from '../projections/graph.js';
import { withoutDefaultUser } from '../projections/filters.js';
import { groupOperations, privilegeGroup, toggleOperation, privilegeTree, tableLevelOperations } from '../model/privileges.js';
import { objectKey } from '../model/types.js';
import type { Attributes, Capture, ObjectKind, Relationship, Source } from '../model/types.js';
import { describeObjects } from '../presentation/cards.js';
import { catalogPageSize, isGap, isWaypoint, layoutCatalog, layoutFocus, routeEdges, withRoutes, withSourceNotices } from '../presentation/layout.js';
import { essentialEdges, focusGraphFrom, searchGraph } from '../projections/focus.js';
import { GUIDES, investigationFor } from '../presentation/guides.js';
import { describeDetail } from '../presentation/details.js';

const DB = 'tracehouse_object_demo';
const key = (kind: ObjectKind, ...name: string[]) => objectKey('test', kind, ...name);
const table = (name: string) => key('table', DB, name);
const SCOPE = key('grant-scope', 'select-demo');

/** Demo-shaped capture: analyst role, a database-wide scope and the two-stage MV cascade. */
function demo(hosts = ['ch-1'], extra: Relationship[] = []): Capture {
  const observations: Capture['observations'] = [];
  const relationships: Relationship[] = [];
  const add = (k: string, kind: ObjectKind, name: string, source: Source, attributes: Attributes) =>
    hosts.forEach(host => observations.push({ key: k, kind, name, host, source, attributes }));
  const rel = (from: string, to: string, kind: Relationship['kind'], source: Source, evidence: Attributes = {}) =>
    hosts.forEach(host => relationships.push({ id: `${host}:${from}:${to}:${kind}`, host, from, to, kind, label: kind.replace('-', ' '), source, evidence }));
  add(key('user', 'th_ob_bob'), 'user', 'th_ob_bob', 'users', { storage: 'local_directory', default_roles_all: 0,
    default_roles_list: ['th_ob_analyst'], default_roles_except: [], host_ip: ['::/0'], host_names: [], host_names_regexp: [], host_names_like: [] });
  add(key('role', 'th_ob_analyst'), 'role', 'th_ob_analyst', 'roles', { storage: 'local_directory' });
  add(SCOPE, 'grant-scope', `SELECT ON ${DB}.*`, 'grants', { access_type: 'SELECT', access_object: '', database: DB, table: null, column: null, is_wildcard: 0,
    table_operations: ['SELECT'], privilege_group: 'SELECT', privilege_level: 'COLUMN' });
  const mergeTree = (name: string, engine: string) => add(table(name), 'table', `${DB}.${name}`, 'tables',
    { database: DB, name, engine, sorting_key: 'tenant_id, minute', partition_key: '', storage_policy: 'default' });
  mergeTree('events', 'MergeTree');mergeTree('events_minute', 'SummingMergeTree');mergeTree('events_hourly', 'SummingMergeTree');
  for (const mv of ['events_to_minute_mv', 'minute_to_hourly_mv']) add(table(mv), 'table', `${DB}.${mv}`, 'tables', { database: DB, name: mv, engine: 'MaterializedView' });
  add(key('row-policy', DB, 'events', 'tenant'), 'row-policy', `tenant ON ${DB}.events`, 'row_policies', { short_name: 'tenant', database: DB,
    table: 'events', select_filter: 'tenant_id = 1', is_restrictive: 0, apply_to_all: 0, apply_to_list: ['th_ob_analyst'], apply_to_except: [] });
  rel(key('user', 'th_ob_bob'), key('role', 'th_ob_analyst'), 'has-role', 'role_grants');
  rel(key('role', 'th_ob_analyst'), SCOPE, 'grant', 'grants', { is_partial_revoke: 0 });
  rel(table('events'), table('events_to_minute_mv'), 'insert-trigger', 'tables');
  rel(table('events_to_minute_mv'), table('events_minute'), 'writes-to', 'tables');
  rel(table('events_minute'), table('minute_to_hourly_mv'), 'insert-trigger', 'tables');
  rel(table('minute_to_hourly_mv'), table('events_hourly'), 'writes-to', 'tables');
  rel(key('row-policy', DB, 'events', 'tenant'), table('events'), 'policy-target', 'row_policies');
  for (const r of extra) rel(r.from, r.to, r.kind, r.source, r.evidence);
  return { schemaVersion: 1, mode: 'catalog', investigation: { connection: 'test', principal: '', database: '', table: '', operation: 'SELECT' },
    startedAt: '', completedAt: '', hosts: hosts.map(host => ({ host, version: '26.9.7.9' })), observations, relationships, coverage:
    (['users', 'roles', 'role_grants', 'grants', 'tables', 'row_policies'] as Source[]).map(source => ({ source, state: 'readable', hosts, visibility: 'unknown' })),
    verdict: { kind: 'not-checked', reason: '' } };
}
const candidate = (to: string) => ({ id: '', host: '', from: SCOPE, to, kind: 'candidate-scope', label: '', source: 'grants', evidence: {} }) as Relationship;

describe('object cards', () => {
  it('describes each type from its own records and relationships', () => {
    const cards = describeObjects(projectGraph(demo(['ch-1'], [candidate(table('events')), candidate(table('events_hourly'))])));
    expect(cards.get(table('events_to_minute_mv'))).toMatchObject({ type: 'materialized-view', title: 'events_to_minute_mv', qualifier: DB,
      label: 'Materialized view', facts: [{ label: 'Reads from', value: 'events' }, { label: 'Writes to', value: 'events_minute' }] });
    expect(cards.get(table('events_minute'))).toMatchObject({ type: 'merge-tree', label: 'SummingMergeTree',
      facts: [{ label: 'ORDER BY', value: 'tenant_id, minute' }] });
    expect(cards.get(key('user', 'th_ob_bob'))!.facts).toEqual([
      { label: 'Default roles', value: 'th_ob_analyst' }, { label: 'Storage', value: 'local_directory' }]);
    expect(cards.get(key('role', 'th_ob_analyst'))!.facts.slice(0, 2)).toEqual([{ label: 'Members', value: '1' }, { label: 'Grants', value: '1' }]);
    expect(cards.get(SCOPE)).toMatchObject({ title: 'SELECT', qualifier: `ON ${DB}.*`, label: 'Privilege', facts: [
      { label: 'Group', value: 'SELECT' }, { label: 'Scope', value: 'database' }, { label: 'Matched tables', value: '2' }] });
    expect(cards.get(key('row-policy', DB, 'events', 'tenant'))).toMatchObject({ title: 'tenant', qualifier: `ON ${DB}.events`, facts: [
      { label: 'Filter', value: 'tenant_id = 1' }, { label: 'Mode', value: 'permissive' }, { label: 'Applies to', value: 'th_ob_analyst' }] });
  });
  it('gives each card one summary line: the type and its most telling fact', () => {
    const cards = describeObjects(projectGraph(demo(['ch-1'], [candidate(table('events')), candidate(table('events_hourly'))])));
    expect(cards.get(table('events_to_minute_mv'))!.summary).toBe('Materialized view · Reads from events');
    expect(cards.get(table('events_minute'))!.summary).toBe(`SummingMergeTree · ${DB}`);
    expect(cards.get(SCOPE)!.summary).toBe(`All tables in ${DB} · 2 matched`);
  });
  it('gives the hover the database, size and parts across hosts, the view query and comment, without flagging size differences', () => {
    const capture = demo(['ch-1', 'ch-2']);
    capture.observations.filter(r => r.key === table('events_minute')).forEach((r, i) =>
      r.attributes = { ...r.attributes, total_rows: String(1000 * (i + 1)), total_bytes: '4096', active_parts: String(3 + i), newest_part: 1700000000 + i, comment: 'Per-minute rollup' });
    capture.observations.filter(r => r.key === table('events_to_minute_mv')).forEach(r =>
      r.attributes = { ...r.attributes, as_select: 'SELECT tenant_id, count() FROM events GROUP BY tenant_id' });
    const cards = describeObjects(projectGraph(capture));
    expect(cards.get(table('events_minute'))!.hover).toEqual({ type: 'MergeTree', context: { label: 'db', value: DB }, title: 'events_minute',
      engine: 'SummingMergeTree', rows: { min: 1000, max: 2000 }, bytes: { min: 4096, max: 4096 }, parts: { min: 3, max: 4 },
      newestPart: 1700000001, comment: 'Per-minute rollup', notes: [] });
    expect(cards.get(table('events_to_minute_mv'))!.hover).toEqual({ type: 'Materialized view', context: { label: 'db', value: DB },
      title: 'events_to_minute_mv', definition: { label: 'Query', text: 'SELECT tenant_id, count() FROM events GROUP BY tenant_id' }, notes: [] });
    expect(cards.get(SCOPE)!.hover.context).toEqual({ value: `ON ${DB}.*` });
  });
  it('shows host coverage only when it carries information', () => {
    expect(describeObjects(projectGraph(demo())).get(table('events'))!.hosts).toBeUndefined();
    const capture = demo(['ch-1', 'ch-2']);
    capture.observations = capture.observations.filter(o => !(o.host === 'ch-2' && o.key === table('events')));
    const card = describeObjects(projectGraph(capture)).get(table('events'))!;
    expect(card.hosts).toBe('1/2 hosts');expect(card.flags).toContain('partial');
  });
});

describe('focus layout', () => {
  it('lays a materialized view cascade out horizontally after the access columns', () => {
    const columns = layoutFocus(projectGraph(demo()));
    expect(columns.map(c => c.keys)).toEqual([[key('user', 'th_ob_bob')], [key('role', 'th_ob_analyst')], [SCOPE],
      [table('events'), key('row-policy', DB, 'events', 'tenant')], [table('events_to_minute_mv')], [table('events_minute')],
      [table('minute_to_hourly_mv')], [table('events_hourly')]]);
    expect(columns.map(c => c.title)).toEqual(['Users', 'Roles', 'Privileges', 'Tables', 'Materialized views', 'Tables', 'Materialized views', 'Tables']);
    expect(columns.map(c => c.band)).toEqual(['access', 'access', 'access', 'data', 'data', 'data', 'data', 'data']);
  });
  it('keeps a chain on one row, leaving gaps where a column has nothing level with it', () => {
    const capture = demo();
    capture.observations.push({ key: table('archive'), kind: 'table', name: `${DB}.archive`, host: 'ch-1', source: 'tables', attributes: { database: DB, name: 'archive', engine: 'MergeTree' } });
    const columns = layoutFocus(projectGraph(capture));
    // archive sorts before events in the first data column, so the whole cascade moves down one row.
    const data = columns.slice(3).map(c => c.keys.map(k => isGap(k) ? 'gap' : k));
    expect(data[0]).toEqual([table('archive'), table('events'), key('row-policy', DB, 'events', 'tenant')]);
    for (const [c, k] of [[1, 'events_to_minute_mv'], [2, 'events_minute'], [3, 'minute_to_hourly_mv'], [4, 'events_hourly']] as const) expect(data[c]).toEqual(['gap', table(k)]);
  });
  it('titles a mixed column by its kinds', () => {
    const capture = demo();
    capture.observations.push({ key: table('events_buffer'), kind: 'table', name: `${DB}.events_buffer`, host: 'ch-1', source: 'tables', attributes: { database: DB, name: 'events_buffer', engine: 'Buffer' } });
    capture.relationships.push({ id: 'b', host: 'ch-1', from: table('events'), to: table('events_buffer'), kind: 'reads-from', label: 'read by', source: 'tables', evidence: {} });
    expect(layoutFocus(projectGraph(capture)).map(c => c.title)).toContain('Buffers and materialized views');
  });
  it('keeps positions when selection adds candidate scope links', () => {
    const before = layoutFocus(projectGraph(demo()));
    expect(layoutFocus(projectGraph(demo(['ch-1'], [candidate(table('events_hourly'))])))).toEqual(before);
  });
  it('ranks a cycle without looping', () => {
    const columns = layoutFocus(projectGraph(demo(['ch-1'], [{ ...candidate(table('events')), from: table('events_hourly'), kind: 'writes-to' }])));
    expect(columns.flatMap(c => c.keys)).toHaveLength(9);
  });
});

describe('catalog layout', () => {
  it('keeps access lanes and lays data objects out by flow, bounding every column', () => {
    const graph = projectGraph(demo());
    const columns = layoutCatalog(graph, null);
    expect(columns.map(c => c.title)).toEqual(['Users', 'Roles', 'Privileges', 'Tables', 'Materialized views', 'Tables', 'Materialized views', 'Tables']);
    expect(columns.map(c => c.band)).toEqual(['access', 'access', 'access', 'data', 'data', 'data', 'data', 'data']);
    // The policy sits under its table, as in focus.
    expect(columns[3].keys).toEqual([table('events'), key('row-policy', DB, 'events', 'tenant')]);
    // One card per column at this bound; the selected policy is kept past it.
    expect(layoutCatalog(graph, null, 8)[3].keys).toEqual([table('events')]);
    expect(layoutCatalog(graph, key('row-policy', DB, 'events', 'tenant'), 8)[3].keys).toEqual([table('events'), key('row-policy', DB, 'events', 'tenant')]);
    // A selected user keeps its whole lineage visible past the bound: its role and that role's privilege.
    const other = graph.nodes.find(n => n.key === SCOPE)!;
    const crowded = { ...graph, nodes: [{ ...graph.nodes[0], key: key('user', 'aaa'), name: 'aaa' }, { ...other, key: key('grant-scope', 'first'), name: 'first' }, ...graph.nodes] };
    const lanes = layoutCatalog(crowded, key('user', 'th_ob_bob'), 5);
    expect(lanes[0].keys).toEqual([key('user', 'aaa'), key('user', 'th_ob_bob')]);
    // One card per lane at this bound: 'first' fills the Privileges lane, and bob's privilege is kept after it.
    expect(lanes[2].keys).toEqual([key('grant-scope', 'first'), SCOPE]);
  });
  it('counts the cards a column hides and shows one more page of them per request', () => {
    const graph = projectGraph(demo());
    const crowded = { ...graph, nodes: [...['aaa', 'bbb', 'ccc'].map(name => ({ ...graph.nodes.find(n => n.kind === 'user')!, key: key('user', name), name })), ...graph.nodes] };
    const users = (pages: number[]) => layoutCatalog(crowded, null, 8, pages)[0];
    // One card per column at this bound.
    expect(catalogPageSize(8)).toBe(1);
    const total = crowded.nodes.filter(n => n.kind === 'user').length;
    expect(users([])).toMatchObject({ total, hidden: total - 1 });
    expect(users([]).keys).toHaveLength(1);
    expect(users([2]).keys).toHaveLength(3);
    expect(users([2])).toMatchObject({ total, hidden: total - 3 });
    // Other columns keep their bound.
    expect(layoutCatalog(crowded, null, 8, [2])[2].keys).toHaveLength(1);
    // Objects to show later move behind the rest of their lane, so the bound keeps the others.
    const first = users([]).keys[0];
    expect(layoutCatalog(crowded, null, 8, [], new Set([first]))[0].keys[0]).not.toBe(first);
  });
});

describe('focus graph', () => {
  it('follows lineage in each direction and attaches policies naming a reached principal', () => {
    const graph = projectGraph(demo(['ch-1'], [{ id: '', host: '', from: key('row-policy', DB, 'events', 'tenant'), to: key('role', 'th_ob_analyst'),
      kind: 'policy-recipient', label: '', source: 'row_policies', evidence: {} }]));
    const keys = (origin: string) => focusGraphFrom(graph, origin).nodes.map(n => n.key).sort();
    // From the role: its user upstream, its scope downstream, and the policy naming it with that policy's table.
    expect(keys(key('role', 'th_ob_analyst'))).toEqual([key('role', 'th_ob_analyst'), key('row-policy', DB, 'events', 'tenant'), SCOPE,
      table('events'), key('user', 'th_ob_bob')].sort());
    // From the last table: only its upstream chain; the role's user is not reached sideways.
    expect(keys(table('events_hourly'))).toEqual([table('events'), table('events_to_minute_mv'), table('events_minute'),
      table('minute_to_hourly_mv'), table('events_hourly'), key('row-policy', DB, 'events', 'tenant')].sort());
  });
  it('searches names and keeps the selected object with its neighbors', () => {
    const graph = projectGraph(demo());
    const found = searchGraph(graph, ' Hourly ', table('events_to_minute_mv'));
    expect(found.matches.map(n => n.key)).toEqual([table('events_hourly'), table('minute_to_hourly_mv')]);
    expect(found.graph.nodes.map(n => n.key).sort()).toEqual([table('events'), table('events_to_minute_mv'), table('events_minute'),
      table('events_hourly'), table('minute_to_hourly_mv')].sort());
    expect(searchGraph(graph, '  ', null).graph).toBe(graph);
  });
});

describe('object detail', () => {
  it('lists type statements, full facts, relationships and records', () => {
    const graph = projectGraph(demo());
    const detail = (k: string) => describeDetail(graph.nodes.find(n => n.key === k)!, graph);
    const statements = (k: string) => detail(k).sections.flatMap(s => s.kind === 'statement' ? [s.statement.sql] : []);
    expect(statements(key('user', 'th_ob_bob'))).toEqual([
      'SHOW CREATE USER `th_ob_bob` SETTINGS format_display_secrets_in_show_and_select = 0', 'SHOW GRANTS FOR `th_ob_bob`']);
    expect(statements(key('row-policy', DB, 'events', 'tenant'))).toEqual([`SHOW CREATE ROW POLICY \`tenant\` ON \`${DB}\`.\`events\``]);
    expect(statements(SCOPE)).toEqual([]);
    const mv = detail(table('events_to_minute_mv'));
    expect(mv.sections.map(s => s.kind)).toEqual(['facts', 'activity', 'statement', 'relationships', 'records']);
    // One row per wording from the view's side; names drop the shared database.
    expect(mv.sections.find(s => s.kind === 'relationships')).toMatchObject({ groups: [{ title: 'Data flow', rows: [
      { relation: 'Triggered by inserts into', items: [{ name: 'events', fullName: `${DB}.events` }] },
      { relation: 'Writes to', items: [{ name: 'events_minute' }] }] }] });
    // A table read through a database-wide SELECT lists it once under the privilege's coverage.
    const events = detail(table('events')).sections.find(s => s.kind === 'relationships');
    expect(events?.kind === 'relationships' && events.groups.find(g => g.title === 'Access')?.rows.map(r => r.relation)).toContain('Row policies');
  });
  it('names the fields that differ between hosts and which hosts hold each value, ignoring per-host ids and sizes', () => {
    const capture = demo(['ch-1', 'ch-2', 'ch-3']);
    capture.observations.filter(r => r.key === table('events')).forEach((r, i) =>
      r.attributes = { ...r.attributes, uuid: `uuid-${i}`, total_rows: String(i), sorting_key: i === 2 ? 'tenant_id' : 'tenant_id, minute' });
    const graph = projectGraph(capture);
    const node = graph.nodes.find(n => n.key === table('events'))!;
    expect(node.differences).toEqual(['sorting_key']);
    expect(describeDetail(node, graph).sections[0]).toEqual({ kind: 'facts', title: 'Differs across hosts', facts: [
      { label: 'sorting_key', value: 'ch-1, ch-2: tenant_id, minute' }, { label: '', value: 'ch-3: tenant_id' }] });
    expect(graph.nodes.find(n => n.key === table('events_minute'))!.differs).toBe(false);
  });
});

describe('broad scope expansion for a focus object', () => {
  const globalGrant = (): Relationship => ({ id: '', host: '', from: key('user', 'th_ob_bob'), to: SCOPE, kind: 'grant', label: '', source: 'grants',
    evidence: { access_type: 'SELECT', access_object: '', database: null, table: null, is_wildcard: 0, is_partial_revoke: 0 } });
  it('links a principal focus to its global scope tables, capped with the full count', () => {
    const capture = demo(['ch-1'], [globalGrant()]);
    expect(projectCatalogGraph(capture).edges.some(e => e.kind === 'candidate-scope')).toBe(false);
    const graph = projectCatalogGraph(capture, key('user', 'th_ob_bob'), 2);
    expect(new Set(graph.edges.filter(e => e.kind === 'candidate-scope').map(e => e.to)).size).toBe(2);
    expect(graph.scopeMatches).toEqual({ [SCOPE]: 5 });
    expect(describeObjects(graph).get(SCOPE)!.facts).toContainEqual({ label: 'Matched tables', value: '2 of 5' });
    expect(projectCatalogGraph(capture, key('user', 'th_ob_bob')).scopeMatches).toBeUndefined();
  });
});

describe('hide default user', () => {
  it('drops the default user and scopes only it holds, keeping shared scopes', () => {
    const own = key('grant-scope', 'default-only');
    const capture = demo(['ch-1'], [
      { id: '', host: '', from: key('user', 'default'), to: own, kind: 'grant', label: '', source: 'grants', evidence: {} },
      { id: '', host: '', from: key('user', 'default'), to: SCOPE, kind: 'grant', label: '', source: 'grants', evidence: {} },
    ]);
    capture.observations.push({ key: key('user', 'default'), kind: 'user', name: 'default', host: 'ch-1', source: 'users', attributes: {} },
      { key: own, kind: 'grant-scope', name: 'own', host: 'ch-1', source: 'grants', attributes: {} });
    const keys = withoutDefaultUser(projectGraph(capture)).nodes.map(n => n.key);
    expect(keys).not.toContain(key('user', 'default'));expect(keys).not.toContain(own);expect(keys).toContain(SCOPE);
  });
});

describe('privilege groups', () => {
  // Rows from system.privileges on ClickHouse 26.9.7.9.
  const tree = privilegeTree([
    { privilege: 'SHOW TABLES', level: 'TABLE', parent_group: 'SHOW' }, { privilege: 'SHOW', level: null, parent_group: 'ALL' },
    { privilege: 'SHOW USERS', level: 'GLOBAL', parent_group: 'SHOW ACCESS' }, { privilege: 'SHOW ACCESS', level: null, parent_group: 'ACCESS MANAGEMENT' },
    { privilege: 'ACCESS MANAGEMENT', level: null, parent_group: 'ALL' }, { privilege: 'SELECT', level: 'COLUMN', parent_group: 'ALL' },
    { privilege: 'READ', level: 'SOURCE', parent_group: 'ALL' }, { privilege: 'ALL', level: null, parent_group: null },
  ]);
  it('files each privilege under its ancestor just below ALL', () => {
    expect(privilegeGroup(tree, 'SHOW TABLES')).toBe('SHOW');
    expect(privilegeGroup(tree, 'SHOW USERS')).toBe('ACCESS MANAGEMENT');
    expect(privilegeGroup(tree, 'SELECT')).toBe('SELECT');
    expect(privilegeGroup(tree, 'READ')).toBe('READ');
    expect(privilegeGroup(tree, 'ALL')).toBe('ALL');
    expect(privilegeGroup(tree, 'UNKNOWN')).toBeNull();
    expect(tree.get('READ')!.level).toBe('SOURCE');
  });
  it('lists the privileges that act on one table, with their group', () => {
    expect(tableLevelOperations(tree)).toEqual([{ name: 'SHOW TABLES', group: 'SHOW' }, { name: 'SELECT', group: 'SELECT' }]);
    expect(groupOperations([{ name: 'ALTER UPDATE', group: 'ALTER' }, { name: 'DROP TABLE', group: 'DROP' }, { name: 'ALTER DELETE', group: 'ALTER' }, { name: 'OPTIMIZE', group: null }]))
      .toEqual([{ group: 'ALTER', names: ['ALTER UPDATE', 'ALTER DELETE'] }, { group: 'DROP', names: ['DROP TABLE'] }, { group: 'OPTIMIZE', names: ['OPTIMIZE'] }]);
    expect(toggleOperation(['SELECT'], 'INSERT')).toEqual(['SELECT', 'INSERT']);
    expect(toggleOperation(['SELECT', 'INSERT'], 'SELECT')).toEqual(['INSERT']);
    // The last selected operation stays.
    expect(toggleOperation(['SELECT'], 'SELECT')).toEqual(['SELECT']);
  });
});

describe('guided questions', () => {
  it('offers candidates per question, counts the catalog and builds a provenance capture', () => {
    const graph = projectGraph(demo());
    const guide = (id: string) => GUIDES.find(g => g.id === id)!;
    const names = (id: string, pick = 0) => guide(id).picks[pick].candidates(graph).map(n => n.key);
    expect(names('row-policies')).toEqual([table('events')]);
    expect(names('principal-reach')).toEqual([key('user', 'th_ob_bob'), key('role', 'th_ob_analyst')]);
    expect(names('data-flow')).toHaveLength(5);
    expect(names('storage')).toEqual([]);
    expect(GUIDES.map(g => g.signal(graph))).toEqual(['5 tables · 1 privilege', '1 user · 1 role', '4 links between tables',
      '0 storage servers · 0 tables', '1 row policy', 'SELECT · paths per host']);
    const [user, events] = [key('user', 'th_ob_bob'), table('events')].map(k => graph.nodes.find(n => n.key === k)!);
    expect(investigationFor(user, events)).toEqual({ principal: 'th_ob_bob', database: DB, table: 'events', operation: 'SELECT' });
  });
});

describe('diagram options', () => {
  it('keeps only essential lines: a database-wide privilege ends in a stub, its tables stay', () => {
    const graph = projectGraph(demo(['ch-1'], [candidate(table('events')), candidate(table('events_hourly'))]));
    const essential = essentialEdges(graph);
    expect(essential.stubs).toEqual([SCOPE]);
    expect(essential.graph.edges.some(e => e.kind === 'candidate-scope')).toBe(false);
    expect(essential.graph.nodes).toEqual(graph.nodes);
  });
  it('routes a line that skips columns through a waypoint in each column it crosses', () => {
    // The scope links to the last table, four columns to its right.
    const graph = projectGraph(demo(['ch-1'], [candidate(table('events_hourly'))]));
    const { columns, routes } = routeEdges(graph, layoutFocus(graph));
    const edge = graph.edges.find(e => e.kind === 'candidate-scope')!;
    expect(routes.get(edge.key)).toHaveLength(4);
    const crossed = columns.map(c => c.keys.filter(isWaypoint).length);
    expect(crossed).toEqual([0, 0, 0, 1, 1, 1, 1, 0]);
    // Adjacent links get no waypoint, and every card keeps its column.
    expect(routes.size).toBe(1);
    expect(columns.map(c => c.keys.filter(k => !isWaypoint(k)))).toEqual(layoutFocus(graph).map(c => c.keys));
    // With routing off the layout passes through unchanged.
    expect(withRoutes(graph, layoutFocus(graph), false)).toEqual({ columns: layoutFocus(graph), routes: new Map() });
    expect(withRoutes(graph, layoutFocus(graph), true).routes.size).toBe(1);
  });
});

describe('source notices', () => {
  it('explains each not-granted source in the column its objects would fill', () => {
    const table = { key: objectKey('c', 'table', 'db', 't'), kind: 'table' as ObjectKind, name: 'db.t', records: [], observedHosts: [], expectedHosts: [], differs: false, partial: false };
    const columns = layoutCatalog({ nodes: [table], edges: [] }, null);
    const missing = (source: Source) => ({ source, state: 'not-granted' as const, hosts: [], visibility: 'unknown' as const, reason: `Missing SELECT permission on system.${source}` });
    const noticed = withSourceNotices(columns, [missing('users'), missing('role_grants'), missing('grants'), missing('row_policies'),
      { source: 'tables', state: 'truncated', hosts: [], visibility: 'unknown', reason: 'Row limit 1000 reached; more rows exist.' }]);
    expect(noticed.map(column => [column.title, column.notices ?? []])).toEqual([
      ['Users', ['Missing SELECT permission on system.users']],
      ['Roles', ['Missing SELECT permission on system.role_grants']],
      ['Privileges', ['Missing SELECT permission on system.grants']],
      // Row policies sit with the tables they filter; other coverage stays out of the columns.
      [columns[3].title, ['Missing SELECT permission on system.row_policies']],
    ]);
  });
});
