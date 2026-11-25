import { describe, expect, it } from 'vitest';
import { projectGraph } from '../projections/graph.js';
import { describeDetail } from '../presentation/details.js';
import { describeObjects } from '../presentation/cards.js';
import { focusGraphFrom } from '../projections/focus.js';
import { describeMissingLink, describeProvenance, provenanceState, tableKeyOf, summarizePrincipalPaths, objectLinkFor, objectLinkUrl, parseObjectLink, resolveObjectLink, withPrincipalContext, type ObjectLink } from '../presentation/links.js';
import type { Capture, ObjectKind, Relationship, Source } from '../model/types.js';

const record = (key: string, kind: ObjectKind, source: Source, attributes: Record<string, unknown> = {}) =>
  ({ key, name: key, kind, source, host: 'ch-1', attributes });
const edge = (from: string, to: string, kind: Relationship['kind'], source: Source): Relationship =>
  ({ id: `${from}>${to}`, host: 'ch-1', from, to, kind, label: kind, source, evidence: {} });
// alice and bob share the reader role; writer holds the same privilege; tenant-filter names alice.
const capture: Capture = {
  schemaVersion: 1, mode: 'catalog', investigation: { connection: 'c', principal: '', database: '', table: '', operation: 'SELECT' },
  startedAt: '', completedAt: '', hosts: [{ host: 'ch-1', version: '26.9.7.9' }], coverage: [], verdict: { kind: 'not-checked', reason: '' },
  observations: [record('alice', 'user', 'users'), record('bob', 'user', 'users'), record('carol', 'user', 'users'),
    record('reader', 'role', 'roles'), record('writer', 'role', 'roles'), record('scope', 'grant-scope', 'grants'),
    { ...record('trips', 'table', 'tables', { database: 'demo', name: 'trips' }), name: 'demo.trips' },
    { ...record('daily', 'table', 'tables', { database: 'demo', name: 'daily' }), name: 'demo.daily' },
    record('tenant-filter', 'row-policy', 'row_policies'), record('bob-filter', 'row-policy', 'row_policies')],
  relationships: [edge('alice', 'reader', 'has-role', 'role_grants'), edge('bob', 'reader', 'has-role', 'role_grants'),
    edge('reader', 'scope', 'grant', 'grants'), edge('writer', 'scope', 'grant', 'grants'), edge('scope', 'trips', 'candidate-scope', 'grants'),
    edge('trips', 'daily', 'insert-trigger', 'tables'),
    edge('tenant-filter', 'alice', 'policy-recipient', 'row_policies'), edge('tenant-filter', 'trips', 'policy-target', 'row_policies'),
    edge('bob-filter', 'bob', 'policy-recipient', 'row_policies'), edge('bob-filter', 'trips', 'policy-target', 'row_policies')],
};

describe('object links', () => {
  it('reads back the URL it writes and resolves names to graph keys', () => {
    const url = objectLinkUrl({ kind: 'table', database: 'demo', table: 'trips', user: 'alice' });
    const link = parseObjectLink(name => new URLSearchParams(url.split('?')[1]).get(name)) as Extract<ObjectLink, { kind: 'table' }>;
    expect(link).toEqual({ kind: 'table', database: 'demo', table: 'trips', user: 'alice' });
    expect(resolveObjectLink(projectGraph(capture), link)).toEqual({ focus: 'trips', context: 'alice' });
    expect(resolveObjectLink(projectGraph(capture), { ...link, table: 'gone', user: 'nobody' }))
      .toEqual({ missing: [{ kind: 'table', name: 'demo.gone' }, { kind: 'user', name: 'nobody' }] });
    expect(parseObjectLink(name => ({ ob_kind: 'table', ob_db: 'demo' } as Record<string, string>)[name])).toBeNull();
  });
  it('narrows a table focus to one user: its path and its row policies, data flow unchanged', () => {
    const focus = focusGraphFrom(projectGraph(capture), 'trips');
    const narrowed = withPrincipalContext(focus, 'alice', 'trips');
    expect(narrowed.paths).toBe(1);
    expect(narrowed.graph.nodes.map(n => n.key).sort()).toEqual(['alice', 'daily', 'reader', 'scope', 'tenant-filter', 'trips']);
    expect(narrowed.graph.edges.map(e => `${e.from}>${e.to}`).sort()).toEqual(['alice>reader', 'reader>scope', 'scope>trips',
      'tenant-filter>alice', 'tenant-filter>trips', 'trips>daily']);
    // A user with no configured path leaves no access objects.
    const none = withPrincipalContext(focus, 'carol', 'trips');
    expect(none.paths).toBe(0);
    expect(none.graph.nodes.map(n => n.key).sort()).toEqual(['daily', 'trips']);
  });
  it('explains a link that opened nothing with similar names and incomplete sources', () => {
    const graph = projectGraph(capture);
    // A user two edits away is offered on the same table; a truncated source is named.
    const link = { kind: 'table' as const, database: 'demo', table: 'trips', user: 'alise' };
    const missing = [{ kind: 'user' as const, name: 'alise' }];
    expect(describeMissingLink(graph, link, missing, [{ source: 'users', state: 'truncated', hosts: [], visibility: 'unknown' }])).toEqual({
      requested: 'demo.trips for alise', missing, incomplete: ['system.users: truncated'],
      suggestions: [{ focus: 'trips', context: 'alice', name: 'demo.trips for alice' }] });
    // Each part of the name present in another one is a match.
    expect(describeMissingLink(graph, { kind: 'user', user: 'th_alice' }, [{ kind: 'user', name: 'th_alice' }], []).suggestions)
      .toEqual([{ focus: 'alice', context: null, name: 'alice' }]);
    // A name contained in another one is a match: `trip` finds demo.trips.
    expect(describeMissingLink(graph, { kind: 'table', database: 'demo', table: 'trip' }, [{ kind: 'table', name: 'demo.trip' }], []).suggestions)
      .toEqual([{ focus: 'trips', context: null, name: 'demo.trips' }]);
  });
  it('offers the pages each object type opens, with the chain user carried to a table', () => {
    const graph = projectGraph({ ...capture, observations: capture.observations.map(o => o.key === 'trips' ? { ...o, attributes: { ...o.attributes, engine: 'MergeTree' } }
      : o.key === 'daily' ? { ...o, attributes: { ...o.attributes, engine: 'View' } } : o) });
    const detailLinks = (key: string, contextUser?: string) => describeDetail(graph.nodes.find(n => n.key === key)!, graph, { contextUser }).links;
    const format = (list: { page: string; label: string; description: string; url: string }[]) =>
      list.map(d => `${d.page}: ${d.label} | ${d.description} | ${d.url}`);
    const trips = detailLinks('trips', 'alice');
    expect(trips.users).toEqual(['alice', 'bob']);
    expect(trips.defaultUser).toBe('alice');
    expect(format(trips.byUser[''])).toEqual([
      'Queries: Queries | Queries on demo.trips | /queries?tableName=demo.trips',
      'Time Travel: Time Travel | Time Travel tracking demo.trips | /timetravel?tt_table=demo.trips&tt_matched_only=1',
      'Merges: Merges | Merges of demo.trips (merges have no user) | /merges?database=demo&table=trips']);
    expect(format(trips.byUser['alice'])).toEqual([
      'Queries: Queries | Queries by alice on demo.trips | /queries?user=alice&tableName=demo.trips',
      'Time Travel: Time Travel | Time Travel tracking alice on demo.trips | /timetravel?tt_user=alice&tt_failed_start=1&tt_table=demo.trips&tt_matched_only=1',
      'Merges: Merges | Merges of demo.trips (merges have no user) | /merges?database=demo&table=trips']);
    // A view has no merges; without a chain user defaultUser is empty.
    const daily = detailLinks('daily');
    expect(daily.defaultUser).toBe('');
    expect(format(daily.byUser[''])).toEqual([
      'Queries: Queries | Queries on demo.daily | /queries?tableName=demo.daily',
      'Time Travel: Time Travel | Time Travel tracking demo.daily | /timetravel?tt_table=demo.daily&tt_matched_only=1']);
    const alice = detailLinks('alice');
    expect(alice.users).toEqual([]);
    // Only tables and views link to other pages.
    expect(alice.byUser['']).toEqual([]);
    expect(detailLinks('reader', 'alice')).toEqual({ users: [], defaultUser: '', byUser: { '': [] } });
    // Returning reopens focus on the origin, with the chain user on a table.
    expect(objectLinkUrl(objectLinkFor(graph.nodes.find(n => n.key === 'trips'), 'alice')!)).toBe('/objects?ob_kind=table&ob_db=demo&ob_tbl=trips&ob_user=alice');
  });
  it('tells a revoke from a grant: own card, own count, and roles that are not default', () => {
    const rec = (key: string, kind: ObjectKind, source: Source, attributes: Record<string, unknown> = {}) => ({ key, name: key, kind, source, host: 'ch-1', attributes });
    const link = (from: string, to: string, kind: Relationship['kind'], source: Source, evidence: Record<string, unknown> = {}): Relationship =>
      ({ id: `${from}>${to}`, host: 'ch-1', from, to, kind, label: kind, source, evidence });
    // GRANT SELECT ON demo.* TO analyst; REVOKE SELECT ON demo.audit FROM analyst; bob holds analyst, not by default.
    const graph = projectGraph({ ...capture, observations: [rec('bob', 'user', 'users'), rec('analyst', 'role', 'roles'),
      rec('all', 'grant-scope', 'grants', { access_type: 'SELECT', database: 'demo', table: null }),
      rec('revoke', 'grant-scope', 'grants', { access_type: 'SELECT', database: 'demo', table: 'audit', partial_revoke: 1 }),
      { ...rec('audit', 'table', 'tables', { database: 'demo', name: 'audit', engine: 'MergeTree' }), name: 'demo.audit' },
      { ...rec('sales', 'table', 'tables', { database: 'demo', name: 'sales', engine: 'MergeTree' }), name: 'demo.sales' },
      // GRANT SELECT(id) ON demo.orders TO bob
      rec('orders-id', 'grant-scope', 'grants', { access_type: 'SELECT', database: 'demo', table: 'orders', column: 'id' }),
      { ...rec('orders', 'table', 'tables', { database: 'demo', name: 'orders', engine: 'MergeTree' }), name: 'demo.orders' }],
    relationships: [link('bob', 'analyst', 'has-role', 'role_grants', { granted_role_is_default: 0 }),
      link('analyst', 'all', 'grant', 'grants', { is_partial_revoke: 0 }), link('analyst', 'revoke', 'grant', 'grants', { is_partial_revoke: 1 }),
      link('all', 'audit', 'candidate-scope', 'grants', { is_partial_revoke: 0 }), link('revoke', 'audit', 'candidate-scope', 'grants', { is_partial_revoke: 1 }),
      link('all', 'sales', 'candidate-scope', 'grants', { is_partial_revoke: 0 }),
      link('bob', 'orders-id', 'grant', 'grants', { is_partial_revoke: 0 }), link('orders-id', 'orders', 'candidate-scope', 'grants', { is_partial_revoke: 0 })] });
    const narrowed = withPrincipalContext(focusGraphFrom(graph, 'audit'), 'bob', 'audit');
    // The revoke sits on the same role as the database grant: that grant is cancelled for the table.
    expect({ paths: narrowed.paths, revokes: narrowed.revokes, inactiveRoles: narrowed.inactiveRoles, cancelled: narrowed.cancelled })
      .toEqual({ paths: 0, revokes: 1, inactiveRoles: ['analyst'], cancelled: [{ holder: 'analyst', grant: 'all' }] });
    expect(narrowed.cancelledEdges.map(key => narrowed.graph.edges.find(e => e.key === key)).map(e => `${e?.from}>${e?.to}`)).toEqual(['all>audit']);
    expect(describeProvenance(narrowed, 'bob', 'demo.audit').text)
      .toBe("No grant path left: analyst's all is revoked for demo.audit. analyst is not active when bob logs in; it needs SET ROLE analyst");
    expect(provenanceState(narrowed)).toBe('revoked');
    // No revoke on sales: the grant holds, but only through analyst, which bob must SET ROLE first.
    const sales = withPrincipalContext(focusGraphFrom(graph, 'sales'), 'bob', 'sales');
    expect({ paths: sales.paths, activePaths: sales.activePaths, state: provenanceState(sales) }).toEqual({ paths: 1, activePaths: 0, state: 'set-role' });
    // A grant on some columns is a path to those columns only.
    const orders = withPrincipalContext(focusGraphFrom(graph, 'orders'), 'bob', 'orders');
    expect({ columnsOnly: orders.columnsOnly, state: provenanceState(orders) }).toEqual({ columnsOnly: ['id'], state: 'columns' });
    expect(describeProvenance(orders, 'bob', 'demo.orders').text).toBe('Granted through bob · only columns id');
    const revoke = describeObjects(graph).get('revoke')!;
    expect(revoke.title).toBe('REVOKE SELECT');
    expect(revoke.flags).toContain('revoke');
    const rows = describeDetail(graph.nodes.find(n => n.key === 'audit')!, graph).sections.flatMap(s => s.kind === 'relationships' ? s.groups.flatMap(g => g.rows) : []);
    expect(rows.find(r => r.relation === 'Revoked by')?.items.map(i => i.key)).toEqual(['revoke']);
  });
  it('reads a table key from database.table, split at the first dot', () => {
    expect(tableKeyOf('c', 'demo.trips')).toBe(JSON.stringify(['c', 'table', 'demo', 'trips']));
    expect(tableKeyOf('c', 'demo.trips.v2')).toBe(JSON.stringify(['c', 'table', 'demo', 'trips.v2']));
    expect(tableKeyOf('c', 'trips')).toBeNull();
    expect(tableKeyOf('c', 'demo.')).toBeNull();
  });
  it('says in words when a provenance has no grant path', () => {
    const graph = projectGraph(capture);
    // carol holds no role or privilege that covers trips.
    const none = describeProvenance(summarizePrincipalPaths(graph, 'carol', 'trips'), 'carol', 'demo.trips', 'SELECT');
    expect(none.text).toBe('No grant path: no role or privilege of carol covers SELECT on demo.trips');
    expect(none.parts.filter(p => p.kind === 'name').map(p => p.text)).toEqual(['carol', 'SELECT', 'demo.trips']);
    expect(provenanceState(summarizePrincipalPaths(graph, 'carol', 'trips'))).toBe('no-path');
    expect(provenanceState(summarizePrincipalPaths(graph, 'alice', 'trips'))).toBe('granted');
    // A catalog focus asks about any of several access types.
    expect(describeProvenance(summarizePrincipalPaths(graph, 'alice', 'trips'), 'alice', 'demo.trips', ['SELECT', 'INSERT']).text).toBe('Granted through reader for SELECT or INSERT');
    expect(describeProvenance(summarizePrincipalPaths(graph, 'carol', 'trips'), 'carol', 'demo.trips', ['SELECT', 'INSERT']).text)
      .toBe('No grant path: no role or privilege of carol covers SELECT or INSERT on demo.trips');
    expect(describeProvenance(summarizePrincipalPaths(graph, 'alice', 'trips'), 'alice', 'demo.trips').text).toBe('Granted through reader');
  });
});
