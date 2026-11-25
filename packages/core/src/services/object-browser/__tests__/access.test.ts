import { describe, expect, it } from 'vitest';
import { absentFrom, answerFor, cancelledAcross, edgesBetween, reachFor } from '../presentation/access.js';
import { projectGraph } from '../projections/graph.js';
import type { Capture, Coverage, Observation, Relationship } from '../model/types.js';

// Records as the collector writes them: one per host, scopes with their table operations.
const SOURCE = { user: 'users', role: 'roles', table: 'tables', 'grant-scope': 'grants' } as const;
const record = (key: string, kind: keyof typeof SOURCE, host: string, attributes: Record<string, unknown> = {}): Observation =>
  ({ key, name: key, kind, source: SOURCE[kind], host, attributes });
const table = (name: string, host: string) => record(`demo.${name}`, 'table', host, { database: 'demo', name });
const scope = (key: string, host: string, attributes: Record<string, unknown>) => record(key, 'grant-scope', host,
  { access_type: 'SELECT', table_operations: ['SELECT'], database: null, table: null, column: null, is_wildcard: 0, ...attributes });
const role = (from: string, to: string, host: string, isDefault = 1): Relationship =>
  ({ id: `${host}:${from}>${to}`, host, from, to, kind: 'has-role', label: 'has role', source: 'role_grants', evidence: { granted_role_is_default: isDefault } });
const grant = (from: string, to: string, host: string): Relationship =>
  ({ id: `${host}:${from}>${to}`, host, from, to, kind: 'grant', label: 'grant', source: 'grants', evidence: {} });
const capture = (observations: Observation[], relationships: Relationship[], coverage: Coverage[] = []): Capture => ({
  schemaVersion: 1, mode: 'catalog', investigation: { connection: 'c', principal: '', database: '', table: '', operation: 'SELECT' },
  startedAt: '', completedAt: '', hosts: [...new Set(observations.map(o => o.host))].map(host => ({ host, version: '26.9.7.9' })),
  coverage, verdict: { kind: 'not-checked', reason: '' }, operations: [{ name: 'SELECT', group: 'SELECT' }], observations, relationships,
});
const on = (host: string, ...records: ((host: string) => Observation)[]) => records.map(r => r(host));

describe('access evaluation', () => {
  it('answers granted, after SET ROLE, on some columns, and cancelled by a revoke on the same holder', () => {
    const h = 'ch-1';
    const c = capture([...on(h, x => record('bob', 'user', x), x => record('carol', 'user', x), x => record('ivy', 'user', x), x => record('analyst', 'role', x), x => record('reader', 'role', x),
      x => table('trips', x), x => table('audit', x), x => table('orders', x),
      x => scope('db', x, { database: 'demo' }), x => scope('no-audit', x, { database: 'demo', table: 'audit', partial_revoke: 1 }),
      x => scope('orders-id', x, { database: 'demo', table: 'orders', column: 'id' }), x => scope('no-trips-email', x, { database: 'demo', table: 'trips', column: 'email', partial_revoke: 1 }))],
    // GRANT SELECT ON demo.* TO analyst, reader; REVOKE SELECT ON demo.audit FROM analyst; REVOKE SELECT(email) ON demo.trips FROM reader;
    // GRANT SELECT(id) ON demo.orders TO carol, ivy; bob holds analyst, not by default; carol holds reader.
    [role('bob', 'analyst', h, 0), role('carol', 'reader', h), grant('analyst', 'db', h), grant('analyst', 'no-audit', h),
      grant('reader', 'db', h), grant('reader', 'no-trips-email', h), grant('carol', 'orders-id', h), grant('ivy', 'orders-id', h)]);
    expect(answerFor(c, 'bob', 'demo.trips', 'SELECT')).toMatchObject({ state: 'set-role',
      line: { text: 'Granted through analyst for SELECT. analyst is not active when bob logs in; it needs SET ROLE analyst' } });
    // The revoke sits on the holder of the database grant: that grant is taken back for audit.
    expect(answerFor(c, 'bob', 'demo.audit', 'SELECT')).toMatchObject({ state: 'revoked', cancelledScopes: ['db'],
      line: { text: 'No grant path left: analyst has a revoke that takes back its SELECT on demo.audit' } });
    // A column revoke narrows the table grant to the other columns; it does not cancel it.
    expect(answerFor(c, 'carol', 'demo.trips', 'SELECT')).toMatchObject({ state: 'columns',
      line: { text: 'Granted through reader for SELECT · all columns except email' } });
    // carol's reader grants all of orders; ivy has the id column only.
    expect(answerFor(c, 'carol', 'demo.orders', 'SELECT')).toMatchObject({ state: 'granted', line: { text: 'Granted through reader for SELECT' } });
    expect(answerFor(c, 'ivy', 'demo.orders', 'SELECT')).toMatchObject({ state: 'columns', line: { text: 'Granted through ivy for SELECT · only columns id' } });
    expect(answerFor(c, 'carol', 'demo.orders', 'INSERT')).toMatchObject({ state: 'no-path',
      line: { text: 'No grant path: no role or privilege of carol covers INSERT on demo.orders' } });
  });
  it('keeps a revoke to its own holder: another role granting the table still grants it', () => {
    const h = 'ch-1';
    const c = capture(on(h, x => record('dave', 'user', x), x => record('narrow', 'role', x), x => record('broad', 'role', x), x => table('audit', x),
      x => scope('db', x, { database: 'demo' }), x => scope('no-audit', x, { database: 'demo', table: 'audit', partial_revoke: 1 })),
    [role('dave', 'narrow', h), role('dave', 'broad', h), grant('narrow', 'db', h), grant('narrow', 'no-audit', h), grant('broad', 'db', h)]);
    expect(answerFor(c, 'dave', 'demo.audit', 'SELECT')).toMatchObject({ state: 'granted', line: { text: 'Granted through broad for SELECT' } });
    // narrow and broad share demo.*: only narrow takes it back, so the shared line is not marked cancelled.
    expect(answerFor(c, 'dave', 'demo.audit', 'SELECT').cancelledScopes).toEqual([]);
  });
  it('evaluates each host on its own records: a role here and its grant there make no path', () => {
    // eve holds r on ch-1 only; r holds the grant on ch-2 only.
    const c = capture([...on('ch-1', x => record('eve', 'user', x), x => record('r', 'role', x), x => table('trips', x)),
      ...on('ch-2', x => record('eve', 'user', x), x => record('r', 'role', x), x => table('trips', x), x => scope('db', x, { database: 'demo' }))],
    [role('eve', 'r', 'ch-1'), grant('r', 'db', 'ch-2')]);
    expect(answerFor(c, 'eve', 'demo.trips', 'SELECT').state).toBe('no-path');
    expect(reachFor(c, 'eve', 'SELECT')).toMatchObject({ state: 'no-path', tables: 0 });
    expect(edgesBetween(projectGraph(c), 'eve', 'demo.trips').size).toBe(0);
  });
  it('says when hosts differ: a revoke on one host does not cancel the grant on another', () => {
    const both = (x: string) => [record('fay', 'user', x), record('r', 'role', x), table('audit', x), scope('db', x, { database: 'demo' })];
    const c = capture([...both('ch-1'), ...both('ch-2'), scope('no-audit', 'ch-2', { database: 'demo', table: 'audit', partial_revoke: 1 })],
      ['ch-1', 'ch-2'].flatMap(h => [role('fay', 'r', h), grant('r', 'db', h)]).concat(grant('r', 'no-audit', 'ch-2')));
    const answer = answerFor(c, 'fay', 'demo.audit', 'SELECT');
    expect(answer.state).toBe('differs');
    expect(answer.hosts.map(h => [h.host, h.state])).toEqual([['ch-1', 'granted'], ['ch-2', 'revoked']]);
    expect(answer.line.text).toBe('Differs by host: granted on ch-1; revoked on ch-2');
  });
  it('finds every revoke however many grants come first', () => {
    // 60 roles, each granting demo.* and revoking trips: every grant is taken back.
    const h = 'ch-1', roles = Array.from({ length: 60 }, (_, i) => `r${i}`);
    const c = capture([record('gil', 'user', h), table('trips', h), scope('db', h, { database: 'demo' }),
      scope('no-trips', h, { database: 'demo', table: 'trips', partial_revoke: 1 }), ...roles.map(r => record(r, 'role', h))],
    roles.flatMap(r => [role('gil', r, h), grant(r, 'db', h), grant(r, 'no-trips', h)]));
    const answer = answerFor(c, 'gil', 'demo.trips', 'SELECT');
    expect(answer.state).toBe('revoked');
    expect(answer.hosts[0].cancelled).toHaveLength(60);
  });
  it('counts tables across the catalog with the same evaluation, revokes included', () => {
    const h = 'ch-1';
    const c = capture(on(h, x => record('hal', 'user', x), x => record('r', 'role', x), x => table('trips', x), x => table('sales', x),
      x => scope('db', x, { database: 'demo' }), x => scope('no-trips', x, { database: 'demo', table: 'trips', partial_revoke: 1 })),
    [role('hal', 'r', h), grant('r', 'db', h), grant('r', 'no-trips', h)]);
    expect(reachFor(c, 'hal', 'SELECT')).toMatchObject({ state: 'granted', tables: 1, line: { text: 'Reaches 1 table for SELECT' } });
  });
  it('reads prefix scopes as system.grants records them: the longer prefix and the exact table rank above', () => {
    const h = 'ch-1';
    // GRANT SELECT ON demo.*; REVOKE SELECT ON demo.tri*; GRANT SELECT ON demo.trips_eu; GRANT INSERT ON de*.*
    const c = capture(on(h, x => record('jo', 'user', x), x => table('trips', x), x => table('trips_eu', x), x => table('sales', x),
      x => scope('db', x, { database: 'demo' }), x => scope('no-tri', x, { database: 'demo', table: 'tri', is_wildcard: 1, partial_revoke: 1 }),
      x => scope('eu', x, { database: 'demo', table: 'trips_eu' }), x => scope('de-insert', x, { access_type: 'INSERT', table_operations: ['INSERT'], database: 'de', is_wildcard: 1 })),
    [grant('jo', 'db', h), grant('jo', 'no-tri', h), grant('jo', 'eu', h), grant('jo', 'de-insert', h)]);
    expect(answerFor(c, 'jo', 'demo.trips', 'SELECT').state).toBe('revoked');
    expect(answerFor(c, 'jo', 'demo.trips_eu', 'SELECT').state).toBe('granted');
    expect(answerFor(c, 'jo', 'demo.sales', 'SELECT').state).toBe('granted');
    expect(answerFor(c, 'jo', 'demo.sales', 'INSERT').state).toBe('granted');
    expect(reachFor(c, 'jo', 'SELECT')).toMatchObject({ state: 'granted', tables: 2 });
  });
  it('compares hosts on their columns too, and counts tables that differ by host as such', () => {
    const base = (x: string) => [record('kim', 'user', x), table('orders', x)];
    const c = capture([...base('ch-1'), ...base('ch-2'), scope('id', 'ch-1', { database: 'demo', table: 'orders', column: 'id' }),
      scope('email', 'ch-2', { database: 'demo', table: 'orders', column: 'email' })], [grant('kim', 'id', 'ch-1'), grant('kim', 'email', 'ch-2')]);
    expect(answerFor(c, 'kim', 'demo.orders', 'SELECT').line.text).toBe('Differs by host: columns (only id) on ch-1; columns (only email) on ch-2');
    // Both hosts grant columns: the table is reached, not reached on some hosts only.
    expect(reachFor(c, 'kim', 'SELECT')).toMatchObject({ state: 'granted', tables: 1 });
    // A host whose negative is incomplete says so in the differs line.
    const truncated = { ...c, coverage: [{ source: 'grants' as const, state: 'truncated' as const, hosts: ['ch-2'], visibility: 'unknown' as const }] };
    expect(answerFor(truncated, 'kim', 'demo.orders', 'SELECT').line.text).toContain('· collection incomplete: grants truncated');
    // Granted on one host and absent on the other: the count says so instead of "no privilege".
    const half = capture([...base('ch-1'), ...base('ch-2'), scope('db', 'ch-1', { database: 'demo' })], [grant('kim', 'db', 'ch-1')]);
    expect(reachFor(half, 'kim', 'SELECT')).toMatchObject({ state: 'differs', tables: 1, line: { text: 'Reaches 1 table on some hosts only for SELECT' } });
  });
  it('names every holder whose columns make up the access, and leaves placeholders unanswered', () => {
    const h = 'ch-1';
    // a grants all of orders but email; b grants email: together, all of it.
    const c = capture(on(h, x => record('lee', 'user', x), x => record('a', 'role', x), x => record('b', 'role', x), x => table('orders', x),
      x => scope('orders', x, { database: 'demo', table: 'orders' }), x => scope('no-email', x, { database: 'demo', table: 'orders', column: 'email', partial_revoke: 1 }),
      x => scope('email', x, { database: 'demo', table: 'orders', column: 'email' }), x => scope('all', x, {}),
      // A view names demo.gone, which no inventory lists.
      x => ({ ...table('gone', x), attributes: { unresolvedReference: true } })),
    [role('lee', 'a', h), role('lee', 'b', h), grant('a', 'orders', h), grant('a', 'no-email', h), grant('b', 'email', h), grant('b', 'all', h)]);
    expect(answerFor(c, 'lee', 'demo.orders', 'SELECT').line.text).toBe('Granted through b for SELECT');
    const split = capture(c.observations.filter(o => o.key !== 'all'), c.relationships.filter(r => r.to !== 'all'));
    expect(answerFor(split, 'lee', 'demo.orders', 'SELECT')).toMatchObject({ state: 'granted', line: { text: 'Granted through a, b for SELECT' } });
    // b's global grant would cover any table, but demo.gone is a placeholder, not a collected table.
    expect(answerFor(c, 'lee', 'demo.gone', 'SELECT')).toMatchObject({ state: 'no-path', line: { text: 'Not found: demo.gone is not among the collected tables' } });
  });
  it('keeps which roles each host needs activated, and says when no host grants at login', () => {
    const base = (x: string) => [record('max', 'user', x), table('trips', x), scope('db', x, { database: 'demo' })];
    // Not default: reader-a on ch-1, reader-b on ch-2.
    const c = capture([...base('ch-1'), record('reader-a', 'role', 'ch-1'), ...base('ch-2'), record('reader-b', 'role', 'ch-2')],
      [role('max', 'reader-a', 'ch-1', 0), grant('reader-a', 'db', 'ch-1'), role('max', 'reader-b', 'ch-2', 0), grant('reader-b', 'db', 'ch-2')]);
    expect(answerFor(c, 'max', 'demo.trips', 'SELECT').line.text).toBe('Differs by host: set-role (SET ROLE reader-a) on ch-1; set-role (SET ROLE reader-b) on ch-2');
    expect(reachFor(c, 'max', 'SELECT')).toMatchObject({ state: 'set-role', tables: 1,
      line: { text: 'Reaches 1 table for SELECT only after SET ROLE reader-a on ch-1; SET ROLE reader-b on ch-2' } });
    // After SET ROLE on ch-1, nothing on ch-2: not reached at login anywhere.
    const half = capture([...base('ch-1'), record('reader-a', 'role', 'ch-1'), ...base('ch-2')], [role('max', 'reader-a', 'ch-1', 0), grant('reader-a', 'db', 'ch-1')]);
    expect(reachFor(half, 'max', 'SELECT')).toMatchObject({ state: 'set-role',
      line: { text: 'Reaches 1 table for SELECT only after SET ROLE reader-a, 1 of them on some hosts only' } });
  });
  it('marks a shared line cancelled only when every shown access type cancels it, and tells absent from unread', () => {
    const h = 'ch-1';
    // ALL ON demo.* with SELECT revoked on trips: SELECT is taken back, INSERT stays.
    const c = capture(on(h, x => record('ned', 'user', x), x => table('trips', x),
      x => scope('all', x, { access_type: 'ALL', table_operations: ['SELECT', 'INSERT'], database: 'demo' }),
      x => scope('no-select', x, { database: 'demo', table: 'trips', partial_revoke: 1 })),
    [grant('ned', 'all', h), grant('ned', 'no-select', h)]);
    const select = answerFor(c, 'ned', 'demo.trips', 'SELECT'), insert = answerFor(c, 'ned', 'demo.trips', 'INSERT');
    expect([select.state, insert.state]).toEqual(['revoked', 'granted']);
    expect(cancelledAcross([select])).toEqual(['all']);
    expect(cancelledAcross([select, insert])).toEqual([]);
    // A user missing from a fully read inventory is absent; with users unread it is unknown.
    const key = JSON.stringify(['c', 'user', 'nobody']);
    const read = { ...c, coverage: [{ source: 'users' as const, state: 'readable' as const, hosts: [h], visibility: 'unknown' as const }] };
    expect(absentFrom(read, [key])).toEqual([key]);
    expect(absentFrom({ ...c, coverage: [{ source: 'users', state: 'failed', hosts: [h], visibility: 'unknown' }] }, [key])).toEqual([]);
  });
  it('does not answer no from evidence it did not read', () => {
    const h = 'ch-1';
    const c = capture(on(h, x => record('ida', 'user', x), x => table('trips', x)), [],
      [{ source: 'grants', state: 'failed', hosts: [h], visibility: 'unknown', reason: 'timeout' }]);
    expect(answerFor(c, 'ida', 'demo.trips', 'SELECT')).toMatchObject({ state: 'incomplete', gaps: ['grants failed'],
      line: { text: 'Incomplete: grants failed. No grant path: no role or privilege of ida covers SELECT on demo.trips' } });
    expect(reachFor(c, 'ida', 'SELECT').state).toBe('incomplete');
    // A positive count keeps the collection warning its tables carry.
    const partly = capture(on(h, x => record('ida', 'user', x), x => table('trips', x), x => scope('db', x, { database: 'demo' })), [grant('ida', 'db', h)],
      [{ source: 'role_grants', state: 'failed', hosts: [h], visibility: 'unknown', reason: 'timeout' }]);
    expect(reachFor(partly, 'ida', 'SELECT').line.text).toBe('Reaches 1 table for SELECT · collection incomplete: role_grants failed');
    // A failed table inventory leaves no table to carry the gap: the count still says it.
    const noTables = capture([record('ida', 'user', h)], [], [{ source: 'tables', state: 'failed', hosts: [h], visibility: 'unknown', reason: 'timeout' }]);
    expect(reachFor(noTables, 'ida', 'SELECT')).toMatchObject({ state: 'incomplete', line: { text: 'Incomplete: tables failed. No privilege of ida includes SELECT' } });
    // Without the privilege tree, ALL cannot be matched to SELECT.
    const noTree = { ...capture(on(h, x => record('ida', 'user', x), x => table('trips', x)), []), operations: undefined };
    expect(answerFor(noTree, 'ida', 'demo.trips', 'SELECT').gaps).toEqual(['privilege tree unavailable']);
  });
});
