import { objectKey, type Capture, type ObjectKind, type Source } from '@tracehouse/core/services/object-browser';

export const key = (kind: ObjectKind, ...names: string[]) => objectKey('test', kind, ...names);

/** Two-host evidence: bob needs SET ROLE; analyst revokes audit_log, while alice's
 * exact grant stays independent. A view depends on payments without inheriting access. */
export function captureFixture(): Capture {
  const capture: Capture = { schemaVersion: 1, mode: 'catalog', investigation: { connection: 'test', principal: '', database: '', table: '', operation: 'SELECT' },
    startedAt: 'start', completedAt: 'done', hosts: ['ch-1', 'ch-2'].map(host => ({ host, version: '26.9' })),
    observations: [], relationships: [], coverage: [], operations: [{ name: 'SELECT', group: 'SELECT' }], verdict: { kind: 'not-checked', reason: 'Configured metadata only' } };
  const sources: Source[] = ['users', 'roles', 'role_grants', 'grants', 'tables', 'row_policies'];
  capture.coverage = sources.map(source => ({ source, state: 'readable', hosts: ['ch-1', 'ch-2'], visibility: 'unknown' }));
  for (const host of ['ch-1', 'ch-2']) {
    const add = (kind: ObjectKind, names: string[], source: Source, attributes: Record<string, unknown> = {}) => capture.observations.push({ key: key(kind, ...names), name: names.join('.'), kind, host, source, attributes });
    add('user', ['alice'], 'users', { default_roles_all: 1, default_roles_list: [], default_roles_except: [] });
    add('user', ['bob'], 'users', { default_roles_all: 0, default_roles_list: [], default_roles_except: [] });
    for (const role of ['reader', 'analyst']) add('role', [role], 'roles');
    for (const name of ['payments', 'audit_log', 'payments_view']) add('table', ['db.with.dots', name], 'tables', { database: 'db.with.dots', name, engine: name.endsWith('view') ? 'View' : 'MergeTree' });
    add('grant-scope', ['broad'], 'grants', { access_type: 'SELECT', database: 'db.with.dots', table: null, column: null, is_wildcard: 0, table_operations: ['SELECT'] });
    add('grant-scope', ['exact'], 'grants', { access_type: 'SELECT', database: 'db.with.dots', table: 'payments', column: null, is_wildcard: 0, table_operations: ['SELECT'] });
    add('grant-scope', ['revoke'], 'grants', { access_type: 'SELECT', database: 'db.with.dots', table: 'audit_log', column: null, is_partial_revoke: 1, is_wildcard: 0, table_operations: ['SELECT'] });
    const edge = (from: string, to: string, kind: Capture['relationships'][number]['kind'], evidence: Record<string, unknown> = {}) => capture.relationships.push({ id: `${host}:${from}:${to}`, host, from, to, kind, evidence, label: kind, source: kind === 'has-role' ? 'role_grants' : kind === 'reads-from' ? 'tables' : 'grants' });
    edge(key('user', 'alice'), key('role', 'reader'), 'has-role', { granted_role_is_default: 1 });
    edge(key('user', 'bob'), key('role', 'analyst'), 'has-role', { granted_role_is_default: 0 });
    edge(key('role', 'analyst'), key('role', 'reader'), 'has-role', { granted_role_is_default: 1 });
    edge(key('role', 'reader'), key('grant-scope', 'exact'), 'grant');
    edge(key('role', 'analyst'), key('grant-scope', 'broad'), 'grant');
    edge(key('role', 'analyst'), key('grant-scope', 'revoke'), 'grant', { is_partial_revoke: 1 });
    edge(key('table', 'db.with.dots', 'payments'), key('table', 'db.with.dots', 'payments_view'), 'reads-from');
  }
  return capture;
}
