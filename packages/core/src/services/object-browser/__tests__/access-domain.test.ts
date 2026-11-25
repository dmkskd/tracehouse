import { describe, expect, it } from 'vitest';
import { accessEvaluator } from '../access/evaluate.js';
import { createAccessIndex } from '../access/index.js';
import { exactTableScope, nameScopeRank, normalizeScope, scopeRank } from '../access/scope.js';
import { edgesBetween, principalsReaching } from '../access/traversal.js';
import { projectCatalogGraph, projectGraph } from '../projections/graph.js';
import { describeDetail } from '../presentation/details.js';
import type { Capture, Observation, Relationship } from '../model/types.js';

const user = (key: string, host = 'a'): Observation => ({ key, name: key, host, kind: 'user', source: 'users', attributes: {} });
function fixture(): Capture {
  return { schemaVersion: 1, mode: 'catalog', investigation: { connection: 'c', principal: '', database: '', table: '', operation: 'SELECT' },
    startedAt: '', completedAt: '', hosts: [{ host: 'a', version: '26.9' }, { host: 'b', version: '26.9' }],
    operations: [{ name: 'SELECT', group: 'SELECT' }], verdict: { kind: 'not-checked', reason: '' }, coverage: [],
    observations: [user('alice'), { key: 't', name: 'demo.trips', kind: 'table', host: 'a', source: 'tables', attributes: { database: 'demo', name: 'trips', engine: 'MergeTree' } },
      { key: 's', name: 'SELECT', kind: 'grant-scope', host: 'a', source: 'grants', attributes: { access_type: 'SELECT', database: 'demo', table: null, table_operations: ['SELECT'] } }],
    relationships: [{ id: 'grant', from: 'alice', to: 's', host: 'a', kind: 'grant', source: 'grants', label: 'grant', evidence: { access_type: 'SELECT', database: 'demo', table: null, table_operations: ['SELECT'] } }] };
}
const link = (from: string, to: string, kind: Relationship['kind'], host: string): Relationship => ({ id: `${host}:${from}:${to}`, from, to, kind, host, source: 'grants', label: kind, evidence: {} });

describe('normalized access domain', () => {
  it('evaluates raw evidence independently of graph filtering and produces structured results', () => {
    const capture = fixture();
    expect(projectGraph(capture).edges.every(edge => edge.kind !== 'candidate-scope')).toBe(true);
    const answer = accessEvaluator(capture).answer('alice', 't', 'SELECT');
    expect(answer.state).toBe('granted');
    expect(answer).not.toHaveProperty('line');
    expect(answer.hosts[0].columns).toEqual({ kind: 'all' });
  });

  it('copies an immutable evaluation snapshot without caching mutable caller data', () => {
    const capture = fixture(), evaluator = accessEvaluator(capture), index = createAccessIndex(capture, []);
    const scope = index.scope('s', 'a')!;
    expect(Object.isFrozen(scope)).toBe(true);
    expect(Object.isFrozen(scope.operations)).toBe(true);
    capture.relationships = [];
    capture.observations.find(o => o.key === 's')!.attributes.table_operations = ['INSERT'];
    expect(evaluator.answer('alice', 't', 'SELECT').state).toBe('granted');
    expect(accessEvaluator(capture).answer('alice', 't', 'SELECT').state).toBe('no-path');
  });

  it.each([
    [{ database: null, table: null }, [0, 0]],
    [{ database: 'de', table: null, is_wildcard: 1 }, [1, 2]],
    [{ database: 'demo', table: null }, [2, 0]],
    [{ database: 'demo', table: 'tri', is_wildcard: 1 }, [3, 3]],
    [{ database: 'demo', table: 'trips' }, [4, 0]],
    [{ database: 'demo', table: 'trips', column: 'email' }, [5, 0]],
    [{ database: 'elsewhere', table: null }, null],
    [{ database: 'demo', table: 'nope' }, null],
  ])('matches the same scope in evaluation and graph projection: %j', (target, rank) => {
    const attributes = { access_type: 'SELECT', table_operations: ['SELECT'], ...target };
    const normalized = normalizeScope(attributes), table = { database: 'demo', name: 'trips' };
    expect(nameScopeRank(normalized.target, table)).toEqual(rank);
    expect(scopeRank(normalized, table, 'SELECT')).toEqual(rank);
    expect(scopeRank(normalized, table, 'INSERT')).toBeNull();
    const capture = fixture();capture.observations.find(o => o.key === 's')!.attributes = attributes;
    capture.relationships[0].evidence = attributes;
    const graph = projectCatalogGraph(capture, 't');
    expect(graph.edges.some(e => e.kind === 'candidate-scope')).toBe(rank !== null);
    expect(projectCatalogGraph(capture).edges.some(e => e.kind === 'candidate-scope')).toBe(rank !== null && exactTableScope(normalized.target));
  });
});

describe('host-specific reverse traversal', () => {
  it('finds deep paths and cycles without joining assignments from other hosts', () => {
    const capture = fixture();capture.relationships = [];
    capture.observations.push(user('bob'));
    for (let i = 0; i < 60; i++) {
      capture.observations.push({ key: `r${i}`, name: `r${i}`, kind: 'role', source: 'roles', host: 'a', attributes: {} });
      capture.relationships.push(link(i ? `r${i-1}` : 'alice', `r${i}`, 'has-role', 'a'));
    }
    capture.relationships.push(link('r59', 'r0', 'has-role', 'a'), link('r59', 's', 'grant', 'a'), link('s', 't', 'candidate-scope', 'a'), link('bob', 'r0', 'has-role', 'b'));
    const graph = projectGraph(capture), reaching = principalsReaching(graph, 't');
    expect(reaching.has('alice')).toBe(true);
    expect(reaching.has('bob')).toBe(false);
    expect(edgesBetween(graph, 'alice', 't').size).toBe(63);
    expect(edgesBetween(graph, 'bob', 't').size).toBe(0);
  });

  it('offers the sorted first 25 users from a large shared grant, preserving host isolation', () => {
    const capture = fixture();
    for (let i = 0; i < 600; i++) { const key = `user-${String(i).padStart(3, '0')}`;capture.observations.push(user(key));capture.relationships.push({ ...capture.relationships[0], id: key, from: key }); }
    const graph = projectCatalogGraph(capture, 't'), table = graph.nodes.find(n => n.key === 't')!;
    expect(describeDetail(table, graph).links.users).toEqual(['alice', ...Array.from({ length: 24 }, (_, i) => `user-${String(i).padStart(3, '0')}`)]);
  });
});
