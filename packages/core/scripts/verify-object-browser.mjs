// Explicit opt-in verifier: creates/drops only the uniquely prefixed fixture in core.
import { execFileSync } from 'node:child_process';
import assert from 'node:assert/strict';
import { accessFixtureQueries, ClusterService, ClusterAwareAdapter } from '@tracehouse/core';
import { HttpAdapter } from '@tracehouse/core/adapters/http-adapter';
import { collectObjects, projectGraph, provenancePaths, objectKey } from '@tracehouse/core/services/object-browser';
const container = process.argv[2];
if (!container) throw Error('Pass an existing test ClickHouse container name.');
const prefix = `th_object_model_${Date.now().toString(36)}`;
const fixture = accessFixtureQueries(prefix);
function query(sql, user = 'default', id) {
  const args = ['exec', container, 'clickhouse-client', '--user', user, '--format', 'JSONEachRow'];
  if (id) args.push('--query_id', id);
  args.push('--query', ClusterService.resolveTableRefs(sql, null));
  return execFileSync('docker', args, { encoding: 'utf8', stdio: ['ignore','pipe','pipe'] })
    .trim().split('\n').filter(Boolean).map(line => JSON.parse(line));
}
const httpClients = [];
const port = Number(process.argv[3] || 0);
const adapter = user => {
  if (!port) return { executeQuery: async sql => query(sql, user) };
  const client = new HttpAdapter({ host: '127.0.0.1', port, user, password: '', database: 'default', secure: false, connect_timeout: 5, send_receive_timeout: 15 });
  httpClients.push(client);
  return new ClusterAwareAdapter(client);
};
try {
  for (const sql of fixture.setup) query(sql);
  const input = { connection: 'container-test', principal: fixture.alice, database: fixture.db, table: 'trips', operation: 'SELECT' };
  const capture = await collectObjects(adapter('default'), input);
  assert.equal(capture.coverage.every(c => c.state === 'readable'), true, JSON.stringify(capture.coverage));
  const graph = projectGraph(capture);
  assert.equal(graph.nodes.filter(n => n.kind === 'role').length, 2);
  assert.equal(graph.nodes.filter(n => n.kind === 'row-policy').length, 1);
  assert.ok(provenancePaths(graph, objectKey(input.connection,'user',fixture.alice), objectKey(input.connection,'table',fixture.db,'trips')).length);
  assert.equal(capture.verdict.kind, 'not-checked');
  assert.deepEqual(query(fixture.read, fixture.alice).map(r => Number(r.id)), [1]);
  let failure = false;
  try { query(fixture.read, fixture.denied, `${prefix}_denied_execution`); }
  catch (e) { failure = /ACCESS_DENIED|Not enough privileges|Code: 497/.test(String(e.stderr)); }
  assert.equal(failure, true, 'Restricted principal should fail with missing privilege');
  const restricted = await collectObjects(adapter(fixture.observer), input);
  assert.ok(restricted.coverage.some(c => c.state === 'failed' || c.state === 'schema-unavailable'));
  assert.equal(restricted.verdict.kind, 'not-checked');
  console.log(JSON.stringify({ serverHosts: capture.hosts, sources: capture.coverage.map(c => [c.source,c.state]),
    nestedRolePath: true, actualPolicyFilteredRows: 1, deniedExecutionObserved: true,
    restrictedObserver: restricted.coverage.map(c => [c.source,c.state]),
    currentVerdict: capture.verdict.kind, transport: port ? 'HttpAdapter + ClusterAwareAdapter (single-host HTTP); not Grafana' : 'docker client JSONEachRow; not HTTP/Grafana',
    queryLogPersistence: 'not verified' }, null, 2));
} finally { for (const client of httpClients) await client.close(); for (const sql of fixture.teardown) query(sql); }
