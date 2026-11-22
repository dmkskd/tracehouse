import { describe, expect, it } from 'vitest';
import { locationNodes, storageServer } from '../collectors/locations.js';
import { projectCatalogGraph, projectGraph } from '../projections/graph.js';
import { isGap, layoutFocus } from '../presentation/layout.js';
import { describeObjects } from '../presentation/cards.js';
import { describeDetail } from '../presentation/details.js';
import type { Capture } from '../model/types.js';

const t = (name: string) => JSON.stringify(['c', 'table', 'lake', name]);
const entry = (name: string, location: string, host = 'ch-1', backend = 'S3') => ({ host, table: t(name), location, backend, definition: '' });

describe('storage locations', () => {
  it('takes the storage server from a URL, drops credentials, and keeps local paths per host', () => {
    expect(storageServer('http://minio:9000/iceberg-warehouse/tracehouse/nyc/trips/', 'ch-1')).toBe('http://minio:9000');
    expect(storageServer('https://user:secret@s3.example.com/b/k.parquet', 'ch-1')).toBe('https://s3.example.com');
    expect(storageServer('s3://bucket/wh/t', 'ch-1')).toBe('s3://bucket');
    expect(storageServer('/var/lib/clickhouse/user_files/wh/t/', 'ch-2')).toBe('local disk · ch-2');
  });
  it('joins tables stored on the same server at one node, keeping each full location as evidence', () => {
    const { observations, relationships } = locationNodes('c', [
      entry('nyc_trips', 'http://minio:9000/iceberg-warehouse/tracehouse/iceberg_nyc_taxi/trips/'),
      entry('nyc_locations', 'http://minio:9000/iceberg-warehouse/tracehouse/iceberg_nyc_taxi/locations/'),
      entry('local', '/data/wh/t/', 'ch-1', 'Local'),
    ]);
    expect(observations.map(o => [o.name, o.attributes.backend, o.attributes.tables])).toEqual([['http://minio:9000', 'S3', 2], ['local disk · ch-1', 'Local', 1]]);
    const minio = observations[0].key;
    expect(relationships.filter(r => r.to === minio).map(r => [r.from, r.kind, r.label, r.evidence.location])).toEqual([
      [t('nyc_trips'), 'points-to', 'stored on', 'http://minio:9000/iceberg-warehouse/tracehouse/iceberg_nyc_taxi/trips/'],
      [t('nyc_locations'), 'points-to', 'stored on', 'http://minio:9000/iceberg-warehouse/tracehouse/iceberg_nyc_taxi/locations/']]);
  });
  it('places tables left of the server node they are stored on', () => {
    const nodes = locationNodes('c', [entry('a', 'http://minio:9000/w/a/'), entry('b', 'http://minio:9000/w/b/')]);
    const tables = ['a', 'b'].map(name => ({ key: t(name), kind: 'table' as const, name, host: 'ch-1', source: 'tables' as const,
      attributes: { database: 'lake', name, engine: 'IcebergS3' } }));
    const capture: Capture = { schemaVersion: 1, investigation: { connection: 'c', principal: '', database: '', table: '', operation: 'SELECT' }, startedAt: '', completedAt: '',
      hosts: [{ host: 'ch-1', version: '26.9.7.9' }], observations: [...nodes.observations, ...tables], relationships: nodes.relationships,
      coverage: [{ source: 'tables', state: 'readable', hosts: ['ch-1'], visibility: 'unknown' }], verdict: { kind: 'not-checked', reason: '' } };
    const graph = projectGraph(capture);
    const columns = layoutFocus(graph);
    expect(columns.map(c => [c.title, c.keys.filter(k => !isGap(k)).length])).toEqual([['Tables', 2], ['Storage', 1]]);
    // The server sits level with its tables: below the first, beside the second.
    expect(columns[1].keys.filter(isGap)).toHaveLength(1);
    expect(describeObjects(graph).get(nodes.observations[0].key)).toMatchObject({ title: 'http://minio:9000', label: 'S3', summary: 'S3 · 2 tables', facts: [{ label: 'Tables', value: '2' }] });
    expect(describeObjects(graph).get(t('a'))!.facts).toContainEqual({ label: 'Stored on', value: 'http://minio:9000' });
  });
  it('keeps the storage server in the last column when a longer chain sits beside its tables', () => {
    const nodes = locationNodes('c', [entry('a', 'http://minio:9000/w/a/')]);
    const table = (name: string, engine: string) => ({ ...tableRecord(name), attributes: { database: 'lake', name, engine } });
    const link = (from: string, to: string, kind: 'insert-trigger' | 'writes-to') => ({ id: `${from}-${to}`, host: 'ch-1', from: t(from), to: t(to), kind, label: kind, source: 'tables' as const, evidence: {} });
    const graph = projectGraph(capture(['ch-1'], [...nodes.observations, table('a', 'IcebergS3'), table('src', 'MergeTree'), table('mv', 'MaterializedView'), table('dst', 'MergeTree')],
      [...nodes.relationships, link('src', 'mv', 'insert-trigger'), link('mv', 'dst', 'writes-to')]));
    const columns = layoutFocus(graph);
    expect(columns.at(-1)).toEqual({ title: 'Storage', band: 'external', keys: [nodes.observations[0].key] });
    expect(columns.slice(0, -1).flatMap(c => c.keys)).toContain(t('dst'));
  });
});

const tableRecord = (name: string, host = 'ch-1') => ({ key: t(name), kind: 'table' as const, name: `lake.${name}`, host, source: 'tables' as const,
  attributes: { database: 'lake', name, engine: 'IcebergS3' } });
const capture = (hosts: string[], observations: Capture['observations'], relationships: Capture['relationships']): Capture => ({ schemaVersion: 1, mode: 'catalog',
  investigation: { connection: 'c', principal: '', database: '', table: '', operation: 'SELECT' }, startedAt: '', completedAt: '',
  hosts: hosts.map(host => ({ host, version: '26.9.7.9' })), observations, relationships,
  coverage: [{ source: 'tables', state: 'readable', hosts, visibility: 'unknown' }], verdict: { kind: 'not-checked', reason: '' } });

describe('focus from a storage server', () => {
  it('links a global privilege to the tables stored there, even past the match cap', () => {
    const nodes = locationNodes('c', [entry('stored', 'http://minio:9000/w/stored/')]);
    const user = JSON.stringify(['c', 'user', 'reader']), scope = JSON.stringify(['c', 'grant-scope', 'select-all']);
    // `other` comes first in the catalog and would take the only slot of a global cap.
    const observed = capture(['ch-1'], [...nodes.observations, tableRecord('other'), tableRecord('stored'),
      { key: user, kind: 'user', name: 'reader', host: 'ch-1', source: 'users', attributes: {} },
      { key: scope, kind: 'grant-scope', name: 'SELECT ON *.*', host: 'ch-1', source: 'grants', attributes: { table_operations: ['SELECT'] } }],
    [...nodes.relationships, { id: 'g', host: 'ch-1', from: user, to: scope, kind: 'grant', label: 'SELECT grant', source: 'grants',
      evidence: { access_type: 'SELECT', access_object: '', database: null, table: null, is_wildcard: 0, is_partial_revoke: 0 } }]);
    // Without a focus object the global privilege has no table link; focused on the server it links.
    expect(projectCatalogGraph(observed).edges.some(e => e.kind === 'candidate-scope')).toBe(false);
    expect(projectCatalogGraph(observed, nodes.observations[0].key, 1).edges.filter(e => e.kind === 'candidate-scope').map(e => e.to)).toEqual([t('stored')]);
  });
});

describe('storage servers on a cluster', () => {
  it('compares no hosts for a server named by one host, and shows how the link was obtained', () => {
    const nodes = locationNodes('c', [entry('a', '/data/wh/a/', 'ch-1', 'Local'), entry('b', 'http://minio:9000/w/b/', 'ch-2')]);
    const graph = projectGraph(capture(['ch-1', 'ch-2'], [...nodes.observations, tableRecord('a', 'ch-1'), tableRecord('a', 'ch-2'),
      tableRecord('b', 'ch-1'), tableRecord('b', 'ch-2')], nodes.relationships));
    const cards = describeObjects(graph);
    for (const server of nodes.observations) {
      expect(cards.get(server.key)!.flags).toEqual([]);
      expect(cards.get(server.key)!.hosts).toBeUndefined();
    }
    const detail = describeDetail(graph.nodes.find(n => n.key === t('b'))!, graph);
    expect(detail.sections.find(s => s.kind === 'relationships')).toMatchObject({ groups: [{ title: 'Storage', rows: [
      { relation: 'Stored on', items: [{ name: 'http://minio:9000', method: expect.stringContaining('no storage request') }] }] }] });
  });
});
