import { describe, expect, it } from 'vitest';
import { diskLocations } from '../collectors/storage.js';
import { projectCatalogGraph } from '../projections/graph.js';
import { describeCard, edgeCaption } from '../presentation/cards.js';
import { describeDetail } from '../presentation/details.js';
import { parseTTLRules } from '../../../utils/ttl-parser.js';
import type { Capture } from '../model/types.js';

/** system.storage_policies and system.disks rows as ClickHouse 26.10 returned them for the demo's
 * s3_storage.xml (an S3 disk `s3` and the cache disk `s3cache` over it). */
const policies = [
  { host: 'ch-1', policy_name: 'default', volume_name: 'default', volume_priority: 1, disks: ['default'] },
  { host: 'ch-1', policy_name: 's3tiered', volume_name: 'default', volume_priority: 1, disks: ['default'] },
  { host: 'ch-1', policy_name: 's3tiered', volume_name: 's3cached', volume_priority: 2, disks: ['s3cache'] },
  { host: 'ch-1', policy_name: 's3only', volume_name: 'main', volume_priority: 1, disks: ['s3cache'] },
];
const disks = [
  { host: 'ch-1', name: 'default', type: 'Local', object_storage_type: 'None', is_remote: 0, path: '/var/lib/clickhouse/', cache_path: '' },
  { host: 'ch-1', name: 's3', type: 'ObjectStorage', object_storage_type: 'S3', is_remote: 1, path: '/var/lib/clickhouse/disks/s3/', cache_path: '' },
  { host: 'ch-1', name: 's3cache', type: 'ObjectStorage', object_storage_type: 'S3', is_remote: 1, path: '/var/lib/clickhouse/disks/s3/', cache_path: '/var/lib/clickhouse/disks/s3_cache/' },
];
const table = (name: string, storage_policy: string, engine_full: string) => ({ host: 'ch-1', database: 'db', name, engine: 'MergeTree', storage_policy, engine_full });
const tables = [
  table('events', 's3tiered', "MergeTree ORDER BY k TTL d + toIntervalDay(7) TO VOLUME 's3cached', d + toIntervalDay(90) SETTINGS storage_policy = 's3tiered', index_granularity = 8192"),
  table('archive', 's3only', "MergeTree ORDER BY k SETTINGS storage_policy = 's3only', index_granularity = 8192"),
  table('local', 'default', 'MergeTree ORDER BY k SETTINGS index_granularity = 8192'),
];
const t = (name: string) => JSON.stringify(['c', 'table', 'db', name]);

describe('remote disks of storage policies', () => {
  it('links tables to the disk a cache disk caches, labeled by the TTL move or the volume', () => {
    const { observations, relationships } = diskLocations('c', tables, { policies, disks,
      buckets: [{ host: 'ch-1', disk_name: 's3', buckets: ['clickhouse'] }] });
    expect(observations.map(o => [o.name, o.attributes])).toEqual([['S3 disk · s3',
      { server: 'S3 disk · s3', backend: 'S3 disk', disk: 's3', tables: 2, cache_disks: ['s3cache'], buckets: ['clickhouse'] }]]);
    expect(relationships.map(r => [r.from, r.label, r.evidence])).toEqual([
      [t('events'), 'moves after TTL d + toIntervalDay(7)', { method: 'storage-policy', policy: 's3tiered', volume: 's3cached', disk: 's3cache', ttl: ['d + toIntervalDay(7)'] }],
      [t('archive'), 'stored on', { method: 'storage-policy', policy: 's3only', volume: 'main', disk: 's3cache' }],
    ]);
  });
  it('shows the disk, cache and bucket on the card, and says the endpoint is not exposed', () => {
    const stored = diskLocations('c', tables, { policies, disks, buckets: [] });
    const capture = { hosts: [{ host: 'ch-1', version: '26.10' }], relationships: stored.relationships, coverage: [],
      observations: [...stored.observations, ...tables.map(row => ({ key: t(row.name), kind: 'table', name: `db.${row.name}`, host: 'ch-1', source: 'tables', attributes: { ...row, ttl_rules: parseTTLRules(row.engine_full) } }))] } as unknown as Capture;
    const graph = projectCatalogGraph(capture);
    const { card, facts } = describeCard(graph.nodes.find(node => node.key === stored.observations[0].key)!, graph);
    expect(card.label).toBe('S3 disk');
    expect(facts).toEqual([{ label: 'Tables', value: '2' }, { label: 'Disk', value: 's3' }, { label: 'Cached by', value: 's3cache' },
      { label: 'Bucket', value: 'not in system.blob_storage_log' }, { label: 'Endpoint', value: 'not in system tables' }]);
    // The table shows how data reaches the disk, in its detail and in its hover.
    const events = graph.nodes.find(node => node.key === t('events'))!;
    const storage = describeDetail(events, graph).sections.flatMap(section => section.kind === 'relationships' ? section.groups : [])
      .find(group => group.title === 'Storage')!;
    expect(storage.rows.map(row => [row.relation, row.items.map(item => item.name)])).toEqual([['Moves after TTL d + toIntervalDay(7)', ['S3 disk · s3']]]);
    expect(graph.edges.map(edge => [edge.from, edgeCaption(edge)])).toEqual([[t('events'), '7 d'], [t('archive'), undefined]]);
    const lifecycle = describeDetail(events, graph).sections.find(section => section.kind === 'lifecycle');
    expect(lifecycle).toMatchObject({ steps: [
      { when: '7 days after d', action: 'Move parts to volume s3cached', target: { key: stored.observations[0].key, name: 'S3 disk · s3' } },
      { when: '90 days after d', action: 'Delete rows' }] });
    expect(describeCard(events, graph).card.badge).toEqual({ text: 'TTL', detail: 'd + toIntervalDay(7) · move to volume s3cached\nd + toIntervalDay(90) · delete' });
    expect(describeCard(graph.nodes.find(node => node.key === t('archive'))!, graph).card.badge).toBeUndefined();
    expect(describeCard(events, graph).card.hover.definition).toEqual({ label: 'TTL', text: 'd + toIntervalDay(7) · move to volume s3cached\nd + toIntervalDay(90) · delete' });
  });
});
