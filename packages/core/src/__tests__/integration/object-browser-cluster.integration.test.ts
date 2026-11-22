import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { startCluster, stopCluster, type ClusterTestContext } from './setup/cluster-container.js';
import { accessFixtureQueries } from '../../queries/access-fixture-queries.js';
import { ClusterAwareAdapter } from '../../adapters/cluster-adapter.js';
import { collectObjects } from '../../services/object-browser/collectors/collect.js';
import { projectCatalogGraph, provenancePaths } from '../../services/object-browser/projections/graph.js';
import { objectKey, type Capture } from '../../services/object-browser/model/types.js';

describe('Object browser real cluster collection', { tags: ['cluster', 'security'] }, () => {
  const fixture = accessFixtureQueries(`th_object_model_cluster_${Date.now().toString(36)}`);
  const input = { connection: 'cluster-proof', principal: fixture.alice, database: fixture.db, table: 'trips', operation: 'SELECT' as const };
  let ctx: ClusterTestContext;
  let adapter: ClusterAwareAdapter;
  let capture: Capture;
  beforeAll(async () => {
    ctx = await startCluster();
    for (const client of [ctx.client1, ctx.client2]) for (const sql of fixture.setup) await client.command({ query: sql });
    adapter = new ClusterAwareAdapter(ctx.adapter1);adapter.setClusterName('default');
    capture = await collectObjects(adapter, input, { mode: 'catalog' });
    console.info('Object cluster proof:', JSON.stringify({ hosts: capture.hosts, sources: capture.coverage }));
  }, 180_000);
  afterAll(async () => {
    if (!ctx) return;
    try {
      for (const client of [ctx.client1, ctx.client2]) for (const sql of fixture.teardown) await client.command({ query: sql });
    } finally { await stopCluster(ctx); }
  }, 60_000);
  it('reads all six sources on both replicas through clusterAllReplicas', () => {
    expect(capture.hosts).toHaveLength(2);
    expect(new Set(capture.hosts.map(host => host.host)).size).toBe(2);
    expect(capture.coverage).toHaveLength(6);
    for (const source of capture.coverage) { expect(source.state).toBe('readable');expect(source.hosts).toHaveLength(2); }
  });
  it('draws one logical user with both native records rather than merging attributes', () => {
    const graph = projectCatalogGraph(capture);
    const users = graph.nodes.filter(node => node.kind === 'user' && node.name === fixture.alice);
    expect(users).toHaveLength(1);expect(users[0].records).toHaveLength(2);
    expect(users[0].observedHosts).toHaveLength(2);
    // Independent CREATE USER assigns different native IDs: this must be visible.
    expect(new Set(users[0].records.map(record => record.attributes.id)).size).toBe(2);
    expect(users[0].differs).toBe(true);
  });
  it('preserves both replicas on an actual nested-role provenance path', () => {
    const target = objectKey(input.connection,'table',fixture.db,'trips');
    const graph = projectCatalogGraph(capture, target);
    const paths = provenancePaths(graph, objectKey(input.connection,'user',fixture.alice), target);
    expect(paths.length).toBeGreaterThan(0);
    // Role UUID differences may produce separate edges/paths; their host union
    // must still account for both replicas without inventing common evidence.
    expect(new Set(paths.flatMap(path => path.hosts)).size).toBe(2);
    expect(capture.verdict.kind).toBe('not-checked');
  });
  it('does not join a user assignment on one real host to a grant on the other', async () => {
    await ctx.client1.command({ query: fixture.removeReaderGrant });
    await ctx.client2.command({ query: fixture.removeAliceAssignment });
    const split = await collectObjects(adapter, input, { mode: 'catalog' });
    const target = objectKey(input.connection,'table',fixture.db,'trips');
    const graph = projectCatalogGraph(split, target);
    expect(provenancePaths(graph, objectKey(input.connection,'user',fixture.alice), target)).toEqual([]);
    expect(graph.nodes.find(node => node.name === fixture.alice)!.differs).toBe(true);
    expect(split.verdict.kind).toBe('not-checked');
  });
});
