import { describe, expect, it } from 'vitest';
import { projectCatalogGraph, type GraphNode, type ObjectGraph } from '@tracehouse/core/services/object-browser';
import { captureFixture, key } from '../../shared/__tests__/captureFixture';
import { groupObjects } from '../grouping';
import { layoutScene, PAGE_SIZE } from '../layout';
import { sceneConnectors } from '../connectors';
import { projectedBounds } from '../useCamera';

describe('spatial aggregation and model-space geometry', () => {
  it('groups equal role profiles without merging direct grants or host/default-role differences', () => {
    const capture = captureFixture();
    for (const host of ['ch-1','ch-2']) for (const name of ['direct_reader','direct_writer']) capture.observations.push({ key: key('user', name), name, kind: 'user', source: 'users', host, attributes: { default_roles_all: 1 } });
    capture.relationships.push({ id: 'direct', host: 'ch-1', kind: 'grant', from: key('user', 'direct_reader'), to: key('grant-scope', 'exact'), label: 'grant', source: 'grants', evidence: {} });
    // Give the same own grant on both hosts so this user is comparable. The other
    // user has no direct grant; grouping still only claims equal role assignments.
    capture.relationships.push({ ...capture.relationships.at(-1)!, id: 'direct-2', host: 'ch-2' });
    const graph = projectCatalogGraph(capture);
    const snapshot = JSON.stringify(graph);
    const group = groupObjects(graph).find(g => g.kind === 'user-set')!;
    expect(group.keys).toEqual([key('user','direct_reader'), key('user','direct_writer')]);
    expect(group.note).toContain('Direct grants can differ');
    expect(JSON.stringify(graph)).toBe(snapshot);
    const different = { ...graph, nodes: graph.nodes.map(n => n.key === key('user','direct_reader') ? { ...n, partial: true } : n) };
    expect(groupObjects(different).some(g => g.kind === 'user-set')).toBe(false);
  });
  it('keeps database ownership, bounded display and original anchors when groups collapse', () => {
    const graph = projectCatalogGraph(captureFixture(), key('user','bob'));
    const groups = groupObjects(graph), open = new Set(groups.map(g => g.id));
    const scene = layoutScene(graph, groups, open, new Map(), null);
    expect(scene.tiles.length).toBe(graph.nodes.length);
    const storage = scene.groups.find(g => g.group.kind === 'external')!;
    expect(storage).toBeTruthy();
    expect(storage.x).toBeGreaterThan(Math.max(...scene.groups.filter(g => g.group.kind === 'database').map(g => g.x + g.width)));
    expect(storage.y).toBe(38);
    expect(new Set(groups.flatMap(g => g.keys)).size).toBe(graph.nodes.length);
    for (const tile of scene.tiles) {
      const box = scene.groups.find(g => g.group.id === tile.group)!;
      expect(tile.x).toBeGreaterThanOrEqual(box.x);
      expect(tile.y + tile.height).toBeLessThanOrEqual(box.y + box.height);
      expect(tile.x + tile.width).toBeLessThanOrEqual(box.x + box.width);
    }
    const closed = layoutScene(graph, groups, new Set(), new Map(), null);
    expect(closed.tiles).toHaveLength(0);
    expect(closed.anchors.size).toBe(graph.nodes.length);
    expect(sceneConnectors(graph, closed).flatMap(c => c.edges).length).toBeGreaterThan(0);
  });
  it('keeps revokes distinct from bundled broad matches and retains native edge evidence', () => {
    const graph = projectCatalogGraph(captureFixture(), key('user','bob'));
    const groups = groupObjects(graph), scene = layoutScene(graph, groups, new Set(groups.map(g => g.id)), new Map(), null);
    const connectors = sceneConnectors(graph, scene);
    const broad = connectors.find(c => c.edges.some(e => e.from === key('grant-scope','broad')))!;
    expect(broad.edges.length).toBeGreaterThan(1);
    expect(broad.revoke).toBe(false);
    expect(connectors.some(c => c.revoke)).toBe(true);
    expect(broad.edges.every(e => e.records.length > 0)).toBe(true);
    const full = sceneConnectors(graph, scene, false).filter(c => c.edges.some(e => e.from === key('grant-scope','broad')));
    expect(full.length).toBeGreaterThan(1);
    expect(full.flatMap(c => c.edges).map(e => e.key).sort()).toEqual(broad.edges.map(e => e.key).sort());
  });
  it('bounds large groups, reveals a selected off-page object, and produces deterministic finite geometry with cycles', () => {
    const base = projectCatalogGraph(captureFixture());
    const template = base.nodes.find(n => n.kind === 'table')!;
    const nodes: GraphNode[] = Array.from({ length: PAGE_SIZE + 10 }, (_, i) => ({ ...template,
      key: key('table','big',String(i).padStart(2,'0')), name: `big.${i}` }));
    const graph: ObjectGraph = { nodes, edges: [
      { ...base.edges[0], key: 'cycle1', from: nodes[0].key, to: nodes[1].key, kind: 'reads-from' },
      { ...base.edges[0], key: 'cycle2', from: nodes[1].key, to: nodes[0].key, kind: 'reads-from' },
    ] };
    const groups = groupObjects(graph), expanded = new Set(groups.map(g => g.id));
    const scene = layoutScene(graph, groups, expanded, new Map(), nodes.at(-1)!.key);
    expect(scene.tiles).toHaveLength(PAGE_SIZE + 1);
    expect(scene.tiles.some(t => t.key === nodes.at(-1)!.key)).toBe(true);
    expect(Number.isFinite(scene.height)).toBe(true);
    expect(layoutScene(graph, groups, expanded, new Map(), nodes.at(-1)!.key)).toEqual(scene);
    const bounds = projectedBounds(scene.width, scene.height, 30, -12);
    expect(bounds.width).toBeGreaterThan(0);expect(bounds.height).toBeGreaterThan(0);
  });
  it('bounds initial drawing across many databases without moving cross-database dependencies into another owner', () => {
    const base = projectCatalogGraph(captureFixture()), template = base.nodes.find(n => n.kind === 'table')!;
    const nodes = Array.from({ length: 600 }, (_, i) => ({ ...template,
      key: key('table', `db${Math.floor(i / 20)}`, `t${i}`), name: `db${Math.floor(i / 20)}.t${i}` }));
    const cross = { ...base.edges[0], key: 'cross-database', from: nodes[0].key, to: nodes[20].key, kind: 'reads-from' as const };
    const graph = { nodes, edges: [cross] }, groups = groupObjects(graph);
    const scene = layoutScene(graph, groups, new Set(groups.map(g => g.id)), new Map(), null);
    expect(scene.tiles.length).toBeLessThanOrEqual(240);
    expect(groups.find(g => g.database === 'db0')!.keys).toContain(nodes[0].key);
    expect(groups.find(g => g.database === 'db1')!.keys).toContain(nodes[20].key);
    expect(groups.find(g => g.database === 'db0')!.keys).not.toContain(nodes[20].key);
    expect(sceneConnectors(graph, scene).some(c => c.edges.includes(cross))).toBe(true);
  });
  it('pages privileges across the entire scope column in twenties and packs each scope compactly', () => {
    const template = projectCatalogGraph(captureFixture()).nodes.find(n => n.kind === 'grant-scope')!;
    const nodes = Array.from({ length: 70 }, (_, i) => ({ ...template, key: key('grant-scope', `privilege-${i}`), name: `privilege-${i}`,
      records: template.records.map(r => ({ ...r, attributes: { ...r.attributes, database: `scope-${Math.floor(i / 35)}` } })) }));
    const graph = { nodes, edges: [] }, groups = groupObjects(graph), open = new Set(groups.map(g => g.id));
    const first = layoutScene(graph, groups, open, new Map(), null);
    expect(first.tiles).toHaveLength(20);
    expect(new Set(first.tiles.map(t => t.x)).size).toBe(3);
    expect(first.height).toBeLessThan(1100);
    expect(layoutScene(graph, groups, open, new Map(), null, 40).tiles).toHaveLength(40);
    expect(layoutScene(graph, groups, open, new Map(), nodes.at(-1)!.key).tiles.some(t => t.key === nodes.at(-1)!.key)).toBe(true);
  });
});
