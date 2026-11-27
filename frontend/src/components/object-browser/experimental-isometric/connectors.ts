import { isRevokeEdge, RELATIONSHIPS, type GraphEdge, type ObjectGraph } from '@tracehouse/core/services/object-browser';
import type { Rect, SpatialScene } from './layout';

export interface SceneConnector { key: string; edges: GraphEdge[]; from: Rect; to: Rect; d: string; family: string; revoke: boolean; caption: string }
const centerY = (r: Rect) => r.y + r.height / 2;

function path(a: Rect, b: Rect): string {
  const sy = centerY(a), ty = centerY(b);
  if (Math.abs(a.x - b.x) < 1) {
    const rail = a.x + a.width + 12;
    return `M${a.x + a.width} ${sy} C${rail} ${sy} ${rail} ${ty} ${b.x + b.width} ${ty}`;
  }
  const forward = b.x > a.x, sx = forward ? a.x + a.width : a.x, tx = forward ? b.x : b.x + b.width;
  const mid = (sx + tx) / 2;
  return `M${sx} ${sy} C${mid} ${sy} ${mid} ${ty} ${tx} ${ty}`;
}

/** Bundles are drawing records only. Preserve every original edge, including host
 * evidence and revoke effect, so a collapsed header cannot turn a revoke into a grant. */
export function sceneConnectors(graph: ObjectGraph, scene: SpatialScene, essential = true, selected: string | null = null): SceneConnector[] {
  const batches = new Map<string, SceneConnector>();
  const byKey = new Map(graph.nodes.map(n => [n.key, n]));
  const databaseGroups = new Map(scene.groups.filter(g => g.group.database !== undefined).map(g => [g.group.database!, g]));
  for (const edge of graph.edges) {
    const from = scene.anchors.get(edge.from);
    let to = scene.anchors.get(edge.to);
    if (!from || !to || from === to) continue;
    const source = byKey.get(edge.from);
    // Broad scopes run to a database header. Exact scopes and revokes retain their
    // targets. No target databases or table matches are inferred by the renderer.
    if (essential && edge.to !== selected && edge.kind === 'candidate-scope' && !isRevokeEdge(edge) && source?.records.some(r => r.attributes.table == null)) {
      const member = scene.groups.find(g => g.group.database !== undefined && g.group.keys.includes(edge.to));
      const box = member && databaseGroups.get(member.group.database!);
      if (box) to = { x: box.x + 16, y: box.y + 16, width: box.width - 32, height: 32 };
    }
    const family = RELATIONSHIPS[edge.kind].family, revoke = isRevokeEdge(edge);
    const key = JSON.stringify([from.x, from.y, to.x, to.y, edge.kind, revoke]);
    const previous = batches.get(key);
    if (previous) { previous.edges.push(edge);continue; }
    batches.set(key, { key, edges: [edge], from, to, d: path(from, to), family, revoke, caption: edge.label });
  }
  return [...batches.values()];
}
