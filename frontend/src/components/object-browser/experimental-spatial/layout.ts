import { isGap, layoutFocus, RELATIONSHIPS, type ObjectGraph } from '@tracehouse/core/services/object-browser';
import type { SpatialGroup } from './grouping';

export interface Rect { x: number; y: number; width: number; height: number }
export interface SceneTile extends Rect { key: string; group: string }
export interface SceneGroup extends Rect { group: SpatialGroup; expanded: boolean; hidden: number; tiles: SceneTile[] }
export interface SpatialScene { width: number; height: number; lanes: (Rect & { label: string })[]; groups: SceneGroup[]; tiles: SceneTile[]; anchors: Map<string, Rect> }
export const PAGE_SIZE = 24;
const TILE_WIDTH = 190, TILE_HEIGHT = 86, PAD = 16, HEADER = 104, GAP = 20;

/** Bounded, deterministic model-space geometry. No DOM measurements or camera
 * values enter layout. Cross-database edges rank objects but never change ownership. */
export function layoutScene(graph: ObjectGraph, groups: readonly SpatialGroup[], expanded: ReadonlySet<string>,
  pages: ReadonlyMap<string, number>, selected: string | null, privilegeLimit = 20): SpatialScene {
  const visible = new Map<string, string[]>();
  // Bound initial work across expanded groups as well as within each group.
  // Explicit Show more pages can exceed this initial budget, never collection limits.
  const privilegeTotal = groups.filter(g => g.kind === 'scope').reduce((n, g) => n + g.keys.length, 0);
  const others = groups.filter(g => g.kind !== 'scope' && expanded.has(g.id)).length;
  const share = Math.max(1, Math.min(PAGE_SIZE, Math.floor((240 - Math.min(20, privilegeTotal)) / Math.max(1, others))));
  let remainingPrivileges = privilegeLimit;
  for (const group of groups) {
    if (group.kind === 'scope' && remainingPrivileges <= 0 && !group.keys.includes(selected ?? '')) continue;
    const keys = !expanded.has(group.id) ? [] : group.kind === 'scope'
      ? group.keys.filter((key, i) => i < remainingPrivileges || key === selected)
      : group.keys.filter((key, i) => i < share * (1 + (pages.get(group.id) ?? 0)) || key === selected);
    if (group.kind === 'scope') remainingPrivileges = Math.max(0, remainingPrivileges - keys.length);
    visible.set(group.id, keys);
  }
  const shown = new Set([...visible.values()].flat());
  const data = new Set(graph.nodes.filter(n => shown.has(n.key) && (n.kind === 'table' || n.kind === 'row-policy')).map(n => n.key));
  const columns = layoutFocus({ nodes: graph.nodes.filter(n => data.has(n.key)), edges: graph.edges.filter(e => data.has(e.from) && data.has(e.to)) });
  const rank = new Map(columns.flatMap((c, i) => c.keys.filter(k => !isGap(k)).map(k => [k, i] as const)));
  const flowOrder = new Map(columns.flatMap(c => c.keys.map((k, i) => [k, i] as const)));
  const inFlow = new Set(graph.edges.filter(e => RELATIONSHIPS[e.kind].family === 'data-flow').flatMap(e => [e.from, e.to]));
  // The privilege lane owns its paging controls directly below its heading.
  const bottoms = [38, 38, 76, 38, 38];
  // Pack a scope's privilege cards as a compact block, as tf-view packs leaves.
  // Reserve that block's width before placing databases, rather than growing a tail.
  const scopeColumns = (count: number) => Math.max(1, Math.min(3, Math.ceil(Math.sqrt(count))));
  const privilegeColumns = Math.max(1, ...groups.filter(g => g.kind === 'scope').map(g => scopeColumns(visible.get(g.id)?.length ?? 0)));
  const privilegeWidth = privilegeColumns * TILE_WIDTH + (privilegeColumns - 1) * GAP + PAD * 2;
  const laneX = [0, TILE_WIDTH + PAD * 2 + 48, 2 * (TILE_WIDTH + PAD * 2 + 48), 2 * (TILE_WIDTH + PAD * 2 + 48) + privilegeWidth + 48];
  laneX.push(laneX[3] + 3 * TILE_WIDTH + 2 * GAP + PAD * 2 + 48);
  const placed: SceneGroup[] = [], tiles: SceneTile[] = [], anchors = new Map<string, Rect>();
  for (const group of groups) {
    if (!visible.has(group.id)) continue;
    const column = group.kind === 'users' || group.kind === 'user-set' ? 0 : group.kind === 'roles' ? 1 : group.kind === 'scope' ? 2 : group.kind === 'external' ? 4 : 3;
    const x = laneX[column], y = bottoms[column];
    const keys = visible.get(group.id) ?? [];
    const wide = column >= 3, columnsPerGroup = group.kind === 'scope' ? scopeColumns(keys.length) : wide ? 3 : 1;
    const width = columnsPerGroup * TILE_WIDTH + (columnsPerGroup - 1) * GAP + PAD * 2;
    const slots = new Map<string, { col: number; row: number }>();
    if (group.kind === 'database') {
      // Wrap dependency ranks into blocks of three columns. Every block reserves
      // enough rows for its busiest rank; unrelated objects share rank zero.
      const ranked = new Map<number, string[]>();
      const connected = keys.filter(k => inFlow.has(k)), independent = keys.filter(k => !inFlow.has(k));
      const min = Math.min(0, ...connected.map(k => rank.get(k) ?? 0));
      for (const key of connected) { const r = (rank.get(key) ?? 0) - min;ranked.set(r, [...ranked.get(r) ?? [], key]); }
      let row = 0;
      const end = Math.max(-1, ...ranked.keys());
      for (let start = 0; start <= end; start += columnsPerGroup) {
        let rows = 0;
        for (let col = 0; col < columnsPerGroup; col++) {
          const each = [...ranked.get(start + col) ?? []].sort((a, b) => (flowOrder.get(a) ?? 0) - (flowOrder.get(b) ?? 0));
          each.forEach((key, i) => slots.set(key, { col, row: row + i }));rows = Math.max(rows, each.length);
        }
        row += rows;
      }
      // Isolated tables and policies occupy a compact grid below the chains;
      // they must not add empty rows between stages of an unrelated pipeline.
      independent.forEach((key, i) => slots.set(key, { col: i % columnsPerGroup, row: row + Math.floor(i / columnsPerGroup) }));
    } else keys.forEach((key, i) => slots.set(key, { col: i % columnsPerGroup, row: Math.floor(i / columnsPerGroup) }));
    const groupTiles = keys.map(key => {
      const slot = slots.get(key)!;
      return { key, group: group.id, x: x + PAD + slot.col * (TILE_WIDTH + GAP), y: y + HEADER + slot.row * (TILE_HEIGHT + GAP), width: TILE_WIDTH, height: TILE_HEIGHT };
    });
    const rows = Math.max(0, ...[...slots.values()].map(s => s.row + 1));
    const hidden = group.keys.length - keys.length;
    const height = HEADER + rows * (TILE_HEIGHT + GAP) + (keys.length && hidden && group.kind !== 'scope' ? 28 : 0) + PAD;
    const box: SceneGroup = { group, expanded: expanded.has(group.id), hidden, tiles: groupTiles, x, y, width, height };
    placed.push(box);tiles.push(...groupTiles);
    const header = { x: x + PAD, y: y + PAD, width: width - PAD * 2, height: 32 };
    for (const key of group.keys) anchors.set(key, header);
    for (const tile of groupTiles) anchors.set(tile.key, tile);
    bottoms[column] = y + height + 32;
  }
  const labels = ['Users', 'Roles', 'Privileges by scope', 'Databases', 'External storage'];
  const lanes = [...new Set(placed.map(g => g.x))].map(x => ({ x, y: 0, width: Math.max(...placed.filter(g => g.x === x).map(g => g.width)), height: 28, label: labels[laneX.indexOf(x)] }));
  return { width: Math.max(400, ...placed.map(g => g.x + g.width)), height: Math.max(220, ...placed.map(g => g.y + g.height)), lanes, groups: placed, tiles, anchors };
}
