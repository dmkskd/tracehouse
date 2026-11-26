import type { Coverage, GraphEdge, GraphNode, ObjectGraph, ObjectKind, Source } from '../model/types.js';
import { RELATIONSHIPS } from '../model/relationships.js';
import { isAccessType, objectTypeOf, type ObjectType } from './cards.js';
import { lineage } from '../projections/focus.js';
import { databaseOf } from '../projections/filters.js';

/** Access configuration, ClickHouse data objects, and external storage. */
export type LayoutBand = 'access' | 'data' | 'external';
/** Column keys are object keys, plus `isGap` keys for empty rows and `isWaypoint` keys for lines. */
/** `notices` explain what the column lacks, such as a source without SELECT (withSourceNotices).
 * `total` counts the column's objects and `hidden` those past the catalog bound (layoutCatalog). */
export interface LayoutColumn { title?: string; band: LayoutBand; keys: string[]; notices?: string[]; total?: number; hidden?: number }
/** Title shown once over the first column of each band. */
export const BAND_TITLES: Record<LayoutBand, string> = { access: 'Access', data: 'Data', external: 'External' };
const GAP = 'gap:';
/** An empty row a column keeps so the rows of neighboring columns line up. */
export const isGap = (key: string) => key.startsWith(GAP);

/** Catalog lane and band per object kind, in lane order. Kinds sharing a lane title
 * share a lane. A row policy sits with the tables it filters. */
const KIND_LAYOUT: Record<ObjectKind, { lane: string; band: LayoutBand }> = {
  user: { lane: 'Users', band: 'access' }, role: { lane: 'Roles', band: 'access' }, 'grant-scope': { lane: 'Privileges', band: 'access' },
  table: { lane: 'Tables / row policies', band: 'data' }, 'row-policy': { lane: 'Tables / row policies', band: 'data' },
  location: { lane: 'Storage', band: 'external' }, disk: { lane: 'Storage', band: 'external' },
};
const KIND_ORDER = Object.keys(KIND_LAYOUT) as ObjectKind[];

/** Catalog layout. Access objects (users, roles, privileges) are lists, so each kind
 * keeps its KIND_LAYOUT lane in collection order. Data objects (tables, views, storage)
 * are laid out as in focus, so chains read left to right. Every column is bounded to an
 * equal share of `maxNodes` (catalogPageSize), raised by that share for each page in `pages[i]`
 * for column `i`; the selected object and its lineage are kept past the bound so a search result
 * and its connections stay reachable, and selection never reorders cards. Access objects in
 * `later` (privilegesOutside) follow the rest of their lane, so the bound keeps the others;
 * those connected to the selection stay in place, next to the lines that reach them. */
export function layoutCatalog(graph: ObjectGraph, selected: string | null, maxNodes = 100, pages: readonly number[] = [], later: ReadonlySet<string> = new Set(), groupDatabases = true): LayoutColumn[] {
  const perColumn = catalogPageSize(maxNodes);
  // Gaps do not count toward the bound, and trailing gaps are dropped. The selected object
  // and everything connected to it stay past the bound, so selecting a user shows its grants.
  const connected = selected ? lineage(graph, selected) : new Set<string>();
  const bound = (column: LayoutColumn, index: number): LayoutColumn => {
    const limit = perColumn * (1 + (pages[index] ?? 0));
    let shown = 0;
    const kept = column.keys.filter(key => isGap(key) || shown++ < limit || connected.has(key));
    while (kept.length && isGap(kept[kept.length - 1])) kept.pop();
    const total = column.keys.filter(key => !isGap(key)).length;
    return { ...column, keys: kept, total, hidden: total - kept.filter(key => !isGap(key)).length };
  };
  const access = LANES.filter(([, band]) => band === 'access').map(([title, band]): LayoutColumn => ({ title, band,
    keys: graph.nodes.filter(n => KIND_LAYOUT[n.kind].lane === title).map(n => n.key)
      .sort((a, b) => Number(later.has(a) && !connected.has(a)) - Number(later.has(b) && !connected.has(b))) }));
  const data = new Set(graph.nodes.filter(n => KIND_LAYOUT[n.kind].band !== 'access').map(n => n.key));
  const flow = layoutFocus({ nodes: graph.nodes.filter(n => data.has(n.key)), edges: graph.edges.filter(e => data.has(e.from) && data.has(e.to)) });
  const columns = [...access, ...flow].map(bound);
  return groupDatabases ? alignCatalogDatabases(graph, columns) : columns;
}

/** Group only displayed data cards into database bands after pagination. Column ranks
 * still describe dependencies, including cross-database views; ownership only sets rows.
 * Hidden cards reserve no rows. Each database starts with an object at its band's top. */
function alignCatalogDatabases(graph: ObjectGraph, columns: LayoutColumn[]): LayoutColumn[] {
  const nodes = new Map(graph.nodes.map(n => [n.key, n]));
  const groups = new Map<string, Map<number, string[]>>();
  columns.forEach((column, c) => {
    if (column.band !== 'data') return;
    for (const key of column.keys) {
      const node = nodes.get(key), database = node && databaseOf(node);
      if (database === undefined) continue;
      if (!groups.has(database)) groups.set(database, new Map());
      const group = groups.get(database)!;
      group.set(c, [...group.get(c) ?? [], key]);
    }
  });
  const rows = new Map<number, string[]>();
  let start = 0;
  for (const group of groups.values()) {
    for (const [c, keys] of group) {
      const out = rows.get(c) ?? [];
      while (out.length < start) out.push(`${GAP}database:${c}:${out.length}`);
      out.push(...keys);rows.set(c, out);
    }
    start += Math.max(...[...group.values()].map(keys => keys.length));
  }
  return columns.map((column, c) => column.band === 'data' ? { ...column, keys: rows.get(c) ?? [] } : column);
}
const LANES = [...new Map(KIND_ORDER.map(kind => [KIND_LAYOUT[kind].lane, KIND_LAYOUT[kind].band])).entries()];
/** Cards per catalog column before "more": an equal share of `maxNodes` across the lanes. */
export function catalogPageSize(maxNodes = 100): number {
  return Math.floor(maxNodes / LANES.length);
}
const COLUMN_TITLES: Partial<Record<ObjectType, string>> = {
  user: 'Users', role: 'Roles', 'grant-scope': 'Privileges', view: 'Views', 'materialized-view': 'Materialized views',
  'merge-tree': 'Tables', table: 'Tables', distributed: 'Distributed tables', dictionary: 'Dictionaries', buffer: 'Buffers',
  iceberg: 'Tables', lake: 'Tables', 'object-storage': 'Tables', location: 'Storage', disk: 'Storage',
};
/** One title per column: a single type's title, two joined ("Buffers and views"), else a general one. */
function columnTitle(titles: string[]): string | undefined {
  const distinct = [...new Set(titles)];
  if (distinct.length <= 1) return distinct[0];
  return distinct.length === 2 ? `${distinct[0]} and ${distinct[1].toLowerCase()}` : 'Data objects';
}
const SWEEPS = 4;

/** Layered layout for a focus graph: access configuration first, then data flow,
 * so a materialized view chain reads left to right. Objects in `later` (privilegesOutside)
 * follow the rest of their column. Deterministic for equal input. */
export function layoutFocus(graph: ObjectGraph, later: ReadonlySet<string> = new Set(), groupDatabases = false): LayoutColumn[] {
  const initial = [...graph.nodes].sort((a, b) =>
    KIND_ORDER.indexOf(a.kind) - KIND_ORDER.indexOf(b.kind) || (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
  const present = new Set(initial.map(n => n.key));
  // Ranking kinds come from RELATIONSHIPS: candidate matches follow the selection and
  // policies sit beside their targets, so neither moves columns.
  const candidates = graph.edges.filter(e => RELATIONSHIPS[e.kind].ranks && e.from !== e.to && present.has(e.from) && present.has(e.to));
  // Drop back edges found by DFS, so cycles such as an MV writing to its own source still rank.
  const outgoing = new Map<string, GraphEdge[]>();
  for (const e of candidates) outgoing.set(e.from, [...outgoing.get(e.from) ?? [], e]);
  const state = new Map<string, 'open' | 'done'>(), forward: GraphEdge[] = [];
  const visit = (key: string) => {
    state.set(key, 'open');
    for (const e of outgoing.get(key) ?? []) {
      if (state.get(e.to) === 'open') continue;
      forward.push(e);
      if (!state.has(e.to)) visit(e.to);
    }
    state.set(key, 'done');
  };
  for (const node of initial) if (!state.has(node.key)) visit(node.key);

  const type = new Map(initial.map(n => [n.key, objectTypeOf(n)]));
  const rank = new Map(initial.map(n => [n.key, 0]));
  const ranked = initial.filter(n => n.kind !== 'row-policy');
  const alignedKinds = new Map<string, Set<string>>();
  for (const e of graph.edges) if (RELATIONSHIPS[e.kind].alignTargets && rank.has(e.to) && rank.has(e.from)) {
    alignedKinds.set(e.kind, (alignedKinds.get(e.kind) ?? new Set()).add(e.to));
  }
  const aligned = [...alignedKinds.values()];
  for (let pass = 0; pass <= ranked.length; pass++) {
    let changed = false;
    const raise = (key: string, value: number) => { if (rank.get(key)! < value) { rank.set(key, value);changed = true; } };
    for (const e of forward) raise(e.to, rank.get(e.from)! + 1);
    // Targets of aligned kinds (RELATIONSHIPS[kind].alignTargets) share one column.
    for (const targets of aligned) {
      const column = Math.max(-1, ...[...targets].map(key => rank.get(key)!));
      for (const key of targets) raise(key, column);
    }
    // Data objects start after the access band.
    const accessEnd = Math.max(-1, ...ranked.filter(n => isAccessType(type.get(n.key)!)).map(n => rank.get(n.key)!));
    for (const n of ranked) if (!isAccessType(type.get(n.key)!)) raise(n.key, accessEnd + 1);
    if (!changed) break;
  }

  // Targets of last-column kinds (RELATIONSHIPS[kind].lastColumn) follow every other object.
  const last = new Set(graph.edges.filter(e => RELATIONSHIPS[e.kind].lastColumn && rank.has(e.to)).map(e => e.to));
  if (last.size) {
    const end = Math.max(-1, ...ranked.filter(n => !last.has(n.key)).map(n => rank.get(n.key)!)) + 1;
    for (const key of last) rank.set(key, Math.max(end, rank.get(key)!));
  }
  const ranks = [...new Set(ranked.map(n => rank.get(n.key)!))].sort((a, b) => a - b);
  const columns = ranks.map(r => ranked.filter(n => rank.get(n.key) === r).map(n => n.key));
  const position = () => new Map(columns.flatMap(c => c.map((key, i) => [key, i] as const)));
  const neighbors = (key: string, up: boolean) => forward.filter(e => (up ? e.to : e.from) === key).map(e => up ? e.from : e.to);
  for (let sweep = 0; sweep < SWEEPS; sweep++) {
    const up = sweep % 2 === 0;
    const order = up ? columns.map((_, i) => i) : columns.map((_, i) => columns.length - 1 - i);
    for (const c of order) {
      const at = position();
      const weight = (key: string) => {
        const linked = neighbors(key, up);
        return linked.length ? linked.reduce((sum, k) => sum + at.get(k)!, 0) / linked.length : at.get(key)!;
      };
      const weights = new Map(columns[c].map(key => [key, weight(key)]));
      columns[c] = [...columns[c]].sort((a, b) => weights.get(a)! - weights.get(b)!);
    }
  }
  for (let c = 0; c < columns.length; c++) columns[c] = [...columns[c]].sort((a, b) => Number(later.has(a)) - Number(later.has(b)));
  // Policies sit directly below their first target table, else in the last column.
  for (const policy of initial.filter(n => n.kind === 'row-policy')) {
    const target = graph.edges.find(e => e.kind === 'policy-target' && e.from === policy.key && present.has(e.to))?.to;
    const column = columns.find(c => target !== undefined && c.includes(target)) ?? columns[columns.length - 1];
    if (!column) { columns.push([policy.key]);continue; }
    let at = target === undefined ? column.length : column.indexOf(target) + 1;
    while (at < column.length && type.get(column[at]) === 'row-policy') at++;
    column.splice(at, 0, policy.key);
  }
  // Rows line up across columns: each object sits level with the average row of the
  // objects feeding it, pushed down only when that row is taken; gaps fill the rest.
  const row = new Map<string, number>();
  const rowed = columns.map((keys, c) => {
    const out: string[] = [];
    let next = 0;
    for (const key of keys) {
      const fed = forward.filter(e => e.to === key && row.has(e.from)).map(e => row.get(e.from)!);
      const at = Math.max(next, fed.length ? Math.round(fed.reduce((sum, r) => sum + r, 0) / fed.length) : next);
      for (let gap = next; gap < at; gap++) out.push(`${GAP}${c}:${gap}`);
      out.push(key); row.set(key, at); next = at + 1;
    }
    return out;
  });
  const kind = new Map(initial.map(n => [n.key, n.kind]));
  const laidOut: LayoutColumn[] = rowed.map(keys => {
    const objects = keys.filter(k => !isGap(k));
    const title = columnTitle(objects.filter(k => type.get(k) !== 'row-policy').flatMap(k => COLUMN_TITLES[type.get(k)!] ?? []));
    // Access objects lead the layout and storage ends it, so one object names the band.
    const band = KIND_LAYOUT[kind.get(objects.find(k => kind.get(k) !== 'row-policy') ?? objects[0])!].band;
    return title ? { title, band, keys } : { band, keys };
  });
  return groupDatabases ? alignCatalogDatabases(graph, laidOut) : laidOut;
}

/** The kind whose catalog column shows a source's objects. Role assignments show with roles. */
const SOURCE_KIND: Record<Source, ObjectKind> = {
  users: 'user', roles: 'role', role_grants: 'role', grants: 'grant-scope', row_policies: 'row-policy', tables: 'table',
};
/** Coverage shown in the diagram's columns rather than with the other coverage notes. */
export const isColumnNotice = (coverage: Coverage) => coverage.state === 'not-granted';
/** Puts each not-granted source's reason in the catalog column of its objects, so an empty
 * column says why. Access kinds have their own column; data kinds use the first data column. */
export function withSourceNotices(columns: LayoutColumn[], coverage: Coverage[]): LayoutColumn[] {
  const notices = new Map<number, string[]>();
  for (const entry of coverage.filter(isColumnNotice)) {
    const { lane, band } = KIND_LAYOUT[SOURCE_KIND[entry.source]];
    const index = columns.findIndex(column => band === 'access' ? column.title === lane : column.band === band);
    if (index >= 0) notices.set(index, [...notices.get(index) ?? [], entry.reason ?? entry.source]);
  }
  return columns.map((column, i) => notices.has(i) ? { ...column, notices: notices.get(i) } : column);
}


/** Columns for a focus with no path from its user: the Roles and Privileges columns it lacks, empty,
 * so the broken line runs across them, and a Tables column with `notice` when no data column exists,
 * so the line ends at the data divider. Other layouts pass through. */
export function withPathColumns(columns: LayoutColumn[], notice: string): LayoutColumn[] {
  const access = columns.filter(c => c.band === 'access'), rest = columns.filter(c => c.band !== 'access');
  const lane = (title: string): LayoutColumn => access.find(c => c.title === title) ?? { title, band: 'access', keys: [] };
  const others = access.filter(c => !['Users', 'Roles', 'Privileges'].includes(c.title ?? ''));
  const users = access.filter(c => c.title === 'Users');
  const data = rest.some(c => c.band === 'data') ? rest : [{ title: 'Tables', band: 'data' as const, keys: [], notices: [notice] }, ...rest];
  return [...users, lane('Roles'), ...others, lane('Privileges'), ...data];
}

const WAYPOINT = 'waypoint:';
/** A key `routeEdges` placed in a column for a line to pass through. */
export const isWaypoint = (key: string) => key.startsWith(WAYPOINT);
export interface RoutedLayout { columns: LayoutColumn[]; routes: Map<string, string[]> }

/** Gives every line that skips columns a waypoint in each column it crosses, placed
 * between cards by interpolating the two ends' positions, so lines run between cards
 * and never behind one. `routes` lists an edge's waypoints from its `from` end. */
export function routeEdges(graph: ObjectGraph, columns: LayoutColumn[]): RoutedLayout {
  const at = new Map(columns.flatMap((column, c) => column.keys.map((key, i) => [key, { c, i }] as const)));
  const added = columns.map(() => [] as { position: number; key: string }[]);
  const routes = new Map<string, string[]>();
  for (const edge of graph.edges) {
    const from = at.get(edge.from), to = at.get(edge.to);
    if (!from || !to || Math.abs(to.c - from.c) < 2) continue;
    const step = Math.sign(to.c - from.c), waypoints: string[] = [];
    for (let c = from.c + step; c !== to.c; c += step) {
      const key = `${WAYPOINT}${edge.key}:${c}`;
      // Half a slot down: the waypoint sits between the card at that row and the next one.
      added[c].push({ position: from.i + (to.i - from.i) * (c - from.c) / (to.c - from.c) + .5, key });
      waypoints.push(key);
    }
    routes.set(edge.key, waypoints);
  }
  return { routes, columns: columns.map((column, c) => ({ ...column, keys: [...column.keys.map((key, i) => ({ position: i, key })), ...added[c]]
    .sort((a, b) => a.position - b.position).map(entry => entry.key) })) };
}

/** A layout with its line routes: through waypoints when `route` is on, else none.
 * Catalog and focus both lay out this way. */
export function withRoutes(graph: ObjectGraph | null, columns: LayoutColumn[], route: boolean): RoutedLayout {
  return graph && route ? routeEdges(graph, columns) : { columns, routes: new Map() };
}
