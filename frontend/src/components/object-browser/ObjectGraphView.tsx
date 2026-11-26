import { ObjectCardBody, ObjectHoverCard } from './ObjectCard';
import { placeHover, type Box } from './hoverPlacement';
import { createTracehousePortal } from '../../providers/TracehousePortal';
import { ZoomControls } from '../../components/common/ZoomControls';
import { EDGE_STYLES, toneStyle, REVOKE_EDGE_STYLE } from './objectStyle';
import { DIAGRAM_STYLES } from './diagramStyles';
import { accessConnector, previewWaypoints, type ConnectorLane } from './accessConnector';
import type { ObjectDiagramStyle } from '../../stores/userPreferenceStore';
import { useEffect, useId, useLayoutEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import { BAND_TITLES, databaseOf, catalogPageSize, isGap, isWaypoint, lineage, RELATIONSHIPS, type GraphNode, type GraphEdge, type RelationshipFamily, type LayoutColumn, type ObjectCard, type ObjectGraph, isRevokeEdge, edgeCaption } from '@tracehouse/core/services/object-browser';

// Card width and gap as in the mockups.
const COLUMN_WIDTH = 176, COLUMN_GAP = 40;
// Room the cut's label needs, "No role or privilege on this path" at 11px.
const CUT_LABEL_WIDTH = 200;
// Stable default: the measuring effect depends on `stubs`.
const NO_STUBS: string[] = [];
// Pause before the hover card opens, so moving across the diagram does not flash cards.
const HOVER_DELAY_MS = 350;
type Point = { left: number; right: number; y: number };

/** Pure presentation of a laid-out graph (layoutCatalog or layoutFocus); contains no
 * collection, access or layout rules. `routes` (routeEdges) pass lines through
 * waypoints, `highlight` dims lines outside the hovered card's lineage, and
 * `diagramStyle` picks how cards and lines are drawn (DIAGRAM_STYLES). */
export function ObjectGraphView({ graph, cards, columns, selected, onSelect, onFocus, onOpen, onMore, muted, covered, actions, caption, noPath, cancelledEdges = NO_STUBS, showAll = false, origin, routes, stubs = NO_STUBS, highlight = false, diagramStyle = 'cards', databases, previewEdges, routePreviews = false, onHover }: {
  /** Transient privilege matches for hover; never changes layout or selection. */
  previewEdges?: readonly GraphEdge[];
  routePreviews?: boolean;
  onHover?: (key: string | null) => void;
  databases?: { items: { name: string; count: number }[]; selected: string | null; onSelect: (name: string) => void };
  graph: ObjectGraph; cards: Map<string, ObjectCard>; columns: LayoutColumn[]; selected: string | null;
  onSelect: (key: string) => void; onFocus?: (key: string) => void; onOpen?: (key: string) => void;
  actions?: ReactNode;
  /** Shown under the cards, inside the scrolling diagram: a line about what the diagram shows. */
  caption?: ReactNode;
  /** Shows the next page of a column the catalog bound cut (LayoutColumn.hidden). */
  onMore?: (column: number) => void;
  /** Objects drawn faded, such as privileges without the catalog's operation (privilegesOutside). */
  muted?: ReadonlySet<string>;
  /** Selected access types a broad privilege includes (coveredOperations), shown on its card. */
  covered?: ReadonlyMap<string, readonly string[]>;
  /** No configured path from the user: a broken line runs from the user's row to the data
   * divider and stops there, and the table, when there is one, is outlined. */
  noPath?: { from: string; to?: string };
  /** Links of grants a revoke on the same holder cancels: drawn dashed and faint. */
  cancelledEdges?: readonly string[]; showAll?: boolean; origin?: string | null;
  routes?: Map<string, string[]>; stubs?: string[]; highlight?: boolean; diagramStyle?: ObjectDiagramStyle;
}) {
  const style = DIAGRAM_STYLES[diagramStyle];
  const noPathFrom = noPath?.from;
  const root = useRef<HTMLDivElement>(null);
  const arrowId = `object-flow-${useId().replace(/:/g, '')}`;
  const [zoom, setZoom] = useState(1);
  const databaseElements = useRef(new Map<string, HTMLButtonElement>());
  const elements = useRef(new Map<string, HTMLDivElement>());
  const [geometry, setGeometry] = useState<{ width: number; height: number;
    occluders: { x: number; y: number; width: number; height: number }[];
    paths: { key: string; from: string; to: string; d: string; family: RelationshipFamily; revoke: boolean; database?: string;
      /** Text drawn at the target end (edgeCaption), with that end point and the side the line arrives from. */
      caption?: { text: string; x: number; y: number; fromLeft: boolean } }[];
    stubs: { from: string; d: string; x: number; y: number }[]; bracket: { x: number; top: number; bottom: number } | null;
    cut: { from: number; to: number; y: number; label: boolean } | null }>({ width: 0, height: 0, occluders: [], paths: [], stubs: [], bracket: null, cut: null });
  const [hovered, setHovered] = useState<string | null>(null);
  // Hover card: the card under the pointer after HOVER_DELAY_MS, measured then placed beside it.
  const [tip, setTip] = useState<{ key: string; anchor: Box } | null>(null);
  const tipTimer = useRef<ReturnType<typeof setTimeout>>();
  const showTip = (key: string, element: HTMLElement) => {
    clearTimeout(tipTimer.current);
    tipTimer.current = setTimeout(() => setTip({ key, anchor: element.getBoundingClientRect() }), HOVER_DELAY_MS);
  };
  const hideTip = () => { clearTimeout(tipTimer.current);setTip(null); };
  useEffect(() => () => clearTimeout(tipTimer.current), []);
  // Placed once mounted and measured, then shown (placeHover).
  const placeTip = (element: HTMLDivElement | null) => {
    if (!element || !tip) return;
    const { left, top } = placeHover(tip.anchor, { width: element.offsetWidth, height: element.offsetHeight }, { width: window.innerWidth, height: window.innerHeight });
    element.style.left = `${left}px`;element.style.top = `${top}px`;element.style.visibility = 'visible';
  };
  // Scrolling moves the card away from its anchor; a press starts selecting, dragging or opening.
  useEffect(() => {
    if (!tip) return;
    const hide = () => { clearTimeout(tipTimer.current);setTip(null); };
    window.addEventListener('scroll', hide, true);window.addEventListener('mousedown', hide, true);
    return () => { window.removeEventListener('scroll', hide, true);window.removeEventListener('mousedown', hide, true); };
  }, [tip]);
  const connected = useMemo(() => selected ? lineage(graph, selected) : new Set<string>(), [graph, selected]);
  const previewGraph = useMemo(() => {
    if (!hovered || !previewEdges) return graph;
    const present = new Set(graph.nodes.map(n => n.key));
    const edges = new Map(graph.edges.map(e => [e.key, e]));
    for (const edge of previewEdges) if (present.has(edge.from) && present.has(edge.to)) edges.set(edge.key, edge);
    return { ...graph, edges: [...edges.values()] };
  }, [graph, hovered, previewEdges]);
  const hoverConnected = useMemo(() => hovered ? lineage(previewGraph, hovered) : new Set<string>(), [previewGraph, hovered]);
  const databaseSelected = databases?.selected;
  const databaseMembers = new Set(graph.nodes.filter(n => databaseSelected && databaseOf(n) === databaseSelected).map(n => n.key));
  const otherDatabaseMembers = new Set(graph.nodes.filter(n => databaseSelected && databaseOf(n) !== undefined && databaseOf(n) !== databaseSelected).map(n => n.key));
  const outsideDatabase = (key: string) => otherDatabaseMembers.has(key);
  // Hover previews the same lineage for cards and connectors, then restores the
  // persistent object/database selection when the pointer leaves.
  const dimmed = (key: string) => highlight && hovered
    ? !hoverConnected.has(key)
    : outsideDatabase(key) || (!showAll && !!selected && !connected.has(key));
  const activeDatabases = new Set(graph.nodes.filter(n => n.kind === 'table' && !dimmed(n.key) && !muted?.has(n.key)).map(databaseOf));
  const linked = (from: string, to: string) => highlight && hovered
    ? hoverConnected.has(from) && hoverConnected.has(to)
    : databaseSelected ? !outsideDatabase(from) && !outsideDatabase(to) : showAll || !selected || (connected.has(from) && connected.has(to));
  const byKey = new Map(graph.nodes.map(n => [n.key, n]));
  const shown: { databaseColumn?: boolean; columnIndex?: number; title?: string; band: LayoutColumn['band']; notices?: string[]; total?: number; hidden?: number; items: (GraphNode | string)[] }[] = columns.map((c, columnIndex) => ({ columnIndex, title: c.title, band: c.band, notices: c.notices, total: c.total, hidden: c.hidden,
    items: c.keys.flatMap((k): (GraphNode | string)[] => isWaypoint(k) || isGap(k) ? [k] : [byKey.get(k)].filter((n): n is GraphNode => !!n)) }));
  const nodes = shown.flatMap(c => c.items.filter((item): item is GraphNode => typeof item !== 'string'));
  const visibleDatabaseCounts = new Map<string, number>();
  for (const node of nodes) {
    const database = databaseOf(node);
    if (node.kind === 'table' && database !== undefined) visibleDatabaseCounts.set(database, (visibleDatabaseCounts.get(database) ?? 0) + 1);
  }
  const visibleDatabases = databases?.items.filter(db => visibleDatabaseCounts.has(db.name)) ?? [];
  if (databases?.items.length) {
    const firstData = shown.findIndex(c => c.band === 'data');
    shown.splice(firstData < 0 ? shown.filter(c => c.band === 'access').length : firstData, 0,
      { title: 'Databases', band: 'data', items: [], databaseColumn: true, total: databases.items.length, hidden: databases.items.length - visibleDatabases.length });
  }
  const lanesWidth = shown.length * COLUMN_WIDTH + Math.max(0, shown.length - 1) * COLUMN_GAP;
  // Bring the selected card into view, e.g. after picking a search result. Cards already visible stay put.
  useEffect(() => {
    if (selected) elements.current.get(selected)?.scrollIntoView?.({ block: 'nearest', inline: 'nearest', behavior: 'smooth' });
  }, [selected]);
  useLayoutEffect(() => {
    const measure = () => {
      if (!root.current) return;
      const bounds = root.current.getBoundingClientRect();
      // Measure actual card tops: notices, routed waypoints and diagram styles can
      // change pixel positions even when the logical row is the same.
      const databaseLane = root.current.querySelector<HTMLElement>('[data-databases] .lane-items');
      if (databaseLane) {
        const laneTop = databaseLane.getBoundingClientRect().top;
        let bottom = 0;
        for (const [database, element] of databaseElements.current) {
          const tops = graph.nodes.filter(n => n.kind === 'table' && databaseOf(n) === database)
            .flatMap(n => { const el = elements.current.get(n.key);return el ? [el.getBoundingClientRect().top] : []; });
          if (!tops.length) continue;
          const top = (Math.min(...tops) - laneTop) / zoom;
          element.style.top = `${top}px`;
          bottom = Math.max(bottom, top + element.offsetHeight);
        }
        databaseLane.style.minHeight = `${bottom}px`;
      }
      const point = (key: string): Point | null => {
        const r = elements.current.get(key)?.getBoundingClientRect();
        return r ? { left: (r.left - bounds.left) / zoom, right: (r.right - bounds.left) / zoom, y: (r.top + r.height / 2 - bounds.top) / zoom } : null;
      };
      const baseEdges = new Set(graph.edges.map(edge => edge.key));
      const lanes: ConnectorLane[] = routePreviews ? [...root.current.querySelectorAll<HTMLElement>('.lane:not([data-databases])')].map(lane => {
        const r = lane.getBoundingClientRect();
        return { left: (r.left - bounds.left) / zoom, right: (r.right - bounds.left) / zoom,
          cards: [...lane.querySelectorAll<HTMLElement>('.node')].map(card => {
            const box = card.getBoundingClientRect();
            return { top: (box.top - bounds.top) / zoom, bottom: (box.bottom - bounds.top) / zoom };
          }) };
      }) : [];
      const waypointsFor = (edge: GraphEdge, from: Point, to: Point) => {
        const routed = routes?.get(edge.key);
        return routed ? routed.flatMap(key => { const p = point(key);return p ? [p] : []; })
          : routePreviews && !baseEdges.has(edge.key) ? previewWaypoints(from, to, lanes) : [];
      };
      const paths = previewGraph.edges.flatMap<(typeof geometry.paths)[number]>(edge => {
        const a = point(edge.from), b = point(edge.to);
        if (!a || !b) return [];
        const sameLane = Math.abs(b.left - a.left) < 1;
        const forward = b.left > a.left;
        const sx = forward || sameLane ? a.right : a.left, tx = forward && !sameLane ? b.left : b.right;
        const family = RELATIONSHIPS[edge.kind].family, revoke = isRevokeEdge(edge);
        const target = byKey.get(edge.to);
        const database = target && databaseOf(target);
        const databaseRect = database !== undefined ? databaseElements.current.get(database)?.getBoundingClientRect() : undefined;
        if (edge.kind === 'candidate-scope' && databaseRect && target?.kind === 'table') {
          const stop = { left: (databaseRect.left - bounds.left) / zoom, right: (databaseRect.right - bounds.left) / zoom,
            y: (databaseRect.top + databaseRect.height / 2 - bounds.top) / zoom };
          const waypoints = waypointsFor(edge, stop, b);
          return [{ key: edge.key, from: edge.from, to: edge.to, family, revoke, database,
            d: accessConnector(a, stop, b, style.segment, waypoints) }];
        }
        if (sameLane) return [{ key: edge.key, from: edge.from, to: edge.to, family, revoke, d: `M${sx} ${a.y} C${sx + 28} ${a.y} ${sx + 28} ${b.y} ${tx} ${b.y}` }];
        // Through each waypoint: enter on the near side, cross it level, leave on the far side.
        let d = `M${sx} ${a.y}`, x = sx, y = a.y;
        for (const waypoint of waypointsFor(edge, a, b)) {
          const [enter, leave] = forward ? [waypoint.left, waypoint.right] : [waypoint.right, waypoint.left];
          d += `${style.segment(x, y, enter, waypoint.y)} L${leave} ${waypoint.y}`;
          x = leave; y = waypoint.y;
        }
        const text = edgeCaption(edge), caption = text ? { text, x: tx, y: b.y, fromLeft: forward } : undefined;
        return [{ key: edge.key, from: edge.from, to: edge.to, family, revoke, d: d + style.segment(x, y, tx, b.y), ...(caption ? { caption } : {}) }];
      });
      // Essential lines: each broad privilege runs to the divider before the data band and meets
      // a bracket spanning that band's objects, so the stub reads as reaching all of them.
      const lane = root.current.querySelector('.lane[data-band="data"]');
      const items = lane?.querySelector('.lane-items')?.getBoundingClientRect();
      const divider = lane ? (lane.getBoundingClientRect().left - bounds.left) / zoom - COLUMN_GAP / 2 : null;
      const drawnStubs = divider === null ? [] : stubs.flatMap(from => {
        const a = point(from);
        return a ? [{ from, d: `M${a.right} ${a.y} L${divider} ${a.y}`, x: divider, y: a.y }] : [];
      });
      const bracket = divider !== null && items && drawnStubs.length
        ? { x: divider, top: (items.top - bounds.top) / zoom, bottom: (items.bottom - bounds.top) / zoom } : null;
      // The broken line leaves the last card in the user's row (the user, or a grant of theirs that
      // does not cover the table) and stops at the divider. Across a clear row it carries a label.
      const user = noPathFrom ? point(noPathFrom) : null;
      // Without a data band, the cut sits where it would begin: half a gap past the last column.
      const last = [...root.current.querySelectorAll('.lane')].at(-1)?.getBoundingClientRect();
      const edge = divider ?? (user && last ? (last.right - bounds.left) / zoom + COLUMN_GAP / 2 : null);
      const inRow = user && edge !== null ? [...elements.current].flatMap(([key, element]) => {
        if (key === noPathFrom || isWaypoint(key)) return [];
        const r = element.getBoundingClientRect();
        const top = (r.top - bounds.top) / zoom, bottom = (r.bottom - bounds.top) / zoom, right = (r.right - bounds.left) / zoom;
        return top < user.y && bottom > user.y && right > user.right && right < edge ? [right] : [];
      }) : [];
      const cut = edge === null || !user ? null
        // The label rides on a clear row long enough to hold it; a short cut keeps only the mark.
        : { from: Math.max(user.right, ...inRow), to: edge, y: user.y, label: inRow.length === 0 && edge - Math.max(user.right, ...inRow) >= CUT_LABEL_WIDTH };
      // Opacity on dimmed cards makes their background translucent. Mask connectors
      // under every real card so lines cannot show through its text or background.
      const occluders = [...[...elements.current].filter(([key]) => !isWaypoint(key)).map(([, element]) => element), ...databaseElements.current.values()]
        .map(element => {
          const r = element.getBoundingClientRect();
          return { x: (r.left - bounds.left) / zoom, y: (r.top - bounds.top) / zoom, width: r.width / zoom, height: r.height / zoom };
        });
      setGeometry({ width: root.current.offsetWidth, height: root.current.offsetHeight, occluders, paths, stubs: drawnStubs, bracket, cut });
    };
    measure();window.addEventListener('resize', measure);
    return () => window.removeEventListener('resize', measure);
  }, [graph, previewGraph, columns, routes, stubs, zoom, style, noPathFrom, databases, routePreviews]);
  const tile = (n: GraphNode) => {
    const card = cards.get(n.key);
    return <div key={n.key} ref={element => { if (element) elements.current.set(n.key, element);else elements.current.delete(n.key); }}
      className={`node ${dimmed(n.key) ? 'dim' : ''}`} style={card && toneStyle(card.flags.includes('revoke') ? 'revoke' : card.type)} data-selected={n.key === selected || (!!databaseSelected && databaseMembers.has(n.key))} data-origin={n.key === origin} data-unreached={n.key === noPath?.to} data-muted={muted?.has(n.key) || undefined} onDoubleClick={() => (onOpen ?? onFocus)?.(n.key)}
      onMouseEnter={event => { if (highlight) { setHovered(n.key);onHover?.(n.key); } if (card) showTip(n.key, event.currentTarget); }}
      onMouseLeave={() => { if (highlight) { setHovered(null);onHover?.(null); } hideTip(); }}>
      <button className="node-select" aria-pressed={n.key === selected} onClick={() => onSelect(n.key)}>
        {card ? <ObjectCardBody card={card} includes={covered?.get(n.key)} /> : <strong>{n.name}</strong>}
      </button>
      {card?.badge && <span className="node-badge" title={card.badge.detail}>{card.badge.text}</span>}
      {onFocus && <button className="node-focus" aria-label={`Focus ${n.name}`} title="Focus object and relationships · or double-click the card" onClick={() => onFocus(n.key)}>↗</button>}
    </div>;
  };
  return <>
    {graph.nodes.length > nodes.length && <p className="muted" style={{ margin: '0 0 4px' }}>{nodes.length} of {graph.nodes.length} objects shown</p>}
    {/* Zoom and the page's actions float over the graph's bottom-right corner and stay put while the graph scrolls. */}
    <div className="graph-frame" data-style={diagramStyle}>
    <div className="graph"><div style={{ width: (lanesWidth + 36) * zoom, height: geometry.height ? geometry.height * zoom : undefined }}><div className="lanes" data-database-layout={!!databases?.items.length || undefined} ref={root} style={{ width: lanesWidth, gridTemplateColumns: `repeat(${shown.length}, ${COLUMN_WIDTH}px)`, columnGap: COLUMN_GAP, transform: `scale(${zoom})`, transformOrigin: 'top left' }}>
      <svg className="wire-layer" width={geometry.width + 36} height={geometry.height} aria-hidden="true">
        <defs><marker id={arrowId} viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M0 0L10 5L0 10Z" fill="#26948c" /></marker>
          <mask id={`${arrowId}-cards`} maskUnits="userSpaceOnUse" x={0} y={0} width={geometry.width + 36} height={geometry.height}>
            <rect width={geometry.width + 36} height={geometry.height} fill="white" />
            {geometry.occluders.map((r, i) => <rect key={i} {...r} fill="black" />)}
          </mask>
        </defs>
        <g mask={`url(#${arrowId}-cards)`}>
        {geometry.paths.map(p => {
          // A partial revoke is drawn as its own kind of line: it removes access, it does not grant it.
          const color = p.revoke ? REVOKE_EDGE_STYLE : EDGE_STYLES[p.family], weight = style.weight(p.family);
          const cancelled = cancelledEdges.includes(p.key);
          const active = linked(p.from, p.to);
          const emphasized = p.family === 'candidate' && active && !!(selected || databaseSelected || (highlight && hovered));
          const opacity = emphasized ? .6 : active ? weight.opacity : weight.opacity / 6;
          return <path key={p.key} d={p.d} data-via-database={p.database} fill="none" stroke={color.stroke} strokeWidth={emphasized ? Math.max(weight.width, 1.5) : weight.width} strokeDasharray={cancelled ? '2 4' : color.dash}
            markerEnd={color.arrow && weight.arrow ? `url(#${arrowId})` : undefined} opacity={opacity * (cancelled ? .5 : 1)} />;
        })}
        {geometry.paths.flatMap(p => p.caption ? [<text key={`${p.key}:caption`} className="edge-caption" x={p.caption.x + (p.caption.fromLeft ? -4 : 4)} y={p.caption.y - 5}
          textAnchor={p.caption.fromLeft ? 'end' : 'start'} opacity={linked(p.from, p.to) ? 1 : .3}>{p.caption.text}</text>] : [])}
        {geometry.bracket && <path d={`M${geometry.bracket.x} ${geometry.bracket.top} L${geometry.bracket.x} ${geometry.bracket.bottom}`}
          stroke={EDGE_STYLES.access.stroke} strokeWidth={2} strokeLinecap="round" opacity={.28} />}
        {geometry.cut && <g className="no-path">
          <path d={`M${geometry.cut.from} ${geometry.cut.y} L${geometry.cut.to - 9} ${geometry.cut.y}`} fill="none" strokeWidth={1.6} strokeDasharray="5 4" />
          {/* The disc covers the dashed divider under the mark. */}
          <circle cx={geometry.cut.to} cy={geometry.cut.y} r={9} className="no-path-disc" />
          <path d={`M${geometry.cut.to - 5} ${geometry.cut.y - 5} L${geometry.cut.to + 5} ${geometry.cut.y + 5} M${geometry.cut.to + 5} ${geometry.cut.y - 5} L${geometry.cut.to - 5} ${geometry.cut.y + 5}`}
            fill="none" strokeWidth={2} strokeLinecap="round" />
          {geometry.cut.label && <text x={(geometry.cut.from + geometry.cut.to) / 2} y={geometry.cut.y - 8} textAnchor="middle">No role or privilege on this path</text>}
        </g>}
        {geometry.stubs.map(s => <g key={s.from} opacity={linked(s.from, s.from) ? 1 : .3}>
          <path d={s.d} fill="none" stroke={EDGE_STYLES.access.stroke} strokeWidth={1.5} opacity={.6} />
          <circle cx={s.x} cy={s.y} r={4} fill="var(--bg-secondary, #fff)" stroke={EDGE_STYLES.access.stroke} strokeWidth={1.5} />
        </g>)}
        </g>
      </svg>
      {/* A divider marks where access configuration, data objects and external storage meet. */}
      {shown.map((c, i) => {
        const bandStart = i === 0 || shown[i - 1].band !== c.band;
        return <div key={i} className={i > 0 && bandStart ? 'lane band-start' : 'lane'} data-band={c.band} data-databases={c.databaseColumn || undefined}>
          <div className="band-title">{bandStart ? BAND_TITLES[c.band] : '\u00a0'}</div>
          {/* Header strip: the column title, kept visible while the diagram scrolls. */}
          <div className="lane-title"><span>{c.title ?? '\u00a0'}</span>
            {c.total !== undefined && c.total > 0 && <span className="lane-count">{c.hidden ? `${c.total - c.hidden} of ${c.total}` : c.total}</span>}</div>
          {c.notices?.map(notice => <p key={notice} className="lane-notice">{notice}</p>)}
          <div className="lane-items">{c.databaseColumn && databases && visibleDatabases.map(db => <button type="button" key={db.name} className={`database-card${activeDatabases.has(db.name) ? '' : ' dim'}`} ref={element => { if (element) databaseElements.current.set(db.name, element);else databaseElements.current.delete(db.name); }}
            aria-pressed={databases.selected === db.name} title={`Highlight objects in ${db.name}`} onClick={() => databases.onSelect(db.name)}>
            <strong>{db.name}</strong><small>{visibleDatabaseCounts.get(db.name) !== db.count ? `${visibleDatabaseCounts.get(db.name)} of ${db.count}` : db.count} {db.count === 1 ? 'table / view' : 'tables / views'}</small>
          </button>)}{c.items.map(item => typeof item !== 'string' ? tile(item)
            : isGap(item) ? <div key={item} className="slot" aria-hidden="true" />
            : <div key={item} className="waypoint" ref={element => { if (element) elements.current.set(item, element);else elements.current.delete(item); }} />)}</div>
          {c.databaseColumn && !!c.hidden && <p className="lane-notice">More databases appear as tables are shown. Use Show to filter any database.</p>}
          {onMore && !c.databaseColumn && !!c.hidden && <button type="button" className="lane-more" onClick={() => onMore(c.columnIndex ?? i)}>Show {Math.min(c.hidden, catalogPageSize())} more</button>}
        </div>;
      })}
    </div></div>{caption && <div className="graph-caption">{caption}</div>}</div>
    <ZoomControls sticky name="object graph" zoom={zoom} canZoomOut={zoom > .5} canZoomIn={zoom < 2}
      onZoomOut={() => setZoom(value => Math.max(.5, value - .25))} onZoomIn={() => setZoom(value => Math.min(2, value + .25))} onReset={() => setZoom(1)}>{actions}</ZoomControls>
    </div>
    {tip && cards.get(tip.key) && createTracehousePortal(<ObjectHoverCard key={tip.key} ref={placeTip} card={cards.get(tip.key)!} diagramStyle={diagramStyle}
      position={{ left: 0, top: 0, visibility: 'hidden' }} />, document.body)}
  </>;
}
