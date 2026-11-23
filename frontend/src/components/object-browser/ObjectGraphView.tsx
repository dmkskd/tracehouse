import { ObjectCardBody, ObjectHoverCard } from './ObjectCard';
import { placeHover, type Box } from './hoverPlacement';
import { createTracehousePortal } from '../../providers/TracehousePortal';
import { ZoomControls } from '../../components/common/ZoomControls';
import { EDGE_STYLES, toneStyle, REVOKE_EDGE_STYLE } from './objectStyle';
import { DIAGRAM_STYLES } from './diagramStyles';
import type { ObjectDiagramStyle } from '../../stores/userPreferenceStore';
import { useEffect, useId, useLayoutEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import { BAND_TITLES, isGap, isWaypoint, lineage, RELATIONSHIPS, type GraphNode, type RelationshipFamily, type LayoutColumn, type ObjectCard, type ObjectGraph, isRevokeEdge, edgeCaption } from '@tracehouse/core/services/object-browser';

// Card width and gap as in the mockups.
const COLUMN_WIDTH = 176, COLUMN_GAP = 40;
// Stable default: the measuring effect depends on `stubs`.
const NO_STUBS: string[] = [];
// Pause before the hover card opens, so moving across the diagram does not flash cards.
const HOVER_DELAY_MS = 350;
type Point = { left: number; right: number; y: number };

/** Pure presentation of a laid-out graph (layoutCatalog or layoutFocus); contains no
 * collection, access or layout rules. `routes` (routeEdges) pass lines through
 * waypoints, `highlight` dims lines outside the hovered card's lineage, and
 * `diagramStyle` picks how cards and lines are drawn (DIAGRAM_STYLES). */
export function ObjectGraphView({ graph, cards, columns, selected, onSelect, onFocus, onOpen, actions, caption, cancelledEdges = NO_STUBS, showAll = false, origin, routes, stubs = NO_STUBS, highlight = false, diagramStyle = 'cards' }: {
  graph: ObjectGraph; cards: Map<string, ObjectCard>; columns: LayoutColumn[]; selected: string | null;
  onSelect: (key: string) => void; onFocus?: (key: string) => void; onOpen?: (key: string) => void;
  actions?: ReactNode;
  /** Shown under the cards, inside the scrolling diagram: a line about what the diagram shows. */
  caption?: ReactNode;
  /** Links of grants a revoke on the same holder cancels: drawn dashed and faint. */
  cancelledEdges?: readonly string[]; showAll?: boolean; origin?: string | null;
  routes?: Map<string, string[]>; stubs?: string[]; highlight?: boolean; diagramStyle?: ObjectDiagramStyle;
}) {
  const style = DIAGRAM_STYLES[diagramStyle];
  const root = useRef<HTMLDivElement>(null);
  const arrowId = `object-flow-${useId().replace(/:/g, '')}`;
  const [zoom, setZoom] = useState(1);
  const elements = useRef(new Map<string, HTMLDivElement>());
  const [geometry, setGeometry] = useState<{ width: number; height: number;
    paths: { key: string; from: string; to: string; d: string; family: RelationshipFamily; revoke: boolean;
      /** Text drawn at the target end (edgeCaption), with that end point and the side the line arrives from. */
      caption?: { text: string; x: number; y: number; fromLeft: boolean } }[];
    stubs: { from: string; d: string; x: number; y: number }[]; bracket: { x: number; top: number; bottom: number } | null }>({ width: 0, height: 0, paths: [], stubs: [], bracket: null });
  const [hovered, setHovered] = useState<string | null>(null);
  // Hover card: the card under the pointer after HOVER_DELAY_MS, measured then placed beside it.
  const [tip, setTip] = useState<{ key: string; anchor: Box } | null>(null);
  const [tipPosition, setTipPosition] = useState<{ left: number; top: number } | null>(null);
  const tipElement = useRef<HTMLDivElement>(null);
  const tipTimer = useRef<ReturnType<typeof setTimeout>>();
  const showTip = (key: string, element: HTMLElement) => {
    clearTimeout(tipTimer.current);
    tipTimer.current = setTimeout(() => setTip({ key, anchor: element.getBoundingClientRect() }), HOVER_DELAY_MS);
  };
  const hideTip = () => { clearTimeout(tipTimer.current);setTip(null); };
  useEffect(() => () => clearTimeout(tipTimer.current), []);
  useLayoutEffect(() => {
    const element = tipElement.current;
    if (!tip || !element) { setTipPosition(null); return; }
    setTipPosition(placeHover(tip.anchor, { width: element.offsetWidth, height: element.offsetHeight }, { width: window.innerWidth, height: window.innerHeight }));
  }, [tip]);
  // Scrolling moves the card away from its anchor; a press starts selecting, dragging or opening.
  useEffect(() => {
    if (!tip) return;
    const hide = () => { clearTimeout(tipTimer.current);setTip(null); };
    window.addEventListener('scroll', hide, true);window.addEventListener('mousedown', hide, true);
    return () => { window.removeEventListener('scroll', hide, true);window.removeEventListener('mousedown', hide, true); };
  }, [tip]);
  const connected = useMemo(() => selected ? lineage(graph, selected) : new Set<string>(), [graph, selected]);
  const hoverConnected = useMemo(() => hovered ? lineage(graph, hovered) : new Set<string>(), [graph, hovered]);
  const linked = (from: string, to: string) => highlight && hovered
    ? hoverConnected.has(from) && hoverConnected.has(to)
    : showAll || !selected || (connected.has(from) && connected.has(to));
  const byKey = new Map(graph.nodes.map(n => [n.key, n]));
  const shown: { title?: string; band: LayoutColumn['band']; notices?: string[]; items: (GraphNode | string)[] }[] = columns.map(c => ({ title: c.title, band: c.band, notices: c.notices,
    items: c.keys.flatMap((k): (GraphNode | string)[] => isWaypoint(k) || isGap(k) ? [k] : [byKey.get(k)].filter((n): n is GraphNode => !!n)) }));
  const nodes = shown.flatMap(c => c.items.filter((item): item is GraphNode => typeof item !== 'string'));
  const lanesWidth = shown.length * COLUMN_WIDTH + Math.max(0, shown.length - 1) * COLUMN_GAP;
  // Bring the selected card into view, e.g. after picking a search result. Cards already visible stay put.
  useEffect(() => {
    if (selected) elements.current.get(selected)?.scrollIntoView?.({ block: 'nearest', inline: 'nearest', behavior: 'smooth' });
  }, [selected]);
  useLayoutEffect(() => {
    const measure = () => {
      if (!root.current) return;
      const bounds = root.current.getBoundingClientRect();
      const point = (key: string): Point | null => {
        const r = elements.current.get(key)?.getBoundingClientRect();
        return r ? { left: (r.left - bounds.left) / zoom, right: (r.right - bounds.left) / zoom, y: (r.top + r.height / 2 - bounds.top) / zoom } : null;
      };
      const paths = graph.edges.flatMap(edge => {
        const a = point(edge.from), b = point(edge.to);
        if (!a || !b) return [];
        const sameLane = Math.abs(b.left - a.left) < 1;
        const forward = b.left > a.left;
        const sx = forward || sameLane ? a.right : a.left, tx = forward && !sameLane ? b.left : b.right;
        const family = RELATIONSHIPS[edge.kind].family, revoke = isRevokeEdge(edge);
        if (sameLane) return [{ key: edge.key, from: edge.from, to: edge.to, family, revoke, d: `M${sx} ${a.y} C${sx + 28} ${a.y} ${sx + 28} ${b.y} ${tx} ${b.y}` }];
        // Through each waypoint: enter on the near side, cross it level, leave on the far side.
        let d = `M${sx} ${a.y}`, x = sx, y = a.y;
        for (const waypoint of (routes?.get(edge.key) ?? []).map(point)) {
          if (!waypoint) continue;
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
      setGeometry({ width: root.current.offsetWidth, height: root.current.offsetHeight, paths, stubs: drawnStubs, bracket });
    };
    measure();window.addEventListener('resize', measure);
    return () => window.removeEventListener('resize', measure);
  }, [graph, columns, routes, stubs, zoom, style]);
  const tile = (n: GraphNode) => {
    const card = cards.get(n.key);
    return <div key={n.key} ref={element => { if (element) elements.current.set(n.key, element);else elements.current.delete(n.key); }}
      className={`node ${!showAll && selected && !connected.has(n.key) ? 'dim' : ''}`} style={card && toneStyle(card.flags.includes('revoke') ? 'revoke' : card.type)} data-selected={n.key === selected} data-origin={n.key === origin} onDoubleClick={() => (onOpen ?? onFocus)?.(n.key)}
      onMouseEnter={event => { if (highlight) setHovered(n.key); if (card) showTip(n.key, event.currentTarget); }}
      onMouseLeave={() => { if (highlight) setHovered(null); hideTip(); }}>
      <button className="node-select" aria-pressed={n.key === selected} onClick={() => onSelect(n.key)}>
        {card ? <ObjectCardBody card={card} /> : <strong>{n.name}</strong>}
      </button>
      {card?.badge && <span className="node-badge" title={card.badge.detail}>{card.badge.text}</span>}
      {onFocus && <button className="node-focus" aria-label={`Focus ${n.name}`} title="Focus object and relationships · or double-click the card" onClick={() => onFocus(n.key)}>↗</button>}
    </div>;
  };
  return <>
    {graph.nodes.length > nodes.length && <p className="muted" style={{ margin: '0 0 4px' }}>{nodes.length} of {graph.nodes.length} objects shown</p>}
    {/* Zoom and the page's actions float over the graph's bottom-right corner and stay put while the graph scrolls. */}
    <div className="graph-frame" data-style={diagramStyle}>
    <div className="graph"><div style={{ width: (lanesWidth + 36) * zoom, height: geometry.height ? geometry.height * zoom : undefined }}><div className="lanes" ref={root} style={{ width: lanesWidth, gridTemplateColumns: `repeat(${shown.length}, ${COLUMN_WIDTH}px)`, columnGap: COLUMN_GAP, transform: `scale(${zoom})`, transformOrigin: 'top left' }}>
      <svg className="wire-layer" width={geometry.width + 36} height={geometry.height} aria-hidden="true">
        <defs><marker id={arrowId} viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M0 0L10 5L0 10Z" fill="#26948c" /></marker></defs>
        {geometry.paths.map(p => {
          // A partial revoke is drawn as its own kind of line: it removes access, it does not grant it.
          const color = p.revoke ? REVOKE_EDGE_STYLE : EDGE_STYLES[p.family], weight = style.weight(p.family);
          const cancelled = cancelledEdges.includes(p.key);
          return <path key={p.key} d={p.d} fill="none" stroke={color.stroke} strokeWidth={weight.width} strokeDasharray={cancelled ? '2 4' : color.dash}
            markerEnd={color.arrow && weight.arrow ? `url(#${arrowId})` : undefined} opacity={(linked(p.from, p.to) ? weight.opacity : weight.opacity / 6) * (cancelled ? .5 : 1)} />;
        })}
        {geometry.paths.flatMap(p => p.caption ? [<text key={`${p.key}:caption`} className="edge-caption" x={p.caption.x + (p.caption.fromLeft ? -4 : 4)} y={p.caption.y - 5}
          textAnchor={p.caption.fromLeft ? 'end' : 'start'} opacity={linked(p.from, p.to) ? 1 : .3}>{p.caption.text}</text>] : [])}
        {geometry.bracket && <path d={`M${geometry.bracket.x} ${geometry.bracket.top} L${geometry.bracket.x} ${geometry.bracket.bottom}`}
          stroke={EDGE_STYLES.access.stroke} strokeWidth={2} strokeLinecap="round" opacity={.28} />}
        {geometry.stubs.map(s => <g key={s.from} opacity={linked(s.from, s.from) ? 1 : .3}>
          <path d={s.d} fill="none" stroke={EDGE_STYLES.access.stroke} strokeWidth={1.5} opacity={.6} />
          <circle cx={s.x} cy={s.y} r={4} fill="var(--bg-secondary, #fff)" stroke={EDGE_STYLES.access.stroke} strokeWidth={1.5} />
        </g>)}
      </svg>
      {/* A divider marks where access configuration, data objects and external storage meet. */}
      {shown.map((c, i) => {
        const bandStart = i === 0 || shown[i - 1].band !== c.band;
        return <div key={i} className={i > 0 && bandStart ? 'lane band-start' : 'lane'} data-band={c.band}>
          <div className="band-title">{bandStart ? BAND_TITLES[c.band] : '\u00a0'}</div>
          {/* Header strip: the column title, kept visible while the diagram scrolls. */}
          <div className="lane-title"><span>{c.title ?? '\u00a0'}</span></div>
          {c.notices?.map(notice => <p key={notice} className="lane-notice">{notice}</p>)}
          <div className="lane-items">{c.items.map(item => typeof item !== 'string' ? tile(item)
            : isGap(item) ? <div key={item} className="slot" aria-hidden="true" />
            : <div key={item} className="waypoint" ref={element => { if (element) elements.current.set(item, element);else elements.current.delete(item); }} />)}</div>
        </div>;
      })}
    </div></div>{caption && <div className="graph-caption">{caption}</div>}</div>
    <ZoomControls sticky name="object graph" zoom={zoom} canZoomOut={zoom > .5} canZoomIn={zoom < 2}
      onZoomOut={() => setZoom(value => Math.max(.5, value - .25))} onZoomIn={() => setZoom(value => Math.min(2, value + .25))} onReset={() => setZoom(1)}>{actions}</ZoomControls>
    </div>
    {tip && cards.get(tip.key) && createTracehousePortal(<ObjectHoverCard ref={tipElement} card={cards.get(tip.key)!} diagramStyle={diagramStyle}
      position={tipPosition ?? { left: 0, top: 0, visibility: 'hidden' }} />, document.body)}
  </>;
}
