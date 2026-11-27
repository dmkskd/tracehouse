import { useEffect, useLayoutEffect, useId, useMemo, useRef, useState, type ReactNode } from 'react';
import { edgeCaption, isRevokeEdge, RELATIONSHIPS, type GraphNode, type ProvenanceState } from '@tracehouse/core/services/object-browser';
import type { ObjectViewActions, ObjectViewModel } from '../shared/ObjectViewModel';
import { selectionEmphasis } from '../shared/selectionEmphasis';
import { useObjectHover } from '../shared/useObjectHover';
import { ObjectCardBody } from '../ObjectCard';
import { EDGE_STYLES, REVOKE_EDGE_STYLE, OBJECT_SYMBOLS, toneStyle } from '../objectStyle';
import { groupObjects } from './grouping';
import { layoutScene } from './layout';
import { sceneConnectors } from './connectors';
import { MAX_ZOOM, MIN_ZOOM, useCamera } from './useCamera';
import { spatialStyles } from './styles';
import { ZoomControls } from '../../common/ZoomControls';
import { CameraControlsMenu } from './CameraControlsMenu';
import type { ObjectGraph } from '@tracehouse/core/services/object-browser';

interface Props extends ObjectViewActions {
  model: ObjectViewModel;
  hoverGraph: ObjectGraph | null;
  onHover: (key: string | null) => void;
  highlight: boolean;
  essential: boolean;
  actions?: ReactNode;
}

/** Isolated experimental renderer. No collection, SQL, store access, permission
 * evaluation or global styles. Removing this directory leaves the shared model and
 * the existing graph renderer intact. */
export default function SpatialObjectView({ model, hoverGraph, onHover, highlight, essential, onSelect, onFocus, onDatabase, onClear, actions }: Props) {
  const selectionPanel = useRef<HTMLElement>(null);
  const clickedSelection = useRef<string | null>(null);
  const revealedSelection = useRef<string | null>(null);
  const allGroups = useMemo(() => groupObjects(model.graph), [model.graph]);
  const [groupLimit, setGroupLimit] = useState(12);
  const [privilegeLimit, setPrivilegeLimit] = useState(20);
  const groups = useMemo(() => {
    const counts = new Map<string, number>();
    return allGroups.filter(g => {
      const count = counts.get(g.kind) ?? 0;counts.set(g.kind, count + 1);
      return g.kind === 'scope' || count < groupLimit || g.keys.includes(model.selected ?? '');
    });
  }, [allGroups, groupLimit, model.selected]);
  const [folds, setFolds] = useState<ReadonlyMap<string, boolean>>(new Map());
  const [pages, setPages] = useState<ReadonlyMap<string, number>>(new Map());
  const [hovered, setHovered] = useState<string | null>(null);
  // Keys are scoped to this capture's graph by the model. A selected search result
  // is revealed even when its group was collapsed or the group has reached its cap.
  const expanded = useMemo(() => new Set(groups.filter(g => g.keys.includes(model.selected ?? '') || (folds.get(g.id) ?? g.defaultExpanded)).map(g => g.id)), [groups, folds, model.selected]);
  const scene = useMemo(() => layoutScene(model.graph, groups, expanded, pages, model.selected, privilegeLimit), [model.graph, groups, expanded, pages, model.selected, privilegeLimit]);
  const privilegeTotal = allGroups.filter(g => g.kind === 'scope').reduce((n, g) => n + g.keys.length, 0);
  const drawnPrivileges = scene.groups.filter(g => g.group.kind === 'scope').reduce((n, g) => n + g.tiles.length, 0);
  const { viewport, scale, dragging, reset, zoomIn, zoomOut, reveal, frame, transform, pointerHandlers } = useCamera(scene.width, scene.height);
  const { showTip, hideTip, tooltip } = useObjectHover(model.cards);
  const graph = useMemo(() => {
    if (!hovered || !highlight || !hoverGraph) return model.graph;
    const keys = new Set(model.graph.nodes.map(n => n.key)), edges = new Map(model.graph.edges.map(e => [e.key, e]));
    for (const edge of hoverGraph.edges) if (keys.has(edge.from) && keys.has(edge.to)) edges.set(edge.key, edge);
    return { ...model.graph, edges: [...edges.values()] };
  }, [model.graph, hoverGraph, hovered, highlight]);
  const emphasis = useMemo(() => selectionEmphasis({ graph: model.graph, lineageGraph: model.graph,
    hoverGraph: hoverGraph ?? graph, selected: model.selected, hovered, highlight, selectedDatabase: model.selectedDatabase, muted: model.muted }),
  [model.graph, model.selected, model.selectedDatabase, model.muted, hoverGraph, graph, hovered, highlight]);
  const connectors = useMemo(() => sceneConnectors(graph, scene, essential, model.selected), [graph, scene, essential, model.selected]);
  const byKey = useMemo(() => new Map(model.graph.nodes.map(n => [n.key, n])), [model.graph]);
  const selected = model.selected ? byKey.get(model.selected) : undefined;
  const id = `spatial-${useId().replace(/:/g, '')}`;
  useEffect(() => {
    if (revealedSelection.current === model.selected) return;
    revealedSelection.current = model.selected;
    if (clickedSelection.current === model.selected) return;
    const tile = scene.tiles.find(t => t.key === model.selected);
    if (tile) reveal(tile);
  }, [model.selected, scene, reveal]);
  useLayoutEffect(() => {
    const panel = selectionPanel.current, root = viewport.current?.parentElement?.parentElement;
    if (!panel || !root) return;
    const anchor = [...root.querySelectorAll<HTMLElement>('[data-object-key]')].find(el => el.dataset.objectKey === model.selected);
    const bounds = root.getBoundingClientRect(), target = anchor?.getBoundingClientRect();
    const width = panel.offsetWidth, height = panel.offsetHeight;
    const right = target ? target.right - bounds.left + 12 : 12;
    const left = target ? target.left - bounds.left - width - 12 : 12;
    panel.style.left = `${Math.max(12, Math.min(right + width <= bounds.width - 12 ? right : left, bounds.width - width - 12))}px`;
    panel.style.top = `${Math.max(12, Math.min(target ? target.top - bounds.top : 12, bounds.height - height - 44))}px`;
  }, [model.selected, model.selectedDatabase, transform, scene, viewport]);
  const fold = (group: (typeof groups)[number]) => {
    // A selected member is always revealed. Clear selection before collapsing its
    // group rather than leaving an invisible active object or a synthetic selection.
    if (expanded.has(group.id) && group.keys.includes(model.selected ?? '')) onClear();
    setFolds(previous => new Map(previous).set(group.id, !expanded.has(group.id)));
  };
  const tile = (node: GraphNode) => {
    const card = model.cards.get(node.key);
    const answers = node.kind === 'table' ? model.accessForTable?.(node.key) : undefined;
    const labels: Record<ProvenanceState, string> = { granted: 'configured at login', columns: 'column limits', 'set-role': 'needs SET ROLE', revoked: 'revoked', 'no-path': 'no configured path', incomplete: 'incomplete evidence', differs: 'differs by host' };
    const matching = graph.edges.some(e => e.kind === 'candidate-scope' && e.to === node.key && emphasis.linked(e.from, e.to));
    const holders = node.kind === 'grant-scope' ? [...new Set(graph.edges.filter(e => e.kind === 'grant' && e.to === node.key).map(e => byKey.get(e.from)?.name ?? e.from))] : [];
    const status = (holders.length ? `Held by ${holders.slice(0, 2).join(', ')}${holders.length > 2 ? ` +${holders.length - 2}` : ''}` : undefined) ?? answers?.map(a => `${a.operation}: ${labels[a.state]}`).join(' · ')
      ?? (node.kind === 'table' && (model.selected || hovered) ? matching ? 'Privilege target match · inspect evidence' : 'Data object / flow context' : card?.hosts ?? card?.label);
    const revoked = answers?.length && answers.every(a => a.state === 'revoked');
    return <div className="spatial-tile"
      data-object-key={node.key} data-selected={model.selected === node.key} data-dim={emphasis.dimmed(node.key)} data-muted={model.muted?.has(node.key)} data-evidence={revoked ? 'revoked' : undefined}
      style={{ width: '100%', height: '100%', ...(card ? toneStyle(card.flags.includes('revoke') ? 'revoke' : card.type) : {}) }}
      onMouseEnter={event => { if (dragging) return;if (highlight) { setHovered(node.key);onHover(node.key); } if (model.selected !== node.key) showTip(node.key, event.currentTarget); }}
      onMouseLeave={() => { setHovered(null);onHover(null);hideTip(); }} onDoubleClick={() => onFocus(node.key)}>
      <button className="spatial-select" aria-label={`Select ${node.name}`} aria-pressed={model.selected === node.key} onClick={() => { hideTip();clickedSelection.current = node.key;onSelect(node.key); }}
>
        {card ? <ObjectCardBody card={card} includes={model.covered?.get(node.key)} /> : <strong>{node.name}</strong>}
        <span className="spatial-evidence">{status}</span>
      </button>
      <button className="spatial-focus" aria-label={`Focus ${node.name}`} title="Open existing object focus" onClick={() => onFocus(node.key)}>↗</button>
      <span className="spatial-face spatial-north" aria-hidden="true" /><span className="spatial-face spatial-west" aria-hidden="true" />
    </div>;
  };
  return <section className={spatialStyles} aria-label="Isometric object catalog">
    <span id={`${id}-gestures`} className="spatial-gesture-help">Drag to rotate. Shift-drag or middle-drag to pan. Wheel to zoom. Touch: drag to rotate, two fingers to pan or pinch to zoom.</span>
    <div className="spatial-canvas-wrap">
    <div ref={viewport} className="spatial-viewport" aria-describedby={`${id}-gestures`} data-dragging={dragging} {...pointerHandlers}
      onPointerDown={event => { hideTip();setHovered(null);onHover(null);pointerHandlers.onPointerDown(event); }}
      onWheelCapture={() => { hideTip();setHovered(null);onHover(null); }}>
      <div className="spatial-frame" style={frame}>
        <div className="spatial-scene" style={{ width: scene.width, height: scene.height, transform: transform }}>
          {scene.lanes.map(lane => <div key={lane.label} className="spatial-lane-header" style={{ left: lane.x, top: lane.y, width: lane.width }}>
            <div role="heading" aria-level={2} className="spatial-lane-title">{lane.label}</div>
            {lane.label === 'Privileges by scope' && <div className="spatial-privilege-controls" role="group" aria-label="Privilege paging">
              <span aria-live="polite">{drawnPrivileges} of {privilegeTotal}</span>
              <button aria-label="Show 20 more privileges" disabled={privilegeLimit >= privilegeTotal} onClick={() => setPrivilegeLimit(n => n + 20)}>More</button>
              <button aria-label="Show 20 fewer privileges" disabled={privilegeLimit <= 20} onClick={() => setPrivilegeLimit(n => Math.max(20, n - 20))}>Less</button>
            </div>}
          </div>)}
          {scene.groups.map(box => <section key={box.group.id} className="spatial-platform" aria-label={`${box.group.label} group`}
            data-active={box.group.database !== undefined && emphasis.activeDatabases.has(box.group.database)}
            style={{ left: box.x, top: box.y, width: box.width, height: box.height }}>
            <div className="spatial-platform-title">
              {box.group.database !== undefined ? <button title={box.group.label} onClick={() => onDatabase(box.group.database!)} aria-pressed={model.selectedDatabase === box.group.database}>{box.group.label}</button> : <strong title={box.group.label}>{box.group.kind === 'scope' ? `ON ${box.group.label}` : box.group.label}</strong>}
              <button className="spatial-fold" aria-label={`${box.expanded ? 'Collapse' : 'Expand'} ${box.group.label}`} aria-expanded={box.expanded} onClick={() => fold(box.group)}>{box.expanded ? '−' : '+'}</button>
            </div>
            <div className="spatial-platform-note" title={box.group.note}>{box.tiles.length} of {box.group.keys.length} {box.group.kind === 'scope' ? 'privileges · target scope of the grant' : `objects · ${box.group.note}`}</div>
            {box.expanded && box.hidden > 0 && box.group.kind !== 'scope' && <button className="spatial-more" onClick={() => setPages(previous => new Map(previous).set(box.group.id, (previous.get(box.group.id) ?? 0) + 1))}>Show more · {box.hidden} hidden</button>}
            <span className="spatial-face spatial-north" aria-hidden="true" /><span className="spatial-face spatial-west" aria-hidden="true" />
          </section>)}
          {scene.tiles.map(placed => { const node = byKey.get(placed.key);return node && <div key={placed.key} style={{ position: 'absolute', left: placed.x, top: placed.y, width: placed.width, height: placed.height, transformStyle: 'preserve-3d' }}>{tile(node)}</div>; })}
          <svg className="spatial-wire" width={scene.width} height={scene.height} aria-hidden="true">
            <defs>
              <marker id={`${id}-arrow`} viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto"><path d="M0 0L10 5L0 10Z" fill={EDGE_STYLES['data-flow'].stroke} /></marker>
              <mask id={`${id}-mask`} maskUnits="userSpaceOnUse" x={0} y={0} width={scene.width} height={scene.height}>
                <rect width={scene.width} height={scene.height} fill="white" />
                {scene.tiles.map(r => <rect key={r.key} x={r.x + 2} y={r.y + 2} width={r.width - 4} height={r.height - 4} fill="black" />)}
              </mask>
            </defs>
            <g mask={`url(#${id}-mask)`}>{connectors.map(connector => {
              const family = RELATIONSHIPS[connector.edges[0].kind].family;
              const color = connector.revoke ? REVOKE_EDGE_STYLE : EDGE_STYLES[family];
              const active = connector.edges.some(e => emphasis.linked(e.from, e.to));
              return <path key={connector.key} data-edge-count={connector.edges.length} data-family={family} data-revoke={connector.revoke}
                d={connector.d} fill="none" stroke={color.stroke} strokeWidth={active && (model.selected || hovered) ? 2.8 : 1.5} strokeDasharray={color.dash ?? (family === 'candidate' ? '3 4' : undefined)}
                markerEnd={color.arrow ? `url(#${id}-arrow)` : undefined} opacity={active ? (model.selected || hovered) ? 1 : .65 : .07} />;
            })}</g>
          </svg>
        </div>
      </div>
    </div>
    <ZoomControls name="isometric view" zoom={scale} canZoomOut={scale > MIN_ZOOM} canZoomIn={scale < MAX_ZOOM}
      onZoomOut={zoomOut} onZoomIn={zoomIn} onReset={reset} resetTitle="Reset camera">
      {(model.selected || model.selectedDatabase) && <button onClick={onClear}>Clear selection</button>}
      {actions}<CameraControlsMenu onReset={reset} />
    </ZoomControls>
    </div>
    {groups.length < allGroups.length && <button className="spatial-group-more" onClick={() => setGroupLimit(n => n + 12)}>Show more groups · {allGroups.length - groups.length} hidden</button>}
    {model.coverage.filter(c => c.state !== 'readable').map((c, i) => <p key={`${c.source}:${i}`} className="spatial-caption">{c.source}: {c.state} · {c.reason}</p>)}
    <div className="spatial-caption" title="Other objects are inside collapsed groups or on later pages.">{scene.tiles.length} of {model.graph.nodes.length} objects shown</div>
    {(selected || model.selectedDatabase) && <aside ref={selectionPanel} className="spatial-selection" aria-label="Isometric selection"
      style={selected && model.cards.get(selected.key) ? toneStyle(model.cards.get(selected.key)!.type) : undefined}>
      <button className="spatial-selection-close" aria-label="Dismiss object details" onClick={onClear}>×</button>
      <div className="spatial-selection-kind"><span aria-hidden="true">{selected ? OBJECT_SYMBOLS[model.cards.get(selected.key)?.type ?? 'table'] : '▦'}</span>{selected ? model.cards.get(selected.key)?.label : 'Database'}</div>
      <strong className="spatial-selection-name">{selected ? model.cards.get(selected.key)?.title ?? selected.name : model.selectedDatabase}</strong>
      <p className="spatial-selection-summary">{selected ? model.cards.get(selected.key)?.summary : 'Tables, views and row policies'}</p>
      {selected && <dl>{model.graph.edges.filter(e => e.from === selected.key || e.to === selected.key).slice(0, 3).map(e => {
        const target = byKey.get(e.from === selected.key ? e.to : e.from);
        return <div key={e.key}><dt>{e.kind === 'candidate-scope' ? 'Privilege target' : e.label}{isRevokeEdge(e) ? ' · partial revoke' : ''}</dt><dd title={edgeCaption(e) || target?.name}>{target ? model.cards.get(target.key)?.title ?? target.name : 'Unknown'}</dd></div>;
      })}</dl>}
      {selected && <button className="spatial-selection-open" onClick={() => onFocus(selected.key)}>Open object focus ↗</button>}
    </aside>}
    {tooltip}
  </section>;
}
