import { useCallback, useEffect, useMemo, useState } from 'react';
import { useUrlState } from '../../hooks/useUrlState';
import { describeObjects, essentialEdges, layoutCatalog, withRoutes, objectKey, projectGraph, projectCatalogGraph, provenancePaths, searchGraph, withoutDefaultUser, type ObjectCard } from '@tracehouse/core/services/object-browser';
import { ObjectTypeIcon } from './ObjectCard';
import { ObjectGraphView } from './ObjectGraphView';
import { ModalWrapper } from '../../components/shared/ModalWrapper';
import { ObjectDetailModal } from './ObjectDetailModal';
import { useObjectCapture } from './useObjectCapture';
import { useObjectFocus } from './useObjectFocus';
import { DiagramMenu } from './DiagramMenu';
import { InfoPopover } from '../../components/common/InfoPopover';
import { FocusTrailHeader } from './FocusTrailHeader';
import { ObjectGuides } from './ObjectGuides';
import { ObjectsLoading } from './ObjectsLoading';
import { page } from './objectBrowserStyles';
import { useUserPreferenceStore } from '../../stores/userPreferenceStore';

const urlSchema = {
  ob_selected: { type: 'string', default: '' },
} as const;
export function ObjectBrowserPage() {
  const { state: url, update } = useUrlState(urlSchema);
  const { capture, error, loading, collect, browse, connected } = useObjectCapture();
  const diagramStyle = useUserPreferenceStore(s => s.objectDiagramStyle);
  const lines = useUserPreferenceStore(s => s.objectDiagramLines);
  const [selected, setSelected] = useState<string | null>(null);
  const [search, setSearch] = useState('');
  const [hideDefaultUser, setHideDefaultUser] = useState(false);
  useEffect(() => { void browse(); }, [browse]);
  const baseGraph = useMemo(() => capture ? projectGraph(capture) : null, [capture]);
  const selectedKey = selected ?? baseGraph?.nodes.find(n => JSON.stringify([n.kind,n.name]) === url.ob_selected)?.key ?? null;
  // Broad scopes expand for one focus object: a selected table in the catalog, any starting point in focus.
  const projectFor = useCallback((focus: string | null) => {
    const projected = capture ? capture.mode === 'catalog' ? projectCatalogGraph(capture, focus ?? undefined) : baseGraph : null;
    return projected && hideDefaultUser ? withoutDefaultUser(projected) : projected;
  }, [capture, baseGraph, hideDefaultUser]);
  const selectedTable = baseGraph?.nodes.find(n => n.key === selectedKey && n.kind === 'table')?.key ?? null;
  const graph = useMemo(() => projectFor(selectedTable), [projectFor, selectedTable]);
  const searched = useMemo(() => graph ? searchGraph(graph, search, selectedKey) : null, [graph, search, selectedKey]);
  const matches = searched?.matches ?? [];
  // The catalog shares the diagram's line options with focus.
  const catalogLayout = useMemo(() => {
    if (!searched) return { ...withRoutes(null, [], false), graph: null, stubs: [] as string[] };
    const drawn = lines.essential ? essentialEdges(searched.graph) : { graph: searched.graph, stubs: [] as string[] };
    return { ...withRoutes(drawn.graph, layoutCatalog(drawn.graph, selectedKey), lines.route), ...drawn };
  }, [searched, selectedKey, lines.route, lines.essential]);
  // Provenance paths answer the provenance question; a catalog capture has none to show.
  const paths = useMemo(() => capture && graph && capture.mode !== 'catalog' ? provenancePaths(graph,
    objectKey(capture.investigation.connection, 'user', capture.investigation.principal),
    objectKey(capture.investigation.connection, 'table', capture.investigation.database, capture.investigation.table)) : [], [capture, graph]);
  const chosen = graph?.nodes.find(n => n.key === selectedKey);
  const cards = useMemo(() => graph ? describeObjects(graph) : new Map<string, ObjectCard>(), [graph]);
  const focus = useObjectFocus(projectFor);
  const choose = (key: string | null) => {
    setSelected(key);setSearch('');if (!key) focus.close();
    const node = graph?.nodes.find(n => n.key === key);
    update({ ob_selected: node ? JSON.stringify([node.kind,node.name]) : '' });
  };
  return <div className={page}>
    {/* One row: the diagram gets the height. Notes and help live in the ⓘ. */}
    <div className="toolbar">
      <span className="title"><h1>Object browser</h1><span className="beta" title="Early access. Layout and behavior may change.">Beta</span></span>
      <input type="search" aria-label="Find an object" placeholder="Find a user, role, table or privilege" value={search} onChange={e => setSearch(e.target.value)} />
      <label className="toggle" title="Hides the default user, its links and privileges held only by it. Collected metadata stays."><input type="checkbox" checked={hideDefaultUser} onChange={e => {
        setHideDefaultUser(e.target.checked);
        if (e.target.checked && chosen && graph && !withoutDefaultUser(graph).nodes.some(node => node.key === chosen.key)) choose(null);
      }} />Hide default user</label>
      <span className="capture-info">{graph && capture ? `${graph.nodes.length} objects · ${graph.edges.length} relationships · captured ${new Date(capture.completedAt).toLocaleTimeString()} from ${capture.hosts.map(h => `${h.host} (${h.version})`).join(', ')}` : loading ? 'Loading objects…' : 'Read-only catalog from the active connection'}
        <button className="icon-button" aria-label="Refresh catalog" title="Collect the catalog again" disabled={!connected || loading} onClick={() => { setSelected(null);void browse(); }}>↻</button></span>
      <InfoPopover id="object-browser-help" label="About the object browser" title="About the object browser">
        <p style={{ margin: '0 0 6px' }}>Experimental. Shows configured metadata: grants, roles and table links. It never decides whether a user is allowed to run a query.</p>
        <ul style={{ margin: 0 }}>
          <li><strong>Click</strong> a card: highlights what it is connected to</li>
          <li><strong>↗ or double-click</strong>: opens focus on it</li>
          <li><strong>Double-click in focus</strong>: opens its details</li>
          <li><strong>Selected table</strong>: adds the broad privileges that may cover it</li>
          <li>Up to 20 cards per column; search finds the rest</li>
          <li>System tables are not listed</li>
        </ul>
      </InfoPopover>
    </div>
    {search.trim() && <div className="coverage">{matches.slice(0, 30).map(n => <button key={n.key} onClick={() => choose(n.key)}>{cards.get(n.key) && <ObjectTypeIcon card={cards.get(n.key)!} />}{n.name} · {cards.get(n.key)?.label}</button>)}<span className="muted">{matches.length} matches in the collected catalog</span></div>}
    {capture?.mode === 'catalog' && graph && <ObjectGuides graph={graph} cards={cards} onFocus={key => { choose(key);focus.openAt(key); }}
      onInvestigate={input => { setSelected(null);update({ ob_selected: '' });void collect(input); }} />}
    {!connected && <p>Connect a ClickHouse datasource to collect metadata.</p>}
    {error && <p role="alert">Collection failed: {error}</p>}
    {loading && !capture && <ObjectsLoading />}
    {capture && graph && <>
      {(capture.mode !== 'catalog' || capture.coverage.some(c => c.state !== 'readable' || c.reason)) && <div className="panel">
        {/* Same trail shape as the focus header: the catalog is the way back. */}
        {capture.mode !== 'catalog' && <h2 className="scope-trail"><button className="focus-start" title="Back to the catalog" onClick={() => { setSelected(null);void browse(); }}>Catalog</button>
          <span className="focus-separator" aria-hidden="true">›</span>
          <span>Provenance · {capture.investigation.principal} → {capture.investigation.database}.{capture.investigation.table} · {capture.investigation.operation}</span></h2>}
        <div className="coverage">{capture.coverage.filter(c => c.state !== 'readable').map(c => <span className="badge warn" key={c.source}>{c.source}: {c.state}</span>)}</div>
        {capture.coverage.filter(c => c.reason).map(c => <p className="diagnostic" key={c.source}>{c.source}: {c.reason}</p>)}
      </div>}
      <div className="panel catalog-panel">
        <ObjectGraphView graph={catalogLayout.graph ?? graph} cards={cards} columns={catalogLayout.columns} routes={catalogLayout.routes} stubs={catalogLayout.stubs} highlight={lines.highlight} selected={selectedKey} onSelect={key => choose(key === selectedKey ? null : key)}
          onFocus={key => { choose(key);focus.openAt(key); }}
          diagramStyle={diagramStyle} actions={<>{selectedKey && <button onClick={() => choose(null)}>Clear selection</button>}<DiagramMenu /></>} />
        {!graph.nodes.length && <p>No object records observed. Inspect source coverage above.</p>}
        {capture.mode !== 'catalog' && <><h3>Common-host provenance paths</h3><p className="muted">{paths.length ? `${paths.length} bounded candidate path(s); exclusions and role activation remain evidence, not evaluated access.` : 'No common-host candidate path observed. This is not a denial result.'}</p>
        {paths.map((p, i) => <p className="muted" key={i}>{p.edges.map(k => graph.edges.find(e => e.key === k)?.label).join(' / ')} · hosts: {p.hosts.join(', ')}</p>)}</>}
      </div>
    </>}
    {!capture && !loading && <p className="muted">The catalog loads when this page opens. Refresh explicitly to collect again; no background polling runs.</p>}
    <ModalWrapper isOpen={focus.open} onClose={focus.close} ariaLabel="Object focus" maxWidth="min(1800px, 96vw)" backdropBackground="rgba(0, 0, 0, 0.45)" backdropBlur={0}>
      <div className={`${page} focus-shell`}>
        <div className="focus-header">
          <FocusTrailHeader trail={focus.trail} cards={focus.cards} nameOf={focus.nameOf} onStep={focus.reroot} />
          <div className="focus-actions">
            <button className="modal-close" aria-label="Close focus" title="Close (Esc)" onClick={focus.close}>✕</button>
          </div>
        </div>
        <div className="focus-body">
        {focus.graph && <ObjectGraphView graph={focus.graph} cards={focus.cards} columns={focus.columns} selected={focus.origin} origin={focus.origin} onSelect={focus.reroot} onOpen={focus.openDetail} showAll
          routes={focus.routes} stubs={focus.stubs} highlight={lines.highlight} diagramStyle={diagramStyle}
          actions={<DiagramMenu />} />}
        {!focus.graph?.edges.length && <p className="muted">No relationships observed in this capture.</p>}
        </div>
      </div>
    </ModalWrapper>
    <ObjectDetailModal detail={focus.detail} onClose={focus.closeDetail} />
  </div>;
}
