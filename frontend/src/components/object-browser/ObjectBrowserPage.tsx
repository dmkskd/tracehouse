import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useUrlState } from '../../hooks/useUrlState';
import { describeObjects, essentialEdges, isColumnNotice, layoutCatalog, withRoutes, withSourceNotices, projectGraph, projectCatalogGraph, privilegesOutside, coveredOperations, toggleOperation, objectKey, tableKeyOf, searchGraph, withoutDefaultUser, parseObjectLink, resolveObjectLink, describeMissingLink, OBJECT_LINK_PARAMS, type Capture, type ObjectCard } from '@tracehouse/core/services/object-browser';
import { useReturnToOrigin } from '../../hooks/useReturnToOrigin';
import { useNavigate } from '../../hooks/useAppLocation';
import { ObjectTypeIcon } from './ObjectCard';
import { ObjectGraphView } from './ObjectGraphView';
import { ModalWrapper } from '../../components/shared/ModalWrapper';
import { ObjectDetailModal } from './ObjectDetailModal';
import { useObjectCapture } from './useObjectCapture';
import { OperationAnswers, OperationPicker, TablePicker } from './ProvenanceOperations';
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
  // An object link from another page: open focus on this object (objectLinkUrl).
  ob_kind: { type: 'string', default: '' },
  ob_db: { type: 'string', default: '' },
  ob_tbl: { type: 'string', default: '' },
  ob_user: { type: 'string', default: '' },
  ob_role: { type: 'string', default: '' },
} as const;

const NO_PAGES: number[] = [];

export function ObjectBrowserPage() {
  const { state: url, update } = useUrlState(urlSchema);
  const { capture, error, loading, browse, read, connected } = useObjectCapture();
  const diagramStyle = useUserPreferenceStore(s => s.objectDiagramStyle);
  const lines = useUserPreferenceStore(s => s.objectDiagramLines);
  const [selected, setSelected] = useState<string | null>(null);
  const [search, setSearch] = useState('');
  const [showDefaultUser, setShowDefaultUser] = useState(true);
  // System tables are collected on request: the toggle collects the catalog again.
  const [systemTables, setSystemTables] = useState(false);
  // Access types catalog lines follow: privileges that include any of them link to tables.
  const [catalogOperations, setCatalogOperations] = useState<string[]>(['SELECT']);
  // Extra pages of cards per catalog column (layoutCatalog), from "Show more". Another capture,
  // search, filter or access type starts every column at its first page; selecting a card does not.
  const pagesFor = JSON.stringify([capture?.completedAt, search, showDefaultUser, catalogOperations]);
  const [paged, setPaged] = useState<{ key: string; pages: number[] }>({ key: '', pages: [] });
  const pages = paged.key === pagesFor ? paged.pages : NO_PAGES;
  // A new connection collects the catalog as the toggle stands.
  const systemTablesRef = useRef(systemTables);
  useEffect(() => { systemTablesRef.current = systemTables; }, [systemTables]);
  useEffect(() => { void browse(systemTablesRef.current); }, [browse]);
  const baseGraph = useMemo(() => capture ? projectGraph(capture) : null, [capture]);
  const selectedKey = selected ?? baseGraph?.nodes.find(n => JSON.stringify([n.kind,n.name]) === url.ob_selected)?.key ?? null;
  // Broad scopes expand for one object: the selection in the catalog, the starting point in focus.
  // `from` is another capture to project the same way, such as a focus read by name.
  const projectFor = useCallback((focus: string | null, from?: Capture) => {
    const source = from ?? capture;
    const projected = source ? source.mode === 'catalog' || from ? projectCatalogGraph(source, focus ?? undefined, undefined, catalogOperations) : baseGraph : null;
    return projected && !showDefaultUser ? withoutDefaultUser(projected) : projected;
  }, [capture, baseGraph, showDefaultUser, catalogOperations]);
  // The selected object scopes broad grants: for a table, the grants that may cover it; for a
  // user, role or privilege, the tables its database-wide and global grants may cover.
  const graph = useMemo(() => projectFor(selectedKey), [projectFor, selectedKey]);
  const searched = useMemo(() => graph ? searchGraph(graph, search, selectedKey) : null, [graph, search, selectedKey]);
  const matches = searched?.matches ?? [];
  // The catalog shares the diagram's line options with focus.
  // Privileges without the catalog's operation: dimmed, and listed after the others.
  const outside = useMemo(() => graph && capture?.mode === 'catalog' ? privilegesOutside(graph, catalogOperations) : undefined, [graph, capture, catalogOperations]);
  // Broad privileges (ALL, ALTER) name the selected types they include.
  const covered = useMemo(() => graph && capture?.mode === 'catalog' ? coveredOperations(graph, catalogOperations) : undefined, [graph, capture, catalogOperations]);
  const catalogLayout = useMemo(() => {
    if (!searched) return { ...withRoutes(null, [], false), graph: null, stubs: [] as string[] };
    const drawn = lines.essential ? essentialEdges(searched.graph) : { graph: searched.graph, stubs: [] as string[] };
    // A source without SELECT is explained in the column its objects would fill.
    const withCoverage = withSourceNotices(layoutCatalog(drawn.graph, selectedKey, undefined, pages, outside), capture?.coverage ?? []);
    return { ...withRoutes(drawn.graph, withCoverage, lines.route), ...drawn };
  }, [searched, selectedKey, lines.route, lines.essential, capture, pages, outside]);
  const chosen = graph?.nodes.find(n => n.key === selectedKey);
  // Coverage not already shown in a diagram column.
  const notes = capture?.coverage.filter(c => !isColumnNotice(c)) ?? [];
  const cards = useMemo(() => graph ? describeObjects(graph) : new Map<string, ObjectCard>(), [graph]);
  // Focus applies the catalog's access types too: matching privileges first, the rest dimmed.
  const focus = useObjectFocus(projectFor, catalogOperations, capture, read);
  const focusOrigin = focus.graph?.nodes.find(n => n.key === focus.origin);
  // Tables focus can answer for without a read: those in the catalog.
  const tableNames = useMemo(() => [...new Set(capture?.observations.filter(o => o.kind === 'table' && !o.attributes.unresolvedReference).map(o => o.name) ?? [])].sort(), [capture]);
  // A link opens focus once per capture; closing it returns to the page the link came from.
  const link = useMemo(() => parseObjectLink(name => url[name as keyof typeof url]), [url]);
  const { from, returnToOrigin } = useReturnToOrigin();
  const navigate = useNavigate();
  // A detail link leaves for another page; its back link and close reopen this focus.
  const openElsewhere = (url: string) => navigate(url, { state: { from: { path: focus.returnUrl, label: 'Object browser' } } });
  const resolvedLink = useMemo(() => link && baseGraph && capture?.mode === 'catalog' ? resolveObjectLink(baseGraph, link) : null, [link, baseGraph, capture]);
  const missingLink = useMemo(() => link && baseGraph && capture && resolvedLink && 'missing' in resolvedLink
    ? describeMissingLink(baseGraph, link, resolvedLink.missing, capture.coverage) : null, [link, baseGraph, capture, resolvedLink]);
  const clearLink = () => update(Object.fromEntries(OBJECT_LINK_PARAMS.map(name => [name, ''])));
  const openedLink = useRef<string | null>(null);
  const { openAt } = focus;
  useEffect(() => {
    if (!resolvedLink || 'missing' in resolvedLink) return;
    const id = `${capture?.completedAt}|${JSON.stringify(link)}`;
    if (openedLink.current === id) return;
    openedLink.current = id;
    openAt(resolvedLink.focus, resolvedLink.context);
  }, [resolvedLink, capture, link, openAt]);
  const closeFocus = () => {
    focus.close();
    if (!link) return;
    if (from) returnToOrigin();
    else clearLink();
  };
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
      {capture?.mode === 'catalog' && <span className="lines-for" title="Privilege-to-table links for the selected access types. Privileges that include none of them are dimmed and listed last.">Access type
        <OperationPicker multiple current={catalogOperations} operations={capture.operations} onPick={op => setCatalogOperations(prev => toggleOperation(prev, op))} /></span>}
      {capture?.mode === 'catalog' && <span className="toolbar-separator" aria-hidden="true" />}
      <label className="toggle" title="Off: hides the default user, its links and privileges held only by it. Collected metadata stays."><input type="checkbox" checked={showDefaultUser} onChange={e => {
        setShowDefaultUser(e.target.checked);
        if (!e.target.checked && chosen && graph && !withoutDefaultUser(graph).nodes.some(node => node.key === chosen.key)) choose(null);
      }} />Show default user</label>
      <label className="toggle" title="On: lists only the tables of the system database. Collects the catalog again."><input type="checkbox" checked={systemTables} disabled={!connected || loading} onChange={e => {
        setSystemTables(e.target.checked);choose(null);void browse(e.target.checked);
      }} />Show system tables</label>
      <span className="capture-info">{graph && capture ? `${graph.nodes.length} objects · ${graph.edges.length} relationships · captured ${new Date(capture.completedAt).toLocaleTimeString()}` : loading ? 'Loading objects…' : 'Read-only catalog from the active connection'}
        <button className="icon-button" aria-label="Refresh catalog" title="Collect the catalog again" disabled={!connected || loading} onClick={() => { setSelected(null);void browse(systemTables); }}>↻</button></span>
      <InfoPopover id="object-browser-help" label="About the object browser" title="About the object browser">
        <p style={{ margin: '0 0 6px' }}>Experimental. Shows configured metadata: grants, roles and table links. It never decides whether a user is allowed to run a query.</p>
        <ul style={{ margin: 0 }}>
          <li><strong>Click</strong> a card: highlights what it is connected to</li>
          <li><strong>↗ or double-click</strong>: opens focus on it</li>
          <li><strong>Double-click in focus</strong>: opens its details</li>
          <li><strong>Selected object</strong>: adds the tables its broad privileges may cover, or for a table the privileges that may cover it</li>
          <li>20 cards per column; Show more adds 20, search finds the rest</li>
          <li>Access type: privileges that include any selected type link to tables; the rest are dimmed and listed last</li>
          <li>Show system tables: lists the system database alone; otherwise only system tables a view reads from</li>
        </ul>
        {capture && <div style={{ marginTop: 8, paddingTop: 8, borderTop: '1px solid var(--border-secondary)' }}>
          <div style={{ color: 'var(--text-muted)', marginBottom: 4 }}>Captured {new Date(capture.completedAt).toLocaleTimeString()} from</div>
          {capture.hosts.map(h => <div key={h.host} style={{ display: 'flex', justifyContent: 'space-between', gap: 16 }}><span style={{ fontFamily: 'monospace' }}>{h.host}</span><span style={{ color: 'var(--text-muted)' }}>{h.version}</span></div>)}
        </div>}
      </InfoPopover>
    </div>
    {search.trim() && <div className="coverage">{matches.slice(0, 30).map(n => <button key={n.key} onClick={() => choose(n.key)}>{cards.get(n.key) && <ObjectTypeIcon card={cards.get(n.key)!} />}{n.name} · {cards.get(n.key)?.label}</button>)}<span className="muted">{matches.length} matches in the collected catalog</span></div>}
    {capture?.mode === 'catalog' && graph && <ObjectGuides graph={graph} cards={cards} onFocus={key => { choose(key);focus.openAt(key); }}
      onInvestigate={input => {
        // "How does a user reach a table?" opens focus on the user, answering for the table.
        const connection = capture.investigation.connection;
        choose(null);focus.openAt(objectKey(connection, 'user', input.principal), null, objectKey(connection, 'table', input.database, input.table));
      }} />}
    {!connected && <p>Connect a ClickHouse datasource to collect metadata.</p>}
    {error && <p role="alert">Collection failed: {error}</p>}
    {missingLink && <div className="link-missing" role="alert">
      <div className="link-missing-text">
        <strong>Link not opened: {missingLink.requested}</strong>
        <span>Not in the collected catalog: {missingLink.missing.map(m => `${m.kind} ${m.name}`).join(', ')}. Showing the full catalog.</span>
        {missingLink.incomplete.length > 0 && <span>Catalog incomplete: {missingLink.incomplete.join(', ')}</span>}
        {missingLink.suggestions.length > 0 && <span className="link-missing-suggestions">Similar:{missingLink.suggestions.map(s =>
          <button key={`${s.focus}|${s.context}`} onClick={() => focus.openAt(s.focus, s.context)}>{s.name}</button>)}</span>}
      </div>
      <button className="link-missing-dismiss" onClick={from ? returnToOrigin : clearLink}>{from ? `Back to ${from.label}` : 'Dismiss'}</button>
    </div>}
    {loading && !capture && <ObjectsLoading />}
    {capture && graph && <>
      {notes.some(c => c.state !== 'readable' || c.reason) && <div className="panel">
        <div className="coverage">{notes.filter(c => c.state !== 'readable').map(c => <span className="badge warn" key={c.source}>{c.source}: {c.state}</span>)}</div>
        {notes.filter(c => c.reason).map(c => <p className="diagnostic" key={c.source}>{c.source}: {c.reason}</p>)}
      </div>}
      <div className="panel catalog-panel">
        <ObjectGraphView graph={catalogLayout.graph ?? graph} cards={cards} columns={catalogLayout.columns} routes={catalogLayout.routes} stubs={catalogLayout.stubs} muted={outside} covered={covered}
          onMore={column => setPaged(() => { const next = [...pages]; next[column] = (next[column] ?? 0) + 1; return { key: pagesFor, pages: next }; })} highlight={lines.highlight} selected={selectedKey} onSelect={key => choose(key === selectedKey ? null : key)}
          onFocus={key => { choose(key);focus.openAt(key); }}
          diagramStyle={diagramStyle} actions={<>{selectedKey && <button onClick={() => choose(null)}>Clear selection</button>}<DiagramMenu /></>} />
        {!graph.nodes.length && <p>No object records observed. Inspect source coverage above.</p>}
      </div>
    </>}
    {!capture && !loading && <p className="muted">The catalog loads when this page opens. Refresh explicitly to collect again; no background polling runs.</p>}
    <ModalWrapper isOpen={focus.open} onClose={closeFocus} ariaLabel="Object focus" maxWidth="min(1800px, 96vw)" backdropBackground="rgba(0, 0, 0, 0.45)" backdropBlur={0}>
      <div className={`${page} focus-shell`}>
        <div className="focus-header">
          <FocusTrailHeader trail={focus.trail} cards={focus.cards} nameOf={focus.nameOf} onStep={focus.reroot} />
          {/* Focus follows the catalog's access types; changing them here changes both. */}
          {capture?.mode === 'catalog' && <span className="toolbar-separator" aria-hidden="true" />}
          {capture?.mode === 'catalog' && <span className="lines-for" title="Privilege-to-table links for the selected access types. Privileges that include none of them are dimmed.">Access type
            <OperationPicker multiple current={catalogOperations} operations={capture.operations} onPick={op => setCatalogOperations(prev => toggleOperation(prev, op))} /></span>}
          {capture?.mode === 'catalog' && focusOrigin?.kind === 'user' && <>
            <span className="toolbar-separator" aria-hidden="true" />
            <TablePicker key={focus.targetTable ?? ''} tables={tableNames} value={focus.targetTable ? focus.nameOf(focus.targetTable) : null}
              onPick={name => focus.pickTable(name ? tableKeyOf(capture.investigation.connection, name) : null)} /></>}
          {focus.context && <span className="focus-context" title="Access objects on this user's configured paths to the table. Other principals are hidden.">
            for {focus.nameOf(focus.context)}
            <button onClick={focus.clearContext}>Show all principals</button></span>}
          <div className="focus-actions">
            <button className="modal-close" aria-label="Close focus" title={link && from ? `Close and return to ${from.label} (Esc)` : 'Close (Esc)'} onClick={closeFocus}>✕</button>
          </div>
        </div>
        {(focus.answers.length > 0 || focus.reading || focus.readError || focus.notFound.length > 0) && <div className="focus-answers">
          {focus.answers.length > 0 && <OperationAnswers answers={focus.answers} />}
          {focus.reading && <span className="muted">Reading the user and table by name…</span>}
          {focus.readError && <span className="diagnostic" role="alert">Read failed: {focus.readError}</span>}
          {focus.notFound.length > 0 && <span className="diagnostic" role="alert">Not found on the server: {focus.notFound.join(', ')}</span>}
        </div>}
        <div className="focus-body">
        {focus.graph && <ObjectGraphView graph={focus.graph} cards={focus.cards} columns={focus.columns} selected={focus.origin} origin={focus.origin} onSelect={focus.reroot} onOpen={focus.openDetail} showAll
          muted={focus.muted} covered={focus.covered} noPath={focus.noPath} cancelledEdges={focus.cancelledEdges}
          routes={focus.routes} stubs={focus.stubs} highlight={lines.highlight} diagramStyle={diagramStyle}
          actions={<DiagramMenu />} />}
        {!focus.graph?.edges.length && <p className="muted">No relationships observed in this capture.</p>}
        </div>
      </div>
    </ModalWrapper>
    <ObjectDetailModal detail={focus.detail} onClose={focus.closeDetail} open={openElsewhere} select={focus.openDetail} />
  </div>;
}
