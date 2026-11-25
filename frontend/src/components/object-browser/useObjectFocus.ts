import { useEffect, useMemo, useRef, useState } from 'react';
import { stepTo, type FocusTrail } from './focusTrail';
import { useUserPreferenceStore } from '../../stores/userPreferenceStore';
import { answerFor, coveredOperations, dataOutside, describeDetail, describeObjects, focusGraphFrom, layoutFocus, privilegesOutside, objectLinkFor, objectLinkUrl, withPrincipalContext, withRoutes, essentialEdges, type Capture, type Investigation, type ObjectCard, type ObjectGraph, type ProvenanceAnswer } from '@tracehouse/core/services/object-browser';


/** Focus modal state. `trail` runs from where focus opened to the current `origin`;
 * each click re-roots, and clicking a step on the trail goes back to it. Membership depends on the origin only, so inspecting
 * a card does not change it. `project` gives the graph for one focus object. A `context`
 * principal, set when a link opens focus on a table for one user, narrows the access side
 * to that user's paths while the table is the origin. The chain's user, that context user or
 * the user the trail starts from, is passed on by the detail's links to other pages. */
/** `operations` are the catalog's access types: privileges with none of them are dimmed and listed
 * last. A user and a table (a `target` table on a user origin, or a table origin with a context
 * user) get one answer per access type, from the catalog `capture`, or from `read` of the user and
 * table by name when the catalog does not hold both. Other data objects are then dimmed. */
export function useObjectFocus(project: (focus: string | null, from?: Capture) => ObjectGraph | null, operations: readonly string[] = ['SELECT'],
  capture?: Capture | null, read?: (input: Omit<Investigation, 'connection'>) => Promise<Capture>) {
  const [open, setOpen] = useState(false);
  const [trail, setTrail] = useState<FocusTrail>([]);
  const origin = trail.at(-1) ?? null;
  const [detailKey, setDetailKey] = useState<string | null>(null);
  const [context, setContext] = useState<{ principal: string; root: string } | null>(null);
  const activeContext = context && context.root === origin ? context.principal : null;
  // A table picked while a user is the origin; it applies while that user is the origin.
  const [target, setTarget] = useState<{ user: string; table: string } | null>(null);
  const targetTable = target && target.user === origin ? target.table : null;
  // The user and table answered for, in either form.
  const pair = useMemo(() => targetTable && origin ? { user: origin, table: targetTable } : activeContext && origin ? { user: activeContext, table: origin } : null,
    [targetTable, origin, activeContext]);
  const pairKey = pair ? JSON.stringify([pair.user, pair.table]) : '';
  const inCatalog = !!pair && !!capture && [pair.user, pair.table].every(key => capture.observations.some(o => o.key === key));
  // The by-name read for a pair the catalog does not hold; one per pair, for every access type.
  const [side, setSide] = useState<{ key: string; capture?: Capture; error?: string } | null>(null);
  // The read depends on the pair only; its access type is any one, since a read serves them all.
  const firstOperation = useRef(operations[0] ?? 'SELECT');
  useEffect(() => { firstOperation.current = operations[0] ?? 'SELECT'; }, [operations]);
  useEffect(() => {
    if (!pairKey || inCatalog || !read) return;
    const [user, table] = JSON.parse(pairKey) as [string, string];
    const [, , principal] = JSON.parse(user) as string[], [, , database, name] = JSON.parse(table) as string[];
    let live = true;
    read({ principal, database, table: name, operation: firstOperation.current }).then(
      result => { if (live) setSide({ key: pairKey, capture: result }); },
      (error: unknown) => { if (live) setSide({ key: pairKey, error: error instanceof Error ? error.message : String(error) }); });
    return () => { live = false; };
  }, [pairKey, inCatalog, read]);
  const sideCapture = !inCatalog && side?.key === pairKey ? side.capture : undefined;
  // A read that finds no such user or table answers nothing; focus says what it did not find.
  const missing = pair && sideCapture ? [pair.user, pair.table].filter(key => !sideCapture.observations.some(o => o.key === key))
    .map(key => { const [, kind, ...name] = JSON.parse(key) as string[]; return `${kind} ${name.join('.')}`; }) : [];
  const answerSource = inCatalog ? capture : missing.length ? undefined : sideCapture;
  const answers = useMemo((): ProvenanceAnswer[] => pair && answerSource
    ? operations.map(operation => answerFor(answerSource, pair.user, pair.table, operation)) : [], [pair, answerSource, operations]);
  const route = useUserPreferenceStore(s => s.objectDiagramLines.route);
  const essential = useUserPreferenceStore(s => s.objectDiagramLines.essential);
  // With a target table, privileges link to that table as for a focused table; a by-name read is
  // projected the same way. Until the read arrives, the user's own catalog view shows.
  const source = useMemo(() => {
    if (!origin) return null;
    if (targetTable && sideCapture && !missing.length) {
      return project(targetTable, { ...sideCapture, relationships: sideCapture.relationships.filter(r => r.kind !== 'candidate-scope') });
    }
    return project(targetTable && inCatalog ? targetTable : origin);
  }, [project, origin, targetTable, inCatalog, sideCapture, missing.length]);
  const graph = useMemo(() => {
    if (!source || !origin) return null;
    const lineage = focusGraphFrom(source, origin);
    if (activeContext) return withPrincipalContext(lineage, activeContext, origin).graph;
    // The target table shows even with no path to it, for the broken line to end at.
    const table = targetTable && !lineage.nodes.some(n => n.key === targetTable) ? source.nodes.find(n => n.key === targetTable) : undefined;
    return table ? { ...lineage, nodes: [...lineage.nodes, table] } : lineage;
  }, [source, origin, activeContext, targetTable]);
  const cards = useMemo(() => source ? describeObjects(source) : new Map<string, ObjectCard>(), [source]);
  // Essential lines drop broad privileges' per-table lines before layout; the view draws their stubs.
  const drawn = useMemo(() => graph && essential ? essentialEdges(graph) : { graph, stubs: [] as string[] }, [graph, essential]);
  const outside = useMemo(() => drawn.graph ? privilegesOutside(drawn.graph, operations) : undefined, [drawn, operations]);
  const covered = useMemo(() => drawn.graph ? coveredOperations(drawn.graph, operations) : undefined, [drawn, operations]);
  const layout = useMemo(() => withRoutes(drawn.graph, drawn.graph ? layoutFocus(drawn.graph, outside) : [], route), [drawn, route, outside]);
  // Dimmed: privileges without a selected access type and, while answering for a table, other data objects.
  const muted = useMemo(() => pair && drawn.graph ? new Set([...outside ?? [], ...dataOutside(drawn.graph, pair.table)]) : outside, [outside, drawn, pair]);
  const noPath = pair && answers.length && answers.every(a => a.state === 'no-path') ? { from: pair.user, to: pair.table } : undefined;
  const cancelledEdges = useMemo(() => answers.flatMap(a => a.summary.cancelledEdges), [answers]);
  const chainUser = useMemo(() => {
    const key = context?.principal ?? trail[0];
    const node = source?.nodes.find(n => n.key === key);
    return node?.kind === 'user' ? node.name : undefined;
  }, [context, trail, source]);
  const detail = useMemo(() => {
    const node = source?.nodes.find(n => n.key === detailKey);
    return node && source ? describeDetail(node, source, { contextUser: chainUser }) : null;
  }, [source, detailKey, chainUser]);
  /** URL that reopens this focus, for pages the detail links to. */
  const returnUrl = useMemo(() => {
    const link = objectLinkFor(source?.nodes.find(n => n.key === origin), chainUser);
    return link ? objectLinkUrl(link) : '/objects';
  }, [source, origin, chainUser]);
  return { muted, covered, answers, noPath, cancelledEdges, targetTable,
    /** The by-name read for a pair the catalog does not hold: in flight, or its error. */
    reading: !!pair && !inCatalog && side?.key !== pairKey,
    readError: !inCatalog && side?.key === pairKey ? side.error ?? null : null,
    /** What a by-name read did not find, such as `table system.query_logs`. */
    notFound: missing,
    open: open && !!source?.nodes.some(n => n.key === origin),
    trail, origin, chainUser, returnUrl, graph: drawn.graph, context: activeContext, stubs: drawn.stubs, cards, columns: layout.columns, routes: layout.routes, detail,
    /** Name of any object in the current projection, for the trail. */
    nameOf: (key: string) => source?.nodes.find(n => n.key === key)?.name ?? key,
    openAt: (key: string, principal: string | null = null, table: string | null = null) => {
      setTrail([key]);setContext(principal ? { principal, root: key } : null);setTarget(table ? { user: key, table } : null);setOpen(true);
    },
    /** Picks the table a user origin answers for, or clears it. */
    pickTable: (table: string | null) => { if (origin) setTarget(table ? { user: origin, table } : null); },
    clearContext: () => setContext(null),
    reroot: (key: string) => setTrail(previous => stepTo(previous, key)),
    close: () => setOpen(false),
    openDetail: setDetailKey,
    closeDetail: () => setDetailKey(null),
  };
}
