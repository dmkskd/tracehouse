import { useMemo, useState } from 'react';
import { stepTo, type FocusTrail } from './focusTrail';
import { useUserPreferenceStore } from '../../stores/userPreferenceStore';
import { coveredOperations, describeDetail, describeObjects, focusGraphFrom, layoutFocus, privilegesOutside, objectLinkFor, objectLinkUrl, withPrincipalContext, withRoutes, essentialEdges, type ObjectCard, type ObjectGraph, type PrincipalContextSummary } from '@tracehouse/core/services/object-browser';


/** Focus modal state. `trail` runs from where focus opened to the current `origin`;
 * each click re-roots, and clicking a step on the trail goes back to it. Membership depends on the origin only, so inspecting
 * a card does not change it. `project` gives the graph for one focus object. A `context`
 * principal, set when a link opens focus on a table for one user, narrows the access side
 * to that user's paths while the table is the origin. The chain's user, that context user or
 * the user the trail starts from, is passed on by the detail's links to other pages. */
/** `operations`: the catalog's access types; privileges with none of them are dimmed and listed last. */
export function useObjectFocus(project: (focus: string | null) => ObjectGraph | null, operations?: readonly string[]) {
  const [open, setOpen] = useState(false);
  const [trail, setTrail] = useState<FocusTrail>([]);
  const origin = trail.at(-1) ?? null;
  const [detailKey, setDetailKey] = useState<string | null>(null);
  const [context, setContext] = useState<{ principal: string; root: string } | null>(null);
  const activeContext = context && context.root === origin ? context.principal : null;
  const route = useUserPreferenceStore(s => s.objectDiagramLines.route);
  const essential = useUserPreferenceStore(s => s.objectDiagramLines.essential);
  const source = useMemo(() => origin ? project(origin) : null, [project, origin]);
  const focused = useMemo(() => {
    if (!source || !origin) return null;
    const lineage = focusGraphFrom(source, origin);
    return activeContext ? withPrincipalContext(lineage, activeContext, origin) : { graph: lineage };
  }, [source, origin, activeContext]);
  const graph = focused?.graph ?? null;
  // What the paths to the table hold, when a user is in context (withPrincipalContext).
  const contextSummary = useMemo((): PrincipalContextSummary | null => {
    if (!focused || !('revokes' in focused)) return null;
    const { graph: _graph, ...summary } = focused;
    return summary;
  }, [focused]);
  const cards = useMemo(() => source ? describeObjects(source) : new Map<string, ObjectCard>(), [source]);
  // Essential lines drop broad privileges' per-table lines before layout; the view draws their stubs.
  const drawn = useMemo(() => graph && essential ? essentialEdges(graph) : { graph, stubs: [] as string[] }, [graph, essential]);
  const outside = useMemo(() => drawn.graph && operations ? privilegesOutside(drawn.graph, operations) : undefined, [drawn, operations]);
  const covered = useMemo(() => drawn.graph && operations ? coveredOperations(drawn.graph, operations) : undefined, [drawn, operations]);
  const layout = useMemo(() => withRoutes(drawn.graph, drawn.graph ? layoutFocus(drawn.graph, outside) : [], route), [drawn, route, outside]);
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
  return { outside, covered,
    open: open && !!source?.nodes.some(n => n.key === origin),
    trail, origin, chainUser, returnUrl, graph: drawn.graph, context: activeContext, contextSummary, stubs: drawn.stubs, cards, columns: layout.columns, routes: layout.routes, detail,
    /** Name of any object in the current projection, for the trail. */
    nameOf: (key: string) => source?.nodes.find(n => n.key === key)?.name ?? key,
    openAt: (key: string, principal: string | null = null) => {
      setTrail([key]);setContext(principal ? { principal, root: key } : null);setOpen(true);
    },
    clearContext: () => setContext(null),
    reroot: (key: string) => setTrail(previous => stepTo(previous, key)),
    close: () => setOpen(false),
    openDetail: setDetailKey,
    closeDetail: () => setDetailKey(null),
  };
}
