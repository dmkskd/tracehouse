import { useMemo, useState } from 'react';
import { stepTo, type FocusTrail } from './focusTrail';
import { useUserPreferenceStore } from '../../stores/userPreferenceStore';
import { describeDetail, describeObjects, focusGraphFrom, layoutFocus, withRoutes, essentialEdges, type ObjectCard, type ObjectGraph } from '@tracehouse/core/services/object-browser';


/** Focus modal state. `trail` runs from where focus opened to the current `origin`;
 * each click re-roots, and clicking a step on the trail goes back to it. Membership depends on the origin only, so inspecting
 * a card does not change it. `project` gives the graph for one focus object. */
export function useObjectFocus(project: (focus: string | null) => ObjectGraph | null) {
  const [open, setOpen] = useState(false);
  const [trail, setTrail] = useState<FocusTrail>([]);
  const origin = trail.at(-1) ?? null;
  const [detailKey, setDetailKey] = useState<string | null>(null);
  const route = useUserPreferenceStore(s => s.objectDiagramLines.route);
  const essential = useUserPreferenceStore(s => s.objectDiagramLines.essential);
  const source = useMemo(() => origin ? project(origin) : null, [project, origin]);
  const graph = useMemo(() => source && origin ? focusGraphFrom(source, origin) : null, [source, origin]);
  const cards = useMemo(() => source ? describeObjects(source) : new Map<string, ObjectCard>(), [source]);
  // Essential lines drop broad privileges' per-table lines before layout; the view draws their stubs.
  const drawn = useMemo(() => graph && essential ? essentialEdges(graph) : { graph, stubs: [] as string[] }, [graph, essential]);
  const layout = useMemo(() => withRoutes(drawn.graph, drawn.graph ? layoutFocus(drawn.graph) : [], route), [drawn, route]);
  const detail = useMemo(() => {
    const node = source?.nodes.find(n => n.key === detailKey);
    return node && source ? describeDetail(node, source) : null;
  }, [source, detailKey]);
  return {
    open: open && !!source?.nodes.some(n => n.key === origin),
    trail, origin, graph: drawn.graph, stubs: drawn.stubs, cards, columns: layout.columns, routes: layout.routes, detail,
    /** Name of any object in the current projection, for the trail. */
    nameOf: (key: string) => source?.nodes.find(n => n.key === key)?.name ?? key,
    openAt: (key: string) => { setTrail([key]); setOpen(true); },
    reroot: (key: string) => setTrail(previous => stepTo(previous, key)),
    close: () => setOpen(false),
    openDetail: setDetailKey,
    closeDetail: () => setDetailKey(null),
  };
}
