import { useMemo, type ReactNode } from 'react';
import { essentialEdges, layoutCatalog, withRoutes, withSourceNotices, type ObjectGraph } from '@tracehouse/core/services/object-browser';
import type { ObjectDiagramLines, ObjectDiagramStyle } from '../../stores/userPreferenceStore';
import type { ObjectViewActions, ObjectViewModel } from './shared/ObjectViewModel';
import { ObjectGraphView } from './ObjectGraphView';
import { DiagramMenu } from './DiagramMenu';

export interface CatalogGraphViewProps extends ObjectViewActions {
  model: ObjectViewModel;
  hoverGraph: ObjectGraph | null;
  onHover: (key: string | null) => void;
  lines: ObjectDiagramLines;
  diagramStyle: ObjectDiagramStyle;
  pages: readonly number[];
  onMore: (column: number) => void;
  showDatabaseColumn: boolean;
  diagramMenu?: ReactNode;
}

/** Adapter for the existing column renderer. Connector suppression, pagination and
 * routing stay here; the semantic model passed to another renderer is untouched. */
export function CatalogGraphView({ model, hoverGraph, onHover, lines, diagramStyle, pages, onMore,
  showDatabaseColumn, diagramMenu, onSelect, onFocus, onDatabase, onClear }: CatalogGraphViewProps) {
  const layout = useMemo(() => {
    const drawn = lines.essential ? essentialEdges(model.graph) : { graph: model.graph, stubs: [] as string[] };
    const columns = withSourceNotices(layoutCatalog(model.graph, model.selected, undefined, pages, model.muted, showDatabaseColumn), [...model.coverage]);
    return { ...withRoutes(drawn.graph, columns, lines.route), ...drawn };
  }, [model.graph, model.selected, model.muted, model.coverage, pages, showDatabaseColumn, lines.essential, lines.route]);
  const hoverEdges = useMemo(() =>
    (hoverGraph && lines.essential ? essentialEdges(hoverGraph).graph : hoverGraph)?.edges.filter(e => e.kind === 'candidate-scope'),
  [hoverGraph, lines.essential]);
  const stubs = useMemo(() => hoverGraph && lines.essential
    ? [...new Set([...layout.stubs, ...essentialEdges(hoverGraph).stubs])] : layout.stubs, [hoverGraph, lines.essential, layout.stubs]);
  return <ObjectGraphView graph={layout.graph} lineageGraph={model.graph} hoverLineageGraph={hoverGraph}
    cards={model.cards} columns={layout.columns} routes={layout.routes} stubs={stubs} muted={model.muted} covered={model.covered}
    selected={model.selected} onSelect={onSelect} onFocus={onFocus} onMore={onMore} highlight={lines.highlight}
    routePreviews={lines.route} previewEdges={hoverEdges} onHover={onHover} diagramStyle={diagramStyle}
    databases={showDatabaseColumn ? { items: [...model.databases], selected: model.selectedDatabase, onSelect: onDatabase } : undefined}
    actions={<>{(model.selected || model.selectedDatabase) && <button onClick={onClear}>Clear selection</button>}{diagramMenu ?? <DiagramMenu />}</>} />;
}
