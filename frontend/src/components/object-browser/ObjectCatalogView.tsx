import { Component, lazy, Suspense, useId, useState, type ReactNode } from 'react';
import { css } from '@emotion/css';
import { CatalogGraphView, type CatalogGraphViewProps } from './CatalogGraphView';
import { DiagramMenu } from './DiagramMenu';

// The only import of the experiment outside its directory. Delete this loader and
// the Spatial branch/choice to remove it; neither shared nor core code depends on it.
const SpatialObjectView = lazy(() => import('./experimental-spatial/SpatialObjectView'));

const catalogView = css`
  display: flex; flex: 1; flex-direction: column; min-height: 0; min-width: 0;
`;

class SpatialBoundary extends Component<{ children: ReactNode; fallback: ReactNode }, { failed: boolean }> {
  state = { failed: false };
  static getDerivedStateFromError() { return { failed: true }; }
  render() {
    return this.state.failed ? <><p role="alert">Isometric view unavailable. Showing the graph view.</p>{this.props.fallback}</> : this.props.children;
  }
}

/** One integration point. View choice is session-local, separate from card style
 * preferences and object-link identity. Collection and selected objects live above it. */
export function ObjectCatalogView(props: CatalogGraphViewProps) {
  const [view, setView] = useState<'graph' | 'spatial'>('graph');
  const id = useId();
  const choose = (next: typeof view) => { props.onHover(null);setView(next); };
  const menu = <DiagramMenu graphOptions={view === 'graph'}>
    <div className="menu-section" role="radiogroup" aria-labelledby={`${id}-view`}>
      <span id={`${id}-view`} className="menu-heading">View</span>
      <label><input type="radio" name={`${id}-view`} checked={view === 'graph'} onChange={() => choose('graph')} />Graph</label>
      <label><input type="radio" name={`${id}-view`} checked={view === 'spatial'} onChange={() => choose('spatial')} />Isometric</label>
    </div>
  </DiagramMenu>;
  const current = <CatalogGraphView {...props} diagramMenu={menu} />;
  return <div className={catalogView}>
    {view === 'graph' ? current : <SpatialBoundary fallback={current}>
      <Suspense fallback={<p>Loading isometric view…</p>}>
        <SpatialObjectView model={props.model} hoverGraph={props.hoverGraph} onHover={props.onHover} highlight={props.lines.highlight} essential={props.lines.essential}
          onSelect={props.onSelect} onFocus={props.onFocus} onDatabase={props.onDatabase} onClear={props.onClear} actions={menu} />
      </Suspense>
    </SpatialBoundary>}
  </div>;
}
