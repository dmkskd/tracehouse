/** Route entry. The page's code loads on first render. */
import { Component, lazy, Suspense } from 'react';
import type { ReactNode } from 'react';

class FeatureBoundary extends Component<{ children: ReactNode }, { failed: boolean }> {
  state = { failed: false };
  static getDerivedStateFromError() { return { failed: true }; }
  render() { return this.state.failed ? <div style={{ padding: 24 }}>Object browser failed to load. Reload this page to retry.</div> : this.props.children; }
}
const LazyObjectBrowserPage = lazy(() => import('./ObjectBrowserPage').then(m => ({ default: m.ObjectBrowserPage })));
export function ObjectBrowser() {
  return <FeatureBoundary><Suspense fallback={<div style={{ padding: 24 }}>Loading object browser…</div>}><LazyObjectBrowserPage /></Suspense></FeatureBoundary>;
}
