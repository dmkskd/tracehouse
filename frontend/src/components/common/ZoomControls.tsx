import type { ReactNode } from 'react';
import { css } from '@emotion/css';

const style = css`
  position: absolute; right: 16px; bottom: 16px; z-index: 10;
  display: flex; align-items: center; gap: 2px; padding: 4px;
  background: var(--bg-secondary); border: 1px solid var(--border-primary); border-radius: 6px; box-shadow: 0 1px 4px rgba(0, 0, 0, .08);
  button { min-width: 24px; padding: 3px 7px; font-size: 11px; line-height: 1.4; border: 1px solid var(--border-primary); border-radius: 5px;
    background: var(--bg-secondary); color: var(--text-primary); cursor: pointer; font-variant-numeric: tabular-nums; }
  button:hover:not(:disabled) { background: var(--bg-hover, var(--bg-tertiary)); }
  button:disabled { opacity: .5; cursor: default; }
  .zoom-extra { display: flex; gap: 2px; margin-right: 4px; padding-right: 6px; border-right: 1px solid var(--border-primary); }
`;

/** Keeps the controls at the bottom of the visible screen while a tall diagram extends below it. */
const stickyAnchor = css`position: sticky; bottom: 0; height: 0; z-index: 10;`;

/** Zoom out, reset and zoom in, floating at the bottom right of a diagram. The parent
 * must be positioned. With `sticky`, render it as the parent's last child: the controls
 * then stay in view while the diagram runs past the bottom of the screen.
 * `name` completes the accessible labels ("Zoom in Sankey"); `children` adds buttons
 * before a divider on the left, so the zoom buttons keep their place at the corner. */
export function ZoomControls({ zoom, name, onZoomOut, onZoomIn, onReset, canZoomOut, canZoomIn, resetTitle = 'Reset zoom', sticky = false, children }: {
  zoom: number; name: string; onZoomOut: () => void; onZoomIn: () => void; onReset: () => void;
  canZoomOut: boolean; canZoomIn: boolean; resetTitle?: string; sticky?: boolean; children?: ReactNode;
}) {
  const controls = <Controls {...{ zoom, name, onZoomOut, onZoomIn, onReset, canZoomOut, canZoomIn, resetTitle, children }} />;
  return sticky ? <div className={stickyAnchor}>{controls}</div> : controls;
}

function Controls({ zoom, name, onZoomOut, onZoomIn, onReset, canZoomOut, canZoomIn, resetTitle, children }: {
  zoom: number; name: string; onZoomOut: () => void; onZoomIn: () => void; onReset: () => void;
  canZoomOut: boolean; canZoomIn: boolean; resetTitle: string; children?: ReactNode;
}) {
  // Clicks here must not start a pan or select what lies underneath.
  return <div className={style} role="group" aria-label={`${name} zoom`} onMouseDown={e => e.stopPropagation()} onPointerDown={e => e.stopPropagation()} onClick={e => e.stopPropagation()}>
    {children && <div className="zoom-extra">{children}</div>}
    <button type="button" aria-label={`Zoom out ${name}`} title="Zoom out" disabled={!canZoomOut} onClick={onZoomOut}>−</button>
    <button type="button" aria-label={`Reset ${name} zoom`} title={resetTitle} onClick={onReset}>{Math.round(zoom * 100)}%</button>
    <button type="button" aria-label={`Zoom in ${name}`} title="Zoom in" disabled={!canZoomIn} onClick={onZoomIn}>+</button>
  </div>;
}
