import { useEffect, useId, useRef, useState } from 'react';
import { EDGE_STYLES, REVOKE_EDGE_STYLE } from '../objectStyle';

/** Camera actions and gesture help beside the shared zoom controls. */
export function CameraControlsMenu({ onReset }: { onReset: () => void }) {
  const [open, setOpen] = useState(false);
  const root = useRef<HTMLDivElement>(null);
  const trigger = useRef<HTMLButtonElement>(null);
  const id = useId();
  useEffect(() => {
    if (!open) return;
    const outside = (event: PointerEvent) => { if (!root.current?.contains(event.target as Node)) setOpen(false); };
    const close = () => setOpen(false);
    const escape = (event: KeyboardEvent) => {
      if (event.key !== 'Escape') return;
      event.preventDefault();event.stopPropagation();setOpen(false);trigger.current?.focus();
    };
    document.addEventListener('pointerdown', outside, true);document.addEventListener('keydown', escape, true);
    const scroll = (event: Event) => { if (!root.current?.contains(event.target as Node)) close(); };
    window.addEventListener('resize', close);window.addEventListener('scroll', scroll, true);
    return () => {
      document.removeEventListener('pointerdown', outside, true);document.removeEventListener('keydown', escape, true);
      window.removeEventListener('resize', close);window.removeEventListener('scroll', scroll, true);
    };
  }, [open]);
  const place = (element: HTMLDivElement | null) => {
    if (!element || !trigger.current) return;
    const anchor = trigger.current.getBoundingClientRect();
    element.style.left = `${Math.max(8, Math.min(anchor.right - element.offsetWidth, window.innerWidth - element.offsetWidth - 8))}px`;
    element.style.top = `${Math.max(8, anchor.top - element.offsetHeight - 8)}px`;
  };
  return <div ref={root} className="spatial-controls-menu">
    <button ref={trigger} type="button" aria-haspopup="dialog" aria-controls={id} aria-expanded={open} onClick={() => setOpen(value => !value)}>Controls</button>
    {open && <div ref={place} id={id} className="spatial-controls-panel" role="dialog" aria-label="Camera controls">
      <button type="button" onClick={() => { onReset();setOpen(false);trigger.current?.focus(); }}><span aria-hidden="true">↺</span>Reset camera</button>
      <dl>
        <div><dt>Rotate</dt><dd>Drag</dd></div>
        <div><dt>Pan</dt><dd>Shift-drag or middle-drag</dd></div>
        <div><dt>Zoom</dt><dd>Mouse wheel or + / −</dd></div>
        <div><dt>Touch</dt><dd>Drag to rotate; two fingers to pan or pinch to zoom</dd></div>
      </dl>
      <details onToggle={event => place(event.currentTarget.closest('.spatial-controls-panel') as HTMLDivElement)}><summary>Diagram legend</summary>
        <ul className="spatial-legend-list">
          <li><span className="spatial-legend-line" style={{ color: EDGE_STYLES.access.stroke }} aria-hidden="true" /><span>Access configuration</span></li>
          <li><span className="spatial-legend-line" style={{ color: EDGE_STYLES.candidate.stroke, borderTopStyle: 'dashed' }} aria-hidden="true" /><span>Privilege target match</span></li>
          <li><span className="spatial-legend-line spatial-legend-arrow" style={{ color: EDGE_STYLES['data-flow'].stroke }} aria-hidden="true" /><span>Data flow</span></li>
          <li><span className="spatial-legend-line" style={{ color: REVOKE_EDGE_STYLE.stroke, borderTopStyle: 'dashed' }} aria-hidden="true" /><span>Partial revoke</span></li>
        </ul>
        <p>Privileges are grouped by their ON target. Tables and views are inside database containers. Configured access does not check query execution.</p>
      </details>
    </div>}
  </div>;
}
