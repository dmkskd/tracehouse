import { useEffect, useRef, useState, type ReactNode } from 'react';
import { css } from '@emotion/css';

const styles = css`
  position: relative;
  button { display: inline-flex; align-items: center; justify-content: center; width: 26px; height: 26px; padding: 4px; border: 0; border-radius: 5px; background: transparent; color: var(--text-secondary); cursor: pointer; }
  button:hover, button[aria-expanded="true"] { background: var(--bg-tertiary); color: var(--text-primary); }
  .resource-info-panel { position: absolute; top: calc(100% + 6px); right: 0; z-index: 80; width: min(360px, calc(100vw - 64px)); padding: 12px; border: 1px solid var(--border-primary); border-radius: 6px; background: var(--bg-tertiary); color: var(--text-primary); box-shadow: 0 6px 20px rgba(0,0,0,.18); font-size: 12px; line-height: 1.5; white-space: normal; }
  strong { font-weight: 600; }
  ul { margin: 6px 0 0; padding-left: 18px; }
  li + li { margin-top: 5px; }
`;

/** Nonmodal help: pointer dismissal and Escape never dismiss the enclosing modal. */
export function ResourceInfoPopover({ id, label, title, children }: {
  id: string; label: string; title: string; children: ReactNode;
}) {
  const [open, setOpen] = useState(false);
  const root = useRef<HTMLDivElement>(null);
  const button = useRef<HTMLButtonElement>(null);
  useEffect(() => {
    if (!open) return;
    const closeOutside = (event: PointerEvent) => {
      if (!root.current?.contains(event.target as Node)) setOpen(false);
    };
    const escape = (event: KeyboardEvent) => {
      if (event.key !== 'Escape') return;
      event.preventDefault(); event.stopPropagation();
      setOpen(false); button.current?.focus();
    };
    document.addEventListener('pointerdown', closeOutside);
    document.addEventListener('keydown', escape, true);
    return () => {
      document.removeEventListener('pointerdown', closeOutside);
      document.removeEventListener('keydown', escape, true);
    };
  }, [open]);
  return <div className={styles} ref={root}>
    <button ref={button} type="button" aria-label={label} title={title} aria-expanded={open} aria-controls={id} onClick={() => setOpen(value => !value)}>
      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.75" aria-hidden="true"><circle cx="12" cy="12" r="9" /><path d="M12 11v6" /><circle cx="12" cy="7" r=".75" fill="currentColor" stroke="none" /></svg>
    </button>
    <div id={id} className="resource-info-panel" hidden={!open}>{children}</div>
  </div>;
}
