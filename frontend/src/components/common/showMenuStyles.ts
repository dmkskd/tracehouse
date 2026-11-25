import { css } from '@emotion/css';

/** A "Show" button whose label summarizes the choices, and the panel of choices it opens: one
 * phrase per row, its options as buttons. Time Travel's operations and the object browser use it.
 * `data-align="left"` on the root opens the panel from the button's left edge. */
export const showMenu = css`
  position: relative;
  .filters-button { display: flex; align-items: center; gap: 8px; height: 32px; padding: 0 10px 0 0; white-space: nowrap; font-size: 12px; cursor: pointer;
    color: var(--text-primary); background: var(--bg-tertiary); border: 1px solid var(--border-primary); border-radius: 6px; overflow: hidden; }
  .filters-button .filters-label { align-self: stretch; display: flex; align-items: center; padding: 0 10px; margin: 0; font: inherit; line-height: inherit; background: transparent; color: var(--text-muted); border-radius: 0; border-right: 1px solid var(--border-primary); }
  .filters-button .filters-caret { color: var(--text-muted); font-size: 10px; }
  .filters-button[aria-expanded="true"] { border-color: var(--border-accent); }
  .menu-panel { position: absolute; right: 0; top: calc(100% + 6px); z-index: 20; width: max-content; max-width: calc(100vw - 32px); padding: 14px 16px; display: flex; flex-direction: column; gap: 12px;
    background: var(--bg-secondary); border: 1px solid var(--border-primary); border-radius: 8px; box-shadow: 0 10px 30px rgba(0, 0, 0, 0.25); font-size: 12px; color: var(--text-secondary); }
  .rows { display: grid; grid-template-columns: max-content max-content; align-items: center; gap: 8px 12px; }
  .phrase { color: var(--text-secondary); white-space: nowrap; }
  .note { margin-top: -4px; font-size: 11px; color: var(--text-muted); white-space: nowrap; }
  .choices { display: flex; flex-wrap: nowrap; gap: 4px; }
  .choices button { padding: 4px 9px; font-size: 11px; white-space: nowrap; cursor: pointer; border-radius: 4px; border: 1px solid var(--border-primary); background: var(--bg-tertiary); color: var(--text-secondary); }
  .choices button[aria-pressed="true"] { border-color: var(--border-accent); background: rgba(88, 166, 255, 0.15); color: var(--text-primary); }
  .choices button:disabled { opacity: 0.5; cursor: not-allowed; }
  .footer { display: flex; justify-content: space-between; align-items: center; gap: 8px; padding-top: 10px; border-top: 1px solid var(--border-secondary); color: var(--text-muted); font-size: 11px; }
  .footer button { font-size: 11px; padding: 3px 9px; cursor: pointer; border-radius: 4px; border: 1px solid var(--border-primary); background: var(--bg-tertiary); color: var(--text-secondary); }
  &[data-align="left"] .menu-panel { left: 0; right: auto; }
`;
