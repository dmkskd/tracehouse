import { css } from '@emotion/css';

/** Object browser page, focus modal and graph cards. Card tone comes from `--tone` (objectStyle.ts). */
export const page = css`
  padding: 16px 24px; color: var(--text-primary); height: 100%; overflow: auto; box-sizing: border-box; display: flex; flex-direction: column;
  & > * { flex-shrink: 0; }
  /* The catalog diagram takes the height left on the page; the header and questions keep theirs. */
  & > .catalog-panel { flex: 1 1 auto; min-height: 420px; display: flex; flex-direction: column; }
  .catalog-panel .graph-frame { flex: 1; min-height: 0; }
  .catalog-panel .graph { height: 100%; max-height: none; box-sizing: border-box; }
  .muted { color: var(--text-muted); font-size: 12px; }
  p { line-height: 1.6; }
  .toolbar { display: flex; align-items: center; gap: 14px; flex-wrap: wrap; margin: 0 0 12px; }
  .toolbar .title { display: flex; align-items: center; gap: 8px; margin-right: 6px; }
  .toolbar .beta { font-size: 9px; font-weight: 700; color: #eab308; border: 1px solid rgba(234, 179, 8, 0.35); border-radius: 3px; padding: 0 4px; line-height: 14px; text-transform: uppercase; letter-spacing: .3px; cursor: default; }
  .toolbar h1 { margin: 0; font-size: 17px; font-weight: 500; }
  .toolbar input[type=search] { width: 280px; padding: 6px 9px; border: 1px solid var(--border-primary); border-radius: 5px; background: var(--bg-secondary); color: var(--text-primary); font-size: 12px; }
  .toggle { display: flex; align-items: center; gap: 6px; font-size: 12px; color: var(--text-secondary); cursor: pointer; }
  .toggle input { margin: 0; }
  .capture-info { margin-left: auto; display: flex; align-items: center; gap: 4px; font-size: 11px; color: var(--text-muted); }
  .icon-button { border: 0; background: none; padding: 2px 6px; font-size: 13px; color: var(--text-muted); }
  .icon-button:hover:not(:disabled) { color: var(--text-primary); }
  label { font-size: 12px; color: var(--text-secondary); }
  button { background: var(--bg-secondary); color: var(--text-primary); border: 1px solid var(--border-primary); border-radius: 5px; padding: 9px 12px; cursor: pointer; }
  button:disabled { opacity: .5; cursor: default; }
  /* Sections are separated by a thin rule, as in Resource usage; no boxed panels. */
  .panel { padding: 10px 0 0; border-top: 1px solid var(--border-primary); min-width: 0; }
  .graph { overflow: auto; padding: 10px 4px; max-height: 60vh; }
  .graph-frame { position: relative; }
  .wire-layer { position: absolute; inset: 0; pointer-events: none; }
  .lanes { position: relative; display: grid; row-gap: 24px; align-items: start; }
  .lane { position: relative; align-self: stretch; }
  .band-start::before { content: ''; position: absolute; top: 0; bottom: 0; left: -20px; border-left: 1px dashed var(--border-primary); }
  .band-title { height: 14px; margin-bottom: 8px; font-size: 10px; font-weight: 600; letter-spacing: .08em; text-transform: uppercase; color: var(--text-muted); white-space: nowrap; }
  /* An empty row keeps rows level across columns: same height and spacing as a card of the style. */
  .slot { height: 52px; margin-bottom: 16px; }
  [data-style=bars] .slot { height: 44px; margin-bottom: 24px; }
  [data-style=columns] .slot { display: none; }
  [data-style=metro] .slot { height: 32px; margin: 18px 0; }
  .waypoint { height: 6px; margin-bottom: 12px; }
  /* Focus fills the modal: the graph takes the height left under the header, so zoom stays in view. */
  &.focus-shell { display: flex; flex-direction: column; overflow: hidden; padding: 0; }
  &.focus-shell .focus-body { flex: 1; min-height: 0; display: flex; flex-direction: column; }
  &.focus-shell .graph-frame { flex: 1; min-height: 0; }
  &.focus-shell .graph { height: 100%; max-height: none; box-sizing: border-box; }
  /* Diagram styles (diagramStyles.ts): shared card behavior first, then each style's look under [data-style]. */
  .lane-title { position: sticky; top: 0; z-index: 3; display: flex; align-items: center; gap: 6px; height: 28px; margin-bottom: 12px; padding: 0 10px; border-radius: 6px;
    background: var(--bg-tertiary); color: var(--text-primary); font-size: 12px; font-weight: 500; white-space: nowrap; overflow: hidden; }
  .lane-title > span:last-child { overflow: hidden; text-overflow: ellipsis; }
  .node { box-sizing: border-box; display: block; position: relative; z-index: 1; width: 100%; text-align: left; transition: opacity .15s, background .15s; }
  .node-select { display: block; width: 100%; border: 0; padding: 0; background: transparent; text-align: left; color: inherit; }
  .node-focus { position: absolute; top: 6px; right: 6px; padding: 0 5px; font-size: 11px; line-height: 18px; opacity: 0; transition: opacity .15s; }
  .node:hover .node-focus, .node:focus-within .node-focus { opacity: 1; }
  .node-copy { display: block; min-width: 0; padding-right: 18px; }
  .object-symbol { color: var(--tone); display: inline-block; font: 25px/1.2 sans-serif; vertical-align: middle; margin-right: 8px; }
  .node strong { display: block; font-size: 13px; font-weight: 500; line-height: 18px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .node small { display: block; font-size: 11px; line-height: 16px; color: var(--text-muted); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .node small .badge { margin: 0 0 0 6px; padding: 0 4px; }
  .type-label { display: none; }
  .type-dot { display: inline-block; width: 8px; height: 8px; margin-right: 7px; border-radius: 2px; background: var(--tone); vertical-align: 1px; }
  .node[data-selected=true] { outline: 1.5px solid var(--accent-purple, #9672c9); outline-offset: 2px; }
  .node[data-origin=true] strong { font-weight: 600; }
  .node.dim { opacity: .3; }
  .node.dim:hover { opacity: .8; }
  .badge { display: inline-block; margin: 7px 4px 0 0; font-size: 10px; color: var(--text-secondary); background: var(--bg-secondary); padding: 3px 5px; border-radius: 3px; }
  .badge.warn { color: var(--accent-yellow, #ba913f); }

  /* A · cards: small cards with a soft shadow. */
  [data-style=cards] .node { height: 52px; margin-bottom: 16px; padding: 8px 10px; background: var(--bg-secondary); border-radius: 6px;
    box-shadow: 0 1px 2px rgba(16, 24, 40, .06), 0 0 0 1px rgba(16, 24, 40, .05); }
  [data-style=cards] .node[data-origin=true] { box-shadow: 0 0 0 1.5px var(--accent-purple, #9672c9); }

  /* B · bars: a colored bar with its label, as in Resource usage. */
  [data-style=bars] .node { height: 44px; margin-bottom: 24px; padding: 4px 8px 4px 18px; background: transparent; border-radius: 3px; }
  [data-style=bars] .node::before { content: ''; position: absolute; left: 0; top: 0; bottom: 0; width: 8px; border-radius: 2px; background: var(--tone); }
  [data-style=bars] .node:hover { background: color-mix(in srgb, var(--tone, #9672c9) 6%, transparent); }
  [data-style=bars] .node[data-origin=true]::before { width: 10px; }
  [data-style=bars] .type-dot { display: none; }

  /* C · columns: each column is one list. */
  [data-style=columns] .lane-items { background: var(--bg-secondary); border: 1px solid var(--border-primary); border-radius: 8px; overflow: hidden; }
  [data-style=columns] .lane-items:empty { border: 0; }
  [data-style=columns] .node { height: 44px; padding: 5px 12px; }
  [data-style=columns] .lane-items > * + * { border-top: 1px solid var(--border-secondary, var(--border-primary)); }
  [data-style=columns] .node[data-origin=true] { background: color-mix(in srgb, var(--accent-purple, #9672c9) 10%, var(--bg-secondary)); }
  [data-style=columns] .node[data-selected=true] { outline-offset: -2px; }
  [data-style=columns] .type-dot { width: 7px; height: 7px; border-radius: 50%; }
  [data-style=columns] .waypoint { height: 8px; margin: 0; }

  /* D · metro: pills with the summary as a colored caption above. */
  [data-style=metro] .node { height: 32px; margin: 18px 0; padding: 0 14px; background: var(--bg-secondary); border: 1px solid var(--border-primary); border-radius: 16px; }
  [data-style=metro] .node strong { font-size: 12.5px; line-height: 30px; }
  [data-style=metro] .node small { display: none; }
  [data-style=metro] .type-label { display: block; position: absolute; left: 14px; right: 0; top: -16px; font-size: 9.5px; line-height: 12px; font-weight: 600; letter-spacing: .06em; text-transform: uppercase; color: var(--tone); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  [data-style=metro] .node[data-origin=true] { background: var(--accent-purple, #9672c9); border-color: var(--accent-purple, #9672c9); }
  [data-style=metro] .node[data-origin=true] strong { color: #fff; }
  [data-style=metro] .node-focus { top: 5px; }
  [data-style=metro] .type-dot { display: none; }
  .focus-start { display: inline-flex; align-items: center; gap: 6px; min-width: 0; padding: 0; border: 0; background: none; font: inherit; color: var(--text-secondary); white-space: nowrap; }
  .step-name { max-width: 220px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .focus-gap { position: relative; display: inline-flex; }
  .focus-gap-menu { position: absolute; left: 0; top: calc(100% + 6px); z-index: 80; display: grid; gap: 8px; padding: 10px 12px; width: max-content; font-size: 13px;
    background: var(--bg-secondary); border: 1px solid var(--border-primary); border-radius: 8px; box-shadow: 0 8px 28px rgba(0, 0, 0, .35); }
  .focus-start:hover { color: var(--accent-blue, #6696cf); text-decoration: underline; }
  .focus-separator { color: var(--text-muted); margin: 0 4px; }
  .scope-trail { display: flex; align-items: center; gap: 6px; font-size: 15px; font-weight: 500; }
  .focus-current { display: inline-flex; align-items: center; gap: 6px; min-width: 0; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .focus-actions { display: flex; align-items: center; gap: 8px; flex-shrink: 0; }
  .diagram-menu { position: relative; }
  .diagram-menu > button[data-active=true] { color: #58a6ff; background: rgba(88, 166, 255, .1); border-color: rgba(88, 166, 255, .35); }
  /* The menu sits in the zoom bar at the bottom of the diagram, so it opens upward. */
  .diagram-panel { position: absolute; right: 0; bottom: calc(100% + 11px); z-index: 80; display: grid; gap: 8px; padding: 12px; width: max-content;
    background: var(--bg-secondary); border: 1px solid var(--border-primary); border-radius: 8px; box-shadow: 0 8px 28px rgba(0, 0, 0, .35); }
  .diagram-panel label { display: flex; align-items: center; gap: 6px; font-size: 11px; color: var(--text-primary); cursor: pointer; }
  .diagram-panel input { margin: 0; accent-color: #58a6ff; }
  .diagram-panel .menu-section { display: grid; gap: 7px; }
  .diagram-panel .menu-section + .menu-section { padding-top: 10px; border-top: 1px solid var(--border-primary); }
  .diagram-panel .menu-heading { font-size: 10px; font-weight: 600; letter-spacing: .06em; text-transform: uppercase; color: var(--text-muted); }
  .modal-close { border: 0; background: none; padding: 4px 6px; font-size: 14px; color: var(--text-muted); }
  .focus-header h2 { display: flex; align-items: center; gap: 6px; margin: 0; min-width: 0; white-space: nowrap; }
  .focus-header .object-symbol { margin-right: 0; font-size: 18px; }
  .focus-origin-label { flex-shrink: 0; font-size: 11px; color: var(--text-muted); margin-right: 4px; }
  .focus-header { position: sticky; top: 0; z-index: 3; display: flex; justify-content: space-between; align-items: center; gap: 16px; padding: 10px 24px; background: var(--bg-card); border-bottom: 1px solid var(--border-primary); }
  .focus-body { padding: 0 24px 24px; }
  .coverage { display: flex; gap: 7px; flex-wrap: wrap; margin: 14px 0; }
  .diagnostic { margin: 8px 0; font-size: 12px; color: var(--text-secondary); }
  h2 { font-size: 15px; margin: 0 0 10px; overflow-wrap: anywhere; }
  h3 { font-size: 12px; margin: 15px 0 8px; }
`;
