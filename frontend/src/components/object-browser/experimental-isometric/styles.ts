import { css } from '@emotion/css';

/** Entirely scoped to the experiment. No selectors, variables or global rules are
 * added to the existing graph renderer. Shared card content is styled locally. */
export const spatialStyles = css`
  position: relative; display: flex; flex-direction: column; flex: 1; min-height: 420px; min-width: 0;
  color: var(--text-primary);
  .spatial-gesture-help { position: absolute; width: 1px; height: 1px; overflow: hidden; clip-path: inset(50%); white-space: nowrap; }
  .spatial-canvas-wrap { position: relative; display: flex; flex: 1; min-width: 0; min-height: 380px; }
  .spatial-navigation { position: absolute; right: 16px; bottom: 16px; z-index: 10; max-width: calc(100% - 32px); display: flex; flex-direction: column; align-items: flex-end; gap: 8px; }
  .spatial-navigation > [role=group] { position: static; max-width: 100%; flex-wrap: wrap; justify-content: flex-end; }
  .spatial-canvas-wrap .zoom-extra { flex-wrap: wrap; }
  .spatial-overview { box-sizing: border-box; width: 180px; max-width: 100%; padding: 10px; border: 1px solid var(--border-primary); border-radius: 8px; background: var(--bg-secondary); box-shadow: 0 1px 4px rgba(0,0,0,.06); font-size: 11px; color: var(--text-secondary); }
  .spatial-overview svg { display: block; width: 100%; height: 140px; cursor: grab; touch-action: none; overflow: hidden; border-radius: 3px; }
  .spatial-map-group { fill: var(--bg-tertiary); stroke: color-mix(in srgb, var(--text-muted) 40%, transparent); stroke-width: .6; vector-effect: non-scaling-stroke; }
  .spatial-map-group[data-kind=database] { fill: color-mix(in srgb, var(--accent-teal, #26948c) 10%, var(--bg-secondary)); }
  .spatial-map-group[data-kind=external] { fill: color-mix(in srgb, #5996bc 10%, var(--bg-secondary)); }
  .spatial-map-tile { fill: var(--text-muted); opacity: .25; }
  .spatial-map-viewport { fill: var(--accent-purple, #9672c9); fill-opacity: .07; stroke: var(--accent-purple, #9672c9); stroke-width: 1; stroke-linejoin: round; vector-effect: non-scaling-stroke; pointer-events: auto; }
  .spatial-overview svg:hover .spatial-map-viewport, .spatial-overview svg:focus-visible .spatial-map-viewport { fill-opacity: .12; stroke-width: 1.5; }
  .spatial-controls-menu { position: relative; }
  .spatial-controls-panel { position: fixed; z-index: 100; box-sizing: border-box; width: min(340px, calc(100vw - 32px)); max-height: calc(100vh - 32px); overflow: auto;
    padding: 12px; border: 1px solid var(--border-primary); border-radius: 8px; background: var(--bg-secondary); color: var(--text-primary); box-shadow: 0 8px 28px rgba(0,0,0,.25); font-size: 12px; }
  .spatial-camera-actions > button { display: inline-flex; align-items: center; gap: 6px; width: auto; padding: 6px 10px; text-align: left; font-size: 12px; font-weight: 500;
    background: var(--bg-tertiary); border: 1px solid var(--border-primary); border-radius: 5px; }
  .spatial-camera-actions > button:hover { background: var(--bg-hover, var(--bg-primary)); border-color: var(--text-muted); }
  .spatial-camera-actions { display: flex; align-items: center; justify-content: space-between; gap: 12px; flex-wrap: wrap; padding-bottom: 12px; border-bottom: 1px solid var(--border-primary); }
  .spatial-minimap-toggle { display: flex; align-items: center; gap: 8px; cursor: pointer; }
  .spatial-controls-panel dl { margin: 12px 0; }
  .spatial-controls-panel dl > div { display: grid; grid-template-columns: 55px 1fr; gap: 12px; margin-top: 8px; }
  .spatial-controls-panel dt { color: var(--text-secondary); }
  .spatial-controls-panel dd { margin: 0; line-height: 1.5; }
  .spatial-controls-panel details { padding-top: 10px; border-top: 1px solid var(--border-primary); }
  .spatial-controls-panel summary { cursor: pointer; color: var(--text-secondary); }
  .spatial-controls-panel ul { padding-left: 16px; line-height: 1.5; }
  .spatial-controls-panel .spatial-legend-list { display: grid; gap: 10px; list-style: none; padding: 0; margin: 12px 0; }
  .spatial-legend-list li { display: flex; align-items: center; gap: 12px; }
  .spatial-legend-line { display: inline-block; position: relative; flex: 0 0 32px; width: 32px; height: 0; border-top: 2px solid currentColor; }
  .spatial-legend-arrow::after { content: ''; position: absolute; right: -1px; top: -5px; border-left: 6px solid currentColor; border-top: 4px solid transparent; border-bottom: 4px solid transparent; }
  .spatial-controls-panel p { margin: 8px 0 0; font-size: 11px; line-height: 1.5; color: var(--text-secondary); }
  .spatial-viewport { position: relative; flex: 1; min-width: 0; min-height: 0; overflow: hidden; background: var(--bg-primary); cursor: grab; touch-action: none; user-select: none; }
  .spatial-group-more { align-self: start; margin-top: 8px; }
  .spatial-viewport[data-dragging=true] { cursor: grabbing; }
  .spatial-frame { position: absolute; inset: 0; }
  .spatial-scene { position: absolute; left: 0; top: 0; transform-origin: 0 0; transform-style: preserve-3d; }
  .spatial-lane-header { position: absolute; transform-style: preserve-3d; }
  .spatial-lane-title { font: 600 15px/24px sans-serif; color: var(--text-primary); }
  .spatial-privilege-controls { display: flex; align-items: center; gap: 6px; margin-top: 5px; font-size: 12px; color: var(--text-secondary); }
  .spatial-privilege-controls button { padding: 3px 7px; font-size: 12px; }
  .spatial-platform { position: absolute; box-sizing: border-box; border: 1px solid var(--border-primary); border-radius: 5px;
    background: color-mix(in srgb, var(--accent-purple, #9672c9) 5%, var(--bg-secondary)); transform-style: preserve-3d; }
  .spatial-platform::after { content: ''; position: absolute; left: -1px; right: -1px; top: 100%; height: 8px; background: color-mix(in srgb, var(--text-secondary) 20%, var(--bg-tertiary)); border: 1px solid var(--border-primary); transform-origin: top; transform: rotateX(-90deg); backface-visibility: hidden; }
  .spatial-platform::before { content: ''; position: absolute; top: -1px; bottom: -1px; left: 100%; width: 8px; background: var(--bg-tertiary); border: 1px solid var(--border-primary); transform-origin: left; transform: rotateY(90deg); backface-visibility: hidden; }
  .spatial-platform[data-active=true] { border-color: var(--accent-teal, #26948c); }
  .spatial-platform-title { position: absolute; top: 12px; left: 16px; right: 16px; display: flex; align-items: start; gap: 8px; transform: translateZ(8px); }
  .spatial-platform-title > button { min-width: 0; max-height: 36px; overflow: hidden; text-align: left; font: 500 13px/1.4 monospace; padding: 0; background: none; border: 0; overflow-wrap: anywhere; }
  .spatial-platform-title > strong { min-width: 0; max-height: 36px; overflow: hidden; font: 500 13px/1.4 monospace; overflow-wrap: anywhere; }
  .spatial-platform-title .spatial-fold { position: relative; flex-shrink: 0; margin-left: auto; overflow: visible; max-height: none; width: 44px; height: 44px; margin-top: -8px; margin-right: -8px; display: grid; place-items: center; padding: 0; font: 16px/1 sans-serif; cursor: pointer; border-radius: 6px; }
  .spatial-fold::before { content: ''; position: absolute; inset: -10px; }
  .spatial-platform-title .spatial-fold:hover, .spatial-platform-title .spatial-fold:focus-visible { background: var(--bg-tertiary); outline: 1px solid var(--border-primary); }
  .spatial-platform-note { position: absolute; top: 47px; left: 16px; right: 16px; max-height: 45px; overflow: hidden; font-size: 11px; line-height: 15px; color: var(--text-muted); }
  .spatial-more { position: absolute; bottom: 10px; left: 16px; padding: 4px 7px; font-size: 11px; }
  .spatial-tile { position: absolute; box-sizing: border-box; border: 1px solid var(--border-primary); border-left: 3px solid var(--tone);
    background: var(--bg-raised, var(--bg-secondary)); border-radius: 4px; transform: translateZ(26px); transform-style: preserve-3d; }
  .spatial-tile::after { content: ''; position: absolute; left: -1px; right: -1px; top: 100%; height: 26px; background: color-mix(in srgb, var(--text-secondary) 20%, var(--bg-tertiary));
    border: 1px solid var(--border-primary); border-top: 0; transform-origin: top; transform: rotateX(-90deg); pointer-events: none; backface-visibility: hidden; }
  .spatial-tile::before { content: ''; position: absolute; top: -1px; bottom: -1px; left: 100%; width: 26px; background: color-mix(in srgb, var(--bg-tertiary) 70%, var(--text-secondary));
    border: 1px solid var(--border-primary); border-left: 0; transform-origin: left; transform: rotateY(90deg); pointer-events: none; backface-visibility: hidden; }
  .spatial-face { position: absolute; pointer-events: none; backface-visibility: hidden; background: var(--bg-tertiary); border: 1px solid var(--border-primary); }
  .spatial-north { left: -1px; right: -1px; bottom: 100%; height: 26px; transform-origin: bottom; transform: rotateX(90deg); }
  .spatial-west { top: -1px; bottom: -1px; right: 100%; width: 26px; transform-origin: right; transform: rotateY(-90deg); background: color-mix(in srgb, var(--bg-tertiary) 70%, var(--text-secondary)); }
  .spatial-platform > .spatial-north { height: 8px; }
  .spatial-platform > .spatial-west { width: 8px; }
  .spatial-tile[data-selected=true] { outline: 2px solid var(--accent-purple, #9672c9); outline-offset: 3px; }
  .spatial-tile[data-evidence=revoked] { border-color: var(--accent-yellow, #d29922); border-style: dashed; }
  .spatial-tile[data-muted=true], .spatial-tile[data-dim=true] { background: color-mix(in srgb, var(--bg-secondary) 70%, var(--bg-primary)); color: var(--text-muted); }
  .spatial-tile[data-muted=true] .node-copy, .spatial-tile[data-dim=true] .node-copy { opacity: .65; }
  .spatial-tile[data-muted=true]::before, .spatial-tile[data-muted=true]::after,
  .spatial-tile[data-dim=true]::before, .spatial-tile[data-dim=true]::after { opacity: .4; }
  .spatial-select { display: block; box-sizing: border-box; width: 100%; height: 100%; border: 0; border-radius: 3px;
    padding: 8px 9px; text-align: left; background: transparent; color: inherit; cursor: pointer; }
  .spatial-select:hover { background: color-mix(in srgb, var(--tone, #9672c9) 8%, transparent); }
  .spatial-tile .node-copy { padding-right: 14px; }
  .spatial-tile strong { display: block; font: 500 12px/17px monospace; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .spatial-tile small { display: block; font-size: 11px; line-height: 15px; color: var(--text-muted); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .spatial-tile .node-database, .spatial-tile .type-label { display: none; }
  .spatial-tile .badge { margin: 0 0 0 4px; padding: 0; font-size: 10px; background: none; }
  .spatial-focus { position: absolute; top: 2px; right: 2px; padding: 1px 4px; font-size: 11px; }
  .spatial-evidence { display: block; margin-top: 5px; font-size: 11px; line-height: 14px; color: var(--text-secondary); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .spatial-wire { position: absolute; inset: 0; overflow: visible; pointer-events: none; transform: translateZ(26px); }
  .spatial-caption { padding: 9px 0; font-size: 11px; line-height: 1.5; color: var(--text-muted); }
  .spatial-selection { position: absolute; z-index: 5; left: 12px; top: 12px; box-sizing: border-box; width: min(340px, calc(100% - 24px)); max-height: calc(100% - 56px); overflow: auto; padding: 16px; border: 1px solid var(--border-primary); border-radius: 10px; background: var(--bg-secondary); box-shadow: 0 8px 28px rgba(0,0,0,.18); }
  .spatial-selection-close { position: absolute; right: 8px; top: 8px; width: 28px; height: 28px; padding: 0; border: 0; background: transparent; }
  .spatial-selection-kind { display: flex; align-items: center; gap: 8px; padding-right: 24px; margin-bottom: 12px; color: var(--tone, var(--text-secondary)); text-transform: uppercase; letter-spacing: .07em; font-size: 10px; font-weight: 600; }
  .spatial-selection-kind > span { display: grid; place-items: center; width: 22px; height: 22px; border-radius: 5px; background: var(--tone, var(--text-secondary)); color: white; font-size: 16px; }
  .spatial-selection-name { display: block; font: 600 15px/1.4 sans-serif; overflow-wrap: anywhere; }
  .spatial-selection-summary { margin: 10px 0; padding: 10px; border-radius: 6px; border: 1px solid color-mix(in srgb, var(--tone, var(--border-primary)) 35%, transparent); background: color-mix(in srgb, var(--tone, var(--border-primary)) 8%, var(--bg-secondary)); font-size: 12px; line-height: 1.5; }
  .spatial-selection dl { margin: 12px 0; font-size: 11px; }
  .spatial-selection dl > div { display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1.2fr); gap: 12px; padding: 6px 8px; border-radius: 4px; }
  .spatial-selection dl > div:nth-of-type(odd) { background: var(--bg-tertiary); }
  .spatial-selection dt { color: var(--text-secondary); }
  .spatial-selection dd { margin: 0; text-align: right; font-family: monospace; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .spatial-selection-open { padding: 6px 9px; border: 1px solid var(--border-primary); border-radius: 5px; background: var(--bg-tertiary); font-size: 11px; }
  @media (max-width: 600px) { .spatial-canvas-wrap { min-height: 320px; } }
`;
