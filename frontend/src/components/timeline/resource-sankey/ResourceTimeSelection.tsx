import { useEffect, useId, useRef, useState } from 'react';
import { css } from '@emotion/css';
import { controlsStyle } from '../TimeTravelControls.styles';
import { segmentsStyle } from '../TimeTravelSegments.styles';

const style = css`
  position: relative; flex-shrink: 0;
  .range-options { position: absolute; top: calc(100% + 6px); right: 0; z-index: 80;
    display: flex; gap: 3px; width: max-content; max-width: calc(100vw - 48px); padding: 5px; border: 1px solid var(--border-primary); border-radius: 8px;
    background: var(--bg-secondary); box-shadow: 0 4px 16px rgba(0,0,0,.2); }
  .range-options button { flex: 0 0 auto; padding: 7px 10px; text-align: center; white-space: nowrap;
    border: 0; border-radius: 4px; background: transparent; color: var(--text-primary); cursor: pointer; font: inherit; }
  .range-options button:hover, .range-options button[aria-pressed=true] { background: var(--bg-tertiary); }
`;

/** Duration options stay below the range trigger rather than widening the toolbar. */
export function ResourceTimeSelection({ mode, span, currentViewMs, onMode, onSpan }: {
  mode: 'instant' | 'range'; span: number; currentViewMs: number;
  onMode: (mode: 'instant' | 'range') => void; onSpan: (span: number) => void;
}) {
  const [open,setOpen] = useState(false);
  const root = useRef<HTMLDivElement>(null), trigger = useRef<HTMLButtonElement>(null);
  const id = useId();
  const options = [{ label: '1m', ms: 60000 },{ label: '5m', ms: 300000 },{ label: '15m', ms: 900000 },{ label: '1h', ms: 3600000 },
    ...([60000,300000,900000,3600000].includes(currentViewMs) ? [] : [{ label: 'Current view', ms: currentViewMs }])];
  useEffect(() => {
    if (!open) return;
    const outside = (event: PointerEvent) => { if (!root.current?.contains(event.target as Node)) setOpen(false); };
    const escape = (event: KeyboardEvent) => {
      if (event.key !== 'Escape') return;
      event.preventDefault(); event.stopPropagation(); setOpen(false); trigger.current?.focus();
    };
    document.addEventListener('pointerdown',outside);
    document.addEventListener('keydown',escape,true);
    return () => { document.removeEventListener('pointerdown',outside); document.removeEventListener('keydown',escape,true); };
  },[open]);
  return <div className={style} ref={root}>
    <div className={controlsStyle}><div className={segmentsStyle} role="group" aria-label="Attribution time selection">
      <button type="button" title="Resource usage at the selected timestamp" aria-pressed={mode === 'instant'} onClick={() => { setOpen(false); onMode('instant'); }}>Instant</button>
      <button ref={trigger} type="button" title="Resource usage over the selected time range" aria-label="Time range" aria-pressed={mode === 'range'} aria-expanded={open} aria-controls={id}
        onClick={() => { if (mode === 'instant') { onSpan(60000); onMode('range'); setOpen(false); } else setOpen(value => !value); }}>Time range · {options.find(option => option.ms === (mode === 'instant' ? 60000 : span))?.label ?? 'Custom'} ▾</button>
    </div></div>
    {open && <div id={id} className="range-options" role="group" aria-label="Attribution range">
      {options.map(option => <button type="button" key={option.label} aria-pressed={span === option.ms}
        onClick={() => { onSpan(option.ms); setOpen(false); trigger.current?.focus(); }}>{option.label}</button>)}
    </div>}
  </div>;
}
