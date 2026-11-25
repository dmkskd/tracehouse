import { useEffect, useId, useRef, useState } from 'react';
import { groupOperations, type ProvenanceAnswer, type ProvenanceState, type TableOperation } from '@tracehouse/core/services/object-browser';
import { createTracehousePortal } from '../../providers/TracehousePortal';
import { operationTip } from './objectBrowserStyles';

const PINNED = ['SELECT', 'INSERT'];
const MARKS: Record<ProvenanceState, string> = { granted: '✓', columns: '◐', 'set-role': '!', revoked: '✕', 'no-path': '✕' };
// Pause before the explanation opens, so moving along the chips does not flash it.
const TIP_DELAY_MS = 250, MARGIN = 8;

/** Table-level access types from the server: SELECT and INSERT pinned, the rest under More. One is
 * picked (a provenance), or with `multiple` each pick toggles one and the menu stays open (the catalog). */
export function OperationPicker({ current, operations, onPick, multiple = false }: { current: string | readonly string[]; operations?: TableOperation[];
  onPick: (operation: string) => void; multiple?: boolean }) {
  const selected: readonly string[] = typeof current === 'string' ? [current] : current;
  const selectedMore = selected.filter(op => !PINNED.includes(op));
  const menu = useRef<HTMLDetailsElement>(null);
  const [open, setOpen] = useState(false);
  const [filter, setFilter] = useState('');
  const filterInput = useRef<HTMLInputElement>(null);
  const others = (operations ?? []).filter(op => !PINNED.includes(op.name));
  const needle = filter.trim().toLowerCase();
  const more = groupOperations(others.filter(op => !needle || op.name.toLowerCase().includes(needle) || (op.group ?? '').toLowerCase().includes(needle)));
  const close = () => { if (menu.current) menu.current.open = false; };
  // An open menu closes on Escape or a press outside it.
  useEffect(() => {
    if (!open) return;
    filterInput.current?.focus();
    const onKey = (event: KeyboardEvent) => { if (event.key === 'Escape') close(); };
    const onPress = (event: MouseEvent) => { if (!menu.current?.contains(event.target as Node)) close(); };
    document.addEventListener('keydown', onKey);document.addEventListener('mousedown', onPress);
    return () => { document.removeEventListener('keydown', onKey);document.removeEventListener('mousedown', onPress); };
  }, [open]);
  const pick = (operation: string) => { if (!multiple) close();onPick(operation); };
  return <span className="operation-picker" role="group" aria-label="Operation">
    {PINNED.map(op => <button key={op} type="button" aria-pressed={selected.includes(op)} onClick={() => pick(op)}>{op}</button>)}
    {others.length > 0 && <details className="operation-more" ref={menu} onToggle={event => { setOpen(event.currentTarget.open); if (!event.currentTarget.open) setFilter(''); }}>
      <summary aria-pressed={selectedMore.length > 0}>{selectedMore.length === 0 ? 'More' : selectedMore.length === 1 ? selectedMore[0] : `More · ${selectedMore.length}`} ▾</summary>
      <div className="operation-menu">
        <input ref={filterInput} type="search" className="operation-filter" aria-label="Filter operations" placeholder="Filter" value={filter} onChange={e => setFilter(e.target.value)} />
        {!more.length && <div className="operation-none">No operation matches</div>}
        {more.map(g => <div key={g.group}>
        <div className="operation-group">{g.group}</div>
        {g.names.map(name => <button key={name} type="button" aria-pressed={selected.includes(name)} onClick={() => pick(name)}>{name}</button>)}
      </div>)}</div>
    </details>}
  </span>;
}

/** One chip per operation asked for this user and table: the state, with the explanation on
 * hover or focus. The explanation opens below the chip and stays inside the viewport. */
export function OperationAnswers({ answers, current, onPick }: { answers: ProvenanceAnswer[]; current?: string; onPick?: (operation: string) => void }) {
  const id = useId();
  const [tip, setTip] = useState<{ index: number; anchor: DOMRect } | null>(null);
  const timer = useRef<ReturnType<typeof setTimeout>>();
  const show = (index: number, element: HTMLElement) => {
    clearTimeout(timer.current);
    timer.current = setTimeout(() => setTip({ index, anchor: element.getBoundingClientRect() }), TIP_DELAY_MS);
  };
  const hide = () => { clearTimeout(timer.current);setTip(null); };
  useEffect(() => () => clearTimeout(timer.current), []);
  // Placed once mounted and measured: below the chip, kept inside the viewport.
  const place = (element: HTMLDivElement | null) => {
    if (!element || !tip) return;
    element.style.left = `${Math.max(MARGIN, Math.min(tip.anchor.left, window.innerWidth - MARGIN - element.offsetWidth))}px`;
    element.style.top = `${tip.anchor.bottom + 6}px`;
    element.style.visibility = 'visible';
  };
  const shown = tip && answers[tip.index];
  return <div className="operation-answers">
    {answers.map((a, i) => <button key={a.operation} type="button" className="operation-answer" data-state={a.state} aria-pressed={current === undefined ? undefined : a.operation === current}
      aria-describedby={`${id}-${i}`} onClick={() => onPick?.(a.operation)}
      onMouseEnter={event => show(i, event.currentTarget)} onMouseLeave={hide} onFocus={event => show(i, event.currentTarget)} onBlur={hide}>
      <span className="mark" aria-hidden="true">{MARKS[a.state]}</span><b>{a.operation}</b><span className="hint" aria-hidden="true">ⓘ</span>
    </button>)}
    {answers.map((a, i) => <span key={a.operation} id={`${id}-${i}`} className="operation-desc">{a.line.text}</span>)}
    {shown && createTracehousePortal(<div ref={place} className={operationTip} aria-hidden="true" style={{ left: 0, top: 0, visibility: 'hidden' }}>
      {shown.line.parts.map((part, j) => part.kind === 'plain' ? part.text : <span key={j} className={part.kind}>{part.text}</span>)}
    </div>, document.body)}
  </div>;
}

/** The table a focus on a user answers for: any catalog table, or a `database.table` typed in
 * (a table the catalog does not list is read by name). Empty means every table. */
export function TablePicker({ tables, value, onPick }: { tables: readonly string[]; value: string | null; onPick: (name: string | null) => void }) {
  const id = useId();
  // Starts from `value`; the caller keys the picker by it, so another value starts fresh.
  const [text, setText] = useState(value ?? '');
  const commit = (name: string) => onPick(name.trim() || null);
  return <span className="table-picker">
    <label htmlFor={id}>Table</label>
    <input id={id} type="search" list={`${id}-tables`} placeholder="Any table" value={text}
      onChange={event => { const next = event.target.value;setText(next); if (next === '' || tables.includes(next)) commit(next); }}
      onKeyDown={event => { if (event.key === 'Enter') commit(text); }} />
    <datalist id={`${id}-tables`}>{tables.map(name => <option key={name} value={name} />)}</datalist>
  </span>;
}
