import { useEffect, useId, useRef, useState } from 'react';
import { DIAGRAM_STYLES } from './diagramStyles';
import { OBJECT_DIAGRAM_LINE_DEFAULTS, useUserPreferenceStore, type ObjectDiagramLines, type ObjectDiagramStyle } from '../../stores/userPreferenceStore';

/** Diagram options: label and what the option does. */
const OPTIONS: [keyof ObjectDiagramLines, string, string][] = [
  ['route', 'Route lines', 'Long lines run between cards'],
  ['highlight', 'Highlight on hover', 'Hovering a card dims unrelated lines'],
  ['essential', 'Essential lines', 'Database and global privileges draw one short line'],
];

/** One "Diagram" button: the drawing style and the line options, kept per browser and
 * shared by catalog and focus. Styled like the Events filter popover. */
export function DiagramMenu() {
  const [open, setOpen] = useState(false);
  // The menu renders on the catalog page and in focus; ids and the radio group stay per instance.
  const id = useId();
  const root = useRef<HTMLDivElement>(null);
  const style = useUserPreferenceStore(s => s.objectDiagramStyle);
  const setStyle = useUserPreferenceStore(s => s.setObjectDiagramStyle);
  const lines = useUserPreferenceStore(s => s.objectDiagramLines);
  const toggleLine = useUserPreferenceStore(s => s.toggleObjectDiagramLine);
  const databaseColumn = useUserPreferenceStore(s => s.objectDatabaseColumn);
  const toggleDatabaseColumn = useUserPreferenceStore(s => s.toggleObjectDatabaseColumn);
  useEffect(() => {
    if (!open) return;
    const close = (event: MouseEvent) => { if (!root.current?.contains(event.target as Node)) setOpen(false); };
    const escape = (event: KeyboardEvent) => { if (event.key === 'Escape') { event.stopPropagation(); setOpen(false); } };
    document.addEventListener('mousedown', close);
    document.addEventListener('keydown', escape, true);
    return () => { document.removeEventListener('mousedown', close); document.removeEventListener('keydown', escape, true); };
  }, [open]);
  return <div className="diagram-menu" ref={root}>
    <button aria-haspopup="true" aria-expanded={open} data-active={open} onClick={() => setOpen(value => !value)}>Diagram</button>
    {open && <div className="diagram-panel" role="group" aria-label="Diagram options">
      <div className="menu-section" role="radiogroup" aria-labelledby={`${id}-style`}><span id={`${id}-style`} className="menu-heading">Style</span>
        {(Object.entries(DIAGRAM_STYLES) as [ObjectDiagramStyle, typeof DIAGRAM_STYLES[ObjectDiagramStyle]][]).map(([name, entry]) =>
          <label key={name} title={entry.hint}><input type="radio" name={`${id}-style`} checked={style === name} onChange={() => setStyle(name)} />{entry.label}</label>)}
      </div>
      <div className="menu-section" role="group" aria-labelledby={`${id}-columns`}><span id={`${id}-columns`} className="menu-heading">Columns</span>
        <label title="Show databases aligned with their tables · Default: off"><input type="checkbox" checked={databaseColumn} onChange={toggleDatabaseColumn} />Show Database</label>
      </div>
      <div className="menu-section" role="group" aria-labelledby={`${id}-lines`}><span id={`${id}-lines`} className="menu-heading">Lines</span>
        {OPTIONS.map(([name, label, hint]) => <label key={name} title={`${hint} · Default: ${OBJECT_DIAGRAM_LINE_DEFAULTS[name] ? 'on' : 'off'}`}>
          <input type="checkbox" checked={lines[name]} onChange={() => toggleLine(name)} />{label}
        </label>)}
      </div>
    </div>}
  </div>;
}
