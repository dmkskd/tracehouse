import { useEffect, useRef, useState } from 'react';
import { showMenu } from '../common/showMenuStyles';

/** What the catalog shows: which databases' tables, and whether the default user is drawn.
 * The button label is the summary; more rows can join the panel. */
export function ObjectShowMenu({ systemTables, onSystemTables, defaultUser, onDefaultUser, disabled = false }: {
  systemTables: boolean; onSystemTables: (system: boolean) => void;
  defaultUser: boolean; onDefaultUser: (show: boolean) => void; disabled?: boolean;
}) {
  const [open, setOpen] = useState(false);
  const root = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!open) return;
    const close = (event: MouseEvent) => { if (!root.current?.contains(event.target as Node)) setOpen(false); };
    const escape = (event: KeyboardEvent) => { if (event.key === 'Escape') setOpen(false); };
    document.addEventListener('mousedown', close);document.addEventListener('keydown', escape);
    return () => { document.removeEventListener('mousedown', close);document.removeEventListener('keydown', escape); };
  }, [open]);
  const summary = `${systemTables ? 'system' : 'all databases'}${defaultUser ? '' : ' · no default'}`;
  const changed = systemTables || !defaultUser;
  return <div className={showMenu} ref={root} data-align="left">
    <button type="button" className="filters-button" aria-expanded={open} aria-haspopup="dialog" aria-label="Shown objects" onClick={() => setOpen(!open)}>
      <span className="label">Show</span><span className="value">{summary}</span>
      <span className="caret" aria-hidden="true">▾</span>
    </button>
    {open && <div className="menu-panel" role="dialog" aria-label="Shown objects">
      <div className="rows">
        <span className="phrase">Databases</span>
        <span className="choices">
          <button type="button" aria-pressed={!systemTables} disabled={disabled} onClick={() => onSystemTables(false)}>all databases</button>
          <button type="button" aria-pressed={systemTables} disabled={disabled} onClick={() => onSystemTables(true)}>system</button>
        </span>
        <span />
        <span className="note">Collects the catalog again</span>
        <span className="phrase">Hide</span>
        <span className="choices">
          <button type="button" aria-pressed={!defaultUser} title="The default user, its links and privileges held only by it"
            onClick={() => onDefaultUser(!defaultUser)}>{!defaultUser && <span aria-hidden="true">✓ </span>}default user</button>
        </span>
      </div>
      <div className="footer">
        <span>Default: all databases</span>
        {changed && <button type="button" disabled={disabled} onClick={() => { if (systemTables) onSystemTables(false); if (!defaultUser) onDefaultUser(true); }}>Reset to default</button>}
      </div>
    </div>}
  </div>;
}
