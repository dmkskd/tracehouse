import { useEffect, useRef, useState } from 'react';
import { css } from '@emotion/css';
import { showMenu } from '../common/showMenuStyles';

const objectMenu = css`
  .menu-panel { width: 350px; box-sizing: border-box; max-height: calc(100vh - 96px); overflow: auto; }
  .rows { grid-template-columns: 72px minmax(0, 1fr); align-items: start; }
  .phrase { padding-top: 5px; }
  .note { white-space: normal; line-height: 16px; }
  .database-list { display: flex; flex-direction: column; gap: 6px; min-width: 0; }
  .database-list input[type=search] { width: 100%; height: 28px; box-sizing: border-box; padding: 4px 6px; font: inherit; font-size: 11px; color: var(--text-primary); background: var(--bg-primary); border: 1px solid var(--border-primary); border-radius: 4px; }
  .database-options { max-height: 168px; overflow: auto; display: flex; flex-direction: column; }
  .database-options label { display: flex; align-items: center; gap: 6px; min-height: 24px; padding: 2px 0; box-sizing: border-box; cursor: pointer; }
  .database-options input { flex-shrink: 0; margin: 0; }
  .database-name { min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .database-count { flex-shrink: 0; margin-left: auto; color: var(--text-muted); font-size: 11px; font-variant-numeric: tabular-nums; }
  .database-actions { align-items: center; }
  .database-selected { margin-left: auto; color: var(--text-muted); font-size: 11px; }
  .database-empty { padding: 4px 0; color: var(--text-muted); font-size: 11px; }
`;

/** What the catalog shows: which databases' tables, and whether the default user is drawn.
 * The button label is the summary; more rows can join the panel. */
export function ObjectShowMenu({ systemTables, onSystemTables, defaultUser, onDefaultUser, databases, selectedDatabases, onDatabases, disabled = false }: {
  databases: { name: string; count: number }[]; selectedDatabases: string[] | null; onDatabases: (databases: string[] | null) => void;
  systemTables: boolean; onSystemTables: (system: boolean) => void;
  defaultUser: boolean; onDefaultUser: (show: boolean) => void; disabled?: boolean;
}) {
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState('');
  const root = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!open) return;
    const close = (event: MouseEvent) => { if (!root.current?.contains(event.target as Node)) setOpen(false); };
    const escape = (event: KeyboardEvent) => { if (event.key === 'Escape') setOpen(false); };
    document.addEventListener('mousedown', close);document.addEventListener('keydown', escape);
    return () => { document.removeEventListener('mousedown', close);document.removeEventListener('keydown', escape); };
  }, [open]);
  const summary = `${selectedDatabases === null ? systemTables ? 'system' : 'all databases' : selectedDatabases.length === 1 ? selectedDatabases[0] : `${selectedDatabases.length} databases`}${defaultUser ? '' : ' · no default'}`;
  const matchingDatabases = databases.filter(db => db.name.toLowerCase().includes(search.trim().toLowerCase()));
  const selectedCount = selectedDatabases === null ? databases.length : databases.filter(db => selectedDatabases.includes(db.name)).length;
  const changed = systemTables || selectedDatabases !== null || !defaultUser;
  return <div className={`${showMenu} ${objectMenu}`} ref={root} data-align="left">
    <button type="button" className="filters-button" aria-expanded={open} aria-haspopup="dialog" aria-label="Shown objects" onClick={() => setOpen(!open)}>
      <span className="filters-label">Show</span><span className="filters-value">{summary}</span>
      <span className="filters-caret" aria-hidden="true">▾</span>
    </button>
    {open && <div className="menu-panel" role="dialog" aria-label="Shown objects">
      <div className="rows">
        <span className="phrase">Catalog</span>
        <span className="choices">
          <button type="button" aria-pressed={!systemTables} disabled={disabled} onClick={() => onSystemTables(false)}>all databases</button>
          <button type="button" aria-pressed={systemTables} disabled={disabled} onClick={() => onSystemTables(true)}>system</button>
        </span>
        <span />
        <span className="note">Collects the catalog again</span>
        <span className="phrase">Databases</span>
        <div className="database-list">
          <span className="choices database-actions" role="group" aria-label="Database selection">
            <button type="button" aria-pressed={selectedDatabases === null} onClick={() => onDatabases(null)}>All</button>
            <button type="button" aria-pressed={selectedDatabases?.length === 0} onClick={() => onDatabases([])}>None</button>
            <span className="database-selected">{selectedCount} / {databases.length}</span>
          </span>
          <input type="search" aria-label="Find database" placeholder="Find database" value={search} onChange={e => setSearch(e.target.value)} />
          <div className="database-options" role="group" aria-label="Databases">
            {matchingDatabases.map(db => <label key={db.name}>
              <input type="checkbox" checked={selectedDatabases === null || selectedDatabases.includes(db.name)} onChange={e => {
                const current = selectedDatabases ?? databases.map(db => db.name);
                onDatabases(e.target.checked ? [...current, db.name] : current.filter(name => name !== db.name));
              }} /><span className="database-name" title={db.name}>{db.name}</span><span className="database-count" aria-hidden="true" title={`${db.count} tables / views`}>{db.count}</span>
            </label>)}
            {!matchingDatabases.length && <span className="database-empty">{databases.length ? 'No matching databases' : 'No databases collected'}</span>}
          </div>
        </div>
        <span className="phrase">Default user</span>
        <span className="choices" role="group" aria-label="Default user">
          <button type="button" aria-pressed={defaultUser} onClick={() => onDefaultUser(true)}>Show</button>
          <button type="button" aria-pressed={!defaultUser} title="Hide the default user, its links and privileges held only by it"
            onClick={() => onDefaultUser(false)}>Hide</button>
        </span>
      </div>
      <div className="footer">
        <span>Default: all databases</span>
        {changed && <button type="button" disabled={disabled} onClick={() => { onDatabases(null); if (systemTables) onSystemTables(false); if (!defaultUser) onDefaultUser(true); }}>Reset to default</button>}
      </div>
    </div>}
  </div>;
}
