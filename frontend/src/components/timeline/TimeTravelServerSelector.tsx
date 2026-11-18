import { useEffect, useId, useRef, useState } from 'react';
import { TruncatedHost } from '../common/TruncatedHost';
import { updateTimeTravelHostSelection } from './time-travel-host-selection';
import { segmentsStyle } from './TimeTravelSegments.styles';
import { serverSelectorStyle } from './TimeTravelServerSelector.styles';

interface Props {
  hosts: readonly string[];
  selectedHosts: readonly string[];
  onChange: (hosts: string[]) => void;
  /** Keep constrained layouts compact regardless of cluster size. */
  display?: 'auto' | 'dropdown';
}

export function TimeTravelServerSelector({ hosts, selectedHosts, onChange, display = 'auto' }: Props) {
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState('');
  const rootRef = useRef<HTMLDivElement>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const searchRef = useRef<HTMLInputElement>(null);
  const panelId = useId();
  const allSelected = selectedHosts.length === 0 || hosts.every(host => selectedHosts.includes(host));

  useEffect(() => {
    if (!open) return;
    searchRef.current?.focus();
    let pointerInside = false;
    let releaseTimer: ReturnType<typeof setTimeout> | undefined;
    const closeOutside = (event: PointerEvent) => {
      clearTimeout(releaseTimer);
      pointerInside = !!rootRef.current?.contains(event.target as Node);
      if (!pointerInside) setOpen(false);
    };
    const closeOnFocusOutside = (event: FocusEvent) => {
      // Safari can move focus out of the search field without focusing the
      // clicked option. Keep that option mounted until its click completes.
      if (pointerInside) return;
      if (!rootRef.current?.contains(event.target as Node)) setOpen(false);
    };
    const releasePointer = () => {
      clearTimeout(releaseTimer);
      releaseTimer = setTimeout(() => { pointerInside = false; }, 0);
    };
    document.addEventListener('pointerdown', closeOutside, true);
    document.addEventListener('pointerup', releasePointer, true);
    document.addEventListener('pointercancel', releasePointer, true);
    document.addEventListener('focusin', closeOnFocusOutside);
    return () => {
      clearTimeout(releaseTimer);
      document.removeEventListener('pointerdown', closeOutside, true);
      document.removeEventListener('pointerup', releasePointer, true);
      document.removeEventListener('pointercancel', releasePointer, true);
      document.removeEventListener('focusin', closeOnFocusOutside);
    };
  }, [open]);

  const toggleHost = (host: string) => {
    const current = allSelected ? hosts : selectedHosts;
    const next = updateTimeTravelHostSelection(current, host, true);
    if (next.length > 0) onChange(next.length === hosts.length ? [] : next);
  };

  return (
    <div className={`tt-server-selector ${serverSelectorStyle}`} ref={rootRef} role="group" aria-label="Servers"
      onKeyDown={event => {
        if (event.key === 'Escape' && open) {
          event.stopPropagation();
          setOpen(false);
          triggerRef.current?.focus();
        }
      }}>
      {display === 'auto' && hosts.length <= 1 ? (
        <div className={`tt-segments tt-server-tabs ${segmentsStyle}`}>
          <button type="button"
            aria-pressed={allSelected} onClick={() => onChange([])}>All servers</button>
          {hosts.map(host => (
            <button type="button" key={host} aria-label={host}
              aria-pressed={selectedHosts.includes(host)} onClick={event => onChange(updateTimeTravelHostSelection(
                selectedHosts, host, event.shiftKey || event.ctrlKey || event.metaKey,
              ))}
              title={`Select ${host}. Shift, Ctrl, or Command-click to select multiple servers.`}>
              <TruncatedHost name={host} maxLen={10} />
            </button>
          ))}
        </div>
      ) : (
        <div className="tt-server-dropdown">
          <button type="button" ref={triggerRef} className="tt-server-trigger"
            aria-label={!allSelected && selectedHosts.length === 1 ? selectedHosts[0] : undefined}
            aria-expanded={open} aria-controls={panelId}
            onClick={() => { setSearch(''); setOpen(!open); }}>
            <span className="tt-server-summary" title={selectedHosts.length === 1 ? selectedHosts[0] : undefined}>
              {allSelected ? 'All servers' : selectedHosts.length === 1
                ? <TruncatedHost name={selectedHosts[0]} maxLen={18} />
                : `${selectedHosts.length} servers`}
            </span>
            <span aria-hidden="true">▾</span>
          </button>
          {open && (
            <div id={panelId} className="tt-server-panel">
              <input ref={searchRef} className="tt-server-search" type="search" aria-label="Search servers"
                placeholder="Search servers…" value={search} onChange={event => setSearch(event.target.value)} />
              <button type="button" className="tt-server-all" onClick={() => onChange([])}
                aria-pressed={allSelected}>All servers ({hosts.length})</button>
              <div className="tt-server-options">
                {hosts.filter(host => host.toLowerCase().includes(search.toLowerCase())).map(host => (
                  <div key={host} className="tt-server-option">
                    <label title={selectedHosts.length === 1 && selectedHosts[0] === host ? 'At least one server must be selected' : host}>
                      <input type="checkbox" checked={allSelected || selectedHosts.includes(host)}
                        disabled={selectedHosts.length === 1 && selectedHosts[0] === host}
                        onChange={() => toggleHost(host)} />
                      <span>{host}</span>
                    </label>
                    <button type="button" className="tt-server-only" aria-label={`Select only ${host}`}
                      onClick={() => onChange([host])}>Only</button>
                  </div>
                ))}
                {!hosts.some(host => host.toLowerCase().includes(search.toLowerCase())) && (
                  <div className="tt-server-empty">No matching servers</div>
                )}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
