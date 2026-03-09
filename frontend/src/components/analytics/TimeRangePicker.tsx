/**
 * TimeRangePicker — compact segmented control for selecting a time range.
 *
 * Preset intervals (15m, 1h, 6h, …) override the query's default.
 * "Custom" opens inline datetime inputs for an absolute range.
 *
 * Styling mirrors the app-wide .tab / .tab.active pattern used in
 * MergeTracker and other views (subtle bg swap, no accent colours).
 */

import React, { useState } from 'react';
import { TIME_RANGE_OPTIONS } from './presetQueries';

interface Props {
  value: string | null;
  onChange: (interval: string | null) => void;
}

export const TimeRangePicker: React.FC<Props> = ({ value, onChange }) => {
  const [showCustom, setShowCustom] = useState(false);
  const [customStart, setCustomStart] = useState('');
  const [customEnd, setCustomEnd] = useState('');

  const isCustomActive = value?.startsWith('CUSTOM:') ?? false;
  const isPresetActive = (interval: string) => value === interval;

  const handlePreset = (interval: string) => {
    setShowCustom(false);
    onChange(interval);
  };

  const handleCustomToggle = () => {
    if (showCustom) {
      setShowCustom(false);
    } else {
      setShowCustom(true);
      const now = new Date();
      const dayAgo = new Date(now.getTime() - 86400000);
      setCustomEnd(toLocalISOString(now));
      setCustomStart(toLocalISOString(dayAgo));
    }
  };

  const handleCustomApply = () => {
    if (customStart && customEnd) {
      onChange(`CUSTOM:${customStart},${customEnd}`);
      setShowCustom(false);
    }
  };

  /* ── Tab-style button (matches .tab / .tab.active from index.css) ── */
  const btnStyle = (active: boolean): React.CSSProperties => ({
    padding: '3px 10px',
    fontSize: 10,
    fontWeight: 600,
    border: 'none',
    borderRadius: 5,
    cursor: 'pointer',
    fontFamily: "'Share Tech Mono',monospace",
    background: active ? 'var(--bg-primary)' : 'transparent',
    color: active ? 'var(--text-primary)' : 'var(--text-muted)',
    boxShadow: active ? '0 1px 3px rgba(0,0,0,0.1)' : 'none',
    transition: 'all 0.15s ease',
  });

  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 6, position: 'relative' }}>
      <div style={{
        display: 'flex', gap: 2, padding: 3, borderRadius: 8,
        background: 'var(--bg-secondary)', border: '1px solid var(--border-primary)',
      }}>
        {TIME_RANGE_OPTIONS.map(opt => (
          <button key={opt.label} onClick={() => handlePreset(opt.interval!)}
            style={btnStyle(isPresetActive(opt.interval!))}
            title={`Set time range to ${opt.interval}`}>
            {opt.label}
          </button>
        ))}
        <button onClick={handleCustomToggle}
          style={btnStyle(isCustomActive || showCustom)}
          title="Pick a custom date range">
          Custom
        </button>
      </div>

      {/* Custom range popover */}
      {showCustom && (
        <div style={{
          position: 'absolute', top: 'calc(100% + 6px)', right: 0, zIndex: 100,
          background: 'var(--bg-secondary)', border: '1px solid var(--border-primary)',
          borderRadius: 8, padding: 12, boxShadow: '0 4px 16px rgba(0,0,0,0.25)',
          display: 'flex', flexDirection: 'column', gap: 8, minWidth: 280,
        }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <label style={{ fontSize: 10, fontWeight: 600, color: 'var(--text-muted)', width: 32 }}>From</label>
            <input type="datetime-local" value={customStart} onChange={e => setCustomStart(e.target.value)}
              style={inputStyle} />
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <label style={{ fontSize: 10, fontWeight: 600, color: 'var(--text-muted)', width: 32 }}>To</label>
            <input type="datetime-local" value={customEnd} onChange={e => setCustomEnd(e.target.value)}
              style={inputStyle} />
          </div>
          <button onClick={handleCustomApply} disabled={!customStart || !customEnd}
            style={{
              padding: '5px 0', fontSize: 11, fontWeight: 600, borderRadius: 5, border: 'none',
              cursor: customStart && customEnd ? 'pointer' : 'not-allowed',
              background: customStart && customEnd ? 'var(--bg-primary)' : 'transparent',
              color: customStart && customEnd ? 'var(--text-primary)' : 'var(--text-muted)',
              boxShadow: customStart && customEnd ? '0 1px 3px rgba(0,0,0,0.1)' : 'none',
              transition: 'all 0.15s ease',
            }}>
            Apply
          </button>
        </div>
      )}
    </div>
  );
};

const inputStyle: React.CSSProperties = {
  flex: 1, padding: '4px 8px', fontSize: 11, borderRadius: 4,
  border: '1px solid var(--border-primary)', background: 'var(--bg-card)',
  color: 'var(--text-primary)', fontFamily: "'Share Tech Mono',monospace",
};

function toLocalISOString(d: Date): string {
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
}
