/**
 * TimeRangePicker — compact segmented control for selecting a time range.
 *
 * Preset intervals (15m, 1h, 6h, …) override the query's default.
 * "Custom" opens inline datetime inputs for an absolute range.
 *
 * Styling mirrors the app-wide .tab / .tab.active pattern used in
 * MergeTracker and other views (subtle bg swap, no accent colours).
 */

import React, { useState, useRef, useEffect } from 'react';
import { TIME_RANGE_OPTIONS } from './templateResolution';
import { RangeSlider } from '../shared/RangeSlider';

interface Props {
  value: string | null;
  onChange: (interval: string | null) => void;
  /** Override the default preset list. Each entry needs a label and a ClickHouse interval string. */
  presets?: { label: string; interval: string }[];
}

const SLIDER_ZOOMS = [
  { label: '15m', ms: 15 * 60000 },
  { label: '1h',  ms: 3600000 },
  { label: '6h',  ms: 6 * 3600000 },
  { label: '1d',  ms: 86400000 },
  { label: '2d',  ms: 2 * 86400000 },
  { label: '7d',  ms: 7 * 86400000 },
  { label: '30d', ms: 30 * 86400000 },
];

/** Map ClickHouse interval strings to milliseconds */
const INTERVAL_TO_MS: Record<string, number> = {
  '15 MINUTE': 15 * 60000,
  '1 HOUR':    3600000,
  '6 HOUR':    6 * 3600000,
  '1 DAY':     86400000,
  '2 DAY':     2 * 86400000,
  '7 DAY':     7 * 86400000,
  '30 DAY':    30 * 86400000,
};

export const TimeRangePicker: React.FC<Props> = ({ value, onChange, presets }) => {
  const effectivePresets = presets ?? TIME_RANGE_OPTIONS.map(o => ({ label: o.label, interval: o.interval! }));
  const [showCustom, setShowCustom] = useState(false);
  const [customStart, setCustomStart] = useState('');
  const [customEnd, setCustomEnd] = useState('');
  const [sliderZoomMs, setSliderZoomMs] = useState(SLIDER_ZOOMS[3].ms); // default 1d
  const popoverRef = useRef<HTMLDivElement>(null);

  // Close on click-outside or Escape
  useEffect(() => {
    if (!showCustom) return;
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') setShowCustom(false); };
    const onClick = (e: MouseEvent) => {
      if (popoverRef.current && !popoverRef.current.contains(e.target as Node)) setShowCustom(false);
    };
    document.addEventListener('keydown', onKey);
    document.addEventListener('mousedown', onClick);
    return () => { document.removeEventListener('keydown', onKey); document.removeEventListener('mousedown', onClick); };
  }, [showCustom]);

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
      if (isCustomActive) {
        // Restore previously applied custom range
        const parts = value!.replace('CUSTOM:', '').split(',');
        setCustomStart(parts[0]);
        setCustomEnd(parts[1]);
      } else {
        const presetMs = value ? (INTERVAL_TO_MS[value] ?? 86400000) : 86400000;
        const now = new Date();
        setCustomEnd(toLocalISOString(now));
        setCustomStart(toLocalISOString(new Date(now.getTime() - presetMs)));
        // Match slider zoom to preset (pick same or next-larger zoom)
        const zoom = SLIDER_ZOOMS.find(z => z.ms >= presetMs) ?? SLIDER_ZOOMS[SLIDER_ZOOMS.length - 1];
        setSliderZoomMs(zoom.ms);
      }
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
        {effectivePresets.map(opt => (
          <button key={opt.label} onClick={() => handlePreset(opt.interval)}
            style={btnStyle(isPresetActive(opt.interval))}
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

      {/* Active custom range label */}
      {isCustomActive && !showCustom && (() => {
        const parts = value!.replace('CUSTOM:', '').split(',');
        const fmt = (iso: string) => {
          const d = new Date(iso);
          const pad = (n: number) => String(n).padStart(2, '0');
          return `${pad(d.getMonth() + 1)}/${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
        };
        return (
          <span style={{
            fontSize: 9, color: 'var(--text-muted)', fontFamily: "'Share Tech Mono',monospace",
            whiteSpace: 'nowrap',
          }}>
            {fmt(parts[0])} — {fmt(parts[1])}
          </span>
        );
      })()}

      {/* Custom range popover */}
      {showCustom && (
        <div ref={popoverRef} style={{
          position: 'absolute', top: 'calc(100% + 6px)', right: 0, zIndex: 100,
          background: 'var(--bg-secondary)', border: '1px solid var(--border-primary)',
          borderRadius: 8, padding: '10px 12px', boxShadow: '0 4px 16px rgba(0,0,0,0.25)',
          display: 'flex', flexDirection: 'column', gap: 6, width: 420,
        }}>
          {/* From / To / Apply — compact row */}
          <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <label style={{ fontSize: 9, fontWeight: 600, color: 'var(--text-muted)' }}>From</label>
            <input type="datetime-local" value={customStart} onChange={e => setCustomStart(e.target.value)}
              style={inputStyle} />
            <label style={{ fontSize: 9, fontWeight: 600, color: 'var(--text-muted)' }}>To</label>
            <input type="datetime-local" value={customEnd} onChange={e => setCustomEnd(e.target.value)}
              style={inputStyle} />
            <button onClick={handleCustomApply} disabled={!customStart || !customEnd}
              style={{
                padding: '4px 12px', fontSize: 10, fontWeight: 600, borderRadius: 5, border: 'none',
                cursor: customStart && customEnd ? 'pointer' : 'not-allowed',
                background: customStart && customEnd ? 'var(--bg-primary)' : 'transparent',
                color: customStart && customEnd ? 'var(--text-primary)' : 'var(--text-muted)',
                boxShadow: customStart && customEnd ? '0 1px 3px rgba(0,0,0,0.1)' : 'none',
                transition: 'all 0.15s ease', whiteSpace: 'nowrap',
              }}>
              Apply
            </button>
          </div>
          {/* Zoom level buttons */}
          <div style={{
            display: 'flex', gap: 2, padding: 2, borderRadius: 6,
            background: 'var(--bg-card)', alignSelf: 'center',
          }}>
            {SLIDER_ZOOMS.map(z => (
              <button key={z.label} onClick={() => setSliderZoomMs(z.ms)}
                style={btnStyle(sliderZoomMs === z.ms)}>
                {z.label}
              </button>
            ))}
          </div>
          {/* Slider — full width */}
          <RangeSlider
            rangeStartMs={Date.now() - sliderZoomMs} rangeEndMs={Date.now()}
            start={customStart} end={customEnd}
            onStartChange={setCustomStart} onEndChange={setCustomEnd}
          />
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

