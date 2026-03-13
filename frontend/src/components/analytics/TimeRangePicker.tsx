/**
 * TimeRangePicker — compact segmented control for selecting a time range.
 *
 * Preset intervals (15m, 1h, 6h, …) override the query's default.
 * "Custom" opens inline datetime inputs for an absolute range.
 *
 * Styling mirrors the app-wide .tab / .tab.active pattern used in
 * MergeTracker and other views (subtle bg swap, no accent colours).
 */

import React, { useState, useRef, useCallback, useMemo } from 'react';
import { TIME_RANGE_OPTIONS } from './presetQueries';

interface Props {
  value: string | null;
  onChange: (interval: string | null) => void;
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

export const TimeRangePicker: React.FC<Props> = ({ value, onChange }) => {
  const [showCustom, setShowCustom] = useState(false);
  const [customStart, setCustomStart] = useState('');
  const [customEnd, setCustomEnd] = useState('');
  const [sliderZoomMs, setSliderZoomMs] = useState(SLIDER_ZOOMS[3].ms); // default 1d

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
        const now = new Date();
        const dayAgo = new Date(now.getTime() - 86400000);
        setCustomEnd(toLocalISOString(now));
        setCustomStart(toLocalISOString(dayAgo));
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
        <div style={{
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
            rangeMs={sliderZoomMs}
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

/* ── Dual-handle range slider ── */

const TRACK_HEIGHT = 4;
const HANDLE_SIZE = 12;

interface RangeSliderProps {
  rangeMs: number;
  start: string; end: string;
  onStartChange: (v: string) => void;
  onEndChange: (v: string) => void;
}

const RangeSlider: React.FC<RangeSliderProps> = ({ rangeMs, start, end, onStartChange, onEndChange }) => {
  const trackRef = useRef<HTMLDivElement>(null);
  const dragging = useRef<'start' | 'end' | null>(null);

  const rangeEnd = Date.now();
  const rangeStart = rangeEnd - rangeMs;

  const toFrac = (iso: string) => {
    const t = new Date(iso).getTime();
    if (isNaN(t)) return 0;
    return Math.max(0, Math.min(1, (t - rangeStart) / rangeMs));
  };

  const toIso = (frac: number) => {
    const ms = rangeStart + frac * rangeMs;
    return toLocalISOString(new Date(ms));
  };

  const fracFromEvent = useCallback((e: MouseEvent | React.MouseEvent) => {
    const rect = trackRef.current?.getBoundingClientRect();
    if (!rect || rect.width === 0) return 0;
    return Math.max(0, Math.min(1, (e.clientX - rect.left) / rect.width));
  }, []);

  const startFrac = toFrac(start);
  const endFrac = toFrac(end);

  const onMouseDown = (which: 'start' | 'end') => (e: React.MouseEvent) => {
    e.preventDefault();
    dragging.current = which;
    const onMove = (ev: MouseEvent) => {
      const f = fracFromEvent(ev);
      if (dragging.current === 'start') {
        onStartChange(toIso(Math.min(f, endFrac - 0.005)));
      } else {
        onEndChange(toIso(Math.max(f, startFrac + 0.005)));
      }
    };
    const onUp = () => {
      dragging.current = null;
      window.removeEventListener('mousemove', onMove);
      window.removeEventListener('mouseup', onUp);
    };
    window.addEventListener('mousemove', onMove);
    window.addEventListener('mouseup', onUp);
  };

  // Click on track → move nearest handle
  const onTrackClick = (e: React.MouseEvent) => {
    if (dragging.current) return;
    const f = fracFromEvent(e);
    const distStart = Math.abs(f - startFrac);
    const distEnd = Math.abs(f - endFrac);
    if (distStart <= distEnd) {
      onStartChange(toIso(Math.min(f, endFrac - 0.005)));
    } else {
      onEndChange(toIso(Math.max(f, startFrac + 0.005)));
    }
  };

  // Generate ticks adapted to zoom level
  const ticks = useMemo(() => {
    const result: { frac: number; label: string }[] = [];
    const pad = (n: number) => String(n).padStart(2, '0');
    const dayNames = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];

    if (rangeMs <= 3600000) {
      // up to 1h: tick every 10 min
      const stepMs = 10 * 60000;
      const firstTick = Math.ceil(rangeStart / stepMs) * stepMs;
      for (let t = firstTick; t <= rangeEnd; t += stepMs) {
        const f = (t - rangeStart) / rangeMs;
        if (f >= 0 && f <= 1) {
          const d = new Date(t);
          result.push({ frac: f, label: `${pad(d.getHours())}:${pad(d.getMinutes())}` });
        }
      }
    } else if (rangeMs <= 6 * 3600000) {
      // up to 6h: tick every hour
      const stepMs = 3600000;
      const firstTick = Math.ceil(rangeStart / stepMs) * stepMs;
      for (let t = firstTick; t <= rangeEnd; t += stepMs) {
        const f = (t - rangeStart) / rangeMs;
        if (f >= 0 && f <= 1) {
          const d = new Date(t);
          result.push({ frac: f, label: `${pad(d.getHours())}:00` });
        }
      }
    } else if (rangeMs <= 2 * 86400000) {
      // up to 2d: tick every 4 hours
      const stepMs = 4 * 3600000;
      const firstTick = Math.ceil(rangeStart / stepMs) * stepMs;
      for (let t = firstTick; t <= rangeEnd; t += stepMs) {
        const f = (t - rangeStart) / rangeMs;
        if (f >= 0 && f <= 1) {
          const d = new Date(t);
          result.push({ frac: f, label: `${pad(d.getHours())}:00` });
        }
      }
    } else if (rangeMs <= 7 * 86400000) {
      // 7d zoom: tick per day at midnight
      const firstDay = new Date(rangeStart);
      firstDay.setHours(0, 0, 0, 0);
      if (firstDay.getTime() < rangeStart) firstDay.setDate(firstDay.getDate() + 1);
      for (let d = new Date(firstDay); d.getTime() <= rangeEnd; d.setDate(d.getDate() + 1)) {
        const f = (d.getTime() - rangeStart) / rangeMs;
        if (f >= 0 && f <= 1) {
          result.push({ frac: f, label: `${dayNames[d.getDay()]} ${pad(d.getDate())}` });
        }
      }
    } else {
      // 30d zoom: tick every 5 days
      const firstDay = new Date(rangeStart);
      firstDay.setHours(0, 0, 0, 0);
      if (firstDay.getTime() < rangeStart) firstDay.setDate(firstDay.getDate() + 1);
      // Align to multiples of 5
      while (firstDay.getDate() % 5 !== 0) firstDay.setDate(firstDay.getDate() + 1);
      for (let d = new Date(firstDay); d.getTime() <= rangeEnd; d.setDate(d.getDate() + 5)) {
        const f = (d.getTime() - rangeStart) / rangeMs;
        if (f >= 0 && f <= 1) {
          result.push({ frac: f, label: `${pad(d.getMonth() + 1)}/${pad(d.getDate())}` });
        }
      }
    }
    return result;
  }, [rangeStart, rangeEnd, rangeMs]);

  const totalH = HANDLE_SIZE + 4;
  const trackY = (totalH - TRACK_HEIGHT) / 2;

  return (
    <div style={{ padding: '2px 0 4px' }}>
      <div ref={trackRef} onClick={onTrackClick} style={{
        position: 'relative', height: totalH, cursor: 'pointer',
        userSelect: 'none',
      }}>
        {/* Track background (full width, dim) */}
        <div style={{
          position: 'absolute', top: trackY,
          left: 0, right: 0, height: TRACK_HEIGHT, borderRadius: TRACK_HEIGHT / 2,
          background: 'var(--bg-card)', opacity: 0.5,
        }} />
        {/* Selected range bar — bright & highlighted */}
        <div style={{
          position: 'absolute', top: trackY - 1,
          left: `${startFrac * 100}%`, width: `${(endFrac - startFrac) * 100}%`,
          height: TRACK_HEIGHT + 2, borderRadius: (TRACK_HEIGHT + 2) / 2,
          background: 'rgba(99, 102, 241, 0.85)',
          boxShadow: '0 0 6px rgba(99, 102, 241, 0.5)',
        }} />
        {/* Start handle */}
        <div onMouseDown={onMouseDown('start')} style={{
          position: 'absolute', top: (totalH - HANDLE_SIZE) / 2,
          left: `${startFrac * 100}%`, marginLeft: -HANDLE_SIZE / 2,
          width: HANDLE_SIZE, height: HANDLE_SIZE, borderRadius: '50%',
          background: '#e0e0ff', border: '2px solid rgba(99, 102, 241, 0.9)',
          cursor: 'grab', zIndex: 2,
          boxShadow: '0 0 4px rgba(99, 102, 241, 0.4)',
        }} />
        {/* End handle */}
        <div onMouseDown={onMouseDown('end')} style={{
          position: 'absolute', top: (totalH - HANDLE_SIZE) / 2,
          left: `${endFrac * 100}%`, marginLeft: -HANDLE_SIZE / 2,
          width: HANDLE_SIZE, height: HANDLE_SIZE, borderRadius: '50%',
          background: '#e0e0ff', border: '2px solid rgba(99, 102, 241, 0.9)',
          cursor: 'grab', zIndex: 2,
          boxShadow: '0 0 4px rgba(99, 102, 241, 0.4)',
        }} />
      </div>
      {/* Tick labels */}
      <div style={{ position: 'relative', height: 12, marginTop: 1 }}>
        {ticks.map(t => (
          <span key={t.frac} style={{
            position: 'absolute', left: `${t.frac * 100}%`, transform: 'translateX(-50%)',
            fontSize: 8, color: 'var(--text-muted)', fontFamily: "'Share Tech Mono',monospace",
            whiteSpace: 'nowrap',
          }}>
            {t.label}
          </span>
        ))}
      </div>
    </div>
  );
};
