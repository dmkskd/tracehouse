/**
 * RangeSlider — dual-handle slider for selecting a time sub-range within a
 * larger span.  Used by both the Analytics TimeRangePicker and the Time Travel
 * custom popover.
 *
 * The slider shows a track spanning [rangeStartMs, rangeEndMs] with two
 * draggable handles (start/end) and a draggable selected-range bar.
 * Tick labels adapt automatically to the zoom level.
 */

import React, { useRef, useCallback, useMemo } from 'react';

const TRACK_HEIGHT = 4;
const HANDLE_SIZE = 12;

export interface RangeSliderProps {
  /** Total visible range start (epoch ms) */
  rangeStartMs: number;
  /** Total visible range end (epoch ms) */
  rangeEndMs: number;
  /** Selected start (local ISO string, e.g. "2026-03-28T09:00") */
  start: string;
  /** Selected end (local ISO string) */
  end: string;
  onStartChange: (v: string) => void;
  onEndChange: (v: string) => void;
}

function toLocalISOString(d: Date): string {
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

export const RangeSlider: React.FC<RangeSliderProps> = ({
  rangeStartMs, rangeEndMs, start, end, onStartChange, onEndChange,
}) => {
  const trackRef = useRef<HTMLDivElement>(null);
  const dragging = useRef<'start' | 'end' | 'range' | null>(null);

  const rangeMs = rangeEndMs - rangeStartMs;

  const toFrac = (iso: string) => {
    const t = new Date(iso).getTime();
    if (isNaN(t) || rangeMs <= 0) return 0;
    return Math.max(0, Math.min(1, (t - rangeStartMs) / rangeMs));
  };

  const toIso = (frac: number) => {
    const ms = rangeStartMs + frac * rangeMs;
    return toLocalISOString(new Date(ms));
  };

  const fracFromEvent = useCallback((e: MouseEvent | React.MouseEvent) => {
    const rect = trackRef.current?.getBoundingClientRect();
    if (!rect || rect.width === 0) return 0;
    return Math.max(0, Math.min(1, (e.clientX - rect.left) / rect.width));
  }, []);

  const startFrac = toFrac(start);
  const endFrac = toFrac(end);

  const setCursor = (cursor: string) => { document.body.style.cursor = cursor; };
  const clearCursor = () => { document.body.style.cursor = ''; };

  const onMouseDown = (which: 'start' | 'end') => (e: React.MouseEvent) => {
    e.preventDefault();
    dragging.current = which;
    setCursor('ew-resize');
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
      clearCursor();
      window.removeEventListener('mousemove', onMove);
      window.removeEventListener('mouseup', onUp);
    };
    window.addEventListener('mousemove', onMove);
    window.addEventListener('mouseup', onUp);
  };

  const onRangeMouseDown = (e: React.MouseEvent) => {
    e.preventDefault();
    e.stopPropagation();
    dragging.current = 'range';
    setCursor('grabbing');
    const span = endFrac - startFrac;
    const grabOffset = fracFromEvent(e) - startFrac;
    const onMove = (ev: MouseEvent) => {
      const f = fracFromEvent(ev);
      let newStart = f - grabOffset;
      newStart = Math.max(0, Math.min(1 - span, newStart));
      onStartChange(toIso(newStart));
      onEndChange(toIso(newStart + span));
    };
    const onUp = () => {
      dragging.current = null;
      clearCursor();
      window.removeEventListener('mousemove', onMove);
      window.removeEventListener('mouseup', onUp);
    };
    window.addEventListener('mousemove', onMove);
    window.addEventListener('mouseup', onUp);
  };

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

  const ticks = useMemo(() => {
    const result: { frac: number; label: string }[] = [];
    const pad = (n: number) => String(n).padStart(2, '0');
    const dayNames = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];

    if (rangeMs <= 3600000) {
      const stepMs = 10 * 60000;
      const firstTick = Math.ceil(rangeStartMs / stepMs) * stepMs;
      for (let t = firstTick; t <= rangeEndMs; t += stepMs) {
        const f = (t - rangeStartMs) / rangeMs;
        if (f >= 0 && f <= 1) {
          const d = new Date(t);
          result.push({ frac: f, label: `${pad(d.getHours())}:${pad(d.getMinutes())}` });
        }
      }
    } else if (rangeMs <= 6 * 3600000) {
      const stepMs = 3600000;
      const firstTick = Math.ceil(rangeStartMs / stepMs) * stepMs;
      for (let t = firstTick; t <= rangeEndMs; t += stepMs) {
        const f = (t - rangeStartMs) / rangeMs;
        if (f >= 0 && f <= 1) {
          const d = new Date(t);
          result.push({ frac: f, label: `${pad(d.getHours())}:00` });
        }
      }
    } else if (rangeMs <= 2 * 86400000) {
      const stepMs = 4 * 3600000;
      const firstTick = Math.ceil(rangeStartMs / stepMs) * stepMs;
      for (let t = firstTick; t <= rangeEndMs; t += stepMs) {
        const f = (t - rangeStartMs) / rangeMs;
        if (f >= 0 && f <= 1) {
          const d = new Date(t);
          result.push({ frac: f, label: `${pad(d.getHours())}:00` });
        }
      }
    } else if (rangeMs <= 7 * 86400000) {
      const firstDay = new Date(rangeStartMs);
      firstDay.setHours(0, 0, 0, 0);
      if (firstDay.getTime() < rangeStartMs) firstDay.setDate(firstDay.getDate() + 1);
      for (let d = new Date(firstDay); d.getTime() <= rangeEndMs; d.setDate(d.getDate() + 1)) {
        const f = (d.getTime() - rangeStartMs) / rangeMs;
        if (f >= 0 && f <= 1) {
          result.push({ frac: f, label: `${dayNames[d.getDay()]} ${pad(d.getDate())}` });
        }
      }
    } else {
      const firstDay = new Date(rangeStartMs);
      firstDay.setHours(0, 0, 0, 0);
      if (firstDay.getTime() < rangeStartMs) firstDay.setDate(firstDay.getDate() + 1);
      while (firstDay.getDate() % 5 !== 0) firstDay.setDate(firstDay.getDate() + 1);
      for (let d = new Date(firstDay); d.getTime() <= rangeEndMs; d.setDate(d.getDate() + 5)) {
        const f = (d.getTime() - rangeStartMs) / rangeMs;
        if (f >= 0 && f <= 1) {
          result.push({ frac: f, label: `${pad(d.getMonth() + 1)}/${pad(d.getDate())}` });
        }
      }
    }
    return result;
  }, [rangeStartMs, rangeEndMs, rangeMs]);

  const totalH = HANDLE_SIZE + 4;
  const trackY = (totalH - TRACK_HEIGHT) / 2;

  return (
    <div style={{ padding: '2px 0 4px' }}>
      <div ref={trackRef} onClick={onTrackClick} style={{
        position: 'relative', height: totalH, cursor: 'pointer',
        userSelect: 'none',
      }}>
        <div style={{
          position: 'absolute', top: trackY,
          left: 0, right: 0, height: TRACK_HEIGHT, borderRadius: TRACK_HEIGHT / 2,
          background: 'var(--bg-card)', opacity: 0.5,
        }} />
        <div onMouseDown={onRangeMouseDown} style={{
          position: 'absolute', top: trackY - 1,
          left: `${startFrac * 100}%`, width: `${(endFrac - startFrac) * 100}%`,
          height: TRACK_HEIGHT + 2, borderRadius: (TRACK_HEIGHT + 2) / 2,
          background: 'rgba(99, 102, 241, 0.85)',
          boxShadow: '0 0 6px rgba(99, 102, 241, 0.5)',
          cursor: 'grab', zIndex: 1,
        }} />
        <div onMouseDown={onMouseDown('start')} style={{
          position: 'absolute', top: (totalH - HANDLE_SIZE) / 2,
          left: `${startFrac * 100}%`, marginLeft: -HANDLE_SIZE / 2,
          width: HANDLE_SIZE, height: HANDLE_SIZE, borderRadius: '50%',
          background: '#e0e0ff', border: '2px solid rgba(99, 102, 241, 0.9)',
          cursor: 'ew-resize', zIndex: 2,
          boxShadow: '0 0 4px rgba(99, 102, 241, 0.4)',
        }} />
        <div onMouseDown={onMouseDown('end')} style={{
          position: 'absolute', top: (totalH - HANDLE_SIZE) / 2,
          left: `${endFrac * 100}%`, marginLeft: -HANDLE_SIZE / 2,
          width: HANDLE_SIZE, height: HANDLE_SIZE, borderRadius: '50%',
          background: '#e0e0ff', border: '2px solid rgba(99, 102, 241, 0.9)',
          cursor: 'ew-resize', zIndex: 2,
          boxShadow: '0 0 4px rgba(99, 102, 241, 0.4)',
        }} />
      </div>
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
