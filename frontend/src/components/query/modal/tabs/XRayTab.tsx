/**
 * XRayTab — 3D resource corridor with log event scrubber.
 *
 * Renders a 3D "corridor" where:
 *   X = time (seconds)
 *   Y = CPU cores used (width of corridor)
 *   Z = Memory MB (height of corridor)
 *
 * Inner traces show I/O Wait, Read throughput, and Network.
 * A slider scrubs through text_log events, highlighting each event's
 * time position with a vertical marker in the 3D scene.
 */

import React, { useMemo, useState, useEffect, useRef, useCallback } from 'react';
import type { TraceLog } from '@tracehouse/core';
import { useProcessSamples } from '../hooks/useProcessSamples';
import type { ProcessSample } from '../hooks/useProcessSamples';
import { useTraceSampleCounts, hasTraceSamplesInRange, useTimeScopedFlamegraph } from '../hooks/useHotFunctions';
import { SpeedscopeViewer } from '../../../tracing/SpeedscopeViewer';
import { XRayVisualization } from './XRayVisualization';

interface LogEvent {
  t: number;           // seconds from query start
  source: string;      // logger name
  message: string;     // truncated message
  level: string;
}

const fmtMB = (mb: number) =>
  mb < 1024 ? `${mb.toFixed(0)} MB` : `${(mb / 1024).toFixed(1)} GB`;

/* ── Scrubber (React UI outside Canvas) ───────────────────────────────── */

type ScrubberMode = 'time' | 'logs';

const MODE_META: Record<ScrubberMode, { icon: string; label: string; color: string }> = {
  time: { icon: '⏱', label: 'Time', color: '#636EFA' },
  logs: { icon: '☰', label: 'Logs', color: '#FECB52' },
};

const pillStyle = (active: boolean, color: string): React.CSSProperties => ({
  padding: '3px 10px',
  borderRadius: 12,
  fontSize: 10,
  fontWeight: 600,
  letterSpacing: '0.4px',
  cursor: 'pointer',
  border: `1px solid ${active ? color + '66' : '#2a2a3a'}`,
  background: active ? `${color}1a` : 'rgba(255,255,255,0.02)',
  color: active ? color : '#555',
  transition: 'all 0.2s ease',
  userSelect: 'none',
  whiteSpace: 'nowrap',
  backdropFilter: 'blur(4px)',
});

const navBtnStyle: React.CSSProperties = {
  background: 'rgba(255,255,255,0.03)',
  border: '1px solid #2a2a3a',
  color: '#777',
  borderRadius: 6,
  padding: '4px 10px',
  cursor: 'pointer',
  fontSize: 13,
  fontWeight: 500,
  lineHeight: 1,
  transition: 'all 0.15s ease',
};

/* Custom range slider styles injected once */
const SLIDER_STYLE_ID = 'xray-slider-styles';
function ensureSliderStyles() {
  let el = document.getElementById(SLIDER_STYLE_ID) as HTMLStyleElement | null;
  const css = `
    .xray-scrubber-slider {
      -webkit-appearance: none;
      appearance: none;
      width: 100%;
      height: 6px;
      border-radius: 3px;
      background: linear-gradient(90deg, rgba(255,255,255,0.06) 0%, rgba(255,255,255,0.1) 100%);
      outline: none;
      cursor: pointer;
      margin: 0;
    }
    .xray-scrubber-slider::-webkit-slider-thumb {
      -webkit-appearance: none;
      appearance: none;
      width: 14px;
      height: 14px;
      border-radius: 50%;
      background: var(--xray-accent);
      border: 2px solid rgba(0,0,0,0.4);
      box-shadow: 0 0 8px var(--xray-accent-glow), 0 1px 3px rgba(0,0,0,0.5);
      cursor: pointer;
      transition: transform 0.1s ease, box-shadow 0.15s ease;
    }
    .xray-scrubber-slider::-webkit-slider-thumb:hover {
      transform: scale(1.2);
      box-shadow: 0 0 14px var(--xray-accent-glow), 0 1px 4px rgba(0,0,0,0.6);
    }
    .xray-scrubber-slider::-webkit-slider-thumb:active {
      transform: scale(1.1);
    }
    .xray-scrubber-slider::-moz-range-thumb {
      width: 14px;
      height: 14px;
      border-radius: 50%;
      background: var(--xray-accent);
      border: 2px solid rgba(0,0,0,0.4);
      box-shadow: 0 0 8px var(--xray-accent-glow), 0 1px 3px rgba(0,0,0,0.5);
      cursor: pointer;
    }
    .xray-scrubber-slider::-moz-range-track {
      height: 6px;
      border-radius: 3px;
      background: linear-gradient(90deg, rgba(255,255,255,0.06) 0%, rgba(255,255,255,0.1) 100%);
    }
  `;
  if (!el) {
    el = document.createElement('style');
    el.id = SLIDER_STYLE_ID;
    document.head.appendChild(el);
  }
  el.textContent = css;
}

const levelColors: Record<string, string> = {
  Fatal: '#ef4444', Critical: '#ef4444', Error: '#ef4444',
  Warning: '#f59e0b', Notice: '#3b82f6', Information: '#3b82f6',
  Debug: '#6b7280', Trace: '#4b5563',
};

const Scrubber: React.FC<{
  mode: ScrubberMode;
  onModeChange: (m: ScrubberMode) => void;
  logEvents: LogEvent[];
  samples: ProcessSample[];
  activeIdx: number;
  onChange: (idx: number) => void;
}> = ({ mode, onModeChange, logEvents, samples, activeIdx, onChange }) => {
  const items = mode === 'logs' ? logEvents : samples;
  const count = items.length;
  const sliderRef = useRef<HTMLInputElement>(null);

  // Inject custom slider CSS
  useEffect(() => { ensureSliderStyles(); }, []);

  if (count === 0) return null;

  const clampedIdx = Math.max(0, Math.min(count - 1, activeIdx));
  const progressPct = count > 1 ? (clampedIdx / (count - 1)) * 100 : 0;
  const currentT = mode === 'logs'
    ? (logEvents[clampedIdx]?.t ?? 0)
    : (samples[clampedIdx]?.t ?? 0);

  // Detail line content depends on mode
  const detailContent = (() => {
    if (mode === 'logs') {
      const evt = logEvents[clampedIdx];
      if (!evt) return null;
      return (
        <div style={{ display: 'flex', gap: 8, alignItems: 'baseline', minWidth: 0, overflow: 'hidden' }}>
          <span style={{ color: '#FECB52', flexShrink: 0 }}>{evt.t.toFixed(1)}s</span>
          <span style={{
            color: levelColors[evt.level] || '#888',
            flexShrink: 0, fontSize: 10, textTransform: 'uppercase',
          }}>{evt.level}</span>
          <span style={{ color: '#aaa', flexShrink: 0 }}>
            {evt.source.split('(')[0].trim()}
          </span>
          <span style={{
            color: '#555', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
          }}>{evt.message}</span>
        </div>
      );
    }
    // Time mode — show sample metrics
    const s = samples[clampedIdx];
    if (!s) return null;
    return (
      <div style={{ display: 'flex', gap: 14, alignItems: 'baseline', minWidth: 0, overflow: 'hidden' }}>
        <span style={{ color: '#636EFA', flexShrink: 0 }}>{s.t.toFixed(0)}s</span>
        <span style={{ color: '#5577dd' }}>Mem {fmtMB(s.memory_mb)}</span>
        <span style={{ color: '#ddaa33' }}>CPU {s.d_cpu_cores.toFixed(1)}</span>
        <span style={{ color: '#7B83FF' }}>IO {s.d_io_wait_s.toFixed(2)}s</span>
        <span style={{ color: '#00DD99' }}>read_bytes {s.d_read_mb.toFixed(0)} MB/s</span>
        <span style={{ color: '#aaa' }}>{s.thread_count} thr</span>
      </div>
    );
  })();

  const accentColor = MODE_META[mode].color;
  const accentGlow = accentColor + '66';

  // Build track background with filled portion
  const trackBg = `linear-gradient(90deg, ${accentColor}44 0%, ${accentColor}88 ${progressPct}%, rgba(255,255,255,0.08) ${progressPct}%)`;

  return (
    <div style={{
      padding: '8px 14px 10px',
      background: 'linear-gradient(180deg, rgba(15,15,30,0.97) 0%, rgba(8,8,18,0.99) 100%)',
      borderTop: '1px solid rgba(100,110,250,0.12)',
      fontFamily: 'monospace',
      fontSize: 11,
      overflow: 'hidden',
    }}>
      {/* Top row: mode pills + time + nav buttons */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6, minWidth: 0 }}>
        {/* Mode selector pills */}
        <div style={{ display: 'flex', gap: 5, flexShrink: 0 }}>
          {(Object.keys(MODE_META) as ScrubberMode[]).map(m => (
            <span
              key={m}
              style={pillStyle(mode === m, MODE_META[m].color)}
              onClick={() => onModeChange(m)}
            >
              {MODE_META[m].icon} {MODE_META[m].label}
              <span style={{ opacity: 0.5, marginLeft: 4, fontSize: 9 }}>
                {m === 'logs' ? logEvents.length : samples.length}
              </span>
            </span>
          ))}
        </div>

        {/* Time indicator */}
        <span style={{
          color: accentColor,
          fontSize: 13,
          fontWeight: 700,
          flexShrink: 0,
          minWidth: 48,
          textAlign: 'right',
          textShadow: `0 0 10px ${accentGlow}`,
        }}>
          {currentT.toFixed(mode === 'logs' ? 1 : 0)}s
        </span>

        {/* Spacer */}
        <span style={{ flex: 1 }} />

        {/* Nav buttons */}
        <div style={{ display: 'flex', gap: 4, flexShrink: 0 }}>
          <button onClick={() => onChange(Math.max(0, activeIdx - 1))} style={navBtnStyle}>‹</button>
          <button onClick={() => onChange(Math.min(count - 1, activeIdx + 1))} style={navBtnStyle}>›</button>
        </div>
      </div>

      {/* Slider on its own row for full width */}
      <div style={{
        position: 'relative',
        marginBottom: 6,
        paddingTop: 2,
        // Pass accent color to CSS via custom properties
        ['--xray-accent' as string]: accentColor,
        ['--xray-accent-glow' as string]: accentGlow,
      }}>
        <input
          ref={sliderRef}
          type="range"
          className="xray-scrubber-slider"
          min={0}
          max={count - 1}
          value={clampedIdx}
          onChange={e => onChange(parseInt(e.target.value))}
          style={{
            width: '100%',
            background: trackBg,
          }}
        />
      </div>

      {/* Detail row */}
      <div style={{ lineHeight: 1.5, minHeight: 18, display: 'flex', alignItems: 'center', gap: 10 }}>
        {detailContent}
      </div>
    </div>
  );
};

/* ── Host tab pill ────────────────────────────────────────────────────── */

const HostTab: React.FC<{
  label: string;
  title?: string;
  active: boolean;
  onClick: () => void;
}> = ({ label, title, active, onClick }) => (
  <span
    title={title}
    onClick={onClick}
    style={{
      padding: '3px 10px',
      borderRadius: 12,
      fontSize: 10,
      fontWeight: 600,
      letterSpacing: '0.4px',
      cursor: 'pointer',
      border: `1px solid ${active ? '#636EFA66' : '#2a2a3a'}`,
      background: active ? '#636EFA1a' : 'rgba(255,255,255,0.02)',
      color: active ? '#636EFA' : '#555',
      transition: 'all 0.2s ease',
      userSelect: 'none',
      whiteSpace: 'nowrap',
    }}
  >
    {label}
  </span>
);

/* ── Main component ──────────────────────────────────────────────────── */

export interface XRayTabProps {
  queryId: string;
  logs: TraceLog[];
  queryStartTime?: string;
}

export const XRayTab: React.FC<XRayTabProps> = ({
  queryId,
  logs,
  queryStartTime,
}) => {
  const { samples: allSamples, hostSamples, hosts, isLoading: isLoadingSamples, error, fetch: fetchSamples } = useProcessSamples(queryId);
  const { sampleCounts, fetch: fetchSampleCounts } = useTraceSampleCounts(queryId, queryStartTime, queryStartTime);
  const timeScopedFlamegraph = useTimeScopedFlamegraph();
  const [showFlamegraphPopup, setShowFlamegraphPopup] = useState(false);
  const [selectedHost, setSelectedHost] = useState<string | null>(null);
  const [stackedView, setStackedView] = useState(false);
  const [scrubberMode, setScrubberMode] = useState<ScrubberMode>('time');
  const [scrubberIdx, setScrubberIdx] = useState(0);

  // Active samples: "All" (aggregated) or per-host filtered
  const samples = useMemo(() => {
    if (selectedHost === null) return allSamples;
    return hostSamples.get(selectedHost) || [];
  }, [selectedHost, allSamples, hostSamples]);

  // Fetch samples and probe trace_log on mount
  useEffect(() => {
    fetchSamples();
    fetchSampleCounts();
  }, [fetchSamples, fetchSampleCounts]);

  // Convert TraceLog[] to LogEvent[] relative to query start
  const logEvents = useMemo(() => {
    if (!logs.length || !samples.length) return [];

    const maxT = samples[samples.length - 1].t;

    // Parse all log timestamps
    const logTimes = logs.map(log =>
      new Date(log.event_time_microseconds || log.event_time).getTime()
    );

    // Use the first log timestamp as reference, then scale to sample range.
    // Log timestamps are absolute; sample t values are relative to query start.
    // The first log typically fires at query start, so first_log_time ≈ t=0.
    const firstLogMs = Math.min(...logTimes);
    const lastLogMs = Math.max(...logTimes);
    const logSpanMs = lastLogMs - firstLogMs;

    // If we have a queryStartTime, use it to align logs to sample timeline.
    // Otherwise, scale log span to fit within sample range.
    const queryStartMs = queryStartTime ? new Date(queryStartTime).getTime() : null;

    const events: LogEvent[] = [];

    for (let i = 0; i < logs.length; i++) {
      const log = logs[i];
      let elapsed: number;

      if (queryStartMs) {
        elapsed = (logTimes[i] - queryStartMs) / 1000;
      } else if (logSpanMs > 0) {
        elapsed = ((logTimes[i] - firstLogMs) / logSpanMs) * maxT;
      } else {
        elapsed = maxT / 2;
      }

      // Skip if far outside sample range
      if (elapsed < -2 || elapsed > maxT + 2) continue;
      elapsed = Math.max(0, Math.min(maxT, elapsed));

      events.push({
        t: elapsed,
        source: log.source,
        message: (log.message || '').slice(0, 120),
        level: log.level || 'Debug',
      });
    }

    return events.sort((a, b) => a.t - b.t);
  }, [logs, samples, queryStartTime]);

  // Compute highlight time + label from scrubber state
  const scrubberItems = scrubberMode === 'logs' ? logEvents : samples;
  const clampedIdx = Math.max(0, Math.min(scrubberItems.length - 1, scrubberIdx));

  const highlightTime = scrubberItems.length > 0
    ? (scrubberMode === 'logs' ? logEvents[clampedIdx]?.t : samples[clampedIdx]?.t) ?? null
    : null;

  const highlightLabel = scrubberMode === 'logs' && logEvents[clampedIdx]
    ? logEvents[clampedIdx].source.split('(')[0].trim().slice(0, 25)
    : undefined;

  // Offset between queryStartTime and the first process sample.
  // Process sample t=0 corresponds to queryStartTime + sampleOffset seconds.
  // The probe's t_second values are relative to queryStartTime, so we add this offset
  // when mapping scrubber positions to probe keys and absolute times.
  const sampleOffset = samples.length > 0 ? samples[0].elapsed : 0;

  // Compute absolute time window for current scrubber position (for flamegraph popup)
  // Only returns non-null when we know there are profiler samples in this window.
  // Works in both time and logs mode.
  const currentTimeWindow = useMemo(() => {
    if (samples.length === 0 || !queryStartTime || !sampleCounts) return null;
    const startMs = new Date(queryStartTime).getTime();
    let currentT: number;
    let nextT: number;
    if (scrubberMode === 'logs') {
      currentT = logEvents[clampedIdx]?.t ?? 0;
      nextT = currentT + 1;
    } else {
      currentT = samples[clampedIdx]?.t ?? 0;
      nextT = clampedIdx < samples.length - 1 ? samples[clampedIdx + 1].t : currentT + 1;
    }
    // Convert from process-sample-relative t to queryStartTime-relative t_second
    const probeFrom = currentT + sampleOffset;
    const probeTo = nextT + sampleOffset;
    if (!hasTraceSamplesInRange(sampleCounts, probeFrom, probeTo)) return null;
    return {
      from: new Date(startMs + probeFrom * 1000).toISOString(),
      to: new Date(startMs + probeTo * 1000).toISOString(),
      label: `${currentT.toFixed(0)}s – ${nextT.toFixed(0)}s`,
    };
  }, [scrubberMode, samples, logEvents, clampedIdx, queryStartTime, sampleCounts, sampleOffset]);

  // Open flamegraph popup for current time window
  const handleShowFlamegraph = useCallback(() => {
    if (!currentTimeWindow) return;
    timeScopedFlamegraph.clear();
    setShowFlamegraphPopup(true);
    timeScopedFlamegraph.fetch(queryId, currentTimeWindow.from, currentTimeWindow.to, queryStartTime);
  }, [queryId, queryStartTime, currentTimeWindow, timeScopedFlamegraph]);

  // Open flamegraph for a specific t value (from hover card click)
  // t is process-sample-relative; add sampleOffset to get queryStartTime-relative
  const handleShowFlamegraphForT = useCallback((t: number) => {
    if (!queryStartTime || !sampleCounts) return;
    const startMs = new Date(queryStartTime).getTime();
    const absT = t + sampleOffset;
    const from = new Date(startMs + absT * 1000).toISOString();
    const to = new Date(startMs + (absT + 1) * 1000).toISOString();
    timeScopedFlamegraph.clear();
    setShowFlamegraphPopup(true);
    timeScopedFlamegraph.fetch(queryId, from, to, queryStartTime);
  }, [queryId, queryStartTime, sampleCounts, sampleOffset, timeScopedFlamegraph]);

  // Esc closes flamegraph popup (capture phase to prevent parent modal from closing)
  useEffect(() => {
    if (!showFlamegraphPopup) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        e.stopImmediatePropagation();
        e.preventDefault();
        setShowFlamegraphPopup(false);
      }
    };
    window.addEventListener('keydown', onKey, true); // capture phase
    return () => window.removeEventListener('keydown', onKey, true);
  }, [showFlamegraphPopup]);

  // Reset index on mode change
  const handleModeChange = useCallback((m: ScrubberMode) => {
    setScrubberMode(m);
    setScrubberIdx(0);
  }, []);

  // Keyboard navigation
  useEffect(() => {
    const maxIdx = scrubberItems.length - 1;
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'ArrowLeft') {
        setScrubberIdx(prev => Math.max(0, prev - 1));
        e.preventDefault();
      } else if (e.key === 'ArrowRight') {
        setScrubberIdx(prev => Math.min(maxIdx, prev + 1));
        e.preventDefault();
      }
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [scrubberItems.length]);

  // Loading state
  if (isLoadingSamples) {
    return (
      <div style={{
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        height: '100%', minHeight: 400, color: 'var(--text-muted)',
      }}>
        <div style={{ textAlign: 'center' }}>
          <div style={{
            width: 24, height: 24, borderWidth: 2,
            borderStyle: 'solid', borderColor: 'var(--text-muted) transparent var(--text-muted) transparent',
            borderRadius: '50%', animation: 'spin 1s linear infinite',
            margin: '0 auto 12px',
          }} />
          Loading process samples...
        </div>
      </div>
    );
  }

  // Error state
  if (error) {
    return (
      <div style={{
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        height: '100%', minHeight: 400, padding: 40,
      }}>
        <div style={{ textAlign: 'center', maxWidth: 500 }}>
          <div style={{ fontSize: 14, color: '#ef4444', marginBottom: 8 }}>
            Failed to load process samples
          </div>
          <div style={{ fontSize: 12, color: 'var(--text-muted)', marginBottom: 16, fontFamily: 'monospace' }}>
            {error}
          </div>
          <div style={{ fontSize: 12, color: '#888', lineHeight: 1.6 }}>
            This requires <code style={{ background: '#1a1a2e', padding: '1px 4px', borderRadius: 3 }}>
            tracehouse.processes_history</code> — see{' '}
            <code style={{ background: '#1a1a2e', padding: '1px 4px', borderRadius: 3 }}>
            infra/scripts/setup_sampling.sh</code>
          </div>
          <button
            onClick={fetchSamples}
            style={{
              marginTop: 12, padding: '6px 16px', background: '#1a1a2e',
              border: '1px solid #333', borderRadius: 4, color: '#ccc',
              cursor: 'pointer', fontSize: 12,
            }}
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  // No data
  if (samples.length < 2) {
    return (
      <div style={{
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        height: '100%', minHeight: 400, padding: 40,
      }}>
        <div style={{ textAlign: 'center', maxWidth: 400 }}>
          <div style={{ fontSize: 14, color: 'var(--text-secondary)', marginBottom: 8 }}>
            No Process Samples
          </div>
          <div style={{ fontSize: 12, color: 'var(--text-muted)', lineHeight: 1.6 }}>
            This query ran too fast for the process sampler to capture data,
            or the sampler wasn't active when this query executed.
          </div>
        </div>
      </div>
    );
  }

  // Summary stats
  const peakMem = Math.max(...samples.map(s => s.peak_memory_mb));
  const peakCpu = Math.max(...samples.map(s => s.d_cpu_cores));
  const duration = samples[samples.length - 1].t;
  const totalRows = Math.max(...samples.map(s => s.read_rows));

  const multiHost = hosts.length > 1;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
      {/* Host tab bar — only shown for multi-host (distributed) queries */}
      {multiHost && (
        <div style={{
          padding: '4px 16px',
          background: 'var(--bg-secondary, #111)',
          borderBottom: '1px solid var(--border-accent, #333)',
          display: 'flex',
          gap: 6,
          fontSize: 10,
          fontFamily: 'monospace',
          flexShrink: 0,
          alignItems: 'center',
        }}>
          <HostTab
            label="All"
            active={selectedHost === null && !stackedView}
            onClick={() => { setSelectedHost(null); setStackedView(false); setScrubberIdx(0); }}
          />
          <HostTab
            label="Split"
            title="Show each host in its own lane for direct comparison"
            active={selectedHost === null && stackedView}
            onClick={() => { setSelectedHost(null); setStackedView(true); setScrubberIdx(0); }}
          />
          <span style={{ color: '#333', margin: '0 2px' }}>│</span>
          {hosts.map(h => (
            <HostTab
              key={h}
              label={h.length > 18 ? h.slice(0, 8) + '...' + h.slice(-8) : h}
              title={h}
              active={selectedHost === h}
              onClick={() => { setSelectedHost(h); setStackedView(false); setScrubberIdx(0); }}
            />
          ))}
          <span style={{ color: '#555', marginLeft: 4 }}>{hosts.length} hosts</span>
        </div>
      )}

      {/* Summary bar */}
      <div style={{
        padding: '6px 16px',
        background: 'var(--bg-secondary, #111)',
        borderBottom: '1px solid var(--border-accent, #333)',
        display: 'flex',
        gap: 16,
        fontSize: 11,
        fontFamily: 'monospace',
        color: '#888',
        flexShrink: 0,
      }}>
        <span>{duration.toFixed(1)}s</span>
        <span style={{ color: '#636EFA' }}>{fmtMB(peakMem)} peak mem</span>
        <span style={{ color: '#FECB52' }}>{peakCpu.toFixed(1)} peak cores</span>
        <span>{totalRows.toLocaleString()} rows</span>
        <span>{samples.length} samples</span>
        {logEvents.length > 0 && <span>{logEvents.length} log events</span>}
      </div>

      <XRayVisualization
        samples={samples}
        highlightTime={highlightTime}
        highlightLabel={highlightLabel}
        sampleCounts={sampleCounts}
        sampleOffset={sampleOffset}
        onShowFlamegraphForT={handleShowFlamegraphForT}
        stackedView={stackedView}
        selectedHost={selectedHost}
        hostSamples={hostSamples}
        hosts={hosts}
        currentTimeWindow={currentTimeWindow}
        onShowFlamegraph={handleShowFlamegraph}
      />

      {/* Scrubber */}
      <Scrubber
        mode={scrubberMode}
        onModeChange={handleModeChange}
        logEvents={logEvents}
        samples={samples}
        activeIdx={scrubberIdx}
        onChange={setScrubberIdx}
      />

      {/* Time-scoped flamegraph popup */}
      {showFlamegraphPopup && (
        <div style={{
          position: 'fixed',
          top: 0, left: 0, right: 0, bottom: 0,
          zIndex: 9999,
          display: 'flex',
          flexDirection: 'column',
          background: '#000',
        }}>
          <div style={{
            padding: '10px 16px',
            background: '#111',
            borderBottom: '1px solid #333',
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
          }}>
            <span style={{ color: '#ccc', fontSize: 13, fontFamily: 'monospace' }}>
              Flamegraph @ {currentTimeWindow?.label}
            </span>
            <button
              onClick={() => setShowFlamegraphPopup(false)}
              style={{
                padding: '4px 12px', fontSize: 12, borderRadius: 4,
                border: '1px solid #444', background: '#222', color: '#ccc', cursor: 'pointer',
              }}
            >
              Close (Esc)
            </button>
          </div>
          <div style={{ flex: 1, minHeight: 0 }}>
            <SpeedscopeViewer
              folded={timeScopedFlamegraph.folded}
              isLoading={timeScopedFlamegraph.isLoading}
              error={timeScopedFlamegraph.error}
              unavailableReason={timeScopedFlamegraph.unavailableReason}
              onRefresh={() => {
                if (currentTimeWindow) {
                  timeScopedFlamegraph.fetch(queryId, currentTimeWindow.from, currentTimeWindow.to, queryStartTime);
                }
              }}
            />
          </div>
        </div>
      )}
    </div>
  );
};
