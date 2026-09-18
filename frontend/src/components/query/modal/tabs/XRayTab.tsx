/**
 * XRayTab — 3D resource corridor with log event scrubber.
 *
 * Renders a 3D "corridor" where:
 *   X = time (seconds)
 *   Y = CPU cores used (width of corridor)
 *   Z = Memory MB (height of corridor)
 *
 * Inner traces show read throughput plus the three metered waits — disk, CPU
 * queue, and network — each drawn only when it actually fired.
 * A slider scrubs through text_log events, highlighting each event's
 * time position with a vertical marker in the 3D scene.
 */

import React, { useMemo, useState, useEffect, useRef, useCallback } from 'react';
import type { TraceLog, XRayQueryState } from '@tracehouse/core';
import { peakSustainedCores } from '@tracehouse/core';
import { useProcessSamples } from '../hooks/useProcessSamples';
import { XRaySourceBadge } from '../XRaySourceBadge';
import { XRaySourceProvider } from '../XRaySourceContext';
import type { ProcessSample } from '../hooks/useProcessSamples';
import { useTraceSampleCounts, hasTraceSamplesInRange, useTimeScopedFlamegraph } from '../hooks/useHotFunctions';
import { SpeedscopeViewer } from '../../../tracing/SpeedscopeViewer';
import { XRayVisualization } from './XRayVisualization';
import { Query2DCharts } from './Query2DCharts';

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

/**
 * One segment of a toggle, styled like the modal's tab bar rather than as a
 * standalone pill, so every switch in the X-Ray reads as the same control.
 */
const segmentStyle = (active: boolean, first: boolean): React.CSSProperties => ({
  padding: '3px 10px',
  border: 'none',
  borderLeft: first ? 'none' : '1px solid var(--border-primary)',
  fontFamily: 'monospace',
  fontSize: 10,
  letterSpacing: '1px',
  textTransform: 'uppercase',
  cursor: 'pointer',
  whiteSpace: 'nowrap',
  background: active ? 'rgba(88, 166, 255, 0.12)' : 'transparent',
  color: active ? '#58a6ff' : 'var(--text-muted)',
  transition: 'all 0.2s ease',
});

const segmentGroupStyle: React.CSSProperties = {
  display: 'flex',
  flexShrink: 0,
  border: '1px solid var(--border-primary)',
  borderRadius: 5,
  overflow: 'hidden',
};

const navBtnStyle: React.CSSProperties = {
  background: 'var(--bg-tertiary)',
  border: '1px solid var(--border-primary)',
  color: 'var(--text-secondary)',
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
  /** Fields the active X-Ray source cannot populate; their readouts are hidden. */
  missingFields: readonly string[];
}> = ({ mode, onModeChange, logEvents, samples, activeIdx, onChange, missingFields }) => {
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
        <span style={{ color: '#7B83FF' }}>IO {s.d_io_wait_s.toFixed(2)}</span>
        <span style={{ color: '#FF6692' }}>Net {s.d_net_recv_wait_s.toFixed(2)}</span>
        {!missingFields.includes('d_read_mb') && (
          <span style={{ color: '#00DD99' }}>read_bytes {s.d_read_mb.toFixed(0)} MB/s</span>
        )}
        {!missingFields.includes('thread_count') && (
          <span style={{ color: '#aaa' }} title="Threads joined so far, cumulative — not concurrency">{s.thread_count} thr used</span>
        )}
      </div>
    );
  })();

  const accentColor = MODE_META[mode].color;
  const accentGlow = accentColor + '66';

  // Build track background with filled portion
  const trackBg = `linear-gradient(90deg, ${accentColor}44 0%, ${accentColor}88 ${progressPct}%, var(--border-primary) ${progressPct}%)`;

  return (
    <div style={{
      padding: '8px 14px 10px',
      background: 'var(--bg-secondary)',
      borderTop: '1px solid var(--border-primary)',
      fontFamily: 'monospace',
      fontSize: 11,
      overflow: 'hidden',
    }}>
      {/* Top row: mode pills + time + nav buttons */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6, minWidth: 0 }}>
        {/* Mode selector pills */}
        <div style={segmentGroupStyle}>
          {(Object.keys(MODE_META) as ScrubberMode[]).map((m, i) => (
            <button
              key={m}
              type="button"
              aria-pressed={mode === m}
              style={segmentStyle(mode === m, i === 0)}
              onClick={() => onModeChange(m)}
            >
              {MODE_META[m].icon} {MODE_META[m].label}
              <span style={{ opacity: 0.5, marginLeft: 4, fontSize: 9 }}>
                {m === 'logs' ? logEvents.length : samples.length}
              </span>
            </button>
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
  /**
   * Whether the query is still executing. Feeds source selection: both sources
   * can serve a live query, but the sampler is preferred for one — it is
   * current to the last tick, and query_metric_log has no thread ceiling for
   * the clamp until peak_threads_usage reaches query_log at finish.
   * Undefined means unknown, treated as possibly live.
   */
  isRunning?: boolean;
}

export const XRayTab: React.FC<XRayTabProps> = ({
  queryId,
  logs,
  queryStartTime,
  isRunning,
}) => {
  const queryState: XRayQueryState = isRunning === undefined
    ? 'unknown'
    : isRunning ? 'running' : 'finished';
  const { meta: sourceMeta, samples: allSamples, hostSamples, hosts, isLoading: isLoadingSamples, error, fetch: fetchSamples } = useProcessSamples(queryId, queryState, queryStartTime);
  const { sampleCounts, fetch: fetchSampleCounts } = useTraceSampleCounts(queryId, queryStartTime, queryStartTime);
  const timeScopedFlamegraph = useTimeScopedFlamegraph();
  const [showFlamegraphPopup, setShowFlamegraphPopup] = useState(false);
  const [selectedHost, setSelectedHost] = useState<string | null>(null);
  const [stackedView, setStackedView] = useState(false);
  const [viewMode, setViewMode] = useState<'3d' | '2d'>('3d');
  const [scrubberMode, setScrubberMode] = useState<ScrubberMode>('time');
  const [scrubberIdx, setScrubberIdx] = useState(0);

  // Active samples: "All" (aggregated) or per-host filtered
  const samples = useMemo(() => {
    if (selectedHost === null) return allSamples;
    return hostSamples.get(selectedHost) || [];
  }, [selectedHost, allSamples, hostSamples]);

  // Reported by the query, not inferred: only the SQL knows whether a thread
  // ceiling was actually found. Derived from the ACTIVE samples so selecting a
  // host does not carry another host's warning onto this host's numbers.
  const anyUnclamped = samples.some(s => s.rate_unclamped);

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
          {/* Name the source that actually failed. Pointing at the sampler
              while reading system.query_metric_log sends people to fix the
              wrong thing. */}
          <div style={{ fontSize: 12, color: '#888', lineHeight: 1.6 }}>
            {sourceMeta.source === 'query_metric_log' ? (
              <>
                Read from <code style={{ background: '#1a1a2e', padding: '1px 4px', borderRadius: 3 }}>
                system.query_metric_log</code>. Pin a different source from the badge above, or install{' '}
                <code style={{ background: '#1a1a2e', padding: '1px 4px', borderRadius: 3 }}>
                tracehouse.processes_history</code> via{' '}
                <code style={{ background: '#1a1a2e', padding: '1px 4px', borderRadius: 3 }}>
                infra/scripts/setup_sampling.sh</code>
              </>
            ) : (
              <>
                This requires <code style={{ background: '#1a1a2e', padding: '1px 4px', borderRadius: 3 }}>
                tracehouse.processes_history</code> — see{' '}
                <code style={{ background: '#1a1a2e', padding: '1px 4px', borderRadius: 3 }}>
                infra/scripts/setup_sampling.sh</code>
              </>
            )}
          </div>
          <div style={{ marginTop: 12, fontSize: 11, fontFamily: 'monospace', display: 'flex', justifyContent: 'center', color: '#888' }}>
            <XRaySourceBadge meta={sourceMeta} />
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
            {sourceMeta.source === 'query_metric_log'
              ? <>Nothing in <code>system.query_metric_log</code> for this query.
                  It may predate the log's retention, or its rows may not have
                  flushed yet.</>
              : <>This query ran too fast for the process sampler to capture data,
                  or the sampler wasn't active when this query executed.</>}
          </div>
          {/* The source picker lives in the summary bar, which this early return
              skips — without it here, the message names a fix the user has no
              way to apply on this screen. */}
          <div style={{ marginTop: 12, fontSize: 11, fontFamily: 'monospace', display: 'flex', justifyContent: 'center', color: '#888' }}>
            <XRaySourceBadge meta={sourceMeta} />
          </div>
        </div>
      </div>
    );
  }

  // Summary stats
  const peakMem = Math.max(...samples.map(s => s.peak_memory_mb));
  // Peak SUSTAINED cores: capped intervals are excluded, since they are thread
  // ceilings hit during teardown rather than measured work. See
  // peakSustainedCores() for why a plain max made the two X-Ray sources
  // disagree on the same query.
  const peakCores = peakSustainedCores(samples);
  const peakCpu = peakCores.value;
  const duration = samples[samples.length - 1].t;
  const totalRows = Math.max(...samples.map(s => s.read_rows));

  const multiHost = hosts.length > 1;

  return (
    <XRaySourceProvider meta={sourceMeta}>
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
        // Without this the row stretches to its tallest child, so any control
        // in the bar (the source picker, the view toggle) pushes the stats out
        // of line instead of sitting with them.
        alignItems: 'center',
        gap: 16,
        fontSize: 11,
        fontFamily: 'monospace',
        color: '#888',
        flexShrink: 0,
      }}>
        <span>{duration.toFixed(1)}s</span>
        <span style={{ color: '#636EFA' }}>{fmtMB(peakMem)} peak mem</span>
        <span style={{ color: '#FECB52' }}>
          {peakCpu.toFixed(1)} peak cores
          {/* The 3D view has no chart-level badge, so the caveat rides on the
              number it applies to. */}
          {peakCores.allCapped && !anyUnclamped && (
            <span
              style={{ color: '#FFA15A', marginLeft: 4 }}
              title="Every interval hit the query's thread ceiling, so this is a cap rather than a measured rate. No sustained peak could be derived."
            >
              ⚠ capped
            </span>
          )}
          {peakCores.excludedCapped && !anyUnclamped && (
            <span
              style={{ color: '#666', marginLeft: 4 }}
              title="Sustained peak. Intervals that hit the query's thread ceiling are excluded — those are teardown spikes, where pooled threads detach and merge their accumulated time into one interval. They are still drawn on the chart and marked there."
            >
              (sustained)
            </span>
          )}
          {anyUnclamped && (
            <span
              style={{ color: '#FFA15A', marginLeft: 4 }}
              title="No thread ceiling was available for this query, so this peak is the raw counter delta. peak_threads_usage reaches system.query_log when a query finishes; without it a detaching pool thread can produce a spike no thread count could sustain."
            >
              ⚠ unclamped
            </span>
          )}
        </span>
        {!sourceMeta.missing.includes('read_rows') && (
          <span>{totalRows.toLocaleString()} rows</span>
        )}
        <span>{samples.length} samples</span>
        <XRaySourceBadge meta={sourceMeta} />
        {logEvents.length > 0 && <span>{logEvents.length} log events</span>}

        {/* 3D / 2D view toggle. Styled like the modal's tab bar - monospace,
            uppercase, theme variables - so it reads as part of the same chrome
            in both light and dark mode. */}
        <span style={{ flex: 1 }} />
        <div style={segmentGroupStyle}>
          {(['3d', '2d'] as const).map((m, i) => (
            <button
              key={m}
              type="button"
              onClick={() => setViewMode(m)}
              aria-pressed={viewMode === m}
              title={m === '3d' ? '3D resource corridor' : '2D stacked charts (exact values)'}
              style={segmentStyle(viewMode === m, i === 0)}
            >
              {m}
            </button>
          ))}
        </div>
      </div>

      {viewMode === '3d' ? (
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
      ) : (
        <Query2DCharts samples={samples} highlightTime={highlightTime} />
      )}

      {/* Scrubber */}
      <Scrubber
        mode={scrubberMode}
        onModeChange={handleModeChange}
        logEvents={logEvents}
        samples={samples}
        activeIdx={scrubberIdx}
        onChange={setScrubberIdx}
        missingFields={sourceMeta.missing}
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
    </XRaySourceProvider>
  );
};
