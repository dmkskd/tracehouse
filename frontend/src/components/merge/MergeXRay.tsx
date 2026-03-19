/**
 * MergeXRay — 2D timeline visualization of merge progress from tracehouse.merges_history.
 *
 * Shows progress, I/O throughput (read/write rows and MB/s), and memory usage
 * over the lifetime of a merge operation, sampled from system.merges.
 *
 * Experimental feature — gated by experimentalEnabled + tracehouse_merges_history capability.
 */

import React, { useEffect, useState, useCallback, useRef, useMemo } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Area, AreaChart, ReferenceLine } from 'recharts';
import { useMergeSamples } from './useMergeSamples';
import { useClickHouseServices } from '../../providers/ClickHouseProvider';
import { formatElapsed, formatNumberCompact, formatBytes } from '../../utils/formatters';
import type { MergeTextLog, MergeSample } from '@tracehouse/core';

interface MergeXRayProps {
  database: string;
  table: string;
  resultPartName: string;
  /** event_time from part_log — used to correlate text_log timestamps */
  eventTime?: string;
  /** duration_ms from part_log */
  durationMs?: number;
  /** query_id from part_log (often empty for background merges) */
  queryId?: string;
}

const CHART_HEIGHT = 120;
const SCRUBBER_LINE_COLOR = '#FECB52';

const ACCENT = '#f0883e';
const COLORS = {
  progress: '#3fb950',
  rowsRead: '#58a6ff',
  rowsWritten: '#f0883e',
  readMb: '#58a6ff',
  writtenMb: '#f0883e',
  memory: '#d2a8ff',
};

const fmtTime = (v: number) => formatElapsed(v);
const fmtRows = (v: number) => formatNumberCompact(v);
const fmtMb = (v: number) => `${v.toFixed(1)} MB/s`;
const fmtMemMb = (v: number) => formatBytes(v * 1024 * 1024);
const fmtMemAxis = (v: number) => formatNumberCompact(v);
const fmtPct = (v: number) => `${(v * 100).toFixed(0)}%`;
interface TimedLogEntry {
  t: number;
  level: string;
  message: string;
  source: string;
  thread_name: string;
}

const ChartCard: React.FC<{
  title: string;
  children: React.ReactNode;
}> = ({ title, children }) => (
  <div style={{ marginBottom: 6 }}>
    <div style={{ fontSize: 9, fontWeight: 600, color: 'var(--text-muted)', marginBottom: 2, textTransform: 'uppercase', letterSpacing: '0.5px' }}>
      {title}
    </div>
    <div style={{ background: 'var(--bg-tertiary)', borderRadius: 6, padding: '4px 4px 2px 4px' }}>
      {children}
    </div>
  </div>
);

const CustomTooltip: React.FC<{
  active?: boolean;
  payload?: Array<{ name: string; value: number; color: string }>;
  label?: number;
  formatter?: (name: string, value: number) => string;
}> = ({ active, payload, label, formatter }) => {
  if (!active || !payload?.length) return null;
  return (
    <div style={{
      background: 'var(--bg-primary)', border: '1px solid var(--border-primary)',
      borderRadius: 4, padding: '6px 8px', fontSize: 10, lineHeight: 1.6,
    }}>
      <div style={{ color: 'var(--text-muted)', marginBottom: 2 }}>t = {fmtTime(label ?? 0)}</div>
      {payload.map((p, i) => (
        <div key={i} style={{ color: p.color }}>
          {p.name}: {formatter ? formatter(p.name, p.value) : p.value}
        </div>
      ))}
    </div>
  );
};

export const MergeXRay: React.FC<MergeXRayProps> = ({ database, table, resultPartName, eventTime, durationMs, queryId }) => {
  const services = useClickHouseServices();
  const { samples, isLoading, error, fetch } = useMergeSamples({
    database,
    table,
    resultPartName,
  });

  // Text log entries correlated to this merge
  const [textLogs, setTextLogs] = useState<MergeTextLog[]>([]);
  const [isLoadingLogs, setIsLoadingLogs] = useState(false);

  useEffect(() => {
    fetch();
  }, [fetch]);

  // Fetch text logs when samples are loaded
  const fetchLogs = useCallback(async () => {
    if (!services || !eventTime) return;
    setIsLoadingLogs(true);
    try {
      const logs = await services.mergeTracker.getMergeEventTextLogs({
        query_id: queryId,
        event_time: eventTime,
        duration_ms: durationMs ?? 0,
        database,
        table,
        part_name: resultPartName,
      });
      setTextLogs(logs);
    } catch {
      // Non-critical — text_log may not be available
    } finally {
      setIsLoadingLogs(false);
    }
  }, [services, eventTime, durationMs, queryId, database, table, resultPartName]);

  useEffect(() => {
    if (samples.length > 0) fetchLogs();
  }, [samples.length > 0, fetchLogs]); // eslint-disable-line react-hooks/exhaustive-deps

  // Scrubber state
  const [scrubberMode, setScrubberMode] = useState<'time' | 'logs'>('time');
  const [scrubberIdx, setScrubberIdx] = useState(0);

  // Compute timed log entries for scrubber (must be before early returns to preserve hook order)
  const totalElapsedForLogs = samples.length > 0 ? samples[samples.length - 1].elapsed : 0;
  const timedLogs = useMemo(() => {
    if (!eventTime || textLogs.length === 0 || samples.length === 0) return [] as TimedLogEntry[];
    const endMs = new Date(eventTime).getTime();
    const startMs = endMs - totalElapsedForLogs * 1000;
    return textLogs
      .map(log => {
        const logMs = new Date(log.event_time_microseconds?.replace(' ', 'T') || log.event_time).getTime();
        return {
          t: Math.max(0, (logMs - startMs) / 1000),
          level: log.level,
          message: log.message,
          source: log.source,
          thread_name: log.thread_name,
        };
      });
  }, [textLogs, eventTime, totalElapsedForLogs, samples.length]);

  if (isLoading) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: 200, color: 'var(--text-muted)' }}>
        <div style={{ textAlign: 'center' }}>
          <div style={{ fontSize: 12, marginBottom: 4 }}>Loading merge samples...</div>
          <div style={{ fontSize: 10 }}>{database}.{table} / {resultPartName}</div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: 200, color: '#e5534b' }}>
        <div style={{ textAlign: 'center' }}>
          <div style={{ fontSize: 12, marginBottom: 4 }}>Failed to load merge samples</div>
          <div style={{ fontSize: 10 }}>{error}</div>
        </div>
      </div>
    );
  }

  if (samples.length === 0) {
    return (
      <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', height: 200, color: 'var(--text-muted)' }}>
        <div style={{ textAlign: 'center' }}>
          <div style={{ fontSize: 12, marginBottom: 8 }}>No samples found for this merge</div>
          <div style={{ fontSize: 10, color: 'var(--text-muted)', lineHeight: 1.6 }}>
            This requires <code style={{ background: 'var(--bg-tertiary)', padding: '1px 4px', borderRadius: 3 }}>
            tracehouse.merges_history</code> — see{' '}
            <code style={{ background: 'var(--bg-tertiary)', padding: '1px 4px', borderRadius: 3 }}>
            infra/scripts/setup_sampling.sh</code>
          </div>
          <div style={{ fontSize: 10, color: 'var(--text-muted)', marginTop: 8 }}>
            Merge samples are only captured while a merge is in progress.<br />
            Short merges may complete between sampling intervals.
          </div>
          <button
            onClick={fetch}
            style={{
              marginTop: 12, padding: '6px 16px', background: 'var(--bg-tertiary)',
              border: '1px solid var(--border-primary)', borderRadius: 4, color: 'var(--text-secondary)',
              cursor: 'pointer', fontSize: 11,
            }}
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  // Summary stats
  const lastSample = samples[samples.length - 1];
  const maxProgress = Math.max(...samples.map(s => s.progress));
  const maxMemory = Math.max(...samples.map(s => s.memory_usage));
  const totalElapsed = lastSample.elapsed;

  const chartData = samples.map(s => ({
    t: s.t,
    progress: s.progress,
    rows_read: s.rows_read,
    rows_written: s.rows_written,
    d_rows_read: s.d_rows_read,
    d_rows_written: s.d_rows_written,
    d_read_mb: s.d_read_mb,
    d_written_mb: s.d_written_mb,
    memory_mb: s.memory_usage / (1024 * 1024),
  }));

  const hasReadActivity = samples.some(s => s.d_rows_read > 0 || s.d_read_mb > 0);
  const hasWriteActivity = samples.some(s => s.d_rows_written > 0 || s.d_written_mb > 0);
  const hasMemory = samples.some(s => s.memory_usage > 0);

  // Scrubber highlight time
  const scrubberItems = scrubberMode === 'logs' ? timedLogs : samples;
  const clampedIdx = Math.max(0, Math.min(scrubberItems.length - 1, scrubberIdx));
  const highlightTime = scrubberItems.length > 0
    ? (scrubberMode === 'logs' ? timedLogs[clampedIdx]?.t : samples[clampedIdx]?.t) ?? null
    : null;

  return (
    <div style={{ padding: 0 }}>
      {/* Summary cards */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 6, marginBottom: 8 }}>
        {[
          { label: 'Samples', value: `${samples.length}` },
          { label: 'Duration', value: `${totalElapsed.toFixed(1)}s` },
          { label: 'Max Progress', value: fmtPct(maxProgress) },
          { label: 'Peak Memory', value: maxMemory > 0 ? `${(maxMemory / (1024 * 1024)).toFixed(1)} MB` : '-' },
        ].map(s => (
          <div key={s.label} style={{ borderRadius: 6, padding: '4px 8px', background: 'var(--bg-tertiary)' }}>
            <div style={{ fontSize: 9, color: 'var(--text-muted)' }}>{s.label}</div>
            <div style={{ fontWeight: 600, color: 'var(--text-primary)', fontFamily: 'monospace', fontSize: 11 }}>{s.value}</div>
          </div>
        ))}
      </div>

      {/* Progress chart */}
      <ChartCard title="Progress">
        <ResponsiveContainer width="100%" height={CHART_HEIGHT}>
          <AreaChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" stroke="var(--border-primary)" />
            <XAxis dataKey="t" type="number" domain={[(d: number) => Math.max(0, d - 1), (d: number) => d + 1]} tickFormatter={fmtTime} tick={{ fontSize: 9, fill: 'var(--text-muted)' }} />
            <YAxis tickFormatter={fmtPct} tick={{ fontSize: 9, fill: 'var(--text-muted)' }} domain={[0, 1]} />
            <Tooltip content={<CustomTooltip formatter={(_, v) => fmtPct(v)} />} />
            <Area type="monotone" dataKey="progress" stroke={COLORS.progress} fill={COLORS.progress} fillOpacity={0.15} strokeWidth={1.5} dot={false} name="Progress" />
            {highlightTime != null && <ReferenceLine x={highlightTime} stroke={SCRUBBER_LINE_COLOR} strokeWidth={1.5} strokeDasharray="4 3" />}
          </AreaChart>
        </ResponsiveContainer>
      </ChartCard>

      {/* Row throughput chart */}
      {(hasReadActivity || hasWriteActivity) && (
        <ChartCard title="Row Throughput (rows/s)">
          <ResponsiveContainer width="100%" height={CHART_HEIGHT}>
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border-primary)" />
              <XAxis dataKey="t" type="number" domain={[(d: number) => Math.max(0, d - 1), (d: number) => d + 1]} tickFormatter={fmtTime} tick={{ fontSize: 9, fill: 'var(--text-muted)' }} />
              <YAxis tickFormatter={fmtRows} tick={{ fontSize: 9, fill: 'var(--text-muted)' }} />
              <Tooltip content={<CustomTooltip formatter={(_, v) => `${fmtRows(v)} rows/s`} />} />
              {hasReadActivity && <Line type="monotone" dataKey="d_rows_read" stroke={COLORS.rowsRead} strokeWidth={1.5} dot={false} name="Read" />}
              {hasWriteActivity && <Line type="monotone" dataKey="d_rows_written" stroke={COLORS.rowsWritten} strokeWidth={1.5} dot={false} name="Written" />}
              {highlightTime != null && <ReferenceLine x={highlightTime} stroke={SCRUBBER_LINE_COLOR} strokeWidth={1.5} strokeDasharray="4 3" />}
            </LineChart>
          </ResponsiveContainer>
        </ChartCard>
      )}

      {/* MB/s throughput chart */}
      {(hasReadActivity || hasWriteActivity) && (
        <ChartCard title="Throughput (MB/s)">
          <ResponsiveContainer width="100%" height={CHART_HEIGHT}>
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border-primary)" />
              <XAxis dataKey="t" type="number" domain={[(d: number) => Math.max(0, d - 1), (d: number) => d + 1]} tickFormatter={fmtTime} tick={{ fontSize: 9, fill: 'var(--text-muted)' }} />
              <YAxis tickFormatter={v => fmtMb(v).replace(' MB/s', '')} tick={{ fontSize: 9, fill: 'var(--text-muted)' }} />
              <Tooltip content={<CustomTooltip formatter={(_, v) => fmtMb(v)} />} />
              {hasReadActivity && <Line type="monotone" dataKey="d_read_mb" stroke={COLORS.readMb} strokeWidth={1.5} dot={false} name="Read MB/s" />}
              {hasWriteActivity && <Line type="monotone" dataKey="d_written_mb" stroke={COLORS.writtenMb} strokeWidth={1.5} dot={false} name="Written MB/s" strokeDasharray="4 2" />}
              {highlightTime != null && <ReferenceLine x={highlightTime} stroke={SCRUBBER_LINE_COLOR} strokeWidth={1.5} strokeDasharray="4 3" />}
            </LineChart>
          </ResponsiveContainer>
        </ChartCard>
      )}

      {/* Memory chart */}
      {hasMemory && (
        <ChartCard title="Memory Usage">
          <ResponsiveContainer width="100%" height={CHART_HEIGHT}>
            <AreaChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border-primary)" />
              <XAxis dataKey="t" type="number" domain={[(d: number) => Math.max(0, d - 1), (d: number) => d + 1]} tickFormatter={fmtTime} tick={{ fontSize: 9, fill: 'var(--text-muted)' }} />
              <YAxis tickFormatter={fmtMemAxis} tick={{ fontSize: 9, fill: 'var(--text-muted)' }} />
              <Tooltip content={<CustomTooltip formatter={(_, v) => fmtMemMb(v)} />} />
              <Area type="monotone" dataKey="memory_mb" stroke={COLORS.memory} fill={COLORS.memory} fillOpacity={0.15} strokeWidth={1.5} dot={false} name="Memory" />
              {highlightTime != null && <ReferenceLine x={highlightTime} stroke={SCRUBBER_LINE_COLOR} strokeWidth={1.5} strokeDasharray="4 3" />}
            </AreaChart>
          </ResponsiveContainer>
        </ChartCard>
      )}

      {/* Merge info footer */}
      <div style={{ marginTop: 8, fontSize: 9, color: 'var(--text-muted)', display: 'flex', gap: 12, flexWrap: 'wrap' }}>
        {lastSample.merge_type && <span>Type: {lastSample.merge_type}</span>}
        {lastSample.merge_algorithm && <span>Algorithm: {lastSample.merge_algorithm}</span>}
        <span>Parts: {lastSample.num_parts}</span>
        {lastSample.is_mutation && <span style={{ color: ACCENT }}>Mutation</span>}
        <span>Partition: {lastSample.partition_id}</span>
      </div>

      {/* Scrubber */}
      <MergeScrubber
        mode={scrubberMode}
        onModeChange={m => { setScrubberMode(m); setScrubberIdx(0); }}
        samples={samples}
        logEvents={timedLogs}
        activeIdx={scrubberIdx}
        onChange={setScrubberIdx}
        isLoadingLogs={isLoadingLogs}
      />

    </div>
  );
};

/* ------------------------------------------------------------------ */
/*  Scrubber — slider + detail row, matching Query X-Ray pattern       */
/* ------------------------------------------------------------------ */

const LOG_LEVEL_COLORS: Record<string, string> = {
  Trace: '#4b5563', Debug: '#6b7280', Information: '#58a6ff',
  Warning: '#d29922', Error: '#e5534b', Fatal: '#e5534b',
};

type ScrubberMode = 'time' | 'logs';

const MODE_META: Record<ScrubberMode, { label: string; color: string }> = {
  time: { label: 'Samples', color: '#636EFA' },
  logs: { label: 'Logs', color: '#FECB52' },
};

const SCRUBBER_SLIDER_ID = 'merge-xray-scrubber-styles';
function ensureScrubberStyles() {
  if (document.getElementById(SCRUBBER_SLIDER_ID)) return;
  const el = document.createElement('style');
  el.id = SCRUBBER_SLIDER_ID;
  el.textContent = `
    .merge-scrubber-slider {
      -webkit-appearance: none; appearance: none;
      width: 100%; height: 6px; border-radius: 3px;
      outline: none; cursor: pointer; margin: 0;
    }
    .merge-scrubber-slider::-webkit-slider-thumb {
      -webkit-appearance: none; appearance: none;
      width: 14px; height: 14px; border-radius: 50%;
      background: var(--xray-accent, #636EFA); border: 2px solid rgba(0,0,0,0.4);
      box-shadow: 0 0 8px var(--xray-accent-glow, rgba(99,110,250,0.4));
      cursor: pointer; transition: transform 0.1s ease;
    }
    .merge-scrubber-slider::-webkit-slider-thumb:hover { transform: scale(1.2); }
    .merge-scrubber-slider::-moz-range-thumb {
      width: 14px; height: 14px; border-radius: 50%;
      background: var(--xray-accent, #636EFA); border: 2px solid rgba(0,0,0,0.4);
      box-shadow: 0 0 8px var(--xray-accent-glow, rgba(99,110,250,0.4));
      cursor: pointer;
    }
    .merge-scrubber-slider::-moz-range-track {
      height: 6px; border-radius: 3px;
      background: linear-gradient(90deg, rgba(255,255,255,0.06) 0%, rgba(255,255,255,0.1) 100%);
    }
  `;
  document.head.appendChild(el);
}

const pillStyle = (active: boolean, color: string): React.CSSProperties => ({
  display: 'inline-flex', alignItems: 'center', gap: 3,
  padding: '2px 8px', fontSize: 10, fontWeight: 600, borderRadius: 10,
  cursor: 'pointer', userSelect: 'none', transition: 'all 0.15s ease',
  background: active ? `${color}22` : 'transparent',
  color: active ? color : 'var(--text-muted)',
  border: `1px solid ${active ? `${color}44` : 'transparent'}`,
});

const navBtnStyle: React.CSSProperties = {
  padding: '2px 8px', fontSize: 12, fontWeight: 700, borderRadius: 4,
  background: 'rgba(255,255,255,0.05)', color: 'var(--text-muted)',
  border: '1px solid rgba(255,255,255,0.08)', cursor: 'pointer',
};

const MergeScrubber: React.FC<{
  mode: ScrubberMode;
  onModeChange: (m: ScrubberMode) => void;
  samples: MergeSample[];
  logEvents: TimedLogEntry[];
  activeIdx: number;
  onChange: (idx: number) => void;
  isLoadingLogs: boolean;
}> = ({ mode, onModeChange, samples, logEvents, activeIdx, onChange, isLoadingLogs }) => {
  const items = mode === 'logs' ? logEvents : samples;
  const count = items.length;
  const sliderRef = useRef<HTMLInputElement>(null);

  useEffect(() => { ensureScrubberStyles(); }, []);

  // Keyboard navigation
  useEffect(() => {
    const maxIdx = items.length - 1;
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'ArrowLeft') { onChange(Math.max(0, activeIdx - 1)); e.preventDefault(); }
      else if (e.key === 'ArrowRight') { onChange(Math.min(maxIdx, activeIdx + 1)); e.preventDefault(); }
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [items.length, activeIdx, onChange]);

  if (count === 0 && !isLoadingLogs) {
    // Show logs mode pill even when empty so user knows logs exist
    if (mode === 'logs') {
      return (
        <div style={{ marginTop: 12, padding: '8px 14px 10px', background: 'var(--bg-tertiary)', borderRadius: 6, border: '1px solid var(--border-primary)' }}>
          <div style={{ display: 'flex', gap: 5, marginBottom: 6 }}>
            {(Object.keys(MODE_META) as ScrubberMode[]).map(m => (
              <span key={m} style={pillStyle(mode === m, MODE_META[m].color)} onClick={() => onModeChange(m)}>
                {MODE_META[m].label}
                <span style={{ opacity: 0.5, marginLeft: 3, fontSize: 9 }}>
                  {m === 'logs' ? logEvents.length : samples.length}
                </span>
              </span>
            ))}
          </div>
          <div style={{ fontSize: 10, color: 'var(--text-muted)', textAlign: 'center', padding: 8 }}>
            No text_log entries found. Requires system.text_log to be enabled.
          </div>
        </div>
      );
    }
    return null;
  }

  if (count === 0 && isLoadingLogs) {
    return (
      <div style={{ marginTop: 12, padding: '8px 14px', background: 'var(--bg-tertiary)', borderRadius: 6, border: '1px solid var(--border-primary)', fontSize: 10, color: 'var(--text-muted)', textAlign: 'center' }}>
        Loading text logs...
      </div>
    );
  }

  const clampedIdx = Math.max(0, Math.min(count - 1, activeIdx));
  const progressPct = count > 1 ? (clampedIdx / (count - 1)) * 100 : 0;
  const currentT = mode === 'logs'
    ? (logEvents[clampedIdx]?.t ?? 0)
    : (samples[clampedIdx]?.t ?? 0);

  const accentColor = MODE_META[mode].color;
  const accentGlow = accentColor + '66';
  const trackBg = `linear-gradient(90deg, ${accentColor}44 0%, ${accentColor}88 ${progressPct}%, rgba(255,255,255,0.08) ${progressPct}%)`;

  // Detail content
  const detailContent = (() => {
    if (mode === 'logs') {
      const evt = logEvents[clampedIdx];
      if (!evt) return null;
      return (
        <div style={{ display: 'flex', gap: 8, alignItems: 'baseline', minWidth: 0, overflow: 'hidden' }}>
          <span style={{ color: accentColor, flexShrink: 0 }}>{evt.t.toFixed(1)}s</span>
          <span style={{ color: LOG_LEVEL_COLORS[evt.level] || '#888', flexShrink: 0, fontSize: 10, textTransform: 'uppercase' }}>
            {evt.level}
          </span>
          <span style={{ color: 'var(--text-muted)', flexShrink: 0 }}>
            {evt.source.split('(')[0].trim()}
          </span>
          <span style={{ color: 'var(--text-secondary)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
            {evt.message}
          </span>
        </div>
      );
    }
    const s = samples[clampedIdx];
    if (!s) return null;
    return (
      <div style={{ display: 'flex', gap: 12, alignItems: 'baseline', minWidth: 0, overflow: 'hidden' }}>
        <span style={{ color: accentColor, flexShrink: 0 }}>{s.t.toFixed(0)}s</span>
        <span style={{ color: COLORS.progress }}>Progress {fmtPct(s.progress)}</span>
        <span style={{ color: COLORS.memory }}>Mem {fmtMemMb(s.memory_usage / (1024 * 1024))}</span>
        <span style={{ color: COLORS.rowsRead }}>Read {fmtRows(s.rows_read)}</span>
        <span style={{ color: COLORS.rowsWritten }}>Written {fmtRows(s.rows_written)}</span>
        {s.d_rows_read > 0 && <span style={{ color: COLORS.readMb }}>r/s {fmtRows(s.d_rows_read)}</span>}
      </div>
    );
  })();

  return (
    <div style={{
      marginTop: 12, padding: '8px 14px 10px',
      background: 'var(--bg-tertiary)', borderRadius: 6,
      border: '1px solid var(--border-primary)',
      fontFamily: 'monospace', fontSize: 11,
    }}>
      {/* Top row: mode pills + time + nav */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
        <div style={{ display: 'flex', gap: 5, flexShrink: 0 }}>
          {(Object.keys(MODE_META) as ScrubberMode[]).map(m => (
            <span key={m} style={pillStyle(mode === m, MODE_META[m].color)} onClick={() => onModeChange(m)}>
              {MODE_META[m].label}
              <span style={{ opacity: 0.5, marginLeft: 3, fontSize: 9 }}>
                {m === 'logs' ? (isLoadingLogs ? '...' : logEvents.length) : samples.length}
              </span>
            </span>
          ))}
        </div>
        <span style={{ color: accentColor, fontSize: 13, fontWeight: 700, flexShrink: 0, minWidth: 48, textAlign: 'right' }}>
          {currentT.toFixed(mode === 'logs' ? 1 : 0)}s
        </span>
        <span style={{ flex: 1 }} />
        <div style={{ display: 'flex', gap: 4, flexShrink: 0 }}>
          <button onClick={() => onChange(Math.max(0, activeIdx - 1))} style={navBtnStyle}>‹</button>
          <button onClick={() => onChange(Math.min(count - 1, activeIdx + 1))} style={navBtnStyle}>›</button>
        </div>
      </div>

      {/* Slider */}
      <div style={{
        position: 'relative', marginBottom: 6, paddingTop: 2,
        ['--xray-accent' as string]: accentColor,
        ['--xray-accent-glow' as string]: accentGlow,
      }}>
        <input
          ref={sliderRef}
          type="range"
          className="merge-scrubber-slider"
          min={0}
          max={count - 1}
          value={clampedIdx}
          onChange={e => onChange(parseInt(e.target.value))}
          style={{ width: '100%', background: trackBg }}
        />
      </div>

      {/* Detail row */}
      <div style={{ lineHeight: 1.5, minHeight: 18 }}>
        {detailContent}
      </div>
    </div>
  );
};

