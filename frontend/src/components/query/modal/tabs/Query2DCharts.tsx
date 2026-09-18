/**
 * Query2DCharts — flat 2D timeline view of per-second process samples.
 *
 * An alternative to the 3D corridor in XRayVisualization: stacked recharts
 * cards (CPU cores, Memory, read_bytes/s, I/O wait) over the query's lifetime,
 * driven by the same ProcessSample[] the 3D view already loads. Easier to read
 * for exact values; mirrors the 2D style of MergeXRay and the History/Compare
 * timeline.
 *
 * Chrome (surface, cards, grid, axes, tooltip) honors the active light/dark
 * theme via CSS variables, matching MergeXRay. Only the metric line colors are
 * fixed hues (shared with the 3D corridor) since they read on both themes.
 *
 * The scrubber highlight time is drawn as a vertical ReferenceLine on every
 * chart, matching MergeXRay.
 */

import React, { useMemo } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Area, AreaChart, ReferenceLine } from 'recharts';
import type { ProcessSample } from '../hooks/useProcessSamples';
import { useXRaySourceMeta } from '../XRaySourceContext';
import { peakSustainedCores } from '@tracehouse/core';
import { formatElapsed, formatBytes } from '../../../../utils/formatters';

const CHART_HEIGHT = 120;

/* ── Theme-aware chrome (light/dark via CSS variables) ────────────────── */
const SURFACE_BG = 'var(--bg-primary)';
const CARD_BG = 'var(--bg-tertiary)';
const CARD_BORDER = 'var(--border-primary)';
const GRID_STROKE = 'var(--border-primary)';
const AXIS_COLOR = 'var(--text-muted)';
const TITLE_COLOR = 'var(--text-muted)';
const TOOLTIP_BG = 'var(--bg-primary)';
const TOOLTIP_BORDER = 'var(--border-primary)';
const TOOLTIP_LABEL = 'var(--text-muted)';

/* Metric line colors — identical to the 3D corridor edges/traces */
const COLORS = {
  cpu: '#FECB52',      // matches 3D CPU edge
  memory: '#636EFA',   // matches 3D memory edge
  read: '#00DD99',     // matches 3D read trace
  ioWait: '#7B83FF',   // matches 3D I/O wait trace
  cpuWait: '#B682FF',  // run-queue contention
  netWait: '#FF6692',  // socket blocking
  net: '#33DDFF',      // matches 3D network trace
};

const SCRUBBER_LINE_COLOR = '#FECB52';
/** Shared by the clamp dots and the clamp badge so they read as one signal. */
const CLAMP_COLOR = '#FFA15A';

const fmtTime = (v: number) => formatElapsed(v);
const fmtCores = (v: number) => v.toFixed(2);
const fmtMemAxis = (v: number) => (v >= 1024 ? `${(v / 1024).toFixed(1)}G` : `${v.toFixed(0)}M`);
const fmtMemFull = (v: number) => formatBytes(v * 1024 * 1024);
const fmtMbs = (v: number) => `${v.toFixed(1)} MB/s`;

const ChartCard: React.FC<{
  title: string;
  /** Optional caveat shown next to the title, e.g. a clamp warning. */
  badge?: React.ReactNode;
  children: React.ReactNode;
}> = ({ title, badge, children }) => (
  <div style={{ marginBottom: 8 }}>
    <div style={{ fontSize: 9, fontWeight: 600, color: TITLE_COLOR, marginBottom: 3, textTransform: 'uppercase', letterSpacing: '0.5px', display: 'flex', alignItems: 'center', gap: 6 }}>
      {title}
      {badge}
    </div>
    <div style={{ background: CARD_BG, border: `1px solid ${CARD_BORDER}`, borderRadius: 6, padding: '4px 4px 2px 4px' }}>
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
      background: TOOLTIP_BG, border: `1px solid ${TOOLTIP_BORDER}`,
      borderRadius: 4, padding: '6px 8px', fontSize: 10, lineHeight: 1.6,
      fontFamily: 'monospace',
    }}>
      <div style={{ color: TOOLTIP_LABEL, marginBottom: 2 }}>t = {fmtTime(label ?? 0)}</div>
      {payload.map((p, i) => (
        <div key={i} style={{ color: p.color }}>
          {p.name}: {formatter ? formatter(p.name, p.value) : p.value}
        </div>
      ))}
    </div>
  );
};

const axisTick = { fontSize: 9, fill: AXIS_COLOR };
const xAxisProps = {
  dataKey: 't',
  type: 'number' as const,
  domain: [(d: number) => Math.max(0, d - 1), (d: number) => d + 1] as [(v: number) => number, (v: number) => number],
  tickFormatter: fmtTime,
  tick: axisTick,
  stroke: GRID_STROKE,
};

/**
 * Dot renderer that marks only the samples whose thread-time rate was capped.
 * Recharts calls this for every point, so it returns null for the rest.
 */
const ClampDot: React.FC<{
  cx?: number;
  cy?: number;
  payload?: { clamped?: boolean };
}> = ({ cx, cy, payload }) => {
  if (!payload?.clamped || cx == null || cy == null) return null;
  return <circle cx={cx} cy={cy} r={2.5} fill={CLAMP_COLOR} stroke={CARD_BG} strokeWidth={1} />;
};

/**
 * Caveat shown on the CPU card. Two different situations, and conflating them
 * would mislead: 'clamped' means we capped a spike at the query's peak
 * concurrency, 'unclamped' means no ceiling was available at all, so a spike
 * may be an artefact rather than real work.
 */
const ClampBadge: React.FC<{ anyUnclamped: boolean; anyClamped: boolean }> = ({ anyUnclamped, anyClamped }) => {
  if (anyUnclamped) {
    return (
      <span
        style={{ color: CLAMP_COLOR, textTransform: 'none', fontWeight: 500 }}
        title="No thread ceiling is available for this query yet: peak_threads_usage is written to query_log when the query finishes. A pooled thread detaching can merge its whole accumulated time into one sample, so spikes here may exceed what the query's threads could actually produce."
      >
        ⚠ unclamped
      </span>
    );
  }
  if (!anyClamped) return null;
  return (
    <span
      style={{ color: CLAMP_COLOR, textTransform: 'none', fontWeight: 500 }}
      title="Marked samples were capped at the query's peak concurrent threads. A thread detaching merges its whole accumulated time into one interval, producing a rate no thread count could reach. The plotted value is a floor, not the raw counter delta."
    >
      ⚠ clamped samples marked
    </span>
  );
};

export const Query2DCharts: React.FC<{
  samples: ProcessSample[];
  highlightTime: number | null;
}> = ({ samples, highlightTime }) => {
  const { missing } = useXRaySourceMeta();
  const chartData = useMemo(() => samples.map(s => ({
    t: s.t,
    clamped: s.rate_clamped,
    cpu_cores: s.d_cpu_cores,
    memory_mb: s.memory_mb,
    read_mb: s.d_read_mb,
    io_wait_s: s.d_io_wait_s,
    cpu_wait_s: s.d_cpu_wait_s,
    net_wait_s: s.d_net_recv_wait_s,
    net_kb: s.d_net_send_kb + s.d_net_recv_kb,
  })), [samples]);

  const hasIoWait = samples.some(s =>
    s.d_io_wait_s > 0 || s.d_cpu_wait_s > 0 || s.d_net_recv_wait_s > 0);
  // Hidden rather than approximated when the active source has no real progress
  // counters: a plausible-looking curve built from a different quantity is worse
  // than no curve.
  const hasRead = !missing.includes('d_read_mb') && samples.some(s => s.d_read_mb > 0);
  const anyClamped = samples.some(s => s.rate_clamped);
  // Observed from the rows, not inferred from the source: only the query knows
  // whether a thread ceiling was actually found.
  const anyUnclamped = samples.some(s => s.rate_unclamped);
  const sustainedCpuMax = Math.max(peakSustainedCores(samples).value, 0.001);
  const hasNet = samples.some(s => s.d_net_send_kb > 0 || s.d_net_recv_kb > 0);

  const scrubberLine = highlightTime != null
    ? <ReferenceLine x={highlightTime} stroke={SCRUBBER_LINE_COLOR} strokeWidth={1.5} strokeDasharray="4 3" />
    : null;

  return (
    <div style={{ flex: 1, minHeight: 0, overflow: 'auto', padding: '12px 16px', background: SURFACE_BG }}>
      {/* CPU cores */}
      <ChartCard
        title="CPU Cores (cores)"
        badge={<ClampBadge anyUnclamped={anyUnclamped} anyClamped={anyClamped} />}
      >
        <ResponsiveContainer width="100%" height={CHART_HEIGHT}>
          <AreaChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" stroke={GRID_STROKE} />
            <XAxis {...xAxisProps} />
            {/* Same ceiling as the 3D cage and the summary-bar headline:
                scaled to the sustained peak, with capped intervals clipping at
                the top and marked by ClampDot rather than setting the scale. */}
            <YAxis
              tickFormatter={fmtCores}
              tick={axisTick}
              stroke={GRID_STROKE}
              domain={[0, sustainedCpuMax]}
              allowDataOverflow
            />
            <Tooltip content={<CustomTooltip formatter={(_, v) => `${fmtCores(v)} cores`} />} cursor={{ stroke: GRID_STROKE }} />
            <Area type="monotone" dataKey="cpu_cores" stroke={COLORS.cpu} fill={COLORS.cpu} fillOpacity={0.15} strokeWidth={1.5} dot={<ClampDot />} activeDot={false} name="CPU" isAnimationActive={false} />
            {scrubberLine}
          </AreaChart>
        </ResponsiveContainer>
      </ChartCard>

      {/* Memory */}
      <ChartCard title="Memory">
        <ResponsiveContainer width="100%" height={CHART_HEIGHT}>
          <AreaChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" stroke={GRID_STROKE} />
            <XAxis {...xAxisProps} />
            <YAxis tickFormatter={fmtMemAxis} tick={axisTick} stroke={GRID_STROKE} />
            <Tooltip content={<CustomTooltip formatter={(_, v) => fmtMemFull(v)} />} cursor={{ stroke: GRID_STROKE }} />
            <Area type="monotone" dataKey="memory_mb" stroke={COLORS.memory} fill={COLORS.memory} fillOpacity={0.15} strokeWidth={1.5} dot={false} name="Memory" isAnimationActive={false} />
            {scrubberLine}
          </AreaChart>
        </ResponsiveContainer>
      </ChartCard>

      {/* read_bytes throughput */}
      {hasRead && (
        <ChartCard title="read_bytes (MB/s)">
          <ResponsiveContainer width="100%" height={CHART_HEIGHT}>
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke={GRID_STROKE} />
              <XAxis {...xAxisProps} />
              <YAxis tickFormatter={v => v.toFixed(0)} tick={axisTick} stroke={GRID_STROKE} />
              <Tooltip content={<CustomTooltip formatter={(_, v) => fmtMbs(v)} />} cursor={{ stroke: GRID_STROKE }} />
              <Line type="monotone" dataKey="read_mb" stroke={COLORS.read} strokeWidth={1.5} dot={false} name="Read" isAnimationActive={false} />
              {scrubberLine}
            </LineChart>
          </ResponsiveContainer>
        </ChartCard>
      )}

      {/* Wait breakdown — same unit as CPU cores above (threads-worth of the
          sampling window), so these read directly against the CPU chart. */}
      {hasIoWait && (
        <ChartCard title="Wait (threads)">
          <ResponsiveContainer width="100%" height={CHART_HEIGHT}>
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke={GRID_STROKE} />
              <XAxis {...xAxisProps} />
              <YAxis tickFormatter={v => v.toFixed(2)} tick={axisTick} stroke={GRID_STROKE} />
              <Tooltip content={<CustomTooltip formatter={(_, v) => v.toFixed(2)} />} cursor={{ stroke: GRID_STROKE }} />
              <Line type="monotone" dataKey="io_wait_s" stroke={COLORS.ioWait} strokeWidth={1.5} dot={false} name="Disk" isAnimationActive={false} />
              <Line type="monotone" dataKey="cpu_wait_s" stroke={COLORS.cpuWait} strokeWidth={1.5} dot={false} name="CPU queue" isAnimationActive={false} />
              <Line type="monotone" dataKey="net_wait_s" stroke={COLORS.netWait} strokeWidth={1.5} dot={false} name="Network" isAnimationActive={false} />
              {scrubberLine}
            </LineChart>
          </ResponsiveContainer>
        </ChartCard>
      )}

      {/* Network */}
      {hasNet && (
        <ChartCard title="Network (KB/s)">
          <ResponsiveContainer width="100%" height={CHART_HEIGHT}>
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke={GRID_STROKE} />
              <XAxis {...xAxisProps} />
              <YAxis tickFormatter={v => v.toFixed(0)} tick={axisTick} stroke={GRID_STROKE} />
              <Tooltip content={<CustomTooltip formatter={(_, v) => `${v.toFixed(1)} KB/s`} />} cursor={{ stroke: GRID_STROKE }} />
              <Line type="monotone" dataKey="net_kb" stroke={COLORS.net} strokeWidth={1.5} dot={false} name="Net" isAnimationActive={false} />
              {scrubberLine}
            </LineChart>
          </ResponsiveContainer>
        </ChartCard>
      )}
    </div>
  );
};
