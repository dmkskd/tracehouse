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
  net: '#33DDFF',      // matches 3D network trace
};

const SCRUBBER_LINE_COLOR = '#FECB52';

const fmtTime = (v: number) => formatElapsed(v);
const fmtCores = (v: number) => v.toFixed(2);
const fmtMemAxis = (v: number) => (v >= 1024 ? `${(v / 1024).toFixed(1)}G` : `${v.toFixed(0)}M`);
const fmtMemFull = (v: number) => formatBytes(v * 1024 * 1024);
const fmtMbs = (v: number) => `${v.toFixed(1)} MB/s`;
const fmtSec = (v: number) => `${v.toFixed(2)}s`;

const ChartCard: React.FC<{ title: string; children: React.ReactNode }> = ({ title, children }) => (
  <div style={{ marginBottom: 8 }}>
    <div style={{ fontSize: 9, fontWeight: 600, color: TITLE_COLOR, marginBottom: 3, textTransform: 'uppercase', letterSpacing: '0.5px' }}>
      {title}
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
  domain: [(d: number) => Math.max(0, d - 1), (d: number) => d + 1] as [unknown, unknown],
  tickFormatter: fmtTime,
  tick: axisTick,
  stroke: GRID_STROKE,
};

export const Query2DCharts: React.FC<{
  samples: ProcessSample[];
  highlightTime: number | null;
}> = ({ samples, highlightTime }) => {
  const chartData = useMemo(() => samples.map(s => ({
    t: s.t,
    cpu_cores: s.d_cpu_cores,
    memory_mb: s.memory_mb,
    read_mb: s.d_read_mb,
    io_wait_s: s.d_io_wait_s,
    net_kb: s.d_net_send_kb + s.d_net_recv_kb,
  })), [samples]);

  const hasIoWait = samples.some(s => s.d_io_wait_s > 0);
  const hasRead = samples.some(s => s.d_read_mb > 0);
  const hasNet = samples.some(s => s.d_net_send_kb > 0 || s.d_net_recv_kb > 0);

  const scrubberLine = highlightTime != null
    ? <ReferenceLine x={highlightTime} stroke={SCRUBBER_LINE_COLOR} strokeWidth={1.5} strokeDasharray="4 3" />
    : null;

  return (
    <div style={{ flex: 1, minHeight: 0, overflow: 'auto', padding: '12px 16px', background: SURFACE_BG }}>
      {/* CPU cores */}
      <ChartCard title="CPU Cores (cores)">
        <ResponsiveContainer width="100%" height={CHART_HEIGHT}>
          <AreaChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" stroke={GRID_STROKE} />
            <XAxis {...xAxisProps} />
            <YAxis tickFormatter={fmtCores} tick={axisTick} stroke={GRID_STROKE} />
            <Tooltip content={<CustomTooltip formatter={(_, v) => `${fmtCores(v)} cores`} />} cursor={{ stroke: GRID_STROKE }} />
            <Area type="monotone" dataKey="cpu_cores" stroke={COLORS.cpu} fill={COLORS.cpu} fillOpacity={0.15} strokeWidth={1.5} dot={false} name="CPU" isAnimationActive={false} />
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

      {/* I/O wait */}
      {hasIoWait && (
        <ChartCard title="I/O Wait (s)">
          <ResponsiveContainer width="100%" height={CHART_HEIGHT}>
            <LineChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke={GRID_STROKE} />
              <XAxis {...xAxisProps} />
              <YAxis tickFormatter={v => v.toFixed(2)} tick={axisTick} stroke={GRID_STROKE} />
              <Tooltip content={<CustomTooltip formatter={(_, v) => fmtSec(v)} />} cursor={{ stroke: GRID_STROKE }} />
              <Line type="monotone" dataKey="io_wait_s" stroke={COLORS.ioWait} strokeWidth={1.5} dot={false} name="I/O Wait" isAnimationActive={false} />
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
