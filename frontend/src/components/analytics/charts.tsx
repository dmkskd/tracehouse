/**
 * Shared 2D chart components used by both QueryExplorer and DashboardViewer.
 *
 * Uses recharts for all chart types for consistency, responsiveness,
 * and built-in interactivity (tooltips, hover, animations).
 */

import React, { useState } from 'react';
import { createPortal } from 'react-dom';
import {
  BarChart as RBarChart,
  Bar,
  LineChart as RLineChart,
  Line,
  AreaChart as RAreaChart,
  Area,
  PieChart as RPieChart,
  Pie,
  Cell,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  ReferenceLine,
} from 'recharts';
import type { ChartType } from './metaLanguage';
import { formatBytes } from '../../utils/formatters';

// ─── Mouse tracker for portal tooltips ───
let _tooltipMouseX = 0, _tooltipMouseY = 0;
if (typeof window !== 'undefined') {
  window.addEventListener('mousemove', (e) => { _tooltipMouseX = e.clientX; _tooltipMouseY = e.clientY; }, { passive: true });
}

// ─── Constants ───

export const CHART_COLORS = [
  '#6366f1','#22c55e','#f97316','#06b6d4','#ec4899',
  '#eab308','#8b5cf6','#14b8a6','#f43f5e','#84cc16',
  '#0ea5e9','#d946ef','#3b82f6','#a855f7',
];

/** Pick a distinct color for a panel by index — used by overlay charts when queries don't specify a color. */
export function chartColorByIndex(index: number): string {
  return CHART_COLORS[index % CHART_COLORS.length];
}

/** Generate a smooth gradient palette for sequential bar charts. */
function sequentialPalette(count: number): string[] {
  // Curated stops: violet → indigo → blue → teal → emerald → amber → coral
  // Hand-picked to stay vibrant without going neon
  const stops = [
    [270, 70, 60], // violet
    [240, 70, 62], // indigo
    [210, 75, 58], // blue
    [175, 65, 50], // teal
    [150, 60, 52], // emerald
    [35, 80, 58],  // amber
    [10, 75, 60],  // coral
  ];
  if (count <= 1) return [`hsl(${stops[0][0]}, ${stops[0][1]}%, ${stops[0][2]}%)`];
  return Array.from({ length: count }, (_, i) => {
    const t = i / (count - 1) * (stops.length - 1);
    const idx = Math.min(Math.floor(t), stops.length - 2);
    const frac = t - idx;
    const [h, s, l] = stops[idx].map((v, j) => v + frac * (stops[idx + 1][j] - v));
    return `hsl(${Math.round(h)}, ${Math.round(s)}%, ${Math.round(l)}%)`;
  });
}

/** Distinct colors for grouped/stacked chart series (high contrast between adjacent groups) */
export const GROUP_COLORS = [
  '#3b82f6', // blue
  '#f59e0b', // amber
  '#ef4444', // red
  '#10b981', // emerald
  '#8b5cf6', // violet
  '#ec4899', // pink
  '#06b6d4', // cyan
  '#f97316', // orange
  '#84cc16', // lime
  '#6366f1', // indigo
];

// ─── Types ───

export interface ChartDataPoint {
  label: string;
  value: number;
  color: string;
  /** Optional description text, surfaced in tooltip (from @chart description=column) */
  description?: string;
}

export interface ChartConfig {
  type: ChartType;
  groupByColumn: string;
  valueColumn: string;
  /** Multiple value columns for multi-series charts (e.g. value=col1,col2,col3). When set, each column becomes a separate series. */
  valueColumns?: string[];
  seriesColumn?: string;
  orientation?: 'horizontal' | 'vertical';
  visualization: '2d' | '3d';
  title?: string;
  description?: string;
  /** Unit suffix for value axis ticks, e.g. 'ms', 'MB', '%'. Parsed from @chart unit=... */
  unit?: string;
  /** Column whose value is shown as description in chart tooltips */
  descriptionColumn?: string;
  /** Override chart color (hex). Parsed from @chart color=... */
  color?: string;
}

export interface GroupedChartData {
  label: string;
  groups: { name: string; value: number; color: string }[];
}

export interface DrillDownEvent {
  /** The label value that was clicked (used to filter the drill target) */
  label: string;
  value: number;
}

// ─── Helpers ───

export function isNumericValue(v: unknown): boolean {
  if (typeof v === 'number') return true;
  if (typeof v === 'string' && v.trim() !== '' && !isNaN(Number(v))) return true;
  return false;
}

export function extractNumeric(v: unknown): number {
  if (typeof v === 'number') return v;
  if (typeof v === 'string') return Number(v) || 0;
  return 0;
}

import { formatCell } from '@tracehouse/core';
export { formatCell };

/** Smart time formatter: 500 ms → "500 ms", 1400 ms → "1.4 s", 90000 ms → "1.5 min" */
function formatTime(v: number, unit: string): string {
  const abs = Math.abs(v);
  if (unit === 'ms') {
    if (abs >= 60_000) return `${(v / 60_000).toFixed(1)} min`;
    if (abs >= 1_000) return `${(v / 1_000).toFixed(1)} s`;
    return `${Number.isInteger(v) ? v : v.toFixed(1)} ms`;
  }
  if (unit === 's') {
    if (abs >= 3_600) return `${(v / 3_600).toFixed(1)} h`;
    if (abs >= 60) return `${(v / 60).toFixed(1)} min`;
    return `${Number.isInteger(v) ? v : v.toFixed(1)} s`;
  }
  return `${formatCompact(v)} ${unit}`;
}

const TIME_UNITS = new Set(['ms', 's']);

/** Build a tick formatter that handles units smartly (auto-scales time units) */
export function compactFormatter(unit?: string): (v: number) => string {
  if (!unit) return formatCompact;
  if (unit === 'bytes') return formatBytes;
  if (TIME_UNITS.has(unit)) return (v: number) => formatTime(v, unit);
  return (v: number) => `${formatCompact(v)} ${unit}`;
}

/** Compact number formatting for axis ticks: 1200 → 1.2K, 3500000 → 3.5M */
function formatCompact(v: number): string {
  const abs = Math.abs(v);
  if (abs >= 1e9) return `${(v / 1e9).toFixed(1)}B`;
  if (abs >= 1e6) return `${(v / 1e6).toFixed(1)}M`;
  if (abs >= 1e3) return `${(v / 1e3).toFixed(1)}K`;
  if (Number.isInteger(v)) return v.toString();
  return v.toFixed(2);
}

/**
 * Build GroupedChartData from multi-column results (no group column needed).
 * Each valueColumn becomes a separate series — like ClickHouse native dashboards.
 */
export function buildMultiColumnGrouped(
  rows: Record<string, unknown>[],
  groupByColumn: string,
  valueColumns: string[],
): GroupedChartData[] {
  return rows.map(row => ({
    label: formatCell(row[groupByColumn], groupByColumn),
    groups: valueColumns.map((col, i) => ({
      name: col,
      value: extractNumeric(row[col]),
      color: GROUP_COLORS[i % GROUP_COLORS.length],
    })),
  }));
}

/** Smart X-axis tick formatter: extracts HH:MM:SS from datetime strings, truncates others */
export function formatXTick(v: string): string {
  // Match datetime like "2026-03-08 19:15:00" → "19:15:00"
  const m = v.match(/\d{4}-\d{2}-\d{2}\s+(\d{2}:\d{2}(?::\d{2})?)/);
  if (m) return m[1];
  // Truncate other long labels
  return v.length > 12 ? v.slice(0, 12) + '…' : v;
}

// ─── Data builders (shared by QueryExplorer, DashboardViewer, MiniPanelCard) ───

/**
 * Auto-detect label/value columns and build ChartDataPoint[] from query results.
 * When labelCol/valueCol are provided they're used directly; otherwise heuristics pick
 * the first non-numeric column as label and the first numeric column as value.
 */
export function buildChartData(
  rows: Record<string, unknown>[],
  columns: string[],
  labelCol?: string,
  valueCol?: string,
  maxRows?: number,
  descriptionCol?: string,
): ChartDataPoint[] {
  if (rows.length === 0) return [];
  const lblCol = labelCol || columns.find(c => !rows.some(r => isNumericValue(r[c]))) || columns[0];
  const valCol = valueCol || columns.find(c => c !== lblCol && rows.some(r => isNumericValue(r[c]))) || columns[1];
  if (!lblCol || !valCol) return [];
  const sliced = maxRows ? rows.slice(0, maxRows) : rows;
  const palette = sequentialPalette(sliced.length);
  return sliced.map((r, i) => ({
    label: formatCell(r[lblCol], lblCol),
    value: extractNumeric(r[valCol]),
    color: palette[i],
    ...(descriptionCol && r[descriptionCol] != null ? { description: String(r[descriptionCol]) } : {}),
  }));
}

/**
 * Build GroupedChartData from rows using either multi-column mode (valueColumns)
 * or standard group-column mode (groupColumn).
 */
export function buildGroupedChartData(
  rows: Record<string, unknown>[],
  groupByColumn: string,
  valueColumn: string,
  seriesColumn?: string,
  valueColumns?: string[],
  maxRows?: number,
): GroupedChartData[] {
  // Multi-column mode: each valueColumn becomes a series (no series column needed)
  if (valueColumns && valueColumns.length > 1) {
    const data = buildMultiColumnGrouped(rows, groupByColumn, valueColumns);
    return maxRows ? data.slice(0, maxRows * 2) : data;
  }
  // Standard series-column mode
  if (!seriesColumn) return [];
  const grouped = new Map<string, Map<string, number>>();
  for (const row of rows) {
    const label = formatCell(row[groupByColumn], groupByColumn);
    const series = formatCell(row[seriesColumn], seriesColumn);
    const value = extractNumeric(row[valueColumn]);
    if (!grouped.has(label)) grouped.set(label, new Map());
    grouped.get(label)!.set(series, value);
  }
  const allSeries = [...new Set(rows.map(r => formatCell(r[seriesColumn], seriesColumn)))];
  return Array.from(grouped.entries()).map(([label, groups]) => ({
    label,
    groups: allSeries.map((seriesName, i) => ({
      name: seriesName,
      value: groups.get(seriesName) || 0,
      color: CHART_COLORS[i % CHART_COLORS.length],
    })),
  })).slice(0, maxRows ?? Infinity);
}

/** Check whether a chart type is grouped/stacked (needs GroupedChartData). */
export function isGroupedChartType(type: ChartType): boolean {
  return type === 'grouped_bar' || type === 'stacked_bar' || type === 'grouped_line';
}

/** Sort rows by a column, handling numeric vs string comparison. */
export function sortRows(
  rows: Record<string, unknown>[],
  column: string,
  direction: 'asc' | 'desc',
): Record<string, unknown>[] {
  return [...rows].sort((a, b) => {
    const av = a[column], bv = b[column];
    const an = isNumericValue(av) ? extractNumeric(av) : NaN;
    const bn = isNumericValue(bv) ? extractNumeric(bv) : NaN;
    const cmp = !isNaN(an) && !isNaN(bn) ? an - bn : String(av ?? '').localeCompare(String(bv ?? ''));
    return direction === 'asc' ? cmp : -cmp;
  });
}

// ─── Shared tooltip style ───

const tooltipStyle: React.CSSProperties = {
  background: 'var(--bg-secondary, #1e293b)',
  border: '1px solid var(--border-primary, #334155)',
  borderRadius: 8,
  padding: '8px 12px',
  boxShadow: '0 4px 12px rgba(0,0,0,0.3)',
};

const tooltipLabelStyle: React.CSSProperties = {
  fontSize: 11,
  color: 'var(--text-muted, #94a3b8)',
  marginBottom: 2,
};

const tooltipValueStyle: React.CSSProperties = {
  fontSize: 13,
  color: 'var(--text-primary, #e2e8f0)',
  fontWeight: 600,
  fontFamily: "'Share Tech Mono', monospace",
};

const drillHintStyle: React.CSSProperties = {
  fontSize: 10,
  color: 'var(--accent-primary, #6366f1)',
  marginTop: 4,
  opacity: 0.8,
  fontStyle: 'italic',
};

// ─── Shared axis / grid props ───

const axisTickStyle = { fontSize: 10, fill: 'var(--text-muted, #9ca3af)' };
const axisLineStyle = { stroke: 'var(--border-secondary, #9ca3af)' };
const gridStroke = 'var(--border-secondary, rgba(148,163,184,0.15))';

// ─── Cross-panel correlation ───

/** A single entry in the correlation tooltip — one per participating panel. */
export interface CorrelationEntry {
  name: string;
  color: string;
  value: number | null;
  unit?: string;
}

/** Props added to time-series charts when correlation mode is active. */
export interface CrosshairProps {
  /** Timestamp label broadcast by another panel — render a ReferenceLine here. */
  hoveredTimestamp?: string | null;
  /** Called when this chart's hover position changes (label string, or null on leave). */
  onTimestampHover?: (label: string | null) => void;
  /** Values from all correlated panels at the hovered timestamp — shown in tooltip. */
  correlationValues?: CorrelationEntry[];
  /** Name of the panel rendering this chart — used to highlight "self" in the correlation tooltip. */
  currentPanelName?: string;
  /** Whether this panel is the one currently being hovered — used to hide stale tooltips on other panels. */
  isHoveredPanel?: boolean;
}

const crosshairLineProps = {
  stroke: 'rgba(255,255,255,0.45)',
  strokeDasharray: '4 3',
  strokeWidth: 1,
};

/** Custom recharts cursor that draws both vertical and horizontal crosshair lines. */
const CrosshairCursor: React.FC<any> = ({ points, width, height, top, left }) => {
  if (!points || !points.length) return null;
  const { x, y } = points[0];
  return (
    <g>
      <line x1={x} y1={top} x2={x} y2={top + height} {...crosshairLineProps} />
      <line x1={left} y1={y} x2={left + width} y2={y} {...crosshairLineProps} />
    </g>
  );
};

/** Helper: check if a chart type participates in cross-panel correlation. */
export function isTimeSeriesChartType(type: ChartType): boolean {
  return type === 'line' || type === 'area' || type === 'grouped_line';
}

// ─── Pulsating active dot for drillable charts ───

const pulseKeyframes = `@keyframes chartDotPulse { 0%,100% { opacity: .3; r: 8; } 50% { opacity: 0; r: 14; } }`;
let pulseStyleInjected = false;
function ensurePulseStyle() {
  if (pulseStyleInjected) return;
  const s = document.createElement('style');
  s.textContent = pulseKeyframes;
  document.head.appendChild(s);
  pulseStyleInjected = true;
}

/** Active dot that gently pulsates when the chart supports drill-down. */
const PulsatingDot: React.FC<{ cx?: number; cy?: number; fill?: string; drillable?: boolean; onClick?: () => void }> = ({ cx, cy, fill, drillable, onClick }) => {
  if (cx == null || cy == null) return null;
  ensurePulseStyle();
  return (
    <g style={{ cursor: drillable ? 'pointer' : undefined }} onClick={onClick}>
      {drillable && (
        <circle cx={cx} cy={cy} r={8} fill={fill} opacity={0.3}
          style={{ animation: 'chartDotPulse 1.8s ease-in-out infinite' }} />
      )}
      <circle cx={cx} cy={cy} r={4} fill={fill} stroke={fill} strokeWidth={2} />
    </g>
  );
};

// ═══════════════════════════════════════════════════════════════════════════
// Custom Tooltip
// ═══════════════════════════════════════════════════════════════════════════

export const AnalyticsTooltip: React.FC<{
  active?: boolean;
  payload?: Array<{ value: number; name?: string; color?: string; payload?: Record<string, unknown> }>;
  label?: string;
  drillIntoQuery?: string;
  correlationValues?: CorrelationEntry[];
  currentPanelName?: string;
  isHoveredPanel?: boolean;
}> = ({ active, payload, label, drillIntoQuery, correlationValues, currentPanelName, isHoveredPanel }) => {
  if (!active || !payload?.length) return null;
  if (isHoveredPanel === false) return null;
  // Extract description from the underlying data point (set by buildChartData)
  const description = payload[0]?.payload?.description as string | undefined;

  // Reorder correlation values: current panel first, then the rest in dashboard order
  const orderedCorrelation = correlationValues && correlationValues.length > 0 && currentPanelName
    ? [
        ...correlationValues.filter(cv => cv.name === currentPanelName),
        ...correlationValues.filter(cv => cv.name !== currentPanelName),
      ]
    : correlationValues;

  const content = (
    <div style={tooltipStyle}>
      {label && <div style={tooltipLabelStyle}>{label}</div>}
      {payload.map((entry, i) => (
        <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 6, marginTop: i > 0 ? 2 : 0 }}>
          {payload.length > 1 && (
            <span style={{ width: 8, height: 8, borderRadius: '50%', background: entry.color, flexShrink: 0 }} />
          )}
          <span style={tooltipValueStyle}>
            {Number(entry.value).toLocaleString(undefined, { maximumFractionDigits: 2 })}
          </span>
          {entry.name && entry.name !== 'value' && payload.length > 1 && (
            <span style={{ fontSize: 10, color: 'var(--text-muted, #94a3b8)' }}>{entry.name}</span>
          )}
        </div>
      ))}
      {description && (
        <div style={{ fontSize: 11, color: 'var(--text-muted, #94a3b8)', marginTop: 4, maxWidth: 280, lineHeight: 1.3 }}>
          {description}
        </div>
      )}
      {drillIntoQuery && (
        <div style={drillHintStyle}>{drillIntoQuery === 'Part Inspector' ? 'Click to inspect part' : `Click to drill into ${drillIntoQuery}`}</div>
      )}
      {orderedCorrelation && orderedCorrelation.length > 0 && (
        <div style={{ borderTop: '1px solid var(--border-secondary, rgba(148,163,184,0.2))', marginTop: 6, paddingTop: 5 }}>
          {orderedCorrelation.map((cv, i) => {
            const fmt = compactFormatter(cv.unit);
            const isSelf = cv.name === currentPanelName;
            return (
              <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 5, marginTop: i > 0 ? 2 : 0 }}>
                <span style={{ width: 6, height: 6, borderRadius: '50%', background: cv.color, flexShrink: 0 }} />
                <span style={{ fontSize: 11, color: isSelf ? 'var(--text-primary, #e2e8f0)' : 'var(--text-muted, #94a3b8)', fontWeight: isSelf ? 600 : 400, flex: 1 }}>
                  {cv.name.length > 18 ? cv.name.slice(0, 17) + '…' : cv.name}
                </span>
                <span style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-primary, #e2e8f0)', fontFamily: "'Share Tech Mono', monospace" }}>
                  {cv.value != null ? fmt(cv.value) : '—'}
                </span>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );

  // Portal mode: render at body level to escape overflow:hidden/auto ancestors
  if (orderedCorrelation && orderedCorrelation.length > 0) {
    let x = _tooltipMouseX + 20;
    let y = _tooltipMouseY - 20;
    if (x + 300 > window.innerWidth) x = _tooltipMouseX - 320;
    if (y + 400 > window.innerHeight) y = Math.max(10, window.innerHeight - 420);
    if (y < 10) y = 10;
    return createPortal(
      <div style={{ position: 'fixed', left: x, top: y, zIndex: 10000, pointerEvents: 'none' }}>
        {content}
      </div>,
      document.body,
    );
  }

  return content;
};

// Pie-specific tooltip
const PieTooltip: React.FC<{
  active?: boolean;
  payload?: Array<{ payload: { label: string; value: number; pct: number; description?: string }; color?: string }>;
  drillIntoQuery?: string;
  unit?: string;
}> = ({ active, payload, drillIntoQuery, unit }) => {
  if (!active || !payload?.length) return null;
  const d = payload[0].payload;
  return (
    <div style={tooltipStyle}>
      <div style={tooltipLabelStyle}>{d.label}</div>
      <div style={tooltipValueStyle}>
        {unit === 'bytes' ? formatBytes(d.value) : d.value.toLocaleString(undefined, { maximumFractionDigits: 2 })}
        <span style={{ fontSize: 11, fontWeight: 400, color: 'var(--text-muted)', marginLeft: 6 }}>
          ({d.pct.toFixed(1)}%)
        </span>
      </div>
      {d.description && (
        <div style={{ fontSize: 11, color: 'var(--text-muted, #94a3b8)', marginTop: 4, maxWidth: 280, lineHeight: 1.3 }}>
          {d.description}
        </div>
      )}
      {drillIntoQuery && (
        <div style={drillHintStyle}>{drillIntoQuery === 'Part Inspector' ? 'Click to inspect part' : `Click to drill into ${drillIntoQuery}`}</div>
      )}
    </div>
  );
};

// ═══════════════════════════════════════════════════════════════════════════
// 2D Charts — all using recharts
// ═══════════════════════════════════════════════════════════════════════════

export const BarChart2D: React.FC<{ data: ChartDataPoint[]; fullHeight?: boolean; onDrillDown?: (e: DrillDownEvent) => void; unit?: string; drillIntoQuery?: string }> = ({ data, fullHeight, onDrillDown, unit, drillIntoQuery }) => {
  if (!data.length) return null;
  const rechartsData = data.map(d => ({ name: d.label, value: d.value, fill: d.color, description: d.description }));
  const defaultHeight = Math.max(200, data.length * 28 + 40);
  // Use horizontal layout (bars going right) for readability with labels
  return (
    <ResponsiveContainer width="100%" height={fullHeight ? '100%' : defaultHeight} minHeight={fullHeight ? 180 : undefined}>
      <RBarChart data={rechartsData} layout="vertical" margin={{ top: 5, right: 40, left: 10, bottom: 5 }}>
        <CartesianGrid strokeDasharray="3 3" stroke={gridStroke} horizontal={false} />
        <XAxis
          type="number"
          tick={axisTickStyle}
          tickLine={axisLineStyle}
          axisLine={axisLineStyle}
          tickFormatter={compactFormatter(unit)}
        />
        <YAxis
          type="category"
          dataKey="name"
          tick={axisTickStyle}
          tickLine={false}
          axisLine={axisLineStyle}
          width={110}
          tickFormatter={(v: string) => v.length > 16 ? v.slice(0, 16) + '…' : v}
        />
        <Tooltip content={<AnalyticsTooltip drillIntoQuery={drillIntoQuery} />} cursor={{ fill: 'rgba(99,102,241,0.06)' }} />
        <Bar dataKey="value" radius={[0, 4, 4, 0]} animationDuration={400} maxBarSize={22}
          cursor={onDrillDown ? 'pointer' : undefined}
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          onClick={onDrillDown ? (entry: any) => onDrillDown({ label: String(entry?.name ?? ''), value: Number(entry?.value ?? 0) }) : undefined}
        >
          {rechartsData.map((entry, i) => (
            <Cell key={i} fill={entry.fill} />
          ))}
        </Bar>
      </RBarChart>
    </ResponsiveContainer>
  );
};

export const LineChart2D: React.FC<{ data: ChartDataPoint[]; fullHeight?: boolean; onDrillDown?: (e: DrillDownEvent) => void; unit?: string; drillIntoQuery?: string; color?: string } & CrosshairProps> = ({ data, fullHeight, onDrillDown, unit, drillIntoQuery, color, hoveredTimestamp, onTimestampHover, correlationValues, currentPanelName, isHoveredPanel }) => {
  if (data.length < 2) return null;
  const c = color || '#6366f1';
  const drillable = !!onDrillDown;
  const rechartsData = data.map(d => ({ name: d.label, value: d.value, description: d.description }));
  return (
    <ResponsiveContainer width="100%" height={fullHeight ? '100%' : 280} minHeight={fullHeight ? 180 : undefined}>
      <RLineChart data={rechartsData} margin={{ top: 5, right: 20, left: 10, bottom: 5 }}
        style={drillable ? { cursor: 'pointer' } : undefined}
        onClick={drillable ? (e: any) => { if (e?.activeLabel) { const row = data.find(d => d.label === e.activeLabel); if (row) onDrillDown!({ label: row.label, value: row.value }); } } : undefined}
        onMouseMove={onTimestampHover ? (e: any) => { if (e?.activeLabel) onTimestampHover(e.activeLabel); } : undefined}
        onMouseLeave={onTimestampHover ? () => onTimestampHover(null) : undefined}
      >
        <CartesianGrid strokeDasharray="3 3" stroke={gridStroke} />
        <XAxis
          dataKey="name"
          tick={axisTickStyle}
          tickLine={axisLineStyle}
          axisLine={axisLineStyle}
          interval="preserveStartEnd"
          tickFormatter={formatXTick}
        />
        <YAxis
          tick={axisTickStyle}
          tickLine={axisLineStyle}
          axisLine={axisLineStyle}
          tickFormatter={compactFormatter(unit)}
          width={50}
        />
        <Tooltip content={<AnalyticsTooltip drillIntoQuery={drillIntoQuery} correlationValues={correlationValues} currentPanelName={currentPanelName} isHoveredPanel={isHoveredPanel} />} offset={20} wrapperStyle={{ zIndex: 1000, pointerEvents: 'none' }} cursor={<CrosshairCursor />} />
        {hoveredTimestamp && <ReferenceLine x={hoveredTimestamp} {...crosshairLineProps} />}
        <Line
          type="monotone"
          dataKey="value"
          stroke={c}
          strokeWidth={2}
          dot={false}
          activeDot={(props: any) => (
            <PulsatingDot cx={props.cx as number} cy={props.cy as number} fill={c} drillable={drillable} />
          )}
          animationDuration={300}
        />
      </RLineChart>
    </ResponsiveContainer>
  );
};

export const PieChart2D: React.FC<{ data: ChartDataPoint[]; fullHeight?: boolean; onDrillDown?: (e: DrillDownEvent) => void; drillIntoQuery?: string; unit?: string }> = ({ data, fullHeight, onDrillDown, drillIntoQuery, unit }) => {
  if (!data.length) return null;
  const total = data.reduce((s, d) => s + d.value, 0);
  const pieData = data.map(d => ({
    label: d.label,
    value: d.value,
    pct: total > 0 ? (d.value / total) * 100 : 0,
    color: d.color,
    description: d.description,
  }));

  return (
    <ResponsiveContainer width="100%" height={fullHeight ? '100%' : 300} minHeight={fullHeight ? 180 : undefined}>
      <RPieChart>
        <Pie
          data={pieData}
          dataKey="value"
          nameKey="label"
          cx="40%"
          cy="50%"
          outerRadius={100}
          innerRadius={40}
          paddingAngle={1}
          animationDuration={400}
          cursor={onDrillDown ? 'pointer' : undefined}
          onClick={onDrillDown ? (entry: { label: string; value: number }) => onDrillDown({ label: entry.label, value: entry.value }) : undefined}
          label={(props) => {
            const { x, y, percent } = props as unknown as { x: number; y: number; percent: number };
            const pct = (percent ?? 0) * 100;
            if (pct < 5) return <g />;
            return (
              <text x={x as number} y={y as number} textAnchor="middle" dominantBaseline="central"
                style={{ fontSize: 10, fill: 'var(--text-muted, #9ca3af)', fontFamily: "'Share Tech Mono', monospace" }}>
                {pct.toFixed(1)}%
              </text>
            );
          }}
          labelLine={{ stroke: 'var(--border-secondary, rgba(148,163,184,0.3))', strokeWidth: 1 }}
        >
          {pieData.map((entry, i) => (
            <Cell key={i} fill={entry.color} stroke="var(--bg-primary, #030712)" strokeWidth={1} />
          ))}
        </Pie>
        <Tooltip content={<PieTooltip drillIntoQuery={drillIntoQuery} unit={unit} />} />
        <Legend
          layout="vertical"
          align="right"
          verticalAlign="middle"
          iconType="circle"
          iconSize={8}
          formatter={(value: string) => (
            <span style={{ fontSize: 11, color: 'var(--text-secondary, #cbd5e1)' }}>
              {value.length > 20 ? value.slice(0, 20) + '…' : value}
            </span>
          )}
          wrapperStyle={{ fontSize: 11, lineHeight: '20px', paddingLeft: 10 }}
        />
      </RPieChart>
    </ResponsiveContainer>
  );
};

export const AreaChart2D: React.FC<{ data: ChartDataPoint[]; fullHeight?: boolean; onDrillDown?: (e: DrillDownEvent) => void; unit?: string; drillIntoQuery?: string; color?: string } & CrosshairProps> = ({ data, fullHeight, onDrillDown, unit, drillIntoQuery, color, hoveredTimestamp, onTimestampHover, correlationValues, currentPanelName, isHoveredPanel }) => {
  if (data.length < 2) return null;
  const c = color || '#6366f1';
  const gradId = `areaGrad-${c.replace('#', '')}`;
  const drillable = !!onDrillDown;
  const rechartsData = data.map(d => ({ name: d.label, value: d.value, description: d.description }));
  return (
    <ResponsiveContainer width="100%" height={fullHeight ? '100%' : 280} minHeight={fullHeight ? 180 : undefined}>
      <RAreaChart data={rechartsData} margin={{ top: 5, right: 20, left: 10, bottom: 5 }}
        style={drillable ? { cursor: 'pointer' } : undefined}
        onClick={drillable ? (e: any) => { if (e?.activeLabel) { const row = data.find(d => d.label === e.activeLabel); if (row) onDrillDown!({ label: row.label, value: row.value }); } } : undefined}
        onMouseMove={onTimestampHover ? (e: any) => { if (e?.activeLabel) onTimestampHover(e.activeLabel); } : undefined}
        onMouseLeave={onTimestampHover ? () => onTimestampHover(null) : undefined}
      >
        <defs>
          <linearGradient id={gradId} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={c} stopOpacity={0.45} />
            <stop offset="100%" stopColor={c} stopOpacity={0.06} />
          </linearGradient>
        </defs>
        <CartesianGrid strokeDasharray="3 3" stroke={gridStroke} />
        <XAxis
          dataKey="name"
          tick={axisTickStyle}
          tickLine={axisLineStyle}
          axisLine={axisLineStyle}
          interval="preserveStartEnd"
          tickFormatter={formatXTick}
        />
        <YAxis
          tick={axisTickStyle}
          tickLine={axisLineStyle}
          axisLine={axisLineStyle}
          tickFormatter={compactFormatter(unit)}
          width={50}
        />
        <Tooltip content={<AnalyticsTooltip drillIntoQuery={drillIntoQuery} correlationValues={correlationValues} currentPanelName={currentPanelName} isHoveredPanel={isHoveredPanel} />} offset={20} wrapperStyle={{ zIndex: 1000, pointerEvents: 'none' }} cursor={<CrosshairCursor />} />
        {hoveredTimestamp && <ReferenceLine x={hoveredTimestamp} {...crosshairLineProps} />}
        <Area
          type="monotone"
          dataKey="value"
          stroke={c}
          strokeWidth={2}
          fill={`url(#${gradId})`}
          dot={false}
          activeDot={(props: any) => (
            <PulsatingDot cx={props.cx as number} cy={props.cy as number} fill={c} drillable={drillable} />
          )}
          animationDuration={300}
        />
      </RAreaChart>
    </ResponsiveContainer>
  );
};

// ═══════════════════════════════════════════════════════════════════════════
// Dual-axis Area Chart — two value columns, left + right Y axes
// ═══════════════════════════════════════════════════════════════════════════

export const DualAxisAreaChart2D: React.FC<{ data: GroupedChartData[]; valueColumns: string[]; fullHeight?: boolean; onDrillDown?: (e: DrillDownEvent) => void; drillIntoQuery?: string } & CrosshairProps> = ({ data, valueColumns, fullHeight, onDrillDown, drillIntoQuery, hoveredTimestamp, onTimestampHover, correlationValues, currentPanelName, isHoveredPanel }) => {
  if (data.length < 2) return null;
  const { rows, groupNames, colorMap } = flattenGrouped(data);
  const leftCol = groupNames[0];
  const rightCol = groupNames[1];
  const leftColor = colorMap[leftCol] || GROUP_COLORS[0];
  const rightColor = colorMap[rightCol] || GROUP_COLORS[1];
  const gradIdLeft = `dualGradL-${leftColor.replace('#', '')}`;
  const gradIdRight = `dualGradR-${rightColor.replace('#', '')}`;
  const drillable = !!onDrillDown;
  return (
    <ResponsiveContainer width="100%" height={fullHeight ? '100%' : 280} minHeight={fullHeight ? 180 : undefined}>
      <RAreaChart data={rows} margin={{ top: 5, right: 60, left: 10, bottom: 5 }}
        style={drillable ? { cursor: 'pointer' } : undefined}
        onClick={drillable ? (e: any) => { if (e?.activeLabel) onDrillDown!({ label: e.activeLabel, value: 0 }); } : undefined}
        onMouseMove={onTimestampHover ? (e: any) => { if (e?.activeLabel) onTimestampHover(e.activeLabel); } : undefined}
        onMouseLeave={onTimestampHover ? () => onTimestampHover(null) : undefined}
      >
        <defs>
          <linearGradient id={gradIdLeft} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={leftColor} stopOpacity={0.45} />
            <stop offset="100%" stopColor={leftColor} stopOpacity={0.06} />
          </linearGradient>
          <linearGradient id={gradIdRight} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={rightColor} stopOpacity={0.45} />
            <stop offset="100%" stopColor={rightColor} stopOpacity={0.06} />
          </linearGradient>
        </defs>
        <CartesianGrid strokeDasharray="3 3" stroke={gridStroke} />
        <XAxis dataKey="name" tick={axisTickStyle} tickLine={axisLineStyle} axisLine={axisLineStyle}
          interval="preserveStartEnd" tickFormatter={formatXTick} />
        <YAxis yAxisId="left" tick={axisTickStyle} tickLine={axisLineStyle} axisLine={axisLineStyle}
          tickFormatter={compactFormatter()} width={50} />
        <YAxis yAxisId="right" orientation="right" tick={axisTickStyle} tickLine={axisLineStyle} axisLine={axisLineStyle}
          tickFormatter={compactFormatter()} width={55} />
        <Tooltip content={<AnalyticsTooltip drillIntoQuery={drillIntoQuery} correlationValues={correlationValues} currentPanelName={currentPanelName} isHoveredPanel={isHoveredPanel} />} offset={20} wrapperStyle={{ zIndex: 1000, pointerEvents: 'none' }} cursor={<CrosshairCursor />} />
        {hoveredTimestamp && <ReferenceLine x={hoveredTimestamp} yAxisId="left" {...crosshairLineProps} />}
        <Legend iconType="line" iconSize={14} formatter={(value: string) => (
          <span style={{ fontSize: 11, color: 'var(--text-secondary)' }}>
            {value === leftCol ? `${valueColumns[0]} (left)` : `${valueColumns[1]} (right)`}
          </span>
        )} />
        <Area type="monotone" dataKey={leftCol} yAxisId="left" stroke={leftColor} strokeWidth={2}
          fill={`url(#${gradIdLeft})`} dot={false}
          activeDot={(props: any) => (
            <PulsatingDot cx={props.cx as number} cy={props.cy as number} fill={leftColor} drillable={drillable} />
          )} animationDuration={300} />
        <Area type="monotone" dataKey={rightCol} yAxisId="right" stroke={rightColor} strokeWidth={2}
          fill={`url(#${gradIdRight})`} dot={false}
          activeDot={(props: any) => (
            <PulsatingDot cx={props.cx as number} cy={props.cy as number} fill={rightColor} drillable={drillable} />
          )} animationDuration={300} />
      </RAreaChart>
    </ResponsiveContainer>
  );
};

// ═══════════════════════════════════════════════════════════════════════════
// Grouped / Stacked 2D Charts — recharts-based
// ═══════════════════════════════════════════════════════════════════════════

/** Transform GroupedChartData[] into recharts-friendly flat rows with one key per group */
function flattenGrouped(data: GroupedChartData[]): { rows: Record<string, unknown>[]; groupNames: string[]; colorMap: Record<string, string> } {
  const groupNames = [...new Set(data.flatMap(d => d.groups.map(g => g.name)))];
  const colorMap: Record<string, string> = {};
  data.forEach(d => d.groups.forEach(g => { if (!colorMap[g.name]) colorMap[g.name] = g.color; }));
  // Fallback colors
  groupNames.forEach((name, i) => { if (!colorMap[name]) colorMap[name] = GROUP_COLORS[i % GROUP_COLORS.length]; });

  const rows = data.map(d => {
    const row: Record<string, unknown> = { name: d.label };
    d.groups.forEach(g => { row[g.name] = g.value; });
    return row;
  });
  return { rows, groupNames, colorMap };
}

/** Extract the x-axis label from a Recharts bar onClick entry (works for simple, grouped, and stacked bars) */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function barClickLabel(entry: any): string {
  return String(entry?.payload?.name ?? entry?.name ?? '');
}

export const GroupedBarChart2D: React.FC<{ data: GroupedChartData[]; orientation?: 'horizontal' | 'vertical'; onDrillDown?: (e: DrillDownEvent) => void; unit?: string; drillIntoQuery?: string }> = ({ data, orientation = 'horizontal', onDrillDown, unit, drillIntoQuery }) => {
  if (!data.length) return null;
  const { rows, groupNames, colorMap } = flattenGrouped(data);

  if (orientation === 'horizontal') {
    return (
      <ResponsiveContainer width="100%" height={Math.max(250, data.length * 50 + 60)}>
        <RBarChart data={rows} layout="vertical" margin={{ top: 10, right: 30, left: 10, bottom: 10 }}>
          <CartesianGrid strokeDasharray="3 3" stroke={gridStroke} horizontal={false} />
          <XAxis type="number" tick={axisTickStyle} tickLine={axisLineStyle} axisLine={axisLineStyle} tickFormatter={compactFormatter(unit)} />
          <YAxis type="category" dataKey="name" tick={axisTickStyle} tickLine={false} axisLine={axisLineStyle} width={110}
            tickFormatter={(v: string) => v.length > 15 ? v.slice(0, 15) + '…' : v} />
          <Tooltip content={<AnalyticsTooltip drillIntoQuery={drillIntoQuery} />} cursor={{ fill: 'rgba(99,102,241,0.06)' }} />
          <Legend iconType="circle" iconSize={8}
            formatter={(value: string) => <span style={{ fontSize: 11, color: 'var(--text-secondary)' }}>{value}</span>} />
          {groupNames.map(name => (
            <Bar key={name} dataKey={name} fill={colorMap[name]} radius={[0, 3, 3, 0]} maxBarSize={16} animationDuration={400}
              cursor={onDrillDown ? 'pointer' : undefined}
              // eslint-disable-next-line @typescript-eslint/no-explicit-any
              onClick={onDrillDown ? (entry: any) => onDrillDown({ label: barClickLabel(entry), value: 0 }) : undefined}
            />
          ))}
        </RBarChart>
      </ResponsiveContainer>
    );
  }

  // Vertical (default)
  return (
    <ResponsiveContainer width="100%" height={320}>
      <RBarChart data={rows} margin={{ top: 10, right: 20, left: 10, bottom: 40 }}>
        <CartesianGrid strokeDasharray="3 3" stroke={gridStroke} vertical={false} />
        <XAxis dataKey="name" tick={axisTickStyle} tickLine={axisLineStyle} axisLine={axisLineStyle}
          interval={0} angle={-45} textAnchor="end" height={60}
          tickFormatter={formatXTick} />
        <YAxis tick={axisTickStyle} tickLine={axisLineStyle} axisLine={axisLineStyle} tickFormatter={compactFormatter(unit)} width={50} />
        <Tooltip content={<AnalyticsTooltip drillIntoQuery={drillIntoQuery} />} cursor={{ fill: 'rgba(99,102,241,0.06)' }} />
        <Legend iconType="circle" iconSize={8}
          formatter={(value: string) => <span style={{ fontSize: 11, color: 'var(--text-secondary)' }}>{value}</span>} />
        {groupNames.map(name => (
          <Bar key={name} dataKey={name} fill={colorMap[name]} radius={[3, 3, 0, 0]} maxBarSize={24} animationDuration={400}
            cursor={onDrillDown ? 'pointer' : undefined}
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
              onClick={onDrillDown ? (entry: any) => onDrillDown({ label: barClickLabel(entry), value: 0 }) : undefined}
          />
        ))}
      </RBarChart>
    </ResponsiveContainer>
  );
};

export const StackedBarChart2D: React.FC<{ data: GroupedChartData[]; orientation?: 'horizontal' | 'vertical'; onDrillDown?: (e: DrillDownEvent) => void; unit?: string; drillIntoQuery?: string }> = ({ data, orientation = 'horizontal', onDrillDown, unit, drillIntoQuery }) => {
  if (!data.length) return null;
  const { rows, groupNames, colorMap } = flattenGrouped(data);

  if (orientation === 'horizontal') {
    return (
      <ResponsiveContainer width="100%" height={Math.max(250, data.length * 40 + 60)}>
        <RBarChart data={rows} layout="vertical" margin={{ top: 10, right: 30, left: 10, bottom: 10 }}>
          <CartesianGrid strokeDasharray="3 3" stroke={gridStroke} horizontal={false} />
          <XAxis type="number" tick={axisTickStyle} tickLine={axisLineStyle} axisLine={axisLineStyle} tickFormatter={compactFormatter(unit)} />
          <YAxis type="category" dataKey="name" tick={axisTickStyle} tickLine={false} axisLine={axisLineStyle} width={110}
            tickFormatter={(v: string) => v.length > 15 ? v.slice(0, 15) + '…' : v} />
          <Tooltip content={<AnalyticsTooltip drillIntoQuery={drillIntoQuery} />} cursor={{ fill: 'rgba(99,102,241,0.06)' }} />
          <Legend iconType="circle" iconSize={8}
            formatter={(value: string) => <span style={{ fontSize: 11, color: 'var(--text-secondary)' }}>{value}</span>} />
          {groupNames.map(name => (
            <Bar key={name} dataKey={name} fill={colorMap[name]} stackId="stack" maxBarSize={28} animationDuration={400}
              stroke="var(--bg-primary, #030712)" strokeWidth={1}
              cursor={onDrillDown ? 'pointer' : undefined}
              // eslint-disable-next-line @typescript-eslint/no-explicit-any
              onClick={onDrillDown ? (entry: any) => onDrillDown({ label: barClickLabel(entry), value: 0 }) : undefined}
            />
          ))}
        </RBarChart>
      </ResponsiveContainer>
    );
  }

  return (
    <ResponsiveContainer width="100%" height={380}>
      <RBarChart data={rows} margin={{ top: 10, right: 20, left: 10, bottom: 10 }}>
        <CartesianGrid strokeDasharray="3 3" stroke={gridStroke} vertical={false} />
        <XAxis dataKey="name" tick={axisTickStyle} tickLine={axisLineStyle} axisLine={axisLineStyle}
          interval={0} angle={-45} textAnchor="end" height={100}
          tickFormatter={(v: string) => v.length > 16 ? v.slice(0, 16) + '…' : v} />
        <YAxis tick={axisTickStyle} tickLine={axisLineStyle} axisLine={axisLineStyle} tickFormatter={compactFormatter(unit)} width={50} />
        <Tooltip content={<AnalyticsTooltip drillIntoQuery={drillIntoQuery} />} cursor={{ fill: 'rgba(99,102,241,0.06)' }} />
        <Legend iconType="circle" iconSize={8}
          formatter={(value: string) => <span style={{ fontSize: 11, color: 'var(--text-secondary)' }}>{value}</span>} />
        {groupNames.map(name => (
          <Bar key={name} dataKey={name} fill={colorMap[name]} stackId="stack" radius={[3, 3, 0, 0]} maxBarSize={40} animationDuration={400}
            stroke="var(--bg-primary, #030712)" strokeWidth={1}
            cursor={onDrillDown ? 'pointer' : undefined}
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
              onClick={onDrillDown ? (entry: any) => onDrillDown({ label: barClickLabel(entry), value: 0 }) : undefined}
          />
        ))}
      </RBarChart>
    </ResponsiveContainer>
  );
};

export const GroupedLineChart2D: React.FC<{ data: GroupedChartData[]; onDrillDown?: (e: DrillDownEvent) => void; unit?: string; drillIntoQuery?: string } & CrosshairProps> = ({ data, onDrillDown, unit, drillIntoQuery, hoveredTimestamp, onTimestampHover, correlationValues, currentPanelName, isHoveredPanel }) => {
  const [hoveredLine, setHoveredLine] = useState<string | null>(null);
  if (!data.length) return null;
  const { rows, groupNames, colorMap } = flattenGrouped(data);

  return (
    <ResponsiveContainer width="100%" height={320}>
      <RLineChart data={rows} margin={{ top: 10, right: 20, left: 10, bottom: 5 }}
        style={onDrillDown ? { cursor: 'pointer' } : undefined}
        onClick={onDrillDown ? (e: any) => { if (e?.activeLabel) onDrillDown({ label: e.activeLabel, value: 0 }); } : undefined}
        onMouseMove={onTimestampHover ? (e: any) => { if (e?.activeLabel) onTimestampHover(e.activeLabel); } : undefined}
        onMouseLeave={onTimestampHover ? () => onTimestampHover(null) : undefined}
      >
        <CartesianGrid strokeDasharray="3 3" stroke={gridStroke} />
        <XAxis dataKey="name" tick={axisTickStyle} tickLine={axisLineStyle} axisLine={axisLineStyle}
          interval="preserveStartEnd"
          tickFormatter={formatXTick} />
        <YAxis tick={axisTickStyle} tickLine={axisLineStyle} axisLine={axisLineStyle} tickFormatter={compactFormatter(unit)} width={50} />
        <Tooltip content={<AnalyticsTooltip drillIntoQuery={drillIntoQuery} correlationValues={correlationValues} currentPanelName={currentPanelName} isHoveredPanel={isHoveredPanel} />} offset={20} wrapperStyle={{ zIndex: 1000, pointerEvents: 'none' }} cursor={<CrosshairCursor />} />
        {hoveredTimestamp && <ReferenceLine x={hoveredTimestamp} {...crosshairLineProps} />}
        <Legend iconType="line" iconSize={14}
          onMouseEnter={(e) => setHoveredLine(String(e.dataKey))}
          onMouseLeave={() => setHoveredLine(null)}
          formatter={(value: string) => (
            <span style={{
              fontSize: 11,
              color: 'var(--text-secondary)',
              opacity: hoveredLine && hoveredLine !== value ? 0.3 : 1,
              transition: 'opacity 0.15s',
            }}>{value.length > 14 ? value.slice(0, 14) + '…' : value}</span>
          )} />
        {groupNames.map(name => (
          <Line
            key={name}
            type="monotone"
            dataKey={name}
            stroke={colorMap[name]}
            strokeWidth={hoveredLine === name ? 3 : 2}
            strokeOpacity={hoveredLine && hoveredLine !== name ? 0.2 : 1}
            dot={false}
            activeDot={(props: any) => (
              <PulsatingDot cx={props.cx as number} cy={props.cy as number} fill={colorMap[name]} drillable={!!onDrillDown} />
            )}
            animationDuration={300}
          />
        ))}
      </RLineChart>
    </ResponsiveContainer>
  );
};

// ═══════════════════════════════════════════════════════════════════════════
// Overlay Chart — multiple normalized series on one chart with correlation coloring
// ═══════════════════════════════════════════════════════════════════════════

import {
  normalizePanelData,
  correlateToFocused,
  correlationToOpacity,
  correlationStrength,
  rollingCorrelation,
  computeInsightsAndLags,
  CORRELATION_ALGORITHMS,
  ROLLING_WINDOW_TRIGGER,
  ROLLING_WINDOW_THRESHOLD,
  type CorrelationFn,
  type CorrelationInsight,
} from '@tracehouse/core';

export interface OverlayChartProps {
  /** Panel time-series data collected from the dashboard */
  panels: { name: string; color: string; unit?: string; dataByLabel: Map<string, number> }[];
}

/** All possible insight labels for filtering. */
const INSIGHT_FILTERS = [
  { label: 'strong linear', color: '#22c55e' },
  { label: 'non-linear', color: '#eab308' },
  { label: 'lagged', color: '#eab308' },
  { label: 'outlier-driven', color: '#eab308' },
] as const;

/** Correlation strength buckets for filtering. */
const STRENGTH_FILTERS = [
  { id: 'strong', label: 'Strong (|r| >= 0.7)', test: (r: number) => Math.abs(r) >= 0.7, color: '#22c55e' },
  { id: 'moderate', label: 'Moderate (0.4-0.7)', test: (r: number) => Math.abs(r) >= 0.4 && Math.abs(r) < 0.7, color: '#eab308' },
  { id: 'weak', label: 'Weak (|r| < 0.4)', test: (r: number) => Math.abs(r) < 0.4, color: 'var(--text-muted)' },
  { id: 'inverse', label: 'Inverse (r < -0.4)', test: (r: number) => r < -0.4, color: '#ef4444' },
] as const;

export const OverlayChart: React.FC<OverlayChartProps> = ({ panels }) => {
  const [hoveredSeries, setHoveredSeries] = useState<string | null>(null);
  const [lockedSeries, setLockedSeries] = useState<string | null>(null);
  const [hiddenSeries, setHiddenSeries] = useState<Set<string>>(new Set());
  const [algorithmId, setAlgorithmId] = useState('pearson');
  const [contextMenu, setContextMenu] = useState<{ x: number; y: number } | null>(null);
  // Lightweight chart hover — just dims other lines, no correlation computation
  const [chartHoveredSeries, setChartHoveredSeries] = useState<string | null>(null);
  const [showInfoPopover, setShowInfoPopover] = useState(false);

  const chartContainerRef = React.useRef<HTMLDivElement>(null);

  // Locked focus takes priority over legend hover (chart hover is separate)
  const focusedSeries = lockedSeries ?? hoveredSeries;

  const algorithm = CORRELATION_ALGORITHMS.find(a => a.id === algorithmId) ?? CORRELATION_ALGORITHMS[0];
  const correlationFn: CorrelationFn = algorithm.fn;

  const normalized = React.useMemo(() => normalizePanelData(panels), [panels]);

  const correlationMap = React.useMemo(() => {
    if (!focusedSeries) return null;
    return correlateToFocused(normalized, focusedSeries, correlationFn);
  }, [normalized, focusedSeries, correlationFn]);

  // Compute all 3 algorithm scores for insight comparison + lag details
  const { insightMap, lagMap } = React.useMemo(() => {
    if (!focusedSeries) return { insightMap: new Map<string, CorrelationInsight | null>(), lagMap: new Map<string, number>() };
    const { insights, lags } = computeInsightsAndLags(normalized, focusedSeries);
    return { insightMap: insights, lagMap: lags };
  }, [normalized, focusedSeries]);

  const correlatedWindows = React.useMemo(() => {
    if (!focusedSeries) return [];
    const focused = normalized.find(s => s.name === focusedSeries);
    if (!focused) return [];
    const windows: { seriesName: string; startTs: string; endTs: string; r: number }[] = [];
    const windowSize = Math.max(5, Math.floor(focused.timestamps.length / 10));
    for (const s of normalized) {
      if (s.name === focusedSeries) continue;
      const r = correlationMap?.get(s.name) ?? 0;
      if (Math.abs(r) < ROLLING_WINDOW_TRIGGER) {
        const hits = rollingCorrelation(focused.normalized, s.normalized, windowSize, ROLLING_WINDOW_THRESHOLD, correlationFn);
        for (const h of hits) {
          windows.push({
            seriesName: s.name,
            startTs: focused.timestamps[h.startIdx],
            endTs: focused.timestamps[Math.min(h.endIdx, focused.timestamps.length - 1)],
            r: h.r,
          });
        }
      }
    }
    return windows;
  }, [normalized, focusedSeries, correlationMap]);

  if (normalized.length === 0 || normalized[0].timestamps.length < 2) {
    return <div style={{ padding: 24, color: 'var(--text-muted)', fontSize: 12 }}>No time-series data available for overlay. Enable Correlate mode and hover panels to collect data.</div>;
  }

  const timestamps = normalized[0].timestamps;
  const visibleSeries = normalized.filter(s => !hiddenSeries.has(s.name));

  const rechartsData = timestamps.map((t, i) => {
    const row: Record<string, unknown> = { name: t };
    for (const s of visibleSeries) {
      const v = s.normalized[i];
      if (!Number.isNaN(v)) row[s.name] = v;
    }
    return row;
  });

  const getOpacity = (name: string): number => {
    // Chart hover: light dimming only
    if (!focusedSeries && chartHoveredSeries) {
      return name === chartHoveredSeries ? 1 : 0.5;
    }
    if (!focusedSeries) return 0.8;
    if (name === focusedSeries) return 1;
    return correlationToOpacity(correlationMap?.get(name) ?? 0);
  };

  const getStrokeWidth = (name: string): number => {
    if (!focusedSeries && chartHoveredSeries) {
      return name === chartHoveredSeries ? 3 : 1;
    }
    if (!focusedSeries) return 1.5;
    return name === focusedSeries ? 3 : 1;
  };

  const formatRaw = (seriesName: string, normalizedVal: number): string => {
    const s = normalized.find(ns => ns.name === seriesName);
    if (!s) return normalizedVal.toFixed(2);
    const validValues = s.raw.filter(v => !Number.isNaN(v));
    const min = Math.min(...validValues);
    const max = Math.max(...validValues);
    const raw = min + normalizedVal * (max - min);
    return compactFormatter(s.unit)(raw);
  };

  const STRENGTH_COLORS: Record<string, string> = {
    strong: '#22c55e',
    moderate: '#eab308',
    weak: 'var(--text-muted)',
  };
  const rColor = (r: number) => STRENGTH_COLORS[correlationStrength(r)];

  // Stable legend order: keep original panel order, let visual styling communicate correlation.
  // Re-sorting on hover causes distracting flickering.
  const legendSeries = normalized;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
      {/* Algorithm selector */}
      <div style={{
        display: 'flex', alignItems: 'center', gap: 8,
        padding: '4px 10px', borderBottom: '1px solid var(--border-primary)',
        flexShrink: 0,
      }}>
        <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>Algorithm</span>
        <select
          value={algorithmId}
          onChange={e => setAlgorithmId(e.target.value)}
          style={{
            fontSize: 11, padding: '2px 6px',
            background: 'var(--bg-secondary)', color: 'var(--text-primary)',
            border: '1px solid var(--border-primary)', borderRadius: 4,
            cursor: 'pointer',
          }}
        >
          {CORRELATION_ALGORITHMS.map(a => (
            <option key={a.id} value={a.id}>{a.name}</option>
          ))}
        </select>
        <span style={{ fontSize: 10, color: 'var(--text-muted)', fontStyle: 'italic' }}>
          {algorithm.description}
        </span>
        <span
          style={{ position: 'relative', marginLeft: 'auto', flexShrink: 0 }}
          onMouseEnter={() => setShowInfoPopover(true)}
          onMouseLeave={() => setShowInfoPopover(false)}
        >
          <span style={{ fontSize: 14, cursor: 'help', color: 'var(--text-secondary, #94a3b8)' }}>ⓘ</span>
          {showInfoPopover && (
            <div style={{
              position: 'absolute', right: 0, top: '100%', marginTop: 4, zIndex: 9999,
              background: 'var(--bg-primary, #1a1a2e)', border: '1px solid var(--border-primary, #333)',
              borderRadius: 6, padding: '10px 14px', width: 340,
              boxShadow: '0 8px 24px rgba(0,0,0,0.5)', fontSize: 11, lineHeight: 1.6,
              color: 'var(--text-secondary, #cbd5e1)',
            }}>
              <div style={{ fontWeight: 600, fontSize: 12, color: 'var(--text-primary, #e2e8f0)', marginBottom: 6 }}>Correlation Guide</div>
              <div style={{ marginBottom: 8 }}>
                <span style={{ color: 'var(--text-primary, #e2e8f0)' }}>r value</span> — strength of relationship (−1 to +1). Closer to ±1 = stronger.
              </div>
              <div style={{ fontWeight: 600, fontSize: 11, color: 'var(--text-primary, #e2e8f0)', marginBottom: 4 }}>Algorithms</div>
              <div style={{ marginBottom: 2 }}><span style={{ color: '#22c55e' }}>Pearson</span> — linear (proportional) relationships. Sensitive to outliers.</div>
              <div style={{ marginBottom: 2 }}><span style={{ color: '#22c55e' }}>Spearman</span> — monotonic relationships via ranks. Robust to outliers.</div>
              <div style={{ marginBottom: 8 }}><span style={{ color: '#22c55e' }}>Cross-corr</span> — shifts series in time to find best match. <span style={{ color: 'var(--text-muted)' }}>@+3</span> = A leads B by 3 intervals.</div>
              <div style={{ fontWeight: 600, fontSize: 11, color: 'var(--text-primary, #e2e8f0)', marginBottom: 4 }}>Insight Badges</div>
              <div style={{ marginBottom: 2 }}><span style={{ color: '#22c55e', background: 'rgba(34,197,94,0.15)', padding: '0 4px', borderRadius: 3 }}>strong linear</span> — all algorithms agree.</div>
              <div style={{ marginBottom: 2 }}><span style={{ color: '#eab308', background: 'rgba(234,179,8,0.15)', padding: '0 4px', borderRadius: 3 }}>non-linear</span> — Spearman sees it, Pearson doesn't (e.g. exponential).</div>
              <div style={{ marginBottom: 2 }}><span style={{ color: '#eab308', background: 'rgba(234,179,8,0.15)', padding: '0 4px', borderRadius: 3 }}>lagged</span> — Cross-corr sees it, Pearson doesn't (time-delayed).</div>
              <div><span style={{ color: '#eab308', background: 'rgba(234,179,8,0.15)', padding: '0 4px', borderRadius: 3 }}>outlier-driven</span> — Pearson inflated by extreme values.</div>
            </div>
          )}
        </span>
      </div>
      <div style={{ display: 'flex', flex: 1, minHeight: 0 }}>
      {/* Chart area */}
      <div ref={chartContainerRef} style={{ flex: 1, minHeight: 0 }}>
        <ResponsiveContainer width="100%" height="100%">
          <RLineChart
            data={rechartsData}
            margin={{ top: 10, right: 10, left: 10, bottom: 5 }}
            onMouseMove={(state: Record<string, unknown>) => {
              if (lockedSeries) return;
              const s = state as { activeIndex?: number; activeCoordinate?: { y: number } };
              if (s.activeIndex == null || !s.activeCoordinate) return;
              const container = chartContainerRef.current;
              if (!container) return;
              const h = container.getBoundingClientRect().height;
              // Map cursor Y to normalized 0→1 value (Y axis domain=[0,1], margins top=10 bottom=5)
              const plotH = h - 15;
              if (plotH <= 0) return;
              const yVal = 1 - (s.activeCoordinate.y - 10) / plotH;
              // Find closest series at this data index
              const row = rechartsData[s.activeIndex];
              if (!row) return;
              let closest: string | null = null;
              let minDist = Infinity;
              for (const vs of visibleSeries) {
                const val = row[vs.name];
                if (val == null) continue;
                const dist = Math.abs((val as number) - yVal);
                if (dist < minDist) { minDist = dist; closest = vs.name; }
              }
              if (closest && closest !== chartHoveredSeries) setChartHoveredSeries(closest);
            }}
            onMouseLeave={() => setChartHoveredSeries(null)}
          >
            <CartesianGrid strokeDasharray="3 3" stroke={gridStroke} />
            <XAxis dataKey="name" tick={axisTickStyle} tickLine={axisLineStyle} axisLine={axisLineStyle}
              interval="preserveStartEnd" tickFormatter={formatXTick} />
            <YAxis tick={false} tickLine={false} axisLine={false} domain={[0, 1]} width={5} />
            <Tooltip
              content={(props: Record<string, unknown>) => {
                const { active, payload: rawPayload, label } = props as {
                  active?: boolean;
                  payload?: ReadonlyArray<{ value: number; dataKey: string; color: string }>;
                  label?: string;
                };
                if (!active || !rawPayload?.length) return null;
                const sorted = [...rawPayload].sort((a, b) => (b.value ?? 0) - (a.value ?? 0));
                return (
                  <div style={tooltipStyle}>
                    {label && <div style={tooltipLabelStyle}>{label}</div>}
                    {sorted.map((entry, i) => {
                      const name = entry.dataKey;
                      const isFocused = name === focusedSeries;
                      const r = correlationMap?.get(name);
                      return (
                        <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 6, marginTop: i > 0 ? 1 : 0, opacity: isFocused || !focusedSeries ? 1 : 0.6 }}>
                          <span style={{ width: 8, height: 8, borderRadius: '50%', background: entry.color, flexShrink: 0 }} />
                          <span style={{ fontSize: 11, color: 'var(--text-muted)', flex: 1, maxWidth: 140, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', fontWeight: isFocused ? 600 : 400 }}>
                            {name.length > 20 ? name.slice(0, 19) + '…' : name}
                          </span>
                          <span style={{ fontSize: 12, fontWeight: 600, color: 'var(--text-primary)', fontFamily: "'Share Tech Mono', monospace" }}>
                            {formatRaw(name, entry.value)}
                          </span>
                          {r !== undefined && r !== 1 && focusedSeries && (
                            <span style={{ fontSize: 9, color: rColor(r), fontFamily: "'Share Tech Mono', monospace", minWidth: 32, textAlign: 'right' }}>
                              r={r.toFixed(2)}
                              {algorithmId === 'cross' && (() => {
                                const lag = lagMap.get(name);
                                return lag ? <span style={{ color: 'var(--text-muted)' }}> @{lag > 0 ? '+' : ''}{lag}</span> : null;
                              })()}
                            </span>
                          )}
                        </div>
                      );
                    })}
                  </div>
                );
              }}
              offset={20}
              wrapperStyle={{ zIndex: 1000, pointerEvents: 'none' }}
            />
            {correlatedWindows.map((w, i) => (
              <ReferenceLine key={`cw-${i}`} x={w.startTs} stroke={normalized.find(s => s.name === w.seriesName)?.color ?? '#fff'} strokeOpacity={0.2} strokeDasharray="2 2" />
            ))}
            {visibleSeries.map(s => (
              <Line
                key={s.name}
                type="monotone"
                dataKey={s.name}
                stroke={s.color}
                strokeWidth={getStrokeWidth(s.name)}
                strokeOpacity={getOpacity(s.name)}
                dot={false}
                activeDot={s.name === focusedSeries ? { r: 4, fill: s.color } : false}
                connectNulls
                animationDuration={300}
              />
            ))}
          </RLineChart>
        </ResponsiveContainer>
      </div>

      {/* Legend sidebar — hover to focus, click to hide/show */}
      <div style={{
        width: 260,
        flexShrink: 0,
        borderLeft: '1px solid var(--border-primary)',
        overflowY: 'auto',
        padding: '6px 0',
      }}>
        <div style={{
          borderBottom: '1px solid var(--border-primary)',
          padding: '6px 10px',
          display: 'flex', alignItems: 'center', gap: 6,
          marginBottom: 4,
        }}>
          {lockedSeries ? (
            <>
              <span style={{ fontSize: 10, color: 'var(--text-muted)' }}>
                Locked: <strong style={{ color: 'var(--text-primary)' }}>{lockedSeries}</strong>
              </span>
              <button
                onClick={() => { setLockedSeries(null); setHiddenSeries(new Set()); }}
                style={{
                  fontSize: 9, padding: '1px 6px', marginLeft: 'auto',
                  background: 'rgba(255,255,255,0.08)', color: 'var(--text-secondary)',
                  border: '1px solid var(--border-primary)', borderRadius: 3,
                  cursor: 'pointer',
                }}
              >
                Unlock
              </button>
            </>
          ) : (
            <span style={{ fontSize: 9, color: 'var(--text-muted)', fontStyle: 'italic', lineHeight: '1.5' }}>
              <strong style={{ fontStyle: 'normal', color: 'var(--text-secondary)' }}>Hover in legend to correlate</strong><br />
              Right-click to lock &amp; filter
            </span>
          )}
        </div>
        {legendSeries.map(s => {
          const isFocused = s.name === focusedSeries;
          const isHidden = hiddenSeries.has(s.name);
          const r = correlationMap?.get(s.name);
          const insight = focusedSeries && !isHidden ? insightMap.get(s.name) : null;
          return (
            <div
              key={s.name}
              onMouseEnter={() => setHoveredSeries(s.name)}
              onMouseLeave={() => setHoveredSeries(null)}
              onClick={() => {
                if (lockedSeries === s.name) {
                  setLockedSeries(null);
                } else if (lockedSeries) {
                  // When locked, click toggles visibility
                  setHiddenSeries(prev => {
                    const next = new Set(prev);
                    if (next.has(s.name)) next.delete(s.name);
                    else if (next.size < normalized.length - 1) next.add(s.name);
                    return next;
                  });
                } else {
                  setHiddenSeries(prev => {
                    const next = new Set(prev);
                    if (next.has(s.name)) next.delete(s.name);
                    else if (next.size < normalized.length - 1) next.add(s.name);
                    return next;
                  });
                }
              }}
              onContextMenu={(e) => {
                e.preventDefault();
                setLockedSeries(s.name);
                setContextMenu({ x: e.clientX, y: e.clientY });
              }}
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: 6,
                padding: '3px 10px',
                cursor: 'pointer',
                opacity: isHidden ? 0.25 : (chartHoveredSeries && !focusedSeries && s.name !== chartHoveredSeries) ? 0.4 : 1,
                background: lockedSeries === s.name ? 'rgba(255,255,255,0.08)' : isFocused ? 'rgba(255,255,255,0.04)' : (chartHoveredSeries === s.name && !focusedSeries) ? 'rgba(255,255,255,0.06)' : 'transparent',
                textDecoration: isHidden ? 'line-through' : 'none',
                transition: 'background 0.1s',
              }}
            >
              <span style={{
                width: 14, height: 3, borderRadius: 1, flexShrink: 0,
                background: s.color,
                opacity: isHidden ? 0.3 : getOpacity(s.name),
              }} />
              <span style={{
                fontSize: 11, flex: 1,
                overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                color: isFocused ? 'var(--text-primary)' : 'var(--text-secondary)',
                fontWeight: isFocused ? 600 : 400,
              }}>
                {s.name}
              </span>
              {insight && (
                <span
                  title={insight.detail}
                  style={{
                    fontSize: 8, flexShrink: 0,
                    padding: '1px 4px', borderRadius: 3,
                    background: insight.level === 'interesting' ? 'rgba(234, 179, 8, 0.2)' : 'rgba(34, 197, 94, 0.15)',
                    color: insight.level === 'interesting' ? '#eab308' : '#22c55e',
                    whiteSpace: 'nowrap',
                    border: `1px solid ${insight.level === 'interesting' ? 'rgba(234, 179, 8, 0.3)' : 'rgba(34, 197, 94, 0.2)'}`,
                  }}
                >
                  {insight.label}
                </span>
              )}
              {r !== undefined && r !== 1 && focusedSeries && !isHidden && (
                <span style={{
                  fontSize: 9, fontFamily: "'Share Tech Mono', monospace",
                  color: rColor(r), flexShrink: 0,
                }}>
                  {r >= 0 ? '+' : ''}{r.toFixed(2)}
                  {algorithmId === 'cross' && (() => {
                    const lag = lagMap.get(s.name);
                    return lag ? <span style={{ color: 'var(--text-muted)', marginLeft: 2 }}>@{lag > 0 ? '+' : ''}{lag}</span> : null;
                  })()}
                </span>
              )}
            </div>
          );
        })}
      </div>

      {/* Right-click context menu for filtering by insight/strength */}
      {contextMenu && lockedSeries && (() => {
        // Compute counts for each filter category
        const insightCounts = new Map<string, string[]>();
        const strengthCounts = new Map<string, string[]>();
        for (const s of normalized) {
          if (s.name === lockedSeries) continue;
          const insight = insightMap.get(s.name);
          if (insight) {
            const list = insightCounts.get(insight.label) ?? [];
            list.push(s.name);
            insightCounts.set(insight.label, list);
          }
          const r = correlationMap?.get(s.name) ?? 0;
          for (const sf of STRENGTH_FILTERS) {
            if (sf.test(r)) {
              const list = strengthCounts.get(sf.id) ?? [];
              list.push(s.name);
              strengthCounts.set(sf.id, list);
            }
          }
        }

        return (
          <>
            {/* Backdrop to close menu */}
            <div
              style={{ position: 'fixed', inset: 0, zIndex: 9998 }}
              onClick={() => setContextMenu(null)}
              onContextMenu={(e) => { e.preventDefault(); setContextMenu(null); }}
            />
            <div style={{
              position: 'fixed',
              left: Math.min(contextMenu.x, window.innerWidth - 230),
              top: Math.min(contextMenu.y, window.innerHeight - 400),
              zIndex: 9999,
              background: 'var(--bg-primary, #1a1a2e)',
              border: '1px solid var(--border-primary)',
              borderRadius: 6,
              padding: '6px 0',
              minWidth: 200,
              boxShadow: '0 8px 24px rgba(0,0,0,0.5)',
            }}>
              <div style={{ padding: '4px 12px 6px', fontSize: 10, color: 'var(--text-muted)', borderBottom: '1px solid var(--border-primary)' }}>
                Filter relative to <strong style={{ color: 'var(--text-primary)' }}>{lockedSeries}</strong>
              </div>

              {/* Insight type filters */}
              <div style={{ padding: '6px 12px 2px', fontSize: 9, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: 1 }}>
                Relationship Type
              </div>
              {INSIGHT_FILTERS.map(f => {
                const names = insightCounts.get(f.label) ?? [];
                if (names.length === 0) return null;
                return (
                  <button
                    key={f.label}
                    onClick={() => {
                      const toShow = new Set(names);
                      toShow.add(lockedSeries!);
                      setHiddenSeries(new Set(normalized.filter(s => !toShow.has(s.name)).map(s => s.name)));
                      setContextMenu(null);
                    }}
                    style={{
                      display: 'flex', alignItems: 'center', gap: 8,
                      width: '100%', padding: '5px 12px',
                      background: 'transparent', border: 'none', cursor: 'pointer',
                      color: 'var(--text-secondary)', fontSize: 11, textAlign: 'left',
                    }}
                    onMouseEnter={e => (e.currentTarget.style.background = 'rgba(255,255,255,0.06)')}
                    onMouseLeave={e => (e.currentTarget.style.background = 'transparent')}
                  >
                    <span style={{
                      width: 8, height: 8, borderRadius: 2,
                      background: f.color, opacity: 0.8, flexShrink: 0,
                    }} />
                    <span style={{ flex: 1 }}>{f.label}</span>
                    <span style={{ fontSize: 9, color: 'var(--text-muted)' }}>{names.length}</span>
                  </button>
                );
              })}

              {/* Strength filters */}
              <div style={{ padding: '8px 12px 2px', fontSize: 9, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: 1, borderTop: '1px solid var(--border-primary)', marginTop: 4 }}>
                Correlation Strength
              </div>
              {STRENGTH_FILTERS.map(f => {
                const names = strengthCounts.get(f.id) ?? [];
                if (names.length === 0) return null;
                return (
                  <button
                    key={f.id}
                    onClick={() => {
                      const toShow = new Set(names);
                      toShow.add(lockedSeries!);
                      setHiddenSeries(new Set(normalized.filter(s => !toShow.has(s.name)).map(s => s.name)));
                      setContextMenu(null);
                    }}
                    style={{
                      display: 'flex', alignItems: 'center', gap: 8,
                      width: '100%', padding: '5px 12px',
                      background: 'transparent', border: 'none', cursor: 'pointer',
                      color: 'var(--text-secondary)', fontSize: 11, textAlign: 'left',
                    }}
                    onMouseEnter={e => (e.currentTarget.style.background = 'rgba(255,255,255,0.06)')}
                    onMouseLeave={e => (e.currentTarget.style.background = 'transparent')}
                  >
                    <span style={{
                      width: 8, height: 8, borderRadius: '50%',
                      background: f.color, opacity: 0.8, flexShrink: 0,
                    }} />
                    <span style={{ flex: 1 }}>{f.label}</span>
                    <span style={{ fontSize: 9, color: 'var(--text-muted)' }}>{names.length}</span>
                  </button>
                );
              })}

              {/* Show all / reset */}
              <div style={{ borderTop: '1px solid var(--border-primary)', marginTop: 4, paddingTop: 4 }}>
                <button
                  onClick={() => { setHiddenSeries(new Set()); setContextMenu(null); }}
                  style={{
                    display: 'flex', alignItems: 'center', gap: 8,
                    width: '100%', padding: '5px 12px',
                    background: 'transparent', border: 'none', cursor: 'pointer',
                    color: 'var(--text-secondary)', fontSize: 11, textAlign: 'left',
                  }}
                  onMouseEnter={e => (e.currentTarget.style.background = 'rgba(255,255,255,0.06)')}
                  onMouseLeave={e => (e.currentTarget.style.background = 'transparent')}
                >
                  Show all
                </button>
              </div>
            </div>
          </>
        );
      })()}

      </div>
    </div>
  );
};

// ═══════════════════════════════════════════════════════════════════════════
// ChartRenderer — single component that dispatches to the right 2D chart
// ═══════════════════════════════════════════════════════════════════════════

export interface ChartRendererProps extends CrosshairProps {
  chartType: ChartType;
  data: ChartDataPoint[];
  groupedData: GroupedChartData[];
  orientation?: 'horizontal' | 'vertical';
  fullHeight?: boolean;
  unit?: string;
  color?: string;
  onDrillDown?: (e: DrillDownEvent) => void;
  drillIntoQuery?: string;
  /** When set, enables dual Y-axis mode for area/line charts (first col = left, second col = right) */
  valueColumns?: string[];
}

export const ChartRenderer: React.FC<ChartRendererProps> = ({
  chartType, data, groupedData, orientation, fullHeight, unit, color, onDrillDown, drillIntoQuery,
  hoveredTimestamp, onTimestampHover, correlationValues, currentPanelName, isHoveredPanel, valueColumns,
}) => {
  // Dual-axis mode: area/line with exactly 2 value columns
  if ((chartType === 'area' || chartType === 'line') && valueColumns && valueColumns.length === 2 && groupedData.length > 0) {
    return <DualAxisAreaChart2D data={groupedData} valueColumns={valueColumns} fullHeight={fullHeight} onDrillDown={onDrillDown} drillIntoQuery={drillIntoQuery} hoveredTimestamp={hoveredTimestamp} onTimestampHover={onTimestampHover} correlationValues={correlationValues} currentPanelName={currentPanelName} isHoveredPanel={isHoveredPanel} />;
  }
  switch (chartType) {
    case 'line':
      return <LineChart2D data={data} fullHeight={fullHeight} onDrillDown={onDrillDown} unit={unit} color={color} drillIntoQuery={drillIntoQuery} hoveredTimestamp={hoveredTimestamp} onTimestampHover={onTimestampHover} correlationValues={correlationValues} currentPanelName={currentPanelName} isHoveredPanel={isHoveredPanel} />;
    case 'pie':
      return <PieChart2D data={data} fullHeight={fullHeight} onDrillDown={onDrillDown} drillIntoQuery={drillIntoQuery} unit={unit} />;
    case 'area':
      return <AreaChart2D data={data} fullHeight={fullHeight} onDrillDown={onDrillDown} unit={unit} color={color} drillIntoQuery={drillIntoQuery} hoveredTimestamp={hoveredTimestamp} onTimestampHover={onTimestampHover} correlationValues={correlationValues} currentPanelName={currentPanelName} isHoveredPanel={isHoveredPanel} />;
    case 'grouped_bar':
      return <GroupedBarChart2D data={groupedData} orientation={orientation} onDrillDown={onDrillDown} unit={unit} drillIntoQuery={drillIntoQuery} />;
    case 'stacked_bar':
      return <StackedBarChart2D data={groupedData} orientation={orientation} onDrillDown={onDrillDown} unit={unit} drillIntoQuery={drillIntoQuery} />;
    case 'grouped_line':
      return <GroupedLineChart2D data={groupedData} onDrillDown={onDrillDown} unit={unit} drillIntoQuery={drillIntoQuery} hoveredTimestamp={hoveredTimestamp} onTimestampHover={onTimestampHover} correlationValues={correlationValues} currentPanelName={currentPanelName} isHoveredPanel={isHoveredPanel} />;
    default:
      return <BarChart2D data={data} fullHeight={fullHeight} onDrillDown={onDrillDown} unit={unit} drillIntoQuery={drillIntoQuery} />;
  }
};
