/**
 * Shared 2D chart components used by both QueryExplorer and DashboardViewer.
 *
 * Uses recharts for all chart types for consistency, responsiveness,
 * and built-in interactivity (tooltips, hover, animations).
 */

import React, { useState } from 'react';
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
} from 'recharts';
import type { ChartType } from './presetQueries';

// ─── Constants ───

export const CHART_COLORS = [
  '#6366f1','#8b5cf6','#a855f7','#d946ef','#ec4899',
  '#f43f5e','#f97316','#eab308','#84cc16','#22c55e',
  '#14b8a6','#06b6d4','#0ea5e9','#3b82f6',
];

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
}

export interface ChartConfig {
  type: ChartType;
  labelColumn: string;
  valueColumn: string;
  /** Multiple value columns for multi-series charts (e.g. values=col1,col2,col3). When set, each column becomes a separate series. */
  valueColumns?: string[];
  groupColumn?: string;
  orientation?: 'horizontal' | 'vertical';
  visualization: '2d' | '3d';
  title?: string;
  description?: string;
  /** Unit suffix for value axis ticks, e.g. 'ms', 'MB', '%'. Parsed from @chart unit=... */
  unit?: string;
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

export function formatCell(v: unknown): string {
  if (v == null) return '—';
  if (typeof v === 'number') return Number.isInteger(v) ? v.toLocaleString() : v.toLocaleString(undefined, { maximumFractionDigits: 2 });
  if (Array.isArray(v)) return v.join(', ');
  return String(v);
}

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

export function parseChartDirective(sql: string): Partial<ChartConfig> | null {
  const cm = sql.match(/--\s*@chart:\s*(.+)/i);
  if (!cm) return null;
  const d = cm[1];
  const cfg: Partial<ChartConfig> = {};
  const t = d.match(/type=(\w+)/i); if (t) cfg.type = t[1] as ChartType;
  const l = d.match(/labels=(\w+)/i); if (l) cfg.labelColumn = l[1];
  const v = d.match(/values=([\w,]+)/i);
  if (v) {
    const cols = v[1].split(',').filter(Boolean);
    cfg.valueColumn = cols[0];
    if (cols.length > 1) cfg.valueColumns = cols;
  }
  const g = d.match(/group=(\w+)/i); if (g) cfg.groupColumn = g[1];
  const o = d.match(/orientation=(\w+)/i);
  if (o) cfg.orientation = o[1].toLowerCase() === 'vertical' || o[1].toLowerCase() === 'v' ? 'vertical' : 'horizontal';
  const s = d.match(/style=(\w+)/i); if (s) cfg.visualization = s[1] as '2d' | '3d';
  const u = d.match(/unit=(\S+)/i); if (u) cfg.unit = u[1];
  const mm = sql.match(/--\s*@meta:\s*(.+)/i);
  if (mm) {
    const ti = mm[1].match(/title='([^']+)'/); if (ti) cfg.title = ti[1];
    const de = mm[1].match(/description='([^']+)'/); if (de) cfg.description = de[1];
  }
  return Object.keys(cfg).length > 0 ? cfg : null;
}

/**
 * Build GroupedChartData from multi-column results (no group column needed).
 * Each valueColumn becomes a separate series — like ClickHouse native dashboards.
 */
export function buildMultiColumnGrouped(
  rows: Record<string, unknown>[],
  labelColumn: string,
  valueColumns: string[],
): GroupedChartData[] {
  return rows.map(row => ({
    label: formatCell(row[labelColumn]),
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
): ChartDataPoint[] {
  if (rows.length === 0) return [];
  const lblCol = labelCol || columns.find(c => !rows.some(r => isNumericValue(r[c]))) || columns[0];
  const valCol = valueCol || columns.find(c => c !== lblCol && rows.some(r => isNumericValue(r[c]))) || columns[1];
  if (!lblCol || !valCol) return [];
  const sliced = maxRows ? rows.slice(0, maxRows) : rows;
  return sliced.map((r, i) => ({
    label: formatCell(r[lblCol]),
    value: extractNumeric(r[valCol]),
    color: CHART_COLORS[i % CHART_COLORS.length],
  }));
}

/**
 * Build GroupedChartData from rows using either multi-column mode (valueColumns)
 * or standard group-column mode (groupColumn).
 */
export function buildGroupedChartData(
  rows: Record<string, unknown>[],
  labelColumn: string,
  valueColumn: string,
  groupColumn?: string,
  valueColumns?: string[],
  maxRows?: number,
): GroupedChartData[] {
  // Multi-column mode: each valueColumn becomes a series (no group column needed)
  if (valueColumns && valueColumns.length > 1) {
    const data = buildMultiColumnGrouped(rows, labelColumn, valueColumns);
    return maxRows ? data.slice(0, maxRows * 2) : data;
  }
  // Standard group-column mode
  if (!groupColumn) return [];
  const grouped = new Map<string, Map<string, number>>();
  for (const row of rows) {
    const label = formatCell(row[labelColumn]);
    const group = formatCell(row[groupColumn]);
    const value = extractNumeric(row[valueColumn]);
    if (!grouped.has(label)) grouped.set(label, new Map());
    grouped.get(label)!.set(group, value);
  }
  const allGroups = [...new Set(rows.map(r => formatCell(r[groupColumn])))];
  return Array.from(grouped.entries()).map(([label, groups]) => ({
    label,
    groups: allGroups.map((groupName, i) => ({
      name: groupName,
      value: groups.get(groupName) || 0,
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

// ═══════════════════════════════════════════════════════════════════════════
// Custom Tooltip
// ═══════════════════════════════════════════════════════════════════════════

export const AnalyticsTooltip: React.FC<{
  active?: boolean;
  payload?: Array<{ value: number; name?: string; color?: string; payload?: Record<string, unknown> }>;
  label?: string;
  drillIntoQuery?: string;
}> = ({ active, payload, label, drillIntoQuery }) => {
  if (!active || !payload?.length) return null;
  return (
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
      {drillIntoQuery && (
        <div style={drillHintStyle}>Click to drill into {drillIntoQuery}</div>
      )}
    </div>
  );
};

// Pie-specific tooltip
const PieTooltip: React.FC<{
  active?: boolean;
  payload?: Array<{ payload: { label: string; value: number; pct: number }; color?: string }>;
  drillIntoQuery?: string;
}> = ({ active, payload, drillIntoQuery }) => {
  if (!active || !payload?.length) return null;
  const d = payload[0].payload;
  return (
    <div style={tooltipStyle}>
      <div style={tooltipLabelStyle}>{d.label}</div>
      <div style={tooltipValueStyle}>
        {d.value.toLocaleString(undefined, { maximumFractionDigits: 2 })}
        <span style={{ fontSize: 11, fontWeight: 400, color: 'var(--text-muted)', marginLeft: 6 }}>
          ({d.pct.toFixed(1)}%)
        </span>
      </div>
      {drillIntoQuery && (
        <div style={drillHintStyle}>Click to drill into {drillIntoQuery}</div>
      )}
    </div>
  );
};

// ═══════════════════════════════════════════════════════════════════════════
// 2D Charts — all using recharts
// ═══════════════════════════════════════════════════════════════════════════

export const BarChart2D: React.FC<{ data: ChartDataPoint[]; fullHeight?: boolean; onDrillDown?: (e: DrillDownEvent) => void; unit?: string; drillIntoQuery?: string }> = ({ data, fullHeight, onDrillDown, unit, drillIntoQuery }) => {
  if (!data.length) return null;
  const rechartsData = data.map(d => ({ name: d.label, value: d.value, fill: d.color }));
  const defaultHeight = Math.max(200, data.length * 28 + 40);
  // Use horizontal layout (bars going right) for readability with labels
  return (
    <ResponsiveContainer width="100%" height={fullHeight ? '100%' : defaultHeight}>
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

export const LineChart2D: React.FC<{ data: ChartDataPoint[]; fullHeight?: boolean; onDrillDown?: (e: DrillDownEvent) => void; unit?: string; drillIntoQuery?: string }> = ({ data, fullHeight, onDrillDown, unit, drillIntoQuery }) => {
  if (data.length < 2) return null;
  const rechartsData = data.map(d => ({ name: d.label, value: d.value }));
  return (
    <ResponsiveContainer width="100%" height={fullHeight ? '100%' : 280}>
      <RLineChart data={rechartsData} margin={{ top: 5, right: 20, left: 10, bottom: 5 }}>
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
        <Tooltip content={<AnalyticsTooltip drillIntoQuery={drillIntoQuery} />} />
        <Line
          type="monotone"
          dataKey="value"
          stroke="#6366f1"
          strokeWidth={2}
          dot={false}
          activeDot={{
            r: 4, strokeWidth: 2, fill: '#6366f1',
            cursor: onDrillDown ? 'pointer' : undefined,
            onClick: onDrillDown ? (_: unknown, payload: { payload?: { name: string; value: number } }) => {
              if (payload?.payload) onDrillDown({ label: payload.payload.name, value: payload.payload.value });
            } : undefined,
          }}
          animationDuration={300}
        />
      </RLineChart>
    </ResponsiveContainer>
  );
};

export const PieChart2D: React.FC<{ data: ChartDataPoint[]; fullHeight?: boolean; onDrillDown?: (e: DrillDownEvent) => void; drillIntoQuery?: string }> = ({ data, fullHeight, onDrillDown, drillIntoQuery }) => {
  if (!data.length) return null;
  const total = data.reduce((s, d) => s + d.value, 0);
  const pieData = data.map(d => ({
    label: d.label,
    value: d.value,
    pct: total > 0 ? (d.value / total) * 100 : 0,
    color: d.color,
  }));

  return (
    <ResponsiveContainer width="100%" height={fullHeight ? '100%' : 300}>
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
        <Tooltip content={<PieTooltip drillIntoQuery={drillIntoQuery} />} />
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

export const AreaChart2D: React.FC<{ data: ChartDataPoint[]; fullHeight?: boolean; onDrillDown?: (e: DrillDownEvent) => void; unit?: string; drillIntoQuery?: string }> = ({ data, fullHeight, onDrillDown, unit, drillIntoQuery }) => {
  if (data.length < 2) return null;
  const rechartsData = data.map(d => ({ name: d.label, value: d.value }));
  return (
    <ResponsiveContainer width="100%" height={fullHeight ? '100%' : 280}>
      <RAreaChart data={rechartsData} margin={{ top: 5, right: 20, left: 10, bottom: 5 }}>
        <defs>
          <linearGradient id="analyticsAreaGrad" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="#6366f1" stopOpacity={0.25} />
            <stop offset="100%" stopColor="#6366f1" stopOpacity={0.02} />
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
        <Tooltip content={<AnalyticsTooltip drillIntoQuery={drillIntoQuery} />} />
        <Area
          type="monotone"
          dataKey="value"
          stroke="#6366f1"
          strokeWidth={2}
          fill="url(#analyticsAreaGrad)"
          dot={false}
          activeDot={{
            r: 4, strokeWidth: 2, fill: '#6366f1',
            cursor: onDrillDown ? 'pointer' : undefined,
            onClick: onDrillDown ? (_: unknown, payload: { payload?: { name: string; value: number } }) => {
              if (payload?.payload) onDrillDown({ label: payload.payload.name, value: payload.payload.value });
            } : undefined,
          }}
          animationDuration={300}
        />
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

export const GroupedLineChart2D: React.FC<{ data: GroupedChartData[]; onDrillDown?: (e: DrillDownEvent) => void; unit?: string; drillIntoQuery?: string }> = ({ data, onDrillDown, unit, drillIntoQuery }) => {
  const [hoveredLine, setHoveredLine] = useState<string | null>(null);
  if (!data.length) return null;
  const { rows, groupNames, colorMap } = flattenGrouped(data);

  return (
    <ResponsiveContainer width="100%" height={320}>
      <RLineChart data={rows} margin={{ top: 10, right: 20, left: 10, bottom: 5 }}>
        <CartesianGrid strokeDasharray="3 3" stroke={gridStroke} />
        <XAxis dataKey="name" tick={axisTickStyle} tickLine={axisLineStyle} axisLine={axisLineStyle}
          interval="preserveStartEnd"
          tickFormatter={formatXTick} />
        <YAxis tick={axisTickStyle} tickLine={axisLineStyle} axisLine={axisLineStyle} tickFormatter={compactFormatter(unit)} width={50} />
        <Tooltip content={<AnalyticsTooltip drillIntoQuery={drillIntoQuery} />} />
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
            activeDot={{
              r: 4, strokeWidth: 2,
              cursor: onDrillDown ? 'pointer' : undefined,
              onClick: onDrillDown ? (_: unknown, payload: { payload?: { name: string } }) => {
                if (payload?.payload) onDrillDown({ label: payload.payload.name, value: 0 });
              } : undefined,
            }}
            animationDuration={300}
          />
        ))}
      </RLineChart>
    </ResponsiveContainer>
  );
};

// ═══════════════════════════════════════════════════════════════════════════
// ChartRenderer — single component that dispatches to the right 2D chart
// ═══════════════════════════════════════════════════════════════════════════

export interface ChartRendererProps {
  chartType: ChartType;
  data: ChartDataPoint[];
  groupedData: GroupedChartData[];
  orientation?: 'horizontal' | 'vertical';
  fullHeight?: boolean;
  unit?: string;
  onDrillDown?: (e: DrillDownEvent) => void;
  drillIntoQuery?: string;
}

export const ChartRenderer: React.FC<ChartRendererProps> = ({
  chartType, data, groupedData, orientation, fullHeight, unit, onDrillDown, drillIntoQuery,
}) => {
  switch (chartType) {
    case 'line':
      return <LineChart2D data={data} fullHeight={fullHeight} onDrillDown={onDrillDown} unit={unit} drillIntoQuery={drillIntoQuery} />;
    case 'pie':
      return <PieChart2D data={data} fullHeight={fullHeight} onDrillDown={onDrillDown} drillIntoQuery={drillIntoQuery} />;
    case 'area':
      return <AreaChart2D data={data} fullHeight={fullHeight} onDrillDown={onDrillDown} unit={unit} drillIntoQuery={drillIntoQuery} />;
    case 'grouped_bar':
      return <GroupedBarChart2D data={groupedData} orientation={orientation} onDrillDown={onDrillDown} unit={unit} drillIntoQuery={drillIntoQuery} />;
    case 'stacked_bar':
      return <StackedBarChart2D data={groupedData} orientation={orientation} onDrillDown={onDrillDown} unit={unit} drillIntoQuery={drillIntoQuery} />;
    case 'grouped_line':
      return <GroupedLineChart2D data={groupedData} onDrillDown={onDrillDown} unit={unit} drillIntoQuery={drillIntoQuery} />;
    default:
      return <BarChart2D data={data} fullHeight={fullHeight} onDrillDown={onDrillDown} unit={unit} drillIntoQuery={drillIntoQuery} />;
  }
};
