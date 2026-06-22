import type { RadarCellStyle, RadarAxisRange } from './metaLanguage';

const AXIS_ORDER = ['time', 'memory', 'cpu', 'io', 'scan'];

export const RADAR_COLORS = {
  low: '#3fb950',
  moderate: '#d29922',
  high: '#f85149',
  neutral: '#8b949e',
};

export interface BuiltRadar {
  values: number[];
  rawValues: string[];
  labels: string[];
  color: string;
  tooltip: string;
}

export interface RadarChartConfig {
  labelColumn?: string;
  groupByColumn?: string;
  valueColumn?: string;
  profile?: string;
  axes?: Record<string, string>;
  ranges?: Record<string, RadarAxisRange>;
  transforms?: Record<string, string>;
  valuesColumn?: string;
  labelsColumn?: string;
  colorByColumn?: string;
  color?: string;
}

export interface RadarChartItem extends BuiltRadar {
  label: string;
}

export interface RadarChartPlot extends BuiltRadar {
  rowCount: number;
}

export interface RadarLayoutPoint {
  x: number;
  y: number;
}

export interface RadarLabelLayout extends RadarLayoutPoint {
  label: string;
  anchor: 'start' | 'middle' | 'end';
  baseline: 'hanging' | 'middle' | 'auto';
}

export interface RadarShapeLayout {
  viewBox: string;
  center: RadarLayoutPoint;
  radius: number;
  spokes: RadarLayoutPoint[];
  labels: RadarLabelLayout[];
  polygonPoints: string;
}

export function clamp01(value: number): number {
  if (!Number.isFinite(value)) return 0;
  return Math.max(0, Math.min(1, value));
}

export function shortRadarLabel(label: string): string {
  const normalized = label.trim().toLowerCase();
  if (normalized === 'memory') return 'MEM';
  if (normalized === 'io') return 'I/O';
  return normalized.slice(0, 4).toUpperCase();
}

export function radarShapeLayout(values: number[], labels: string[]): RadarShapeLayout {
  const cleanValues = values.length > 0 ? values.map(clamp01) : [0, 0, 0];
  const cleanLabels = labels.length === cleanValues.length ? labels : cleanValues.map((_, i) => `axis ${i + 1}`);
  const center = { x: 40, y: 40 };
  const radius = 18;
  const inner = 5;
  const labelRadius = 31;

  const pointsFor = (distance: number): RadarLayoutPoint[] => cleanValues.map((_, i) => {
    const angle = (-90 + i * 360 / cleanValues.length) * Math.PI / 180;
    return {
      x: center.x + Math.cos(angle) * distance,
      y: center.y + Math.sin(angle) * distance,
    };
  });

  const spokes = pointsFor(radius);
  const labelsLayout: RadarLabelLayout[] = pointsFor(labelRadius).map((point, i) => {
    const anchor: RadarLabelLayout['anchor'] = Math.abs(point.x - center.x) < 1 ? 'middle' : point.x > center.x ? 'start' : 'end';
    const baseline: RadarLabelLayout['baseline'] = Math.abs(point.y - center.y) < 1 ? 'middle' : point.y > center.y ? 'hanging' : 'auto';
    return {
      ...point,
      label: shortRadarLabel(cleanLabels[i]),
      anchor,
      baseline,
    };
  });
  const polygonPoints = cleanValues.map((score, i) => {
    const angle = (-90 + i * 360 / cleanValues.length) * Math.PI / 180;
    const r = inner + score * (radius - inner);
    return `${center.x + Math.cos(angle) * r},${center.y + Math.sin(angle) * r}`;
  }).join(' ');

  return {
    viewBox: '-8 -8 96 96',
    center,
    radius,
    spokes,
    labels: labelsLayout,
    polygonPoints,
  };
}

export function parseRangeNumber(raw: string): number {
  const value = raw.trim();
  const match = value.match(/^(-?\d+(?:\.\d+)?)([a-zA-Z]+)?$/);
  if (!match) return Number(value);
  const n = Number(match[1]);
  const unit = match[2]?.toLowerCase();
  switch (unit) {
    case 'k': return n * 1_000;
    case 'm': return n * 1_000_000;
    case 'b': return n * 1_000_000_000;
    case 'ki': return n * 1024;
    case 'mi': return n * 1024 * 1024;
    case 'gi': return n * 1024 * 1024 * 1024;
    case 'ti': return n * 1024 * 1024 * 1024 * 1024;
    default: return n;
  }
}

export function normalizeRadarValue(value: number, range: RadarAxisRange | undefined, transform: string | undefined): number {
  if (!range) return clamp01(value);
  const low = parseRangeNumber(range.low);
  const high = parseRangeNumber(range.high);
  if (!Number.isFinite(low) || !Number.isFinite(high) || high <= low) return 0;
  if (transform === 'log') {
    const safeLow = Math.max(1, low);
    const safeHigh = Math.max(safeLow + 1, high);
    if (value <= 0) return 0;
    return clamp01((Math.log10(Math.max(value, safeLow)) - Math.log10(safeLow)) / (Math.log10(safeHigh) - Math.log10(safeLow)));
  }
  return clamp01((value - low) / (high - low));
}

export function profileTransform(profile: string | undefined, axis: string): string {
  if (profile === 'query_pressure') {
    return axis === 'scan' ? 'linear' : 'log';
  }
  return 'linear';
}

export function pressureLevel(
  row: Record<string, unknown>,
  values: Record<string, number>,
  profile?: string,
): 'low' | 'moderate' | 'high' {
  if (profile !== 'query_pressure') {
    const max = Math.max(0, ...Object.values(values));
    if (max >= 0.85) return 'high';
    if (max >= 0.4) return 'moderate';
    return 'low';
  }

  const type = String(row.type ?? '').toLowerCase();
  const kind = String(row.query_kind ?? '').toUpperCase();
  if (row.exception || type === 'exceptionwhileprocessing' || type === 'error') return 'high';
  if (kind === 'INSERT') return 'moderate';

  const resourceValues = [values.time, values.memory, values.cpu, values.io].filter((v): v is number => typeof v === 'number');
  const resourceMax = Math.max(0, ...resourceValues);
  const elevatedResources = resourceValues.filter(value => value >= 0.65).length;
  const scan = values.scan ?? 0;

  if (elevatedResources >= 2 || resourceMax >= 0.9 || (scan >= 0.9 && elevatedResources >= 1)) return 'high';
  if (elevatedResources === 1 || resourceMax >= 0.45 || scan >= 0.75) return 'moderate';
  return 'low';
}

export function toRadarNumber(value: unknown): number {
  if (typeof value === 'number') return value;
  if (typeof value === 'string' && value.trim() !== '') return Number(value);
  return 0;
}

function formatRadarValue(value: unknown): string {
  if (value == null) return '';
  if (typeof value === 'number') return Number.isInteger(value) ? value.toString() : value.toFixed(2);
  if (Array.isArray(value)) return `[${value.map(formatRadarValue).join(', ')}]`;
  return String(value);
}

function formatRadarRawValue(axis: string, value: unknown): string {
  const n = toRadarNumber(value);
  if (!Number.isFinite(n)) return formatRadarValue(value);
  if (axis === 'time' || axis === 'duration') {
    if (n >= 60_000) return `${(n / 60_000).toFixed(1)} min`;
    if (n >= 1_000) return `${(n / 1_000).toFixed(2)} s`;
    return `${Number(n.toFixed(2))} ms`;
  }
  if ((axis === 'cpu' || axis === 'memory') && n >= 0 && n <= 1) {
    return `${Number((n * 100).toFixed(2))}%`;
  }
  if (axis === 'memory' || axis === 'io' || axis === 'network' || axis === 'bytes') {
    if (n === 0) return '0 B';
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    const idx = Math.max(0, Math.min(Math.floor(Math.log(Math.abs(n)) / Math.log(1024)), units.length - 1));
    return `${Number((n / Math.pow(1024, idx)).toFixed(2))} ${units[idx]}`;
  }
  if (axis === 'scan') return n <= 1 ? `${Number((n * 100).toFixed(2))}%` : formatRadarValue(value);
  return formatRadarValue(value);
}

function rowArray(value: unknown): number[] {
  if (Array.isArray(value)) return value.map(toRadarNumber).map(clamp01);
  if (typeof value === 'string') {
    try {
      const parsed = JSON.parse(value);
      if (Array.isArray(parsed)) return parsed.map(toRadarNumber).map(clamp01);
    } catch {
      return [];
    }
  }
  return [];
}

function rowLabels(value: unknown, count: number): string[] {
  if (Array.isArray(value)) return value.map(String).slice(0, count);
  if (typeof value === 'string') {
    try {
      const parsed = JSON.parse(value);
      if (Array.isArray(parsed)) return parsed.map(String).slice(0, count);
    } catch {
      return [];
    }
  }
  return [];
}

export function colorForScore(score: number): string {
  if (score >= 0.85) return RADAR_COLORS.high;
  if (score >= 0.4) return RADAR_COLORS.moderate;
  if (score >= 0.15) return RADAR_COLORS.low;
  return RADAR_COLORS.neutral;
}

export function radarDisplayColumn(style: RadarCellStyle): string | undefined {
  return style.radarColumn ?? style.column;
}

export function buildRadar(style: RadarCellStyle, row: Record<string, unknown>): BuiltRadar {
  if (style.column) {
    const values = rowArray(row[style.column]);
    const labels = style.labels ? rowLabels(row[style.labels], values.length) : [];
    const colorScore = style.colorBy ? clamp01(toRadarNumber(row[style.colorBy])) : Math.max(0, ...values);
    const axisLabels = labels.length === values.length ? labels : values.map((_, i) => `axis ${i + 1}`);
    return {
      values,
      rawValues: values.map(formatRadarValue),
      labels: axisLabels,
      color: colorForScore(colorScore),
      tooltip: axisLabels.map((label, i) => `${label}: ${values[i].toFixed(2)}`).join('\n'),
    };
  }

  const axes = style.axes ?? {};
  const axisNames = AXIS_ORDER.filter(axis => axes[axis]).concat(Object.keys(axes).filter(axis => !AXIS_ORDER.includes(axis)));
  const byAxis: Record<string, number> = {};
  for (const axis of axisNames) {
    const source = axes[axis];
    const raw = toRadarNumber(row[source]);
    const transform = style.transforms?.[axis] ?? profileTransform(style.profile, axis);
    byAxis[axis] = normalizeRadarValue(raw, style.ranges?.[axis], transform);
  }

  const values = axisNames.map(axis => byAxis[axis]);
  const rawValues = axisNames.map(axis => formatRadarRawValue(axis, row[axes[axis]]));
  const level = style.color === 'profile_level' ? pressureLevel(row, byAxis, style.profile) : undefined;
  const color = level ? RADAR_COLORS[level] : colorForScore(Math.max(0, ...values));
  const tooltip = axisNames.map((axis, i) => {
    const source = axes[axis];
    return `${axis}: ${values[i].toFixed(2)} (${formatRadarRawValue(axis, row[source])})`;
  }).join('\n');
  return { values, rawValues, labels: axisNames, color, tooltip };
}

export function buildRadarChartItems(
  config: RadarChartConfig,
  rows: Record<string, unknown>[],
  maxRows = 60,
): RadarChartItem[] {
  const hasArrayValues = !!config.valuesColumn;
  const hasSyntheticAxes = !!config.axes && Object.keys(config.axes).length > 0;
  if (!hasArrayValues && !hasSyntheticAxes) return [];

  const style: RadarCellStyle = hasArrayValues
    ? {
      type: 'radar',
      column: config.valuesColumn!,
      labels: config.labelsColumn,
      colorBy: config.colorByColumn,
    }
    : {
      type: 'radar',
      radarColumn: '__radar_chart__',
      profile: config.profile,
      axes: config.axes,
      ranges: config.ranges,
      transforms: config.transforms,
      color: config.color,
    };

  return rows.slice(0, maxRows).map((row, i) => {
    const fallbackLabel = row.short_id ?? row.query_id ?? row.id ?? `row ${i + 1}`;
    const labelValue = config.labelColumn ? row[config.labelColumn] : row[config.groupByColumn ?? ''];
    const label = formatRadarValue(labelValue ?? fallbackLabel);
    return { label, ...buildRadar(style, row) };
  }).filter(item => item.values.length > 0);
}

export function buildRadarChartPlot(
  config: RadarChartConfig,
  rows: Record<string, unknown>[],
): RadarChartPlot | null {
  const items = buildRadarChartItems(config, rows, rows.length);
  if (items.length !== 1) return null;

  const item = items[0];
  const tooltip = [
    `rows: 1`,
    ...item.labels.map((label, i) => `${label}: ${item.values[i].toFixed(2)}`),
  ].join('\n');

  return {
    values: item.values,
    rawValues: item.rawValues,
    labels: item.labels,
    color: item.color,
    tooltip,
    rowCount: 1,
  };
}
