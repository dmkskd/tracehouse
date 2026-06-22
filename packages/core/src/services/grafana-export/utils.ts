import type { GrafanaExportInput, GrafanaRagRuleInput, GrafanaThreshold } from './types.js';

export const GROUP_COLORS = [
  '#3b82f6',
  '#f59e0b',
  '#ef4444',
  '#10b981',
  '#8b5cf6',
  '#ec4899',
  '#06b6d4',
  '#f97316',
  '#84cc16',
  '#6366f1',
];

export function mapPanelType(chartType: string | undefined): string {
  if (!chartType) return 'table';
  switch (chartType) {
    case 'line':
    case 'area':
    case 'grouped_line':
      return 'timeseries';
    case 'bar':
    case 'grouped_bar':
    case 'stacked_bar':
      return 'barchart';
    case 'pie':
      return 'piechart';
    case 'radar':
      return 'table';
    default:
      return 'timeseries';
  }
}

export function barOrientation(chart: GrafanaExportInput['chart']): 'horizontal' | 'vertical' {
  const groupColumn = chart?.groupByColumn?.toLowerCase() ?? '';
  const isTimeBucket = ['t', 'time', 'minute', 'hour', 'day', 'event_time'].includes(groupColumn) || groupColumn.endsWith('_time');
  return chart?.orientation ?? (isTimeBucket ? 'vertical' : 'horizontal');
}

export function mapThresholds(rag: GrafanaRagRuleInput[] | undefined): { mode: string; steps: GrafanaThreshold[] } | undefined {
  if (!rag?.length) return undefined;
  const numericRule = rag.find(r => r.mode === 'numeric');
  if (!numericRule) return undefined;

  const steps: GrafanaThreshold[] = [];
  if (numericRule.direction === 'desc') {
    steps.push({ color: 'red', value: null });
    if (numericRule.amberThreshold != null) steps.push({ color: 'orange', value: numericRule.amberThreshold });
    if (numericRule.greenThreshold != null) steps.push({ color: 'green', value: numericRule.greenThreshold });
  } else {
    steps.push({ color: 'green', value: null });
    if (numericRule.greenThreshold != null) steps.push({ color: 'orange', value: numericRule.greenThreshold });
    if (numericRule.amberThreshold != null) steps.push({ color: 'red', value: numericRule.amberThreshold });
  }
  return { mode: 'absolute', steps };
}

export function defaultBarThresholds(): { mode: string; steps: GrafanaThreshold[] } {
  return {
    mode: 'percentage',
    steps: [
      { color: '#38bdf8', value: null },
      { color: '#22c55e', value: 20 },
      { color: '#6366f1', value: 40 },
      { color: '#7c3aed', value: 60 },
      { color: '#a855f7', value: 80 },
    ],
  };
}

export function mapUnit(unit: string | undefined): string | undefined {
  if (!unit) return undefined;
  const map: Record<string, string> = {
    ms: 'ms',
    s: 's',
    '%': 'percent',
    MB: 'decmbytes',
    GB: 'decgbytes',
    bytes: 'bytes',
    B: 'bytes',
    KB: 'deckbytes',
    TB: 'dectbytes',
    ops: 'ops',
    qps: 'ops',
    'req/s': 'reqps',
  };
  return map[unit] ?? undefined;
}

export function inferUnit(column: string | undefined, explicitUnit?: string): string | undefined {
  const mapped = mapUnit(explicitUnit);
  if (mapped) return mapped;
  if (!column) return undefined;

  const c = column.toLowerCase();
  if (c === 'bytes' || c.endsWith('_bytes') || c.includes('bytes_') || c.includes('bytes')) return 'bytes';
  if (c.endsWith('_size') || c === 'size') return 'bytes';
  if (c.endsWith('_ms') || c.includes('duration_ms') || c.includes('elapsed_ms')) return 'ms';
  if (c.includes('per_sec') || c.endsWith('_rate')) return 'short';
  if (c.endsWith('_seconds') || c.endsWith('_sec')) return 's';
  if (c.includes('percent') || c.endsWith('_pct') || c.endsWith('_ratio')) return 'percentunit';
  if (c.includes('rows') || c.includes('count') || c.includes('parts') || c.includes('queries') || c.includes('exec')) return 'short';
  return undefined;
}

export function valueColumns(chart: GrafanaExportInput['chart']): string[] {
  if (!chart) return [];
  if (chart.valueColumns?.length) return chart.valueColumns;
  return chart.valueColumn ? [chart.valueColumn] : [];
}

export function displayName(column: string): string {
  return column
    .replace(/_/g, ' ')
    .replace(/\b\w/g, ch => ch.toUpperCase());
}

export function sparklineImageColumn(column: string): string {
  return `${column}__grafana_sparkline`;
}

export function radarImageColumn(column: string): string {
  return `${column}__grafana_radar`;
}

export function resolveResultColumn(column: string, resultColumns: string[] | undefined): string | undefined {
  if (!resultColumns?.length) return column;
  if (resultColumns.includes(column)) return column;
  const lower = column.toLowerCase();
  return resultColumns.find(resultColumn => resultColumn.toLowerCase() === lower);
}

export function numericValue(value: unknown): number | undefined {
  const numeric = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(numeric) ? numeric : undefined;
}

export function resolveNumericColumnMax(column: string, input: GrafanaExportInput): number | undefined {
  const maxColumn = resolveResultColumn(column, input.resultColumns);
  if (!maxColumn || !input.resultRows?.length) return undefined;

  const values = input.resultRows
    .map(row => numericValue(row[maxColumn]))
    .filter((value): value is number => value !== undefined);
  return values.length ? Math.max(...values) : undefined;
}

export function quoteIdent(identifier: string): string {
  return `\`${identifier.replace(/`/g, '``')}\``;
}

export function cleanTracehouseSql(sql: string): string {
  return sql
    .split('\n')
    .filter(line => !/^\s*--\s*@(meta|chart|cell|rag|drill|link)\s*:/i.test(line) && !/^\s*--\s*Source:/i.test(line))
    .join('\n')
    .trim();
}

export function convertTracehouseTimeRangeMacros(sql: string): string {
  return sql
    .replace(
      /\b([A-Za-z_][\w.]*)\s*>=\s*toDate\(\{\{time_range\}\}\)\s+AND\s+([A-Za-z_][\w.]*)\s*>\s*\{\{time_range\}\}/gi,
      '$__dateTimeFilter($1, $2)',
    )
    .replace(
      /\b([A-Za-z_][\w.]*)\s*>\s*\{\{time_range\}\}/g,
      '$__timeFilter($1)',
    )
    .replaceAll('toDate({{time_range}})', 'toDate($__fromTime)')
    .replaceAll('{{time_range}}', '$__fromTime');
}

export function panelGridHeight(input: GrafanaExportInput, panelType: string): number {
  if (input.panel?.height != null) return input.panel.height;
  if (panelType === 'table') return 9;

  if (panelType === 'barchart') {
    const orientation = barOrientation(input.chart);
    const rows = input.chart?.maxRows;
    if (orientation === 'horizontal' && rows) {
      return Math.min(36, Math.max(10, Math.ceil(6 + rows * 0.55)));
    }
  }

  return 10;
}
