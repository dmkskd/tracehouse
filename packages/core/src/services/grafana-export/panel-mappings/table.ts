import type {
  GrafanaCellStyle,
  GrafanaExportInput,
  GrafanaFieldOverride,
  GrafanaRagCellStyle,
  GrafanaTransformation,
} from '../types.js';
import {
  displayName,
  inferUnit,
  mapThresholds,
  resolveNumericColumnMax,
  resolveResultColumn,
  radarImageColumn,
  sparklineImageColumn,
} from '../utils.js';

export function tablePanelOptions(input?: GrafanaExportInput): Record<string, unknown> {
  const hasRadar = (input?.cellStyles ?? []).some(style => style.type === 'radar');
  return {
    showHeader: true,
    cellHeight: hasRadar ? 'lg' : 'md',
    footer: { show: false },
  };
}

export function tableFieldCustomConfig(): Record<string, unknown> {
  return {
    align: 'auto',
    inspect: false,
    cellOptions: { type: 'auto', wrapText: false },
  };
}

function resolveGaugeMax(
  gauge: Extract<GrafanaCellStyle, { type: 'gauge' }> | undefined,
  input: GrafanaExportInput,
): number | undefined {
  if (!gauge) return undefined;
  if (typeof gauge.max === 'number') return gauge.max;
  return resolveNumericColumnMax(gauge.max, input);
}

function resolveGaugeMaxColumn(
  gauge: Extract<GrafanaCellStyle, { type: 'gauge' }> | undefined,
  input: GrafanaExportInput,
): string | undefined {
  return typeof gauge?.max === 'string' ? resolveResultColumn(gauge.max, input.resultColumns) : undefined;
}

function isLongTextColumn(column: string): boolean {
  const lower = column.toLowerCase();
  if (lower.endsWith('_hash') || lower.endsWith('_id')) return false;
  return lower.includes('query') || lower.includes('sql') || lower.includes('statement') || lower.includes('message') || lower.includes('exception');
}

function mergeOverride(
  overrides: GrafanaFieldOverride[],
  column: string,
  properties: GrafanaFieldOverride['properties'],
): void {
  const existing = overrides.find(override => override.matcher.id === 'byName' && override.matcher.options === column);
  if (existing) {
    const existingIds = new Set(existing.properties.map(property => property.id));
    existing.properties.push(...properties.filter(property => !existingIds.has(property.id)));
    return;
  }

  overrides.push({
    matcher: { id: 'byName', options: column },
    properties,
  });
}

export function tableFieldOverrides(input: GrafanaExportInput): GrafanaFieldOverride[] {
  const styles = input.cellStyles ?? [];
  const byColumn = new Map<string, GrafanaCellStyle[]>();
  for (const style of styles) {
    if (style.type === 'radar' && !style.column) continue;
    const column = style.column;
    if (!column) continue;
    const existing = byColumn.get(column) ?? [];
    existing.push(style);
    byColumn.set(column, existing);
  }

  const overrides: GrafanaFieldOverride[] = [];
  const helperColumns = new Set<string>();
  for (const [column, columnStyles] of byColumn) {
    const matchedColumn = resolveResultColumn(column, input.resultColumns);
    if (!matchedColumn) continue;

    const properties: GrafanaFieldOverride['properties'] = [
      { id: 'displayName', value: displayName(matchedColumn) },
    ];
    const gauge = columnStyles.find((style): style is Extract<GrafanaCellStyle, { type: 'gauge' }> => style.type === 'gauge');
    const rag = columnStyles.find((style): style is GrafanaRagCellStyle => style.type === 'rag');
    const unit = inferUnit(matchedColumn, gauge?.unit);
    const gaugeMax = resolveGaugeMax(gauge, input);
    const gaugeMaxColumn = resolveGaugeMaxColumn(gauge, input);
    if (gaugeMaxColumn && gaugeMaxColumn !== matchedColumn) helperColumns.add(gaugeMaxColumn);

    if (unit) properties.push({ id: 'unit', value: unit });
    if (gaugeMax !== undefined) {
      properties.push({ id: 'min', value: 0 });
      properties.push({ id: 'max', value: gaugeMax });
    }
    if (gauge) {
      properties.push({ id: 'custom.align', value: 'right' });
      properties.push({ id: 'custom.width', value: 190 });
      properties.push({ id: 'custom.cellOptions', value: { type: 'gauge', mode: 'basic', valueDisplayMode: 'text' } });
    }
    const thresholds = rag?.mode === 'numeric' ? mapThresholds([rag]) : undefined;
    if (thresholds) {
      properties.push({ id: 'thresholds', value: thresholds });
      properties.push({ id: 'color', value: { mode: 'thresholds' } });
      if (!gauge) {
        properties.push({ id: 'custom.align', value: 'right' });
        properties.push({ id: 'custom.width', value: 120 });
        properties.push({ id: 'custom.cellOptions', value: { type: 'color-text' } });
      }
    }

    if (properties.length > 1) {
      mergeOverride(overrides, matchedColumn, properties);
    }
  }

  for (const column of helperColumns) {
    mergeOverride(overrides, column, [
      { id: 'displayName', value: displayName(column) },
      { id: 'custom.hidden', value: true },
    ]);
  }

  for (const column of input.resultColumns ?? []) {
    if (!isLongTextColumn(column)) continue;
    mergeOverride(overrides, column, [
      { id: 'displayName', value: displayName(column) },
      { id: 'custom.width', value: 420 },
      { id: 'custom.minWidth', value: 260 },
      { id: 'custom.inspect', value: true },
      { id: 'custom.cellOptions', value: { type: 'auto', wrapText: false } },
    ]);
  }

  for (const style of styles) {
    if (style.type !== 'sparkline') continue;
    const matchedColumn = resolveResultColumn(style.column, input.resultColumns);
    if (!matchedColumn) continue;
    mergeOverride(overrides, sparklineImageColumn(matchedColumn), [
      { id: 'displayName', value: displayName(matchedColumn) },
      { id: 'custom.align', value: 'center' },
      { id: 'custom.width', value: 120 },
      { id: 'custom.cellOptions', value: { type: 'image', alt: displayName(matchedColumn), title: displayName(matchedColumn) } },
    ]);
  }

  for (const style of styles) {
    if (style.type !== 'radar') continue;
    const displayColumn = style.radarColumn ?? style.column;
    if (!displayColumn) continue;
    mergeOverride(overrides, radarImageColumn(displayColumn), [
      { id: 'displayName', value: displayName(displayColumn) },
      { id: 'custom.align', value: 'center' },
      { id: 'custom.width', value: 96 },
      { id: 'custom.cellOptions', value: { type: 'image', alt: displayName(displayColumn), title: displayName(displayColumn) } },
    ]);
  }

  return overrides;
}

export function tableTransformations(input: GrafanaExportInput, panelType: string): GrafanaTransformation[] | undefined {
  if (panelType !== 'table') return undefined;
  const hiddenSparklineColumns = (input.cellStyles ?? [])
    .filter((style): style is Extract<GrafanaCellStyle, { type: 'sparkline' }> => style.type === 'sparkline')
    .map(style => resolveResultColumn(style.column, input.resultColumns) ?? style.column);
  const hiddenRadarColumns = (input.cellStyles ?? [])
    .filter((style): style is Extract<GrafanaCellStyle, { type: 'radar' }> => style.type === 'radar')
    .flatMap(style => {
      if (style.column) return [resolveResultColumn(style.column, input.resultColumns) ?? style.column];
      return Object.values(style.axes ?? {}).map(column => resolveResultColumn(column, input.resultColumns) ?? column);
    });
  const hiddenColumns = [...hiddenSparklineColumns, ...hiddenRadarColumns];
  if (hiddenColumns.length === 0) return undefined;

  return [
    {
      id: 'organize',
      options: {
        excludeByName: Object.fromEntries(hiddenColumns.map(column => [column, true])),
      },
    },
  ];
}
