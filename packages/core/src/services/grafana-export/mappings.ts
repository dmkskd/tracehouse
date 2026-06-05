import type {
  GrafanaCellStyle,
  GrafanaExportInput,
  GrafanaFieldOverride,
  GrafanaPanel,
  GrafanaRagCellStyle,
  GrafanaTransformation,
} from './types.js';
import {
  barOrientation,
  defaultBarThresholds,
  displayName,
  GROUP_COLORS,
  inferUnit,
  mapPanelType,
  mapThresholds,
  resolveResultColumn,
  valueColumns,
} from './utils.js';

export function mapPanelOptions(input: GrafanaExportInput): Record<string, unknown> {
  const chartType = input.chart?.type;
  const panelType = mapPanelType(chartType);
  const defaultLegendPlacement = panelType === 'piechart' ? 'right' : 'bottom';
  const legendPlacement = input.panel?.legendPlacement ?? defaultLegendPlacement;
  const legend = legendPlacement === 'hidden'
    ? { displayMode: 'hidden', placement: 'bottom' }
    : { displayMode: 'table', placement: legendPlacement };

  switch (panelType) {
    case 'timeseries':
      return {
        tooltip: { mode: 'multi', sort: 'desc' },
        legend: { ...legend, calcs: ['lastNotNull', 'max'] },
      };
    case 'barchart': {
      const orientation = barOrientation(input.chart);
      return {
        orientation,
        xField: input.chart?.groupByColumn,
        xTickLabelRotation: 0,
        xTickLabelMaxLength: orientation === 'horizontal' ? 32 : 18,
        tooltip: { mode: 'multi', sort: 'desc' },
        legend: { displayMode: 'list', placement: 'bottom' },
        stacking: chartType === 'stacked_bar' ? 'normal' : 'none',
        groupWidth: chartType === 'grouped_bar' ? 0.7 : 0.8,
        barWidth: 0.85,
        showValue: 'never',
      };
    }
    case 'piechart':
      return {
        tooltip: { mode: 'single' },
        legend: { ...legend, values: ['value', 'percent'] },
        pieType: 'donut',
        displayLabels: ['name', 'percent'],
      };
    case 'table':
      return {
        showHeader: true,
        cellHeight: 'sm',
        footer: { show: false },
      };
    default:
      return {};
  }
}

export function defaultFieldCustomConfig(panelType: string, chartType: string | undefined): Record<string, unknown> {
  switch (panelType) {
    case 'timeseries':
      return {
        drawStyle: 'line',
        lineInterpolation: 'smooth',
        lineWidth: 2,
        fillOpacity: chartType === 'area' ? 18 : 0,
        pointSize: 4,
        showPoints: 'never',
        spanNulls: true,
      };
    case 'barchart':
      return {
        fillOpacity: 94,
        lineWidth: 0,
      };
    case 'table':
      return {
        align: 'auto',
        inspect: false,
      };
    default:
      return {};
  }
}

export function chartFieldDefaults(input: GrafanaExportInput, panelType: string): GrafanaPanel['fieldConfig']['defaults'] {
  return {
    thresholds: panelType === 'table'
      ? undefined
      : mapThresholds(input.rag) ?? (panelType === 'barchart' ? defaultBarThresholds() : undefined),
    unit: inferUnit(input.chart?.valueColumn, input.chart?.unit),
    ...(input.chart?.color
      ? { color: { mode: 'fixed', fixedColor: input.chart.color } }
      : panelType === 'barchart' ? { color: { mode: 'thresholds' } } : {}),
    custom: defaultFieldCustomConfig(panelType, input.chart?.type),
  };
}

export function chartFieldOverrides(input: GrafanaExportInput): GrafanaFieldOverride[] {
  const chart = input.chart;
  if (!chart) return [];

  const panelType = mapPanelType(chart.type);
  const columns = (panelType === 'barchart' && chart.seriesValues?.length)
    ? chart.seriesValues
    : valueColumns(chart);
  const shouldColorSeries = columns.length > 1 && (panelType === 'timeseries' || (panelType === 'barchart' && Boolean(chart.seriesValues?.length)));

  return columns.map((column, index) => {
    const properties: GrafanaFieldOverride['properties'] = [
      { id: 'displayName', value: displayName(column) },
    ];
    const unit = inferUnit(column, chart.seriesValues?.includes(column) || chart.valueColumns?.length === 1 || !chart.valueColumns?.length ? chart.unit : undefined);
    if (unit) properties.push({ id: 'unit', value: unit });
    if (shouldColorSeries) {
      properties.push({ id: 'color', value: { mode: 'fixed', fixedColor: GROUP_COLORS[index % GROUP_COLORS.length] } });
    }
    return {
      matcher: { id: 'byName', options: column },
      properties,
    };
  });
}

export function tableFieldOverrides(input: GrafanaExportInput): GrafanaFieldOverride[] {
  const styles = input.cellStyles ?? [];
  const byColumn = new Map<string, GrafanaCellStyle[]>();
  for (const style of styles) {
    const existing = byColumn.get(style.column) ?? [];
    existing.push(style);
    byColumn.set(style.column, existing);
  }

  const overrides: GrafanaFieldOverride[] = [];
  for (const [column, columnStyles] of byColumn) {
    const matchedColumn = resolveResultColumn(column, input.resultColumns);
    if (!matchedColumn) continue;

    const properties: GrafanaFieldOverride['properties'] = [
      { id: 'displayName', value: displayName(matchedColumn) },
    ];
    const gauge = columnStyles.find((style): style is Extract<GrafanaCellStyle, { type: 'gauge' }> => style.type === 'gauge');
    const rag = columnStyles.find((style): style is GrafanaRagCellStyle => style.type === 'rag');
    const unit = inferUnit(matchedColumn, gauge?.unit);

    if (unit) properties.push({ id: 'unit', value: unit });
    if (typeof gauge?.max === 'number') {
      properties.push({ id: 'min', value: 0 });
      properties.push({ id: 'max', value: gauge.max });
    }
    if (gauge) {
      properties.push({ id: 'custom.cellOptions', value: { type: 'gauge', mode: 'basic' } });
    }
    const thresholds = rag?.mode === 'numeric' ? mapThresholds([rag]) : undefined;
    if (thresholds) {
      properties.push({ id: 'thresholds', value: thresholds });
      properties.push({ id: 'color', value: { mode: 'thresholds' } });
      if (!gauge) {
        properties.push({ id: 'custom.cellOptions', value: { type: 'color-text' } });
      }
    }

    if (properties.length > 1) {
      overrides.push({
        matcher: { id: 'byName', options: matchedColumn },
        properties,
      });
    }
  }

  return overrides;
}

export function fieldOverrides(input: GrafanaExportInput): GrafanaFieldOverride[] {
  return input.chart ? chartFieldOverrides(input) : tableFieldOverrides(input);
}

export function tableTransformations(input: GrafanaExportInput, panelType: string): GrafanaTransformation[] | undefined {
  if (panelType !== 'table') return undefined;
  const hiddenSparklineColumns = (input.cellStyles ?? [])
    .filter((style): style is Extract<GrafanaCellStyle, { type: 'sparkline' }> => style.type === 'sparkline')
    .map(style => style.column);
  if (hiddenSparklineColumns.length === 0) return undefined;

  return [
    {
      id: 'organize',
      options: {
        excludeByName: Object.fromEntries(hiddenSparklineColumns.map(column => [column, true])),
      },
    },
  ];
}
