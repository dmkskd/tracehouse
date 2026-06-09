import type { GrafanaExportInput, GrafanaFieldOverride, GrafanaPanel, GrafanaTransformation } from './types.js';
import { barchartFieldCustomConfig, barchartPanelOptions } from './panel-mappings/barchart.js';
import { chartFieldOverrides } from './panel-mappings/chart-fields.js';
import { piechartPanelOptions, piechartTransformations } from './panel-mappings/piechart.js';
import { tableFieldCustomConfig, tableFieldOverrides, tablePanelOptions, tableTransformations } from './panel-mappings/table.js';
import { timeseriesFieldCustomConfig, timeseriesPanelOptions } from './panel-mappings/timeseries.js';
import { defaultBarThresholds, inferUnit, mapPanelType, mapThresholds } from './utils.js';

// Keep this module as the narrow mapping boundary. If the exporter grows beyond
// plain panel JSON, this is the likely place to swap rendering to Grafana's
// Foundation SDK while preserving the TraceHouse export plan API.
export function mapPanelOptions(input: GrafanaExportInput): Record<string, unknown> {
  const chartType = input.chart?.type;
  const panelType = mapPanelType(chartType);

  switch (panelType) {
    case 'timeseries':
      return timeseriesPanelOptions(input);
    case 'barchart':
      return barchartPanelOptions(input);
    case 'piechart':
      return piechartPanelOptions(input);
    case 'table':
      return tablePanelOptions();
    default:
      return {};
  }
}

export function defaultFieldCustomConfig(panelType: string, chartType: string | undefined): Record<string, unknown> {
  switch (panelType) {
    case 'timeseries':
      return timeseriesFieldCustomConfig(chartType);
    case 'barchart':
      return barchartFieldCustomConfig();
    case 'table':
      return tableFieldCustomConfig();
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

export function fieldOverrides(input: GrafanaExportInput): GrafanaFieldOverride[] {
  return input.chart ? chartFieldOverrides(input) : tableFieldOverrides(input);
}

export function panelTransformations(input: GrafanaExportInput, panelType: string): GrafanaTransformation[] | undefined {
  if (panelType === 'piechart') return piechartTransformations(input);

  return tableTransformations(input, panelType);
}
