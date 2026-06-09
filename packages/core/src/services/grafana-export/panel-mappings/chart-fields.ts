import type { GrafanaExportInput, GrafanaFieldOverride } from '../types.js';
import { displayName, GROUP_COLORS, inferUnit, mapPanelType, valueColumns } from '../utils.js';

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
