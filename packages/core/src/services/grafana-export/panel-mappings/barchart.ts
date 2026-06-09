import type { GrafanaExportInput } from '../types.js';
import { barOrientation } from '../utils.js';

export function barchartPanelOptions(input: GrafanaExportInput): Record<string, unknown> {
  const chartType = input.chart?.type;
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

export function barchartFieldCustomConfig(): Record<string, unknown> {
  return {
    fillOpacity: 94,
    lineWidth: 0,
  };
}
